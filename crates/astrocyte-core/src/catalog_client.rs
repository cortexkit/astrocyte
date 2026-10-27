//! Typed fusiform catalog boundary for pricing.
//!
//! `fusiform-protocol` owns the request and response envelopes and money types.
//! Its served rate facts remain raw JSON, so this boundary validates them before
//! pricing sees them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fusiform_protocol::{
    CatalogGetRequest, CatalogGetResponse, RateValue, RefusalKind, StatusRequest, StatusResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const RATE_FACT_PREFIX: &str = "rate.";
const HISTORY_START_MARKER: &str = "fusiform's history begins at ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogCallError {
    pub code: String,
    pub message: String,
}

/// The transport supplied by the serving slice. Calls are synchronous because
/// pricing performs them only after its read transaction has committed.
pub trait CatalogConnection: Send + Sync {
    fn catalog_status(&self, request: StatusRequest) -> Result<StatusResponse, CatalogCallError>;

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogClientError {
    Call(CatalogCallError),
    BoundaryDiscovery(String),
    BoundaryRefusal {
        kind: RefusalKind,
        error: CatalogCallError,
    },
    InvalidRate {
        reason: String,
        entry: Value,
    },
    UnrecognizedRateFact {
        fact_key: String,
        entry: Value,
    },
    ProtocolEncoding(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub source: String,
    pub resolved_at_ms: i64,
    pub model_key: String,
    pub facts: BTreeMap<String, Value>,
    pub withheld_facts: BTreeMap<String, Value>,
    pub uncertain_facts: BTreeMap<String, Value>,
    pub overridden_facts: BTreeMap<String, Value>,
    pub alias_target: Option<CatalogModel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogModel {
    pub provider_id: String,
    pub model_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogLookup {
    PreHistory {
        history_start_ms: i64,
    },
    Resolved(CatalogSnapshot),
    /// A historical read the catalog refused as outside its coverage. The
    /// request was fine and the catalog has no row for the model, so there
    /// is nothing to retry: ledger pricing records the event unpriced with
    /// `no_catalog_coverage` and the multiplier treats it as a miss that may
    /// still resolve through a current-read alias.
    NoCoverage,
}

#[derive(Clone)]
pub struct CatalogClient {
    connection: Arc<dyn CatalogConnection>,
    history_start_ms: i64,
}

impl CatalogClientError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::BoundaryRefusal { kind, .. } if kind.is_retryable()
        )
    }
}

impl CatalogClient {
    /// Creates a client after discovering the earliest instant fusiform can
    /// resolve on this connection, using status or a refused read at time zero.
    pub fn connect(connection: Arc<dyn CatalogConnection>) -> Result<Self, CatalogClientError> {
        let history_start_ms = Self::discover_history_start_ms(connection.as_ref())?;
        Ok(Self::new(connection, history_start_ms))
    }

    pub(crate) fn new(connection: Arc<dyn CatalogConnection>, history_start_ms: i64) -> Self {
        Self {
            connection,
            history_start_ms,
        }
    }

    pub fn history_start_ms(&self) -> i64 {
        self.history_start_ms
    }

    /// Uses the typed status boundary when available. Older producers omit it,
    /// so request time zero and parse the earliest covered instant from the
    /// refusal's text. Status errors return without probing; the serving loop
    /// leaves pricing pending and retries discovery each tick.
    /// [`CatalogClient::connect`] calls this before constructing the client;
    /// no boundary constant is embedded here.
    fn discover_history_start_ms(
        connection: &dyn CatalogConnection,
    ) -> Result<i64, CatalogClientError> {
        match connection.catalog_status(StatusRequest::default()) {
            Ok(status) => {
                if let Some(boundary) = status.history_begins_at_ms {
                    tracing::info!(
                        history_start_ms = boundary,
                        path = "typed_status",
                        "discovered fusiform history boundary"
                    );
                    return Ok(boundary);
                }
            }
            Err(error) => {
                return Err(CatalogClientError::BoundaryRefusal {
                    kind: RefusalKind::of(&error.code),
                    error,
                });
            }
        }
        let request = CatalogGetRequest {
            source: None,
            at_ms: Some(0),
            fact_prefixes: Some(vec![RATE_FACT_PREFIX.to_string()]),
            include_retired: true,
            model_id: None,
            provider_id: None,
            auth_method: None,
        };
        let boundary = match connection.catalog_get(request) {
            Err(error) => match RefusalKind::of(&error.code) {
                RefusalKind::OutsideCoverage => {
                    parse_history_start_ms(&error.message).ok_or_else(|| {
                        CatalogClientError::BoundaryDiscovery(format!(
                            "outside-coverage refusal omitted a parseable history boundary: {}",
                            error.message
                        ))
                    })
                }
                // A producer older than fusiform-protocol 0.14 refuses the
                // pre-history probe as `bad_request`; 0.14 moved it to
                // `no_coverage`. The deployed producer's contract is whatever it
                // serves, so a bad_request carrying a parseable boundary IS the
                // boundary refusal from that older producer -- not our defect.
                // Only a bad_request with no boundary in it is malformed on our
                // side. Remove this arm once every fleet producer is >= 0.14.
                RefusalKind::Malformed => match parse_history_start_ms(&error.message) {
                    Some(boundary) => Ok(boundary),
                    None => Err(CatalogClientError::BoundaryRefusal {
                        kind: RefusalKind::Malformed,
                        error,
                    }),
                },
                kind => Err(CatalogClientError::BoundaryRefusal { kind, error }),
            },
            Ok(_) => Err(CatalogClientError::BoundaryDiscovery(
                "pre-history probe unexpectedly succeeded".into(),
            )),
        }?;
        tracing::info!(
            history_start_ms = boundary,
            path = "text_fallback",
            "discovered fusiform history boundary"
        );
        Ok(boundary)
    }

    pub fn get(
        &self,
        provider_id: &str,
        model_id: &str,
        at_ms: i64,
    ) -> Result<CatalogLookup, CatalogClientError> {
        if at_ms < self.history_start_ms {
            return Ok(CatalogLookup::PreHistory {
                history_start_ms: self.history_start_ms,
            });
        }
        self.get_at(provider_id, model_id, Some(at_ms))
    }

    /// Current reads expose curated aliases that have no historical identity.
    pub(crate) fn get_current(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Result<CatalogLookup, CatalogClientError> {
        self.get_at(provider_id, model_id, None)
    }

    fn get_at(
        &self,
        provider_id: &str,
        model_id: &str,
        at_ms: Option<i64>,
    ) -> Result<CatalogLookup, CatalogClientError> {
        let response = match self.connection.catalog_get(CatalogGetRequest {
            source: None,
            at_ms,
            fact_prefixes: Some(vec![RATE_FACT_PREFIX.to_string()]),
            include_retired: true,
            model_id: Some(model_id.to_string()),
            provider_id: Some(provider_id.to_string()),
            auth_method: None,
        }) {
            Ok(response) => response,
            // A historical read refused as outside coverage is an answer, not
            // a failure: the request was fine and the catalog has no row for
            // the model. Current reads keep the refusal as an error, as today,
            // so alias discovery sees the same refusal it always has.
            Err(error)
                if at_ms.is_some()
                    && RefusalKind::of(&error.code) == RefusalKind::OutsideCoverage =>
            {
                return Ok(CatalogLookup::NoCoverage);
            }
            Err(error) => return Err(CatalogClientError::Call(error)),
        };
        let model_key = format!("{provider_id}/{model_id}");
        // Each fact key appears at most once per model. Fusiform builds `models` as a
        // map keyed by fact key, and aliases, plane views and inherited rates all write
        // into that same map rather than appending, so there is no duplicate key here to
        // refuse or to choose between.
        let facts = response.models.get(&model_key).cloned().unwrap_or_default();
        let alias_target = response
            .aliased
            .iter()
            .find(|alias| alias.model == model_key)
            .map(|alias| CatalogModel {
                provider_id: alias.target_provider_id.clone(),
                model_id: alias.target_model_id.clone(),
            })
            .or_else(|| alias_target_from_rates(&facts));
        // Current reads discover identities, not prices. Only historical facts
        // are decoded for valuation, so unfamiliar current rate keys cannot
        // prevent an otherwise usable alias target from being discovered.
        if at_ms.is_some() {
            validate_rate_fact_keys(&facts)?;
        }
        let withheld_facts = response
            .withheld
            .iter()
            .filter(|fact| fact.model == model_key)
            .map(|fact| {
                serde_json::to_value(fact)
                    .map(|value| (fact.fact_key.clone(), value))
                    .map_err(|error| CatalogClientError::ProtocolEncoding(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let uncertain_facts = response
            .uncertain
            .iter()
            .filter(|fact| fact.provider_id == provider_id && fact.model_id == model_id)
            .map(|fact| {
                serde_json::to_value(fact)
                    .map(|value| (fact.fact_key.clone(), value))
                    .map_err(|error| CatalogClientError::ProtocolEncoding(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let overridden_facts = response
            .overridden
            .iter()
            .filter(|fact| fact.provider_id == provider_id && fact.model_id == model_id)
            .map(|fact| {
                serde_json::to_value(fact)
                    .map(|value| (fact.fact_key.clone(), value))
                    .map_err(|error| CatalogClientError::ProtocolEncoding(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        Ok(CatalogLookup::Resolved(CatalogSnapshot {
            source: response.source,
            resolved_at_ms: response.resolved_at_ms,
            model_key,
            facts,
            withheld_facts,
            uncertain_facts,
            overridden_facts,
            alias_target,
        }))
    }
}

fn alias_target_from_rates(facts: &BTreeMap<String, Value>) -> Option<CatalogModel> {
    facts.iter().find_map(|(key, value)| {
        if !key.starts_with(RATE_FACT_PREFIX) {
            return None;
        }
        let origin = match serde_json::from_value::<RateValue>(value.clone()).ok()? {
            RateValue::Priced { inherited_from, .. }
            | RateValue::StatedZero { inherited_from }
            | RateValue::Unpriced { inherited_from, .. } => inherited_from?,
            RateValue::BilledAs { .. } => return None,
        };
        if origin.basis != "alias" {
            return None;
        }
        Some(CatalogModel {
            provider_id: origin.provider_id,
            model_id: origin.model_id?,
        })
    })
}

/// The token roles the catalog prices, each at `rate.<role>`.
pub const RATE_ROLES: [&str; 6] = [
    "input",
    "output",
    "cache_read",
    "cache_write",
    "reasoning",
    "input_audio",
];

/// Mode names a request record uses for ordinary (non-premium) service. A
/// request stated in one of these prices at the unconditional `rate.<role>`.
pub const STANDARD_MODES: [&str; 2] = ["standard", "default"];

/// The condition under which a conditional rate `rate.<role>.<selector>.<value>`
/// replaces the unconditional `rate.<role>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateCondition {
    /// `above_context.<threshold>`: the request's prompt exceeds the threshold.
    AboveContext(u64),
    /// `mode.<name>`: the request's own record states it ran in that mode
    /// (for example `fast`).
    Mode(String),
    /// A selector family this decoder has no producer for. It applies only
    /// when a request's record states that condition, and no record states
    /// one, so it never applies and the unconditional rate prices the request.
    Unknown { selector: String, value: String },
}

/// One decoded `rate.*` fact key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateKey {
    pub role: &'static str,
    /// `None` for the unconditional `rate.<role>`.
    pub condition: Option<RateCondition>,
}

/// The one rate-key grammar, shared by ledger pricing and the multiplier:
/// `rate.<role>` (unconditional) or `rate.<role>.<selector>.<value>`
/// (conditional). A key that fits neither shape, names a role outside
/// [`RATE_ROLES`], or has an empty segment is an error. So is an
/// `above_context` threshold that is not a whole number: that selector is one
/// astrocyte knows, and a threshold it cannot read would otherwise decide
/// nothing.
pub fn parse_rate_key(key: &str) -> Result<RateKey, String> {
    let rest = key
        .strip_prefix(RATE_FACT_PREFIX)
        .ok_or_else(|| format!("rate key {key:?} does not start with {RATE_FACT_PREFIX:?}"))?;
    let segments: Vec<&str> = rest.split('.').collect();
    let role = RATE_ROLES
        .into_iter()
        .find(|role| *role == segments[0])
        .ok_or_else(|| format!("rate key {key:?}: role {:?} is not priced", segments[0]))?;
    let condition = match segments[1..] {
        [] => None,
        [selector, value] if !selector.is_empty() && !value.is_empty() => Some(match selector {
            "above_context" => {
                if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(format!(
                        "tier key {key:?}: threshold {value:?} is not a whole number"
                    ));
                }
                RateCondition::AboveContext(value.parse::<u64>().map_err(|error| {
                    format!("tier key {key:?}: threshold {value:?} is not a u64 ({error})")
                })?)
            }
            "mode" => RateCondition::Mode(value.to_string()),
            _ => RateCondition::Unknown {
                selector: selector.to_string(),
                value: value.to_string(),
            },
        }),
        _ => {
            return Err(format!(
                "rate key {key:?} is neither rate.<role> nor rate.<role>.<selector>.<value>"
            ))
        }
    };
    Ok(RateKey { role, condition })
}

/// The mode-conditional rate a request prices at, from the mode its own
/// record states. `None` means the unconditional `rate.<role>` prices it: the
/// record states a standard mode, or states no mode at all. Callers that
/// report how many tokens priced without a recorded mode check for `None`
/// input themselves.
pub fn stated_mode_condition(recorded_mode: Option<&str>) -> Option<&str> {
    recorded_mode.filter(|mode| !STANDARD_MODES.contains(mode))
}

/// `rate.<role>.mode.<mode>`.
pub fn mode_rate_key(role: &str, mode: &str) -> String {
    format!("{RATE_FACT_PREFIX}{role}.mode.{mode}")
}

/// Every `.above_context.<threshold>` key the catalog served for this model.
///
/// An unparseable threshold is an ERROR, never dropped: the producer declared
/// a tier, and a request that crosses it must go unpriced rather than price
/// at the base rate. Dropping the key made the set smaller -- possibly empty --
/// and silently bypassed the threshold-crossing refusal for exactly the
/// models whose tier spelling we did not understand.
pub fn tier_thresholds(facts: &BTreeMap<String, Value>) -> Result<BTreeSet<u64>, String> {
    let mut thresholds = BTreeSet::new();
    for key in facts.keys() {
        match parse_rate_key(key) {
            Ok(RateKey {
                condition: Some(RateCondition::AboveContext(threshold)),
                ..
            }) => {
                thresholds.insert(threshold);
            }
            Err(error) if key.contains(".above_context.") => return Err(error),
            _ => {}
        }
    }
    Ok(thresholds)
}

fn validate_rate_fact_keys(facts: &BTreeMap<String, Value>) -> Result<(), CatalogClientError> {
    for (key, entry) in facts {
        if !key.starts_with(RATE_FACT_PREFIX) {
            continue;
        }
        if parse_rate_key(key).is_err() {
            return Err(CatalogClientError::UnrecognizedRateFact {
                fact_key: key.clone(),
                entry: entry.clone(),
            });
        }
    }
    Ok(())
}

fn parse_history_start_ms(message: &str) -> Option<i64> {
    let suffix = message.split_once(HISTORY_START_MARKER)?.1;
    let digits = suffix
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use fusiform_protocol::{CatalogGetResponse, Floor, RateValue, UnitProvenance, UnpricedReason};
    use serde_json::json;

    use super::*;

    struct FakeConnection {
        boundary: i64,
        boundary_code: String,
        boundary_message: Option<String>,
        response: CatalogGetResponse,
        requests: Mutex<Vec<CatalogGetRequest>>,
    }

    impl FakeConnection {
        fn new(boundary: i64, facts: BTreeMap<String, Value>) -> Self {
            Self {
                boundary,
                boundary_code: "no_coverage".into(),
                boundary_message: None,
                response: CatalogGetResponse {
                    source: "models.dev".into(),
                    resolved_at_ms: boundary + 10,
                    catalog_version: 99,
                    models: BTreeMap::from([("provider/model".into(), facts)]),
                    withheld: vec![],
                    uncertain: vec![],
                    overridden: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
                },
                requests: Mutex::new(Vec::new()),
            }
        }

        fn with_boundary_code(mut self, code: &str) -> Self {
            self.boundary_code = code.into();
            self
        }

        /// Replace the refusal's message wholesale, so a test can serve a
        /// refusal that carries no boundary at all.
        fn with_boundary_message(mut self, message: &str) -> Self {
            self.boundary_message = Some(message.into());
            self
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    impl CatalogConnection for FakeConnection {
        fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            let at_ms = request.at_ms;
            self.requests.lock().unwrap().push(request);
            if at_ms.is_some_and(|instant| instant < self.boundary) {
                return Err(CatalogCallError {
                    code: self.boundary_code.clone(),
                    message: self.boundary_message.clone().unwrap_or_else(|| {
                        format!(
                            "no record at {}: fusiform's history begins at {}. An empty answer would claim the catalog was empty; it was unobserved.",
                            at_ms.unwrap(),
                            self.boundary
                        )
                    }),
                });
            }
            let mut response = self.response.clone();
            response.resolved_at_ms = at_ms.unwrap_or(response.resolved_at_ms);
            Ok(response)
        }
    }

    fn priced_rate(exponent: i64) -> Value {
        json!({
            "state": "priced",
            "units": 3_000,
            "exponent": exponent,
            "currency": "USD",
            "unit_provenance": {"kind": "stated"}
        })
    }

    #[test]
    fn discovered_history_boundary_changes_pre_history_behavior() {
        let early = Arc::new(FakeConnection::new(100, BTreeMap::new()));
        let late = Arc::new(FakeConnection::new(200, BTreeMap::new()));
        let early_client = CatalogClient::connect(early.clone()).unwrap();
        let late_client = CatalogClient::connect(late.clone()).unwrap();
        assert_eq!(
            (
                early_client.history_start_ms(),
                late_client.history_start_ms()
            ),
            (100, 200)
        );
        assert_eq!((early.request_count(), late.request_count()), (1, 1));
        assert!(matches!(
            early_client.get("provider", "model", 150).unwrap(),
            CatalogLookup::Resolved(_)
        ));
        let late_calls_before = late.request_count();
        assert_eq!(
            late_client.get("provider", "model", 150).unwrap(),
            CatalogLookup::PreHistory {
                history_start_ms: 200
            }
        );
        assert_eq!(late.request_count(), late_calls_before);
    }

    #[test]
    fn boundary_discovery_refuses_success_or_unparseable_error() {
        let success = FakeConnection::new(-1, BTreeMap::new());
        assert!(matches!(
            CatalogClient::discover_history_start_ms(&success),
            Err(CatalogClientError::BoundaryDiscovery(_))
        ));
        let missing_boundary = FakeConnection::new(10, BTreeMap::new());
        struct MissingBoundary;
        impl CatalogConnection for MissingBoundary {
            fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
                Ok(
                    serde_json::from_value(json!({"source": "fake", "catalog_version": 0,
                    "model_count": 0, "era_count": 0, "recent_polls": []}))
                    .unwrap(),
                )
            }

            fn catalog_get(
                &self,
                _: CatalogGetRequest,
            ) -> Result<CatalogGetResponse, CatalogCallError> {
                Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "no historical record".into(),
                })
            }
        }
        assert!(matches!(
            CatalogClient::discover_history_start_ms(&MissingBoundary),
            Err(CatalogClientError::BoundaryDiscovery(_))
        ));
        assert_eq!(missing_boundary.request_count(), 0);
    }

    #[test]
    fn boundary_refusal_is_classified_outside_coverage_not_by_code_string() {
        let wire_control = FakeConnection::new(64, BTreeMap::new());
        let refusal = wire_control
            .catalog_get(CatalogGetRequest {
                source: None,
                at_ms: Some(0),
                fact_prefixes: Some(vec![RATE_FACT_PREFIX.into()]),
                include_retired: true,
                model_id: None,
                provider_id: None,
                auth_method: None,
            })
            .unwrap_err();
        assert_eq!(RefusalKind::of(&refusal.code), RefusalKind::OutsideCoverage);
        assert!(!refusal.message.trim().is_empty());
        assert_eq!(parse_history_start_ms(&refusal.message), Some(64));

        let covered = FakeConnection::new(64, BTreeMap::new());
        assert_eq!(CatalogClient::discover_history_start_ms(&covered), Ok(64));

        // A producer older than protocol 0.14 refuses the probe as bad_request
        // with the same boundary sentence. That is the deployed contract of
        // that producer, so it discovers -- this arm crash-looped production
        // when it was written as a refusal. Only a bad_request with NO
        // boundary in it is malformed on our side.
        let legacy = FakeConnection::new(64, BTreeMap::new()).with_boundary_code("bad_request");
        assert_eq!(CatalogClient::discover_history_start_ms(&legacy), Ok(64));

        let malformed = FakeConnection::new(64, BTreeMap::new())
            .with_boundary_code("bad_request")
            .with_boundary_message("unknown field `fact_prefixes`");
        assert!(matches!(
            CatalogClient::discover_history_start_ms(&malformed),
            Err(CatalogClientError::BoundaryRefusal {
                kind: RefusalKind::Malformed,
                ..
            })
        ));
    }

    #[test]
    fn unrecognized_refusal_code_is_not_retried() {
        let future =
            Arc::new(FakeConnection::new(64, BTreeMap::new()).with_boundary_code("future_code"));
        let Err(error) = CatalogClient::connect(future.clone()) else {
            panic!("unrecognized refusal unexpectedly connected");
        };
        assert!(matches!(
            error,
            CatalogClientError::BoundaryRefusal {
                kind: RefusalKind::Unrecognized,
                ..
            }
        ));
        assert!(!error.is_retryable());
        assert_eq!(future.request_count(), 1);
    }

    #[test]
    fn unknown_state_and_reason_are_refused_by_the_protocol_type() {
        for entry in [
            json!({
                "state": "future_variant",
                "units": 7,
                "metadata": {"must_survive": true}
            }),
            json!({
                "state": "unpriced",
                "reason": "future_reason",
                "metadata": {"must_survive": true}
            }),
        ] {
            assert!(serde_json::from_value::<RateValue>(entry).is_err());
        }
    }

    #[test]
    fn stated_zero_is_not_collapsed_into_priced_zero_or_unpriced() {
        let stated_zero: RateValue =
            serde_json::from_value(json!({"state": "stated_zero"})).unwrap();
        let priced_zero: RateValue = serde_json::from_value(json!({
            "state": "priced",
            "units": 0,
            "exponent": 9,
            "currency": "USD",
            "unit_provenance": {"kind": "stated"}
        }))
        .unwrap();
        let unpriced: RateValue = serde_json::from_value(json!({
            "state": "unpriced",
            "reason": "missing_rate"
        }))
        .unwrap();
        assert_eq!(
            stated_zero,
            RateValue::StatedZero {
                inherited_from: None
            }
        );
        assert!(matches!(priced_zero, RateValue::Priced { .. }));
        assert!(matches!(unpriced, RateValue::Unpriced { .. }));
        assert_ne!(stated_zero, priced_zero);
        assert_ne!(stated_zero, unpriced);
        assert_ne!(priced_zero, unpriced);
    }

    #[test]
    fn protocol_rate_value_refuses_unknown_floor_variant() {
        let mut entry = priced_rate(9);
        entry["floor"] = json!({"kind": "future_floor"});
        assert!(serde_json::from_value::<RateValue>(entry).is_err());
    }

    #[test]
    fn recognized_unpriced_reason_vocabulary_is_exhaustive() {
        for (reason, expected) in [
            ("missing_rate", UnpricedReason::MissingRate),
            ("no_catalog_coverage", UnpricedReason::NoCatalogCoverage),
            ("unknown_charge_basis", UnpricedReason::UnknownChargeBasis),
            ("not_established", UnpricedReason::NotEstablished),
        ] {
            assert_eq!(
                serde_json::from_value::<RateValue>(json!({"state": "unpriced", "reason": reason}))
                    .unwrap(),
                RateValue::Unpriced {
                    reason: expected,
                    inherited_from: None
                }
            );
        }
    }

    #[test]
    fn real_served_rate_fixture_decodes_through_the_protocol_type() {
        let bytes = br#"{"state":"priced","units":300000000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"assumed_by_policy","policy":"models-dev-usd-v1"}}"#;
        let decoded: RateValue = serde_json::from_slice(bytes).unwrap();
        let RateValue::Priced {
            amount,
            floor,
            inherited_from,
        } = decoded
        else {
            panic!("served priced rate decoded as another state");
        };
        assert!(
            inherited_from.is_none(),
            "a published rate carries no origin"
        );
        assert_eq!(amount.units, 300_000_000);
        assert_eq!(amount.exponent, 9);
        assert_eq!(amount.currency.as_str(), "USD");
        assert_eq!(floor, Floor::Unknown);
        assert!(matches!(
            amount.unit_provenance,
            UnitProvenance::AssumedByPolicy { policy }
                if policy.0 == "models-dev-usd-v1"
        ));
    }

    #[test]
    fn unrecognized_rate_sibling_refuses_with_entry_intact() {
        let entry = priced_rate(9);
        let connection = Arc::new(FakeConnection::new(
            100,
            BTreeMap::from([("rate.input.future_selector".into(), entry.clone())]),
        ));
        let client = CatalogClient::new(connection, 100);
        assert_eq!(
            client.get("provider", "model", 150),
            Err(CatalogClientError::UnrecognizedRateFact {
                fact_key: "rate.input.future_selector".into(),
                entry
            })
        );
    }

    #[test]
    fn rate_keys_are_unconditional_or_one_selector_and_value() {
        let parse = |key: &str| parse_rate_key(key).map(|key| (key.role, key.condition));
        assert_eq!(parse("rate.cache_read"), Ok(("cache_read", None)));
        assert_eq!(
            parse("rate.input.above_context.200000"),
            Ok(("input", Some(RateCondition::AboveContext(200_000))))
        );
        assert_eq!(
            parse("rate.cache_read.mode.fast"),
            Ok(("cache_read", Some(RateCondition::Mode("fast".into()))))
        );
        assert_eq!(
            parse("rate.input.region.eu"),
            Ok((
                "input",
                Some(RateCondition::Unknown {
                    selector: "region".into(),
                    value: "eu".into()
                })
            ))
        );
        for malformed in [
            "rate.input.above_context.200k",
            "rate.input.above_context.",
            "rate.input.future_selector",
            "rate.input.mode.fast.extra",
            "rate.input..fast",
            "rate.tokens",
            "rate.",
            "input",
        ] {
            assert!(parse(malformed).is_err(), "{malformed} parsed");
        }
    }

    #[test]
    fn conditional_rate_siblings_of_any_selector_are_served() {
        let facts = BTreeMap::from([
            ("rate.input".into(), priced_rate(9)),
            ("rate.cache_read.mode.fast".into(), priced_rate(9)),
            ("rate.input.region.eu".into(), priced_rate(9)),
        ]);
        let connection = Arc::new(FakeConnection::new(100, facts.clone()));
        let client = CatalogClient::new(connection, 100);
        let Ok(CatalogLookup::Resolved(snapshot)) = client.get("provider", "model", 150) else {
            panic!("conditional rate keys were refused");
        };
        assert_eq!(snapshot.facts, facts);
        assert_eq!(tier_thresholds(&facts).unwrap(), BTreeSet::new());
    }

    #[test]
    fn only_a_stated_non_standard_mode_selects_a_mode_rate() {
        assert_eq!(stated_mode_condition(None), None);
        assert_eq!(stated_mode_condition(Some("standard")), None);
        assert_eq!(stated_mode_condition(Some("default")), None);
        assert_eq!(stated_mode_condition(Some("fast")), Some("fast"));
        assert_eq!(mode_rate_key("input", "fast"), "rate.input.mode.fast");
    }

    #[test]
    fn tier_keys_are_prefix_matched_and_thresholds_are_data() {
        let facts = BTreeMap::from([
            ("rate.input".into(), priced_rate(9)),
            ("rate.input.above_context.200000".into(), priced_rate(9)),
        ]);
        validate_rate_fact_keys(&facts).unwrap();
        assert_eq!(tier_thresholds(&facts).unwrap(), BTreeSet::from([200_000]));
    }

    #[test]
    fn locked_fusiform_protocol_dependency_set_is_serde_only() {
        let lock = include_str!("../../../Cargo.lock");
        let package = lock
            .split("[[package]]")
            .find(|package| package.contains("name = \"fusiform-protocol\""))
            .expect("fusiform-protocol package in Cargo.lock");
        let dependencies = package
            .split_once("dependencies = [")
            .and_then(|(_, dependencies)| dependencies.split_once(']'))
            .map(|(dependencies, _)| {
                dependencies
                    .lines()
                    .map(str::trim)
                    .filter(|line| line.starts_with('"'))
                    .map(|line| line.trim_matches(&['"', ','][..]))
                    .collect::<Vec<_>>()
            })
            .expect("fusiform-protocol dependencies in Cargo.lock");
        assert_eq!(dependencies, vec!["serde", "serde_json"]);
    }

    #[test]
    fn requests_use_original_instant_and_only_the_rate_prefix() {
        let connection = Arc::new(FakeConnection::new(100, BTreeMap::new()));
        let client = CatalogClient::new(connection.clone(), 100);
        client.get("provider", "model", 777).unwrap();
        let requests = connection.requests.lock().unwrap();
        let request = requests.last().unwrap();
        assert_eq!(request.provider_id.as_deref(), Some("provider"));
        assert_eq!(request.model_id.as_deref(), Some("model"));
        assert_eq!(request.at_ms, Some(777));
        assert_eq!(request.fact_prefixes, Some(vec!["rate.".into()]));
        assert!(request.include_retired);
    }
}
