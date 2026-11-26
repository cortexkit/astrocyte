//! Pool observations value subscription calls against served catalog facts.

use std::collections::{BTreeMap, BTreeSet};

use fusiform_protocol::{
    PlanPricesRequest, PlanPricesResponse, RateValue, RefusalKind,
    UnpricedReason as ProtocolUnpricedReason, NANO_EXPONENT,
};
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension, Row,
    Transaction,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};

pub use crate::catalog_client::BillingRateKey;
use crate::catalog_client::{
    billed_as_target, parse_rate_key, token_rate_key, CatalogCallError, CatalogClient,
    CatalogClientError, CatalogLookup, CatalogModel, CatalogSnapshot, RATE_FACT_PREFIX,
};
use crate::money::{round_nano_num, round_nano_ratio, MoneyError};
use crate::pricing::PromptSize;
use crate::valuation::{event_quantities, EventTokens, MeasuredRole};

mod calibration;
pub(crate) mod clean;
mod measurement;
mod pace;
pub mod pools;
pub mod provider_day;
use measurement::value_event;
pub use pace::{multiplier_pace, MultiplierPaceRequest};

/// Minutes in the 30-day month plan prices are quoted over.
const MINUTES_PER_MONTH: i64 = 43_200;
const MS_PER_MONTH: i64 = MINUTES_PER_MONTH * 60_000;
const NANOS_PER_MS: i64 = 1_000_000;
const HOUR_MS: i64 = 3_600_000;

fn millis_to_nanos(ms: i64) -> i128 {
    i128::from(ms) * i128::from(NANOS_PER_MS)
}

fn clamp_nanos(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
}

/// A multiplier whose integer-percent quantisation error could exceed this
/// share of Δused% is flagged `low_resolution`. Quota readings are integer
/// percent, and integer increments telescope within a window, so the error is
/// about one point per window segment; the handoff's resolution table puts
/// ±5% at a 10-point accumulation.
const LOW_RESOLUTION_ERROR_SHARE: f64 = 0.05;

/// What makes the stored figures read LOW. Each item names the figure it
/// affects: `fee_multiplier` divides by the fee paid, so only token-side
/// effects move it; `multiplier` divides by quota consumed, so quota-side
/// effects move it too.
const ERROR_DEFLATING: &str = "Reads low: tokens left out of api_nanos lower both figures: \
     unpriced tokens, BROCA events priced at base rate because a bundled event can't be tested \
     against a long-context threshold (base_rate_tier_untested_tokens), requests that recorded no \
     mode priced at the standard rate on a model that also serves a fast-mode rate \
     (mode_unrecorded_tokens, per model line), and usage of these \
     accounts from a client astrocyte doesn't read (claude.ai chat, Claude desktop, the ChatGPT \
     apps). That unread usage also raises Δused%, which lowers `multiplier` further.";

fn rate_nanos_exact(units: i64, exponent: u8) -> Option<i64> {
    if exponent <= NANO_EXPONENT {
        units.checked_mul(10_i64.checked_pow(u32::from(NANO_EXPONENT - exponent))?)
    } else {
        let divisor = 10_i64.checked_pow(u32::from(exponent - NANO_EXPONENT))?;
        (units % divisor == 0).then(|| units / divisor)
    }
}

fn amount_to_nanos(minor_units: i64, exponent: i32) -> Option<i64> {
    let exponent = u8::try_from(exponent).ok()?;
    let scale = NANO_EXPONENT.checked_sub(exponent)?;
    minor_units.checked_mul(10_i64.checked_pow(u32::from(scale))?)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum MultiplierError {
    Store(rusqlite::Error),
    /// A catalog call failed for a reason other than "no coverage": the rates
    /// are unknown, so the day is not recorded.
    Catalog(String),
    /// The catalog served a rate key or value this decoder does not
    /// recognize. Astrocyte cannot read what the catalog says, so the day is
    /// not recorded, exactly as when fusiform is unreachable.
    RateFactUnrecognized(String),
    /// Plan prices were needed and could not be read.
    PlanPrices(String),
    /// An exact money or token computation exceeded its representable range;
    /// no partial or capped observation can be recorded for the day.
    ArithmeticOutOfRange,
    /// The request itself is wrong.
    Refused {
        code: &'static str,
        message: String,
    },
    /// A row for this provider and day already exists.
    AlreadyRecorded {
        quota_provider: String,
        day: String,
    },
    /// A recompute found no row for this provider and day to correct. The
    /// daily stage writes a day's first row; a recompute only supersedes one.
    NothingToSupersede {
        quota_provider: String,
        day: String,
    },
    Encode(String),
    /// An observation about to be written breaks a shape every observation
    /// the daily stage or a recompute derives has. Nothing is written; the
    /// text names the broken invariant.
    InvalidObservation(String),
}

impl From<rusqlite::Error> for MultiplierError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

impl From<MoneyError> for MultiplierError {
    fn from(_: MoneyError) -> Self {
        Self::ArithmeticOutOfRange
    }
}

impl MultiplierError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Store(_) | Self::Encode(_) | Self::InvalidObservation(_) => "internal",
            Self::Catalog(_) => "catalog_unavailable",
            Self::RateFactUnrecognized(_) => "rate_fact_unrecognized",
            Self::PlanPrices(_) => "plan_prices_unavailable",
            Self::ArithmeticOutOfRange => "arithmetic_out_of_range",
            Self::Refused { code, .. } => code,
            Self::AlreadyRecorded { .. } => "already_recorded",
            Self::NothingToSupersede { .. } => "nothing_to_supersede",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Store(error) => error.to_string(),
            Self::ArithmeticOutOfRange => {
                "multiplier money or token arithmetic is out of range".into()
            }
            Self::Catalog(message)
            | Self::RateFactUnrecognized(message)
            | Self::PlanPrices(message)
            | Self::Encode(message)
            | Self::InvalidObservation(message) => message.clone(),
            Self::Refused { message, .. } => message.clone(),
            Self::AlreadyRecorded {
                quota_provider,
                day,
            } => {
                format!("an observation for {quota_provider} on {day} is already recorded")
            }
            Self::NothingToSupersede {
                quota_provider,
                day,
            } => format!(
                "no observation for {quota_provider} on {day} is recorded, so there is nothing \
                 to recompute; the daily stage records a day's first observation"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Quota side
// ---------------------------------------------------------------------------

/// Δused% for one account over one interval.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct AccountDelta {
    pub account_ref: String,
    pub delta_percent: f64,
    /// Readings whose `fetched_at` falls inside the interval.
    pub readings: u64,
    /// Percentage drops, retained as a diagnostic; a drop alone is not a reset.
    pub decrease_boundaries: u64,
    /// Legacy diagnostic. Source-time sorting no longer skips late arrivals.
    pub fetched_at_backwards: u64,
    pub stale_reset_readings: u64,
    pub reset_boundaries: u64,
    /// The part of the interval this account's readings cover, `[start, end)`
    /// in milliseconds, or `None` with no reading inside the interval.
    pub coverage_ms: Option<(i64, i64)>,
    #[serde(skip)]
    hours: BTreeMap<i64, clean::CounterHour>,
}

impl AccountDelta {
    /// Confirmed reset periods seen; percentage dips do not start periods.
    fn segments(&self) -> u64 {
        if self.readings == 0 {
            0
        } else {
            self.reset_boundaries + 1
        }
    }
}

/// The SQLite VM instructions `work` runs on `conn`, across every statement,
/// prepared or cached. Tests use it to show that a read's cost follows its
/// answer rather than the stored history.
#[cfg(test)]
pub(crate) fn vm_steps<T>(conn: &Connection, work: impl FnOnce() -> T) -> (T, u64) {
    unsafe extern "C" fn count(counter: *mut std::ffi::c_void) -> std::ffi::c_int {
        // SAFETY: the pointer is the boxed counter below, which outlives the
        // handler: the handler is removed before the box is dropped.
        unsafe { *counter.cast::<u64>() += 1 };
        0
    }
    let mut steps = Box::new(0_u64);
    // SAFETY: `conn` is a live connection and the counter stays allocated
    // until the handler is cleared below.
    unsafe {
        rusqlite::ffi::sqlite3_progress_handler(
            conn.handle(),
            1,
            Some(count),
            std::ptr::from_mut(steps.as_mut()).cast(),
        );
    }
    let result = work();
    // SAFETY: clearing the handler on the same live connection.
    unsafe {
        rusqlite::ffi::sqlite3_progress_handler(conn.handle(), 0, None, std::ptr::null_mut());
    }
    (result, *steps)
}

/// The reason a rate fact could not be decoded. It never reaches a written
/// row: [`resolve_rates`] fails the day on it.
const RATE_FACT_UNRECOGNIZED: &str = "rate_fact_unrecognized";

/// A served rate value this decoder cannot read. Like an unrecognized key,
/// it never reaches a written row: [`resolve_rates`] fails the day on it.
const RATE_UNDECODABLE: &str = "rate_undecodable";

/// One role's rate, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum RateState {
    /// Nano-USD per million tokens.
    Priced {
        units_per_million: i64,
    },
    Unpriced {
        reason: String,
    },
    /// A supported cross-role billing rule, not a price.
    BilledAs {
        key: BillingRateKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ModelRates {
    /// `rate.<role>`, keyed by role.
    pub base: BTreeMap<String, RateState>,
    /// `rate.<role>.above_context.<threshold>`, keyed by threshold then role.
    #[serde(deserialize_with = "context_tiers")]
    pub tiers: BTreeMap<u64, BTreeMap<String, RateState>>,
    /// `rate.<role>.mode.<mode>`, keyed by mode then role. Omitted from a
    /// stored row when empty, so a model without mode rates serializes as
    /// it did before mode rates were read.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub modes: BTreeMap<String, BTreeMap<String, RateState>>,
    /// Complete token condition sets, keyed by the selector-sorted rate key.
    /// Empty in observations recorded before lifetime rates were supported.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub conditional: BTreeMap<String, RateState>,
    /// The target catalog model whose historical rates are used to value this alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_model: Option<CatalogModel>,
}

impl ModelRates {
    pub(crate) fn context_tier_for_role(
        &self,
        role: &str,
        prompt: PromptSize,
        mode: Option<&str>,
    ) -> Option<u64> {
        self.tiers
            .iter()
            .filter_map(|(threshold, rates)| rates.contains_key(role).then_some((true, *threshold)))
            .chain(self.conditional.keys().filter_map(|key| {
                let parsed = parse_rate_key(key).ok()?;
                if parsed.role != role {
                    return None;
                }
                let matching_mode = parsed
                    .selectors
                    .get("mode")
                    .is_none_or(|required| Some(required.as_str()) == mode);
                let threshold = parsed.selectors.get("above_context")?.parse::<u64>().ok()?;
                Some((matching_mode, threshold))
            }))
            .filter(|(_, threshold)| prompt.exceeds(*threshold))
            // Prefer a threshold for the requested mode. When only another
            // mode has a crossed tier, keep its threshold so a missing exact
            // mode or lifetime rate cannot fall back to a less specific one.
            .max()
            .map(|(_, threshold)| threshold)
    }
}

// JSON object keys are strings, including inside the tagged rate resolution.
// Decode them explicitly so stored long-context thresholds remain numeric.
fn context_tiers<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<u64, BTreeMap<String, RateState>>, D::Error> {
    BTreeMap::<String, BTreeMap<String, RateState>>::deserialize(deserializer)?
        .into_iter()
        .map(|(key, rates)| {
            key.parse()
                .map(|key| (key, rates))
                .map_err(de::Error::custom)
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ModelResolution {
    Rates(ModelRates),
    /// The whole model prices nowhere; its tokens are counted by this reason.
    Unpriceable {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolvedRates {
    /// The catalog instant every rate was resolved at.
    pub resolved_at_ms: i64,
    /// Keyed by catalog `(provider, model)`.
    pub models: BTreeMap<(String, String), ModelResolution>,
    /// Dated resolutions for cache-backed observations; legacy inputs leave this empty.
    pub eras: BTreeMap<(String, String), Vec<crate::rate_era::ResolvedEra>>,
}

/// Borrowed index for repeated lookups without allocating a tuple of Strings.
struct ModelLookup<'a> {
    providers: BTreeMap<&'a str, BTreeMap<&'a str, &'a ModelResolution>>,
    eras: BTreeMap<&'a str, BTreeMap<&'a str, &'a [crate::rate_era::ResolvedEra]>>,
}

impl<'a> ModelLookup<'a> {
    fn new(rates: &'a ResolvedRates) -> Self {
        let mut providers: BTreeMap<&str, BTreeMap<&str, &ModelResolution>> = BTreeMap::new();
        for ((provider, model), resolution) in &rates.models {
            providers
                .entry(provider)
                .or_default()
                .insert(model, resolution);
        }
        let mut eras: BTreeMap<&str, BTreeMap<&str, &[crate::rate_era::ResolvedEra]>> =
            BTreeMap::new();
        for ((provider, model), timeline) in &rates.eras {
            eras.entry(provider).or_default().insert(model, timeline);
        }
        Self { providers, eras }
    }

    fn get(&self, provider: &str, model: &str) -> Option<&'a ModelResolution> {
        self.providers.get(provider)?.get(model).copied()
    }

    fn get_at(&self, provider: &str, model: &str, at_ms: i64) -> Option<&'a ModelResolution> {
        match self.eras.get(provider).and_then(|models| models.get(model)) {
            Some(eras) => eras
                .iter()
                .find(|era| era.from_ms <= at_ms && era.until_ms.is_none_or(|until| at_ms < until))
                .map(|era| &era.resolution),
            None => self.get(provider, model),
        }
    }
}

/// Rates for every model, resolved once per model at one instant. A model the
/// catalog has no coverage for is recorded as unpriceable; any other failure
/// leaves the rates unknown and fails the day.
///
/// The two outcomes that look alike are treated differently on purpose.
/// `no_catalog_coverage` is something the catalog SAYS: it has no row for the
/// model. That is a fact about the model, so its tokens are counted as
/// unpriced on a row that is still written. `rate_fact_unrecognized` means
/// astrocyte cannot READ what the catalog says: a rate key or value outside
/// the shapes this decoder knows. Counting those tokens as unpriced would
/// record astrocyte's own decoding gap as if it were a property of the model
/// (a new key shape once made most of a day's tokens unpriced and wrote a
/// multiplier near zero), so the whole day fails like a fusiform outage and
/// is retried until the catalog reads cleanly.
pub fn resolve_rates(
    catalog: &CatalogClient,
    models: &BTreeSet<(String, String)>,
    at_ms: i64,
) -> Result<ResolvedRates, MultiplierError> {
    resolve_rate_inputs(catalog, models, at_ms, None)
}

fn reasoning_base_model(model: &str) -> Option<&str> {
    model
        .strip_suffix("-low")
        .or_else(|| model.strip_suffix("-high"))
}

fn resolve_rate_inputs(
    catalog: &CatalogClient,
    models: &BTreeSet<(String, String)>,
    at_ms: i64,
    mut identities: Option<&mut pools::PoolModelIdentities>,
) -> Result<ResolvedRates, MultiplierError> {
    let mut resolved = ResolvedRates {
        resolved_at_ms: at_ms,
        models: BTreeMap::new(),
        eras: BTreeMap::new(),
    };
    for (provider, model) in models {
        // Reasoning levels on the wire are not catalog identities. Prefer an
        // exact row, and only retry a missing row for the two supported levels.
        let lookup = catalog.get(provider, model, at_ms);
        let (lookup_model, mut lookup) =
            match (lookup_missing(&lookup), reasoning_base_model(model)) {
                (true, Some(base)) => (base, catalog.get(provider, base, at_ms)),
                _ => (model.as_str(), lookup),
            };
        let mut reference_model = None;
        let discover_quota_proxy = lookup_without_prices(&lookup);
        if let Some(identities) = identities.as_deref_mut() {
            match &lookup {
                Ok(CatalogLookup::Resolved(_)) if !lookup_missing(&lookup) => {
                    identities.record(
                        provider,
                        model,
                        provider,
                        lookup_model,
                        lookup_model != model,
                    );
                }
                // Before the catalog's history boundary there is no dated
                // identity to inspect. A current identity can prove pool
                // membership, but current rates cannot price these tokens.
                Ok(CatalogLookup::PreHistory { .. }) => {
                    let current = catalog.get_current(provider, model);
                    let exact = matches!(&current, Ok(CatalogLookup::Resolved(snapshot))
                        if snapshot.alias_target.is_none() && !lookup_missing(&current));
                    let (name, current) = match (exact, reasoning_base_model(model)) {
                        (false, Some(base)) if current.is_ok() || lookup_missing(&current) => {
                            (base, catalog.get_current(provider, base))
                        }
                        _ => (model.as_str(), current),
                    };
                    record_current_pool_identity(identities, provider, model, name, current)?;
                }
                _ => {}
            }
        }
        if lookup_missing(&lookup) {
            // Aliases exist only on current reads. Discover the public identity
            // there, but use its rates at the original valuation instant.
            match catalog.get_current(provider, lookup_model) {
                Ok(CatalogLookup::Resolved(snapshot)) => {
                    if let Some(target) = snapshot.alias_target {
                        if let Some(identities) = identities.as_deref_mut() {
                            identities.record(
                                provider,
                                model,
                                &target.provider_id,
                                &target.model_id,
                                true,
                            );
                        }
                        lookup = catalog.get(&target.provider_id, &target.model_id, at_ms);
                        reference_model = Some(target);
                    }
                }
                Err(CatalogClientError::Call(error))
                    if RefusalKind::of(&error.code) == RefusalKind::OutsideCoverage => {}
                Err(error) => {
                    return Err(MultiplierError::Catalog(format!(
                        "catalog.get current {provider}/{lookup_model}: {error:?}"
                    )))
                }
                _ => {}
            }
        }
        if discover_quota_proxy && reference_model.is_none() {
            // An auth-specific lookup is current-only and identifies the public
            // API model; fetch its rates at the original historical instant.
            match catalog.get_current_apikey(provider, lookup_model) {
                Ok(CatalogLookup::Resolved(snapshot)) => {
                    if let Some(target) = quota_proxy_target(&snapshot) {
                        lookup = catalog.get(&target.provider_id, &target.model_id, at_ms);
                        reference_model = Some(target);
                    }
                }
                Err(CatalogClientError::Call(error))
                    if RefusalKind::of(&error.code) == RefusalKind::OutsideCoverage => {}
                Err(error) => {
                    return Err(MultiplierError::Catalog(format!(
                        "catalog.get current apikey {provider}/{lookup_model}: {error:?}"
                    )))
                }
                _ => {}
            }
        }
        let resolution = match lookup {
            Ok(CatalogLookup::Resolved(snapshot)) => match rates_from_snapshot(&snapshot) {
                ModelResolution::Unpriceable { reason } if reason == RATE_FACT_UNRECOGNIZED => {
                    return Err(MultiplierError::RateFactUnrecognized(format!(
                        "catalog.get {provider}/{model}: a rate key outside the shapes this \
                         decoder reads"
                    )))
                }
                ModelResolution::Unpriceable { reason } if reason == RATE_UNDECODABLE => {
                    return Err(MultiplierError::RateFactUnrecognized(format!(
                        "catalog.get {provider}/{model}: a rate value this decoder cannot read"
                    )))
                }
                ModelResolution::Rates(mut rates) => {
                    rates.reference_model = reference_model;
                    ModelResolution::Rates(rates)
                }
                resolution => resolution,
            },
            Ok(CatalogLookup::PreHistory { .. }) => ModelResolution::Unpriceable {
                reason: "catalog_pre_history".into(),
            },
            // A historical miss the catalog answered as outside coverage stays
            // a written unpriceable reason, whether it arrived as the new
            // definite outcome or, for a current read, as the refused call it
            // always was.
            Ok(CatalogLookup::NoCoverage) => ModelResolution::Unpriceable {
                reason: "no_catalog_coverage".into(),
            },
            Err(CatalogClientError::Call(error))
                if RefusalKind::of(&error.code) == RefusalKind::OutsideCoverage =>
            {
                ModelResolution::Unpriceable {
                    reason: "no_catalog_coverage".into(),
                }
            }
            Err(CatalogClientError::UnrecognizedRateFact { fact_key, .. }) => {
                return Err(MultiplierError::RateFactUnrecognized(format!(
                    "catalog.get {provider}/{model}: unrecognized rate key {fact_key:?}"
                )))
            }
            Err(error) => {
                return Err(MultiplierError::Catalog(format!(
                    "catalog.get {provider}/{model}: {error:?}"
                )))
            }
        };
        resolved
            .models
            .insert((provider.clone(), model.clone()), resolution);
    }
    Ok(resolved)
}

fn record_current_pool_identity(
    identities: &mut pools::PoolModelIdentities,
    provider: &str,
    model: &str,
    looked_up: &str,
    current: Result<CatalogLookup, CatalogClientError>,
) -> Result<(), MultiplierError> {
    if let Ok(CatalogLookup::Resolved(snapshot)) = &current {
        if let Some(target) = &snapshot.alias_target {
            identities.record(provider, model, &target.provider_id, &target.model_id, true);
        } else if !lookup_missing(&current) {
            identities.record(provider, model, provider, looked_up, model != looked_up);
        }
    } else if let Err(error) = &current {
        if !lookup_missing(&current) {
            return Err(MultiplierError::Catalog(format!(
                "catalog.get current {provider}/{looked_up}: {error:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn lookup_without_prices(lookup: &Result<CatalogLookup, CatalogClientError>) -> bool {
    let Ok(CatalogLookup::Resolved(snapshot)) = lookup else {
        return false;
    };
    // Any priced fact, even priced zero, keeps its existing historical path.
    // Undecodable facts must still fail the day rather than be hidden by a proxy.
    // A row with no rate facts at all is a missing model, not a plan's card: it
    // keeps the alias path alone, so a provider without an apikey plane never
    // gets a lookup whose refusal could fail the whole day.
    let mut rates = snapshot
        .facts
        .iter()
        .filter(|(key, _)| key.starts_with(RATE_FACT_PREFIX))
        .peekable();
    rates.peek().is_some()
        && rates.all(|(fact_key, value)| {
            match serde_json::from_value::<RateValue>(value.clone()) {
                // A billing rule is not a price and does not preclude finding
                // the dated public API card for this quota proxy.
                Ok(RateValue::BilledAs { key }) => billed_as_target(fact_key, &key).is_some(),
                Ok(RateValue::StatedZero { .. })
                | Ok(RateValue::Unpriced {
                    reason:
                        ProtocolUnpricedReason::MissingRate
                        | ProtocolUnpricedReason::NoCatalogCoverage
                        | ProtocolUnpricedReason::UnknownChargeBasis,
                    ..
                }) => true,
                _ => false,
            }
        })
}

// Keep quota proxies beside, not inside, alias discovery: an alias is a model
// identity, while a quota proxy is only a list-price reference for valuation.
// Ledger pricing must not treat a plan's reference prices as amounts billed.
pub(crate) fn quota_proxy_target(snapshot: &CatalogSnapshot) -> Option<CatalogModel> {
    snapshot.facts.iter().find_map(|(key, value)| {
        if !key.starts_with(RATE_FACT_PREFIX) {
            return None;
        }
        let origin = match serde_json::from_value::<RateValue>(value.clone()).ok()? {
            RateValue::Priced { inherited_from, .. }
            | RateValue::StatedZero { inherited_from }
            | RateValue::Unpriced { inherited_from, .. } => inherited_from?,
            // The public API reference comes from an inherited rate, not a
            // rule for choosing between the model's own rate keys.
            RateValue::BilledAs { .. } => return None,
        };
        (origin.basis == "quota_proxy").then_some(CatalogModel {
            provider_id: origin.provider_id,
            model_id: origin.model_id?,
        })
    })
}

pub(crate) fn lookup_missing(lookup: &Result<CatalogLookup, CatalogClientError>) -> bool {
    match lookup {
        Ok(CatalogLookup::Resolved(snapshot)) => {
            snapshot.facts.is_empty()
                && snapshot.withheld_facts.is_empty()
                && snapshot.uncertain_facts.is_empty()
        }
        // A historical refusal classified as outside coverage arrives as a
        // definite no-coverage outcome now; the error arm stays for a current
        // read, which keeps its refusal as an error.
        Ok(CatalogLookup::NoCoverage) => true,
        Err(CatalogClientError::Call(error)) => {
            RefusalKind::of(&error.code) == RefusalKind::OutsideCoverage
        }
        _ => false,
    }
}

/// Every `rate.*` fact the catalog served for one model, decoded to a rate or
/// to the reason it cannot price.
pub(crate) fn rates_from_snapshot(snapshot: &CatalogSnapshot) -> ModelResolution {
    let keys: BTreeSet<&String> = snapshot
        .facts
        .keys()
        .chain(snapshot.withheld_facts.keys())
        .filter(|key| key.starts_with(RATE_FACT_PREFIX))
        .collect();
    if keys.is_empty() {
        return ModelResolution::Unpriceable {
            reason: "no_catalog_coverage".into(),
        };
    }
    let reasoning_rule = snapshot.has_base_billing_rule("reasoning");
    let cache_write_rule = snapshot.has_base_billing_rule("cache_write");
    let mut rates = ModelRates::default();
    for key in keys {
        let Ok(parsed) = parse_rate_key(key) else {
            return ModelResolution::Unpriceable {
                reason: RATE_FACT_UNRECOGNIZED.into(),
            };
        };
        // Subscription reference value counts only tokens. Duration, image
        // and request prices must neither replace token rates nor fail a day.
        if parsed.unit.is_some() {
            continue;
        }
        let state = rate_state(snapshot, key);
        // Checked on every key, including conditions that never apply: a
        // value astrocyte cannot read means it cannot read what the catalog
        // says about this model.
        if matches!(&state, RateState::Unpriced { reason } if reason == RATE_UNDECODABLE) {
            return ModelResolution::Unpriceable {
                reason: RATE_UNDECODABLE.into(),
            };
        }
        let role = parsed.role.to_string();
        let selectors = &parsed.selectors;
        // An explicit missing rate does not override a base billing rule.
        // Lifetime-tagged writes never use the base rule.
        if ((reasoning_rule && role == "reasoning")
            || (cache_write_rule && role == "cache_write" && !selectors.contains_key("ttl")))
            && !selectors.is_empty()
            && matches!(&state, RateState::Unpriced { reason } if reason == "no_rate")
        {
            continue;
        }
        // Unknown selectors have no measurement producer and never apply.
        if !parsed.has_only_known_token_selectors() {
            continue;
        }
        if let Some(threshold) = selectors.get("above_context") {
            // Parsing already checked the threshold; retain it even when the
            // only tier price is for a particular lifetime or mode.
            if let Ok(threshold) = threshold.parse::<u64>() {
                rates.tiers.entry(threshold).or_default();
            }
        }
        if let Some(mode) = selectors.get("mode") {
            rates.modes.entry(mode.clone()).or_default();
        }
        match selectors.len() {
            0 => {
                rates.base.insert(role, state);
            }
            1 if selectors.contains_key("above_context") => {
                let threshold = selectors["above_context"]
                    .parse::<u64>()
                    .expect("validated threshold");
                rates
                    .tiers
                    .entry(threshold)
                    .or_default()
                    .insert(role, state);
            }
            1 if selectors.contains_key("mode") => {
                rates
                    .modes
                    .entry(selectors["mode"].clone())
                    .or_default()
                    .insert(role, state);
            }
            _ => {
                let key = token_rate_key(parsed.role, selectors);
                if rates.conditional.insert(key.clone(), state).is_some() {
                    rates.conditional.insert(
                        key,
                        RateState::Unpriced {
                            reason: "no_rate".into(),
                        },
                    );
                }
            }
        }
    }
    ModelResolution::Rates(rates)
}

fn rate_state(snapshot: &CatalogSnapshot, key: &str) -> RateState {
    let unpriced = |reason: &str| RateState::Unpriced {
        reason: reason.to_string(),
    };
    if snapshot.withheld_facts.contains_key(key) {
        return unpriced("rate_withheld");
    }
    let Some(entry) = snapshot.facts.get(key) else {
        return unpriced("no_rate");
    };
    if entry
        .get("currency")
        .and_then(Value::as_str)
        .is_some_and(|currency| currency != "USD")
    {
        return unpriced("non_usd");
    }
    if snapshot.uncertain_facts.contains_key(key) {
        return unpriced("rate_uncertain");
    }
    match serde_json::from_value::<RateValue>(entry.clone()) {
        Err(_) => unpriced(RATE_UNDECODABLE),
        Ok(RateValue::Priced { per: Some(_), .. }) => unpriced(RATE_UNDECODABLE),
        Ok(RateValue::BilledAs { key: target }) => billed_as_target(key, &target).map_or_else(
            || unpriced(RATE_UNDECODABLE),
            |key| RateState::BilledAs { key },
        ),
        Ok(RateValue::Priced {
            inherited_from: Some(_),
            ..
        }) => unpriced("rate_inherited"),
        // A rate may state any exponent; the multiplier keeps whole nano-USD per
        // million tokens, so a rate finer than that cannot be held exactly.
        Ok(RateValue::Priced { amount, .. }) => {
            match rate_nanos_exact(amount.units, amount.exponent) {
                Some(units_per_million) => RateState::Priced { units_per_million },
                None => unpriced("rate_exponent_unsupported"),
            }
        }
        // A zero marked `inherited_from` is another catalog row's statement
        // that the model is free, not this provider's, so it gets the same
        // reason as an inherited price rather than the own-zero reason.
        Ok(RateValue::StatedZero {
            inherited_from: Some(_),
        }) => unpriced("rate_inherited"),
        Ok(RateValue::StatedZero {
            inherited_from: None,
        }) => unpriced("stated_zero_requires_policy"),
        Ok(RateValue::Unpriced {
            reason,
            inherited_from: _,
        }) => unpriced(match reason {
            ProtocolUnpricedReason::MissingRate => "no_rate",
            // The catalog has no public API price for the model behind this
            // plan, so its subscription tokens cannot be valued: the same
            // outcome as missing catalog coverage.
            ProtocolUnpricedReason::NoCatalogCoverage | ProtocolUnpricedReason::NoApiCard => {
                "no_catalog_coverage"
            }
            ProtocolUnpricedReason::UnknownChargeBasis => "catalog_unknown_charge_basis",
            ProtocolUnpricedReason::NotEstablished => RATE_UNDECODABLE,
        }),
    }
}

/// The transport for fusiform's `plan.prices`.
pub trait PlanPriceSource: Send + Sync {
    fn plan_prices(
        &self,
        request: PlanPricesRequest,
    ) -> Result<PlanPricesResponse, CatalogCallError>;
}

/// The monthly price one account is valued at, with where it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPriceUsed {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub account_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    pub plan: String,
    /// `account_override`, `plan_override`, or `fusiform`; `null` when unpriced.
    pub source: Option<String>,
    pub monthly_nanos: Option<i64>,
    pub review_by_ms: Option<i64>,
    /// A fusiform price used past its review date.
    pub plan_price_overdue: bool,
    pub source_ref: Option<String>,
    /// Why the account cannot be priced; it is then left out of the
    /// denominator, never valued at a guess.
    pub unpriceable_reason: Option<String>,
    pub refusal_text: Option<String>,
}

fn fusiform_price(served: &PlanPricesResponse, plan: &str, at_ms: i64, used: &mut PlanPriceUsed) {
    let (provider_id, tier) = plan.split_once('/').unwrap_or((plan, ""));
    // Several eras may be served for one key; the one in force is the latest
    // whose boundary is not in the future. Tiers are matched exactly: a
    // similar string may name a repriced plan.
    let row = served
        .prices
        .iter()
        .filter(|row| row.provider_id == provider_id && row.tier == tier)
        .filter(|row| row.boundary_at_ms <= at_ms)
        .max_by_key(|row| row.boundary_at_ms);
    let Some(row) = row else {
        used.unpriceable_reason = Some("plan_price_missing".into());
        return;
    };
    used.review_by_ms = Some(row.review_by_ms);
    used.source_ref = Some(row.source_ref.clone());
    if let Some(refusal) = &row.refusal_reason {
        used.unpriceable_reason = Some("plan_price_refused".into());
        used.refusal_text = Some(refusal.clone());
        return;
    }
    let Some(price) = &row.price else {
        used.unpriceable_reason = Some("plan_price_missing".into());
        return;
    };
    if price.currency != "USD" || price.period != "month" {
        used.unpriceable_reason = Some("plan_price_not_usd_per_month".into());
        return;
    }
    match amount_to_nanos(price.minor_units, price.exponent) {
        Some(nanos) if nanos > 0 => {
            used.source = Some("fusiform".into());
            used.monthly_nanos = Some(nanos);
            used.plan_price_overdue = row.review_by_ms < at_ms;
        }
        _ => used.unpriceable_reason = Some("plan_price_out_of_range".into()),
    }
}

// ---------------------------------------------------------------------------
// Token side (second read) and the observation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct RoleTokens {
    pub input: i64,
    pub cache_read: i64,
    pub cache_write: i64,
    pub output: i64,
    pub reasoning: i64,
}

impl RoleTokens {
    pub(crate) fn add(&mut self, role: &str, quantity: i64) -> Result<(), MoneyError> {
        let slot = match role {
            "input" => &mut self.input,
            "cache_read" => &mut self.cache_read,
            "cache_write" => &mut self.cache_write,
            "output" => &mut self.output,
            "reasoning" => &mut self.reasoning,
            _ => return Ok(()),
        };
        *slot = slot.checked_add(quantity).ok_or(MoneyError::OutOfRange)?;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<(), MoneyError> {
        self.add("input", other.input)?;
        self.add("cache_read", other.cache_read)?;
        self.add("cache_write", other.cache_write)?;
        self.add("output", other.output)?;
        self.add("reasoning", other.reasoning)
    }
}

/// One `(producer, provider, model)` line of the API-dollar split. The
/// multiplier depends on model mix, so a mix change must be visible as one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ModelLine {
    pub producer: String,
    pub provider: String,
    pub model: Option<String>,
    pub events: u64,
    /// As measured, before inclusive output is made disjoint.
    pub measured_tokens: RoleTokens,
    /// Reasoning subtracted from inclusive BROCA output.
    pub inclusive_output_adjustment: i64,
    /// All tokens in per-request events whose prompt crossed any model tier
    /// threshold, including roles priced at base or left unpriced.
    pub tiered_tokens: i64,
    /// Tokens of a bundling producer on a model that publishes tiers, priced at
    /// the base rate because no per-request prompt exists to test.
    pub base_rate_tier_untested_tokens: i64,
    /// Tokens of requests whose record states no mode, on a model that also
    /// serves a mode-conditional rate (e.g. `rate.input.mode.fast`). They are
    /// priced at the standard rate: a premium mode costs extra only when a
    /// client asks for it, and the request says nothing either way.
    pub mode_unrecorded_tokens: i64,
    pub api_nanos: i64,
    /// Rates the line was valued at.
    pub rates: Option<ModelResolution>,
    #[serde(skip)]
    numerator: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnpricedTokens {
    pub reason: String,
    pub model: String,
    pub role: String,
    pub tokens: i64,
    pub measurements: u64,
}

/// Coverage recorded by a pool observation. Older plan-wide observations
/// carry no scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolScope {
    All,
    Subset,
    Unknown,
}

impl PoolScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Subset => "subset",
            Self::Unknown => "unknown",
        }
    }
}

/// One row of `multiplier_observation`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Observation {
    pub plan: Option<String>,
    pub routing_eligible: bool,
    pub eligibility_reasons: Vec<String>,
    pub clean_hours_share: f64,
    pub saturated_share: f64,
    pub gap_share: f64,
    pub unpriced_share: f64,
    pub unconfigured_accounts: Vec<String>,
    pub source_backlog: Vec<String>,
    pub unattributed_event_share: f64,
    pub zero_measurement_events: u64,
    pub missing_quantity_events: u64,
    pub quota_provider: String,
    pub observation_day: String,
    pub snapshot_at_ms: i64,
    pub interval_start_ms: i64,
    pub interval_end_ms: i64,
    pub token_interval_start_ms: Option<i64>,
    pub token_interval_end_ms: Option<i64>,
    pub window_pool_key: String,
    pub window_minutes: i64,
    pub pool_scope: Option<PoolScope>,
    pub pool_models: Option<Vec<String>>,
    pub pool_models_at_ms: Option<i64>,
    pub pool_member_models: Option<Vec<String>>,
    pub catalog_provider: Option<String>,
    /// Input, cache-read, cache-write, output and reasoning counts from plain
    /// and lifetime-specific token units with exponent 0, priced or not,
    /// inside the token interval.
    /// Output excludes reasoning when the stored output includes it.
    /// Unsupported units are listed in `unpriced_tokens`.
    pub tokens: RoleTokens,
    pub api_nanos: i64,
    pub api_nanos_by_model: Vec<ModelLine>,
    pub unpriced_tokens: Vec<UnpricedTokens>,
    /// In-scope tokens outside the span of quota readings: no measured quota
    /// percentage change covers them, so they are reported, never divided.
    /// Uses the same units and disjoint-role counting as `tokens`.
    pub tokens_outside_quota_coverage: i64,
    pub base_rate_tier_untested_tokens: i64,
    pub subscription_nanos: i64,
    pub fee_paid_nanos: i64,
    pub delta_percent: f64,
    pub accounts_included: Vec<Value>,
    pub accounts_excluded: Vec<Value>,
    /// Counts by reason for a measured counter, or an empty array when the
    /// counter cannot be measured.
    pub excluded_pairs: Value,
    pub low_resolution: bool,
    pub scope_rules: Vec<Value>,
    pub plan_prices: Vec<PlanPriceUsed>,
    pub catalog_resolved_at_ms: i64,
    pub api_twins: BTreeMap<String, String>,
    pub reasoning_billed_as_output: Vec<String>,
    pub error_deflating: String,
    pub error_inflating: String,
    pub multiplier: Option<f64>,
    pub fee_multiplier: Option<f64>,
}

/// 00:00Z of a `YYYY-MM-DD` day, in ms: the end of the interval the daily
/// stage observes for that day. `None` unless `day` is exactly that form.
pub fn utc_day_start_ms(day: &str) -> Option<i64> {
    let date = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()?;
    // chrono accepts unpadded fields; only the canonical spelling names a
    // stored observation_day.
    (date.format("%Y-%m-%d").to_string() == day)
        .then(|| date.and_hms_opt(0, 0, 0))
        .flatten()
        .map(|midnight| midnight.and_utc().timestamp_millis())
}

/// The UTC calendar day of an instant, `YYYY-MM-DD`. Refused as
/// `invalid_range` when the instant is outside the calendar chrono can
/// represent (roughly years -262,143 to 262,142), rather than standing in a
/// placeholder that could be stored or compared as if it were a day.
pub fn utc_day(ms: i64) -> Result<String, MultiplierError> {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|instant| instant.format("%Y-%m-%d").to_string())
        .ok_or_else(|| MultiplierError::Refused {
            code: "invalid_range",
            message: format!("instant {ms} ms is outside the representable UTC calendar"),
        })
}

#[derive(Debug, Clone, PartialEq)]
struct ScopedEvent {
    id: i64,
    tokens: EventTokens,
    occurred_at_ms: i64,
    work_class: Option<String>,
    account_ref: Option<String>,
}

fn decode_role(row: &Row<'_>) -> rusqlite::Result<MeasuredRole> {
    Ok((
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
    ))
}

/// Stream each event once with all its measurements. Time bounds and scope
/// are shared by valuation, outside-coverage counts and pace; pace alone uses
/// the event's occurrence time and work class to place it in an hourly bucket.
fn for_events(
    conn: &Connection,
    predicate: &str,
    values: &[SqlValue],
    mut consume: impl FnMut(ScopedEvent, Option<String>) -> Result<(), MultiplierError>,
) -> Result<(), MultiplierError> {
    let sql = format!(
        "SELECT e.event_key, e.producer, COALESCE(e.provider,''), e.model, m.role, m.unit, m.source,
                m.quantity_units,m.quantity_exponent,e.occurred_at_ms,e.work_class,e.account_ref,
                b.event_key IS NOT NULL,b.mode,COALESCE(e.resolved_credential,e.auth_selection)
         FROM usage_event e LEFT JOIN measurement m ON m.event_key=e.event_key
         LEFT JOIN broca_attempt_detail b ON b.event_key=e.event_key
         WHERE {predicate} AND e.occurred_at_ms >= CAST(? AS INTEGER)
           AND e.occurred_at_ms < CAST(? AS INTEGER) ORDER BY e.event_key"
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(values.iter()))?;
    let mut current: Option<(ScopedEvent, Option<String>)> = None;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        if current.as_ref().is_none_or(|(event, _)| event.id != id) {
            if let Some((event, credential)) = current.take() {
                consume(event, credential)?;
            }
            let event = ScopedEvent {
                id,
                tokens: EventTokens {
                    producer: row.get(1)?,
                    provider: row.get(2)?,
                    model: row.get(3)?,
                    mode: row.get(13)?,
                    broca_attempt: row.get(12)?,
                    roles: Vec::new(),
                },
                occurred_at_ms: row.get(9)?,
                work_class: row.get(10)?,
                account_ref: row.get(11)?,
            };
            current = Some((event, row.get(14)?));
        }
        if let Some((event, _)) = current.as_mut() {
            if row.get::<_, Option<String>>(4)?.is_some() {
                event.tokens.roles.push(decode_role(row)?);
            }
        }
    }
    if let Some((event, credential)) = current {
        consume(event, credential)?;
    }
    Ok(())
}

#[derive(Default)]
struct ExcludedPairs {
    decrease_boundary: u64,
    fetched_at_backwards: u64,
}

impl ExcludedPairs {
    fn counts(self) -> BTreeMap<String, u64> {
        BTreeMap::from([
            ("decrease_boundary".into(), self.decrease_boundary),
            ("fetched_at_backwards".into(), self.fetched_at_backwards),
        ])
    }
}

#[derive(Clone, Copy)]
struct NanoRatio {
    numerator: i128,
    denominator: i128,
}

fn gcd(mut a: i128, mut b: i128) -> i128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

impl NanoRatio {
    fn new(numerator: i128, denominator: i128) -> Result<Self, MoneyError> {
        if denominator <= 0 {
            return Err(MoneyError::OutOfRange);
        }
        let divisor = gcd(
            numerator.checked_abs().ok_or(MoneyError::OutOfRange)?,
            denominator,
        );
        Ok(Self {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    // The accumulated quota percentage is an f64. Use its round-trip decimal
    // representation as the exact input to pro-rata money calculations, rather
    // than multiplying nanodollars in floating point.
    fn percent(value: f64) -> Result<Self, MoneyError> {
        if !value.is_finite() || value < 0.0 {
            return Err(MoneyError::OutOfRange);
        }
        let decimal = value.to_string();
        let (whole, fraction) = decimal.split_once('.').unwrap_or((&decimal, ""));
        let denominator = 10_i128
            .checked_pow(u32::try_from(fraction.len()).map_err(|_| MoneyError::OutOfRange)?)
            .ok_or(MoneyError::OutOfRange)?;
        let numerator = format!("{whole}{fraction}")
            .parse()
            .map_err(|_| MoneyError::OutOfRange)?;
        Self::new(numerator, denominator)
    }

    fn mul(self, other: Self) -> Result<Self, MoneyError> {
        let a = gcd(
            self.numerator.checked_abs().ok_or(MoneyError::OutOfRange)?,
            other.denominator,
        );
        let b = gcd(
            other
                .numerator
                .checked_abs()
                .ok_or(MoneyError::OutOfRange)?,
            self.denominator,
        );
        Self::new(
            (self.numerator / a)
                .checked_mul(other.numerator / b)
                .ok_or(MoneyError::OutOfRange)?,
            (self.denominator / b)
                .checked_mul(other.denominator / a)
                .ok_or(MoneyError::OutOfRange)?,
        )
    }

    fn add(self, other: Self) -> Result<Self, MoneyError> {
        let divisor = gcd(self.denominator, other.denominator);
        let numerator = self
            .numerator
            .checked_mul(other.denominator / divisor)
            .and_then(|a| {
                other
                    .numerator
                    .checked_mul(self.denominator / divisor)
                    .and_then(|b| a.checked_add(b))
            })
            .ok_or(MoneyError::OutOfRange)?;
        Self::new(
            numerator,
            self.denominator
                .checked_mul(other.denominator / divisor)
                .ok_or(MoneyError::OutOfRange)?,
        )
    }

    fn round(self) -> Result<i64, MoneyError> {
        round_nano_ratio(self.numerator, self.denominator)
    }
}

fn allocate_subscription(
    shares: &[(String, NanoRatio)],
    total: i64,
) -> Result<Vec<i64>, MoneyError> {
    let mut common = 1_i128;
    for (_, share) in shares {
        common = common
            .checked_mul(share.denominator / gcd(common, share.denominator))
            .ok_or(MoneyError::OutOfRange)?;
    }
    let mut parts = Vec::new();
    let mut remainders = Vec::new();
    let mut floor_sum = 0_i128;
    for (index, (account, share)) in shares.iter().enumerate() {
        let floor = share.numerator.div_euclid(share.denominator);
        floor_sum = floor_sum.checked_add(floor).ok_or(MoneyError::OutOfRange)?;
        parts.push(i64::try_from(floor).map_err(|_| MoneyError::OutOfRange)?);
        let remainder = share
            .numerator
            .rem_euclid(share.denominator)
            .checked_mul(common / share.denominator)
            .ok_or(MoneyError::OutOfRange)?;
        remainders.push((index, account, remainder));
    }
    // Distribute the observation's rounded subscription_nanos across accounts:
    // start with each exact share's floor, then give remaining nanos to the
    // largest fractional shares, breaking equal remainders by account id.
    remainders.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.1.cmp(b.1)));
    let extra = i128::from(total)
        .checked_sub(floor_sum)
        .ok_or(MoneyError::OutOfRange)?;
    let extra = usize::try_from(extra).map_err(|_| MoneyError::OutOfRange)?;
    if extra > parts.len() {
        return Err(MoneyError::OutOfRange);
    }
    for (index, _, _) in remainders.into_iter().take(extra) {
        parts[index] = parts[index].checked_add(1).ok_or(MoneyError::OutOfRange)?;
    }
    Ok(parts)
}

fn to_json<T: Serialize>(value: &T) -> Result<String, MultiplierError> {
    serde_json::to_string(value).map_err(|error| MultiplierError::Encode(error.to_string()))
}

/// The current row for this provider and day: the one no other row
/// supersedes. `None` when the day has no row at all.
///
/// The head is NOT "the row with `supersedes IS NULL`": that is the day's
/// original, which a correction replaces.
pub fn observation_head(
    conn: &Connection,
    quota_provider: &str,
    observation_day: &str,
) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT o.observation_id FROM multiplier_observation o
          WHERE o.quota_provider = ?1 AND o.observation_day = ?2
            AND NOT EXISTS (SELECT 1 FROM multiplier_observation child
                             WHERE child.supersedes = o.observation_id)",
        params![quota_provider, observation_day],
        |row| row.get(0),
    )
    .optional()
}

/// Selects either a plan-wide chain with no recorded scope or a chain for a
/// specific window pool with recorded scope.
#[derive(Clone, Copy)]
pub enum ObservationHeadKey<'a> {
    PreCut {
        plan: Option<&'a str>,
    },
    PostCut {
        plan: &'a str,
        window_pool_key: &'a str,
    },
}

impl<'a> From<Option<&'a str>> for ObservationHeadKey<'a> {
    fn from(plan: Option<&'a str>) -> Self {
        Self::PreCut { plan }
    }
}

impl<'a> From<Option<&'a String>> for ObservationHeadKey<'a> {
    fn from(plan: Option<&'a String>) -> Self {
        Self::PreCut {
            plan: plan.map(String::as_str),
        }
    }
}

pub fn observation_plan_head<'a>(
    conn: &Connection,
    quota_provider: &str,
    day: &str,
    key: impl Into<ObservationHeadKey<'a>>,
) -> rusqlite::Result<Option<i64>> {
    let (lineage, plan, pool) = match key.into() {
        ObservationHeadKey::PreCut { plan } => ("o.pool_scope IS NULL", plan, None),
        ObservationHeadKey::PostCut {
            plan,
            window_pool_key,
        } => (
            "o.pool_scope IS NOT NULL AND o.window_pool_key=?4",
            Some(plan),
            Some(window_pool_key),
        ),
    };
    let sql = format!("SELECT o.observation_id FROM multiplier_observation o
        WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.plan IS ?3 AND {lineage}
        AND NOT EXISTS (SELECT 1 FROM multiplier_observation child WHERE child.supersedes=o.observation_id)");
    let mut query = conn.prepare_cached(&sql)?;
    if let Some(pool) = pool {
        query.query_row(params![quota_provider, day, plan, pool], |row| row.get(0))
    } else {
        query.query_row(params![quota_provider, day, plan], |row| row.get(0))
    }
    .optional()
}

fn observation_head_key(
    observation: &Observation,
) -> Result<ObservationHeadKey<'_>, MultiplierError> {
    let plan = observation.plan.as_deref();
    if observation.pool_scope.is_none() {
        return Ok(ObservationHeadKey::PreCut { plan });
    }
    let plan = plan
        .ok_or_else(|| MultiplierError::InvalidObservation("a scoped pool needs a plan".into()))?;
    Ok(ObservationHeadKey::PostCut {
        plan,
        window_pool_key: &observation.window_pool_key,
    })
}

/// Appends a day's original observation. An observation with a recorded
/// scope belongs to a separate chain for its window pool.
pub fn insert_observation_in_tx(
    tx: &Transaction<'_>,
    observation: &Observation,
) -> Result<(), MultiplierError> {
    if observation_plan_head(
        tx,
        &observation.quota_provider,
        &observation.observation_day,
        observation_head_key(observation)?,
    )?
    .is_some()
    {
        return Err(MultiplierError::AlreadyRecorded {
            quota_provider: observation.quota_provider.clone(),
            day: observation.observation_day.clone(),
        });
    }
    insert_row(tx, observation, None)?;
    Ok(())
}

/// A row that replaced the head of its provider and day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Superseded {
    pub quota_provider: String,
    pub observation_day: String,
    pub observation_id: i64,
    pub supersedes: i64,
}

/// Appends a recomputed observation that supersedes the current head of its
/// own chain. The head is read inside the write transaction. A chain with no
/// row is refused: there is nothing to correct.
pub fn insert_superseding_observation_in_tx(
    tx: &Transaction<'_>,
    observation: &Observation,
) -> Result<Superseded, MultiplierError> {
    let head = observation_plan_head(
        tx,
        &observation.quota_provider,
        &observation.observation_day,
        observation_head_key(observation)?,
    )?
    .ok_or_else(|| MultiplierError::NothingToSupersede {
        quota_provider: observation.quota_provider.clone(),
        day: observation.observation_day.clone(),
    })?;
    let observation_id = insert_row(tx, observation, Some(head))?;
    Ok(Superseded {
        quota_provider: observation.quota_provider.clone(),
        observation_day: observation.observation_day.clone(),
        observation_id,
        supersedes: head,
    })
}

/// Refuses an observation whose shape no producer of observations derives,
/// before anything is inserted. `plan_observation` and `compute_observation`
/// already produce exactly this shape; the store's triggers check only part of
/// it, so a caller that built or altered an observation itself would otherwise
/// write a row the readers misinterpret. Checked here:
///
/// - the token interval's two ends are both present or both absent;
/// - the interval is ordered, and a token interval is non-empty and lies
///   inside it, since it is the interval clipped to quota coverage;
/// - the window is a positive number of minutes;
/// - the recording and catalog instants are not before the Unix epoch, which
///   is what the clock that supplies them returns at worst;
/// - the observation day is the UTC day of the interval's end.
fn validate_observation(observation: &Observation) -> Result<(), MultiplierError> {
    let invalid = |message: String| Err(MultiplierError::InvalidObservation(message));
    let (start, end) = (observation.interval_start_ms, observation.interval_end_ms);
    if start >= end {
        return invalid(format!("interval [{start}, {end}) is not ordered"));
    }
    match (
        observation.token_interval_start_ms,
        observation.token_interval_end_ms,
    ) {
        (None, None) => {}
        (Some(token_start), Some(token_end)) => {
            if token_start >= token_end || token_start < start || token_end > end {
                return invalid(format!(
                    "token interval [{token_start}, {token_end}) is not a non-empty part of \
                     the interval [{start}, {end})"
                ));
            }
        }
        (token_start, token_end) => {
            return invalid(format!(
                "token interval ends {token_start:?} and {token_end:?} are not both present \
                 or both absent"
            ));
        }
    }
    if observation.window_minutes <= 0 {
        return invalid(format!(
            "window_minutes {} is not positive",
            observation.window_minutes
        ));
    }
    if observation.snapshot_at_ms < 0 || observation.catalog_resolved_at_ms < 0 {
        return invalid(format!(
            "snapshot_at_ms {} or catalog_resolved_at_ms {} is before the Unix epoch",
            observation.snapshot_at_ms, observation.catalog_resolved_at_ms
        ));
    }
    let day = utc_day(end)?;
    if observation.observation_day != day {
        return invalid(format!(
            "observation_day {} is not {day}, the UTC day the interval ends on",
            observation.observation_day
        ));
    }
    for share in [
        observation.clean_hours_share,
        observation.saturated_share,
        observation.gap_share,
        observation.unpriced_share,
        observation.unattributed_event_share,
    ] {
        if !share.is_finite() || !(0.0..=1.0).contains(&share) {
            return invalid("quality shares must be finite fractions in [0,1]".into());
        }
    }
    if observation.routing_eligible
        && (!observation.eligibility_reasons.is_empty()
            || !observation.source_backlog.is_empty()
            || !observation.unconfigured_accounts.is_empty())
    {
        return invalid("an eligible row cannot carry a completeness refusal".into());
    }
    Ok(())
}

fn insert_row(
    tx: &Transaction<'_>,
    observation: &Observation,
    supersedes: Option<i64>,
) -> Result<i64, MultiplierError> {
    validate_observation(observation)?;
    tx.execute(
        "INSERT INTO multiplier_observation (
            quota_provider, observation_day, snapshot_at_ms, interval_start_ms, interval_end_ms,
            token_interval_start_ms, token_interval_end_ms, window_pool_key, window_minutes,
            input_tokens, cache_read_tokens, cache_write_tokens, output_tokens, reasoning_tokens,
            api_nanos, api_nanos_by_model, unpriced_tokens, tokens_outside_quota_coverage,
            base_rate_tier_untested_tokens, subscription_nanos, fee_paid_nanos, delta_percent,
            accounts_included_count, accounts_included, accounts_excluded, excluded_pairs,
            low_resolution, scope_rules, plan_prices, catalog_resolved_at_ms, api_twins,
            reasoning_billed_as_output, error_deflating, error_inflating, multiplier,
            fee_multiplier, supersedes, plan, routing_eligible, eligibility_reasons,
            clean_hours_share, saturated_share, gap_share, unpriced_share, unconfigured_accounts, source_backlog,
            unattributed_event_share, zero_measurement_events, missing_quantity_events,
            pool_scope, pool_models, pool_models_at_ms, pool_member_models, catalog_provider
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,
                  ?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41,?42,?43,?44,?45,?46,?47,?48,?49,
                   ?50,?51,?52,?53,?54)",
        params![
            observation.quota_provider,
            observation.observation_day,
            observation.snapshot_at_ms,
            observation.interval_start_ms,
            observation.interval_end_ms,
            observation.token_interval_start_ms,
            observation.token_interval_end_ms,
            observation.window_pool_key,
            observation.window_minutes,
            observation.tokens.input,
            observation.tokens.cache_read,
            observation.tokens.cache_write,
            observation.tokens.output,
            observation.tokens.reasoning,
            observation.api_nanos,
            to_json(&observation.api_nanos_by_model)?,
            to_json(&observation.unpriced_tokens)?,
            observation.tokens_outside_quota_coverage,
            observation.base_rate_tier_untested_tokens,
            observation.subscription_nanos,
            observation.fee_paid_nanos,
            observation.delta_percent,
            i64::try_from(observation.accounts_included.len()).unwrap_or(i64::MAX),
            to_json(&observation.accounts_included)?,
            to_json(&observation.accounts_excluded)?,
            to_json(&observation.excluded_pairs)?,
            observation.low_resolution,
            to_json(&observation.scope_rules)?,
            to_json(&observation.plan_prices)?,
            observation.catalog_resolved_at_ms,
            to_json(&observation.api_twins)?,
            to_json(&observation.reasoning_billed_as_output)?,
            observation.error_deflating,
            observation.error_inflating,
            observation.multiplier,
            observation.fee_multiplier,
            supersedes,
            observation.plan,
            observation.routing_eligible,
            to_json(&observation.eligibility_reasons)?,
            observation.clean_hours_share,
            observation.saturated_share,
            observation.gap_share,
            observation.unpriced_share,
            to_json(&observation.unconfigured_accounts)?,
            to_json(&observation.source_backlog)?,
            observation.unattributed_event_share,
            i64::try_from(observation.zero_measurement_events).map_err(|_| MoneyError::OutOfRange)?,
            i64::try_from(observation.missing_quantity_events).map_err(|_| MoneyError::OutOfRange)?,
            observation.pool_scope.map(PoolScope::as_str),
            observation.pool_models.as_ref().map(to_json).transpose()?,
            observation.pool_models_at_ms,
            observation.pool_member_models.as_ref().map(to_json).transpose()?,
            observation.catalog_provider,
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// `spend.multiplier.report`: the scoped plan and pool heads of each provider's
/// newest recorded day, plus history for intervals ending in `[from_ms, to_ms)`.
/// History keeps legacy heads for keys without scoped observations. Neither
/// view includes retired or superseded heads. `supersedes` names the row a
/// served head replaced, if any.
///
/// Rows are placed by their day, not by `snapshot_at_ms`: a recompute is
/// taken later than the day it corrects, and must stand in its day's slot
/// rather than move the day, or become "latest" over newer days.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiplierReportRequest {
    pub from_ms: i64,
    pub to_ms: i64,
}

/// `spend.multiplier.recompute`: recompute one recorded day with the current
/// configuration and catalog, and append rows that supersede its heads. With
/// no `quota_provider`, every configured quota provider is recomputed, and
/// each must already have a row for the day.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiplierRecomputeRequest {
    pub observation_day: String,
    #[serde(default)]
    pub quota_provider: Option<String>,
}

const JSON_COLUMNS: [&str; 14] = [
    "eligibility_reasons",
    "unconfigured_accounts",
    "source_backlog",
    "api_nanos_by_model",
    "unpriced_tokens",
    "accounts_included",
    "accounts_excluded",
    "excluded_pairs",
    "scope_rules",
    "plan_prices",
    "api_twins",
    "reasoning_billed_as_output",
    "pool_models",
    "pool_member_models",
];

pub fn multiplier_report(
    conn: &Connection,
    request: &MultiplierReportRequest,
) -> Result<Value, MultiplierError> {
    if request.from_ms < 0 || request.from_ms >= request.to_ms {
        return Err(MultiplierError::Refused {
            code: "invalid_range",
            message: format!(
                "from_ms {} must be non-negative and earlier than to_ms {}",
                request.from_ms, request.to_ms
            ),
        });
    }
    let mut latest = observation_rows(
        conn,
        &format!(
            "WITH newest AS (
                 SELECT quota_provider, MAX(observation_day) AS day
                 FROM multiplier_provider_day GROUP BY quota_provider)
             SELECT o.* FROM newest n JOIN multiplier_observation o
               ON o.quota_provider=n.quota_provider AND o.observation_day=n.day
             WHERE o.pool_scope IS NOT NULL AND {UNREPLACED} AND {UNRETIRED}
             ORDER BY o.quota_provider, o.plan, o.window_pool_key"
        ),
        &[],
    )?;
    attach_last_eligible(conn, &mut latest)?;
    let mut series = observation_rows(
        conn,
        &format!(
            "SELECT o.* FROM multiplier_observation o
             WHERE o.interval_end_ms >= ?1 AND o.interval_end_ms < ?2
               AND {UNREPLACED} AND {UNRETIRED}
             ORDER BY o.interval_end_ms, o.quota_provider, o.plan, o.window_pool_key"
        ),
        &[request.from_ms, request.to_ms],
    )?;
    keep_legacy_only_keys(conn, &mut series)?;
    Ok(json!({"latest": latest, "series": series}))
}

const UNREPLACED: &str = "NOT EXISTS (
    SELECT 1 FROM multiplier_observation child WHERE child.supersedes=o.observation_id)";
const UNRETIRED: &str = "NOT EXISTS (
    SELECT 1 FROM json_each(o.eligibility_reasons) WHERE value='pool_retired')";

fn report_pool_key(row: &Value) -> Option<(&str, &str, &str)> {
    Some((
        row["quota_provider"].as_str()?,
        row["plan"].as_str()?,
        row["pool_id"].as_str()?,
    ))
}

fn attach_last_eligible(conn: &Connection, latest: &mut [Value]) -> Result<(), MultiplierError> {
    if latest.is_empty() {
        return Ok(());
    }
    // Preserve SQLite REALs exactly and match pool identity, not the stored
    // key spelling. Eligible legacy heads may seed the same pool's fallback.
    let mut statement = conn.prepare(&format!(
        "SELECT o.window_pool_key, o.observation_day, o.interval_end_ms,
                o.multiplier, o.fee_multiplier
         FROM multiplier_observation o
         WHERE o.quota_provider=?1 AND o.plan=?2 AND o.routing_eligible=1
           AND {UNREPLACED} AND {UNRETIRED}
         ORDER BY o.observation_day DESC, o.observation_id DESC"
    ))?;
    for row in latest {
        let (provider, plan, pool) = report_pool_key(row).ok_or_else(|| {
            MultiplierError::Encode("scoped head has no plan or pool identity".into())
        })?;
        let mut candidates = statement.query(params![provider, plan])?;
        while let Some(candidate) = candidates.next()? {
            let key: String = candidate.get(0)?;
            if crate::capacity::pool_identity(&key) == Some(pool) {
                let eligible = json!({
                    "day": candidate.get::<_, String>(1)?,
                    "interval_end_ms": candidate.get::<_, i64>(2)?,
                    "multiplier": candidate.get::<_, Option<f64>>(3)?,
                    "fee_multiplier": candidate.get::<_, Option<f64>>(4)?,
                });
                row.as_object_mut()
                    .expect("observation row is an object")
                    .insert("last_eligible".into(), eligible);
                break;
            }
        }
    }
    Ok(())
}

fn keep_legacy_only_keys(
    conn: &Connection,
    series: &mut Vec<Value>,
) -> Result<(), MultiplierError> {
    if !series
        .iter()
        .any(|row| row["pool_scope"].is_null() && report_pool_key(row).is_some())
    {
        return Ok(());
    }
    // A key with scoped history no longer shows legacy measurements, even
    // when its scoped rows are outside the range or have been retired.
    let mut statement = conn.prepare(
        "SELECT DISTINCT quota_provider, plan, window_pool_key
         FROM multiplier_observation WHERE pool_scope IS NOT NULL",
    )?;
    let mut rows = statement.query([])?;
    let mut scoped = BTreeSet::new();
    while let Some(row) = rows.next()? {
        let key: String = row.get(2)?;
        if let Some(pool) = crate::capacity::pool_identity(&key) {
            scoped.insert((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                pool.to_owned(),
            ));
        }
    }
    series.retain(|row| {
        !row["pool_scope"].is_null()
            || report_pool_key(row).is_none_or(|(provider, plan, pool)| {
                !scoped.contains(&(provider.to_owned(), plan.to_owned(), pool.to_owned()))
            })
    });
    Ok(())
}

fn observation_rows(
    conn: &Connection,
    sql: &str,
    bounds: &[i64],
) -> Result<Vec<Value>, MultiplierError> {
    let mut statement = conn.prepare(sql)?;
    let names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut rows = statement.query(params_from_iter(bounds.iter()))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let mut object = serde_json::Map::new();
        for (index, name) in names.iter().enumerate() {
            use rusqlite::types::ValueRef;
            let value = match row.get_ref(index)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value)
                    if name == "low_resolution" || name == "routing_eligible" =>
                {
                    json!(value != 0)
                }
                ValueRef::Integer(value) => json!(value),
                ValueRef::Real(value) => json!(value),
                ValueRef::Text(text) => {
                    let text = String::from_utf8_lossy(text);
                    if JSON_COLUMNS.contains(&name.as_str()) {
                        serde_json::from_str(&text)
                            .map_err(|error| MultiplierError::Encode(error.to_string()))?
                    } else {
                        json!(text)
                    }
                }
                ValueRef::Blob(_) => {
                    return Err(MultiplierError::Encode(format!(
                        "column {name} holds a blob"
                    )))
                }
            };
            object.insert(name.clone(), value);
        }
        let pool_id = object["window_pool_key"]
            .as_str()
            .and_then(crate::capacity::pool_identity);
        object.insert("pool_id".into(), json!(pool_id));
        let unknown_quantity_measurements = object
            .get("unpriced_tokens")
            .and_then(Value::as_array)
            .ok_or_else(|| MultiplierError::Encode("unpriced_tokens must be an array".into()))?
            .iter()
            .filter(|entry| entry["reason"] == "missing_quantity")
            .try_fold(0_u64, |sum, entry| -> Result<u64, MultiplierError> {
                let count = entry["measurements"].as_u64().ok_or_else(|| {
                    MultiplierError::Encode("unpriced measurement count must be nonnegative".into())
                })?;
                Ok(sum.checked_add(count).ok_or(MoneyError::OutOfRange)?)
            })?;
        object.insert(
            "unknown_quantity_measurements".into(),
            json!(unknown_quantity_measurements),
        );
        let stability = object
            .get("accounts_included")
            .and_then(Value::as_array)
            .and_then(|accounts| accounts.first())
            .and_then(|account| account.get("plan_calibration"))
            .and_then(|calibration| calibration.get("summary"))
            .cloned()
            .unwrap_or(Value::Null);
        object.insert("stability_diagnostics".into(), stability);
        out.push(Value::Object(object));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    include!("multiplier/shared_valuer_tests.rs");
    include!("multiplier/rate_era_tests.rs");
    include!("multiplier/cache_write_billed_as_tests.rs");
    include!("fusiform_plan_price_tests.rs");
    include!("multiplier/v2_tests.rs");
    include!("multiplier/pace_tests.rs");
    include!("multiplier/context_tier_tests.rs");
    include!("multiplier/history_scaling_tests.rs");
    include!("multiplier/lineage_tests.rs");
    use super::*;
    use crate::test_support::conn;

    /// The multiplier values a rate stated at any exponent, as whole nano-USD
    /// per million tokens: $30/M as 3000 hundredths or 30 dollars, and a finer
    /// exponent only when it is a whole nano. A rate it can't hold exactly is
    /// left unpriced rather than rounded.
    #[test]
    fn rate_state_holds_any_stated_exponent_exactly() {
        let state = |units: i64, exponent: u8| {
            let snapshot = CatalogSnapshot {
                source: "test".into(),
                resolved_at_ms: 0,
                model_key: "provider/model".into(),
                facts: BTreeMap::from([(
                    "rate.input".into(),
                    serde_json::json!({"state":"priced","units":units,"exponent":exponent,
                        "currency":"USD","unit_provenance":{"kind":"stated"}}),
                )]),
                withheld_facts: BTreeMap::new(),
                uncertain_facts: BTreeMap::new(),
                fact_overrides: BTreeMap::new(),
                alias_target: None,
            };
            rate_state(&snapshot, "rate.input")
        };
        let priced = |units_per_million| RateState::Priced { units_per_million };
        assert_eq!(state(3_000, 2), priced(30_000_000_000));
        assert_eq!(state(30, 0), priced(30_000_000_000));
        assert_eq!(state(30_000_000_000, 9), priced(30_000_000_000));
        assert_eq!(state(50, 10), priced(5));
        assert_eq!(
            state(5, 10),
            RateState::Unpriced {
                reason: "rate_exponent_unsupported".into()
            }
        );
    }

    const DAY: i64 = 86_400_000;
    const TEN_PER_M: i64 = 10_000_000_000;

    /// One reading, stored in arrival order. `at_ms` is its `fetched_at`.
    fn reading(conn: &Connection, account: &str, at_ms: i64, used: f64, raw: Option<f64>) {
        conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant,
                 used_percent, raw_used_percent, resets_at, window_minutes, source, model_scope)
             VALUES ('codex', ?1, 'primary', ?2, ?3, ?4, '2026-09-30T00:00:00Z', 10080, 'vault', 'all')",
            params![account, at_ms * NANOS_PER_MS, used, raw],
        )
        .unwrap();
    }

    /// One usage event with its token measurements `(role, source, quantity)`.
    #[allow(clippy::too_many_arguments)]
    fn event(
        conn: &Connection,
        id: &str,
        producer: &str,
        provider: &str,
        model: &str,
        basis: &str,
        at_ms: i64,
        measurements: &[(&str, &str, i64)],
    ) {
        conn.execute(
            "INSERT INTO usage_event (event_id, producer, producer_lineage, producer_ref,
                 occurred_at_ms, recorded_at_ms, provider, model, charge_basis)
             VALUES (?1, ?2, 'lineage', ?1, ?3, ?3, ?4, ?5, ?6)",
            params![id, producer, at_ms, provider, model, basis],
        )
        .unwrap();
        for (role, source, quantity) in measurements {
            conn.execute(
                "INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units,
                     quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1), ?2, 'token', 'billed', ?3, ?4, 0)",
                params![id, role, source, quantity],
            )
            .unwrap();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn account_event(
        conn: &Connection,
        id: &str,
        producer: &str,
        provider: &str,
        model: &str,
        basis: &str,
        at_ms: i64,
        measurements: &[(&str, &str, i64)],
    ) {
        account_event_in_class(
            conn,
            id,
            producer,
            provider,
            model,
            basis,
            at_ms,
            None,
            measurements,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn account_event_in_class(
        conn: &Connection,
        id: &str,
        producer: &str,
        provider: &str,
        model: &str,
        basis: &str,
        at_ms: i64,
        work_class: Option<&str>,
        measurements: &[(&str, &str, i64)],
    ) {
        conn.execute(
            "INSERT INTO usage_event (event_id, producer, producer_lineage, producer_ref,
                 occurred_at_ms, recorded_at_ms, provider, model, charge_basis, account_ref, work_class)
             VALUES (?1, ?2, 'lineage', ?1, ?3, ?3, ?4, ?5, ?6, 'acct', ?7)",
            params![id, producer, at_ms, provider, model, basis, work_class],
        )
        .unwrap();
        for (role, source, quantity) in measurements {
            conn.execute(
                "INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units,
                     quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1), ?2, 'token', 'billed', ?3, ?4, 0)",
                params![id, role, source, quantity],
            )
            .unwrap();
        }
    }

    fn priced(units: i64) -> RateState {
        RateState::Priced {
            units_per_million: units,
        }
    }

    /// Every role at `units`, and at `tier_units` above `threshold` when given.
    fn rates_for(models: &[(&str, &str)], units: i64, tier: Option<(u64, i64)>) -> ResolvedRates {
        let roles = ["input", "cache_read", "cache_write", "output"];
        let base: BTreeMap<String, RateState> = roles
            .iter()
            .map(|role| (role.to_string(), priced(units)))
            .collect();
        let tiers = tier
            .map(|(threshold, tier_units)| {
                BTreeMap::from([(
                    threshold,
                    roles
                        .iter()
                        .map(|role| (role.to_string(), priced(tier_units)))
                        .collect(),
                )])
            })
            .unwrap_or_default();
        ResolvedRates {
            resolved_at_ms: 42,
            models: models
                .iter()
                .map(|(provider, model)| {
                    (
                        (provider.to_string(), model.to_string()),
                        ModelResolution::Rates(ModelRates {
                            base: base.clone(),
                            tiers: tiers.clone(),
                            modes: BTreeMap::new(),
                            conditional: BTreeMap::new(),
                            reference_model: None,
                        }),
                    )
                })
                .collect(),
            eras: BTreeMap::new(),
        }
    }

    fn fixture_pool_schema_at_genesis(conn: &Connection) {
        conn.execute_batch("CREATE TABLE cortexkit_schema_version(namespace TEXT, version INTEGER, applied_at_unix INTEGER);
            INSERT INTO cortexkit_schema_version VALUES ('astrocyte',68,0);").unwrap();
    }

    fn test_plan_price(account: &str, plan: &str, monthly_nanos: Option<i64>) -> PlanPriceUsed {
        PlanPriceUsed {
            account_ref: account.into(),
            credential_ref: None,
            plan: plan.into(),
            source: None,
            monthly_nanos,
            review_by_ms: None,
            plan_price_overdue: false,
            source_ref: None,
            unpriceable_reason: None,
            refusal_text: None,
        }
    }

    fn measure_fixture_quota(
        prices: &[(&str, f64, i64)],
        span: (i64, i64),
    ) -> Result<measurement::QuotaTotals, MultiplierError> {
        let rows: Vec<_> = prices
            .iter()
            .map(|(account, delta, monthly)| {
                let mut counter = clean::measure_run(account, [], 0, span);
                counter.delta_percent = *delta;
                (
                    counter,
                    test_plan_price(account, "openai/pro_20x", Some(*monthly)),
                )
            })
            .collect();
        measurement::measure_quota(
            MINUTES_PER_MONTH,
            Some(span),
            rows.iter().map(|(counter, price)| measurement::CounterFee {
                counter,
                identity_field: "account_ref",
                price: Some(price),
                segments: 1,
            }),
        )
    }

    #[test]
    fn money_subscription_and_fee_keep_large_integer_nanos_exact() {
        let monthly = 9_007_199_254_740_993;
        let quota = measure_fixture_quota(&[("acct", 100.0, monthly)], (0, MS_PER_MONTH)).unwrap();
        assert_eq!(quota.subscription_nanos, monthly);
        assert_eq!(quota.fee_paid_nanos, monthly);
        assert_eq!(quota.accounts_included[0]["subscription_nanos"], monthly);
    }

    #[test]
    fn money_subscription_and_fee_round_exact_ties_to_even() {
        let quota = measure_fixture_quota(&[("acct", 50.0, 1)], (0, MS_PER_MONTH / 2)).unwrap();
        assert_eq!(quota.subscription_nanos, 0);
        assert_eq!(quota.fee_paid_nanos, 0);
    }

    #[test]
    fn money_account_parts_sum_to_the_row_with_deterministic_remainders() {
        for deltas in [
            [50.0, 50.0, 50.0],
            [10.0, 60.0, 90.0],
            [0.0, 0.0, 0.0],
            [12.5, 12.5, 25.0],
        ] {
            let quota = measure_fixture_quota(
                &[
                    ("c", deltas[0], 1),
                    ("b", deltas[1], 1),
                    ("a", deltas[2], 1),
                ],
                (0, MS_PER_MONTH),
            )
            .unwrap();
            let parts: BTreeMap<_, _> = quota
                .accounts_included
                .iter()
                .map(|a| {
                    (
                        a["account_ref"].as_str().unwrap(),
                        a["subscription_nanos"].as_i64().unwrap(),
                    )
                })
                .collect();
            assert_eq!(
                parts.values().sum::<i64>(),
                quota.subscription_nanos,
                "deltas {deltas:?}"
            );
            if deltas.iter().all(|delta| (*delta - 50.0).abs() < 1e-9) {
                assert_eq!(quota.subscription_nanos, 2);
                assert_eq!(parts, BTreeMap::from([("a", 1), ("b", 1), ("c", 0)]));
            }
        }
    }

    #[test]
    fn money_denominator_overflow_is_refused() {
        assert!(matches!(
            measure_fixture_quota(
                &[("a", 100.0, i64::MAX), ("b", 100.0, i64::MAX)],
                (0, MS_PER_MONTH)
            ),
            Err(MultiplierError::ArithmeticOutOfRange)
        ));
    }

    fn delta(conn: &Connection, start: i64, end: i64) -> AccountDelta {
        fixture_vault_plan(conn, "openai/pro_20x");
        fixture_pool_schema_at_genesis(conn);
        let capture = pools::capture_pool_readings(conn, "codex", start, end).unwrap();
        let planned = pools::plan_pools(&capture, &[]);
        let run = &planned[0].reading_runs[0];
        clean::measure_run(
            "acct",
            run.readings.iter().map(|r| {
                (
                    r.fetched_at_instant_ns,
                    r.used_percent,
                    r.resets_at.as_deref(),
                )
            }),
            clean::reset_tolerance_ms(planned[0].window_minutes),
            (start, end),
        )
    }

    #[test]
    fn millisecond_conversion_preserves_both_saturated_extremes() {
        for ms in [
            i64::MIN,
            -9_223_372_036_855,
            -1,
            0,
            1,
            9_223_372_036_855,
            i64::MAX,
        ] {
            assert_eq!(
                clamp_nanos(millis_to_nanos(ms)),
                ms.saturating_mul(NANOS_PER_MS)
            );
        }
    }

    #[test]
    fn money_api_half_even_and_signed_rates_reach_the_observation() {
        for (rate, expected) in [(500_000, 0), (2_500_000, 2), (-1_000_000, -1)] {
            let conn = conn();
            event(
                &conn,
                "e",
                "codex",
                "openai",
                "m",
                "subscription_included",
                50,
                &[("input", "usage.input", 1)],
            );
            let observation = measured_usage(
                &test_calls(&conn, 0, 100),
                (0, 100),
                &rates_for(&[("openai", "m")], rate, None),
            )
            .unwrap();
            assert_eq!(observation.api_nanos, expected, "rate {rate}");
            assert_eq!(observation.api_nanos_by_model[0].api_nanos, expected);
        }
    }

    #[test]
    fn money_outside_coverage_counts_the_same_disjoint_tokens_including_lifetime_writes() {
        let conn = conn();
        for (id, at) in [("inside", 50), ("outside", 95)] {
            event(
                &conn,
                id,
                "broca",
                "openai",
                "m",
                "subscription_included",
                at,
                &[
                    ("input", "usage.input", 10),
                    ("output", "usage.output_tokens", 7),
                    ("reasoning", "usage.reasoning", 3),
                ],
            );
            conn.execute("INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units, quantity_exponent)
                VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1), 'cache_write', 'token_cache_5m', 'billed', 'usage.cache_write', 5, 0)", [id]).unwrap();
        }
        let observation = measured_usage(
            &test_calls(&conn, 0, 100),
            (10, 90),
            &rates_for(&[("openai", "m")], 1_000_000, None),
        )
        .unwrap();
        assert_eq!(
            observation.tokens,
            RoleTokens {
                input: 10,
                cache_write: 5,
                output: 4,
                reasoning: 3,
                ..RoleTokens::default()
            }
        );
        assert_eq!(observation.tokens_outside_quota_coverage, 22);
    }

    fn assert_failed_day(conn: &Connection, reason: Option<String>) {
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            rows, 0,
            "an arithmetic failure must not record a capped or partial day"
        );
        assert_eq!(reason.as_deref(), Some("arithmetic_out_of_range"));
    }

    #[test]
    fn money_api_overflow_does_not_record_a_capped_day() {
        let conn = conn();
        event(
            &conn,
            "e",
            "codex",
            "openai",
            "m",
            "subscription_included",
            50,
            &[("input", "usage.input", i64::MAX)],
        );
        let reason = measured_usage(
            &test_calls(&conn, 0, 100),
            (0, 100),
            &rates_for(&[("openai", "m")], i64::MAX, None),
        )
        .err()
        .map(|error| error.code().to_owned());
        assert_failed_day(&conn, reason);
    }

    #[test]
    fn money_token_overflow_does_not_record_a_wrapped_day() {
        for across_events in [false, true] {
            let conn = conn();
            let mut measurements = vec![("input", "usage.input", i64::MAX)];
            if !across_events {
                measurements.push(("output", "usage.output", 1));
            }
            event(
                &conn,
                "e",
                "codex",
                "openai",
                "m",
                "subscription_included",
                50,
                &measurements,
            );
            if across_events {
                event(
                    &conn,
                    "e2",
                    "codex",
                    "openai",
                    "m",
                    "subscription_included",
                    50,
                    &[("input", "usage.input", 1)],
                );
            }
            let reason = measured_usage(
                &test_calls(&conn, 0, 100),
                (0, 100),
                &rates_for(&[("openai", "m")], 0, None),
            )
            .err()
            .map(|error| error.code().to_owned());
            assert_failed_day(&conn, reason);
        }
    }

    #[test]
    fn money_whole_nano_rate_recompute_preserves_the_stored_api_value() {
        let mut conn = conn();
        event(
            &conn,
            "e",
            "codex",
            "openai",
            "m",
            "subscription_included",
            50,
            &[
                ("input", "usage.input", 100),
                ("output", "usage.output", 60),
                ("reasoning", "usage.reasoning", 20),
            ],
        );
        let rates = rates_with_reasoning_as_output();
        let original = storeable_usage(&conn, (0, 100), &rates, 100);
        assert_eq!(original.api_nanos, 1_800_000);
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &original).unwrap();
        tx.commit().unwrap();
        let old_row: (i64, i64, String) = conn
            .query_row(
                "SELECT observation_id, api_nanos, accounts_included FROM multiplier_observation",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let request = MultiplierReportRequest {
            from_ms: 0,
            to_ms: 101,
        };
        let before = multiplier_report(&conn, &request).unwrap();
        assert_eq!(before["series"][0]["api_nanos"], 1_800_000);
        let recomputed = storeable_usage(&conn, (0, 100), &rates, 200);
        assert_eq!(recomputed.api_nanos, old_row.1);
        let tx = conn.transaction().unwrap();
        insert_superseding_observation_in_tx(&tx, &recomputed).unwrap();
        tx.commit().unwrap();
        let unchanged: (i64, String) = conn.query_row("SELECT api_nanos, accounts_included FROM multiplier_observation WHERE observation_id = ?1", [old_row.0],
            |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
        assert_eq!(unchanged, (old_row.1, old_row.2));
        let after = multiplier_report(&conn, &request).unwrap();
        assert_eq!(
            after["series"][0]["api_nanos"],
            before["series"][0]["api_nanos"]
        );
    }

    #[test]
    fn money_aggregate_and_numerator_overflows_leave_no_observation() {
        for numerator_overflow in [false, true] {
            let conn = conn();
            event(
                &conn,
                "input",
                "codex",
                "openai",
                "m",
                "subscription_included",
                50,
                &[("input", "usage.input", i64::MAX)],
            );
            event(
                &conn,
                "output",
                "codex",
                "openai",
                if numerator_overflow { "m" } else { "other" },
                "subscription_included",
                50,
                &[(
                    "output",
                    "usage.output",
                    if numerator_overflow { i64::MAX } else { 1 },
                )],
            );
            if numerator_overflow {
                event(
                    &conn,
                    "cache",
                    "codex",
                    "openai",
                    "m",
                    "subscription_included",
                    50,
                    &[("cache_read", "usage.cache_read", i64::MAX)],
                );
            }
            let rates = rates_for(
                &[("openai", "m"), ("openai", "other")],
                if numerator_overflow {
                    i64::MAX
                } else {
                    1_000_000
                },
                None,
            );
            let reason = measured_usage(&test_calls(&conn, 0, 100), (0, 100), &rates)
                .err()
                .map(|error| error.code().to_owned());
            assert_failed_day(&conn, reason);
        }
    }

    #[test]
    fn money_largest_remainder_parts_sum_for_all_three_way_eighths() {
        for a in 0..=8 {
            for b in 0..=8 {
                for c in 0..=8 {
                    let shares = [("c", a), ("a", b), ("b", c)].map(|(id, numerator)| {
                        (id.to_string(), NanoRatio::new(numerator, 8).unwrap())
                    });
                    // Independent expectation in eighth-nanos, including ties.
                    let sum = a + b + c;
                    let floor = sum / 8;
                    let total = floor + i128::from(sum % 8 > 4 || (sum % 8 == 4 && floor % 2 == 1));
                    let total = i64::try_from(total).unwrap();
                    let parts = allocate_subscription(&shares, total).unwrap();
                    assert_eq!(parts.iter().sum::<i64>(), total);
                    for (part, (_, share)) in parts.iter().zip(shares) {
                        assert!((share.numerator / share.denominator
                            ..=(share.numerator + share.denominator - 1) / share.denominator)
                            .contains(&i128::from(*part)));
                    }
                }
            }
        }
    }

    #[test]
    fn money_percentage_decimal_is_exact_before_pro_rata_multiplication() {
        for (percent, numerator, denominator) in [
            (10.1, 101, 10),
            (0.00000001, 1, 100_000_000),
            (100.0, 100, 1),
        ] {
            let ratio = NanoRatio::percent(percent).unwrap();
            assert_eq!(
                (ratio.numerator, ratio.denominator),
                (numerator, denominator)
            );
        }
        for invalid in [f64::NAN, f64::INFINITY, -1.0, f64::MAX, f64::MIN_POSITIVE] {
            assert!(NanoRatio::percent(invalid).is_err());
        }
    }

    #[test]
    fn money_pace_uses_half_even_for_list_value_and_per_point_nanos() {
        let mut conn = conn();
        let mut head = pace_ready(sample_observation(&conn));
        head.api_nanos = 5;
        head.delta_percent = 2.0;
        head.api_nanos_by_model = vec![ModelLine {
            producer: "codex".into(),
            provider: "openai-codex".into(),
            model: Some("m".into()),
            rates: rates_for(&[("openai", "m")], 500_000, None)
                .models
                .remove(&("openai".into(), "m".into())),
            ..ModelLine::default()
        }];
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        account_event(
            &conn,
            "e",
            "codex",
            "openai",
            "m",
            "subscription_included",
            100,
            &[("input", "usage.input", 1)],
        );
        let report = multiplier_pace(
            &conn,
            &MultiplierPaceRequest {
                pool: None,
                plan: None,
                quota_provider: "codex".into(),
                from_ms: 0,
                to_ms: DAY,
            },
        )
        .unwrap();
        assert_eq!(report["per_point_nanos"], 2);
        assert_eq!(report["hours"][0]["producers"]["codex"]["list_nanos"], 0);
        assert_eq!(
            report["hours"][0]["producers"]["codex"]["estimated_points"],
            0.0
        );
    }

    /// A drop with the same reset horizon does not lower the high-water mark.
    #[test]
    fn a_decrease_without_a_reset_does_not_lower_the_high_water() {
        let conn = conn();
        for (at, used) in [(10, 10.0), (20, 30.0), (30, 20.0), (40, 25.0)] {
            reading(&conn, "acct", at, used, None);
        }
        let delta = delta(&conn, 0, 100);
        assert_eq!(
            delta.delta_percent, 20.0,
            "the rebound stays below the high-water mark"
        );
        assert_eq!(delta.decrease_boundaries, 1);
        assert_eq!(delta.readings, 4);
    }

    /// insula relaxes a reading by moving the real percent into
    /// `raw_used_percent` and zeroing `used_percent`. Read with COALESCE the
    /// series is 40, 45, 50; read from `used_percent` alone it is 40, 0, 50,
    /// which invents a 50-point climb out of the zero and a false boundary.
    #[test]
    fn raw_used_percent_is_read_over_a_zeroed_used_percent() {
        let conn = conn();
        reading(&conn, "acct", 10, 40.0, None);
        reading(&conn, "acct", 20, 0.0, Some(45.0));
        reading(&conn, "acct", 30, 50.0, None);
        let delta = delta(&conn, 0, 100);
        assert_eq!(delta.delta_percent, 10.0);
        assert_eq!(delta.decrease_boundaries, 0);
    }

    /// Source time, not ingestion order, defines the counter series.
    #[test]
    fn readings_are_ordered_by_fetched_at_not_arrival() {
        let conn = conn();
        for (at, used) in [(10, 10.0), (30, 20.0), (20, 25.0), (40, 22.0)] {
            reading(&conn, "acct", at, used, None);
        }
        let delta = delta(&conn, 0, 100);
        assert_eq!(delta.delta_percent, 15.0);
        assert_eq!(delta.fetched_at_backwards, 0);
        assert_eq!(delta.decrease_boundaries, 1);
    }

    /// The first pair's rise is clipped proportionally to the interval. A
    /// nearby flat end sample can bracket the tail without adding more burn.
    #[test]
    fn the_anchor_before_the_interval_measures_the_first_increment() {
        let conn = conn();
        reading(&conn, "acct", 5, 30.0, None);
        reading(&conn, "acct", 15, 38.0, None);
        reading(&conn, "acct", 120, 38.0, None);
        let delta = delta(&conn, 10, 100);
        assert_eq!(delta.delta_percent, 4.0);
        assert_eq!(delta.coverage_ms, Some((10, 100)));
    }

    /// Tokens before the first quota reading have no Δused% to set against
    /// them: they are reported as outside coverage and never priced.
    #[test]
    fn the_clip_excludes_tokens_outside_quota_coverage() {
        let conn = conn();
        reading(&conn, "acct", 3 * DAY, 0.0, None);
        reading(&conn, "acct", 4 * DAY, 10.0, None);
        event(
            &conn,
            "before",
            "opencode",
            "openai",
            "m",
            "unknown",
            DAY,
            &[("input", "usage.input", 700)],
        );
        event(
            &conn,
            "inside",
            "opencode",
            "openai",
            "m",
            "unknown",
            3 * DAY + 5,
            &[("input", "usage.input", 1_000)],
        );
        let observation = measured_usage(
            &test_calls(&conn, 0, 7 * DAY),
            (3 * DAY, 4 * DAY),
            &rates_for(&[("openai", "m")], TEN_PER_M, None),
        )
        .unwrap();
        assert_eq!(observation.tokens.input, 1_000);
        assert_eq!(observation.tokens_outside_quota_coverage, 700);
        assert_eq!(observation.api_nanos, 10_000_000, "1,000 tokens at $10/M");
    }

    /// BROCA's older rows report `output` INCLUDING reasoning. Valued as is,
    /// the reasoning would be priced twice (once inside output, once at the
    /// output rate as reasoning). The inclusive row and the disjoint row carry
    /// the same work and must be valued the same.
    #[test]
    fn broca_inclusive_output_is_not_double_counted() {
        let conn = conn();
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "inclusive",
            "broca",
            "openai",
            "m",
            "subscription_included",
            10,
            &[
                ("output", "usage.output_tokens", 1_000),
                ("reasoning", "usage.reasoning_tokens", 400),
            ],
        );
        event(
            &conn,
            "disjoint",
            "broca",
            "openai",
            "m",
            "subscription_included",
            20,
            &[
                ("output", "usage.output_tokens-reasoning_tokens", 600),
                ("reasoning", "usage.reasoning_tokens", 400),
            ],
        );
        let observation = storeable_usage(&conn, (0, 200), &rates_with_reasoning_as_output(), 200);
        // 2 × 1,000 tokens of real work at $10/M.
        assert_eq!(observation.api_nanos, 20_000_000);
        assert_eq!(observation.tokens.output, 1_200);
        assert_eq!(observation.tokens.reasoning, 800);
        let line = &observation.api_nanos_by_model[0];
        assert_eq!(line.inclusive_output_adjustment, 400);
        assert_eq!(line.measured_tokens.output, 1_600);
    }

    #[test]
    fn cache_write_ttl_multiplier_matches_complete_tier_and_mode_sets() {
        let rate = |units: i64| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
        let mut snapshot = CatalogSnapshot {
            alias_target: None,
            source: "models.dev".into(),
            resolved_at_ms: 42,
            model_key: "anthropic/m".into(),
            facts: BTreeMap::from([
                ("rate.cache_write".into(), rate(TEN_PER_M)),
                ("rate.cache_write.ttl.1h".into(), rate(2 * TEN_PER_M)),
                ("rate.cache_write.mode.fast".into(), rate(3 * TEN_PER_M)),
                (
                    "rate.cache_write.ttl.1h.above_context.100".into(),
                    rate(4 * TEN_PER_M),
                ),
                (
                    "rate.cache_write.ttl.1h.mode.fast.above_context.100".into(),
                    rate(5 * TEN_PER_M),
                ),
            ]),
            withheld_facts: BTreeMap::new(),
            uncertain_facts: BTreeMap::new(),
            fact_overrides: BTreeMap::new(),
        };
        for (mode, remove_tier, expected) in [
            (None, false, Some(40_000_000_000_000_i128)),
            (Some("fast"), false, Some(50_000_000_000_000)),
            (Some("priority"), false, None),
            (None, true, None),
        ] {
            if remove_tier {
                snapshot
                    .facts
                    .remove("rate.cache_write.ttl.1h.above_context.100");
            }
            let resolution = rates_from_snapshot(&snapshot);
            assert!(matches!(resolution, ModelResolution::Rates(_)));
            let round_trip: ModelResolution =
                serde_json::from_value(serde_json::to_value(&resolution).unwrap()).unwrap();
            assert_eq!(round_trip, resolution);
            let event = EventTokens {
                producer: "claude-code".into(),
                provider: "anthropic".into(),
                model: Some("m".into()),
                mode: mode.map(str::to_owned),
                broca_attempt: false,
                roles: vec![(
                    "cache_write".into(),
                    "token_cache_1h".into(),
                    "usage.cache_write".into(),
                    Some(1_000),
                    Some(0),
                )],
            };
            let mut lines = BTreeMap::new();
            let mut unpriced = BTreeMap::new();
            let mut totals = RoleTokens::default();
            value_event(
                Some(&resolution),
                &event,
                &mut lines,
                &mut unpriced,
                &mut totals,
            )
            .unwrap();
            let line = lines.into_values().next().unwrap();
            assert_eq!(line.numerator, expected.unwrap_or(0));
            assert_eq!(totals.cache_write, 1_000);
            if expected.is_some() {
                assert!(unpriced.is_empty());
            } else {
                assert_eq!(
                    unpriced,
                    BTreeMap::from([(
                        (
                            "no_rate".into(),
                            "claude-code/anthropic/m".into(),
                            "cache_write".into()
                        ),
                        (1_000, 1)
                    )])
                );
            }
        }
    }

    /// Claude Code stores lifetime-split cache writes under the units
    /// `token_cache_5m` and `token_cache_1h`; without a matching lifetime rate
    /// they are counted as `no_rate`, never dropped or valued at the plain rate.
    #[test]
    fn a_cache_write_split_by_lifetime_is_counted_not_dropped() {
        let conn = conn();
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "e",
            "claude-code",
            "anthropic",
            "m",
            "unknown",
            10,
            &[("input", "usage.input_tokens", 100)],
        );
        conn.execute(
            "INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units,
                 quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id='e'), 'cache_write', 'token_cache_1h', 'billed',
                 'usage.cache_creation.ephemeral_1h_input_tokens', 4000, 0)",
            [],
        )
        .unwrap();
        let observation = storeable_usage(
            &conn,
            (0, 200),
            &rates_for(&[("anthropic", "m")], TEN_PER_M, None),
            200,
        );
        assert_eq!(
            observation.api_nanos, 1_000_000,
            "only the 100 input tokens"
        );
        let unpriced: Vec<_> = observation
            .unpriced_tokens
            .iter()
            .map(|entry| (entry.reason.as_str(), entry.role.as_str(), entry.tokens))
            .collect();
        assert_eq!(unpriced, [("no_rate", "cache_write", 4000)]);
    }

    /// A per-request producer's event over the threshold prices every role at
    /// the tier rate; a bundling producer's event is priced at base and its
    /// tokens are flagged, whatever its aggregate prompt.
    #[test]
    fn tiers_are_decided_per_event_only_where_an_event_is_one_call() {
        let conn = conn();
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        let rates = rates_for(&[("openai", "m")], TEN_PER_M, Some((1_000, 2 * TEN_PER_M)));
        event(
            &conn,
            "small",
            "opencode",
            "openai",
            "m",
            "unknown",
            10,
            &[
                ("input", "usage.input", 400),
                ("cache_read", "usage.cache_read", 600),
            ],
        );
        event(
            &conn,
            "large",
            "opencode",
            "openai",
            "m",
            "unknown",
            20,
            &[
                ("input", "usage.input", 401),
                ("cache_read", "usage.cache_read", 600),
            ],
        );
        event(
            &conn,
            "bundled",
            "broca",
            "openai",
            "m",
            "unknown",
            30,
            &[("input", "usage.input", 5_000)],
        );
        let observation = storeable_usage(&conn, (0, 200), &rates, 200);
        let by_producer: BTreeMap<&str, &ModelLine> = observation
            .api_nanos_by_model
            .iter()
            .map(|line| (line.producer.as_str(), line))
            .collect();
        // 1,000 at base + 1,001 at double rate.
        assert_eq!(by_producer["opencode"].api_nanos, 10_000_000 + 20_020_000);
        assert_eq!(by_producer["opencode"].tiered_tokens, 1_001);
        assert_eq!(by_producer["broca"].api_nanos, 50_000_000);
        assert_eq!(by_producer["broca"].base_rate_tier_untested_tokens, 5_000);
        assert_eq!(observation.base_rate_tier_untested_tokens, 5_000);
    }

    /// A lifetime-split cache write is prompt: it carries a per-request
    /// event past the threshold though it is not itself priced. A prompt
    /// measurement without a quantity leaves the prompt unknown, so the
    /// event is counted as `rate_tiered` rather than priced at either rate.
    #[test]
    fn split_cache_writes_count_toward_the_prompt_and_an_unknown_prompt_is_not_priced() {
        let conn = conn();
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        let rates = rates_for(
            &[("anthropic", "m")],
            TEN_PER_M,
            Some((1_000, 2 * TEN_PER_M)),
        );
        event(
            &conn,
            "split",
            "claude-code",
            "anthropic",
            "m",
            "unknown",
            10,
            &[("input", "usage.input_tokens", 400)],
        );
        conn.execute(
            "INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units,
                 quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id='split'), 'cache_write', 'token_cache_1h', 'billed',
                 'usage.cache_creation.ephemeral_1h_input_tokens', 700, 0)",
            [],
        )
        .unwrap();
        event(
            &conn,
            "unknown",
            "claude-code",
            "anthropic",
            "m",
            "unknown",
            20,
            &[("input", "usage.input_tokens", 50)],
        );
        conn.execute(
            "INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units,
                 quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id='unknown'), 'cache_read', 'token', 'billed',
                 'usage.cache_read_input_tokens', NULL, NULL)",
            [],
        )
        .unwrap();
        let observation = storeable_usage(&conn, (0, 200), &rates, 200);
        // 400 input at the tier rate; the unknown-prompt event adds nothing.
        assert_eq!(observation.api_nanos, 8_000_000);
        assert_eq!(observation.api_nanos_by_model[0].tiered_tokens, 1_100);
        let unpriced: Vec<_> = observation
            .unpriced_tokens
            .iter()
            .map(|entry| (entry.reason.as_str(), entry.role.as_str(), entry.tokens))
            .collect();
        assert_eq!(
            unpriced,
            [
                ("missing_quantity", "cache_read", 0),
                ("no_rate", "cache_write", 700),
                ("rate_tiered", "input", 50),
            ]
        );
    }

    /// Base rates at `TEN_PER_M` with `rate.<role>.mode.fast` at twice that,
    /// decoded from served catalog facts the way the daily stage decodes them.
    fn fast_mode_rates(model: &str) -> ResolvedRates {
        let rate = |units: i64| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
        let mut facts = BTreeMap::new();
        for role in ["input", "cache_read", "output"] {
            facts.insert(format!("rate.{role}"), rate(TEN_PER_M));
            facts.insert(format!("rate.{role}.mode.fast"), rate(2 * TEN_PER_M));
        }
        let snapshot = CatalogSnapshot {
            alias_target: None,
            source: "models.dev".into(),
            resolved_at_ms: 42,
            model_key: format!("openai/{model}"),
            facts,
            withheld_facts: BTreeMap::new(),
            uncertain_facts: BTreeMap::new(),
            fact_overrides: BTreeMap::new(),
        };
        ResolvedRates {
            resolved_at_ms: 42,
            models: BTreeMap::from([(
                ("openai".to_string(), model.to_string()),
                rates_from_snapshot(&snapshot),
            )]),
            eras: BTreeMap::new(),
        }
    }

    fn value_one(rates: &ResolvedRates, mode: Option<&str>) -> (ModelLine, Vec<String>) {
        let event = EventTokens {
            producer: "claude-code".into(),
            provider: "openai".into(),
            model: Some("m".into()),
            mode: mode.map(str::to_string),
            broca_attempt: false,
            roles: vec![
                (
                    "input".into(),
                    "token".into(),
                    "usage.input".into(),
                    Some(100),
                    Some(0),
                ),
                (
                    "output".into(),
                    "token".into(),
                    "usage.output".into(),
                    Some(50),
                    Some(0),
                ),
            ],
        };
        let mut lines = BTreeMap::new();
        let mut unpriced = BTreeMap::new();
        let mut totals = RoleTokens::default();
        value_event(
            ModelLookup::new(rates).get("openai", "m"),
            &event,
            &mut lines,
            &mut unpriced,
            &mut totals,
        )
        .unwrap();
        let line = lines.into_values().next().unwrap();
        let reasons = unpriced
            .into_keys()
            .map(|(reason, _, role)| format!("{reason}/{role}"))
            .collect();
        (line, reasons)
    }

    #[test]
    #[ignore = "wall-time probe; run with --ignored --nocapture --test-threads=1"]
    fn performance_repeated_event_valuation() {
        let rates = fast_mode_rates("m");
        let event = EventTokens {
            producer: "claude-code".into(),
            provider: "openai".into(),
            model: Some("m".into()),
            mode: None,
            broca_attempt: false,
            roles: vec![
                (
                    "input".into(),
                    "token".into(),
                    "usage.input".into(),
                    Some(100),
                    Some(0),
                ),
                (
                    "output".into(),
                    "token".into(),
                    "usage.output".into(),
                    Some(50),
                    Some(0),
                ),
            ],
        };
        let mut lines = BTreeMap::new();
        let mut unpriced = BTreeMap::new();
        let mut totals = RoleTokens::default();
        let lookup = ModelLookup::new(&rates);
        let start = std::time::Instant::now();
        let allocations = crate::canon::allocation_probe::count(|| {
            for _ in 0..100_000 {
                value_event(
                    lookup.get("openai", "m"),
                    &event,
                    &mut lines,
                    &mut unpriced,
                    &mut totals,
                )
                .unwrap();
            }
        });
        println!(
            "event valuation: 100000 two-role events on one model: {allocations} allocation/reallocation calls, {:?}",
            start.elapsed()
        );
        assert!(
            allocations <= 450_000,
            "priced events must not format labels or clone rate lookup keys: {allocations}"
        );
        assert_eq!(lines.values().next().unwrap().events, 100_000);
        assert_eq!(totals.input, 10_000_000);
        assert_eq!(totals.output, 5_000_000);
        assert!(unpriced.is_empty());
    }

    /// A request recorded as standard prices at the base rate; one recorded
    /// fast prices at the fast rate; one with no recorded mode prices at the
    /// base rate and its tokens are counted as `mode_unrecorded_tokens`.
    #[test]
    fn a_recorded_mode_selects_the_rate_and_an_unrecorded_one_is_counted_at_base() {
        let rates = fast_mode_rates("m");
        // 150 tokens at 10 or 20 USD per million, in nanos per million.
        let base_numerator = 150 * i128::from(TEN_PER_M);

        let (standard, reasons) = value_one(&rates, Some("standard"));
        assert_eq!(standard.numerator, base_numerator);
        assert_eq!(standard.mode_unrecorded_tokens, 0);
        assert!(reasons.is_empty(), "{reasons:?}");

        let (default, _) = value_one(&rates, Some("default"));
        assert_eq!(default.numerator, base_numerator);

        let (fast, reasons) = value_one(&rates, Some("fast"));
        assert_eq!(fast.numerator, 2 * base_numerator);
        assert_eq!(fast.mode_unrecorded_tokens, 0);
        assert!(reasons.is_empty(), "{reasons:?}");

        let (unrecorded, reasons) = value_one(&rates, None);
        assert_eq!(unrecorded.numerator, base_numerator);
        assert_eq!(unrecorded.mode_unrecorded_tokens, 150);
        assert!(reasons.is_empty(), "{reasons:?}");
    }

    /// A request recorded in a mode the model has no rate for is unpriced,
    /// never priced at the standard rate.
    #[test]
    fn a_recorded_mode_without_its_rate_is_unpriced() {
        let (line, reasons) = value_one(&fast_mode_rates("m"), Some("priority"));
        assert_eq!(line.numerator, 0);
        assert_eq!(reasons, vec!["no_rate/input", "no_rate/output"]);
    }

    /// A stored event carries no mode, so on a model serving a fast rate it
    /// prices at base and the row counts its tokens; on a model without one
    /// nothing is counted.
    #[test]
    fn stored_events_price_at_base_and_are_counted_only_where_a_mode_rate_exists() {
        let conn = conn();
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "e",
            "opencode",
            "openai",
            "m",
            "unknown",
            10,
            &[("input", "usage.input", 1_000)],
        );
        let observation = storeable_usage(&conn, (0, 200), &fast_mode_rates("m"), 200);
        assert_eq!(observation.api_nanos, 10_000_000);
        assert_eq!(
            observation.api_nanos_by_model[0].mode_unrecorded_tokens,
            1_000
        );
        let plain = storeable_usage(
            &conn,
            (0, 200),
            &rates_for(&[("openai", "m")], TEN_PER_M, None),
            200,
        );
        assert_eq!(plain.api_nanos, 10_000_000);
        assert_eq!(plain.api_nanos_by_model[0].mode_unrecorded_tokens, 0);
    }

    /// A conditional rate whose selector family no producer records never
    /// applies: the model still resolves and the base rate prices the request.
    #[test]
    fn an_unknown_selector_never_applies_and_the_base_prices() {
        let rate = |units: i64| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
        let snapshot = CatalogSnapshot {
            alias_target: None,
            source: "models.dev".into(),
            resolved_at_ms: 42,
            model_key: "openai/m".into(),
            facts: BTreeMap::from([
                ("rate.input".to_string(), rate(TEN_PER_M)),
                ("rate.input.region.eu".to_string(), rate(3 * TEN_PER_M)),
                ("rate.output".to_string(), rate(TEN_PER_M)),
            ]),
            withheld_facts: BTreeMap::new(),
            uncertain_facts: BTreeMap::new(),
            fact_overrides: BTreeMap::new(),
        };
        let resolution = rates_from_snapshot(&snapshot);
        assert!(
            matches!(resolution, ModelResolution::Rates(_)),
            "{resolution:?}"
        );
        let rates = ResolvedRates {
            resolved_at_ms: 42,
            models: BTreeMap::from([(("openai".to_string(), "m".to_string()), resolution)]),
            eras: BTreeMap::new(),
        };
        for mode in [None, Some("standard")] {
            let (line, reasons) = value_one(&rates, mode);
            assert_eq!(line.numerator, 150 * i128::from(TEN_PER_M), "{mode:?}");
            assert!(reasons.is_empty(), "{reasons:?}");
            // No mode rate is served, so an unrecorded mode is not a gap.
            assert_eq!(line.mode_unrecorded_tokens, 0);
        }
    }

    /// A served rate value astrocyte cannot decode fails the model's
    /// resolution, like an unreadable key, whatever condition it sits under.
    #[test]
    fn an_undecodable_rate_value_fails_the_model() {
        let snapshot = CatalogSnapshot {
            alias_target: None,
            source: "models.dev".into(),
            resolved_at_ms: 42,
            model_key: "openai/m".into(),
            facts: BTreeMap::from([(
                "rate.input.mode.fast".to_string(),
                json!({"state":"future_variant"}),
            )]),
            withheld_facts: BTreeMap::new(),
            uncertain_facts: BTreeMap::new(),
            fact_overrides: BTreeMap::new(),
        };
        assert_eq!(
            rates_from_snapshot(&snapshot),
            ModelResolution::Unpriceable {
                reason: RATE_UNDECODABLE.into()
            }
        );
    }

    #[test]
    fn billing_plane_values_are_not_ordinary_multiplier_reference_rates() {
        for entry in [
            json!({"state": "billed_as", "key": "rate.output"}),
            json!({"state": "unpriced", "reason": "not_established"}),
        ] {
            let snapshot = CatalogSnapshot {
                alias_target: None,
                source: "models.dev".into(),
                resolved_at_ms: 42,
                model_key: "openai/m".into(),
                facts: BTreeMap::from([("rate.input".into(), entry)]),
                withheld_facts: BTreeMap::new(),
                uncertain_facts: BTreeMap::new(),
                fact_overrides: BTreeMap::new(),
            };
            assert_eq!(
                rates_from_snapshot(&snapshot),
                ModelResolution::Unpriceable {
                    reason: RATE_UNDECODABLE.into(),
                }
            );
        }
    }

    fn sample_observation(conn: &Connection) -> Observation {
        fixture_vault_plan(conn, "openai/pro_20x");
        let pool = pools::PlannedPool {
            quota_provider: "codex",
            plan: "openai/pro_20x",
            pool_id: "primary",
            window_pool_key: "primary",
            pool_scope: PoolScope::All,
            catalog_provider: None,
            pool_models: None,
            pool_models_at_ms: None,
            window_minutes: 10080,
            diagnostic: None,
            reading_runs: Vec::new(),
        };
        let mut row = pools::empty_row(&pool, "1970-01-02", (0, DAY), DAY, DAY);
        row.plan = None;
        row.pool_scope = None;
        row.scope_rules = vec![
            json!({"producer":"opencode","provider":"openai","charge_basis":null,"model_prefix":null}),
        ];
        row
    }

    fn fixture_vault_plan(conn: &Connection, plan: &str) {
        let (provider, tier) = plan.split_once('/').unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        crate::vault_plan::append_in_tx(
            &tx,
            &crate::vault_plan::VaultPlan {
                credential_id: "fixture-acct".into(),
                provider: Some(provider.into()),
                account_id: Some("acct".into()),
                plan_tier: Some(tier.into()),
                plan_key: Some(plan.into()),
                source: "detected".into(),
                effective_from_ms: 0,
                observed_at_ms: 0,
            },
        )
        .unwrap();
        tx.commit().unwrap();
    }

    fn test_calls(conn: &Connection, start: i64, end: i64) -> Vec<(EventTokens, i64)> {
        let mut calls = Vec::new();
        for_events(
            conn,
            "1=1",
            &[SqlValue::Integer(start), SqlValue::Integer(end)],
            |call, _| {
                calls.push((call.tokens, call.occurred_at_ms));
                Ok(())
            },
        )
        .unwrap();
        calls
    }

    fn test_models(conn: &Connection, start: i64, end: i64) -> BTreeSet<(String, String)> {
        test_calls(conn, start, end)
            .into_iter()
            .filter_map(|(event, _)| Some((event.provider, event.model?)))
            .collect()
    }

    fn rates_with_reasoning_as_output() -> ResolvedRates {
        let mut rates = rates_for(&[("openai", "m")], TEN_PER_M, None);
        let Some(ModelResolution::Rates(model)) =
            rates.models.get_mut(&("openai".into(), "m".into()))
        else {
            unreachable!()
        };
        model.base.insert(
            "reasoning".into(),
            RateState::BilledAs {
                key: BillingRateKey::Output,
            },
        );
        rates
    }

    fn measured_usage(
        calls: &[(EventTokens, i64)],
        interval: (i64, i64),
        rates: &ResolvedRates,
    ) -> Result<measurement::UsageTotals, MultiplierError> {
        measurement::measure_usage(
            calls.iter().map(|(tokens, at)| (tokens, *at)),
            Some(interval),
            &ModelLookup::new(rates),
        )
    }

    fn storeable_usage(
        conn: &Connection,
        (start, end): (i64, i64),
        rates: &ResolvedRates,
        snapshot: i64,
    ) -> Observation {
        let calls = test_calls(conn, start, end);
        let usage = measured_usage(&calls, (start, end), rates).unwrap();
        let mut row = sample_observation(conn);
        row.interval_start_ms = start;
        row.interval_end_ms = end;
        row.token_interval_start_ms = Some(start);
        row.token_interval_end_ms = Some(end);
        row.observation_day = utc_day(end).unwrap();
        row.snapshot_at_ms = snapshot;
        row.catalog_resolved_at_ms = rates.resolved_at_ms;
        row.multiplier = usage.ratio(1);
        row.fee_multiplier = usage.ratio(1);
        row.tokens = usage.tokens;
        row.api_nanos = usage.api_nanos;
        row.api_nanos_by_model = usage.api_nanos_by_model;
        row.unpriced_tokens = usage.unpriced_tokens;
        row.tokens_outside_quota_coverage = usage.tokens_outside_quota_coverage;
        row.base_rate_tier_untested_tokens = usage.base_rate_tier_untested_tokens;
        row
    }

    fn pace_ready(mut observation: Observation) -> Observation {
        observation.pool_scope = Some(PoolScope::All);
        observation
            .plan
            .get_or_insert_with(|| "openai/pro_20x".into());
        observation.routing_eligible = true;
        observation
    }

    fn pace_head(conn: &Connection) -> Observation {
        let mut head = sample_observation(conn);
        head.observation_day = "1970-01-01".into();
        head.interval_start_ms = 3 * HOUR_MS - 7 * DAY;
        head.interval_end_ms = 3 * HOUR_MS;
        head.token_interval_start_ms = Some(0);
        head.token_interval_end_ms = Some(3 * HOUR_MS);
        head.pool_scope = Some(PoolScope::All);
        head.plan = Some("openai/pro_20x".into());
        head.routing_eligible = true;
        head.api_nanos = 100_000_000;
        head.delta_percent = 10.0;
        head.api_nanos_by_model = [
            (
                "opencode",
                "large",
                TEN_PER_M,
                Some((100_000, 2 * TEN_PER_M)),
            ),
            ("broca", "small", 2_000_000_000, None),
        ]
        .into_iter()
        .map(|(producer, model, base, tier)| ModelLine {
            producer: producer.into(),
            provider: "openai".into(),
            model: Some(model.into()),
            rates: rates_for(&[("openai", model)], base, tier)
                .models
                .remove(&("openai".into(), model.into())),
            ..ModelLine::default()
        })
        .collect();
        for line in &mut head.api_nanos_by_model {
            if let Some(ModelResolution::Rates(rates)) = &mut line.rates {
                rates.base.insert(
                    "reasoning".into(),
                    RateState::BilledAs {
                        key: BillingRateKey::Output,
                    },
                );
            }
        }
        head
    }

    fn insert_pace_observation(conn: &mut Connection, head: &Observation) {
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, head).unwrap();
        tx.commit().unwrap();
    }

    fn pace_fixture(conn: &mut Connection) -> Observation {
        let head = pace_head(conn);
        insert_pace_observation(conn, &head);
        head
    }

    #[test]
    fn pace_prices_stored_rates_per_hour_producer_and_work_class() {
        let mut conn = conn();
        pace_fixture(&mut conn);
        account_event(
            &conn,
            "long",
            "opencode",
            "openai",
            "large",
            "subscription_included",
            100,
            &[
                ("input", "usage", 200_000),
                ("output", "usage", 20),
                ("reasoning", "usage", 10),
            ],
        );
        // Pace uses only the stored coverage and rates, never live declarations.
        conn.execute("INSERT INTO usage_event (event_id, producer, producer_lineage, producer_ref,
            occurred_at_ms, recorded_at_ms, provider, model, charge_basis, work_class, account_ref)
            VALUES ('worker', 'broca', 'lineage', 'worker', ?1, ?1, 'openai', 'small', 'subscription_included', 'mason', 'acct')", [2 * HOUR_MS + 10]).unwrap();
        for (role, unit, source, quantity) in [
            ("input", "token", "usage", 1000),
            ("output", "token", "usage.output_tokens", 100),
            ("reasoning", "token", "usage.reasoning_tokens", 40),
            ("cache_write", "token_cache_5m", "usage", 7),
        ] {
            conn.execute("INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units, quantity_exponent)
                VALUES ((SELECT event_key FROM usage_event WHERE event_id='worker'), ?1, ?2, 'billed', ?3, ?4, 0)", params![role, unit, source, quantity]).unwrap();
        }
        account_event(
            &conn,
            "unknown-model",
            "opencode",
            "openai",
            "absent",
            "subscription_included",
            2 * HOUR_MS + 20,
            &[("input", "usage", 17)],
        );
        account_event(
            &conn,
            "short",
            "opencode",
            "openai",
            "large",
            "subscription_included",
            3 * HOUR_MS + 10,
            &[("input", "usage", 100), ("output", "usage", 10)],
        );
        for (id, producer, provider, basis, at) in [
            ("wrong-provider", "opencode", "other", "unknown", HOUR_MS),
            ("wrong-basis", "broca", "openai", "metered_api", HOUR_MS),
            ("after", "opencode", "openai", "unknown", 4 * HOUR_MS),
            ("before", "opencode", "openai", "unknown", -1),
        ] {
            event(
                &conn,
                id,
                producer,
                provider,
                "large",
                basis,
                at,
                &[("input", "usage", 999_999)],
            );
        }
        conn.execute_batch("INSERT INTO usage_event (event_id, producer, producer_lineage, producer_ref, recorded_at_ms, provider, model, charge_basis)
            VALUES ('undated', 'opencode', 'lineage', 'undated', 1, 'openai', 'large', 'unknown');
            INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units, quantity_exponent)
            VALUES ((SELECT event_key FROM usage_event WHERE event_id='undated'), 'input', 'token', 'billed', 'usage', 12, 0), ((SELECT event_key FROM usage_event WHERE event_id='undated'), 'output', 'token', 'billed', 'usage', 3, 0);").unwrap();
        let report = multiplier_pace(
            &conn,
            &MultiplierPaceRequest {
                pool: None,
                plan: None,
                quota_provider: "codex".into(),
                from_ms: 1,
                to_ms: 3 * HOUR_MS + 2,
            },
        )
        .unwrap();
        // Hand valuation: long = (200000 + 20 + 10) * 20000 nanos;
        // worker = (1000 + (100 - 40) + 40) * 2000; short = 110 * 10000.
        // The stored pooled rate is 100000000 / 10 = 10000000 nanos/point.
        let expected = json!({"quota_provider": "codex", "plan": "openai/pro_20x", "pool_id": "primary", "from_ms": 0, "to_ms": 14400000,
        "basis": "pro_rata_list_value", "routing_eligible": true, "per_point_nanos": 10000000,
        "observation_day": "1970-01-01", "observation_id": 1, "undated_events": 0,
        "hours": [
            {"hour_start_ms": 0, "producers": {"opencode": {"events": 1, "list_nanos": 4000600000_i64, "estimated_points": 400.06, "unpriced_tokens": 0,
                "by_work_class": {"unlabelled": {"events": 1, "list_nanos": 4000600000_i64, "estimated_points": 400.06}}}}},
            {"hour_start_ms": 3600000, "producers": {}},
            {"hour_start_ms": 7200000, "producers": {
                "broca": {"events": 1, "list_nanos": 2200000, "estimated_points": 0.22, "unpriced_tokens": 7,
                    "by_work_class": {"mason": {"events": 1, "list_nanos": 2200000, "estimated_points": 0.22}}},
                "opencode": {"events": 1, "list_nanos": 0, "estimated_points": 0.0, "unpriced_tokens": 17,
                    "by_work_class": {"unlabelled": {"events": 1, "list_nanos": 0, "estimated_points": 0.0}}}}},
            {"hour_start_ms": 10800000, "producers": {"opencode": {"events": 1, "list_nanos": 1100000, "estimated_points": 0.11, "unpriced_tokens": 0,
                "by_work_class": {"unlabelled": {"events": 1, "list_nanos": 1100000, "estimated_points": 0.11}}}}}
        ]});
        assert_eq!(report, expected);
        println!("pace example response: {report}");
        // Consumers pin synthetic replies in tests/vectors. The comparison
        // below checks every member those pre-pool samples record.
        assert_eq!(report["pool_id"], "primary");
        // The pinned samples predate pool identity, so project out only that new member.
        let mut served_shape = json_shape(&report);
        served_shape.remove(".pool_id");
        for (name, bytes) in [
            (
                "claude",
                include_str!(
                    "../tests/vectors/spend-multiplier-pace/claude-2026-10-09T08Z-24h.json"
                ),
            ),
            (
                "codex",
                include_str!(
                    "../tests/vectors/spend-multiplier-pace/codex-2026-10-09T08Z-24h.json"
                ),
            ),
        ] {
            let vector: Value = serde_json::from_str(bytes).unwrap();
            assert_eq!(
                json_shape(&vector["result"]),
                served_shape,
                "{name} vector shape differs from the served reply"
            );
        }
    }

    /// Every key path in a pace reply, with the map keys that name producers or
    /// work classes, and array positions, folded to `*`, so two replies with
    /// different data but the same layout compare equal.
    fn json_shape(value: &Value) -> BTreeSet<String> {
        fn walk(value: &Value, path: String, folded: bool, out: &mut BTreeSet<String>) {
            out.insert(path.clone());
            match value {
                Value::Object(map) => {
                    for (key, child) in map {
                        let segment = if folded { "*" } else { key.as_str() };
                        let fold = key == "producers" || key == "by_work_class";
                        walk(child, format!("{path}.{segment}"), fold && !folded, out);
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        walk(item, format!("{path}[*]"), false, out);
                    }
                }
                _ => {}
            }
        }
        let mut out = BTreeSet::new();
        walk(value, String::new(), false, &mut out);
        out
    }

    #[test]
    fn pace_uses_the_latest_day_head_not_a_superseded_row() {
        let mut conn = conn();
        let mut head = pace_fixture(&mut conn);
        head.api_nanos = 200_000_000;
        let tx = conn.transaction().unwrap();
        let recompute = insert_superseding_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        let request = MultiplierPaceRequest {
            pool: None,
            plan: None,
            quota_provider: "codex".into(),
            from_ms: 0,
            to_ms: 3 * HOUR_MS,
        };
        let report = multiplier_pace(&conn, &request).unwrap();
        assert_eq!(report["per_point_nanos"], 20_000_000);
        assert_eq!(report["observation_id"], recompute.observation_id);
        // A later snapshot correcting an older day must not become latest.
        head.observation_day = "1969-12-31".into();
        head.interval_end_ms = -DAY;
        head.interval_start_ms = -8 * DAY;
        // The token interval moves with the observation interval: it is that
        // interval clipped to quota coverage, and the insert refuses a token
        // interval lying outside the observation interval.
        head.token_interval_start_ms = Some(-2 * DAY);
        head.token_interval_end_ms = Some(-DAY);
        head.snapshot_at_ms = 10 * DAY;
        head.api_nanos = 500_000_000;
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        assert_eq!(multiplier_pace(&conn, &request).unwrap(), report);
    }

    #[test]
    fn pace_refuses_unknown_provider_invalid_or_excessive_range_and_missing_head() {
        let mut conn = conn();
        pace_fixture(&mut conn);
        for (provider, from_ms, to_ms, code) in [
            ("absent", 0, 1, "unknown_quota_provider"),
            ("codex", 1, 1, "invalid_range"),
            ("codex", 2, 1, "invalid_range"),
            ("codex", 0, 192 * HOUR_MS + 1, "range_too_large"),
            ("codex", 1, 192 * HOUR_MS + 1, "range_too_large"),
            ("codex", 0, 1, "no_multiplier_observation"),
            ("codex", i64::MAX - 1, i64::MAX, "invalid_range"),
            ("codex", i64::MIN, i64::MIN + 1, "invalid_range"),
        ] {
            let error = multiplier_pace(
                &conn,
                &MultiplierPaceRequest {
                    pool: None,
                    plan: None,
                    quota_provider: provider.into(),
                    from_ms,
                    to_ms,
                },
            )
            .unwrap_err();
            assert_eq!(error.code(), code);
        }
        assert!(serde_json::from_value::<MultiplierPaceRequest>(
            json!({"quota_provider": "codex", "from_ms": 0, "to_ms": 1, "extra": true})
        )
        .is_err());
    }

    #[test]
    fn pace_aligns_negative_hours_and_keeps_an_unusable_denominator_null() {
        let mut conn = conn();
        let mut head = pace_fixture(&mut conn);
        head.delta_percent = 0.0;
        let tx = conn.transaction().unwrap();
        insert_superseding_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        account_event(
            &conn,
            "negative",
            "opencode",
            "openai",
            "large",
            "subscription_included",
            0,
            &[("input", "usage", 10)],
        );
        let report = multiplier_pace(
            &conn,
            &MultiplierPaceRequest {
                pool: None,
                plan: None,
                quota_provider: "codex".into(),
                from_ms: -1,
                to_ms: 3 * HOUR_MS,
            },
        )
        .unwrap();
        assert_eq!(report["from_ms"], -HOUR_MS);
        assert_eq!(report["to_ms"], 3 * HOUR_MS);
        assert_eq!(report["hours"].as_array().unwrap().len(), 4);
        assert_eq!(report["per_point_nanos"], Value::Null);
        assert_eq!(
            report["hours"][1]["producers"]["opencode"]["list_nanos"],
            100_000
        );
        assert_eq!(
            report["hours"][1]["producers"]["opencode"]["estimated_points"],
            Value::Null
        );
    }

    /// One row per provider per day: the insert refuses a second, the table's
    /// unique key refuses one written around it, and rows are append-only.
    #[test]
    fn no_two_original_observations_for_one_provider_plan_and_day() {
        let mut conn = conn();
        let observation = sample_observation(&conn);
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &observation).unwrap();
        assert!(matches!(
            insert_observation_in_tx(&tx, &observation),
            Err(MultiplierError::AlreadyRecorded { .. })
        ));
        tx.commit().unwrap();
        let duplicate = conn.execute(
            "INSERT INTO multiplier_observation SELECT * FROM multiplier_observation",
            [],
        );
        assert!(
            duplicate.is_err(),
            "the unique key must refuse a second row"
        );
        let unique_day = conn.execute(
            "INSERT INTO multiplier_observation (plan, quota_provider, observation_day, snapshot_at_ms,
                 interval_start_ms, interval_end_ms, window_pool_key, window_minutes, input_tokens,
                 cache_read_tokens, cache_write_tokens, output_tokens, reasoning_tokens, api_nanos,
                 api_nanos_by_model, unpriced_tokens, tokens_outside_quota_coverage,
                 base_rate_tier_untested_tokens, subscription_nanos, fee_paid_nanos, delta_percent,
                 accounts_included_count, accounts_included, accounts_excluded, excluded_pairs,
                 low_resolution, scope_rules, plan_prices, catalog_resolved_at_ms, api_twins,
                 reasoning_billed_as_output, error_deflating, error_inflating)
             SELECT plan, quota_provider, observation_day, snapshot_at_ms + 1, interval_start_ms,
                 interval_end_ms, window_pool_key, window_minutes, input_tokens, cache_read_tokens,
                 cache_write_tokens, output_tokens, reasoning_tokens, api_nanos, api_nanos_by_model,
                 unpriced_tokens, tokens_outside_quota_coverage, base_rate_tier_untested_tokens,
                 subscription_nanos, fee_paid_nanos, delta_percent, accounts_included_count,
                 accounts_included, accounts_excluded, excluded_pairs, low_resolution, scope_rules,
                 plan_prices, catalog_resolved_at_ms, api_twins, reasoning_billed_as_output,
                 error_deflating, error_inflating FROM multiplier_observation",
            [],
        );
        assert!(
            unique_day.is_err(),
            "a later snapshot of the same day is still refused"
        );
        assert!(conn
            .execute("UPDATE multiplier_observation SET multiplier = 1", [])
            .is_err());
        assert!(conn
            .execute("DELETE FROM multiplier_observation", [])
            .is_err());
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1);
    }

    /// The derived day of the first and last instants chrono can represent,
    /// and a refusal one millisecond past either end. Years past 9999 print
    /// with a sign; the strings are written out rather than formatted here so
    /// the expectation does not come from the code under test.
    #[test]
    fn utc_day_names_the_representable_extremes_and_refuses_past_them() {
        let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_millis();
        let min = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_millis();
        assert_eq!(utc_day(0).unwrap(), "1970-01-01");
        assert_eq!(utc_day(-1).unwrap(), "1969-12-31");
        assert_eq!(utc_day(max).unwrap(), "+262142-12-31");
        assert_eq!(utc_day(min).unwrap(), "-262143-01-01");
        for outside in [max + 1, min - 1, i64::MAX, i64::MIN] {
            let error = utc_day(outside).unwrap_err();
            assert_eq!(error.code(), "invalid_range", "{outside}");
        }
    }

    /// Builds the observation the daily stage would derive, applies `mutate`,
    /// and asserts the insert refuses it as invalid with no row written. The
    /// variant is asserted, not just an error, because the store's own
    /// triggers refuse some of these shapes too and would pass a weaker check.
    fn assert_observation_refused(shape: &str, mutate: impl FnOnce(&mut Observation)) {
        let mut conn = conn();
        let mut observation = sample_observation(&conn);
        assert!(
            observation.token_interval_start_ms.is_some()
                && observation.token_interval_end_ms.is_some(),
            "the fixture must carry a token interval for the pairing cases to mean anything"
        );
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &observation)
            .unwrap_or_else(|error| panic!("{shape}: the unaltered shape is refused: {error:?}"));
        tx.rollback().unwrap();
        mutate(&mut observation);
        let tx = conn.transaction().unwrap();
        let result = insert_observation_in_tx(&tx, &observation);
        assert!(
            matches!(&result, Err(MultiplierError::InvalidObservation(_))),
            "{shape}: {result:?}"
        );
        assert_eq!(result.unwrap_err().code(), "internal", "{shape}");
        let rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0, "{shape}: nothing is written");
    }

    #[test]
    fn an_observation_with_half_a_token_interval_is_refused_before_insert() {
        assert_observation_refused("start only", |observation| {
            observation.token_interval_end_ms = None;
        });
        assert_observation_refused("end only", |observation| {
            observation.token_interval_start_ms = None;
        });
    }

    #[test]
    fn an_observation_with_an_empty_or_reversed_token_interval_is_refused_before_insert() {
        assert_observation_refused("empty", |observation| {
            observation.token_interval_start_ms = Some(10);
            observation.token_interval_end_ms = Some(10);
        });
        assert_observation_refused("reversed", |observation| {
            observation.token_interval_start_ms = Some(20);
            observation.token_interval_end_ms = Some(10);
        });
    }

    #[test]
    fn an_observation_with_a_token_interval_outside_its_interval_is_refused_before_insert() {
        assert_observation_refused("starts early", |observation| {
            observation.token_interval_start_ms = Some(observation.interval_start_ms - 1);
        });
        assert_observation_refused("ends late", |observation| {
            observation.token_interval_end_ms = Some(observation.interval_end_ms + 1);
        });
    }

    #[test]
    fn an_observation_with_an_unordered_interval_is_refused_before_insert() {
        assert_observation_refused("start after end", |observation| {
            observation.interval_start_ms = observation.interval_end_ms + 1;
            observation.token_interval_start_ms = None;
            observation.token_interval_end_ms = None;
        });
    }

    #[test]
    fn an_observation_with_a_non_positive_window_is_refused_before_insert() {
        assert_observation_refused("zero window", |observation| {
            observation.window_minutes = 0;
        });
        assert_observation_refused("negative window", |observation| {
            observation.window_minutes = -1;
        });
    }

    #[test]
    fn an_observation_with_a_pre_epoch_recording_instant_is_refused_before_insert() {
        assert_observation_refused("snapshot", |observation| {
            observation.snapshot_at_ms = -1;
        });
        assert_observation_refused("catalog", |observation| {
            observation.catalog_resolved_at_ms = -1;
        });
    }

    #[test]
    fn an_observation_filed_under_another_day_is_refused_before_insert() {
        assert_observation_refused("next day", |observation| {
            observation.observation_day = "1970-01-03".into();
        });
    }

    /// A recompute's superseding row passes the same pre-insert validation
    /// as the daily stage's original row.
    #[test]
    fn a_superseding_observation_is_validated_before_insert() {
        let mut conn = conn();
        let observation = sample_observation(&conn);
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &observation).unwrap();
        let mut broken = observation.clone();
        broken.token_interval_end_ms = None;
        assert!(matches!(
            insert_superseding_observation_in_tx(&tx, &broken),
            Err(MultiplierError::InvalidObservation(_))
        ));
        let rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1);
    }
    #[test]
    fn the_report_serves_latest_and_series_and_refuses_a_malformed_range() {
        let mut conn = conn();
        let observation = sample_observation(&conn);
        let mut scoped = observation.clone();
        scoped.plan = Some("openai/pro_200".into());
        scoped.pool_scope = Some(PoolScope::All);
        scoped.scope_rules.clear();
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &observation).unwrap();
        record_one(&tx, &scoped);
        tx.commit().unwrap();
        let report = multiplier_report(
            &conn,
            &MultiplierReportRequest {
                from_ms: 0,
                to_ms: 2 * DAY,
            },
        )
        .unwrap();
        assert_eq!(report["latest"][0]["quota_provider"], "codex");
        assert_eq!(
            report["latest"][0]["low_resolution"],
            json!(observation.low_resolution)
        );
        let series = report["series"].as_array().unwrap();
        assert_eq!(series.len(), 2);
        let legacy = series
            .iter()
            .find(|row| row["pool_scope"].is_null())
            .unwrap();
        assert_eq!(legacy["scope_rules"][0]["producer"], "opencode");
        let empty = multiplier_report(
            &conn,
            &MultiplierReportRequest {
                from_ms: 2 * DAY,
                to_ms: 3 * DAY,
            },
        )
        .unwrap();
        assert_eq!(empty["series"], json!([]));
        assert_eq!(
            empty["latest"].as_array().unwrap().len(),
            1,
            "latest ignores the range"
        );
        for (from_ms, to_ms) in [(10, 10), (20, 10), (-1, 10)] {
            let error =
                multiplier_report(&conn, &MultiplierReportRequest { from_ms, to_ms }).unwrap_err();
            assert_eq!(error.code(), "invalid_range", "{from_ms}..{to_ms}");
        }
    }

    fn token_catalog(extra_role: Option<&str>) -> CatalogClient {
        struct Catalog(BTreeMap<String, Value>);
        impl crate::catalog_client::CatalogConnection for Catalog {
            fn catalog_status(
                &self,
                _: fusiform_protocol::StatusRequest,
            ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
                unreachable!("the test client already has its history boundary")
            }

            fn catalog_get(
                &self,
                request: fusiform_protocol::CatalogGetRequest,
            ) -> Result<fusiform_protocol::CatalogGetResponse, CatalogCallError> {
                Ok(fusiform_protocol::CatalogGetResponse {
                    overrides: Vec::new(),
                    source: "models.dev".into(),
                    resolved_at_ms: request.at_ms.unwrap(),
                    catalog_version: 1,
                    models: BTreeMap::from([("google/gemini-3.8-flash".into(), self.0.clone())]),
                    withheld: vec![],
                    uncertain: vec![],
                    next_page_token: None,
                    curated: vec![],
                    quality_scores: None,
                    canonical_links: vec![],
                    wire_corrections: vec![],
                    fact_overrides: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
                    login_methods: vec![],
                    speed_twins: vec![],
                })
            }
        }
        let rate = |units| {
            json!({"state":"priced", "units":units, "exponent":9,
            "currency":"USD", "unit_provenance":{"kind":"stated"}})
        };
        let mut facts = BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000_i64)),
            ("rate.output".into(), rate(15_000_000_000_i64)),
        ]);
        if let Some(role) = extra_role {
            facts.insert(format!("rate.{role}"), rate(6_000_000_000));
        }
        CatalogClient::new(std::sync::Arc::new(Catalog(facts)), 0)
    }

    #[test]
    fn input_audio_catalog_rate_preserves_token_valuation_and_prices_audio() {
        let mut stored_rows = Vec::new();
        for (extra_role, audio_tokens) in [
            (None, 0),
            (Some("input_audio"), 0),
            (Some("input_audio"), 1_000_000),
        ] {
            let mut conn = conn();
            reading(&conn, "acct", 10, 0.0, None);
            reading(&conn, "acct", 90, 50.0, None);
            let mut measurements = vec![
                ("input", "usage.input", 1_000_000),
                ("output", "usage.output", 1_000_000),
            ];
            if audio_tokens > 0 {
                measurements.push(("input_audio", "usage.input_audio", audio_tokens));
            }
            event(
                &conn,
                "tokens",
                "opencode",
                "google",
                "gemini-3.8-flash",
                "metered_api",
                20,
                &measurements,
            );
            let models = test_models(&conn, 0, 100);
            let rates = resolve_rates(&token_catalog(extra_role), &models, 100).unwrap();
            let observation = storeable_usage(&conn, (0, 100), &rates, 100);
            assert!(observation.unpriced_tokens.is_empty());
            assert_eq!(observation.api_nanos_by_model.len(), 1);
            assert_eq!(
                observation.api_nanos_by_model[0].api_nanos,
                observation.api_nanos
            );
            let tx = conn.transaction().unwrap();
            insert_observation_in_tx(&tx, &observation).unwrap();
            tx.commit().unwrap();
            let stored: (i64, i64, i64, String) = conn.query_row(
                "SELECT api_nanos,input_tokens,output_tokens,unpriced_tokens FROM multiplier_observation",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            ).unwrap();
            assert_eq!(
                stored,
                (
                    18_000_000_000 + audio_tokens * 6_000,
                    1_000_000,
                    1_000_000,
                    "[]".into()
                )
            );
            stored_rows.push(stored);
        }
        assert_eq!(stored_rows[0], stored_rows[1]);
        assert_eq!(stored_rows[2].0, 24_000_000_000);
    }

    #[test]
    fn media_token_roles_without_measurements_preserve_multiplier_values() {
        let mut observations = Vec::new();
        for extra_role in [
            None,
            Some("input_image"),
            Some("output_image"),
            Some("output_video"),
        ] {
            let mut conn = conn();
            reading(&conn, "acct", 10, 0.0, None);
            reading(&conn, "acct", 90, 50.0, None);
            event(
                &conn,
                "tokens",
                "opencode",
                "google",
                "gemini-3.8-flash",
                "metered_api",
                20,
                &[
                    ("input", "usage.input", 1_000_000),
                    ("output", "usage.output", 1_000_000),
                ],
            );
            let models = test_models(&conn, 0, 100);
            let rates = resolve_rates(&token_catalog(extra_role), &models, 100).unwrap();
            let mut observation = storeable_usage(&conn, (0, 100), &rates, 100);
            let tx = conn.transaction().unwrap();
            insert_observation_in_tx(&tx, &observation).unwrap();
            tx.commit().unwrap();
            let stored: (i64, i64, i64, String) = conn.query_row(
                "SELECT api_nanos,input_tokens,output_tokens,unpriced_tokens FROM multiplier_observation",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            ).unwrap();
            assert_eq!(stored, (18_000_000_000, 1_000_000, 1_000_000, "[]".into()));
            assert!(observation.multiplier.is_some());
            assert!(observation.fee_multiplier.is_some());
            for line in &mut observation.api_nanos_by_model {
                // `rates` lists the catalog keys each model served, so it gains
                // the extra role's key; every valued amount must stay equal.
                line.rates = None;
            }
            observations.push(observation);
        }
        for observation in &observations[1..] {
            assert_eq!(&observations[0], observation);
        }
    }

    #[test]
    fn unknown_input_video_catalog_role_fails_the_day_without_rows() {
        let conn = conn();
        reading(&conn, "acct", 10, 0.0, None);
        reading(&conn, "acct", 90, 50.0, None);
        event(
            &conn,
            "tokens",
            "opencode",
            "google",
            "gemini-3.8-flash",
            "metered_api",
            20,
            &[
                ("input", "usage.input", 1_000_000),
                ("output", "usage.output", 1_000_000),
            ],
        );
        let models = test_models(&conn, 0, 100);
        let error = resolve_rates(&token_catalog(Some("input_video")), &models, 100).unwrap_err();
        assert_eq!(error.code(), "rate_fact_unrecognized");
        assert!(error.message().contains("rate.input_video"));
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
    }

    /// A rate key the decoder does not know fails the resolution (the day is
    /// not written); a model the catalog has no row for is a reason on a
    /// written row.
    #[test]
    fn an_unreadable_rate_fails_and_missing_coverage_is_a_reason() {
        use crate::catalog_client::CatalogConnection;
        use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
        struct Catalog;
        impl CatalogConnection for Catalog {
            fn catalog_status(
                &self,
                _: fusiform_protocol::StatusRequest,
            ) -> Result<fusiform_protocol::StatusResponse, crate::catalog_client::CatalogCallError>
            {
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
                let model = request.model_id.unwrap_or_default();
                let rate = json!({"state":"priced","units":1,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
                let conditional_key = match model.as_str() {
                    "fast" => "rate.input.above_context.272k",
                    "conditional" => "rate.input.mode.fast",
                    "regional" => "rate.input.region.eu",
                    _ => {
                        return Err(CatalogCallError {
                            code: "no_coverage".into(),
                            message: "no such model".into(),
                        })
                    }
                };
                Ok(CatalogGetResponse {
                    overrides: Vec::new(),
                    source: "models.dev".into(),
                    resolved_at_ms: 5,
                    catalog_version: 1,
                    models: BTreeMap::from([(
                        format!("openai/{model}"),
                        BTreeMap::from([
                            ("rate.input".to_string(), rate.clone()),
                            (conditional_key.to_string(), rate),
                        ]),
                    )]),
                    withheld: vec![],
                    uncertain: vec![],
                    next_page_token: None,
                    curated: vec![],
                    quality_scores: None,
                    canonical_links: vec![],
                    wire_corrections: vec![],
                    fact_overrides: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
                    login_methods: vec![],
                    speed_twins: vec![],
                })
            }
        }
        let catalog = CatalogClient::new(std::sync::Arc::new(Catalog), 0);
        let pair = |model: &str| ("openai".to_string(), model.to_string());
        let unreadable =
            resolve_rates(&catalog, &BTreeSet::from([pair("fast"), pair("gone")]), 5).unwrap_err();
        assert_eq!(unreadable.code(), "rate_fact_unrecognized");
        assert!(unreadable
            .message()
            .contains("rate.input.above_context.272k"));
        // Conditional rates of a known or an unknown selector family are
        // readable, so the day resolves.
        let conditional = resolve_rates(
            &catalog,
            &BTreeSet::from([pair("conditional"), pair("regional")]),
            5,
        )
        .unwrap();
        assert!(matches!(
            &conditional.models[&pair("conditional")],
            ModelResolution::Rates(rates) if rates.modes["fast"]["input"] == priced(1)
        ));
        assert!(matches!(
            &conditional.models[&pair("regional")],
            ModelResolution::Rates(rates) if rates.modes.is_empty() && rates.base["input"] == priced(1)
        ));
        let uncovered = resolve_rates(&catalog, &BTreeSet::from([pair("gone")]), 5).unwrap();
        assert_eq!(
            uncovered.models[&pair("gone")],
            ModelResolution::Unpriceable {
                reason: "no_catalog_coverage".into()
            }
        );
    }

    struct ReferenceCatalog {
        rows: BTreeMap<String, Value>,
        aliased: Vec<fusiform_protocol::AliasedModelWire>,
        empty_on_miss: BTreeSet<String>,
        current_only: BTreeSet<String>,
        requests: std::sync::Mutex<Vec<(String, Option<i64>)>>,
    }

    impl crate::catalog_client::CatalogConnection for ReferenceCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: fusiform_protocol::CatalogGetRequest,
        ) -> Result<fusiform_protocol::CatalogGetResponse, CatalogCallError> {
            let model = request.model_id.unwrap();
            self.requests
                .lock()
                .unwrap()
                .push((model.clone(), request.at_ms));
            let is_alias = self
                .aliased
                .iter()
                .any(|alias| alias.model == format!("google/{model}"))
                || self
                    .rows
                    .get(&model)
                    .is_some_and(|rate| rate["inherited_from"]["basis"] == "alias");
            if request.at_ms.is_some() && (is_alias || self.current_only.contains(&model)) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "aliases apply to current reads only".into(),
                });
            }
            let models = if let Some(rate) = self.rows.get(&model) {
                BTreeMap::from([(
                    format!("google/{model}"),
                    BTreeMap::from([("rate.input".into(), rate.clone())]),
                )])
            } else if self.empty_on_miss.contains(&model) {
                BTreeMap::new()
            } else {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "no such model".into(),
                });
            };
            Ok(fusiform_protocol::CatalogGetResponse {
                overrides: Vec::new(),
                source: "models.dev".into(),
                resolved_at_ms: 100,
                catalog_version: 1,
                models,
                withheld: vec![],
                uncertain: vec![],
                next_page_token: None,
                curated: vec![],
                quality_scores: None,
                canonical_links: vec![],
                wire_corrections: vec![],
                fact_overrides: vec![],
                retired: vec![],
                aliased: self.aliased.clone(),
                plane: None,
                billing_rules: vec![],
                login_methods: vec![],
                speed_twins: vec![],
            })
        }
    }

    fn reference_rate(units: i64) -> Value {
        json!({"state":"priced","units":units,"exponent":9,"currency":"USD",
            "unit_provenance":{"kind":"stated"}})
    }

    struct QuotaProxyCatalog {
        own_rates: BTreeMap<String, Value>,
        target_history_start_ms: i64,
        requests: std::sync::Mutex<Vec<fusiform_protocol::CatalogGetRequest>>,
    }

    impl crate::catalog_client::CatalogConnection for QuotaProxyCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            unreachable!("the test supplies the catalog history boundary")
        }

        fn catalog_get(
            &self,
            request: fusiform_protocol::CatalogGetRequest,
        ) -> Result<fusiform_protocol::CatalogGetResponse, CatalogCallError> {
            let provider = request.provider_id.as_deref().unwrap();
            let model = request.model_id.as_deref().unwrap();
            let rates = match (
                provider,
                model,
                request.at_ms,
                request.auth_method.as_deref(),
            ) {
                ("kimi-code-plan-global", "k3.1-pro", Some(_), None) => Ok(self.own_rates.clone()),
                // The alias lookup a missing row makes: no alias is served.
                ("kimi-code-plan-global", "k3.1-pro", None, None) => Ok(BTreeMap::new()),
                ("kimi-code-plan-global", "k3.1-pro", None, Some("apikey")) => {
                    // This current response identifies the model to query, but
                    // its prices must not be used. Make them unlike the history.
                    let mut rate = reference_rate(100_000_000_000);
                    rate["inherited_from"] = json!({"basis":"quota_proxy",
                        "provider_id":"moonshotai", "family":"kimi", "model_id":"kimi-k3.1-pro"});
                    Ok(BTreeMap::from([("rate.input".into(), rate)]))
                }
                ("moonshotai", "kimi-k3.1-pro", Some(at_ms), None)
                    if at_ms >= self.target_history_start_ms =>
                {
                    Ok(BTreeMap::from([
                        ("rate.input".into(), reference_rate(2_000_000_000)),
                        ("rate.output".into(), reference_rate(10_000_000_000)),
                        ("rate.cache_read".into(), reference_rate(200_000_000)),
                        ("rate.cache_write".into(), reference_rate(2_000_000_000)),
                    ]))
                }
                ("moonshotai", "kimi-k3.1-pro", Some(_), None) => Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "before the target's first rate era".into(),
                }),
                _ => panic!("unexpected catalog request: {request:?}"),
            };
            let model_key = format!("{provider}/{model}");
            self.requests.lock().unwrap().push(request);
            Ok(fusiform_protocol::CatalogGetResponse {
                overrides: Vec::new(),
                source: "models.dev".into(),
                resolved_at_ms: 999,
                catalog_version: 1,
                models: BTreeMap::from([(model_key, rates?)]),
                withheld: vec![],
                uncertain: vec![],
                next_page_token: None,
                curated: vec![],
                quality_scores: None,
                canonical_links: vec![],
                wire_corrections: vec![],
                fact_overrides: vec![],
                retired: vec![],
                aliased: vec![],
                plane: None,
                billing_rules: vec![],
                login_methods: vec![],
                speed_twins: vec![],
            })
        }
    }

    fn quota_proxy_card(state: Value) -> BTreeMap<String, Value> {
        ["input", "output", "cache_read", "cache_write"]
            .into_iter()
            .map(|role| (format!("rate.{role}"), state.clone()))
            .collect()
    }

    fn assert_quota_proxy_requests(connection: &QuotaProxyCatalog) {
        let requests = connection.requests.lock().unwrap();
        let calls: Vec<_> = requests
            .iter()
            .map(|request| {
                (
                    request.provider_id.as_deref().unwrap(),
                    request.model_id.as_deref().unwrap(),
                    request.at_ms,
                    request.auth_method.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            calls,
            vec![
                ("kimi-code-plan-global", "k3.1-pro", Some(100), None),
                ("kimi-code-plan-global", "k3.1-pro", None, Some("apikey")),
                ("moonshotai", "kimi-k3.1-pro", Some(100), None),
            ]
        );
    }

    #[test]
    fn quota_proxy_values_the_day_at_historical_target_rates() {
        for own_rates in [
            quota_proxy_card(json!({"state":"stated_zero"})),
            quota_proxy_card(json!({"state":"unpriced", "reason":"missing_rate"})),
            BTreeMap::from([("rate.input".into(), json!({"state":"stated_zero"}))]),
        ] {
            let mut conn = conn();
            let connection = std::sync::Arc::new(QuotaProxyCatalog {
                own_rates,
                target_history_start_ms: 100,
                requests: std::sync::Mutex::new(Vec::new()),
            });
            let catalog = CatalogClient::new(connection.clone(), 0);
            reading(&conn, "acct", 10, 0.0, None);
            reading(&conn, "acct", 90, 50.0, None);
            event(
                &conn,
                "plan-call",
                "broca",
                "kimi-code-plan-global",
                "k3.1-pro",
                "subscription_included",
                20,
                &[
                    ("input", "usage.input", 1_000),
                    ("output", "usage.output", 1_000),
                    ("cache_read", "usage.cache_read", 1_000),
                    ("cache_write", "usage.cache_write", 1_000),
                ],
            );
            let models = test_models(&conn, 0, 100);
            let rates = resolve_rates(&catalog, &models, 100).unwrap();
            let observation = storeable_usage(&conn, (0, 100), &rates, 100);
            assert_eq!(observation.api_nanos, 14_200_000);
            assert!(observation.unpriced_tokens.is_empty());
            assert!(observation.multiplier.is_some_and(|ratio| ratio > 0.0));
            assert_quota_proxy_requests(&connection);
            let tx = conn.transaction().unwrap();
            insert_observation_in_tx(&tx, &observation).unwrap();
            tx.commit().unwrap();
            let report = multiplier_report(
                &conn,
                &MultiplierReportRequest {
                    from_ms: 0,
                    to_ms: 200,
                },
            )
            .unwrap();
            assert_eq!(report["series"][0]["api_nanos"], 14_200_000);
            assert_eq!(
                report["series"][0]["api_nanos_by_model"][0]["rates"]["reference_model"],
                json!({"provider_id":"moonshotai", "model_id":"kimi-k3.1-pro"})
            );
        }
    }

    #[test]
    fn quota_proxy_before_target_history_writes_an_unpriced_day() {
        let mut conn = conn();
        let connection = std::sync::Arc::new(QuotaProxyCatalog {
            own_rates: quota_proxy_card(json!({"state":"stated_zero"})),
            target_history_start_ms: 101,
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let catalog = CatalogClient::new(connection.clone(), 0);
        reading(&conn, "acct", 10, 0.0, None);
        reading(&conn, "acct", 90, 50.0, None);
        event(
            &conn,
            "before-history",
            "broca",
            "kimi-code-plan-global",
            "k3.1-pro",
            "subscription_included",
            20,
            &[("input", "usage.input", 1_000)],
        );
        let models = test_models(&conn, 0, 100);
        let rates = resolve_rates(&catalog, &models, 100).unwrap();
        let observation = storeable_usage(&conn, (0, 100), &rates, 100);
        assert_quota_proxy_requests(&connection);
        assert_eq!(observation.api_nanos, 0);
        assert_eq!(observation.tokens.input, 1_000);
        assert_eq!(observation.unpriced_tokens.len(), 1);
        assert_eq!(observation.unpriced_tokens[0].reason, "no_catalog_coverage");
        assert_eq!(observation.unpriced_tokens[0].tokens, 1_000);
        assert_eq!(observation.multiplier, None);
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &observation).unwrap();
        tx.commit().unwrap();
        let report = multiplier_report(
            &conn,
            &MultiplierReportRequest {
                from_ms: 0,
                to_ms: 200,
            },
        )
        .unwrap();
        assert_eq!(report["series"].as_array().unwrap().len(), 1);
        assert_eq!(report["series"][0]["unpriced_tokens"][0]["tokens"], 1_000);
        assert_eq!(
            report["series"][0]["unpriced_tokens"][0]["reason"],
            "no_catalog_coverage"
        );
    }

    /// A row with no rate facts is a missing model, not a plan card. It takes
    /// the alias lookup only, never the apikey plane read.
    #[test]
    fn a_row_without_rate_facts_does_not_request_quota_proxy() {
        let conn = conn();
        let connection = std::sync::Arc::new(QuotaProxyCatalog {
            own_rates: BTreeMap::new(),
            target_history_start_ms: 100,
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let catalog = CatalogClient::new(connection.clone(), 0);
        reading(&conn, "acct", 10, 0.0, None);
        reading(&conn, "acct", 90, 50.0, None);
        event(
            &conn,
            "missing-row",
            "broca",
            "kimi-code-plan-global",
            "k3.1-pro",
            "subscription_included",
            20,
            &[("input", "usage.input", 1_000)],
        );
        let models = test_models(&conn, 0, 100);
        resolve_rates(&catalog, &models, 100).unwrap();
        let requests = connection.requests.lock().unwrap();
        assert!(
            requests.iter().all(|request| request.auth_method.is_none()),
            "a missing row must not make the apikey plane read: {requests:?}"
        );
    }

    #[test]
    fn priced_historical_rows_do_not_request_quota_proxy() {
        for units in [0, 2_000_000_000] {
            let conn = conn();
            let mut own_rates = quota_proxy_card(json!({"state":"stated_zero"}));
            own_rates.insert("rate.input".into(), reference_rate(units));
            let connection = std::sync::Arc::new(QuotaProxyCatalog {
                own_rates,
                target_history_start_ms: 100,
                requests: std::sync::Mutex::new(Vec::new()),
            });
            let catalog = CatalogClient::new(connection.clone(), 0);
            reading(&conn, "acct", 10, 0.0, None);
            reading(&conn, "acct", 90, 50.0, None);
            event(
                &conn,
                "own-price",
                "broca",
                "kimi-code-plan-global",
                "k3.1-pro",
                "subscription_included",
                20,
                &[("input", "usage.input", 1_000)],
            );
            let models = test_models(&conn, 0, 100);
            let rates = resolve_rates(&catalog, &models, 100).unwrap();
            let observation = storeable_usage(&conn, (0, 100), &rates, 100);
            assert_eq!(observation.api_nanos, units / 1_000);
            assert!(observation.unpriced_tokens.is_empty());
            let requests = connection.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].at_ms, Some(100));
            assert_eq!(requests[0].auth_method, None);
            let stored_rates =
                serde_json::to_value(&observation.api_nanos_by_model[0].rates).unwrap();
            assert!(stored_rates.get("reference_model").is_none());
        }
    }

    #[test]
    fn a_non_alias_current_price_does_not_fill_missing_history() {
        let conn = conn();
        let connection = std::sync::Arc::new(ReferenceCatalog {
            rows: BTreeMap::from([("new-model".into(), reference_rate(TEN_PER_M))]),
            aliased: vec![],
            empty_on_miss: BTreeSet::new(),
            current_only: BTreeSet::from(["new-model".into()]),
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let catalog = CatalogClient::new(connection.clone(), 0);
        reading(&conn, "acct", 10, 0.0, None);
        reading(&conn, "acct", 90, 50.0, None);
        event(
            &conn,
            "missing",
            "opencode",
            "google",
            "new-model",
            "metered_api",
            20,
            &[("input", "usage.input", 1_000)],
        );
        let models = test_models(&conn, 0, 100);
        let rates = resolve_rates(&catalog, &models, 100).unwrap();
        let observation = storeable_usage(&conn, (0, 100), &rates, 100);
        assert_eq!(
            *connection.requests.lock().unwrap(),
            vec![("new-model".into(), Some(100)), ("new-model".into(), None)]
        );
        assert_eq!(observation.api_nanos, 0);
        assert_eq!(observation.tokens.input, 1_000);
        assert_eq!(observation.unpriced_tokens.len(), 1);
        assert_eq!(observation.unpriced_tokens[0].reason, "no_catalog_coverage");
        assert_eq!(observation.unpriced_tokens[0].tokens, 1_000);
        assert_eq!(observation.multiplier, None);
        assert_eq!(observation.fee_multiplier, None);
        let known_zero = rates_for(&[("google", "new-model")], 0, None);
        let zero = storeable_usage(&conn, (0, 100), &known_zero, 100);
        assert!(zero.unpriced_tokens.is_empty());
        assert_eq!(zero.multiplier, Some(0.0));
        assert_eq!(zero.fee_multiplier, Some(0.0));
    }

    #[test]
    fn alias_values_the_multiplier_while_the_ledger_leaves_the_alias_unpriced() {
        for (metadata, marker) in [(true, false), (true, true), (false, true)] {
            for suffix in ["", "-low", "-high"] {
                let mut conn = conn();
                let model = "antigravity-gemini-3.8-flash";
                let event_model = format!("{model}{suffix}");
                let mut rate = reference_rate(TEN_PER_M);
                if marker {
                    rate["inherited_from"] = json!({"basis":"alias","provider_id":"google",
            "family":"gemini","model_id":"gemini-3.8-flash"});
                }
                let connection = std::sync::Arc::new(ReferenceCatalog {
                    rows: BTreeMap::from([
                        (model.into(), rate),
                        ("gemini-3.8-flash".into(), reference_rate(3 * TEN_PER_M)),
                    ]),
                    empty_on_miss: BTreeSet::new(),
                    current_only: BTreeSet::new(),
                    aliased: if metadata {
                        vec![fusiform_protocol::AliasedModelWire {
                            model: format!("google/{model}"),
                            target_provider_id: "google".into(),
                            target_model_id: "gemini-3.8-flash".into(),
                            source_ref: "plugin/models.ts@abc123".into(),
                        }]
                    } else {
                        vec![]
                    },
                    requests: std::sync::Mutex::new(Vec::new()),
                });
                let catalog = CatalogClient::new(connection.clone(), 0);
                reading(&conn, "acct", 10, 0.0, None);
                reading(&conn, "acct", 90, 50.0, None);
                for id in ["alias", "alias-again"] {
                    event(
                        &conn,
                        id,
                        "opencode",
                        "google",
                        &event_model,
                        "metered_api",
                        20,
                        &[("input", "usage.input", 1_000)],
                    );
                }
                let models = test_models(&conn, 0, 100);
                let rates = resolve_rates(&catalog, &models, 100).unwrap();
                let observation = pace_ready(storeable_usage(&conn, (0, 100), &rates, 100));
                assert_eq!(observation.tokens.input, 2_000);
                assert_eq!(observation.api_nanos, 60_000_000);
                assert!(observation.unpriced_tokens.is_empty());
                let mut expected_requests = vec![
                    (model.into(), Some(100)),
                    (model.into(), None),
                    ("gemini-3.8-flash".into(), Some(100)),
                ];
                if !suffix.is_empty() {
                    expected_requests.insert(0, (event_model.clone(), Some(100)));
                }
                assert_eq!(*connection.requests.lock().unwrap(), expected_requests);
                let stored_rates =
                    serde_json::to_value(&observation.api_nanos_by_model[0].rates).unwrap();
                assert_eq!(
                    stored_rates["reference_model"],
                    json!({"provider_id":"google", "model_id":"gemini-3.8-flash"})
                );
                let tx = conn.transaction().unwrap();
                insert_observation_in_tx(&tx, &observation).unwrap();
                tx.commit().unwrap();
                let report = multiplier_report(
                    &conn,
                    &MultiplierReportRequest {
                        from_ms: 0,
                        to_ms: 200,
                    },
                )
                .unwrap();
                assert_eq!(
                    report["series"][0]["api_nanos_by_model"][0]["rates"]["reference_model"],
                    stored_rates["reference_model"]
                );
                for id in ["pace-alias", "pace-alias-again"] {
                    account_event(
                        &conn,
                        id,
                        "opencode",
                        "google",
                        &event_model,
                        "subscription_included",
                        20,
                        &[("input", "usage.input", 1_000)],
                    );
                }
                let pace = multiplier_pace(
                    &conn,
                    &MultiplierPaceRequest {
                        pool: None,
                        plan: None,
                        quota_provider: "codex".into(),
                        from_ms: 0,
                        to_ms: 100,
                    },
                )
                .unwrap();
                assert_eq!(
                    pace["hours"][0]["producers"]["opencode"]["list_nanos"],
                    60_000_000
                );

                let ledger_plan = crate::subscription::read_reprice(&conn, "alias").unwrap();
                let resolved =
                    crate::subscription::resolve_reprice(&ledger_plan, &catalog).unwrap();
                let tx = conn.transaction().unwrap();
                crate::subscription::commit_reprice_in_tx(
                    &tx,
                    &ledger_plan,
                    &resolved,
                    100,
                    crate::pricing::PricingOrigin::PricingPass,
                )
                .unwrap();
                tx.commit().unwrap();
                let stored: (String, Option<String>) = conn
                    .query_row(
                        "SELECT outcome,unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='alias')",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap();
                assert_eq!(
                    stored,
                    ("unpriced".into(), Some("no_catalog_coverage".into())),
                    "a historical alias miss is an answer, not a failure"
                );
            }
        }
    }

    #[test]
    fn reasoning_suffixes_fold_only_low_and_high_after_a_missing_exact_row() {
        let conn = conn();
        let base = "gemini-4-argon";
        let flash = "gemini-3.8-flash";
        let gpt = "gpt-oss-120b";
        let catalog = CatalogClient::new(
            std::sync::Arc::new(ReferenceCatalog {
                rows: BTreeMap::from([
                    (base.into(), reference_rate(TEN_PER_M)),
                    (format!("{base}-low"), reference_rate(2 * TEN_PER_M)),
                    (flash.into(), reference_rate(3 * TEN_PER_M)),
                    (gpt.into(), reference_rate(4 * TEN_PER_M)),
                ]),
                aliased: vec![],
                empty_on_miss: BTreeSet::from([format!("{base}-high"), "missing-high".into()]),
                current_only: BTreeSet::new(),
                requests: std::sync::Mutex::new(Vec::new()),
            }),
            0,
        );
        reading(&conn, "acct", 10, 0.0, None);
        reading(&conn, "acct", 90, 50.0, None);
        for model in [
            format!("{base}-high"),
            format!("{base}-low"),
            format!("{flash}-low"),
            format!("{gpt}-medium"),
            "missing-high".into(),
        ] {
            event(
                &conn,
                &model,
                "broca",
                "google",
                &model,
                "unknown",
                20,
                &[("input", "usage.input", 1_000)],
            );
        }
        let models = test_models(&conn, 0, 100);
        let rates = resolve_rates(&catalog, &models, 100).unwrap();
        let observation = storeable_usage(&conn, (0, 100), &rates, 100);
        assert_eq!(observation.tokens.input, 5_000);
        assert_eq!(observation.api_nanos, 60_000_000);
        assert_eq!(observation.unpriced_tokens.len(), 2);
        for unpriced in &observation.unpriced_tokens {
            assert_eq!(unpriced.reason, "no_catalog_coverage");
            assert_eq!(unpriced.tokens, 1_000);
        }
        assert!(
            matches!(&rates.models[&("google".into(), format!("{gpt}-medium"))],
            ModelResolution::Unpriceable { reason } if reason == "no_catalog_coverage")
        );
        assert!(observation
            .api_nanos_by_model
            .iter()
            .any(
                |line| line.model.as_deref() == Some(&format!("{base}-high"))
                    && line.api_nanos == 10_000_000
            ));
    }

    /// A correction supersedes its day's head: the report then serves the
    /// correction in that day's slot, never the original, and a correction
    /// taken after later days does not become "latest" over them. A day with
    /// no row has nothing to supersede and is refused without a write.
    #[test]
    fn a_correction_replaces_its_day_in_the_report() {
        let mut conn = conn();
        let mut original = sample_observation(&conn);
        let mut next_day =
            storeable_usage(&conn, (DAY, 2 * DAY), &ResolvedRates::default(), 2 * DAY);
        for row in [&mut original, &mut next_day] {
            row.plan = Some("openai/pro_200".into());
            row.pool_scope = Some(PoolScope::All);
        }
        assert_eq!(next_day.observation_day, "1970-01-03");
        let tx = conn.transaction().unwrap();
        record_one(&tx, &original);
        record_one(&tx, &next_day);
        let original_id = observation_head(&tx, "codex", "1970-01-02")
            .unwrap()
            .unwrap();

        let mut corrected = original.clone();
        corrected.api_nanos = 123;
        corrected.snapshot_at_ms = 10 * DAY;
        let first = insert_superseding_observation_in_tx(&tx, &corrected).unwrap();
        assert_eq!(first.supersedes, original_id);
        corrected.api_nanos = 456;
        let second = insert_superseding_observation_in_tx(&tx, &corrected).unwrap();
        assert_eq!(
            second.supersedes, first.observation_id,
            "a second correction supersedes the head, not the original"
        );

        let mut unrecorded = original.clone();
        unrecorded.observation_day = "1970-01-05".into();
        assert!(matches!(
            insert_superseding_observation_in_tx(&tx, &unrecorded),
            Err(MultiplierError::NothingToSupersede { .. })
        ));
        tx.commit().unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 4, "the refused day wrote nothing");

        let report = multiplier_report(
            &conn,
            &MultiplierReportRequest {
                from_ms: 0,
                to_ms: 3 * DAY,
            },
        )
        .unwrap();
        let series = report["series"].as_array().unwrap();
        let slots: Vec<_> = series
            .iter()
            .map(|row| {
                (
                    row["observation_day"].as_str().unwrap(),
                    row["api_nanos"].as_i64().unwrap(),
                    row["supersedes"].as_i64(),
                )
            })
            .collect();
        assert_eq!(
            slots,
            [
                ("1970-01-02", 456, Some(first.observation_id)),
                ("1970-01-03", next_day.api_nanos, None),
            ],
            "one row per provider and day, each its head"
        );
        assert_eq!(report["latest"].as_array().unwrap().len(), 1);
        assert_eq!(report["latest"][0]["observation_day"], "1970-01-03");
        assert_eq!(report["latest"][0]["supersedes"], Value::Null);
    }

    /// Catalog facts decode to per-role rates and per-threshold tiers; a
    /// withheld or inherited rate is a reason, not a price.
    #[test]
    fn catalog_facts_decode_to_rates_tiers_and_reasons() {
        let rate = |units: i64| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
        let snapshot = CatalogSnapshot {
            alias_target: None,
            source: "models.dev".into(),
            resolved_at_ms: 1,
            model_key: "openai/m".into(),
            facts: BTreeMap::from([
                ("rate.input".to_string(), rate(4)),
                ("rate.input.above_context.272000".to_string(), rate(8)),
                (
                    "rate.output".to_string(),
                    json!({"state":"priced","units":5,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"},"inherited_from":{"provider_id":"x","family":"y","basis":"z"}}),
                ),
            ]),
            withheld_facts: BTreeMap::from([("rate.cache_read".to_string(), json!({}))]),
            uncertain_facts: BTreeMap::new(),
            fact_overrides: BTreeMap::new(),
        };
        let ModelResolution::Rates(rates) = rates_from_snapshot(&snapshot) else {
            panic!("rates expected");
        };
        assert_eq!(rates.base["input"], priced(4));
        assert_eq!(rates.tiers[&272_000]["input"], priced(8));
        assert_eq!(
            rates.base["output"],
            RateState::Unpriced {
                reason: "rate_inherited".into()
            }
        );
        assert_eq!(
            rates.base["cache_read"],
            RateState::Unpriced {
                reason: "rate_withheld".into()
            }
        );
        let empty = CatalogSnapshot {
            alias_target: None,
            facts: BTreeMap::new(),
            withheld_facts: BTreeMap::new(),
            ..snapshot
        };
        assert_eq!(
            rates_from_snapshot(&empty),
            ModelResolution::Unpriceable {
                reason: "no_catalog_coverage".into()
            }
        );
    }

    /// A stated zero marked `inherited_from` resolves to `rate_inherited`,
    /// exactly like an inherited priced rate, while the provider's own stated
    /// zero keeps `stated_zero_requires_policy`. All three rates sit in one
    /// catalog snapshot so the two inherited ones are compared directly.
    #[test]
    fn borrowed_stated_zero_is_refused_like_a_borrowed_price() {
        let origin = json!({"provider_id":"zai","family":"glm","basis":"open_weights"});
        let snapshot = CatalogSnapshot {
            alias_target: None,
            source: "models.dev".into(),
            resolved_at_ms: 1,
            model_key: "openai/m".into(),
            facts: BTreeMap::from([
                (
                    "rate.input".to_string(),
                    json!({"state":"stated_zero","inherited_from": origin}),
                ),
                (
                    "rate.output".to_string(),
                    json!({"state":"priced","units":5,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"},"inherited_from": origin}),
                ),
                (
                    "rate.cache_read".to_string(),
                    json!({"state":"stated_zero"}),
                ),
            ]),
            withheld_facts: BTreeMap::new(),
            uncertain_facts: BTreeMap::new(),
            fact_overrides: BTreeMap::new(),
        };
        let ModelResolution::Rates(rates) = rates_from_snapshot(&snapshot) else {
            panic!("rates expected");
        };
        let inherited = RateState::Unpriced {
            reason: "rate_inherited".into(),
        };
        assert_eq!(rates.base["output"], inherited);
        assert_eq!(rates.base["input"], inherited);
        assert_eq!(
            rates.base["cache_read"],
            RateState::Unpriced {
                reason: "stated_zero_requires_policy".into()
            }
        );
    }
}
