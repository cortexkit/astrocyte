//! Per-measurement pricing and resolve-then-commit repricing.
//!
//! Pricing never performs network I/O while a SQLite transaction or store mutex
//! is held. The public read, resolve, and commit phases are separate so serving
//! code can release the store before calling fusiform and can reject a stale
//! resolution under the event's pricing generation.

use fusiform_protocol::{
    Amount, Floor, RateValue, UnpricedReason as ProtocolUnpricedReason, NANO_EXPONENT,
};
use rusqlite::{named_params, params, types::Type, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::canon::CanonError;
use crate::catalog_client::{
    mode_rate_key, stated_mode_condition, tier_thresholds, CatalogClient, CatalogClientError,
    CatalogLookup, CatalogSnapshot,
};
use crate::fact::ChargeBasis;
use crate::ingest::INCLUSIVE_OUTPUT_SOURCE;
use crate::money::{round_nano_num, MoneyError, NUMERATOR_SCALE};

/// Catalog rates quote nanodollars per million (10^6) quantity units. This
/// exponent is distinct from NUMERATOR_SCALE, the millionths-of-a-nanodollar
/// scale `money` uses to round stored amounts.
const RATE_UNIT_EXPONENT: i32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnpricedReason {
    UndatedEvent,
    ChargeBasisUnknown,
    NoSubscriptionInForce,
    MissingCatalogIdentity,
    CatalogPreHistory,
    NoRate,
    /// Fusiform examined the model and served no row. Mapped verbatim from
    /// their wire; never produced locally.
    NoCatalogCoverage,
    /// No catalog was configured, so nothing examined the model. Distinct
    /// from `NoCatalogCoverage`: that one says a catalog looked and found
    /// nothing, this one says nobody looked. The remedies belong to
    /// different people and the two must never share a string.
    CatalogUnconfigured,
    CatalogUnknownChargeBasis,
    MissingQuantity,
    RateTiered,
    RateTimeBanded,
    RateWithheld,
    RateUncertain,
    StatedZeroRequiresPolicy,
    ArithmeticOutOfRange,
    /// The catalog served a rate the serving provider did not publish,
    /// borrowed from the model's originator and marked `inherited_from`.
    /// Reference data for routing; never a charge on this provider.
    RateInherited,
}

/// Providers whose billing depends on an input no catalog row carries. Each
/// entry names the MISSING AXIS rather than the outcome, so it states what
/// would have to arrive for the entry to be deleted; a reason phrased as an
/// outcome ("goes unpriced") never does, and the next reader cannot tell
/// whether the gap is still real.
///
/// The claim is about the CATALOG PAYLOAD, never about a producer's behaviour.
/// The first version of this guard stored "fusiform does not serve the
/// time-band selector" — a sentence about another module frozen in my source,
/// which goes stale silently the day they serve it and which I am not
/// positioned to re-check. What is checkable here is what the payload carries:
/// models.dev publishes no band, validity or effective-window field anywhere,
/// so no reader of that catalog can attribute a rate to a band.
///
/// The reason travels WITH the provider rather than being assumed from the
/// list's existence: a second entry whose gap is a different axis must not be
/// filed under this one's reason, which would put one refusal's name on
/// another's cause.
const CATALOG_COVERAGE_GAPS: &[(&str, UnpricedReason, &str, i64)] = &[(
    "deepseek",
    UnpricedReason::RateTimeBanded,
    "no band, validity or effective-window field in the catalog payload, so a \
     served rate cannot be attributed to the UTC band that selects it",
    // DeepSeek's peak/off-peak rates take effect at 2026-08-16 16:00 UTC:
    // https://api-docs.deepseek.com/news/news260813
    1_786_896_000_000,
)];

impl UnpricedReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UndatedEvent => "undated_event",
            Self::ChargeBasisUnknown => "charge_basis_unknown",
            Self::NoSubscriptionInForce => "no_subscription_in_force",
            Self::MissingCatalogIdentity => "missing_catalog_identity",
            Self::CatalogPreHistory => "catalog_pre_history",
            Self::NoRate => "no_rate",
            Self::NoCatalogCoverage => "no_catalog_coverage",
            Self::CatalogUnconfigured => "catalog_unconfigured",
            Self::CatalogUnknownChargeBasis => "catalog_unknown_charge_basis",
            Self::MissingQuantity => "missing_quantity",
            Self::RateTiered => "rate_tiered",
            Self::RateTimeBanded => "rate_time_banded",
            Self::RateWithheld => "rate_withheld",
            Self::RateUncertain => "rate_uncertain",
            Self::StatedZeroRequiresPolicy => "stated_zero_requires_policy",
            Self::ArithmeticOutOfRange => "arithmetic_out_of_range",
            Self::RateInherited => "rate_inherited",
        }
    }
}

/// How many output tokens an event's `output` measurement stands for once
/// reasoning is taken out of it. See [`effective_output`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectiveOutput {
    /// The stored output quantity is already disjoint from reasoning (or the
    /// event has no reasoning to take out); use it as stored.
    AsStored,
    /// The stored output includes the event's reasoning; this many tokens
    /// (`output - reasoning`) are output proper.
    ReasoningRemoved(i64),
    /// The stored output includes reasoning but is smaller than it, so the
    /// event's stored output and reasoning contradict each other and no
    /// output quantity can be derived.
    ReasoningExceedsOutput,
}

/// BROCA provider whose older facts cannot be read as inclusive from the
/// stored row alone.
///
/// BROCA stamps each fact it exports with a `usage_policy_version`. Before
/// version 2, BROCA reported xAI output WITHOUT reasoning; from version 2 on,
/// with it, as for every other provider. The astrocyte wire mapping stores pre-fix xAI facts verbatim under
/// `usage.output_tokens` because their output is already disjoint, and facts
/// ingested before that mapping existed also carry `usage.output_tokens` but
/// may be either kind. `usage_policy_version` is not stored on the event, so
/// a stored xAI row with the inclusive source cannot be told apart, and
/// subtracting reasoning from a pre-fix one would undercount it.
///
/// xAI is therefore left out of the subtraction. Measured on a copy of the
/// live store on 2026-09-26: 1,601 BROCA xAI events carry output source
/// `usage.output_tokens`, all 1,601 with a reasoning measurement, and none of
/// them can be charged: 1,197 are `subscription_included`, 404 are `unknown`,
/// none is `metered_api`, and none has ever had a priced charge row. A basis
/// correction to `metered_api` on one of them would make this ambiguity
/// matter again and needs its own decision.
const OUTPUT_EXCLUSIVE_BEFORE_FIX_PROVIDER: &str = "xai";

/// The output quantity ledger pricing charges and the multiplier values for
/// one event. Both must call this so neither counts a token the other does
/// not.
///
/// BROCA's `output_tokens` includes its reasoning tokens. Facts stored after
/// the wire mapping was fixed carry output with reasoning already subtracted
/// (source `usage.output_tokens-reasoning_tokens`); facts stored before it
/// carry the inclusive count under [`INCLUSIVE_OUTPUT_SOURCE`], and stored
/// measurements are never rewritten. Priced as stored, such a fact's
/// reasoning would be charged twice: inside output and again as reasoning.
///
/// The rule applies exactly when ALL of these hold:
/// - `producer` is `broca`;
/// - `provider` is not `xai` (see [`OUTPUT_EXCLUSIVE_BEFORE_FIX_PROVIDER`]);
/// - the output measurement's source is exactly [`INCLUSIVE_OUTPUT_SOURCE`];
/// - the event has a reasoning measurement with a whole token quantity
///   (`reasoning_tokens` is `Some`).
///
/// Callers pass only whole token quantities (unit `token`, exponent 0) for
/// both `output_tokens` and `reasoning_tokens`. When the rule applies, the
/// effective output is `output - reasoning`; when that is negative the result
/// is [`EffectiveOutput::ReasoningExceedsOutput`]. Otherwise the stored
/// output is used as it is.
///
/// What ledger pricing records: the output measurement row keeps the
/// inclusive quantity `o`. The output charge row's `charge_nanos` is
/// `(o - r) x rate.output`, and nothing else on the row states the
/// quantity; `o`, `r` and the output's source are on the event's own
/// measurements, so a reader recomputes the effective quantity exactly with
/// this rule. When reasoning exceeds inclusive output, the output charge row
/// is unpriced with reason `missing_quantity`, the same reason a fact mapped
/// today gets for the same contradiction (its output is stored with no
/// quantity under a `:conflicted` source). Its `catalog_entry` stays NULL
/// because that column holds only what a catalog served; the contradiction
/// is read from the same two measurements. Reasoning is still priced.
pub(crate) fn effective_output(
    producer: &str,
    provider: Option<&str>,
    output_source: &str,
    output_tokens: i64,
    reasoning_tokens: Option<i64>,
) -> EffectiveOutput {
    let Some(reasoning_tokens) = reasoning_tokens else {
        return EffectiveOutput::AsStored;
    };
    if producer != "broca"
        || provider == Some(OUTPUT_EXCLUSIVE_BEFORE_FIX_PROVIDER)
        || output_source != INCLUSIVE_OUTPUT_SOURCE
    {
        return EffectiveOutput::AsStored;
    }
    match output_tokens.checked_sub(reasoning_tokens) {
        Some(effective) if effective >= 0 => EffectiveOutput::ReasoningRemoved(effective),
        _ => EffectiveOutput::ReasoningExceedsOutput,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingRefusal {
    Malformed,
    NotFound,
    NoMeasurementsToPrice,
    AlreadyPriced,
    BasisUnchanged,
    PricingSuperseded,
    CatalogCurrencyMismatch,
}

impl PricingRefusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::NotFound => "not_found",
            Self::NoMeasurementsToPrice => "no_measurements_to_price",
            Self::AlreadyPriced => "already_priced",
            Self::BasisUnchanged => "basis_unchanged",
            Self::PricingSuperseded => "pricing_superseded",
            Self::CatalogCurrencyMismatch => "catalog_currency_mismatch",
        }
    }
}

#[derive(Debug)]
pub enum PricingError {
    Store(rusqlite::Error),
    Canon(CanonError),
    Catalog(CatalogClientError),
    Refused(PricingRefusal),
}

impl From<rusqlite::Error> for PricingError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

impl From<CanonError> for PricingError {
    fn from(error: CanonError) -> Self {
        Self::Canon(error)
    }
}

impl From<CatalogClientError> for PricingError {
    fn from(error: CatalogClientError) -> Self {
        Self::Catalog(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepriceRequest {
    pub event_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepriceOutcome {
    pub event_id: String,
    pub pricing_generation: u64,
    pub measurements_repriced: usize,
    pub measurements_preserved: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CurrentHead {
    charge_key: i64,
    outcome: HeadOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadOutcome {
    Priced,
    Covered,
    Unpriced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MeasurementPlan {
    role: String,
    unit: String,
    source: String,
    quantity_units: Option<i64>,
    quantity_exponent: Option<i32>,
    current_head: Option<CurrentHead>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepricePlan {
    basis_correction: bool,
    event_id: String,
    event_key: i64,
    producer: String,
    provider: Option<String>,
    model: Option<String>,
    account_ref: Option<String>,
    occurred_at_ms: Option<i64>,
    charge_basis: ChargeBasis,
    currency: Option<String>,
    pricing_generation: u64,
    /// The mode the request's own record states it ran in (`fast`,
    /// `standard`, ...), or `None` when the record states none. It selects a
    /// `rate.<role>.mode.<mode>` rate; see [`stated_mode_condition`]. The
    /// ledger stores no request mode yet, so every plan read from the store
    /// carries `None` and prices at the unconditional rate.
    mode: Option<String>,
    targets: Vec<MeasurementPlan>,
    all_measurements: Vec<MeasurementPlan>,
    measurements_preserved: usize,
}

impl RepricePlan {
    /// Reuses the event identity captured while pricing data was loaded.
    pub(crate) fn subscription_coverage_identity(
        &self,
    ) -> (Option<&str>, Option<&str>, Option<i64>, ChargeBasis) {
        (
            self.provider.as_deref(),
            self.account_ref.as_deref(),
            self.occurred_at_ms,
            self.charge_basis,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ChargeDecision {
    Priced {
        charge_nanos: i64,
        amount: Amount,
        floor: Floor,
        catalog_source: String,
        resolved_at_ms: i64,
        catalog_entry: Value,
    },
    Covered {
        currency: String,
    },
    Unpriced {
        reason: UnpricedReason,
        catalog_source: Option<String>,
        resolved_at_ms: Option<i64>,
        catalog_entry: Option<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedMeasurement {
    role: String,
    unit: String,
    supersedes: Option<i64>,
    decision: ChargeDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedReprice {
    event_id: String,
    pricing_generation: u64,
    charge_basis: ChargeBasis,
    measurements: Vec<ResolvedMeasurement>,
}

impl ResolvedReprice {
    fn for_plan(plan: &RepricePlan, measurements: Vec<ResolvedMeasurement>) -> Self {
        Self {
            event_id: plan.event_id.clone(),
            pricing_generation: plan.pricing_generation,
            charge_basis: plan.charge_basis,
            measurements,
        }
    }
}

pub fn read_reprice_in_tx(
    tx: &Transaction<'_>,
    event_id: &str,
) -> Result<RepricePlan, PricingError> {
    read_plan_in_tx(tx, event_id, None)
}

pub(crate) fn read_basis_correction_in_tx(
    tx: &Transaction<'_>,
    request: &crate::ingest::BasisCorrectRequest,
) -> Result<RepricePlan, PricingError> {
    if request.reason.trim().is_empty() {
        return Err(PricingError::Refused(PricingRefusal::Malformed));
    }
    read_plan_in_tx(tx, &request.event_id, Some(request.corrected_basis))
}

fn read_plan_in_tx(
    tx: &Transaction<'_>,
    event_id: &str,
    corrected_basis: Option<ChargeBasis>,
) -> Result<RepricePlan, PricingError> {
    if event_id.is_empty() {
        return Err(PricingError::Refused(PricingRefusal::Malformed));
    }
    let event = tx
        .query_row(
            "SELECT provider,model,account_ref,occurred_at_ms,charge_basis,currency,
                    pricing_generation,producer,event_key
             FROM usage_event WHERE event_id=?1",
            [event_id],
            |row| {
                let basis = row.get::<_, String>(4)?;
                let generation = row.get::<_, i64>(6)?;
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    ChargeBasis::parse(&basis).ok_or_else(|| {
                        stored_integrity_error(
                            4,
                            Type::Text,
                            format!("unknown usage_event.charge_basis '{basis}'"),
                        )
                    })?,
                    row.get::<_, Option<String>>(5)?,
                    u64::try_from(generation).map_err(|_| {
                        stored_integrity_error(
                            6,
                            Type::Integer,
                            format!("negative usage_event.pricing_generation {generation}"),
                        )
                    })?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()?
        .ok_or(PricingError::Refused(PricingRefusal::NotFound))?;

    if corrected_basis == Some(event.4) {
        // A correction that changes nothing is refused rather than recorded.
        // Every entry in a supersession chain is something a reader has to walk
        // and account for, so a chain link that restates its predecessor's
        // basis costs every future reader and tells them nothing.
        return Err(PricingError::Refused(PricingRefusal::BasisUnchanged));
    }

    let mut statement = tx.prepare(
        "SELECT m.role,m.unit,m.quantity_units,m.quantity_exponent,
                pc.charge_key,pc.outcome,m.source
         FROM measurement m
         LEFT JOIN priced_charge pc
           ON pc.event_key=m.event_key AND pc.role=m.role AND pc.unit=m.unit
          AND NOT EXISTS (
              SELECT 1 FROM priced_charge child WHERE child.supersedes=pc.charge_key
          )
         WHERE m.event_key=?1
         ORDER BY m.role,m.unit",
    )?;
    let all_measurements = statement
        .query_map([event.8], |row| {
            let charge_key = row.get::<_, Option<i64>>(4)?;
            let outcome = row.get::<_, Option<String>>(5)?;
            let current_head = match (charge_key, outcome) {
                (None, None) => None,
                (Some(charge_key), Some(outcome)) => Some(CurrentHead {
                    charge_key,
                    outcome: parse_head_outcome(&outcome)?,
                }),
                _ => {
                    return Err(stored_integrity_error(
                        5,
                        Type::Text,
                        "priced_charge head has only one of charge_key and outcome".into(),
                    ))
                }
            };
            Ok(MeasurementPlan {
                role: row.get(0)?,
                unit: row.get(1)?,
                source: row.get(6)?,
                quantity_units: row.get(2)?,
                quantity_exponent: row.get(3)?,
                current_head,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if all_measurements.is_empty() {
        return Err(PricingError::Refused(PricingRefusal::NoMeasurementsToPrice));
    }
    let targets = all_measurements
        .iter()
        .filter(|measurement| {
            corrected_basis.is_some()
                || measurement
                    .current_head
                    .as_ref()
                    .is_none_or(|head| head.outcome == HeadOutcome::Unpriced)
        })
        .cloned()
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(PricingError::Refused(PricingRefusal::AlreadyPriced));
    }
    let measurements_preserved = all_measurements.len() - targets.len();
    Ok(RepricePlan {
        basis_correction: corrected_basis.is_some(),
        event_id: event_id.to_string(),
        event_key: event.8,
        producer: event.7,
        provider: event.0,
        model: event.1,
        account_ref: event.2,
        occurred_at_ms: event.3,
        charge_basis: corrected_basis.unwrap_or(event.4),
        currency: event.5,
        pricing_generation: event.6,
        mode: None,
        targets,
        all_measurements,
        measurements_preserved,
    })
}

/// Resolves catalog rates only after the plan-reading transaction has been released.
pub fn resolve_reprice(
    plan: &RepricePlan,
    catalog: &CatalogClient,
    reasoning_billed_as_output: &[String],
) -> Result<ResolvedReprice, PricingError> {
    resolve_reprice_with_catalog(plan, Some(catalog), reasoning_billed_as_output)
}

/// Resolves a stable plan when no fusiform connection is configured. Local
/// outcomes still win, while catalog-dependent measurements become explicitly
/// unpriced rather than being silently skipped.
pub fn resolve_reprice_without_catalog(
    plan: &RepricePlan,
) -> Result<ResolvedReprice, PricingError> {
    resolve_reprice_with_catalog(plan, None, &[])
}

fn resolve_reprice_with_catalog(
    plan: &RepricePlan,
    catalog: Option<&CatalogClient>,
    reasoning_billed_as_output: &[String],
) -> Result<ResolvedReprice, PricingError> {
    // Undated events are refused before any coverage gap compares dates.
    let Some(occurred_at_ms) = plan.occurred_at_ms else {
        return Ok(unpriced_all(
            plan,
            UnpricedReason::UndatedEvent,
            None,
            None,
            None,
        ));
    };
    match plan.charge_basis {
        ChargeBasis::Unknown => {
            return Ok(unpriced_all(
                plan,
                UnpricedReason::ChargeBasisUnknown,
                None,
                None,
                None,
            ));
        }
        ChargeBasis::SubscriptionIncluded => {
            return Ok(unpriced_all(
                plan,
                UnpricedReason::NoSubscriptionInForce,
                None,
                None,
                None,
            ));
        }
        ChargeBasis::MeteredApi => {}
    }
    if plan
        .targets
        .iter()
        .all(|measurement| measurement.quantity_units.is_none())
    {
        return Ok(unpriced_all(
            plan,
            UnpricedReason::MissingQuantity,
            None,
            None,
            None,
        ));
    }
    let (provider, model) = match (plan.provider.as_deref(), plan.model.as_deref()) {
        (Some(provider), Some(model)) if !provider.is_empty() && !model.is_empty() => {
            (provider, model)
        }
        _ => {
            return Ok(unpriced_all(
                plan,
                UnpricedReason::MissingCatalogIdentity,
                None,
                None,
                None,
            ));
        }
    };
    if let Some((_, reason, _missing_axis, _effective_from_ms)) =
        CATALOG_COVERAGE_GAPS
            .iter()
            .find(|(gap_provider, _, _, effective_from_ms)| {
                *gap_provider == provider && occurred_at_ms >= *effective_from_ms
            })
    {
        // No catalog_entry: nothing was served. This refusal precedes the
        // lookup entirely, so there are no bytes to record, and the column
        // holds what a catalog sent. The first version wrote a synthesised
        // object there and it is the only unpriced reason in production that
        // does — 80 rows against 197,000 that correctly hold NULL — which made
        // a local opinion indistinguishable from a producer's payload for
        // anyone reading the column. The reason enum carries what the row
        // needs; the missing axis is for the reader of this file.
        return Ok(unpriced_all(plan, *reason, None, None, None));
    }
    let Some(catalog) = catalog else {
        return Ok(unpriced_all(
            plan,
            UnpricedReason::CatalogUnconfigured,
            None,
            None,
            None,
        ));
    };
    match catalog.get(provider, model, occurred_at_ms)? {
        CatalogLookup::PreHistory { .. } => Ok(unpriced_all(
            plan,
            UnpricedReason::CatalogPreHistory,
            None,
            None,
            None,
        )),
        CatalogLookup::Resolved(snapshot) => {
            resolve_catalog_snapshot(plan, &snapshot, reasoning_billed_as_output)
        }
    }
}

/// Subscription coverage resolves its effective plan in a separate read phase,
/// then uses the same generation-checked commit path as ordinary repricing.
pub fn resolve_subscription_coverage(
    plan: &RepricePlan,
    currency: &str,
) -> Result<ResolvedReprice, PricingError> {
    if plan.charge_basis != ChargeBasis::SubscriptionIncluded
        || currency.len() != 3
        || !currency.bytes().all(|byte| byte.is_ascii_uppercase())
    {
        return Err(PricingError::Refused(PricingRefusal::Malformed));
    }
    Ok(ResolvedReprice::for_plan(
        plan,
        plan.targets
            .iter()
            .map(|measurement| ResolvedMeasurement {
                role: measurement.role.clone(),
                unit: measurement.unit.clone(),
                supersedes: head_id(measurement),
                decision: ChargeDecision::Covered {
                    currency: currency.to_string(),
                },
            })
            .collect(),
    ))
}

/// The live write paths; historical attribution is deliberately not constructible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PricingOrigin {
    PricingPass,
    OperatorReprice,
    BasisCorrection,
}

impl PricingOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PricingPass => "pricing_pass",
            Self::OperatorReprice => "operator_reprice",
            Self::BasisCorrection => "basis_correction",
        }
    }
}

/// Re-checks the plan's generation and appends every new head in one transaction.
pub fn commit_reprice_in_tx(
    tx: &Transaction<'_>,
    plan: &RepricePlan,
    resolved: &ResolvedReprice,
    recorded_at_ms: i64,
    origin: PricingOrigin,
) -> Result<RepriceOutcome, PricingError> {
    if recorded_at_ms < 0
        || resolved.measurements.len() != plan.targets.len()
        // Resolutions belong to one event and generation, even when another
        // plan happens to have the same number of measurements.
        || resolved.event_id != plan.event_id
        || resolved.pricing_generation != plan.pricing_generation
        || resolved.charge_basis != plan.charge_basis
        || !resolved.measurements.iter().zip(&plan.targets).all(|(resolved, target)| {
            resolved.role == target.role && resolved.unit == target.unit && resolved.supersedes == head_id(target)
        })
        // A plan read as a basis correction must be committed as one, and an
        // ordinary plan must not be. The two produce rows that differ only in
        // `origin`, so a mismatch here would file one authority's act under the
        // other's name — silently, and with no way to tell afterwards.
        || plan.basis_correction != (origin == PricingOrigin::BasisCorrection)
    {
        return Err(PricingError::Refused(PricingRefusal::Malformed));
    }
    let current_generation = tx
        .query_row(
            "SELECT pricing_generation FROM usage_event WHERE event_id=?1",
            [&plan.event_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or(PricingError::Refused(PricingRefusal::NotFound))?;
    if u64::try_from(current_generation).ok() != Some(plan.pricing_generation) {
        return Err(PricingError::Refused(PricingRefusal::PricingSuperseded));
    }

    let currency = resolved_currency(plan.currency.as_deref(), &resolved.measurements)?;
    let next_generation = plan
        .pricing_generation
        .checked_add(1)
        .and_then(|generation| i64::try_from(generation).ok())
        .ok_or(PricingError::Refused(PricingRefusal::Malformed))?;
    let updated = tx.execute(
        "UPDATE usage_event
         SET currency=COALESCE(currency,?1),pricing_generation=?2,charge_basis=?5
         WHERE event_id=?3 AND pricing_generation=?4",
        params![
            currency,
            next_generation,
            plan.event_id,
            current_generation,
            plan.charge_basis.as_str()
        ],
    )?;
    if updated != 1 {
        return Err(PricingError::Refused(PricingRefusal::PricingSuperseded));
    }

    for measurement in &resolved.measurements {
        insert_charge(
            tx,
            plan,
            measurement,
            next_generation,
            recorded_at_ms,
            origin,
        )?;
    }
    // Charge rows and queue removal share `tx`. An event leaves the queue only
    // once every measurement has a current head (a row nothing supersedes),
    // which need not be true after pricing just the targets of this plan.
    dequeue_if_fully_headed(tx, &plan.event_id)?;
    Ok(RepriceOutcome {
        event_id: plan.event_id.clone(),
        pricing_generation: u64::try_from(next_generation).unwrap(),
        measurements_repriced: resolved.measurements.len(),
        measurements_preserved: plan.measurements_preserved,
    })
}

#[derive(Default)]
struct ChargeColumns<'a> {
    outcome: &'static str,
    basis_covered: bool,
    currency: Option<&'a str>,
    charge_nanos: Option<i64>,
    rate_units: Option<i64>,
    rate_exponent: Option<i64>,
    floor_state: Option<&'static str>,
    floor_units: Option<i64>,
    catalog_source: Option<&'a str>,
    resolved_at_ms: Option<i64>,
    catalog_entry: Option<String>,
    unpriced_reason: Option<&'static str>,
}

impl<'a> ChargeColumns<'a> {
    fn from_decision(decision: &'a ChargeDecision) -> rusqlite::Result<Self> {
        let encode = |value: &Value| {
            serde_json::to_string(value)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        };
        Ok(match decision {
            ChargeDecision::Priced {
                charge_nanos,
                amount,
                floor,
                catalog_source,
                resolved_at_ms,
                catalog_entry,
            } => {
                let (floor_state, floor_units) = match floor {
                    Floor::Unknown => ("unknown", None),
                    Floor::None => ("none", None),
                    Floor::Minimum { units } => ("minimum", Some(*units)),
                };
                Self {
                    outcome: "priced",
                    currency: Some(amount.currency.as_str()),
                    charge_nanos: Some(*charge_nanos),
                    rate_units: Some(amount.units),
                    rate_exponent: Some(i64::from(amount.exponent)),
                    floor_state: Some(floor_state),
                    floor_units,
                    catalog_source: Some(catalog_source),
                    resolved_at_ms: Some(*resolved_at_ms),
                    catalog_entry: Some(encode(catalog_entry)?),
                    ..Self::default()
                }
            }
            ChargeDecision::Covered { currency } => Self {
                outcome: "covered",
                basis_covered: true,
                currency: Some(currency),
                charge_nanos: Some(0),
                ..Self::default()
            },
            ChargeDecision::Unpriced {
                reason,
                catalog_source,
                resolved_at_ms,
                catalog_entry,
            } => Self {
                outcome: "unpriced",
                unpriced_reason: Some(reason.as_str()),
                catalog_source: catalog_source.as_deref(),
                resolved_at_ms: *resolved_at_ms,
                catalog_entry: catalog_entry.as_ref().map(encode).transpose()?,
                ..Self::default()
            },
        })
    }
}

fn insert_charge(
    tx: &Transaction<'_>,
    plan: &RepricePlan,
    measurement: &ResolvedMeasurement,
    generation: i64,
    recorded_at_ms: i64,
    origin: PricingOrigin,
) -> rusqlite::Result<i64> {
    let columns = ChargeColumns::from_decision(&measurement.decision)?;
    tx.query_row(
        "INSERT INTO priced_charge (
            event_key,role,unit,pricing_generation,outcome,charge_basis,
            basis_covered,currency,charge_nanos,rate_units,rate_exponent,
            floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
            unpriced_reason,supersedes,origin,recorded_at_ms
        ) VALUES (
            :event_key,:role,:unit,:generation,:outcome,:basis,
            :covered,:currency,:nanos,:rate_units,:rate_exponent,
            :floor_state,:floor_units,:catalog_source,:resolved_at,:catalog_entry,
            :reason,:supersedes,:origin,:recorded_at
        ) RETURNING charge_key",
        named_params! {
            ":event_key": plan.event_key,
            ":role": measurement.role, ":unit": measurement.unit, ":generation": generation,
            ":outcome": columns.outcome, ":basis": plan.charge_basis.as_str(),
            ":covered": columns.basis_covered, ":currency": columns.currency, ":nanos": columns.charge_nanos,
            ":rate_units": columns.rate_units, ":rate_exponent": columns.rate_exponent,
            ":floor_state": columns.floor_state, ":floor_units": columns.floor_units,
            ":catalog_source": columns.catalog_source, ":resolved_at": columns.resolved_at_ms,
            ":catalog_entry": columns.catalog_entry, ":reason": columns.unpriced_reason,
            ":supersedes": measurement.supersedes, ":origin": origin.as_str(), ":recorded_at": recorded_at_ms,
        },
        |row| row.get(0),
    )
}

/// True when the current `measurement` row has no charge row that nothing
/// supersedes. This predicate has no parameters and works in both event and
/// ledger-range queries, whose bounds have different parameter positions.
macro_rules! measurement_has_no_head {
    () => {
        "NOT EXISTS (
          SELECT 1 FROM priced_charge
          WHERE priced_charge.event_key=measurement.event_key
            AND priced_charge.role = measurement.role
            AND priced_charge.unit = measurement.unit
            AND NOT EXISTS (
                SELECT 1 FROM priced_charge AS child
                WHERE child.supersedes = priced_charge.charge_key
            )
      )"
    };
}

/// True when event `?1` has a measurement needing a current charge head.
const EVENT_HAS_HEADLESS_MEASUREMENT: &str = concat!(
    "EXISTS (SELECT 1 FROM measurement WHERE measurement.event_key = (SELECT event_key FROM usage_event WHERE event_id=?1) AND ",
    measurement_has_no_head!(),
    ")"
);

/// Put `event_id` in the pricing queue if it needs pricing.
///
/// Must run in the transaction that wrote the event's measurements: a queue row
/// written in a later transaction is lost if the process dies in between, and
/// the event is then never priced. Enqueuing an event that is already queued or
/// has no headless measurement changes nothing. Returns the number of rows
/// inserted (0 or 1).
pub fn enqueue_if_unpriced_in_tx(conn: &Connection, event_id: &str) -> rusqlite::Result<usize> {
    conn.execute(
        &format!(
            "INSERT OR IGNORE INTO pricing_pending (event_key)
             SELECT event_key FROM usage_event WHERE event_id=?1 AND {EVENT_HAS_HEADLESS_MEASUREMENT}"
        ),
        [event_id],
    )
}

fn dequeue_if_fully_headed(conn: &Connection, event_id: &str) -> rusqlite::Result<usize> {
    conn.execute(
        &format!(
            "DELETE FROM pricing_pending
             WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) AND NOT {EVENT_HAS_HEADLESS_MEASUREMENT}"
        ),
        [event_id],
    )
}

/// Up to `limit` queued event ids after `after`, in event-id order, plus the
/// number of SQLite virtual-machine steps the lookup took.
///
/// The step count lets a caller show that finding work costs what is queued,
/// not what the ledger holds: it is SQLite's own counter for this statement,
/// so it is deterministic where a timing would not be.
pub fn pending_event_ids(
    conn: &Connection,
    after: Option<&str>,
    limit: usize,
) -> rusqlite::Result<(Vec<String>, i64)> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    // An absent cursor starts before every id: the empty string sorts first
    // and is compared with `>=` so an empty event id is not skipped.
    let (comparison, cursor) = match after {
        Some(after) => (">", after),
        None => (">=", ""),
    };
    let mut statement = conn.prepare(&format!(
        "SELECT event.event_id FROM pricing_pending
         CROSS JOIN usage_event event ON event.event_key=pricing_pending.event_key
         WHERE event.event_id {comparison} ?1
         ORDER BY event.event_id
         LIMIT ?2"
    ))?;
    let event_ids = statement
        .query_map(params![cursor, limit], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let steps = i64::from(statement.get_status(rusqlite::StatementStatus::VmStep));
    Ok((event_ids, steps))
}

/// The id of the event `chunk` events after `after` (or after the start when
/// `after` is absent), or `None` when fewer than `chunk` events remain. The
/// reconciliation walks the ledger in these id ranges so it never holds the
/// store for a whole-ledger scan at once. A chunk of zero events has no last
/// event, so the type rules it out rather than this answering for it.
pub fn reconcile_chunk_end(
    conn: &Connection,
    after: Option<&str>,
    chunk: std::num::NonZeroUsize,
) -> rusqlite::Result<Option<String>> {
    let offset = i64::try_from(chunk.get() - 1).unwrap_or(i64::MAX);
    let (comparison, cursor) = match after {
        Some(after) => (">", after),
        None => (">=", ""),
    };
    conn.query_row(
        &format!(
            "SELECT event_id FROM usage_event
             WHERE event_id {comparison} ?1
             ORDER BY event_id
             LIMIT 1 OFFSET ?2"
        ),
        params![cursor, offset],
        |row| row.get(0),
    )
    .optional()
}

/// Enqueue every event in the id range `(after, through]` that needs pricing
/// and is not already queued; either bound may be absent. Returns how many
/// were enqueued.
///
/// This is the old whole-ledger selection, kept as a check on the queue
/// rather than as the way work is found. Every event it enqueues is one that
/// some write path recorded without enqueuing, so a non-zero answer is a
/// defect to report, not routine work.
pub fn reconcile_pending_range_in_tx(
    conn: &Connection,
    after: Option<&str>,
    through: Option<&str>,
) -> rusqlite::Result<usize> {
    let (lower, lower_value) = match after {
        Some(after) => (">", after),
        None => (">=", ""),
    };
    let upper = if through.is_some() {
        "AND event.event_id <= ?2"
    } else {
        "AND ?2 IS NULL"
    };
    let headless = measurement_has_no_head!();
    conn.execute(
        &format!(
            "INSERT OR IGNORE INTO pricing_pending (event_key)
             SELECT DISTINCT measurement.event_key
             FROM usage_event event JOIN measurement USING(event_key)
             WHERE event.event_id {lower} ?1
               {upper}
               AND {headless}"
        ),
        params![lower_value, through],
    )
}

fn resolve_catalog_snapshot(
    plan: &RepricePlan,
    snapshot: &CatalogSnapshot,
    reasoning_billed_as_output: &[String],
) -> Result<ResolvedReprice, PricingError> {
    // Tier refusals leave `catalog_entry` NULL. That column holds
    // only what the catalog sent, as on a priced row; an object built here
    // (the model key with the tier thresholds or the unreadable tier key)
    // would read as the catalog's payload when it is this module's summary
    // of it. The row keeps `rate_tiered`, `catalog_source` and
    // `resolved_at_ms`, and the catalog serves its history, so asking it for
    // the event's provider and model at `resolved_at_ms` returns the exact
    // `.above_context.<threshold>` facts that decided the refusal.
    //
    // CatalogClient rejects malformed rate keys before constructing a snapshot.
    // Keep the fallback for an internally constructed snapshot: an unreadable
    // threshold must not silently select the base rate.
    let Ok(thresholds) = tier_thresholds(&snapshot.facts) else {
        return Ok(unpriced_all(
            plan,
            UnpricedReason::RateTiered,
            Some(snapshot.source.clone()),
            Some(snapshot.resolved_at_ms),
            None,
        ));
    };
    if !thresholds.is_empty() {
        // The threshold is tested against the whole prompt, measured from
        // every measurement of the event, not only the ones being priced
        // now. An unknown prompt cannot be placed below a threshold.
        let prompt = prompt_size(plan.all_measurements.iter().map(|measurement| {
            (
                measurement.role.as_str(),
                measurement.unit.as_str(),
                measurement.quantity_units,
                measurement.quantity_exponent,
            )
        }));
        let crosses_or_lacks_selector = prompt.is_none_or(|prompt| {
            thresholds
                .iter()
                .any(|threshold| prompt.exceeds(*threshold))
        });
        if crosses_or_lacks_selector {
            return Ok(unpriced_all(
                plan,
                UnpricedReason::RateTiered,
                Some(snapshot.source.clone()),
                Some(snapshot.resolved_at_ms),
                None,
            ));
        }
    }

    // This is an explicit billing declaration, not a fallback for a missing
    // reasoning rate. Keep the measurement's role while recording the rate key used.
    let reasoning_as_output = plan
        .provider
        .as_ref()
        .is_some_and(|provider| reasoning_billed_as_output.contains(provider));
    let mut measurements = Vec::with_capacity(plan.targets.len());
    for measurement in &plan.targets {
        let decision = match priced_quantity(plan, measurement) {
            None => ChargeDecision::Unpriced {
                reason: UnpricedReason::MissingQuantity,
                catalog_source: Some(snapshot.source.clone()),
                resolved_at_ms: Some(snapshot.resolved_at_ms),
                catalog_entry: None,
            },
            Some((quantity_units, quantity_exponent)) => resolve_measurement(
                snapshot,
                measurement,
                if measurement.role == "reasoning" && reasoning_as_output {
                    "output"
                } else {
                    &measurement.role
                },
                plan.mode.as_deref(),
                quantity_units,
                quantity_exponent,
            )?,
        };
        measurements.push(ResolvedMeasurement {
            role: measurement.role.clone(),
            unit: measurement.unit.clone(),
            supersedes: head_id(measurement),
            decision,
        });
    }
    resolved_currency(plan.currency.as_deref(), &measurements)?;
    Ok(ResolvedReprice::for_plan(plan, measurements))
}

fn resolve_measurement(
    snapshot: &CatalogSnapshot,
    measurement: &MeasurementPlan,
    rate_role: &str,
    recorded_mode: Option<&str>,
    quantity_units: i64,
    quantity_exponent: i32,
) -> Result<ChargeDecision, PricingError> {
    // A request recorded in a non-standard mode prices only at that mode's
    // rate; a model that serves none leaves it unpriced (`no_rate`) rather
    // than pricing it at the standard rate. A request recorded as standard,
    // or with no recorded mode, prices at the unconditional rate.
    let fact_key = rate_fact_key(rate_role, &measurement.unit).map(|base| {
        match stated_mode_condition(recorded_mode) {
            Some(mode) => mode_rate_key(rate_role, mode),
            None => base.to_string(),
        }
    });
    let Some(fact_key) = fact_key.as_deref() else {
        return Ok(ChargeDecision::Unpriced {
            reason: UnpricedReason::NoRate,
            catalog_source: Some(snapshot.source.clone()),
            resolved_at_ms: Some(snapshot.resolved_at_ms),
            catalog_entry: None,
        });
    };
    if let Some(withheld) = snapshot.withheld_facts.get(fact_key) {
        return Ok(ChargeDecision::Unpriced {
            reason: UnpricedReason::RateWithheld,
            catalog_source: Some(snapshot.source.clone()),
            resolved_at_ms: Some(snapshot.resolved_at_ms),
            catalog_entry: Some(json!({"fact_key": fact_key, "withheld": withheld})),
        });
    }
    let Some(entry) = snapshot.facts.get(fact_key) else {
        return Ok(ChargeDecision::Unpriced {
            reason: UnpricedReason::NoRate,
            catalog_source: Some(snapshot.source.clone()),
            resolved_at_ms: Some(snapshot.resolved_at_ms),
            catalog_entry: Some(json!({"fact_key": fact_key, "state": "absent"})),
        });
    };
    if let Some(uncertain) = snapshot.uncertain_facts.get(fact_key) {
        return Ok(ChargeDecision::Unpriced {
            reason: UnpricedReason::RateUncertain,
            catalog_source: Some(snapshot.source.clone()),
            resolved_at_ms: Some(snapshot.resolved_at_ms),
            catalog_entry: Some(json!({
                "fact_key": fact_key,
                "value": entry,
                "uncertain": uncertain,
            })),
        });
    }
    let mut provenance = json!({
        "fact_key": fact_key,
        "value": entry,
        "overridden": snapshot.overridden_facts.get(fact_key),
    });
    let rate = serde_json::from_value::<RateValue>(entry.clone()).map_err(|error| {
        CatalogClientError::InvalidRate {
            reason: error.to_string(),
            entry: entry.clone(),
        }
    })?;
    match rate {
        // Astrocyte reads the ordinary catalog, not a billing plane. Keep
        // refusing plane-only values rather than interpreting them as charges.
        RateValue::BilledAs { .. } => Err(CatalogClientError::InvalidRate {
            reason: "billing-plane rate is unsupported on an ordinary catalog read".into(),
            entry: entry.clone(),
        }
        .into()),
        // A borrowed rate is not this provider's price. fusiform serves it on
        // rows where the serving provider publishes nothing, taken from the
        // provider that published the model weights, and marks it. Usable as a
        // routing signal; as a charge it would record money the serving
        // provider never asked for. The origin travels in the detail and the
        // served bytes (with the marker) are already in catalog_entry.
        RateValue::Priced {
            inherited_from: Some(origin),
            ..
        }
        | RateValue::StatedZero {
            inherited_from: Some(origin),
        } => {
            // A borrowed zero is no more this provider's statement than a
            // borrowed price. Preserve its origin alongside the common fields.
            provenance["inherited_from"] = json!(origin);
            Ok(ChargeDecision::Unpriced {
                reason: UnpricedReason::RateInherited,
                catalog_source: Some(snapshot.source.clone()),
                resolved_at_ms: Some(snapshot.resolved_at_ms),
                catalog_entry: Some(provenance),
            })
        }
        RateValue::Priced {
            amount,
            floor,
            inherited_from: None,
        } => {
            if amount.exponent != NANO_EXPONENT {
                return Err(CatalogClientError::InvalidRate {
                    reason: format!(
                        "priced rate exponent {} is unsupported; expected {NANO_EXPONENT}",
                        amount.exponent
                    ),
                    entry: entry.clone(),
                }
                .into());
            }
            match price_exact(&amount, quantity_units, quantity_exponent) {
                Ok(charge_nanos) => Ok(ChargeDecision::Priced {
                    charge_nanos,
                    amount,
                    floor,
                    catalog_source: snapshot.source.clone(),
                    resolved_at_ms: snapshot.resolved_at_ms,
                    catalog_entry: provenance,
                }),
                Err(MoneyError::OutOfRange) => Ok(ChargeDecision::Unpriced {
                    reason: UnpricedReason::ArithmeticOutOfRange,
                    catalog_source: Some(snapshot.source.clone()),
                    resolved_at_ms: Some(snapshot.resolved_at_ms),
                    catalog_entry: Some(provenance),
                }),
            }
        }
        RateValue::StatedZero {
            inherited_from: None,
        } => Ok(ChargeDecision::Unpriced {
            reason: UnpricedReason::StatedZeroRequiresPolicy,
            catalog_source: Some(snapshot.source.clone()),
            resolved_at_ms: Some(snapshot.resolved_at_ms),
            catalog_entry: Some(provenance),
        }),
        // The catalog already says unpriced, so an `inherited_from` marker
        // changes nothing: the reason is mapped from the catalog's own reason
        // either way, and the marker survives in the served bytes kept in
        // catalog_entry.
        RateValue::Unpriced {
            reason,
            inherited_from: _,
        } => Ok(ChargeDecision::Unpriced {
            reason: match reason {
                ProtocolUnpricedReason::MissingRate => UnpricedReason::NoRate,
                ProtocolUnpricedReason::NoCatalogCoverage => UnpricedReason::NoCatalogCoverage,
                ProtocolUnpricedReason::UnknownChargeBasis => {
                    UnpricedReason::CatalogUnknownChargeBasis
                }
                ProtocolUnpricedReason::NotEstablished => {
                    return Err(CatalogClientError::InvalidRate {
                        reason: "billing-plane rate is unsupported on an ordinary catalog read"
                            .into(),
                        entry: entry.clone(),
                    }
                    .into());
                }
            },
            catalog_source: Some(snapshot.source.clone()),
            resolved_at_ms: Some(snapshot.resolved_at_ms),
            catalog_entry: Some(provenance),
        }),
    }
}

/// The quantity a target measurement is charged at: its stored quantity,
/// except that an inclusive BROCA output is charged at
/// [`effective_output`]. `None` means there is no quantity to charge: the
/// measurement has none, or its stored output and reasoning contradict each
/// other.
fn priced_quantity(plan: &RepricePlan, measurement: &MeasurementPlan) -> Option<(i64, i32)> {
    let (units, exponent) = measurement
        .quantity_units
        .zip(measurement.quantity_exponent)?;
    if measurement.role != "output" || measurement.unit != "token" || exponent != 0 {
        return Some((units, exponent));
    }
    // The reasoning is read from every measurement of the event, not only the
    // ones being priced now: an already priced reasoning head still holds the
    // tokens that the inclusive output repeats.
    let reasoning = plan
        .all_measurements
        .iter()
        .find(|other| other.role == "reasoning" && other.unit == "token")
        .and_then(
            |other| match (other.quantity_units, other.quantity_exponent) {
                (Some(units), Some(0)) => Some(units),
                _ => None,
            },
        );
    match effective_output(
        &plan.producer,
        plan.provider.as_deref(),
        &measurement.source,
        units,
        reasoning,
    ) {
        EffectiveOutput::AsStored => Some((units, exponent)),
        EffectiveOutput::ReasoningRemoved(effective) => Some((effective, 0)),
        EffectiveOutput::ReasoningExceedsOutput => None,
    }
}

fn price_exact(
    amount: &Amount,
    quantity_units: i64,
    quantity_exponent: i32,
) -> Result<i64, MoneyError> {
    let product = i128::from(amount.units)
        .checked_mul(i128::from(quantity_units))
        .ok_or(MoneyError::OutOfRange)?;
    let divisor_exponent = u32::try_from(RATE_UNIT_EXPONENT - quantity_exponent)
        .map_err(|_| MoneyError::OutOfRange)?;
    let divisor = 10_i128
        .checked_pow(divisor_exponent)
        .ok_or(MoneyError::OutOfRange)?;
    round_rational_nanos(product, divisor)
}

/// Converts an exact nonnegative rational to `round_nano_num`'s fixed scale
/// without an earlier rounding. The sub-micro remainder is mapped to the same
/// side of the half boundary, so the one representation rounding still occurs
/// in `round_nano_num`.
fn round_rational_nanos(numerator: i128, divisor: i128) -> Result<i64, MoneyError> {
    if numerator < 0 || divisor <= 0 {
        return Err(MoneyError::OutOfRange);
    }
    let whole = numerator / divisor;
    let remainder = numerator % divisor;
    let mut scaled_remainder = remainder
        .checked_mul(NUMERATOR_SCALE)
        .ok_or(MoneyError::OutOfRange)?
        / divisor;
    if scaled_remainder == NUMERATOR_SCALE / 2
        && remainder.checked_mul(2).ok_or(MoneyError::OutOfRange)? > divisor
    {
        scaled_remainder += 1;
    }
    let normalized = whole
        .checked_mul(NUMERATOR_SCALE)
        .and_then(|value| value.checked_add(scaled_remainder))
        .ok_or(MoneyError::OutOfRange)?;
    round_nano_num(normalized)
}

fn rate_fact_key(role: &str, unit: &str) -> Option<&'static str> {
    if unit != "token" {
        return None;
    }
    match role {
        "input" => Some("rate.input"),
        "output" => Some("rate.output"),
        "cache_read" => Some("rate.cache_read"),
        "cache_write" => Some("rate.cache_write"),
        "reasoning" => Some("rate.reasoning"),
        _ => None,
    }
}

/// Roles whose tokens are part of the prompt a request sends.
const PROMPT_ROLES: [&str; 3] = ["input", "cache_read", "cache_write"];

/// Units a prompt role is stored in. Claude Code records a cache write split
/// by cache lifetime as `token_cache_5m` and `token_cache_1h` (and a plain
/// `token` cache write only when the record carries no split, so the three
/// never repeat each other). A cache write is sent as prompt whatever its
/// lifetime, so the split units count toward the prompt even though no
/// catalog rate prices them.
const PROMPT_UNITS: [&str; 3] = ["token", "token_cache_5m", "token_cache_1h"];

/// Power of ten a stored token quantity may carry, at the finest.
const PROMPT_SCALE_EXPONENT: u32 = 9;

/// The size of one request's prompt, held exactly: stored quantities may
/// carry a negative decimal exponent, so the sum is kept in units of
/// 10^-9 tokens rather than rounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PromptSize {
    scaled_tokens: i128,
}

impl PromptSize {
    /// Whether the prompt is larger than an `.above_context.<threshold>` tier's
    /// threshold, which is when the tier's rates apply.
    pub(crate) fn exceeds(self, threshold: u64) -> bool {
        self.scaled_tokens > i128::from(threshold) * 10_i128.pow(PROMPT_SCALE_EXPONENT)
    }
}

/// The prompt of one request: the sum of its `input`, `cache_read` and
/// `cache_write` token measurements (see [`PROMPT_UNITS`]). A long-context
/// tier's threshold applies to the whole prompt, not to uncached input alone,
/// so a request whose input is small but whose cache read is large still
/// crosses it. Ledger pricing and the multiplier both decide tiers with this,
/// so neither prices a request at a rate the other would not.
///
/// Each item is `(role, unit, quantity units, quantity exponent)`. A prompt
/// measurement the event does not carry counts as zero. One it carries with
/// no quantity, a negative quantity, or an exponent outside `-9..=0` makes
/// the prompt unknown (`None`): its size cannot be summed, and a caller that
/// must decide a tier refuses rather than assume the base rate.
pub(crate) fn prompt_size<'a>(
    measurements: impl IntoIterator<Item = (&'a str, &'a str, Option<i64>, Option<i32>)>,
) -> Option<PromptSize> {
    let mut scaled_tokens: i128 = 0;
    for (role, unit, units, exponent) in measurements {
        if !PROMPT_ROLES.contains(&role) || !PROMPT_UNITS.contains(&unit) {
            continue;
        }
        let (units, exponent) = units.zip(exponent)?;
        if units < 0 {
            return None;
        }
        let scale = PROMPT_SCALE_EXPONENT.checked_add_signed(exponent)?;
        if scale > PROMPT_SCALE_EXPONENT {
            return None;
        }
        let scaled = i128::from(units).checked_mul(10_i128.pow(scale))?;
        scaled_tokens = scaled_tokens.checked_add(scaled)?;
    }
    Some(PromptSize { scaled_tokens })
}

fn unpriced_all(
    plan: &RepricePlan,
    reason: UnpricedReason,
    catalog_source: Option<String>,
    resolved_at_ms: Option<i64>,
    catalog_entry: Option<Value>,
) -> ResolvedReprice {
    ResolvedReprice::for_plan(
        plan,
        plan.targets
            .iter()
            .map(|measurement| ResolvedMeasurement {
                role: measurement.role.clone(),
                unit: measurement.unit.clone(),
                supersedes: head_id(measurement),
                decision: ChargeDecision::Unpriced {
                    reason,
                    catalog_source: catalog_source.clone(),
                    resolved_at_ms,
                    catalog_entry: catalog_entry.clone(),
                },
            })
            .collect(),
    )
}

fn head_id(measurement: &MeasurementPlan) -> Option<i64> {
    measurement
        .current_head
        .as_ref()
        .map(|head| head.charge_key)
}

fn resolved_currency(
    current: Option<&str>,
    measurements: &[ResolvedMeasurement],
) -> Result<Option<String>, PricingError> {
    let mut resolved = current.map(str::to_string);
    for measurement in measurements {
        let candidate = match &measurement.decision {
            ChargeDecision::Priced { amount, .. } => Some(amount.currency.as_str()),
            ChargeDecision::Covered { currency } => Some(currency.as_str()),
            ChargeDecision::Unpriced { .. } => None,
        };
        if let Some(candidate) = candidate {
            match resolved.as_deref() {
                Some(existing) if existing != candidate => {
                    return Err(PricingError::Refused(
                        PricingRefusal::CatalogCurrencyMismatch,
                    ));
                }
                None => resolved = Some(candidate.to_string()),
                _ => {}
            }
        }
    }
    Ok(resolved)
}

fn parse_head_outcome(value: &str) -> rusqlite::Result<HeadOutcome> {
    match value {
        "priced" => Ok(HeadOutcome::Priced),
        "covered" => Ok(HeadOutcome::Covered),
        "unpriced" => Ok(HeadOutcome::Unpriced),
        _ => Err(stored_integrity_error(
            5,
            Type::Text,
            format!("unknown priced_charge.outcome '{value}'"),
        )),
    }
}

fn stored_integrity_error(column: usize, kind: Type, detail: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        kind,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, detail)),
    )
}

#[cfg(test)]
mod tests {

    #[test]
    fn corrupt_head_outcome_names_the_stored_value() {
        let error = parse_head_outcome("broken").unwrap_err();
        assert!(matches!(
            error,
            rusqlite::Error::FromSqlConversionFailure(5, Type::Text, _)
        ));
        assert!(error
            .to_string()
            .contains("unknown priced_charge.outcome 'broken'"));
        assert_eq!(parse_head_outcome("priced").unwrap(), HeadOutcome::Priced);
    }

    #[test]
    fn a_resolution_for_another_plan_writes_no_charges_or_generation() {
        let mut conn = conn();
        let first = record(
            &mut conn,
            "first-plan",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let second = record(
            &mut conn,
            "second-plan",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(2_000_000))],
        );
        let first_plan = read_reprice(&conn, &first).unwrap();
        let second_plan = read_reprice(&conn, &second).unwrap();
        let resolved = resolve_reprice(
            &second_plan,
            &client(FakeCatalog::new(BTreeMap::from([(
                "rate.input".into(),
                rate(3_000_000_000),
            )]))),
            &[],
        )
        .unwrap();
        let tx = conn.transaction().unwrap();
        let result = commit_reprice_in_tx(
            &tx,
            &first_plan,
            &resolved,
            1_000,
            PricingOrigin::OperatorReprice,
        );
        let rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row.get(0))
            .unwrap();
        let generation: i64 = tx
            .query_row(
                "SELECT pricing_generation FROM usage_event WHERE event_id=?1",
                [&first],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            (rows, generation),
            (0, 0),
            "a foreign resolution must not write money or advance the plan"
        );
        assert!(matches!(
            result,
            Err(PricingError::Refused(PricingRefusal::Malformed))
        ));
        tx.commit().unwrap();
        assert_eq!(pending_event_ids(&conn, None, 10).unwrap().0.len(), 2);
    }

    #[test]
    fn charge_rows_preserve_write_path_origin() {
        let origins = [PricingOrigin::PricingPass, PricingOrigin::OperatorReprice].map(|origin| {
            let mut conn = conn();
            let event_id = record(
                &mut conn,
                "same-event",
                ChargeBasis::MeteredApi,
                Some(700),
                vec![measurement("input", Some(1_000_000))],
            );
            let fake =
                FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
            reprice_event(&mut conn, &event_id, &client(fake), 1_000, origin).unwrap();
            conn.query_row(
                "SELECT origin FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |r| r.get::<_, String>(0),
            )
            .unwrap()
        });
        assert_eq!(origins, ["pricing_pass", "operator_reprice"]);
    }

    #[test]
    fn production_origin_literals_reserve_unattributed_for_migration() {
        // Grep is sufficient for this closed literal set, not for behaviour:
        // PricingOrigin has only live variants and every writer must supply one.
        fn scan(dir: &std::path::Path, hits: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    scan(&path, hits);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    let production = text.split("#[cfg(test)]").next().unwrap();
                    for literal in ["\"unattributed\"", "'unattributed'"] {
                        if production.contains(literal) {
                            assert!(
                                path.ends_with("astrocyte-core/src/store.rs"),
                                "historical origin literal outside migration: {}",
                                path.display()
                            );
                            let migrations =
                                production.split("pub const MIGRATIONS:").nth(1).unwrap();
                            assert_eq!(
                                production.matches(literal).count(),
                                migrations.matches(literal).count()
                            );
                            hits.push(path.clone());
                        }
                    }
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        let mut hits = Vec::new();
        scan(root, &mut hits);
        assert!(
            !hits.is_empty(),
            "the historical migration must be enumerated"
        );
    }

    fn correction_request(
        event_id: &str,
        basis: ChargeBasis,
    ) -> crate::ingest::BasisCorrectRequest {
        crate::ingest::BasisCorrectRequest {
            event_id: event_id.into(),
            corrected_basis: basis,
            reason: "classification evidence".into(),
        }
    }

    fn correct(conn: &mut Connection, event_id: &str, basis: ChargeBasis, catalog: &CatalogClient) {
        let plan =
            crate::subscription::read_basis_correction(conn, &correction_request(event_id, basis))
                .unwrap();
        assert!(conn.is_autocommit());
        let resolved = crate::subscription::resolve_reprice(&plan, catalog, &[]).unwrap();
        let tx = conn.transaction().unwrap();
        crate::ingest::correct_basis_in_tx(&tx, &plan, &resolved, 1100).unwrap();
        tx.commit().unwrap();
    }

    fn charge_rows(conn: &Connection) -> Vec<Vec<rusqlite::types::Value>> {
        let mut stmt = conn
            .prepare("SELECT * FROM priced_charge ORDER BY charge_key")
            .unwrap();
        let columns = stmt.column_count();
        stmt.query_map([], |row| (0..columns).map(|i| row.get(i)).collect())
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn correction_head(
        conn: &Connection,
        event_id: &str,
    ) -> (String, String, i64, Option<i64>, Option<String>, String) {
        conn.query_row("SELECT charge_basis,outcome,basis_covered,charge_nanos,unpriced_reason,origin FROM priced_charge p WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) AND NOT EXISTS (SELECT 1 FROM priced_charge c WHERE c.supersedes=p.charge_key)", [event_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).unwrap()
    }

    #[test]
    fn basis_correction_metered_prices_and_preserves_catalog_reason() {
        for (facts, expected_money, expected_reason) in [
            (
                BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]),
                Some(3_000_000_000),
                None,
            ),
            (BTreeMap::new(), None, Some("no_rate")),
        ] {
            let mut conn = conn();
            let id = record(
                &mut conn,
                "correct",
                ChargeBasis::Unknown,
                Some(700),
                vec![measurement("input", Some(1_000_000))],
            );
            let fake = FakeCatalog::new(facts);
            let catalog = client(fake.clone());
            let before = fake.calls.load(Ordering::SeqCst);
            correct(&mut conn, &id, ChargeBasis::MeteredApi, &catalog);
            assert!(fake.calls.load(Ordering::SeqCst) > before);
            assert_eq!(
                correction_head(&conn, &id),
                (
                    "metered_api".into(),
                    if expected_money.is_some() {
                        "priced"
                    } else {
                        "unpriced"
                    }
                    .into(),
                    0,
                    expected_money,
                    expected_reason.map(str::to_owned),
                    "basis_correction".into()
                )
            );
        }
    }

    #[test]
    fn basis_correction_subscription_is_covered_without_catalog() {
        let mut conn = conn();
        let tx = conn.transaction().unwrap();
        crate::subscription::set_subscription_in_tx(
            &tx,
            &crate::subscription::SetSubscriptionRequest {
                account_ref: "account".into(),
                provider: "provider".into(),
                plan_name: "plan".into(),
                recurring_cost_minor: 100,
                currency: "USD".into(),
                amount_exponent: 2,
                period: crate::subscription::SubscriptionPeriod::Month,
                effective_from: 1,
            },
            None,
        )
        .unwrap();
        tx.commit().unwrap();
        let id = record(
            &mut conn,
            "correct",
            ChargeBasis::Unknown,
            Some(700),
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::new());
        let catalog = client(fake.clone());
        let before = fake.calls.load(Ordering::SeqCst);
        correct(&mut conn, &id, ChargeBasis::SubscriptionIncluded, &catalog);
        assert_eq!(fake.calls.load(Ordering::SeqCst), before);
        assert_eq!(
            correction_head(&conn, &id),
            (
                "subscription_included".into(),
                "covered".into(),
                1,
                Some(0),
                None,
                "basis_correction".into()
            )
        );
    }

    #[test]
    fn basis_correction_unknown_is_unpriced_without_catalog() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "correct",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::new());
        let catalog = client(fake.clone());
        let before = fake.calls.load(Ordering::SeqCst);
        correct(&mut conn, &id, ChargeBasis::Unknown, &catalog);
        assert_eq!(fake.calls.load(Ordering::SeqCst), before);
        assert_eq!(
            correction_head(&conn, &id),
            (
                "unknown".into(),
                "unpriced".into(),
                0,
                None,
                Some("charge_basis_unknown".into()),
                "basis_correction".into()
            )
        );
    }

    #[test]
    fn basis_correction_supersedes_every_head_preserving_every_prior_column() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "correct",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(1_000_000)),
                measurement("output", Some(2_000_000)),
            ],
        );
        let catalog = client(FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000)),
            ("rate.output".into(), rate(4_000_000_000)),
        ])));
        reprice_event(&mut conn, &id, &catalog, 1000, PricingOrigin::PricingPass).unwrap();
        let before = charge_rows(&conn);
        assert_eq!(before.len(), 2);
        correct(&mut conn, &id, ChargeBasis::Unknown, &catalog);
        let after = charge_rows(&conn);
        assert_eq!(after.len(), 4);
        for prior in before {
            assert!(
                after.contains(&prior),
                "every column of a prior charge must remain unchanged"
            );
            let (basis, origin): (String, String) = conn
                .query_row(
                    "SELECT charge_basis,origin FROM priced_charge WHERE supersedes=?1",
                    [&prior[0]],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(
                (basis, origin),
                ("unknown".into(), "basis_correction".into())
            );
        }
        let projection: (String, i64) = conn
            .query_row(
                "SELECT charge_basis,pricing_generation FROM usage_event WHERE event_id=?1",
                [&id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(projection, ("unknown".into(), 2));
    }

    #[test]
    fn basis_correction_undated_remains_undated() {
        for basis in [ChargeBasis::Unknown, ChargeBasis::SubscriptionIncluded] {
            let mut conn = conn();
            let id = record(
                &mut conn,
                "correct",
                ChargeBasis::MeteredApi,
                None,
                vec![measurement("input", Some(1))],
            );
            correct(
                &mut conn,
                &id,
                basis,
                &client(FakeCatalog::new(BTreeMap::new())),
            );
            let head = correction_head(&conn, &id);
            assert_eq!(head.0, basis.as_str());
            assert_eq!(head.4.as_deref(), Some("undated_event"));
        }
    }

    #[test]
    fn basis_correction_same_basis_refuses_without_writes() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "correct",
            ChargeBasis::Unknown,
            Some(700),
            vec![measurement("input", Some(1))],
        );
        let error = crate::subscription::read_basis_correction(
            &conn,
            &correction_request(&id, ChargeBasis::Unknown),
        )
        .unwrap_err();
        assert_eq!(error.code(), "basis_unchanged");
        assert!(charge_rows(&conn).is_empty());
    }

    #[test]
    fn basis_correction_rolls_back_projection_when_append_fails() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "correct",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(1)),
                measurement("output", Some(1)),
            ],
        );
        conn.execute_batch("CREATE TRIGGER kill_correction BEFORE INSERT ON priced_charge WHEN NEW.role='output' BEGIN SELECT RAISE(ABORT,'kill_between_projection_and_append'); END;").unwrap();
        let plan = crate::subscription::read_basis_correction(
            &conn,
            &correction_request(&id, ChargeBasis::Unknown),
        )
        .unwrap();
        let resolved = crate::subscription::resolve_reprice_without_catalog(&plan).unwrap();
        {
            let tx = conn.transaction().unwrap();
            let error =
                crate::ingest::correct_basis_in_tx(&tx, &plan, &resolved, 1100).unwrap_err();
            assert!(error
                .message()
                .contains("kill_between_projection_and_append"));
        }
        assert!(charge_rows(&conn).is_empty());
        let projection: (String, i64) = conn
            .query_row(
                "SELECT charge_basis,pricing_generation FROM usage_event WHERE event_id=?1",
                [&id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(projection, ("metered_api".into(), 0));
    }

    #[test]
    fn basis_correction_stale_resolution_refuses_without_writes() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "correct",
            ChargeBasis::Unknown,
            Some(700),
            vec![measurement("input", Some(1))],
        );
        let plan = crate::subscription::read_basis_correction(
            &conn,
            &correction_request(&id, ChargeBasis::MeteredApi),
        )
        .unwrap();
        let catalog = client(FakeCatalog::new(BTreeMap::new()));
        let resolved = crate::subscription::resolve_reprice(&plan, &catalog, &[]).unwrap();
        reprice_event(&mut conn, &id, &catalog, 1000, PricingOrigin::PricingPass).unwrap();
        let before = charge_rows(&conn);
        {
            let tx = conn.transaction().unwrap();
            let error =
                crate::ingest::correct_basis_in_tx(&tx, &plan, &resolved, 1100).unwrap_err();
            assert_eq!(error.code(), "pricing_superseded");
        }
        assert_eq!(charge_rows(&conn), before);
    }

    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use fusiform_protocol::{
        CatalogGetRequest, CatalogGetResponse, UncertainFactWire, WithheldFactWire,
    };
    use rusqlite::Connection;

    use super::*;
    use crate::catalog_client::{CatalogCallError, CatalogConnection};
    use crate::ingest::{record_event, EventRecordRequest, MeasurementInput};
    use crate::store::{initialize_connection, MIGRATIONS};

    /// An event's pricing state as its current charge heads describe it; the
    /// tests read it through `event_pricing_outcome` to check what repricing left.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum EventPricingOutcome {
        Priced {
            measurement_count: usize,
            charge_nanos: i64,
        },
        Unpriced {
            reason: HeadSummaryReason,
            measurement_count: usize,
            unpriced_measurements: usize,
            partial_charge_nanos: i64,
        },
    }

    /// Why `event_pricing_outcome` calls an event unpriced. This is the test
    /// helper's own summary of the heads, not a charge-row reason: no charge
    /// row is ever written with `NoMeasurements`, because an event with no
    /// measurement has nothing to write a row for.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum HeadSummaryReason {
        NoMeasurements,
        NoRate,
    }

    /// Reads a pricing plan through `read_reprice_in_tx` in its own read
    /// transaction. A measurement with no charge head is the initial unpriced
    /// state; recording and pricing are deliberately separate acts.
    fn read_reprice(conn: &Connection, event_id: &str) -> Result<RepricePlan, PricingError> {
        let tx = conn.unchecked_transaction()?;
        let plan = read_reprice_in_tx(&tx, event_id)?;
        tx.commit()?;
        Ok(plan)
    }

    /// Reprices one event through this module's read, resolve and commit steps,
    /// the same steps the subscription layer composes for the served reprice
    /// operation, without that layer's coverage re-check.
    fn reprice_event(
        conn: &mut Connection,
        event_id: &str,
        catalog: &CatalogClient,
        recorded_at_ms: i64,
        origin: PricingOrigin,
    ) -> Result<RepriceOutcome, PricingError> {
        let plan = read_reprice(conn, event_id)?;
        let resolved = resolve_reprice(&plan, catalog, &[])?;
        let tx = conn.transaction()?;
        let outcome = commit_reprice_in_tx(&tx, &plan, &resolved, recorded_at_ms, origin)?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Derives event state from measurement current heads. Row charges are summed
    /// after their independent representation rounding and are never re-rounded.
    fn event_pricing_outcome(
        conn: &Connection,
        event_id: &str,
    ) -> Result<EventPricingOutcome, PricingError> {
        let event_exists = conn
            .query_row(
                "SELECT 1 FROM usage_event WHERE event_id=?1",
                [event_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !event_exists {
            return Err(PricingError::Refused(PricingRefusal::NotFound));
        }
        let mut statement = conn.prepare(
            "SELECT pc.outcome,pc.charge_nanos
             FROM measurement m
             LEFT JOIN priced_charge pc
               ON pc.event_key=m.event_key AND pc.role=m.role AND pc.unit=m.unit
              AND NOT EXISTS (
                  SELECT 1 FROM priced_charge child WHERE child.supersedes=pc.charge_key
              )
             WHERE m.event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
        )?;
        let rows = statement
            .query_map([event_id], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.is_empty() {
            return Ok(EventPricingOutcome::Unpriced {
                reason: HeadSummaryReason::NoMeasurements,
                measurement_count: 0,
                unpriced_measurements: 0,
                partial_charge_nanos: 0,
            });
        }
        let mut total = 0_i128;
        let mut unpriced = 0;
        for (outcome, charge_nanos) in &rows {
            match outcome.as_deref() {
                Some("priced") => {
                    total = total
                        .checked_add(i128::from(
                            charge_nanos.ok_or(rusqlite::Error::InvalidQuery)?,
                        ))
                        .ok_or(PricingError::Refused(PricingRefusal::Malformed))?;
                }
                Some("covered") => {}
                Some("unpriced") | None => unpriced += 1,
                Some(_) => return Err(PricingError::Store(rusqlite::Error::InvalidQuery)),
            }
        }
        let total =
            i64::try_from(total).map_err(|_| PricingError::Refused(PricingRefusal::Malformed))?;
        if unpriced == 0 {
            Ok(EventPricingOutcome::Priced {
                measurement_count: rows.len(),
                charge_nanos: total,
            })
        } else {
            Ok(EventPricingOutcome::Unpriced {
                reason: HeadSummaryReason::NoRate,
                measurement_count: rows.len(),
                unpriced_measurements: unpriced,
                partial_charge_nanos: total,
            })
        }
    }

    struct FakeCatalog {
        response: Mutex<CatalogGetResponse>,
        calls: AtomicUsize,
        requests: Mutex<Vec<CatalogGetRequest>>,
    }

    impl FakeCatalog {
        fn new(facts: BTreeMap<String, Value>) -> Arc<Self> {
            Arc::new(Self {
                response: Mutex::new(CatalogGetResponse {
                    source: "models.dev".into(),
                    resolved_at_ms: 1_000,
                    catalog_version: 7,
                    models: BTreeMap::from([("provider/model".into(), facts)]),
                    withheld: vec![],
                    uncertain: vec![],
                    overridden: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
                }),
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            })
        }

        fn set_facts(&self, facts: BTreeMap<String, Value>) {
            self.response.lock().unwrap().models =
                BTreeMap::from([("provider/model".into(), facts)]);
        }

        fn set_source(&self, source: &str) {
            self.response.lock().unwrap().source = source.into();
        }
    }

    impl CatalogConnection for FakeCatalog {
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
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            let mut response = self.response.lock().unwrap().clone();
            response.resolved_at_ms = request.at_ms.unwrap_or(response.resolved_at_ms);
            Ok(response)
        }
    }

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    /// A chunk of `n` ends at the `n`th event after the cursor, so a chunk of
    /// one ends at the very next event; a chunk longer than what remains has
    /// no end and is the walk's last.
    #[test]
    fn a_reconcile_chunk_ends_at_its_nth_event_and_none_past_the_end() {
        let conn = conn();
        for event_id in ["ev-a", "ev-b", "ev-c"] {
            conn.execute(
                "INSERT INTO usage_event (event_id,producer,producer_ref,recorded_at_ms,charge_basis)
                 VALUES (?1,'broca',?1,1,'unknown')",
                [event_id],
            )
            .unwrap();
        }
        let chunk = |events: usize| std::num::NonZeroUsize::new(events).unwrap();
        let end = |after: Option<&str>, events: usize| {
            reconcile_chunk_end(&conn, after, chunk(events)).unwrap()
        };
        assert_eq!(end(None, 1).as_deref(), Some("ev-a"));
        assert_eq!(end(Some("ev-a"), 1).as_deref(), Some("ev-b"));
        assert_eq!(end(None, 3).as_deref(), Some("ev-c"));
        assert_eq!(end(None, 4), None);
        assert_eq!(end(Some("ev-b"), 2), None);
    }

    fn measurement(role: &str, quantity_units: Option<i64>) -> MeasurementInput {
        MeasurementInput {
            role: role.into(),
            unit: "token".into(),
            kind: "billed".into(),
            source: format!("usage.{role}"),
            quantity_units,
            quantity_exponent: quantity_units.map(|_| 0),
        }
    }

    #[test]
    fn declared_reasoning_uses_output_rates_and_records_the_resolved_key() {
        for (mode, key, nanos) in [
            (None, "rate.output", 3_000_000),
            (Some("fast"), "rate.output.mode.fast", 6_000_000),
        ] {
            let mut conn = conn();
            let event_id = record(
                &mut conn,
                "thinking",
                ChargeBasis::MeteredApi,
                Some(700),
                vec![measurement("reasoning", Some(1_000))],
            );
            // A reasoning rate must not override the provider's declared billing rule.
            let fake = FakeCatalog::new(BTreeMap::from([
                ("rate.output".into(), rate(3_000_000_000)),
                ("rate.output.mode.fast".into(), rate(6_000_000_000)),
                ("rate.reasoning".into(), rate(9_000_000_000)),
                ("rate.reasoning.mode.fast".into(), rate(12_000_000_000)),
            ]));
            let mut plan = read_reprice(&conn, &event_id).unwrap();
            plan.mode = mode.map(str::to_owned);
            let resolved = resolve_reprice(&plan, &client(fake), &["provider".into()]).unwrap();
            let tx = conn.transaction().unwrap();
            commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
            tx.commit().unwrap();
            let stored: (String, i64, String) = conn.query_row(
                "SELECT outcome,charge_nanos,json_extract(catalog_entry,'$.fact_key') FROM priced_charge",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
            assert_eq!(stored, ("priced".into(), nanos, key.into()));
        }
    }

    #[test]
    fn unlisted_reasoning_without_its_own_rate_stays_no_rate() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "thinking",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("reasoning", Some(1_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([(
            "rate.output".into(),
            rate(3_000_000_000),
        )]));
        let plan = read_reprice(&conn, &event_id).unwrap();
        let resolved = resolve_reprice(&plan, &client(fake), &["another-provider".into()]).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        let stored: (String, Option<i64>, String, String) = conn.query_row(
            "SELECT outcome,charge_nanos,unpriced_reason,json_extract(catalog_entry,'$.fact_key') FROM priced_charge",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
        assert_eq!(
            stored,
            (
                "unpriced".into(),
                None,
                "no_rate".into(),
                "rate.reasoning".into()
            )
        );
    }

    #[test]
    fn declared_reasoning_above_context_is_refused_like_output() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "thinking",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(201)),
                measurement("output", Some(50)),
                measurement("reasoning", Some(100)),
            ],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.output".into(), rate(3_000_000_000)),
            ("rate.output.above_context.200".into(), rate(6_000_000_000)),
        ]));
        let plan = read_reprice(&conn, &event_id).unwrap();
        let resolved = resolve_reprice(&plan, &client(fake), &["provider".into()]).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        let rows: Vec<(String, String, Option<i64>, String)> = conn
            .prepare(
                "SELECT role,outcome,charge_nanos,unpriced_reason FROM priced_charge ORDER BY role",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            ["input", "output", "reasoning"].map(|role| (
                role.into(),
                "unpriced".into(),
                None,
                "rate_tiered".into()
            ))
        );
    }

    fn record(
        conn: &mut Connection,
        producer_ref: &str,
        basis: ChargeBasis,
        occurred_at_ms: Option<i64>,
        measurements: Vec<MeasurementInput>,
    ) -> String {
        record_for_provider(
            conn,
            producer_ref,
            "provider",
            basis,
            occurred_at_ms,
            measurements,
        )
    }

    fn record_for_provider(
        conn: &mut Connection,
        producer_ref: &str,
        provider: &str,
        basis: ChargeBasis,
        occurred_at_ms: Option<i64>,
        measurements: Vec<MeasurementInput>,
    ) -> String {
        record_event(
            conn,
            &EventRecordRequest {
                producer: "producer".into(),
                producer_lineage: Some("lineage".into()),
                producer_ref: producer_ref.into(),
                prior_event_ref: None,
                harness: None,
                project_root: None,
                session: None,
                run_ref: None,
                flow_ref: None,
                work_class: None,
                occurred_at_ms,
                emitted_at_ms: None,
                terminal_status: Some("failed".into()),
                attempt_ordinal: None,
                vendor_request_id: None,
                provider: Some(provider.into()),
                model: Some("model".into()),
                account_ref: Some("account".into()),
                auth_selection: None,
                charge_basis_mapping_version: None,
                charge_basis_overrides: None,
                charge_basis: basis,
                measurements,
            },
            900,
        )
        .unwrap()
        .event_id
    }

    fn rate(units: i64) -> Value {
        json!({
            "state": "priced",
            "units": units,
            "exponent": 9,
            "currency": "USD",
            "unit_provenance": {"kind": "stated"}
        })
    }

    fn client(fake: Arc<FakeCatalog>) -> CatalogClient {
        CatalogClient::new(fake, 500)
    }

    fn count(conn: &Connection, event_id: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
            [event_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn no_measurements_to_price_refuses_without_stored_rows() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "empty",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![],
        );
        let fake = FakeCatalog::new(BTreeMap::new());
        assert!(matches!(
            reprice_event(
                &mut conn,
                &event_id,
                &client(fake),
                1_000,
                PricingOrigin::PricingPass
            ),
            Err(PricingError::Refused(PricingRefusal::NoMeasurementsToPrice))
        ));
        assert_eq!(count(&conn, &event_id), 0);
        assert_eq!(
            event_pricing_outcome(&conn, &event_id).unwrap(),
            EventPricingOutcome::Unpriced {
                reason: HeadSummaryReason::NoMeasurements,
                measurement_count: 0,
                unpriced_measurements: 0,
                partial_charge_nanos: 0,
            }
        );
    }

    fn assert_local_unpriced_without_catalog(
        name: &str,
        basis: ChargeBasis,
        occurred_at_ms: Option<i64>,
        reason: UnpricedReason,
    ) {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            name,
            basis,
            occurred_at_ms,
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::new());
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
        let stored: String = conn
            .query_row(
                "SELECT unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, reason.as_str());
    }

    #[test]
    fn undated_event_is_unpriced_without_catalog_io() {
        assert_local_unpriced_without_catalog(
            "undated",
            ChargeBasis::MeteredApi,
            None,
            UnpricedReason::UndatedEvent,
        );
    }

    #[test]
    fn undated_deepseek_event_is_unpriced_before_coverage_gap_check() {
        let mut conn = conn();
        let event_id = record_for_provider(
            &mut conn,
            "undated-deepseek",
            "deepseek",
            ChargeBasis::MeteredApi,
            None,
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
        let reason: String = conn
            .query_row(
                "SELECT unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reason, "undated_event");
    }

    #[test]
    fn unknown_charge_basis_is_unpriced_without_catalog_io() {
        assert_local_unpriced_without_catalog(
            "unknown",
            ChargeBasis::Unknown,
            Some(700),
            UnpricedReason::ChargeBasisUnknown,
        );
    }

    #[test]
    fn subscription_without_plan_is_unpriced_without_catalog_io() {
        assert_local_unpriced_without_catalog(
            "subscription",
            ChargeBasis::SubscriptionIncluded,
            Some(700),
            UnpricedReason::NoSubscriptionInForce,
        );
    }

    #[test]
    fn deepseek_before_time_banded_pricing_reaches_flat_catalog_rate() {
        let mut conn = conn();
        let before_band = CATALOG_COVERAGE_GAPS[0].3 - 1;
        let event_id = record_for_provider(
            &mut conn,
            "flat-rate",
            "deepseek",
            ChargeBasis::MeteredApi,
            Some(before_band),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::new());
        fake.response.lock().unwrap().models = BTreeMap::from([(
            "deepseek/model".into(),
            BTreeMap::from([("rate.input".into(), rate(3_000))]),
        )]);
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            before_band,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        let stored: (String, Option<i64>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT outcome,charge_nanos,catalog_entry,unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(stored.0, "priced", "{stored:?}");
        assert_eq!(stored.1, Some(3_000));
        assert!(stored.2.is_some(), "the catalog supplied this rate");
    }

    #[test]
    fn deepseek_time_banded_rate_is_unpriced_without_guessing_a_selector() {
        let mut conn = conn();
        let event_id = record_for_provider(
            &mut conn,
            "time-banded",
            "deepseek",
            ChargeBasis::MeteredApi,
            Some(CATALOG_COVERAGE_GAPS[0].3),
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
        let stored: (String, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT unpriced_reason,resolved_at_ms,catalog_entry FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        // catalog_entry must be NULL, not a synthesised object: the refusal
        // runs before any lookup, so nothing was served, and that column is
        // where a catalog's own bytes go. Asserting the effect rather than the
        // verdict is what makes this a fence — the previous version of this
        // test checked only the reason, which is why 80 production rows carry
        // a local sentence in a producer's column.
        assert_eq!(stored, ("rate_time_banded".into(), None, None));
    }

    /// The coverage-gap list is the ONLY source of this refusal. Emptying it
    /// must let a gapped provider reach the catalog, which is the mutation
    /// that proves the list is load-bearing rather than decorative: with a
    /// hardcoded provider name there was nothing to empty, so no test could
    /// distinguish "the guard fired" from "the guard exists".
    ///
    /// The provider is the fixture's own `provider`, the single key the fake
    /// catalog files its row under. Any other name returns `no_rate` from a
    /// missing MODEL rather than a missing RATE, so the test would pass for a
    /// reason unrelated to the gap list.
    #[test]
    fn a_provider_absent_from_the_coverage_gap_list_reaches_the_catalog() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "not-gapped",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(
            fake.calls.load(Ordering::SeqCst),
            1,
            "a provider with no coverage gap must be looked up"
        );
        let (outcome, entry): (String, Option<String>) = conn
            .query_row(
                "SELECT outcome,catalog_entry FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(outcome, "priced");
        assert!(
            entry.is_some(),
            "a priced row records the bytes the catalog served"
        );
    }

    #[test]
    fn covered_head_sets_currency_in_the_generation_checked_transaction() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "covered",
            ChargeBasis::SubscriptionIncluded,
            Some(700),
            vec![measurement("input", Some(1))],
        );
        let plan = read_reprice(&conn, &event_id).unwrap();
        let resolved = resolve_subscription_coverage(&plan, "JPY").unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        let event: (String, i64, String, i64) = conn
            .query_row(
                "SELECT e.currency,e.pricing_generation,p.outcome,p.charge_nanos
                 FROM usage_event e JOIN priced_charge p ON p.event_key=e.event_key
                 WHERE e.event_id=?1",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(event, ("JPY".into(), 1, "covered".into(), 0));
    }

    fn assert_catalog_refusal_without_rows(name: &str, key: &str, entry: Value) {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            name,
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([(key.into(), entry)]));
        assert!(matches!(
            reprice_event(
                &mut conn,
                &event_id,
                &client(fake),
                1_000,
                PricingOrigin::PricingPass
            ),
            Err(PricingError::Catalog(_))
        ));
        assert_eq!(count(&conn, &event_id), 0);
        let generation: i64 = conn
            .query_row(
                "SELECT pricing_generation FROM usage_event WHERE event_id=?1",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(generation, 0);
    }

    #[test]
    fn non_nine_catalog_exponent_refuses_without_any_stored_state() {
        assert_catalog_refusal_without_rows(
            "bad-exponent",
            "rate.input",
            json!({"state":"priced","units":3_000,"exponent":2,"currency":"USD","unit_provenance":{"kind":"stated"}}),
        );
    }

    #[test]
    fn unknown_floor_variant_refuses() {
        assert_catalog_refusal_without_rows(
            "future-floor",
            "rate.input",
            json!({
                "state": "priced",
                "units": 3_000,
                "exponent": 9,
                "currency": "USD",
                "unit_provenance": {"kind": "stated"},
                "floor": {"kind": "future_floor"}
            }),
        );
    }

    #[test]
    fn priced_charge_records_floor_unknown_when_the_rate_carries_the_serde_default() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "floor-default",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let stored: (String, Option<i64>) = conn
            .query_row(
                "SELECT floor_state,floor_units FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("unknown".into(), None));
    }

    #[test]
    fn floor_minimum_round_trips_with_its_units() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "floor-minimum",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let mut rate_with_floor = rate(3_000_000_000);
        rate_with_floor["floor"] = json!({"kind": "minimum", "units": 64});
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate_with_floor)]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let stored: (String, i64) = conn
            .query_row(
                "SELECT floor_state,floor_units FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("minimum".into(), 64));
    }

    #[test]
    fn unknown_catalog_rate_state_refuses_without_any_stored_state() {
        assert_catalog_refusal_without_rows(
            "future-state",
            "rate.input",
            json!({"state":"future_variant","metadata":{"kept":true}}),
        );
    }

    #[test]
    fn billing_plane_values_refuse_ordinary_pricing_without_any_stored_state() {
        for entry in [
            json!({"state": "billed_as", "key": "rate.output"}),
            json!({"state": "unpriced", "reason": "not_established"}),
        ] {
            assert_catalog_refusal_without_rows("plane-only", "rate.input", entry);
        }
    }

    #[test]
    fn unknown_catalog_unpriced_reason_refuses_without_any_stored_state() {
        assert_catalog_refusal_without_rows(
            "future-reason",
            "rate.input",
            json!({"state":"unpriced","reason":"future_reason"}),
        );
    }

    #[test]
    fn unrecognized_rate_sibling_refuses_without_any_stored_state() {
        assert_catalog_refusal_without_rows(
            "future-sibling",
            "rate.input.future_selector",
            rate(3_000),
        );
    }

    /// Base `rate.input` at 3 USD/M with `rate.input.mode.fast` at 6 USD/M and
    /// an unknown-selector sibling at 9 USD/M; charge nanos of each outcome.
    fn conditional_rate_charges(mode: Option<&str>) -> Vec<ChargeDecision> {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "conditional",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000)),
            ("rate.input.mode.fast".into(), rate(6_000_000_000)),
            ("rate.input.region.eu".into(), rate(9_000_000_000)),
        ]));
        let tx = conn.transaction().unwrap();
        let mut plan = read_reprice_in_tx(&tx, &event_id).unwrap();
        drop(tx);
        assert_eq!(plan.mode, None, "the store records no request mode");
        plan.mode = mode.map(str::to_string);
        resolve_reprice(&plan, &client(fake), &[])
            .unwrap()
            .measurements
            .into_iter()
            .map(|measurement| measurement.decision)
            .collect()
    }

    fn charge_nanos(decisions: &[ChargeDecision]) -> Vec<Option<i64>> {
        decisions
            .iter()
            .map(|decision| match decision {
                ChargeDecision::Priced { charge_nanos, .. } => Some(*charge_nanos),
                _ => None,
            })
            .collect()
    }

    /// A request recorded fast prices at `rate.input.mode.fast`; one recorded
    /// standard, and one with no recorded mode, price at `rate.input`. The
    /// unknown-selector sibling never applies.
    #[test]
    fn recorded_mode_selects_the_mode_rate_and_otherwise_the_base_prices() {
        assert_eq!(
            charge_nanos(&conditional_rate_charges(None)),
            vec![Some(3_000_000)]
        );
        assert_eq!(
            charge_nanos(&conditional_rate_charges(Some("standard"))),
            vec![Some(3_000_000)]
        );
        assert_eq!(
            charge_nanos(&conditional_rate_charges(Some("fast"))),
            vec![Some(6_000_000)]
        );
        let ChargeDecision::Priced { catalog_entry, .. } =
            &conditional_rate_charges(Some("fast"))[0]
        else {
            panic!("fast request unpriced");
        };
        assert_eq!(catalog_entry["fact_key"], "rate.input.mode.fast");
    }

    /// A request recorded in a mode the model has no rate for is unpriced,
    /// never priced at the standard rate.
    #[test]
    fn recorded_mode_without_its_rate_is_unpriced_not_priced_at_base() {
        let decisions = conditional_rate_charges(Some("priority"));
        assert!(matches!(
            &decisions[..],
            [ChargeDecision::Unpriced {
                reason: UnpricedReason::NoRate,
                ..
            }]
        ));
    }

    /// A stored event on a model serving base, mode and unknown-selector
    /// rates is priced end to end at the base rate.
    #[test]
    fn stored_event_beside_conditional_rates_prices_at_base() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "beside-conditional",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000)),
            ("rate.cache_read.mode.fast".into(), rate(6_000_000_000)),
            ("rate.input.region.eu".into(), rate(9_000_000_000)),
        ]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let charged: Option<i64> = conn
            .query_row(
                "SELECT charge_nanos FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(charged, Some(3_000_000));
    }

    #[test]
    fn threshold_crossing_request_is_unpriced_instead_of_guessing_scheme() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "tiered",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(250_000)),
                measurement("output", Some(10_000)),
            ],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000)),
            (
                "rate.input.above_context.200000".into(),
                rate(6_000_000_000),
            ),
            ("rate.output".into(), rate(15_000_000_000)),
            (
                "rate.output.above_context.200000".into(),
                rate(30_000_000_000),
            ),
        ]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let reasons = conn
            .prepare("SELECT unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY role")
            .unwrap()
            .query_map([&event_id], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();
        assert_eq!(reasons, vec!["rate_tiered", "rate_tiered"]);
    }

    /// `(unpriced_reason, catalog_entry, catalog_source, resolved_at_ms,
    /// charge_nanos)` of every charge row of an event, by role.
    type TierRow = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    );

    fn tier_rows(conn: &Connection, event_id: &str) -> Vec<TierRow> {
        conn.prepare(
            "SELECT unpriced_reason, catalog_entry, catalog_source, resolved_at_ms, charge_nanos
             FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY role",
        )
        .unwrap()
        .query_map([event_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    }

    /// Records a metered event and prices it against a catalog serving
    /// `rate.input` and `rate.cache_read` with a tier above 272,000 tokens.
    fn price_against_272k_tier(
        conn: &mut Connection,
        name: &str,
        measurements: Vec<MeasurementInput>,
    ) -> String {
        let event_id = record(conn, name, ChargeBasis::MeteredApi, Some(700), measurements);
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(1_000_000_000)),
            (
                "rate.input.above_context.272000".into(),
                rate(2_000_000_000),
            ),
            ("rate.cache_read".into(), rate(100_000_000)),
            (
                "rate.cache_read.above_context.272000".into(),
                rate(200_000_000),
            ),
        ]));
        reprice_event(
            conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        event_id
    }

    /// A tier refusal leaves `catalog_entry` NULL: the column holds what the
    /// catalog sent, and the source and instant it was asked at are what let
    /// a reader ask it again.
    #[test]
    fn crossing_tier_refusal_stores_no_catalog_entry_and_keeps_source_and_instant() {
        let mut conn = conn();
        let event_id = price_against_272k_tier(
            &mut conn,
            "tier-entry",
            vec![measurement("input", Some(300_000))],
        );
        assert_eq!(
            tier_rows(&conn, &event_id),
            vec![(
                Some("rate_tiered".to_string()),
                None,
                Some("models.dev".to_string()),
                Some(700),
                None,
            )]
        );
    }

    /// The catalog client rejects an unreadable tier key while decoding, so
    /// pricing's own refusal of one is reached here with a snapshot built
    /// directly; that refusal must also store no catalog entry.
    #[test]
    fn unreadable_tier_refusal_stores_no_catalog_entry_and_keeps_source_and_instant() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "tier-unreadable-entry",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000))],
        );
        let plan = read_reprice(&conn, &event_id).unwrap();
        let snapshot = CatalogSnapshot {
            source: "models.dev".into(),
            resolved_at_ms: 700,
            model_key: "provider/model".into(),
            facts: BTreeMap::from([
                ("rate.input".into(), rate(1_000_000_000)),
                ("rate.input.above_context.200k".into(), rate(2_000_000_000)),
            ]),
            withheld_facts: BTreeMap::new(),
            uncertain_facts: BTreeMap::new(),
            overridden_facts: BTreeMap::new(),
        };
        let resolved = resolve_catalog_snapshot(&plan, &snapshot, &[]).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            tier_rows(&conn, &event_id),
            vec![(
                Some("rate_tiered".to_string()),
                None,
                Some("models.dev".to_string()),
                Some(700),
                None,
            )]
        );
    }

    /// The threshold applies to the whole prompt: a cache read large enough
    /// to carry the prompt past it refuses the request even though its
    /// uncached input alone is far below.
    #[test]
    fn cached_prompt_over_the_threshold_is_refused_though_input_alone_is_under() {
        let mut conn = conn();
        let event_id = price_against_272k_tier(
            &mut conn,
            "tier-cached-over",
            vec![
                measurement("input", Some(10_000)),
                measurement("cache_read", Some(300_000)),
            ],
        );
        let reasons: Vec<_> = tier_rows(&conn, &event_id)
            .into_iter()
            .map(|(reason, ..)| reason)
            .collect();
        assert_eq!(
            reasons,
            vec![Some("rate_tiered".into()), Some("rate_tiered".into())]
        );
    }

    /// A prompt below the threshold, cache read included, prices every role
    /// at its base rate.
    #[test]
    fn cached_prompt_under_the_threshold_prices_at_base() {
        let mut conn = conn();
        let event_id = price_against_272k_tier(
            &mut conn,
            "tier-cached-under",
            vec![
                measurement("input", Some(10_000)),
                measurement("cache_read", Some(100_000)),
            ],
        );
        let charges: Vec<_> = tier_rows(&conn, &event_id)
            .into_iter()
            .map(|(reason, _, _, _, charge)| (reason, charge))
            .collect();
        // cache_read: 100,000 x $0.10/M; input: 10,000 x $1/M.
        assert_eq!(
            charges,
            vec![(None, Some(10_000_000)), (None, Some(10_000_000))]
        );
    }

    fn price_against_tier_without_same_role_base(
        conn: &mut Connection,
        cache_write_tokens: i64,
    ) -> String {
        let event_id = record(
            conn,
            "tier-without-same-role-base",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(10_000)),
                measurement("output", Some(2_000)),
                measurement("cache_write", Some(cache_write_tokens)),
            ],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(1_000_000_000)),
            ("rate.output".into(), rate(4_000_000_000)),
            (
                "rate.cache_write.above_context.256000".into(),
                rate(9_000_000_000),
            ),
        ]));
        reprice_event(
            conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        event_id
    }

    #[test]
    fn tier_rate_without_same_role_base_below_threshold_leaves_that_role_unpriced() {
        let mut conn = conn();
        let event_id = price_against_tier_without_same_role_base(&mut conn, 10_000);
        let charges: Vec<_> = tier_rows(&conn, &event_id)
            .into_iter()
            .map(|(reason, _, _, _, charge)| (reason, charge))
            .collect();
        // The 20,000-token prompt is below the tier. Only the unconditional
        // input ($1/M) and output ($4/M) rates can produce money.
        assert_eq!(
            charges,
            vec![
                (Some("no_rate".into()), None),
                (None, Some(10_000_000)),
                (None, Some(8_000_000)),
            ],
            "a tier rate without a base rate of the same role must not supply money below its threshold (rows: cache_write, input, output)"
        );
    }

    #[test]
    fn tier_rate_without_same_role_base_above_threshold_refuses_every_measurement() {
        let mut conn = conn();
        let event_id = price_against_tier_without_same_role_base(&mut conn, 250_000);
        let charges: Vec<_> = tier_rows(&conn, &event_id)
            .into_iter()
            .map(|(reason, _, _, _, charge)| (reason, charge))
            .collect();
        // Cache writes bring the whole prompt to 260,000 tokens, so the tier
        // refuses even the roles that have unconditional rates.
        assert_eq!(
            charges,
            vec![(Some("rate_tiered".into()), None); 3],
            "a tier rate without a base rate of the same role must refuse every measurement above its threshold (rows: cache_write, input, output)"
        );
    }

    /// A cache read stored without a quantity makes the prompt's size
    /// unknown, so the request cannot be placed below the threshold.
    #[test]
    fn unknown_cache_read_quantity_makes_the_prompt_unknown_and_refuses() {
        let mut conn = conn();
        let event_id = price_against_272k_tier(
            &mut conn,
            "tier-cached-unknown",
            vec![
                measurement("input", Some(10_000)),
                measurement("cache_read", None),
            ],
        );
        let reasons: Vec<_> = tier_rows(&conn, &event_id)
            .into_iter()
            .map(|(reason, ..)| reason)
            .collect();
        assert_eq!(
            reasons,
            vec![Some("rate_tiered".into()), Some("rate_tiered".into())]
        );
    }

    #[test]
    fn prompt_size_sums_prompt_roles_in_every_prompt_unit() {
        let prompt = prompt_size([
            ("input", "token", Some(100), Some(0)),
            ("cache_read", "token", Some(200), Some(0)),
            ("cache_write", "token_cache_5m", Some(300), Some(0)),
            ("cache_write", "token_cache_1h", Some(400), Some(0)),
            // Not prompt: never counted, even without a quantity.
            ("output", "token", Some(1_000_000), Some(0)),
            ("reasoning", "token", None, None),
        ])
        .unwrap();
        assert!(prompt.exceeds(999));
        assert!(!prompt.exceeds(1_000));
        let fractional = prompt_size([("input", "token", Some(10_005), Some(-1))]).unwrap();
        assert!(fractional.exceeds(1_000));
        assert!(!fractional.exceeds(1_001));
        assert_eq!(
            prompt_size([("cache_write", "token_cache_1h", None, None)]),
            None
        );
        assert_eq!(prompt_size([("input", "token", Some(-1), Some(0))]), None);
        assert_eq!(prompt_size([("input", "token", Some(1), Some(1))]), None);
        assert_eq!(prompt_size([("input", "token", Some(1), Some(-10))]), None);
    }

    /// A tier key whose threshold cannot be parsed is refused at DECODE, as an
    /// unrecognised rate fact, before tier_thresholds ever runs -- so the
    /// pricing path never sees a partial threshold set. tier_thresholds now
    /// errors on the same input as a second layer; this test pins the first,
    /// because a refactor that decoded such keys leniently would otherwise
    /// reach a function that used to drop them silently.
    #[test]
    fn unreadable_tier_threshold_is_refused_at_decode_not_priced_at_base_rate() {
        assert_catalog_refusal_without_rows(
            "tiered-unreadable",
            "rate.input.above_context.200k",
            rate(6_000_000_000),
        );
        let facts = BTreeMap::from([(
            "rate.input.above_context.200k".to_string(),
            serde_json::json!({}),
        )]);
        let error = astrocyte_core_tier_thresholds(&facts).unwrap_err();
        assert!(error.contains("200k"), "{error}");
    }

    fn astrocyte_core_tier_thresholds(
        facts: &BTreeMap<String, serde_json::Value>,
    ) -> Result<std::collections::BTreeSet<u64>, String> {
        crate::catalog_client::tier_thresholds(facts)
    }

    #[test]
    fn priced_charge_records_non_default_catalog_source() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "catalog-source",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
        fake.set_source("private.catalog");
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();

        let source: String = conn
            .query_row(
                "SELECT catalog_source FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source, "private.catalog");
    }

    fn queued(conn: &Connection) -> Vec<String> {
        conn.prepare("SELECT event_id FROM pricing_pending JOIN usage_event USING(event_key) ORDER BY event_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap()
    }

    /// An event leaves the pricing queue when its LAST headless measurement
    /// gets a head, in that commit, and not before.
    ///
    /// Recording writes every measurement of an event at once, so a commit
    /// that heads some measurements and leaves others headless needs a
    /// measurement to appear between reading the plan and committing it; the
    /// test inserts one there directly. That commit must leave the event
    /// queued, since one of its measurements still needs pricing. The next
    /// commit heads that one and takes the event out. An unpriced head counts
    /// as a head: the event recorded with an unknown basis leaves the queue on
    /// its first commit even though nothing was priced.
    #[test]
    fn pricing_the_last_headless_measurement_dequeues_and_a_partially_headed_event_stays() {
        let mut conn = conn();
        let partial = record(
            &mut conn,
            "partial",
            ChargeBasis::MeteredApi,
            Some(777),
            vec![measurement("input", Some(1_000_000))],
        );
        let unknown = record(
            &mut conn,
            "unknown-basis",
            ChargeBasis::Unknown,
            Some(777),
            vec![measurement("input", Some(1_000_000))],
        );
        let mut expected = vec![partial.clone(), unknown.clone()];
        expected.sort();
        assert_eq!(queued(&conn), expected);

        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000)),
            ("rate.output".into(), rate(3_000_000_000)),
        ]));
        let catalog = client(fake);
        let plan = read_reprice(&conn, &partial).unwrap();
        let resolved = resolve_reprice(&plan, &catalog, &[]).unwrap();
        conn.execute(
            "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'output','token','billed','usage.output',1000000,0)",
            [&partial],
        )
        .unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        assert!(
            queued(&conn).contains(&partial),
            "an event with a measurement still headless must stay queued"
        );

        reprice_event(
            &mut conn,
            &partial,
            &catalog,
            1_001,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert!(
            !queued(&conn).contains(&partial),
            "heading the last headless measurement dequeues the event"
        );

        let plan = read_reprice(&conn, &unknown).unwrap();
        let resolved = resolve_reprice_without_catalog(&plan).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        assert!(queued(&conn).is_empty(), "an unpriced head is a head");
    }

    #[test]
    fn first_priced_head_sets_currency_and_terminal_status_never_filters_usage() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "priced-failed",
            ChargeBasis::MeteredApi,
            Some(777),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let stored: (String, i64, i64, i64) = conn
            .query_row(
                "SELECT e.currency,e.pricing_generation,p.charge_nanos,p.resolved_at_ms
                 FROM usage_event e JOIN priced_charge p ON p.event_key=e.event_key
                 WHERE e.event_id=?1",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(stored, ("USD".into(), 1, 3_000_000_000, 777));
        assert_eq!(fake.requests.lock().unwrap()[0].at_ms, Some(777));
    }

    #[test]
    fn mixed_currency_catalog_refuses_without_any_stored_state() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "mixed-currency",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(1)),
                measurement("output", Some(1)),
            ],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000)),
            (
                "rate.output".into(),
                json!({"state":"priced","units":15_000,"exponent":9,"currency":"EUR","unit_provenance":{"kind":"stated"}}),
            ),
        ]));
        assert!(matches!(
            reprice_event(
                &mut conn,
                &event_id,
                &client(fake),
                1_000,
                PricingOrigin::PricingPass
            ),
            Err(PricingError::Refused(
                PricingRefusal::CatalogCurrencyMismatch
            ))
        ));
        assert_eq!(count(&conn, &event_id), 0);
        let event: (Option<String>, i64) = conn
            .query_row(
                "SELECT currency,pricing_generation FROM usage_event WHERE event_id=?1",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(event, (None, 0));
    }

    #[test]
    fn mixed_reprice_preserves_priced_head_and_supersedes_only_unpriced_head() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "mixed",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(1_000_000)),
                measurement("output", Some(1_000_000)),
            ],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(3_000_000_000)),
            (
                "rate.output".into(),
                json!({"state":"unpriced","reason":"missing_rate"}),
            ),
        ]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        fake.set_facts(BTreeMap::from([
            ("rate.input".into(), rate(9_000_000_000)),
            ("rate.output".into(), rate(15_000_000_000)),
        ]));
        let outcome = reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            2_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(outcome.measurements_repriced, 1);
        assert_eq!(outcome.measurements_preserved, 1);
        let rows = conn
            .prepare(
                "SELECT role,COUNT(*),MIN(charge_nanos),MAX(charge_nanos)
                 FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) GROUP BY role ORDER BY role",
            )
            .unwrap()
            .query_map([&event_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows[0],
            ("input".into(), 1, Some(3_000_000_000), Some(3_000_000_000))
        );
        assert_eq!(rows[1].0, "output");
        assert_eq!(rows[1].1, 2);
        assert!(matches!(
            read_reprice(&conn, &event_id),
            Err(PricingError::Refused(PricingRefusal::AlreadyPriced))
        ));
    }

    #[test]
    fn contention_between_resolve_and_commit_aborts_as_pricing_superseded() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astrocyte-pricing-contention-{}-{nonce}.sqlite",
            std::process::id()
        ));
        let mut losing_conn = Connection::open(&path).unwrap();
        initialize_connection(&losing_conn).unwrap();
        for migration in MIGRATIONS {
            losing_conn.execute_batch(migration.statements).unwrap();
        }
        let event_id = record(
            &mut losing_conn,
            "contended",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
        let losing_catalog = client(fake.clone());
        let losing_plan = read_reprice(&losing_conn, &event_id).unwrap();
        let losing_resolution = resolve_reprice(&losing_plan, &losing_catalog, &[]).unwrap();

        let winning_path = path.clone();
        let winning_event_id = event_id.clone();
        // The winning reprice reports back over a channel so the wait for it
        // is bounded: a reprice that blocked (on a lock this test's own
        // connection still held, say) fails here by name instead of hanging
        // the suite. The bound is a hang guard, far above a healthy run.
        let (won, winner_finished) = std::sync::mpsc::channel();
        let winner = std::thread::spawn(move || {
            let mut winning_conn = Connection::open(winning_path).unwrap();
            initialize_connection(&winning_conn).unwrap();
            let winning_catalog = CatalogClient::new(fake, 500);
            let _ = won.send(reprice_event(
                &mut winning_conn,
                &winning_event_id,
                &winning_catalog,
                1_000,
                PricingOrigin::PricingPass,
            ));
        });
        match winner_finished.recv_timeout(std::time::Duration::from_secs(60)) {
            Ok(result) => {
                result.unwrap();
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("the winning reprice did not finish within 60 s")
            }
            // The sender dropped without sending: the winner panicked, and the
            // join below re-raises its panic with its own message.
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {}
        }
        if let Err(panic) = winner.join() {
            std::panic::resume_unwind(panic);
        }

        let rows_before = count(&losing_conn, &event_id);
        let tx = losing_conn.transaction().unwrap();
        assert!(matches!(
            commit_reprice_in_tx(
                &tx,
                &losing_plan,
                &losing_resolution,
                2_000,
                PricingOrigin::PricingPass
            ),
            Err(PricingError::Refused(PricingRefusal::PricingSuperseded))
        ));
        tx.rollback().unwrap();
        assert_eq!(count(&losing_conn, &event_id), rows_before);
        assert_eq!(rows_before, 1);
        drop(losing_conn);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn pre_history_event_records_catalog_pre_history_without_network_io() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "pre-history",
            ChargeBasis::MeteredApi,
            Some(400),
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake.clone()),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
        let stored: (String, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT unpriced_reason,catalog_source,resolved_at_ms
                 FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, ("catalog_pre_history".into(), None, None));
    }

    /// fusiform 0.19 serves a rate on rows where the serving provider publishes
    /// none, borrowed from the model's originator and marked `inherited_from`.
    /// Its own reading: not this provider's price. Input is the producer's
    /// served fixture bytes for the marked row (fusiform b959a08,
    /// crates/fusiform-module/fixtures/served-payloads.json, "inherited rate"),
    /// so the decode is proved against real bytes rather than a hand-written
    /// shape. Read back from the store: no charge, the reason, and the origin
    /// preserved in the served entry.
    #[test]
    fn inherited_rate_is_unpriced_not_charged() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "inherited",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let served: Value = serde_json::from_str(
            r#"{"currency":"USD","exponent":9,"inherited_from":{"basis":"open_weights","family":"glm","provider_id":"zai"},"state":"priced","unit_provenance":{"kind":"assumed_by_policy","policy":"models-dev-usd-v1"},"units":75000000}"#,
        )
        .unwrap();
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), served)]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let (outcome, reason, charge, entry): (String, Option<String>, Option<i64>, String) = conn
            .query_row(
                "SELECT outcome,unpriced_reason,charge_nanos,catalog_entry
                 FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(outcome, "unpriced");
        assert_eq!(reason.as_deref(), Some("rate_inherited"));
        assert_eq!(
            charge, None,
            "a borrowed rate must never reach charge_nanos"
        );
        let entry: Value = serde_json::from_str(&entry).unwrap();
        assert_eq!(entry["inherited_from"]["provider_id"], "zai");
        assert_eq!(
            entry["value"]["inherited_from"]["basis"], "open_weights",
            "the served bytes, marker included, survive in catalog_entry"
        );
    }

    /// The control beside the marked row: the same rate without the marker is
    /// the serving provider's own price and prices exactly as before.
    #[test]
    fn published_rate_without_origin_still_prices() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "published",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(75_000_000))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let (outcome, charge): (String, Option<i64>) = conn
            .query_row(
                "SELECT outcome,charge_nanos FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        // 1,000,000 tokens at 75,000,000 nanos per million = 75,000,000 nanos ($0.075).
        assert_eq!((outcome.as_str(), charge), ("priced", Some(75_000_000)));
    }

    #[test]
    fn stated_zero_remains_distinct_as_policy_unpriced() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "stated-zero",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([(
            "rate.input".into(),
            json!({"state":"stated_zero"}),
        )]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let stored: (String, Option<i64>) = conn
            .query_row(
                "SELECT unpriced_reason,charge_nanos FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("stated_zero_requires_policy".into(), None));
    }

    /// Reads one event's charge row back as (outcome, reason, charge, entry).
    fn stored_charge(
        conn: &Connection,
        event_id: &str,
    ) -> (String, Option<String>, Option<i64>, Value) {
        let (outcome, reason, charge, entry): (String, Option<String>, Option<i64>, String) = conn
            .query_row(
                "SELECT outcome,unpriced_reason,charge_nanos,catalog_entry
                 FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        (
            outcome,
            reason,
            charge,
            serde_json::from_str(&entry).unwrap(),
        )
    }

    /// A zero borrowed from the originator's row is that row saying the model
    /// is free, not this provider stating zero. It is refused with the same
    /// reason as a borrowed price, never recorded as this provider's own zero,
    /// and the origin travels in the stored entry.
    #[test]
    fn borrowed_stated_zero_is_unpriced_as_inherited() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "borrowed-zero",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([(
            "rate.input".into(),
            json!({
                "state": "stated_zero",
                "inherited_from": {"basis": "open_weights", "family": "glm", "provider_id": "zai"}
            }),
        )]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let (outcome, reason, charge, entry) = stored_charge(&conn, &event_id);
        assert_eq!(
            (outcome.as_str(), reason.as_deref(), charge),
            ("unpriced", Some("rate_inherited"), None)
        );
        assert_eq!(entry["inherited_from"]["provider_id"], "zai");
        assert_eq!(entry["value"]["inherited_from"]["basis"], "open_weights");
    }

    /// The provider's own stated zero (no `inherited_from`) is still unpriced
    /// as `stated_zero_requires_policy`, unchanged by the inherited-zero rule,
    /// and its stored entry carries no origin.
    #[test]
    fn own_stated_zero_still_requires_policy() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "own-zero",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([(
            "rate.input".into(),
            json!({"state": "stated_zero"}),
        )]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let (outcome, reason, charge, entry) = stored_charge(&conn, &event_id);
        assert_eq!(
            (outcome.as_str(), reason.as_deref(), charge),
            ("unpriced", Some("stated_zero_requires_policy"), None)
        );
        assert_eq!(entry.get("inherited_from"), None);
    }

    /// fusiform can serve a model under an alias id (listed in the response's
    /// `aliased`) using the facts of the model the alias points at. Every rate
    /// on such an entry is that other model's list price, marked
    /// `inherited_from` with basis `alias`. That is not the route's cost, so
    /// it is refused as inherited like any borrowed price.
    #[test]
    fn aliased_model_rate_is_unpriced_as_inherited() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "aliased",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let mut served = rate(75_000_000);
        served["inherited_from"] = json!({
            "basis": "alias",
            "family": "gemini",
            "provider_id": "google",
            "model_id": "gemini-3.8-flash"
        });
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), served)]));
        fake.response.lock().unwrap().aliased = vec![fusiform_protocol::AliasedModelWire {
            model: "provider/model".into(),
            target_provider_id: "google".into(),
            target_model_id: "gemini-3.8-flash".into(),
            source_ref: "plugin/models.ts@abc123".into(),
        }];
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let (outcome, reason, charge, entry) = stored_charge(&conn, &event_id);
        assert_eq!(
            (outcome.as_str(), reason.as_deref(), charge),
            ("unpriced", Some("rate_inherited"), None)
        );
        assert_eq!(entry["inherited_from"]["basis"], "alias");
        assert_eq!(entry["inherited_from"]["model_id"], "gemini-3.8-flash");
    }

    #[test]
    fn arithmetic_overflow_records_unpriced_instead_of_wrapping_or_refusing_act() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "overflow",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(i64::MAX))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(i64::MAX))]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let stored: (String, Option<i64>) = conn
            .query_row(
                "SELECT unpriced_reason,charge_nanos FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("arithmetic_out_of_range".into(), None));
    }

    #[test]
    fn withheld_and_uncertain_rates_are_named_unpriced_outcomes() {
        for (name, expected, uncertain) in [
            ("withheld", "rate_withheld", false),
            ("uncertain", "rate_uncertain", true),
        ] {
            let mut conn = conn();
            let event_id = record(
                &mut conn,
                name,
                ChargeBasis::MeteredApi,
                Some(700),
                vec![measurement("input", Some(1))],
            );
            let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000))]));
            if uncertain {
                fake.response.lock().unwrap().uncertain = vec![UncertainFactWire {
                    provider_id: "provider".into(),
                    model_id: "model".into(),
                    fact_key: "rate.input".into(),
                    superseded_after_ms: 600,
                    superseded_by_ms: 800,
                }];
            } else {
                fake.response.lock().unwrap().models = BTreeMap::new();
                fake.response.lock().unwrap().withheld = vec![WithheldFactWire {
                    model: "provider/model".into(),
                    fact_key: "rate.input".into(),
                    corrections: vec![],
                }];
            }
            reprice_event(
                &mut conn,
                &event_id,
                &client(fake),
                1_000,
                PricingOrigin::PricingPass,
            )
            .unwrap();
            let stored: (String, String) = conn
                .query_row(
                    "SELECT unpriced_reason,catalog_entry FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                    [&event_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(stored.0, expected);
            assert!(stored
                .1
                .contains(if uncertain { "uncertain" } else { "withheld" }));
        }
    }

    #[test]
    fn already_priced_refuses_without_appending_or_changing_generation() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "already-priced",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(1_000_000))],
        );
        let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(3_000_000_000))]));
        let catalog = client(fake);
        reprice_event(
            &mut conn,
            &event_id,
            &catalog,
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert!(matches!(
            reprice_event(
                &mut conn,
                &event_id,
                &catalog,
                2_000,
                PricingOrigin::PricingPass
            ),
            Err(PricingError::Refused(PricingRefusal::AlreadyPriced))
        ));
        assert_eq!(count(&conn, &event_id), 1);
        let generation: i64 = conn
            .query_row(
                "SELECT pricing_generation FROM usage_event WHERE event_id=?1",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(generation, 1);
    }

    #[test]
    fn representation_rounding_is_per_row_and_event_sum_is_not_rerounded() {
        let mut conn = conn();
        let event_id = record(
            &mut conn,
            "rounding",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![
                measurement("input", Some(1)),
                measurement("output", Some(1)),
            ],
        );
        let fake = FakeCatalog::new(BTreeMap::from([
            ("rate.input".into(), rate(500_000)),
            ("rate.output".into(), rate(500_000)),
        ]));
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let charges = conn
            .prepare("SELECT charge_nanos FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY role")
            .unwrap()
            .query_map([&event_id], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<i64>>>()
            .unwrap();
        assert_eq!(charges, vec![0, 0]);
        assert_eq!(
            event_pricing_outcome(&conn, &event_id).unwrap(),
            EventPricingOutcome::Priced {
                measurement_count: 2,
                charge_nanos: 0,
            }
        );
    }

    /// $0.28 per million output tokens: 42 tokens cost 11,760 nanos, the
    /// output charge an inclusive 42 would get if its reasoning were not
    /// taken out.
    const OUTPUT_RATE: i64 = 280_000_000;
    /// A different reasoning rate, so a sum that swapped the two roles' token
    /// counts could not match by accident.
    const REASONING_RATE: i64 = 300_000_000;

    /// Records a BROCA event with one output measurement under
    /// `output_source` and one reasoning measurement, priced against a fake
    /// catalog that serves both rates for `provider`.
    fn price_broca_output(
        producer_ref: &str,
        provider: &str,
        output_source: &str,
        output: i64,
        reasoning: i64,
    ) -> (Connection, String) {
        let mut conn = conn();
        let event_id = record_event(
            &mut conn,
            &EventRecordRequest {
                producer: "broca".into(),
                producer_lineage: Some("lineage".into()),
                producer_ref: producer_ref.into(),
                prior_event_ref: None,
                harness: None,
                project_root: None,
                session: None,
                run_ref: None,
                flow_ref: None,
                work_class: None,
                occurred_at_ms: Some(700),
                emitted_at_ms: None,
                terminal_status: Some("failed".into()),
                attempt_ordinal: None,
                vendor_request_id: None,
                provider: Some(provider.into()),
                model: Some("model".into()),
                account_ref: Some("account".into()),
                auth_selection: None,
                charge_basis_mapping_version: None,
                charge_basis_overrides: None,
                charge_basis: ChargeBasis::MeteredApi,
                measurements: vec![
                    MeasurementInput {
                        source: output_source.into(),
                        ..measurement("output", Some(output))
                    },
                    MeasurementInput {
                        source: "usage.reasoning_tokens".into(),
                        ..measurement("reasoning", Some(reasoning))
                    },
                ],
            },
            900,
        )
        .unwrap()
        .event_id;
        let facts = BTreeMap::from([
            ("rate.output".into(), rate(OUTPUT_RATE)),
            ("rate.reasoning".into(), rate(REASONING_RATE)),
        ]);
        let fake = FakeCatalog::new(BTreeMap::new());
        fake.response.lock().unwrap().models =
            BTreeMap::from([(format!("{provider}/model"), facts)]);
        reprice_event(
            &mut conn,
            &event_id,
            &client(fake),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        (conn, event_id)
    }

    /// `(role, outcome, charge_nanos, unpriced_reason, catalog_entry)` for
    /// each current charge row of the event, by role.
    #[allow(clippy::type_complexity)]
    fn charge_heads(
        conn: &Connection,
        event_id: &str,
    ) -> Vec<(String, String, Option<i64>, Option<String>, Option<String>)> {
        conn.prepare(
            "SELECT role,outcome,charge_nanos,unpriced_reason,catalog_entry
             FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY role",
        )
        .unwrap()
        .query_map([event_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    }

    fn stored_output(conn: &Connection, event_id: &str) -> (String, i64) {
        conn.query_row(
            "SELECT source,quantity_units FROM measurement WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) AND role='output'",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    /// An old BROCA fact stores output with reasoning inside it. Pricing it as
    /// stored charged the 40 reasoning tokens twice; the output row must
    /// charge only the 2 tokens that are output proper, while the stored
    /// measurement keeps its inclusive 42.
    #[test]
    fn inclusive_broca_output_is_priced_without_its_reasoning() {
        let (conn, event_id) =
            price_broca_output("inclusive", "deepseek", "usage.output_tokens", 42, 40);
        let heads = charge_heads(&conn, &event_id);
        assert_eq!(
            heads
                .iter()
                .map(|(role, outcome, nanos, _, _)| (role.as_str(), outcome.as_str(), *nanos))
                .collect::<Vec<_>>(),
            [
                ("output", "priced", Some(2 * OUTPUT_RATE / 1_000_000)),
                ("reasoning", "priced", Some(40 * REASONING_RATE / 1_000_000)),
            ]
        );
        assert_eq!(
            event_pricing_outcome(&conn, &event_id).unwrap(),
            EventPricingOutcome::Priced {
                measurement_count: 2,
                charge_nanos: (2 * OUTPUT_RATE + 40 * REASONING_RATE) / 1_000_000,
            }
        );
        assert_eq!(
            stored_output(&conn, &event_id),
            ("usage.output_tokens".to_string(), 42),
            "the stored measurement is not rewritten"
        );
    }

    /// A fact stored after the wire mapping was fixed already holds output
    /// without reasoning, and is priced exactly as stored.
    #[test]
    fn disjoint_broca_output_is_priced_as_stored() {
        let (conn, event_id) = price_broca_output(
            "disjoint",
            "deepseek",
            "usage.output_tokens-reasoning_tokens",
            2,
            40,
        );
        assert_eq!(
            event_pricing_outcome(&conn, &event_id).unwrap(),
            EventPricingOutcome::Priced {
                measurement_count: 2,
                charge_nanos: (2 * OUTPUT_RATE + 40 * REASONING_RATE) / 1_000_000,
            }
        );
    }

    /// Inclusive output smaller than its own reasoning contradicts BROCA's
    /// subset rule. The output is refused with `missing_quantity`, as a fact
    /// mapped today with the same contradiction is, and no synthesised detail
    /// goes into `catalog_entry`; reasoning is still charged.
    #[test]
    fn inclusive_broca_output_below_its_reasoning_is_unpriced() {
        let (conn, event_id) =
            price_broca_output("contradicted", "deepseek", "usage.output_tokens", 30, 40);
        assert_eq!(
            charge_heads(&conn, &event_id),
            [
                (
                    "output".to_string(),
                    "unpriced".to_string(),
                    None,
                    Some("missing_quantity".to_string()),
                    None,
                ),
                (
                    "reasoning".to_string(),
                    "priced".to_string(),
                    Some(40 * REASONING_RATE / 1_000_000),
                    None,
                    heads_entry(&conn, &event_id, "reasoning"),
                ),
            ]
        );
    }

    fn heads_entry(conn: &Connection, event_id: &str, role: &str) -> Option<String> {
        conn.query_row(
            "SELECT catalog_entry FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) AND role=?2",
            [event_id, role],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// A stored xAI fact under the inclusive source may be a pre-fix fact
    /// whose output already excludes reasoning, and the stored row cannot
    /// tell. It is priced as stored, never reduced.
    #[test]
    fn inclusive_source_xai_output_is_priced_as_stored() {
        let (conn, event_id) = price_broca_output("xai", "xai", "usage.output_tokens", 42, 40);
        assert_eq!(
            event_pricing_outcome(&conn, &event_id).unwrap(),
            EventPricingOutcome::Priced {
                measurement_count: 2,
                charge_nanos: (42 * OUTPUT_RATE + 40 * REASONING_RATE) / 1_000_000,
            }
        );
    }
}
