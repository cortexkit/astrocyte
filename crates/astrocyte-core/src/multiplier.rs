//! Derived subscription multiplier: what a subscription's quota is worth at
//! API list prices, per quota provider and plan, recorded once a day.
//!
//! For one quota provider over an interval:
//!
//! ```text
//! subscription_nanos = Σ accounts: monthly_price × (window_minutes / 43200) × Δused% / 100
//! api_nanos          = Σ in-scope events, Σ measurements: tokens × list rate
//! multiplier         = api_nanos / subscription_nanos
//! fee_paid_nanos     = Σ accounts: monthly_price × (token interval length / 30 days)
//! fee_multiplier     = api_nanos / fee_paid_nanos
//! ```
//!
//! The computation is split so no network call happens inside a store
//! transaction: [`plan_observation`] captures quota and scoped events in one
//! snapshot, [`resolve_rates`] and [`resolve_plans`] ask fusiform, then
//! [`compute_plan_observations`] prices the captured clean account-hours.
//! [`insert_observation_in_tx`] appends each plan row with rates and diagnostics.
//! Recompute rereads history using current configuration and prices.
//!
//! Display only: nothing here writes to the spend ledger, and a failed day
//! records nothing rather than a guessed row.

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

use crate::catalog_client::{
    parse_rate_key, stated_mode_condition, CatalogCallError, CatalogClient, CatalogClientError,
    CatalogLookup, CatalogModel, CatalogSnapshot, RateCondition, RATE_FACT_PREFIX, RATE_ROLES,
};
use crate::fact::ChargeBasis;
use crate::money::{round_nano_num, round_nano_ratio, MoneyError};
use crate::pricing::{effective_output, prompt_size, EffectiveOutput};

mod calibration;
mod clean;
pub use clean::compute_plan_observations;

/// Producers whose usage event is exactly one model call, so a tiered rate
/// (`.above_context.<size>`) can be decided from the event's own prompt. Every
/// other producer (BROCA) may bundle many calls into one event -- BROCA events
/// reach prompts of 10.7M tokens, ten times any context limit -- so its events
/// are priced at the base rate and their tokens are counted as flagged.
const PER_REQUEST_PRODUCERS: [&str; 4] = ["opencode", "pi", "claude-code", "codex"];

/// Minutes in the 30-day month plan prices are quoted over.
const MINUTES_PER_MONTH: i64 = 43_200;
const MS_PER_MONTH: i64 = MINUTES_PER_MONTH * 60_000;
const NANOS_PER_MS: i64 = 1_000_000;

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

/// What makes the stored figures read HIGH, by figure as above.
const ERROR_INFLATING: &str = "Reads high: tokens of accounts excluded from the denominator \
     still count in api_nanos, because usage is pooled per provider; this raises both figures. \
     `multiplier` only: usage between the last reading before a reset and the reset itself is \
     never observed (1-3 points per reset when the last reading is near 100%), and redemption \
     zeroing, windows dropped as unreadable and non-continuous drop records lose Δused%.";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The operator's declarations the multiplier is computed under. Closed: every
/// member is required (a nullable member must be written as `null`), unknown
/// members and duplicate map keys are refused, and nothing is defaulted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiplierConfig {
    /// Plan-plane provider id → the API provider id whose list prices value
    /// its tokens, with the model id unchanged (`openai-codex` → `openai`).
    #[serde(deserialize_with = "unique_map")]
    pub api_twins: BTreeMap<String, String>,
    /// Monthly price per plan key (`openai/pro_20x`), overriding fusiform.
    #[serde(deserialize_with = "unique_map")]
    pub plan_price_overrides: BTreeMap<String, PriceOverride>,
    /// Quota provider (the `window_sample.provider` value) → its declarations.
    #[serde(deserialize_with = "unique_map")]
    pub quota_providers: BTreeMap<String, QuotaProviderConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaProviderConfig {
    /// The one quota window Δused% is read from.
    pub window: WindowSelector,
    /// The traffic counted as this provider's subscription usage. Declared,
    /// never inferred: most events carry no credential attribution.
    pub scope: Vec<ScopeRule>,
    /// Account ref → the plan it holds.
    #[serde(deserialize_with = "unique_map")]
    pub accounts: BTreeMap<String, AccountPlan>,
    /// Vault credential ref → plan, for providers whose counters have no account.
    /// Never combined with accounts.
    #[serde(deserialize_with = "unique_map")]
    pub credentials: BTreeMap<String, AccountPlan>,
}

impl QuotaProviderConfig {
    /// A provider uses one identity namespace, so equal account and credential
    /// strings can never alias in the estimator's counter maps.
    pub fn members(&self) -> &BTreeMap<String, AccountPlan> {
        if self.credentials.is_empty() {
            &self.accounts
        } else {
            &self.credentials
        }
    }

    fn members_mut(&mut self) -> &mut BTreeMap<String, AccountPlan> {
        if self.credentials.is_empty() {
            &mut self.accounts
        } else {
            &mut self.credentials
        }
    }

    fn identity_field(&self) -> &'static str {
        if self.credentials.is_empty() {
            "account_ref"
        } else {
            "credential_ref"
        }
    }
}

/// Reader keys can differ from their ledger producer names.
pub fn source_in_scope(provider: &QuotaProviderConfig, source: &str) -> bool {
    provider.scope.iter().any(|rule| {
        source == rule.producer
            || source.starts_with(&format!("{}:", rule.producer))
            || (rule.producer == "opencode" && source == "opencode-v2")
            || (rule.producer == "claude-code" && source == "claude-code-workers")
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowSelector {
    pub pool_key: String,
    pub window_minutes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeRule {
    pub producer: String,
    pub provider: String,
    /// `null` counts every charge basis; a value counts only that basis.
    #[serde(deserialize_with = "required_nullable")]
    pub charge_basis: Option<String>,
    /// `null` counts every model; a value selects a literal, case-sensitive prefix.
    #[serde(deserialize_with = "required_nullable")]
    pub model_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountPlan {
    /// A fusiform plan key, `<provider>/<tier>`, matched exactly.
    pub plan: String,
    /// Wins over both the plan-key override and fusiform.
    #[serde(deserialize_with = "required_nullable")]
    pub price_override: Option<PriceOverride>,
}

/// A monthly price in minor units: `20000` at exponent `2` is 200.00.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceOverride {
    pub minor_units: i64,
    pub exponent: i32,
    pub currency: String,
    pub period: String,
}

/// Present-but-nullable: `deserialize_with` makes serde report a missing
/// member instead of silently reading it as `None`.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// A JSON object into a map, refusing a repeated key: the default map decode
/// keeps the last value, so a second account entry would silently replace the
/// first.
fn unique_map<'de, D, V>(deserializer: D) -> Result<BTreeMap<String, V>, D::Error>
where
    D: Deserializer<'de>,
    V: Deserialize<'de>,
{
    struct UniqueMap<V>(std::marker::PhantomData<V>);
    impl<'de, V: Deserialize<'de>> de::Visitor<'de> for UniqueMap<V> {
        type Value = BTreeMap<String, V>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an object with unique member names")
        }
        fn visit_map<A: de::MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
            let mut map = BTreeMap::new();
            while let Some((key, value)) = access.next_entry::<String, V>()? {
                if map.contains_key(&key) {
                    return Err(de::Error::custom(format!("duplicate member '{key}'")));
                }
                map.insert(key, value);
            }
            Ok(map)
        }
    }
    deserializer.deserialize_map(UniqueMap(std::marker::PhantomData))
}

impl MultiplierConfig {
    /// Semantic checks serde cannot express. Every refusal names its member.
    pub fn validate(&self) -> Result<(), String> {
        for (plan_plane, api) in &self.api_twins {
            if plan_plane.is_empty() || api.is_empty() || plan_plane == api {
                return Err(format!(
                    "subscription_multiplier.api_twins entry '{plan_plane}' must map a non-empty \
                     provider to a different non-empty provider"
                ));
            }
        }
        for (plan, price) in &self.plan_price_overrides {
            validate_plan_key(plan).map_err(|error| {
                format!("subscription_multiplier.plan_price_overrides: {error}")
            })?;
            price.validate().map_err(|error| {
                format!("subscription_multiplier.plan_price_overrides '{plan}': {error}")
            })?;
        }
        for (quota_provider, provider) in &self.quota_providers {
            let at = format!("subscription_multiplier.quota_providers '{quota_provider}'");
            if quota_provider.is_empty() {
                return Err(
                    "subscription_multiplier.quota_providers keys must be non-empty".into(),
                );
            }
            if provider.window.pool_key.is_empty()
                || !(1..=MINUTES_PER_MONTH).contains(&provider.window.window_minutes)
            {
                return Err(format!(
                    "{at}: window needs a non-empty pool_key and window_minutes in 1..={MINUTES_PER_MONTH}"
                ));
            }
            if provider.scope.is_empty() {
                return Err(format!("{at}: scope must name at least one rule"));
            }
            let mut rules = BTreeSet::new();
            for rule in &provider.scope {
                if rule.producer.is_empty() || rule.provider.is_empty() {
                    return Err(format!("{at}: a scope rule needs producer and provider"));
                }
                if rule.model_prefix.as_deref().is_some_and(str::is_empty) {
                    return Err(format!(
                        "{at}: scope model_prefix must be non-empty or null"
                    ));
                }
                if let Some(basis) = &rule.charge_basis {
                    if ChargeBasis::parse(basis).is_none() {
                        return Err(format!(
                            "{at}: scope charge_basis '{basis}' is not one of {:?}",
                            accepted_charge_bases()
                        ));
                    }
                }
                if !rules.insert((
                    &rule.producer,
                    &rule.provider,
                    &rule.charge_basis,
                    &rule.model_prefix,
                )) {
                    return Err(format!(
                        "{at}: scope rule {}/{} is listed twice",
                        rule.producer, rule.provider
                    ));
                }
            }
            if provider.accounts.is_empty() == provider.credentials.is_empty() {
                return Err(format!(
                    "{at}: name either non-empty accounts or non-empty credentials, never both"
                ));
            }
            for (account, plan) in provider.members() {
                if account.is_empty() {
                    return Err(format!(
                        "{at}: {} must be non-empty",
                        provider.identity_field()
                    ));
                }
                validate_plan_key(&plan.plan)
                    .map_err(|error| format!("{at} account '{account}': {error}"))?;
                if let Some(price) = &plan.price_override {
                    price
                        .validate()
                        .map_err(|error| format!("{at} account '{account}': {error}"))?;
                }
            }
        }
        Ok(())
    }
}

/// The spellings `ChargeBasis::parse` accepts, in declaration order, for
/// naming them in a config error. Each variant names its successor in a match
/// with no wildcard arm, so a new variant does not compile until it is placed
/// in the chain, and every spelling comes from `ChargeBasis::as_str`.
fn accepted_charge_bases() -> Vec<&'static str> {
    let mut accepted = Vec::new();
    let mut next = Some(ChargeBasis::MeteredApi);
    while let Some(basis) = next {
        accepted.push(basis.as_str());
        next = match basis {
            ChargeBasis::MeteredApi => Some(ChargeBasis::SubscriptionIncluded),
            ChargeBasis::SubscriptionIncluded => Some(ChargeBasis::Unknown),
            ChargeBasis::Unknown => None,
        };
    }
    accepted
}

fn validate_plan_key(plan: &str) -> Result<(), String> {
    match plan.split_once('/') {
        Some((provider, tier))
            if !provider.is_empty() && !tier.is_empty() && !tier.contains('/') =>
        {
            Ok(())
        }
        _ => Err(format!("plan key '{plan}' must be '<provider>/<tier>'")),
    }
}

impl PriceOverride {
    fn validate(&self) -> Result<(), String> {
        if self.minor_units <= 0 {
            return Err("price minor_units must be positive".into());
        }
        if self.currency != "USD" || self.period != "month" {
            return Err(
                "price must be quoted in USD per month, the currency and period the catalog's \
                 token rates and the formula use"
                    .into(),
            );
        }
        amount_to_nanos(self.minor_units, self.exponent)
            .map(|_| ())
            .ok_or_else(|| "price exponent must be 0..=9 and the amount must fit".into())
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

/// Measures a reset-aware high-water series and its bounded hourly coverage.
#[cfg(test)]
fn account_delta(
    conn: &Connection,
    quota_provider: &str,
    account_ref: &str,
    window: &WindowSelector,
    start_ms: i64,
    end_ms: i64,
) -> rusqlite::Result<AccountDelta> {
    clean::counter_delta(
        conn,
        quota_provider,
        (account_ref, false),
        window,
        start_ms,
        end_ms,
    )
}

// ---------------------------------------------------------------------------
// Observation plan (first read) and the network resolutions
// ---------------------------------------------------------------------------

/// Owned quota and usage inputs from one read snapshot, plus their model set.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationPlan {
    pub(crate) quota_provider: String,
    pub provider: QuotaProviderConfig,
    pub(crate) interval_start_ms: i64,
    pub(crate) interval_end_ms: i64,
    /// One entry per configured account.
    pub(crate) accounts: Vec<AccountDelta>,
    /// Accounts with readings in this window that the config does not name.
    /// Their Δused% is reported and never counted.
    pub(crate) unconfigured_accounts: Vec<AccountDelta>,
    /// The interval clipped to the span the quota data covers, or `None` when
    /// no configured account has a reading in the interval.
    pub token_interval_ms: Option<(i64, i64)>,
    /// `(event provider, model)` pairs in scope inside the token interval.
    pub(crate) models: BTreeSet<(String, Option<String>)>,
    events: Vec<ScopedEvent>,
}

pub fn plan_observation(
    conn: &Connection,
    config: &MultiplierConfig,
    quota_provider: &str,
    interval_start_ms: i64,
    interval_end_ms: i64,
) -> Result<ObservationPlan, MultiplierError> {
    let provider = config
        .quota_providers
        .get(quota_provider)
        .ok_or_else(|| MultiplierError::Refused {
            code: "unknown_quota_provider",
            message: format!("quota provider '{quota_provider}' is not configured"),
        })?
        .clone();
    if interval_start_ms >= interval_end_ms {
        return Err(MultiplierError::Refused {
            code: "invalid_range",
            message: "interval start must precede its end".into(),
        });
    }
    let mut accounts = Vec::new();
    for account in provider.members().keys() {
        accounts.push(clean::counter_delta(
            conn,
            quota_provider,
            (account, !provider.credentials.is_empty()),
            &provider.window,
            interval_start_ms,
            interval_end_ms,
        )?);
    }
    let seen_sql = if provider.credentials.is_empty() {
        "SELECT DISTINCT account_ref FROM window_sample
              WHERE provider = ?1 AND pool_key = ?2 AND window_minutes = ?3
                AND fetched_at_instant >= ?4 AND fetched_at_instant < ?5
                AND NOT(provider='antigravity' AND source IS 'vault' AND fetched_at_instant < ?6)
             ORDER BY account_ref"
    } else {
        "SELECT DISTINCT credential_ref FROM credential_window_sample
              WHERE provider = ?1 AND pool_key = ?2 AND window_minutes = ?3
                AND fetched_at_instant >= ?4 AND fetched_at_instant < ?5
                AND NOT(provider='antigravity' AND source IS 'vault' AND fetched_at_instant < ?6)
             ORDER BY credential_ref"
    };
    let seen: Vec<String> = conn
        .prepare(seen_sql)?
        .query_map(
            params![
                quota_provider,
                provider.window.pool_key,
                provider.window.window_minutes,
                clamp_nanos(millis_to_nanos(interval_start_ms)),
                clamp_nanos(millis_to_nanos(interval_end_ms)),
                clean::ANTIGRAVITY_WRONG_VAULT_CUTOFF_NS
            ],
            |row| row.get(0),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let mut unconfigured_accounts = Vec::new();
    for account in seen
        .iter()
        .filter(|account| !provider.members().contains_key(*account))
    {
        let measured = clean::counter_delta(
            conn,
            quota_provider,
            (account, !provider.credentials.is_empty()),
            &provider.window,
            interval_start_ms,
            interval_end_ms,
        )?;
        unconfigured_accounts.push(measured);
    }
    let token_interval_ms = accounts
        .iter()
        .filter_map(|account| account.coverage_ms)
        .reduce(|(start_a, end_a), (start_b, end_b)| (start_a.min(start_b), end_a.max(end_b)));
    // Own both sides before catalog I/O: a concurrent ingest must not change
    // the usage being valued after model discovery has finished.
    let mut events = Vec::new();
    for_scoped_events(
        conn,
        &provider.scope,
        interval_start_ms,
        interval_end_ms,
        |event| {
            events.push(event);
            Ok(())
        },
    )?;
    let models = events
        .iter()
        .map(|event| (event.tokens.provider.clone(), event.tokens.model.clone()))
        .collect();
    Ok(ObservationPlan {
        quota_provider: quota_provider.to_string(),
        provider,
        interval_start_ms,
        interval_end_ms,
        accounts,
        unconfigured_accounts,
        token_interval_ms,
        models,
        events,
    })
}

/// The scope rules as one SQL predicate over `usage_event e`, with its
/// parameters in order.
fn scope_predicate(rules: &[ScopeRule]) -> (String, Vec<SqlValue>) {
    let mut clauses = Vec::new();
    let mut values = Vec::new();
    for rule in rules {
        values.push(SqlValue::Text(rule.producer.clone()));
        values.push(SqlValue::Text(rule.provider.clone()));
        let mut clause = "(e.producer = ? AND e.provider = ?".to_string();
        if let Some(basis) = &rule.charge_basis {
            clause.push_str(" AND e.charge_basis = ?");
            values.push(SqlValue::Text(basis.clone()));
        }
        if let Some(prefix) = &rule.model_prefix {
            clause.push_str(" AND instr(e.model, ?) = 1");
            values.push(SqlValue::Text(prefix.clone()));
        }
        clause.push(')');
        clauses.push(clause);
    }
    (format!("({})", clauses.join(" OR ")), values)
}

fn scope_interval(rules: &[ScopeRule], from_ms: i64, to_ms: i64) -> (String, Vec<SqlValue>) {
    let (predicate, mut values) = scope_predicate(rules);
    values.extend([SqlValue::Integer(from_ms), SqlValue::Integer(to_ms)]);
    (predicate, values)
}

/// The API provider an event's tokens are valued under.
fn catalog_provider<'a>(config: &'a MultiplierConfig, event_provider: &'a str) -> &'a str {
    config
        .api_twins
        .get(event_provider)
        .map(String::as_str)
        .unwrap_or(event_provider)
}

/// The catalog `(provider, model)` pairs [`resolve_rates`] must look up.
pub fn catalog_models(
    config: &MultiplierConfig,
    plan: &ObservationPlan,
) -> BTreeSet<(String, String)> {
    plan.models
        .iter()
        .filter_map(|(provider, model)| {
            model.as_ref().map(|model| {
                (
                    catalog_provider(config, provider).to_string(),
                    model.clone(),
                )
            })
        })
        .collect()
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
    /// The target catalog model whose historical rates are used to value this alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_model: Option<CatalogModel>,
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
}

/// Borrowed index for repeated lookups without allocating a tuple of Strings.
struct ModelLookup<'a> {
    providers: BTreeMap<&'a str, BTreeMap<&'a str, &'a ModelResolution>>,
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
        Self { providers }
    }

    fn get(&self, provider: &str, model: &str) -> Option<&'a ModelResolution> {
        self.providers.get(provider)?.get(model).copied()
    }
}

struct ValuationRules<'a> {
    api_twins: &'a BTreeMap<String, String>,
    reasoning_billed_as_output: &'a [String],
}

impl<'a> ValuationRules<'a> {
    fn from_config(config: &'a MultiplierConfig, reasoning_billed_as_output: &'a [String]) -> Self {
        Self {
            api_twins: &config.api_twins,
            reasoning_billed_as_output,
        }
    }

    fn catalog_provider<'b>(&'b self, event_provider: &'b str) -> &'b str {
        self.api_twins
            .get(event_provider)
            .map(String::as_str)
            .unwrap_or(event_provider)
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
    let mut resolved = ResolvedRates {
        resolved_at_ms: at_ms,
        models: BTreeMap::new(),
    };
    for (provider, model) in models {
        // Reasoning levels on the wire are not catalog identities. Prefer an
        // exact row, and only retry a missing row for the two supported levels.
        let lookup = catalog.get(provider, model, at_ms);
        let (lookup_model, mut lookup) = match (
            lookup_missing(&lookup),
            model
                .strip_suffix("-low")
                .or_else(|| model.strip_suffix("-high")),
        ) {
            (true, Some(base)) => (base, catalog.get(provider, base, at_ms)),
            _ => (model.as_str(), lookup),
        };
        let mut reference_model = None;
        if lookup_missing(&lookup) {
            // Aliases exist only on current reads. Discover the public identity
            // there, but use its rates at the original valuation instant.
            match catalog.get_current(provider, lookup_model) {
                Ok(CatalogLookup::Resolved(snapshot)) => {
                    if let Some(target) = snapshot.alias_target {
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

fn lookup_missing(lookup: &Result<CatalogLookup, CatalogClientError>) -> bool {
    match lookup {
        Ok(CatalogLookup::Resolved(snapshot)) => {
            snapshot.facts.is_empty()
                && snapshot.withheld_facts.is_empty()
                && snapshot.uncertain_facts.is_empty()
                && snapshot.overridden_facts.is_empty()
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
fn rates_from_snapshot(snapshot: &CatalogSnapshot) -> ModelResolution {
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
    let mut rates = ModelRates::default();
    for key in keys {
        let Ok(parsed) = parse_rate_key(key) else {
            return ModelResolution::Unpriceable {
                reason: RATE_FACT_UNRECOGNIZED.into(),
            };
        };
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
        match parsed.condition {
            None => {
                rates.base.insert(role, state);
            }
            Some(RateCondition::AboveContext(threshold)) => {
                rates
                    .tiers
                    .entry(threshold)
                    .or_default()
                    .insert(role, state);
            }
            Some(RateCondition::Mode(mode)) => {
                rates.modes.entry(mode).or_default().insert(role, state);
            }
            // No request records an unknown condition, so its rate never
            // applies and is not kept.
            Some(RateCondition::Unknown { .. }) => {}
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
    if snapshot.uncertain_facts.contains_key(key) {
        return unpriced("rate_uncertain");
    }
    match serde_json::from_value::<RateValue>(entry.clone()) {
        Err(_) => unpriced(RATE_UNDECODABLE),
        // Billing-plane claims are not API reference rates. This consumer
        // makes ordinary catalog reads and does not resolve plane rules.
        Ok(RateValue::BilledAs { .. }) => unpriced(RATE_UNDECODABLE),
        Ok(RateValue::Priced {
            inherited_from: Some(_),
            ..
        }) => unpriced("rate_inherited"),
        Ok(RateValue::Priced { amount, .. }) if amount.exponent != NANO_EXPONENT => {
            unpriced("rate_exponent_unsupported")
        }
        Ok(RateValue::Priced { amount, .. }) => RateState::Priced {
            units_per_million: amount.units,
        },
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
            ProtocolUnpricedReason::NoCatalogCoverage => "no_catalog_coverage",
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

pub fn resolve_plans(
    source: Option<&dyn PlanPriceSource>,
    config: &MultiplierConfig,
    provider: &QuotaProviderConfig,
    at_ms: i64,
) -> Result<BTreeMap<String, PlanPriceUsed>, MultiplierError> {
    let needs_fusiform = provider.members().values().any(|account| {
        account.price_override.is_none() && !config.plan_price_overrides.contains_key(&account.plan)
    });
    let served = if needs_fusiform {
        let source = source.ok_or_else(|| {
            MultiplierError::PlanPrices(
                "plan prices are needed and fusiform is not configured".into(),
            )
        })?;
        Some(
            source
                .plan_prices(PlanPricesRequest { provider_id: None })
                .map_err(|error| {
                    MultiplierError::PlanPrices(format!(
                        "plan.prices: {}: {}",
                        error.code, error.message
                    ))
                })?,
        )
    } else {
        None
    };
    let mut plans = BTreeMap::new();
    for (account_ref, account) in provider.members() {
        let mut used = PlanPriceUsed {
            account_ref: if provider.credentials.is_empty() {
                account_ref.clone()
            } else {
                String::new()
            },
            credential_ref: (!provider.credentials.is_empty()).then(|| account_ref.clone()),
            plan: account.plan.clone(),
            source: None,
            monthly_nanos: None,
            review_by_ms: None,
            plan_price_overdue: false,
            source_ref: None,
            unpriceable_reason: None,
            refusal_text: None,
        };
        let from_override = |price: &PriceOverride, origin: &str, used: &mut PlanPriceUsed| {
            used.source = Some(origin.to_string());
            used.monthly_nanos = amount_to_nanos(price.minor_units, price.exponent);
        };
        if let Some(price) = &account.price_override {
            from_override(price, "account_override", &mut used);
        } else if let Some(price) = config.plan_price_overrides.get(&account.plan) {
            from_override(price, "plan_override", &mut used);
        } else if let Some(served) = &served {
            fusiform_price(served, &account.plan, at_ms, &mut used);
        }
        plans.insert(account_ref.clone(), used);
    }
    Ok(plans)
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
    fn add(&mut self, role: &str, quantity: i64) -> Result<(), MoneyError> {
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
    /// Tokens in events whose prompt crossed a tier threshold and were priced
    /// at the tier's rates.
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
    /// Input, cache-read, cache-write, output and reasoning counts from unit
    /// `token` with exponent 0, priced or not, inside the token interval.
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
    pub excluded_pairs: BTreeMap<String, u64>,
    pub low_resolution: bool,
    pub scope_rules: Vec<ScopeRule>,
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

/// `(role, unit, source, quantity units, quantity exponent)` of one measurement.
type MeasuredRole = (String, String, String, Option<i64>, Option<i32>);

#[derive(Debug, Clone, PartialEq)]
struct EventTokens {
    producer: String,
    provider: String,
    model: Option<String>,
    /// The mode the request's record states it ran in, `None` when it states
    /// none. The ledger stores no request mode yet, so events read from the
    /// store carry `None`.
    mode: Option<String>,
    roles: Vec<MeasuredRole>,
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
fn for_scoped_events(
    conn: &Connection,
    scope: &[ScopeRule],
    from: i64,
    to: i64,
    mut consume: impl FnMut(ScopedEvent) -> Result<(), MultiplierError>,
) -> Result<(), MultiplierError> {
    let (predicate, values) = scope_interval(scope, from, to);
    let sql = format!(
        "SELECT e.event_key, e.producer, e.provider, e.model, m.role, m.unit, m.source,
                m.quantity_units,m.quantity_exponent,e.occurred_at_ms,e.work_class,e.account_ref
         FROM usage_event e LEFT JOIN measurement m ON m.event_key=e.event_key
         WHERE {predicate} AND e.occurred_at_ms >= CAST(? AS INTEGER)
           AND e.occurred_at_ms < CAST(? AS INTEGER) ORDER BY e.event_key"
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(values.iter()))?;
    let mut current: Option<ScopedEvent> = None;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        if current.as_ref().is_none_or(|event| event.id != id) {
            if let Some(event) = current.take() {
                consume(event)?;
            }
            current = Some(ScopedEvent {
                id,
                tokens: EventTokens {
                    producer: row.get(1)?,
                    provider: row.get(2)?,
                    model: row.get(3)?,
                    mode: None,
                    roles: Vec::new(),
                },
                occurred_at_ms: row.get(9)?,
                work_class: row.get(10)?,
                account_ref: row.get(11)?,
            });
        }
        if let Some(event) = current.as_mut() {
            if row.get::<_, Option<String>>(4)?.is_some() {
                event.tokens.roles.push(decode_role(row)?);
            }
        }
    }
    if let Some(event) = current {
        consume(event)?;
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

/// Prices the token side of `plan` and assembles the observation.
pub fn compute_observation(
    _conn: &Connection,
    config: &MultiplierConfig,
    plan: &ObservationPlan,
    rates: &ResolvedRates,
    plan_prices: &BTreeMap<String, PlanPriceUsed>,
    snapshot_at_ms: i64,
    reasoning_billed_as_output: &[String],
) -> Result<Observation, MultiplierError> {
    let mut lines: BTreeMap<(String, String, Option<String>), ModelLine> = BTreeMap::new();
    let mut unpriced: BTreeMap<(String, String, String), (i64, u64)> = BTreeMap::new();
    let mut tokens = RoleTokens::default();
    let lookup = ModelLookup::new(rates);
    let rules = ValuationRules::from_config(config, reasoning_billed_as_output);
    if let Some((from, to)) = plan.token_interval_ms {
        for scoped in plan
            .events
            .iter()
            .filter(|event| event.occurred_at_ms >= from && event.occurred_at_ms < to)
        {
            let event = &scoped.tokens;
            let resolution = event
                .model
                .as_deref()
                .and_then(|model| lookup.get(rules.catalog_provider(&event.provider), model));
            value_event(
                &rules,
                resolution,
                event,
                &mut lines,
                &mut unpriced,
                &mut tokens,
            )?;
        }
    }
    let mut tokens_outside_quota_coverage = 0_i64;
    for event in &plan.events {
        if plan
            .token_interval_ms
            .is_none_or(|(from, to)| event.occurred_at_ms < from || event.occurred_at_ms >= to)
        {
            let roles = event_quantities(
                &event.tokens,
                &mut RoleTokens::default(),
                &mut 0,
                &mut |_, _, _| Ok(()),
            )?;
            for quantity in roles.values() {
                tokens_outside_quota_coverage = tokens_outside_quota_coverage
                    .checked_add(*quantity)
                    .ok_or(MoneyError::OutOfRange)?;
            }
        }
    }

    let mut api_numerator = 0_i128;
    let mut base_rate_tier_untested_tokens = 0_i64;
    let mut api_nanos_by_model = Vec::new();
    for (_, mut line) in lines {
        line.api_nanos = round_nano_num(line.numerator)?;
        api_numerator = api_numerator
            .checked_add(line.numerator)
            .ok_or(MoneyError::OutOfRange)?;
        base_rate_tier_untested_tokens = base_rate_tier_untested_tokens
            .checked_add(line.base_rate_tier_untested_tokens)
            .ok_or(MoneyError::OutOfRange)?;
        line.rates = line
            .model
            .as_deref()
            .and_then(|model| lookup.get(rules.catalog_provider(&line.provider), model))
            .cloned();
        api_nanos_by_model.push(line);
    }
    let api_nanos = round_nano_num(api_numerator)?;

    // The denominator uses only identified counters whose plan has a price.
    let token_span_ms = plan
        .token_interval_ms
        .map(|(from, to)| to.checked_sub(from).ok_or(MoneyError::OutOfRange))
        .transpose()?
        .unwrap_or(0);
    let window_share = NanoRatio::new(
        i128::from(plan.provider.window.window_minutes),
        i128::from(MINUTES_PER_MONTH) * 100,
    )?;
    let fee_share = NanoRatio::new(i128::from(token_span_ms), i128::from(MS_PER_MONTH))?;
    let mut subscription = NanoRatio::new(0, 1)?;
    let mut fee_paid = NanoRatio::new(0, 1)?;
    let mut subscription_shares = Vec::new();
    let mut delta_percent = 0.0;
    let mut segments = 0;
    let mut excluded_pairs = ExcludedPairs::default();
    let mut accounts_included = Vec::new();
    let mut accounts_excluded = Vec::new();
    for account in &plan.accounts {
        excluded_pairs.decrease_boundary += account.decrease_boundaries;
        excluded_pairs.fetched_at_backwards += account.fetched_at_backwards;
        let price = plan_prices.get(&account.account_ref);
        match price.and_then(|price| price.monthly_nanos) {
            Some(monthly) => {
                let monthly = NanoRatio::new(i128::from(monthly), 1)?;
                let account_subscription = monthly
                    .mul(window_share)?
                    .mul(NanoRatio::percent(account.delta_percent)?)?;
                subscription = subscription.add(account_subscription)?;
                fee_paid = fee_paid.add(monthly.mul(fee_share)?)?;
                subscription_shares.push((account.account_ref.clone(), account_subscription));
                delta_percent += account.delta_percent;
                segments += account.segments();
                accounts_included.push(json!({
                    (plan.provider.identity_field()): account.account_ref,
                    "delta_percent": account.delta_percent,
                    "subscription_nanos": 0,
                    "readings": account.readings,
                    "decrease_boundaries": account.decrease_boundaries,
                    "fetched_at_backwards": account.fetched_at_backwards,
                    "stale_reset_readings": account.stale_reset_readings,
                    "reset_boundaries": account.reset_boundaries,
                    "coverage_ms": account.coverage_ms,
                }));
            }
            None => accounts_excluded.push(json!({
                (plan.provider.identity_field()): account.account_ref,
                "reason": price
                    .and_then(|price| price.unpriceable_reason.clone())
                    .unwrap_or_else(|| "plan_price_unresolved".into()),
                "delta_percent": account.delta_percent,
                "readings": account.readings,
            })),
        }
    }
    for account in &plan.unconfigured_accounts {
        accounts_excluded.push(json!({
            (plan.provider.identity_field()): account.account_ref,
            "reason": if plan.provider.credentials.is_empty() { "account_not_configured" } else { "credential_not_configured" },
            "delta_percent": account.delta_percent,
            "readings": account.readings,
        }));
    }
    let subscription_nanos = subscription.round()?;
    let fee_paid_nanos = fee_paid.round()?;
    for (account, amount) in accounts_included.iter_mut().zip(allocate_subscription(
        &subscription_shares,
        subscription_nanos,
    )?) {
        account["subscription_nanos"] = json!(amount);
    }
    // A missing list value is not evidence of free usage. Keep both ratios
    // unknown when unpriced tokens are the only contribution to a zero value.
    let unmeasurable = !plan.unconfigured_accounts.is_empty()
        || unpriced
            .keys()
            .any(|(reason, _, _)| reason == "missing_quantity")
        || (api_nanos == 0 && unpriced.values().any(|(tokens, _)| *tokens > 0));
    let ratio = |denominator: i64| {
        (denominator > 0 && !unmeasurable).then(|| api_nanos as f64 / denominator as f64)
    };
    let low_resolution =
        delta_percent <= 0.0 || segments as f64 / delta_percent > LOW_RESOLUTION_ERROR_SHARE;
    Ok(Observation {
        plan: None,
        routing_eligible: false,
        eligibility_reasons: vec!["legacy_estimator".into()],
        clean_hours_share: 0.0,
        saturated_share: 0.0,
        gap_share: 0.0,
        unpriced_share: 0.0,
        unconfigured_accounts: plan
            .unconfigured_accounts
            .iter()
            .map(|account| account.account_ref.clone())
            .collect(),
        source_backlog: Vec::new(),
        unattributed_event_share: 0.0,
        zero_measurement_events: 0,
        missing_quantity_events: 0,
        quota_provider: plan.quota_provider.clone(),
        observation_day: utc_day(plan.interval_end_ms)?,
        snapshot_at_ms,
        interval_start_ms: plan.interval_start_ms,
        interval_end_ms: plan.interval_end_ms,
        token_interval_start_ms: plan.token_interval_ms.map(|(from, _)| from),
        token_interval_end_ms: plan.token_interval_ms.map(|(_, to)| to),
        window_pool_key: plan.provider.window.pool_key.clone(),
        window_minutes: plan.provider.window.window_minutes,
        tokens,
        api_nanos,
        api_nanos_by_model,
        // A zero quantity has no value to lose, so it is not listed; a missing
        // quantity is, since its size is unknown rather than zero.
        unpriced_tokens: unpriced
            .into_iter()
            .filter(|((reason, _, _), (tokens, _))| *tokens > 0 || reason == "missing_quantity")
            .map(
                |((reason, model, role), (tokens, measurements))| UnpricedTokens {
                    reason,
                    model,
                    role,
                    tokens,
                    measurements,
                },
            )
            .collect(),
        tokens_outside_quota_coverage,
        base_rate_tier_untested_tokens,
        subscription_nanos,
        fee_paid_nanos,
        delta_percent,
        accounts_included,
        accounts_excluded,
        excluded_pairs: excluded_pairs.counts(),
        low_resolution,
        scope_rules: plan.provider.scope.clone(),
        plan_prices: plan_prices.values().cloned().collect(),
        catalog_resolved_at_ms: rates.resolved_at_ms,
        api_twins: config.api_twins.clone(),
        reasoning_billed_as_output: reasoning_billed_as_output.to_vec(),
        error_deflating: ERROR_DEFLATING.into(),
        error_inflating: ERROR_INFLATING.into(),
        multiplier: ratio(subscription_nanos),
        fee_multiplier: ratio(fee_paid_nanos),
    })
}

fn event_quantities<'a>(
    event: &'a EventTokens,
    measured: &mut RoleTokens,
    inclusive_output_adjustment: &mut i64,
    count_unpriced: &mut impl FnMut(&str, &str, i64) -> Result<(), MultiplierError>,
) -> Result<BTreeMap<&'a str, i64>, MultiplierError> {
    // Measured quantities by role. A quantity that is not a whole token count
    // cannot be valued and is counted by reason.
    let mut quantities: BTreeMap<&str, i64> = BTreeMap::new();
    let mut output_source = None;
    for (role, unit, source, units, exponent) in &event.roles {
        // Missing quantities exclude the hour even for an unsupported unit;
        // absence is unknown usage, not evidence of a zero-sized measurement.
        if units.is_none() || exponent.is_none() {
            count_unpriced("missing_quantity", role, units.unwrap_or(0))?;
            continue;
        }
        // The catalog's rate.cache_write cannot distinguish token_cache_5m
        // from token_cache_1h. Keep these units in the unpriced breakdown, not
        // the unit=token totals on either side of quota coverage.
        if unit != "token" {
            count_unpriced(
                "unit_not_priced",
                &format!("{role}/{unit}"),
                units.unwrap_or(0),
            )?;
            continue;
        }
        match (units, exponent) {
            (Some(units), Some(0)) => {
                measured.add(role, *units)?;
                if role == "output" {
                    output_source = Some(source.as_str());
                }
                if RATE_ROLES.contains(&role.as_str()) {
                    quantities.insert(role.as_str(), *units);
                } else {
                    count_unpriced("unrecognized_role", role, *units)?;
                }
            }
            (None, _) => count_unpriced("missing_quantity", role, 0)?,
            (Some(units), Some(_)) => count_unpriced("fractional_quantity", role, *units)?,
            (Some(units), None) => count_unpriced("missing_quantity", role, *units)?,
        }
    }
    // Some BROCA output counts include reasoning tokens. Remove that overlap
    // once, using the ledger's rule, for both valuation and token counts on
    // either side of quota coverage.
    if let (Some(output), Some(source)) = (quantities.get("output").copied(), output_source) {
        let reasoning = quantities.get("reasoning").copied();
        match effective_output(
            &event.producer,
            Some(&event.provider),
            source,
            output,
            reasoning,
        ) {
            EffectiveOutput::AsStored => {}
            EffectiveOutput::ReasoningRemoved(effective) => {
                quantities.insert("output", effective);
                let removed = output
                    .checked_sub(effective)
                    .ok_or(MoneyError::OutOfRange)?;
                *inclusive_output_adjustment = inclusive_output_adjustment
                    .checked_add(removed)
                    .ok_or(MoneyError::OutOfRange)?;
            }
            EffectiveOutput::ReasoningExceedsOutput => {
                // Output below its own reasoning cannot be made disjoint.
                count_unpriced("inclusive_output_below_reasoning", "output", output)?;
                count_unpriced(
                    "inclusive_output_below_reasoning",
                    "reasoning",
                    reasoning.unwrap_or(0),
                )?;
                quantities.remove("output");
                quantities.remove("reasoning");
            }
        }
    }
    Ok(quantities)
}

fn value_event(
    rules: &ValuationRules<'_>,
    resolution: Option<&ModelResolution>,
    event: &EventTokens,
    lines: &mut BTreeMap<(String, String, Option<String>), ModelLine>,
    unpriced: &mut BTreeMap<(String, String, String), (i64, u64)>,
    totals: &mut RoleTokens,
) -> Result<(), MultiplierError> {
    let mut model_label = None;
    let line = lines
        .entry((
            event.producer.clone(),
            event.provider.clone(),
            event.model.clone(),
        ))
        .or_insert_with(|| ModelLine {
            producer: event.producer.clone(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            ..ModelLine::default()
        });
    line.events += 1;
    let mut count_unpriced = |reason: &str, role: &str, tokens: i64| {
        let model_label = model_label.get_or_insert_with(|| {
            format!(
                "{}/{}/{}",
                event.producer,
                event.provider,
                event.model.as_deref().unwrap_or("<none>")
            )
        });
        let entry = unpriced
            .entry((reason.to_string(), model_label.clone(), role.to_string()))
            .or_insert((0, 0));
        entry.0 = entry.0.checked_add(tokens).ok_or(MoneyError::OutOfRange)?;
        entry.1 = entry.1.checked_add(1).ok_or(MoneyError::OutOfRange)?;
        Ok::<_, MultiplierError>(())
    };

    let quantities = event_quantities(
        event,
        &mut line.measured_tokens,
        &mut line.inclusive_output_adjustment,
        &mut count_unpriced,
    )?;
    for (role, quantity) in &quantities {
        totals.add(role, *quantity)?;
    }
    let event_total = quantities.values().try_fold(0_i64, |sum, quantity| {
        sum.checked_add(*quantity).ok_or(MoneyError::OutOfRange)
    })?;

    if event.model.is_none() {
        for (role, quantity) in &quantities {
            count_unpriced("missing_model", role, *quantity)?;
        }
        return Ok(());
    }
    let api_provider = rules.catalog_provider(&event.provider);
    let model_rates = match resolution {
        Some(ModelResolution::Rates(model_rates)) => model_rates,
        Some(ModelResolution::Unpriceable { reason }) => {
            for (role, quantity) in &quantities {
                count_unpriced(reason, role, *quantity)?;
            }
            return Ok(());
        }
        None => {
            for (role, quantity) in &quantities {
                count_unpriced("rate_not_resolved", role, *quantity)?;
            }
            return Ok(());
        }
    };

    // Tier: decided from this event's own prompt, and only where one event is
    // one call. The largest threshold the prompt exceeds applies to every role.
    // The prompt is read from the event's measurements as stored, including
    // the lifetime-split cache writes that are not priced above, with the
    // same function ledger pricing decides its tiers with.
    let per_request = PER_REQUEST_PRODUCERS.contains(&event.producer.as_str());
    let prompt =
        prompt_size(event.roles.iter().map(|(role, unit, _, units, exponent)| {
            (role.as_str(), unit.as_str(), *units, *exponent)
        }));
    if per_request && !model_rates.tiers.is_empty() && prompt.is_none() {
        // A prompt measurement with no usable quantity leaves the prompt's
        // size unknown, so neither the base nor a tier rate can be chosen.
        // Ledger pricing refuses the same request as `rate_tiered`.
        for (role, quantity) in &quantities {
            count_unpriced("rate_tiered", role, *quantity)?;
        }
        return Ok(());
    }
    let tier = if per_request {
        prompt.and_then(|prompt| {
            model_rates
                .tiers
                .iter()
                .rev()
                .find(|(threshold, _)| prompt.exceeds(**threshold))
        })
    } else {
        if !model_rates.tiers.is_empty() {
            line.base_rate_tier_untested_tokens = line
                .base_rate_tier_untested_tokens
                .checked_add(event_total)
                .ok_or(MoneyError::OutOfRange)?;
        }
        None
    };
    if tier.is_some() {
        line.tiered_tokens = line
            .tiered_tokens
            .checked_add(event_total)
            .ok_or(MoneyError::OutOfRange)?;
    }
    // Mode: a request recorded in a non-standard mode prices at that mode's
    // rates only. One recorded as standard, or with no recorded mode, prices
    // at the base; the unrecorded ones are counted where a mode rate existed
    // that they might have owed.
    let stated_mode = stated_mode_condition(event.mode.as_deref());
    if event.mode.is_none() && !model_rates.modes.is_empty() {
        line.mode_unrecorded_tokens = line
            .mode_unrecorded_tokens
            .checked_add(event_total)
            .ok_or(MoneyError::OutOfRange)?;
    }
    let mode_rates = stated_mode.map(|mode| model_rates.modes.get(mode));
    let reasoning_as_output = rules
        .reasoning_billed_as_output
        .iter()
        .any(|provider| provider == api_provider);
    for (role, quantity) in quantities {
        if quantity == 0 {
            continue;
        }
        let rate_role = if role == "reasoning" && reasoning_as_output {
            "output"
        } else {
            role
        };
        let state = match mode_rates {
            // No catalog key prices a stated mode above a context tier, and
            // neither the tier rate nor the mode rate alone is that price.
            Some(_) if tier.is_some() => {
                count_unpriced("mode_rate_above_tier", role, quantity)?;
                continue;
            }
            // A stated mode is priced at its own rate or not at all.
            Some(rates) => rates.and_then(|rates| rates.get(rate_role)),
            // A role whose tier key is absent is valued at its base rate, as
            // the catalog publishes no separate long-context price for it.
            None => tier
                .and_then(|(_, tier_rates)| tier_rates.get(rate_role))
                .or_else(|| model_rates.base.get(rate_role)),
        };
        match state {
            Some(RateState::Priced { units_per_million }) => {
                let product = i128::from(*units_per_million)
                    .checked_mul(i128::from(quantity))
                    .ok_or(MoneyError::OutOfRange)?;
                line.numerator = line
                    .numerator
                    .checked_add(product)
                    .ok_or(MoneyError::OutOfRange)?;
            }
            Some(RateState::Unpriced { reason }) => count_unpriced(reason, role, quantity)?,
            None => count_unpriced("no_rate", role, quantity)?,
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// Whether any row for this provider and day is recorded, original or
/// superseding. The daily stage asks this, so a day an operator has corrected
/// is never given a second automatic row.
pub fn observation_exists(
    conn: &Connection,
    quota_provider: &str,
    observation_day: &str,
) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM multiplier_observation
          WHERE quota_provider = ?1 AND observation_day = ?2)",
        params![quota_provider, observation_day],
        |row| row.get(0),
    )
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

pub fn observation_plan_head(
    conn: &Connection,
    quota_provider: &str,
    day: &str,
    plan: Option<&str>,
) -> rusqlite::Result<Option<i64>> {
    conn.query_row("SELECT o.observation_id FROM multiplier_observation o
        WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.plan IS ?3
        AND NOT EXISTS (SELECT 1 FROM multiplier_observation child WHERE child.supersedes=o.observation_id)",
        params![quota_provider, day, plan], |row| row.get(0)).optional()
}

/// Appends a day's original observation. A second row for the same provider
/// and day is refused before the insert (the table's partial unique index
/// refuses a second original too).
pub fn insert_observation_in_tx(
    tx: &Transaction<'_>,
    observation: &Observation,
) -> Result<(), MultiplierError> {
    if observation_plan_head(
        tx,
        &observation.quota_provider,
        &observation.observation_day,
        observation.plan.as_deref(),
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

/// Appends a recomputed observation that supersedes the day's current head.
/// The head is read inside the write transaction, so the new row always
/// replaces the latest one. A day with no row is refused: there is nothing to
/// correct, and the daily stage owns a day's first row.
pub fn insert_superseding_observation_in_tx(
    tx: &Transaction<'_>,
    observation: &Observation,
) -> Result<Superseded, MultiplierError> {
    let head = observation_plan_head(
        tx,
        &observation.quota_provider,
        &observation.observation_day,
        observation.plan.as_deref(),
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
            unattributed_event_share, zero_measurement_events, missing_quantity_events
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,
                  ?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41,?42,?43,?44,?45,?46,?47,?48,?49)",
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
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// `spend.multiplier.report`: the latest day's observation per quota provider,
/// and one observation per provider and day for every day whose interval ends
/// in `[from_ms, to_ms)`. Only a day's head row is served: a row an operator
/// recompute superseded is kept in the table but never reported. Each row
/// carries `supersedes`, the id of the row it replaced or null.
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

/// Hourly usage valued using the latest recorded multiplier's list rates.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiplierPaceRequest {
    pub quota_provider: String,
    #[serde(default)]
    pub plan: Option<String>,
    pub from_ms: i64,
    pub to_ms: i64,
}

const HOUR_MS: i64 = 3_600_000;
// A weekly calibration may describe the current workload for daily routing,
// but a head older than two days needs a refreshed estimate or fallback.
const PACE_MAX_HEAD_AGE_MS: i64 = 2 * 24 * HOUR_MS;
// Bound the read work while allowing a trailing week plus UTC alignment tails.
const PACE_MAX_RANGE_HOURS: i64 = 192;

#[derive(Deserialize)]
struct PaceRateLine {
    producer: String,
    provider: String,
    model: Option<String>,
    rates: Option<ModelResolution>,
}

struct PaceHead {
    observation_id: i64,
    observation_day: String,
    api_nanos: i64,
    delta_percent: f64,
    api_nanos_by_model: String,
    api_twins: String,
    reasoning_billed_as_output: String,
}

type PaceRates<'a> =
    BTreeMap<&'a str, BTreeMap<&'a str, BTreeMap<Option<&'a str>, Option<&'a ModelResolution>>>>;

fn pace_rates(lines: &[PaceRateLine]) -> PaceRates<'_> {
    let mut index: PaceRates<'_> = BTreeMap::new();
    for line in lines {
        index
            .entry(&line.producer)
            .or_default()
            .entry(&line.provider)
            .or_default()
            .insert(line.model.as_deref(), line.rates.as_ref());
    }
    index
}

#[derive(Default)]
struct PaceClass {
    lines: BTreeMap<(String, String, Option<String>), ModelLine>,
    unpriced: BTreeMap<(String, String, String), (i64, u64)>,
    tokens: RoleTokens,
}

/// Uses current scope declarations but the head's recorded valuation rules and
/// rates. No catalog lookup is needed, even for newly encountered models.
pub fn multiplier_pace(
    conn: &Connection,
    config: Option<&MultiplierConfig>,
    request: &MultiplierPaceRequest,
) -> Result<Value, MultiplierError> {
    let refuse = |code, message| MultiplierError::Refused { code, message };
    let config = config
        .filter(|config| config.quota_providers.contains_key(&request.quota_provider))
        .ok_or_else(|| {
            refuse(
                "unknown_quota_provider",
                format!("unknown quota provider '{}'", request.quota_provider),
            )
        })?;
    let provider = &config.quota_providers[&request.quota_provider];
    let plans: BTreeSet<_> = provider
        .members()
        .values()
        .map(|account| account.plan.as_str())
        .collect();
    if request.plan.is_none() && plans.len() > 1 {
        return Err(refuse(
            "plan_required",
            "pace requires a plan when provider accounts use different plans".into(),
        ));
    }
    let selected_plan = request.plan.as_deref().or_else(|| plans.first().copied());
    if request.from_ms >= request.to_ms {
        return Err(refuse(
            "invalid_range",
            "from_ms must be earlier than to_ms".into(),
        ));
    }
    // Euclidean division also aligns times before the Unix epoch correctly.
    let from = i128::from(request.from_ms).div_euclid(i128::from(HOUR_MS)) * i128::from(HOUR_MS);
    let to = (i128::from(request.to_ms) + i128::from(HOUR_MS) - 1).div_euclid(i128::from(HOUR_MS))
        * i128::from(HOUR_MS);
    if to - from > i128::from(PACE_MAX_RANGE_HOURS) * i128::from(HOUR_MS) {
        return Err(refuse(
            "range_too_large",
            format!("aligned range must not exceed {PACE_MAX_RANGE_HOURS} hours"),
        ));
    }
    let from = i64::try_from(from)
        .map_err(|_| refuse("invalid_range", "aligned from_ms overflows i64".into()))?;
    let to = i64::try_from(to)
        .map_err(|_| refuse("invalid_range", "aligned to_ms overflows i64".into()))?;
    let head = conn
        .query_row(
            "SELECT o.observation_id, o.observation_day, o.api_nanos, o.delta_percent,
                o.api_nanos_by_model, o.api_twins, o.reasoning_billed_as_output
         FROM multiplier_observation o WHERE o.quota_provider = ?1 AND o.routing_eligible=1
            AND (?2 IS NULL OR o.plan=?2)
            AND o.interval_end_ms >= ?3 - ?4
           AND NOT EXISTS (SELECT 1 FROM multiplier_observation child
                           WHERE child.supersedes = o.observation_id)
         ORDER BY o.observation_day DESC, o.observation_id DESC LIMIT 1",
            params![
                request.quota_provider,
                selected_plan,
                request.to_ms,
                PACE_MAX_HEAD_AGE_MS
            ],
            |row| {
                Ok(PaceHead {
                    observation_id: row.get(0)?,
                    observation_day: row.get(1)?,
                    api_nanos: row.get(2)?,
                    delta_percent: row.get(3)?,
                    api_nanos_by_model: row.get(4)?,
                    api_twins: row.get(5)?,
                    reasoning_billed_as_output: row.get(6)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| {
            refuse(
                "no_multiplier_observation",
                "no multiplier row is recorded for this provider".into(),
            )
        })?;
    let decode_error = |error: serde_json::Error| MultiplierError::Encode(error.to_string());
    let rate_lines: Vec<PaceRateLine> =
        serde_json::from_str(&head.api_nanos_by_model).map_err(decode_error)?;
    let stored_rates = pace_rates(&rate_lines);
    let api_twins = serde_json::from_str(&head.api_twins).map_err(decode_error)?;
    let reasoning_billed_as_output: Vec<String> =
        serde_json::from_str(&head.reasoning_billed_as_output).map_err(decode_error)?;
    let valuation_rules = ValuationRules {
        api_twins: &api_twins,
        reasoning_billed_as_output: &reasoning_billed_as_output,
    };
    // Like the report's multipliers, an unusable denominator yields null, not
    // an invented zero rate. Nanodollars are rounded once to an integer.
    let per_point_nanos = if head.delta_percent > 0.0 && head.api_nanos > 0 {
        let percent = NanoRatio::percent(head.delta_percent)?;
        Some(
            NanoRatio::new(i128::from(head.api_nanos), 1)?
                .mul(NanoRatio::new(percent.denominator, percent.numerator)?)?
                .round()?,
        )
        .filter(|nanos| *nanos > 0)
    } else {
        None
    };
    let points = |nanos: i64| per_point_nanos.map(|per_point| nanos as f64 / per_point as f64);
    let (predicate, values) =
        scope_predicate(&config.quota_providers[&request.quota_provider].scope);
    let undated_events: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM usage_event e WHERE {predicate} AND e.occurred_at_ms IS NULL"
        ),
        params_from_iter(values.iter()),
        |row| row.get(0),
    )?;
    let mut hours: BTreeMap<i64, BTreeMap<String, BTreeMap<String, PaceClass>>> = (from..to)
        .step_by(usize::try_from(HOUR_MS).expect("one hour in milliseconds fits usize"))
        .map(|hour| (hour, BTreeMap::new()))
        .collect();
    for_scoped_events(
        conn,
        &config.quota_providers[&request.quota_provider].scope,
        from,
        to,
        |scoped| {
            if let Some(account) = scoped
                .account_ref
                .as_ref()
                .and_then(|account| provider.accounts.get(account))
            {
                if Some(account.plan.as_str()) != selected_plan {
                    return Ok(());
                }
            } else if plans.len() > 1 {
                return Ok(());
            }
            let event = scoped.tokens;
            let hour = scoped.occurred_at_ms.div_euclid(HOUR_MS) * HOUR_MS;
            let work_class = scoped.work_class.unwrap_or_else(|| "unlabelled".into());
            // Rates belong to the exact producer/provider/model line. A different
            // producer's coverage must not hide a missing line in this snapshot.
            let resolution = stored_rates
                .get(event.producer.as_str())
                .and_then(|providers| providers.get(event.provider.as_str()))
                .and_then(|models| models.get(&event.model.as_deref()))
                .copied()
                .flatten();
            let class = hours
                .get_mut(&hour)
                .expect("aligned hour")
                .entry(event.producer.clone())
                .or_default()
                .entry(work_class)
                .or_default();
            value_event(
                &valuation_rules,
                resolution,
                &event,
                &mut class.lines,
                &mut class.unpriced,
                &mut class.tokens,
            )
        },
    )?;
    let hours: Vec<Value> = hours.into_iter().map(|(hour, producers)| -> Result<_, MultiplierError> {
        let producers: BTreeMap<_, _> = producers.into_iter().map(|(producer, classes)| -> Result<_, MultiplierError> {
            let mut events = 0_u64;
            let mut nanos = 0_i64;
            let mut unpriced = 0_i64;
            let classes: BTreeMap<_, _> = classes.into_iter().map(|(name, class)| -> Result<_, MultiplierError> {
                let class_events = class.lines.values().try_fold(0_u64, |sum, line|
                    sum.checked_add(line.events).ok_or(MoneyError::OutOfRange))?;
                let class_nanos = class.lines.values().try_fold(0_i64, |sum, line|
                    sum.checked_add(round_nano_num(line.numerator)?).ok_or(MoneyError::OutOfRange))?;
                events = events.checked_add(class_events).ok_or(MoneyError::OutOfRange)?;
                nanos = nanos.checked_add(class_nanos).ok_or(MoneyError::OutOfRange)?;
                unpriced = class.unpriced.values().try_fold(unpriced, |sum, (tokens, _)|
                    sum.checked_add(*tokens).ok_or(MoneyError::OutOfRange))?;
                Ok((name, json!({"events": class_events, "list_nanos": class_nanos, "estimated_points": points(class_nanos)})))
            }).collect::<Result<_, _>>()?;
            Ok((producer, json!({"events": events, "list_nanos": nanos, "estimated_points": points(nanos), "unpriced_tokens": unpriced, "by_work_class": classes})))
        }).collect::<Result<_, _>>()?;
        Ok(json!({"hour_start_ms": hour, "producers": producers}))
    }).collect::<Result<_, _>>()?;
    Ok(
        json!({"quota_provider": request.quota_provider, "from_ms": from, "to_ms": to,
        "basis": "pro_rata_list_value", "routing_eligible": true, "plan": selected_plan, "per_point_nanos": per_point_nanos,
        "observation_id": head.observation_id, "observation_day": head.observation_day, "undated_events": undated_events, "hours": hours}),
    )
}

const JSON_COLUMNS: [&str; 12] = [
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
    // A head is the row nothing supersedes. `supersedes IS NULL` would select
    // each day's original instead, which is exactly the row a correction
    // replaced.
    const HEADS: &str = "WITH head AS (
        SELECT o.* FROM multiplier_observation o
        WHERE NOT EXISTS (SELECT 1 FROM multiplier_observation child
                           WHERE child.supersedes = o.observation_id))";
    // A pooled legacy head is only a fallback until that provider has any
    // plan-specific observation. The history below still retains every head.
    let latest = observation_rows(
        conn,
        &format!(
            "{HEADS}
             SELECT * FROM head h
             WHERE h.observation_id = (
                 SELECT l.observation_id FROM head l
                  WHERE l.quota_provider = h.quota_provider
                    AND l.plan IS h.plan
                 ORDER BY l.observation_day DESC, l.observation_id DESC LIMIT 1)
               AND (h.plan IS NOT NULL OR NOT EXISTS (
                   SELECT 1 FROM multiplier_observation p
                   WHERE p.quota_provider = h.quota_provider AND p.plan IS NOT NULL))
             ORDER BY h.quota_provider, h.plan"
        ),
        &[],
    )?;
    let series = observation_rows(
        conn,
        &format!(
            "{HEADS}
             SELECT * FROM head
             WHERE interval_end_ms >= ?1 AND interval_end_ms < ?2
             ORDER BY interval_end_ms, quota_provider"
        ),
        &[request.from_ms, request.to_ms],
    )?;
    Ok(json!({"latest": latest, "series": series}))
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
    include!("multiplier/v2_tests.rs");
    use super::*;
    use crate::test_support::conn;

    const DAY: i64 = 86_400_000;
    /// $200.00 a month, in nanos.
    const PLAN_200: i64 = 200_000_000_000;
    /// $10 per million tokens, in nano-USD per million.
    const TEN_PER_M: i64 = 10_000_000_000;

    fn window() -> WindowSelector {
        WindowSelector {
            pool_key: "primary".into(),
            window_minutes: 10080,
        }
    }

    fn rule(producer: &str, provider: &str, charge_basis: Option<&str>) -> ScopeRule {
        ScopeRule {
            producer: producer.into(),
            provider: provider.into(),
            charge_basis: charge_basis.map(str::to_string),
            model_prefix: None,
        }
    }

    fn config(scope: Vec<ScopeRule>) -> MultiplierConfig {
        MultiplierConfig {
            api_twins: BTreeMap::from([("openai-codex".to_string(), "openai".to_string())]),
            plan_price_overrides: BTreeMap::new(),
            quota_providers: BTreeMap::from([(
                "codex".to_string(),
                QuotaProviderConfig {
                    credentials: BTreeMap::new(),
                    window: window(),
                    scope,
                    accounts: BTreeMap::from([(
                        "acct".to_string(),
                        AccountPlan {
                            plan: "openai/pro_20x".into(),
                            price_override: None,
                        },
                    )]),
                },
            )]),
        }
    }

    #[test]
    fn unrecognized_scope_charge_basis_error_lists_the_accepted_values() {
        let error = config(vec![rule("pi", "openai", Some("metered"))])
            .validate()
            .unwrap_err();
        assert!(
            error.ends_with(
                "scope charge_basis 'metered' is not one of \
                 [\"metered_api\", \"subscription_included\", \"unknown\"]"
            ),
            "{error}"
        );
        for accepted in ["metered_api", "subscription_included", "unknown"] {
            config(vec![rule("pi", "openai", Some(accepted))])
                .validate()
                .unwrap();
        }
    }

    /// One reading, stored in arrival order. `at_ms` is its `fetched_at`.
    fn reading(conn: &Connection, account: &str, at_ms: i64, used: f64, raw: Option<f64>) {
        conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant,
                 used_percent, raw_used_percent, resets_at, window_minutes, source)
             VALUES ('codex', ?1, 'primary', ?2, ?3, ?4, '2026-09-30T00:00:00Z', 10080, 'vault')",
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
                            reference_model: None,
                        }),
                    )
                })
                .collect(),
        }
    }

    fn priced_plan() -> BTreeMap<String, PlanPriceUsed> {
        BTreeMap::from([(
            "acct".to_string(),
            PlanPriceUsed {
                account_ref: "acct".into(),
                credential_ref: None,
                plan: "openai/pro_20x".into(),
                source: Some("fusiform".into()),
                monthly_nanos: Some(PLAN_200),
                review_by_ms: Some(i64::MAX),
                plan_price_overdue: false,
                source_ref: None,
                unpriceable_reason: None,
                refusal_text: None,
            },
        )])
    }

    fn observe(
        conn: &Connection,
        config: &MultiplierConfig,
        start: i64,
        end: i64,
        rates: &ResolvedRates,
    ) -> Observation {
        let plan = plan_observation(conn, config, "codex", start, end).unwrap();
        compute_observation(conn, config, &plan, rates, &priced_plan(), end).unwrap()
    }

    fn compute_observation(
        conn: &Connection,
        config: &MultiplierConfig,
        plan: &ObservationPlan,
        rates: &ResolvedRates,
        prices: &BTreeMap<String, PlanPriceUsed>,
        at_ms: i64,
    ) -> Result<Observation, MultiplierError> {
        // Valuation fixtures often append events after constructing quota
        // inputs. Capture those fixtures here; production never refreshes a plan.
        let mut plan = plan.clone();
        plan.events.clear();
        for_scoped_events(
            conn,
            &plan.provider.scope,
            plan.interval_start_ms,
            plan.interval_end_ms,
            |event| {
                plan.events.push(event);
                Ok(())
            },
        )?;
        let mut row = super::compute_observation(
            conn,
            config,
            &plan,
            rates,
            prices,
            at_ms,
            &["openai".into()],
        )?;
        // These fixtures test pace arithmetic, not the calibration gate.
        row.plan = Some("openai/pro_20x".into());
        row.routing_eligible = true;
        row.eligibility_reasons.clear();
        Ok(row)
    }

    fn delta(conn: &Connection, start: i64, end: i64) -> AccountDelta {
        account_delta(conn, "codex", "acct", &window(), start, end).unwrap()
    }

    #[test]
    fn scoped_stream_keeps_half_open_bounds_and_complete_event_groups() {
        let conn = conn();
        let scope = vec![
            rule("codex", "openai-codex", Some("subscription_included")),
            rule("pi", "openai", None),
        ];
        for (id, producer, provider, basis, at) in [
            ("a", "codex", "openai-codex", "subscription_included", 10),
            ("b", "pi", "openai", "metered_api", 19),
            ("before", "pi", "openai", "metered_api", 9),
            ("end", "pi", "openai", "metered_api", 20),
            ("wrong-basis", "codex", "openai-codex", "metered_api", 11),
            ("wrong-provider", "pi", "anthropic", "metered_api", 11),
        ] {
            event(
                &conn,
                id,
                producer,
                provider,
                "m",
                basis,
                at,
                &[
                    ("input", "usage.input", 100),
                    ("output", "usage.output", 50),
                ],
            );
        }
        let mut seen = Vec::new();
        for_scoped_events(&conn, &scope, 10, 20, |mut event| {
            event.tokens.roles.sort_by(|a, b| a.0.cmp(&b.0));
            seen.push((event.id, event.occurred_at_ms, event.tokens.roles));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!((seen[0].0, seen[0].1), (1, 10));
        assert_eq!((seen[1].0, seen[1].1), (2, 19));
        for (_, _, roles) in seen {
            assert_eq!(
                roles,
                vec![
                    (
                        "input".into(),
                        "token".into(),
                        "usage.input".into(),
                        Some(100),
                        Some(0)
                    ),
                    (
                        "output".into(),
                        "token".into(),
                        "usage.output".into(),
                        Some(50),
                        Some(0)
                    )
                ]
            );
        }
    }

    #[test]
    fn model_prefix_excludes_third_party_tokens_and_null_keeps_them() {
        let conn = conn();
        reading(&conn, "acct", 10, 0.0, None);
        reading(&conn, "acct", 90, 50.0, None);
        let gemini = "antigravity-gemini-3.8-flash";
        let claude = "antigravity-claude-sonnet-5-5";
        let uppercase = "ANTIGRAVITY-GEMINI-3.8-FLASH";
        for (id, model, at, tokens) in [
            ("gemini", gemini, 20, 1_000),
            ("claude", claude, 20, 2_000),
            ("uppercase", uppercase, 20, 4_000),
            ("gemini-before", gemini, 5, 100),
            ("claude-before", claude, 5, 200),
        ] {
            event(
                &conn,
                id,
                "opencode",
                "google",
                model,
                "unknown",
                at,
                &[("input", "usage.input", tokens)],
            );
        }
        let rates = rates_for(
            &[
                ("google", gemini),
                ("google", claude),
                ("google", uppercase),
            ],
            TEN_PER_M,
            None,
        );
        let mut config = config(vec![rule("opencode", "google", None)]);
        let all = observe(&conn, &config, 0, 100, &rates);
        assert_eq!(all.tokens.input, 7_000);
        assert_eq!(all.api_nanos, 70_000_000);
        assert_eq!(all.tokens_outside_quota_coverage, 300);
        config.quota_providers.get_mut("codex").unwrap().scope[0].model_prefix =
            Some("antigravity-gemini-".into());
        let plan = plan_observation(&conn, &config, "codex", 0, 100).unwrap();
        assert_eq!(
            catalog_models(&config, &plan),
            BTreeSet::from([("google".into(), gemini.into())])
        );
        let filtered = observe(&conn, &config, 0, 100, &rates);
        assert_eq!(filtered.tokens.input, 1_000);
        assert_eq!(filtered.api_nanos, 10_000_000);
        assert_eq!(filtered.tokens_outside_quota_coverage, 100);
        assert_eq!(filtered.api_nanos_by_model.len(), 1);
        assert_eq!(
            filtered.api_nanos_by_model[0].model.as_deref(),
            Some(gemini)
        );
        config.quota_providers.get_mut("codex").unwrap().scope[0].model_prefix =
            Some("antigravity-gemini-%".into());
        let literal = observe(&conn, &config, 0, 100, &rates);
        assert_eq!(literal.tokens.input, 0);
        assert_eq!(literal.api_nanos, 0);
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

    fn arithmetic_fixture(conn: &Connection) -> (MultiplierConfig, ObservationPlan) {
        let config = config(vec![rule("codex", "openai-codex", None)]);
        reading(conn, "acct", 10, 0.0, None);
        reading(conn, "acct", 90, 50.0, None);
        let plan = plan_observation(conn, &config, "codex", 0, 100).unwrap();
        (config, plan)
    }

    #[test]
    fn money_api_half_even_and_signed_rates_reach_the_observation() {
        for (rate, expected) in [(500_000, 0), (2_500_000, 2), (-1_000_000, -1)] {
            let conn = conn();
            let (config, plan) = arithmetic_fixture(&conn);
            event(
                &conn,
                "e",
                "codex",
                "openai-codex",
                "m",
                "subscription_included",
                50,
                &[("input", "usage.input", 1)],
            );
            let observation = compute_observation(
                &conn,
                &config,
                &plan,
                &rates_for(&[("openai", "m")], rate, None),
                &priced_plan(),
                100,
            )
            .unwrap();
            assert_eq!(observation.api_nanos, expected, "rate {rate}");
            assert_eq!(observation.api_nanos_by_model[0].api_nanos, expected);
        }
    }

    fn record_or_fail(
        conn: &mut Connection,
        config: &MultiplierConfig,
        plan: &ObservationPlan,
        rates: &ResolvedRates,
        prices: &BTreeMap<String, PlanPriceUsed>,
    ) -> Option<String> {
        match compute_observation(conn, config, plan, rates, prices, 100) {
            Ok(observation) => {
                let tx = conn.transaction().unwrap();
                insert_observation_in_tx(&tx, &observation).unwrap();
                tx.commit().unwrap();
                None
            }
            Err(error) => Some(error.code().into()),
        }
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
        let mut conn = conn();
        let (config, plan) = arithmetic_fixture(&conn);
        event(
            &conn,
            "e",
            "codex",
            "openai-codex",
            "m",
            "subscription_included",
            50,
            &[("input", "usage.input", i64::MAX)],
        );
        let reason = record_or_fail(
            &mut conn,
            &config,
            &plan,
            &rates_for(&[("openai", "m")], i64::MAX, None),
            &priced_plan(),
        );
        assert_failed_day(&conn, reason);
    }

    #[test]
    fn money_token_overflow_does_not_record_a_wrapped_day() {
        for across_events in [false, true] {
            let mut conn = conn();
            let (config, plan) = arithmetic_fixture(&conn);
            let mut measurements = vec![("input", "usage.input", i64::MAX)];
            if !across_events {
                measurements.push(("output", "usage.output", 1));
            }
            event(
                &conn,
                "e",
                "codex",
                "openai-codex",
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
                    "openai-codex",
                    "m",
                    "subscription_included",
                    50,
                    &[("input", "usage.input", 1)],
                );
            }
            let reason = record_or_fail(
                &mut conn,
                &config,
                &plan,
                &rates_for(&[("openai", "m")], 0, None),
                &priced_plan(),
            );
            assert_failed_day(&conn, reason);
        }
    }

    #[test]
    fn money_subscription_and_fee_keep_large_integer_nanos_exact() {
        let conn = conn();
        let (config, mut plan) = arithmetic_fixture(&conn);
        plan.provider.window.window_minutes = MINUTES_PER_MONTH;
        plan.accounts[0].delta_percent = 100.0;
        plan.token_interval_ms = Some((0, MS_PER_MONTH));
        let mut prices = priced_plan();
        let monthly = 9_007_199_254_740_993;
        prices.get_mut("acct").unwrap().monthly_nanos = Some(monthly);
        let observation = compute_observation(
            &conn,
            &config,
            &plan,
            &ResolvedRates::default(),
            &prices,
            100,
        )
        .unwrap();
        assert_eq!(observation.subscription_nanos, monthly);
        assert_eq!(observation.fee_paid_nanos, monthly);
        assert_eq!(
            observation.accounts_included[0]["subscription_nanos"],
            monthly
        );
    }

    #[test]
    fn money_subscription_and_fee_round_exact_ties_to_even() {
        let conn = conn();
        let (config, mut plan) = arithmetic_fixture(&conn);
        plan.provider.window.window_minutes = MINUTES_PER_MONTH;
        plan.token_interval_ms = Some((0, MS_PER_MONTH / 2));
        let mut prices = priced_plan();
        prices.get_mut("acct").unwrap().monthly_nanos = Some(1);
        let observation = compute_observation(
            &conn,
            &config,
            &plan,
            &ResolvedRates::default(),
            &prices,
            100,
        )
        .unwrap();
        assert_eq!(observation.subscription_nanos, 0);
        assert_eq!(observation.fee_paid_nanos, 0);
    }

    #[test]
    fn money_account_parts_sum_to_the_row_with_deterministic_remainders() {
        let conn = conn();
        let (config, mut plan) = arithmetic_fixture(&conn);
        plan.provider.window.window_minutes = MINUTES_PER_MONTH;
        let account = plan.accounts[0].clone();
        let price = priced_plan()["acct"].clone();
        for deltas in [
            [50.0, 50.0, 50.0],
            [10.0, 60.0, 90.0],
            [0.0, 0.0, 0.0],
            [12.5, 12.5, 25.0],
        ] {
            let mut prices = BTreeMap::new();
            plan.accounts = ["c", "b", "a"]
                .into_iter()
                .zip(deltas)
                .map(|(id, delta)| {
                    prices.insert(
                        id.into(),
                        PlanPriceUsed {
                            account_ref: id.into(),
                            monthly_nanos: Some(1),
                            ..price.clone()
                        },
                    );
                    AccountDelta {
                        account_ref: id.into(),
                        delta_percent: delta,
                        ..account.clone()
                    }
                })
                .collect();
            let observation = compute_observation(
                &conn,
                &config,
                &plan,
                &ResolvedRates::default(),
                &prices,
                100,
            )
            .unwrap();
            let sum: i64 = observation
                .accounts_included
                .iter()
                .map(|a| a["subscription_nanos"].as_i64().unwrap())
                .sum();
            assert_eq!(sum, observation.subscription_nanos, "deltas {deltas:?}");
            if deltas.iter().all(|delta| (*delta - 50.0).abs() < 1e-9) {
                assert_eq!(observation.subscription_nanos, 2);
                let parts: BTreeMap<_, _> = observation
                    .accounts_included
                    .iter()
                    .map(|a| {
                        (
                            a["account_ref"].as_str().unwrap(),
                            a["subscription_nanos"].as_i64().unwrap(),
                        )
                    })
                    .collect();
                assert_eq!(parts, BTreeMap::from([("a", 1), ("b", 1), ("c", 0)]));
            }
        }
    }

    #[test]
    fn money_outside_coverage_counts_the_same_disjoint_plain_tokens() {
        let conn = conn();
        let (mut config, mut plan) = arithmetic_fixture(&conn);
        config.quota_providers.get_mut("codex").unwrap().scope =
            vec![rule("broca", "openai-codex", None)];
        plan.provider.scope = config.quota_providers["codex"].scope.clone();
        for (id, at) in [("inside", 50), ("outside", 95)] {
            event(
                &conn,
                id,
                "broca",
                "openai-codex",
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
        let observation = compute_observation(
            &conn,
            &config,
            &plan,
            &rates_for(&[("openai", "m")], 1_000_000, None),
            &priced_plan(),
            100,
        )
        .unwrap();
        assert_eq!(
            observation.tokens,
            RoleTokens {
                input: 10,
                output: 4,
                reasoning: 3,
                ..RoleTokens::default()
            }
        );
        assert_eq!(observation.tokens_outside_quota_coverage, 17);
    }

    #[test]
    fn money_whole_nano_rate_recompute_preserves_the_stored_api_value() {
        let mut conn = conn();
        let (config, plan) = arithmetic_fixture(&conn);
        event(
            &conn,
            "e",
            "codex",
            "openai-codex",
            "m",
            "subscription_included",
            50,
            &[
                ("input", "usage.input", 100),
                ("output", "usage.output", 60),
                ("reasoning", "usage.reasoning", 20),
            ],
        );
        let rates = rates_for(&[("openai", "m")], TEN_PER_M, None);
        let original =
            compute_observation(&conn, &config, &plan, &rates, &priced_plan(), 100).unwrap();
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
        assert_eq!(before["latest"][0]["api_nanos"], 1_800_000);
        let recomputed =
            compute_observation(&conn, &config, &plan, &rates, &priced_plan(), 200).unwrap();
        assert_eq!(recomputed.api_nanos, old_row.1);
        let tx = conn.transaction().unwrap();
        insert_superseding_observation_in_tx(&tx, &recomputed).unwrap();
        tx.commit().unwrap();
        let unchanged: (i64, String) = conn.query_row("SELECT api_nanos, accounts_included FROM multiplier_observation WHERE observation_id = ?1", [old_row.0],
            |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
        assert_eq!(unchanged, (old_row.1, old_row.2));
        let after = multiplier_report(&conn, &request).unwrap();
        assert_eq!(
            after["latest"][0]["api_nanos"],
            before["latest"][0]["api_nanos"]
        );
    }

    #[test]
    fn money_aggregate_and_numerator_overflows_leave_no_observation() {
        for numerator_overflow in [false, true] {
            let mut conn = conn();
            let (config, plan) = arithmetic_fixture(&conn);
            event(
                &conn,
                "input",
                "codex",
                "openai-codex",
                "m",
                "subscription_included",
                50,
                &[("input", "usage.input", i64::MAX)],
            );
            event(
                &conn,
                "output",
                "codex",
                "openai-codex",
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
                    "openai-codex",
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
            let reason = record_or_fail(&mut conn, &config, &plan, &rates, &priced_plan());
            assert_failed_day(&conn, reason);
        }
    }

    #[test]
    fn money_denominator_overflow_leaves_no_observation() {
        let mut conn = conn();
        let (config, mut plan) = arithmetic_fixture(&conn);
        plan.provider.window.window_minutes = MINUTES_PER_MONTH;
        plan.accounts[0].delta_percent = 200.0;
        let mut prices = priced_plan();
        prices.get_mut("acct").unwrap().monthly_nanos = Some(i64::MAX);
        let reason = record_or_fail(
            &mut conn,
            &config,
            &plan,
            &ResolvedRates::default(),
            &prices,
        );
        assert_failed_day(&conn, reason);
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
        let config = config(vec![rule("codex", "openai-codex", None)]);
        let mut head = sample_observation(&conn);
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
        event(
            &conn,
            "e",
            "codex",
            "openai-codex",
            "m",
            "subscription_included",
            100,
            &[("input", "usage.input", 1)],
        );
        let report = multiplier_pace(
            &conn,
            Some(&config),
            &MultiplierPaceRequest {
                plan: None,
                quota_provider: "codex".into(),
                from_ms: 0,
                to_ms: HOUR_MS,
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
        let config = config(vec![rule("opencode", "openai", None)]);
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
        let observation = observe(
            &conn,
            &config,
            0,
            7 * DAY,
            &rates_for(&[("openai", "m")], TEN_PER_M, None),
        );
        assert_eq!(observation.token_interval_start_ms, Some(3 * DAY));
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
        let config = config(vec![rule("broca", "openai", Some("subscription_included"))]);
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
        let observation = observe(
            &conn,
            &config,
            0,
            200,
            &rates_for(&[("openai", "m")], TEN_PER_M, None),
        );
        // 2 × 1,000 tokens of real work at $10/M.
        assert_eq!(observation.api_nanos, 20_000_000);
        assert_eq!(observation.tokens.output, 1_200);
        assert_eq!(observation.tokens.reasoning, 800);
        let line = &observation.api_nanos_by_model[0];
        assert_eq!(line.inclusive_output_adjustment, 400);
        assert_eq!(line.measured_tokens.output, 1_600);
    }

    /// Reasoning is valued at the output rate only for providers the config
    /// declares; elsewhere it needs its own rate and is otherwise unpriced.
    #[test]
    fn reasoning_prices_at_output_only_for_declared_providers() {
        let conn = conn();
        let config = config(vec![rule("pi", "openai-codex", None)]);
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "e",
            "pi",
            "openai-codex",
            "m",
            "unknown",
            10,
            &[("reasoning", "usage.reasoning", 500)],
        );
        // Pi's openai-codex prices through its openai twin.
        let rates = rates_for(&[("openai", "m")], TEN_PER_M, None);
        assert_eq!(observe(&conn, &config, 0, 200, &rates).api_nanos, 5_000_000);
        let plan = plan_observation(&conn, &config, "codex", 0, 200).unwrap();
        let observation =
            super::compute_observation(&conn, &config, &plan, &rates, &priced_plan(), 200, &[])
                .unwrap();
        assert_eq!(observation.api_nanos, 0);
        assert_eq!(observation.unpriced_tokens[0].reason, "no_rate");
        assert_eq!(observation.unpriced_tokens[0].tokens, 500);
    }

    /// A model the catalog cannot price is counted by reason and left out of
    /// api_nanos, never valued at zero.
    #[test]
    fn an_unpriced_model_is_counted_and_excluded() {
        let conn = conn();
        let config = config(vec![rule("opencode", "openai", None)]);
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "priced",
            "opencode",
            "openai",
            "known",
            "unknown",
            10,
            &[("input", "usage.input", 1_000)],
        );
        event(
            &conn,
            "unpriced",
            "opencode",
            "openai",
            "unknown-pro",
            "unknown",
            20,
            &[
                ("input", "usage.input", 300),
                ("output", "usage.output", 30),
            ],
        );
        let mut rates = rates_for(&[("openai", "known")], TEN_PER_M, None);
        rates.models.insert(
            ("openai".into(), "unknown-pro".into()),
            ModelResolution::Unpriceable {
                reason: "no_catalog_coverage".into(),
            },
        );
        let observation = observe(&conn, &config, 0, 200, &rates);
        assert_eq!(observation.api_nanos, 10_000_000);
        let unpriced: Vec<_> = observation
            .unpriced_tokens
            .iter()
            .map(|entry| {
                (
                    entry.reason.as_str(),
                    entry.model.as_str(),
                    entry.role.as_str(),
                    entry.tokens,
                )
            })
            .collect();
        assert_eq!(
            unpriced,
            [
                (
                    "no_catalog_coverage",
                    "opencode/openai/unknown-pro",
                    "input",
                    300
                ),
                (
                    "no_catalog_coverage",
                    "opencode/openai/unknown-pro",
                    "output",
                    30
                ),
            ]
        );
        // Its tokens are still in scope; only their value is unknown.
        assert_eq!(observation.tokens.input, 1_300);
    }

    /// Claude Code stores lifetime-split cache writes under the units
    /// `token_cache_5m` and `token_cache_1h`; the catalog's one cache-write
    /// rate cannot price them, so they are counted by reason, never dropped.
    #[test]
    fn a_cache_write_split_by_lifetime_is_counted_not_dropped() {
        let conn = conn();
        let config = config(vec![rule("claude-code", "anthropic", None)]);
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
        let observation = observe(
            &conn,
            &config,
            0,
            200,
            &rates_for(&[("anthropic", "m")], TEN_PER_M, None),
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
        assert_eq!(
            unpriced,
            [("unit_not_priced", "cache_write/token_cache_1h", 4000)]
        );
    }

    /// A per-request producer's event over the threshold prices every role at
    /// the tier rate; a bundling producer's event is priced at base and its
    /// tokens are flagged, whatever its aggregate prompt.
    #[test]
    fn tiers_are_decided_per_event_only_where_an_event_is_one_call() {
        let conn = conn();
        let config = config(vec![
            rule("opencode", "openai", None),
            rule("broca", "openai", None),
        ]);
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
        let observation = observe(&conn, &config, 0, 200, &rates);
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
        let config = config(vec![rule("claude-code", "anthropic", None)]);
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
        let observation = observe(&conn, &config, 0, 200, &rates);
        // 400 input at the tier rate; the unknown-prompt event adds nothing.
        assert_eq!(observation.api_nanos, 8_000_000);
        assert_eq!(observation.api_nanos_by_model[0].tiered_tokens, 400);
        let unpriced: Vec<_> = observation
            .unpriced_tokens
            .iter()
            .map(|entry| (entry.reason.as_str(), entry.role.as_str(), entry.tokens))
            .collect();
        assert_eq!(
            unpriced,
            [
                ("missing_quantity", "cache_read", 0),
                ("rate_tiered", "input", 50),
                ("unit_not_priced", "cache_write/token_cache_1h", 700),
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
            overridden_facts: BTreeMap::new(),
        };
        ResolvedRates {
            resolved_at_ms: 42,
            models: BTreeMap::from([(
                ("openai".to_string(), model.to_string()),
                rates_from_snapshot(&snapshot),
            )]),
        }
    }

    fn value_one(rates: &ResolvedRates, mode: Option<&str>) -> (ModelLine, Vec<String>) {
        let config = config(vec![]);
        let event = EventTokens {
            producer: "claude-code".into(),
            provider: "openai".into(),
            model: Some("m".into()),
            mode: mode.map(str::to_string),
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
            &ValuationRules::from_config(&config, &["openai".into()]),
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
        let config = config(vec![]);
        let rates = fast_mode_rates("m");
        let event = EventTokens {
            producer: "claude-code".into(),
            provider: "openai".into(),
            model: Some("m".into()),
            mode: None,
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
        let reasoning = ["openai".into()];
        let rules = ValuationRules::from_config(&config, &reasoning);
        let start = std::time::Instant::now();
        let allocations = crate::canon::allocation_probe::count(|| {
            for _ in 0..100_000 {
                value_event(
                    &rules,
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
        let config = config(vec![rule("opencode", "openai", None)]);
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
        let observation = observe(&conn, &config, 0, 200, &fast_mode_rates("m"));
        assert_eq!(observation.api_nanos, 10_000_000);
        assert_eq!(
            observation.api_nanos_by_model[0].mode_unrecorded_tokens,
            1_000
        );
        let plain = observe(
            &conn,
            &config,
            0,
            200,
            &rates_for(&[("openai", "m")], TEN_PER_M, None),
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
            overridden_facts: BTreeMap::new(),
        };
        let resolution = rates_from_snapshot(&snapshot);
        assert!(
            matches!(resolution, ModelResolution::Rates(_)),
            "{resolution:?}"
        );
        let rates = ResolvedRates {
            resolved_at_ms: 42,
            models: BTreeMap::from([(("openai".to_string(), "m".to_string()), resolution)]),
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
            overridden_facts: BTreeMap::new(),
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
                overridden_facts: BTreeMap::new(),
            };
            assert_eq!(
                rates_from_snapshot(&snapshot),
                ModelResolution::Unpriceable {
                    reason: RATE_UNDECODABLE.into(),
                }
            );
        }
    }

    /// Both ratios from their stated inputs: 10 points of a 7-day window on a
    /// $200 plan is $200 × 7/30 × 10% = $4.6667, and the fee over a 7-day
    /// token interval is $200 × 7/30.
    #[test]
    fn subscription_and_fee_denominators_follow_the_formula() {
        let conn = conn();
        let config = config(vec![rule("opencode", "openai", None)]);
        reading(&conn, "acct", 0, 50.0, None);
        reading(&conn, "acct", 10, 60.0, None);
        reading(&conn, "acct", 7 * DAY, 60.0, None);
        event(
            &conn,
            "e",
            "opencode",
            "openai",
            "m",
            "unknown",
            10,
            &[("input", "usage.input", 1_000_000_000)],
        );
        let observation = observe(
            &conn,
            &config,
            0,
            7 * DAY,
            &rates_for(&[("openai", "m")], TEN_PER_M, None),
        );
        assert_eq!(observation.delta_percent, 10.0);
        assert_eq!(observation.subscription_nanos, 4_666_666_667);
        assert_eq!(observation.fee_paid_nanos, 46_666_666_667);
        // A billion tokens at $10/M is $10,000.
        assert_eq!(observation.api_nanos, 10_000_000_000_000);
        let multiplier = observation.multiplier.unwrap();
        assert!((multiplier - 10_000_000_000_000.0 / 4_666_666_667.0).abs() < 1e-9);
        // One window segment's ±1 point is 10% of a 10-point Δ: too coarse.
        assert!(observation.low_resolution);
        assert_eq!(observation.observation_day, "1970-01-08");
    }

    fn served_price(
        tier: &str,
        price: Option<i64>,
        refusal: Option<&str>,
    ) -> fusiform_protocol::PlanPriceWire {
        fusiform_protocol::PlanPriceWire {
            provider_id: "openai".into(),
            tier: tier.into(),
            price: price.map(|minor_units| fusiform_protocol::PlanAmount {
                minor_units,
                exponent: 2,
                currency: "USD".into(),
                period: "month".into(),
            }),
            quota_value: None,
            refusal_reason: refusal.map(str::to_string),
            boundary_at_ms: 0,
            established_by: "fusiform".into(),
            established_at_ms: 0,
            review_by_ms: 1_000,
            source_ref: "https://example.invalid".into(),
        }
    }

    struct Served(PlanPricesResponse);

    impl PlanPriceSource for Served {
        fn plan_prices(
            &self,
            _: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            Ok(self.0.clone())
        }
    }

    /// The account override beats the plan override, which beats fusiform; a
    /// key fusiform refuses leaves the account unpriced with the reason, and a
    /// price past its review date is used but flagged.
    #[test]
    fn plan_prices_resolve_by_precedence_and_refusals_are_reasons() {
        let mut config = config(vec![rule("opencode", "openai", None)]);
        let provider = config.quota_providers.get_mut("codex").unwrap();
        let account = |plan: &str, price_override: Option<PriceOverride>| AccountPlan {
            plan: plan.into(),
            price_override,
        };
        let usd = |minor_units| PriceOverride {
            minor_units,
            exponent: 2,
            currency: "USD".into(),
            period: "month".into(),
        };
        provider.accounts = BTreeMap::from([
            (
                "by-account".to_string(),
                account("openai/pro_5x", Some(usd(12_300))),
            ),
            ("by-plan".to_string(), account("openai/pro_5x", None)),
            ("by-fusiform".to_string(), account("openai/pro_20x", None)),
            ("refused".to_string(), account("openai/pro", None)),
            ("missing".to_string(), account("openai/team", None)),
        ]);
        config.plan_price_overrides = BTreeMap::from([("openai/pro_5x".to_string(), usd(9_900))]);
        let served = Served(PlanPricesResponse {
            prices: vec![
                served_price("pro_20x", Some(20_000), None),
                served_price("pro", None, Some("ambiguous: pro_5x or pro_20x")),
            ],
            resolved_at_ms: 5_000,
            tier_vocabulary: "vendor".into(),
            unit_policy: "list".into(),
        });
        let provider = config.quota_providers["codex"].clone();
        let plans = resolve_plans(Some(&served), &config, &provider, 5_000).unwrap();
        let outcome = |account: &str| {
            let used = &plans[account];
            (
                used.source.clone(),
                used.monthly_nanos,
                used.unpriceable_reason.clone(),
            )
        };
        assert_eq!(
            outcome("by-account"),
            (Some("account_override".into()), Some(123_000_000_000), None)
        );
        assert_eq!(
            outcome("by-plan"),
            (Some("plan_override".into()), Some(99_000_000_000), None)
        );
        assert_eq!(
            outcome("by-fusiform"),
            (Some("fusiform".into()), Some(PLAN_200), None)
        );
        assert!(
            plans["by-fusiform"].plan_price_overdue,
            "review_by 1,000 < now 5,000"
        );
        assert_eq!(
            outcome("refused"),
            (None, None, Some("plan_price_refused".into()))
        );
        assert_eq!(
            outcome("missing"),
            (None, None, Some("plan_price_missing".into()))
        );
        // Needed and unreachable: the day cannot be priced, so it fails.
        assert!(matches!(
            resolve_plans(None, &config, &provider, 5_000),
            Err(MultiplierError::PlanPrices(_))
        ));
    }

    /// An account whose plan has no price is left out of the denominator,
    /// with its reason and Δused% on the row, never valued at a guess.
    #[test]
    fn an_unpriced_account_is_excluded_by_reason() {
        let conn = conn();
        let config = config(vec![rule("opencode", "openai", None)]);
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        let plan = plan_observation(&conn, &config, "codex", 0, 200).unwrap();
        let mut plans = priced_plan();
        let used = plans.get_mut("acct").unwrap();
        used.monthly_nanos = None;
        used.source = None;
        used.unpriceable_reason = Some("plan_price_refused".into());
        let observation = compute_observation(
            &conn,
            &config,
            &plan,
            &ResolvedRates::default(),
            &plans,
            200,
        )
        .unwrap();
        assert_eq!(observation.subscription_nanos, 0);
        assert_eq!(observation.multiplier, None);
        assert_eq!(
            observation.accounts_excluded[0]["reason"],
            "plan_price_refused"
        );
        assert_eq!(observation.accounts_excluded[0]["delta_percent"], 10.0);
    }

    fn sample_observation(conn: &Connection) -> Observation {
        let config = config(vec![rule("opencode", "openai", None)]);
        reading(conn, "acct", 0, 0.0, None);
        reading(conn, "acct", 100, 10.0, None);
        observe(conn, &config, 0, DAY, &ResolvedRates::default())
    }

    fn pace_fixture(conn: &mut Connection) -> (MultiplierConfig, Observation) {
        let config = config(vec![
            rule("opencode", "openai", None),
            rule("broca", "openai", Some("subscription_included")),
        ]);
        let mut head = sample_observation(conn);
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
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        (config, head)
    }

    #[test]
    fn pace_prices_stored_rates_per_hour_producer_and_work_class() {
        let mut conn = conn();
        let (config, _) = pace_fixture(&mut conn);
        event(
            &conn,
            "long",
            "opencode",
            "openai",
            "large",
            "unknown",
            100,
            &[
                ("input", "usage", 200_000),
                ("output", "usage", 20),
                ("reasoning", "usage", 10),
            ],
        );
        // Pace receives only the live scope, not the live pricing declarations.
        conn.execute("INSERT INTO usage_event (event_id, producer, producer_lineage, producer_ref,
            occurred_at_ms, recorded_at_ms, provider, model, charge_basis, work_class)
            VALUES ('worker', 'broca', 'lineage', 'worker', ?1, ?1, 'openai', 'small', 'subscription_included', 'mason')", [2 * HOUR_MS + 10]).unwrap();
        for (role, unit, source, quantity) in [
            ("input", "token", "usage", 1000),
            ("output", "token", "usage.output_tokens", 100),
            ("reasoning", "token", "usage.reasoning_tokens", 40),
            ("cache_write", "token_cache_5m", "usage", 7),
        ] {
            conn.execute("INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units, quantity_exponent)
                VALUES ((SELECT event_key FROM usage_event WHERE event_id='worker'), ?1, ?2, 'billed', ?3, ?4, 0)", params![role, unit, source, quantity]).unwrap();
        }
        event(
            &conn,
            "unknown-model",
            "opencode",
            "openai",
            "absent",
            "unknown",
            2 * HOUR_MS + 20,
            &[("input", "usage", 17)],
        );
        event(
            &conn,
            "short",
            "opencode",
            "openai",
            "large",
            "unknown",
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
            Some(&config),
            &MultiplierPaceRequest {
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
        let expected = json!({"quota_provider": "codex", "from_ms": 0, "to_ms": 14400000,
        "basis": "pro_rata_list_value", "routing_eligible": true, "plan": "openai/pro_20x", "per_point_nanos": 10000000,
        "observation_day": "1970-01-02", "observation_id": 1, "undated_events": 1,
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
        // Consumers pin the replies in tests/vectors: synthetic values laid out
        // in the served shape. If this reply's shape drifts from theirs, those
        // pinned bytes no longer describe what is served, so the vectors must
        // be regenerated in the new shape with it.
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
                json_shape(&report),
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
        let (config, mut head) = pace_fixture(&mut conn);
        head.api_nanos = 200_000_000;
        let tx = conn.transaction().unwrap();
        let recompute = insert_superseding_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        let request = MultiplierPaceRequest {
            plan: None,
            quota_provider: "codex".into(),
            from_ms: 0,
            to_ms: HOUR_MS,
        };
        let report = multiplier_pace(&conn, Some(&config), &request).unwrap();
        assert_eq!(report["per_point_nanos"], 20_000_000);
        assert_eq!(report["observation_id"], recompute.observation_id);
        // A later snapshot correcting an older day must not become latest.
        head.observation_day = "1970-01-01".into();
        head.interval_end_ms = 0;
        head.interval_start_ms = -7 * DAY;
        // The token interval moves with the observation interval: it is that
        // interval clipped to quota coverage, and the insert refuses a token
        // interval lying outside the observation interval.
        head.token_interval_start_ms = Some(-DAY);
        head.token_interval_end_ms = Some(0);
        head.snapshot_at_ms = 10 * DAY;
        head.api_nanos = 500_000_000;
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            multiplier_pace(&conn, Some(&config), &request).unwrap(),
            report
        );
    }

    #[test]
    fn pace_refuses_unknown_provider_invalid_or_excessive_range_and_missing_head() {
        let conn = conn();
        let config = config(vec![rule("opencode", "openai", None)]);
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
                Some(&config),
                &MultiplierPaceRequest {
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
        let (config, mut head) = pace_fixture(&mut conn);
        head.delta_percent = 0.0;
        let tx = conn.transaction().unwrap();
        insert_superseding_observation_in_tx(&tx, &head).unwrap();
        tx.commit().unwrap();
        event(
            &conn,
            "negative",
            "opencode",
            "openai",
            "large",
            "unknown",
            -1,
            &[("input", "usage", 10)],
        );
        let report = multiplier_pace(
            &conn,
            Some(&config),
            &MultiplierPaceRequest {
                plan: None,
                quota_provider: "codex".into(),
                from_ms: -1,
                to_ms: 1,
            },
        )
        .unwrap();
        assert_eq!(report["from_ms"], -HOUR_MS);
        assert_eq!(report["to_ms"], HOUR_MS);
        assert_eq!(report["hours"].as_array().unwrap().len(), 2);
        assert_eq!(report["per_point_nanos"], Value::Null);
        assert_eq!(
            report["hours"][0]["producers"]["opencode"]["list_nanos"],
            100_000
        );
        assert_eq!(
            report["hours"][0]["producers"]["opencode"]["estimated_points"],
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
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &observation).unwrap();
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
        assert_eq!(report["series"].as_array().unwrap().len(), 1);
        assert_eq!(
            report["series"][0]["scope_rules"][0]["producer"],
            "opencode"
        );
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
                    source: "models.dev".into(),
                    resolved_at_ms: request.at_ms.unwrap(),
                    catalog_version: 1,
                    models: BTreeMap::from([("google/gemini-3.8-flash".into(), self.0.clone())]),
                    withheld: vec![],
                    uncertain: vec![],
                    overridden: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
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
            let config = config(vec![rule("opencode", "google", None)]);
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
            let plan = plan_observation(&conn, &config, "codex", 0, 100).unwrap();
            let rates = resolve_rates(
                &token_catalog(extra_role),
                &catalog_models(&config, &plan),
                100,
            )
            .unwrap();
            let observation = super::compute_observation(
                &conn,
                &config,
                &plan,
                &rates,
                &priced_plan(),
                100,
                &["google".into()],
            )
            .unwrap();
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
    fn unknown_input_video_catalog_role_fails_the_day_without_rows() {
        let conn = conn();
        let config = config(vec![rule("opencode", "google", None)]);
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
        let plan = plan_observation(&conn, &config, "codex", 0, 100).unwrap();
        let error = resolve_rates(
            &token_catalog(Some("input_video")),
            &catalog_models(&config, &plan),
            100,
        )
        .unwrap_err();
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
                    overridden: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
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
                source: "models.dev".into(),
                resolved_at_ms: 100,
                catalog_version: 1,
                models,
                withheld: vec![],
                uncertain: vec![],
                overridden: vec![],
                retired: vec![],
                aliased: self.aliased.clone(),
                plane: None,
                billing_rules: vec![],
            })
        }
    }

    fn reference_rate(units: i64) -> Value {
        json!({"state":"priced","units":units,"exponent":9,"currency":"USD",
            "unit_provenance":{"kind":"stated"}})
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
        let config = config(vec![rule("opencode", "google", None)]);
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
        let plan = plan_observation(&conn, &config, "codex", 0, 100).unwrap();
        let rates = resolve_rates(&catalog, &catalog_models(&config, &plan), 100).unwrap();
        let observation = observe(&conn, &config, 0, 100, &rates);
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
        let zero = observe(&conn, &config, 0, 100, &known_zero);
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
                let config = config(vec![rule("opencode", "google", None)]);
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
                let plan = plan_observation(&conn, &config, "codex", 0, 100).unwrap();
                let rates = resolve_rates(&catalog, &catalog_models(&config, &plan), 100).unwrap();
                let observation = observe(&conn, &config, 0, 100, &rates);
                assert_eq!(observation.tokens.input, 2_000);
                assert_eq!(observation.api_nanos, 60_000_000);
                assert!(observation.unpriced_tokens.is_empty());
                let mut expected_requests = vec![
                    (model.into(), Some(100)),
                    (model.into(), None),
                    ("gemini-3.8-flash".into(), Some(100)),
                ];
                if !suffix.is_empty() {
                    expected_requests.insert(0, (event_model, Some(100)));
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
                    report["latest"][0]["api_nanos_by_model"][0]["rates"]["reference_model"],
                    stored_rates["reference_model"]
                );
                let pace = multiplier_pace(
                    &conn,
                    Some(&config),
                    &MultiplierPaceRequest {
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
                    crate::subscription::resolve_reprice(&ledger_plan, &catalog, &[]).unwrap();
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
        let config = config(vec![rule("broca", "google", None)]);
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
        let plan = plan_observation(&conn, &config, "codex", 0, 100).unwrap();
        let rates = resolve_rates(&catalog, &catalog_models(&config, &plan), 100).unwrap();
        let observation = observe(&conn, &config, 0, 100, &rates);
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
        let original = sample_observation(&conn);
        let config = config(vec![rule("opencode", "openai", None)]);
        let next_day = observe(&conn, &config, DAY, 2 * DAY, &ResolvedRates::default());
        assert_eq!(next_day.observation_day, "1970-01-03");
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &original).unwrap();
        insert_observation_in_tx(&tx, &next_day).unwrap();
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

    /// Oracle harness for the observation path over two Codex weeks,
    /// 2026-09-10..17 and 2026-09-17..24 UTC. It asserts nothing: it opens the
    /// read-only store copy named by `MULTIPLIER_ORACLE_DB`, computes each
    /// week's observation through the production path, and prints the quota
    /// points, subscription, API list value and multiplier. To use it, run it
    /// against a copy of a real store with that store's Codex account refs
    /// filled in locally in the `accounts` map below (the committed refs are
    /// placeholders that match nothing), then compare the printed output with
    /// a hand computation done on the same copy. The rates are fixed at
    /// fusiform catalog version 1790261588698 and served by a stand-in catalog,
    /// so the result does not move when the live catalog does. Ignored by
    /// default: it needs a store copy.
    #[test]
    #[ignore]
    fn oracle_codex_weeks_on_a_store_copy() {
        use crate::catalog_client::CatalogConnection;
        use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
        use std::sync::Arc;

        // Thousandths of a dollar per million tokens, to nano-USD per million.
        const B: i64 = 1_000_000;
        // model -> (input, cache_read, cache_write, output), base then above
        // 272k, in thousandths of a dollar per million tokens.
        #[allow(clippy::type_complexity)]
        const RATES: [(&str, [Option<(i64, i64)>; 4]); 7] = [
            (
                "gpt-5.4",
                [
                    Some((2_500, 5_000)),
                    Some((250, 500)),
                    None,
                    Some((15_000, 22_500)),
                ],
            ),
            (
                "gpt-5.4-mini",
                [Some((750, 0)), Some((75, 0)), None, Some((4_500, 0))],
            ),
            (
                "gpt-5.6-luna",
                [
                    Some((200, 400)),
                    Some((20, 40)),
                    Some((250, 500)),
                    Some((1_200, 1_800)),
                ],
            ),
            (
                "gpt-5.6-sol",
                [
                    Some((4_000, 8_000)),
                    Some((400, 800)),
                    Some((5_000, 10_000)),
                    Some((20_000, 30_000)),
                ],
            ),
            (
                "gpt-6-astra",
                [
                    Some((10_000, 20_000)),
                    Some((1_000, 2_000)),
                    Some((12_500, 25_000)),
                    Some((50_000, 75_000)),
                ],
            ),
            (
                "gpt-6-luna",
                [
                    Some((100, 200)),
                    Some((10, 20)),
                    Some((125, 250)),
                    Some((500, 750)),
                ],
            ),
            (
                "gpt-6-sol",
                [
                    Some((2_000, 4_000)),
                    Some((200, 400)),
                    Some((2_500, 5_000)),
                    Some((10_000, 15_000)),
                ],
            ),
        ];
        struct AppendixA;
        impl CatalogConnection for AppendixA {
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
                let refuse = |message: &str| CatalogCallError {
                    code: "no_coverage".into(),
                    message: message.into(),
                };
                if request.at_ms == Some(0) {
                    return Err(refuse("fusiform's history begins at 1"));
                }
                let provider = request.provider_id.unwrap();
                let model = request.model_id.unwrap();
                let Some((_, roles)) = RATES.iter().find(|(id, _)| *id == model) else {
                    return Err(refuse("unknown model"));
                };
                let rate = |units: i64| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
                let mut facts = BTreeMap::new();
                for (role, value) in ["input", "cache_read", "cache_write", "output"]
                    .iter()
                    .zip(roles)
                {
                    if let Some((base, tier)) = value {
                        facts.insert(format!("rate.{role}"), rate(base * B));
                        if *tier > 0 {
                            facts.insert(
                                format!("rate.{role}.above_context.272000"),
                                rate(tier * B),
                            );
                        }
                    }
                }
                Ok(CatalogGetResponse {
                    source: "models.dev".into(),
                    resolved_at_ms: request.at_ms.unwrap(),
                    catalog_version: 1_790_261_588_698,
                    models: BTreeMap::from([(format!("{provider}/{model}"), facts)]),
                    withheld: vec![],
                    uncertain: vec![],
                    overridden: vec![],
                    retired: vec![],
                    aliased: vec![],
                    plane: None,
                    billing_rules: vec![],
                })
            }
        }
        let Ok(path) = std::env::var("MULTIPLIER_ORACLE_DB") else {
            panic!("set MULTIPLIER_ORACLE_DB to a read-only copy of the live store");
        };
        let conn =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let config: MultiplierConfig = serde_json::from_value(json!({
            "api_twins": {"openai-codex": "openai"},
            "plan_price_overrides": {},
            "quota_providers": {"codex": {
                "window": {"pool_key": "primary", "window_minutes": 10080},
                "scope": [
                    {"producer": "pi", "provider": "openai-codex", "charge_basis": null, "model_prefix": null},
                    {"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null},
                    {"producer": "broca", "provider": "openai", "charge_basis": "subscription_included", "model_prefix": null}
                ],
                "credentials": {},
                "accounts": {
                    "00000000-0000-4000-8000-00000000000a": {"plan": "openai/pro_20x", "price_override": null},
                    "00000000-0000-4000-8000-00000000000b": {"plan": "openai/pro_20x", "price_override": null}
                }
            }}
        }))
        .unwrap();
        config.validate().unwrap();
        let catalog = CatalogClient::connect(Arc::new(AppendixA)).unwrap();
        let served = Served(PlanPricesResponse {
            prices: vec![fusiform_protocol::PlanPriceWire {
                review_by_ms: 1_797_552_000_000,
                ..served_price("pro_20x", Some(20_000), None)
            }],
            resolved_at_ms: 0,
            tier_vocabulary: "vendor".into(),
            unit_policy: "list".into(),
        });
        let snapshot_at_ms = 1_790_262_183_000; // 2026-09-24T15:03:03Z
        for (name, start, end) in [
            ("W0", 1_788_998_400_000, 1_789_603_200_000),
            ("W1", 1_789_603_200_000, 1_790_208_000_000),
        ] {
            let plan = plan_observation(&conn, &config, "codex", start, end).unwrap();
            let rates =
                resolve_rates(&catalog, &catalog_models(&config, &plan), snapshot_at_ms).unwrap();
            let plans =
                resolve_plans(Some(&served), &config, &plan.provider, snapshot_at_ms).unwrap();
            let observation =
                compute_observation(&conn, &config, &plan, &rates, &plans, snapshot_at_ms).unwrap();
            println!(
                "===== {name} token interval {:?}..{:?}",
                observation.token_interval_start_ms, observation.token_interval_end_ms
            );
            for account in &observation.accounts_included {
                println!(
                    "  account {} delta {} boundaries {} backwards {}",
                    account["account_ref"],
                    account["delta_percent"],
                    account["decrease_boundaries"],
                    account["fetched_at_backwards"]
                );
            }
            for line in &observation.api_nanos_by_model {
                let t = &line.measured_tokens;
                println!(
                    "  {}/{}/{} in={} cr={} cw={} out={} rs={} tiered={} untested={} usd={:.2}",
                    line.producer,
                    line.provider,
                    line.model.as_deref().unwrap_or("-"),
                    t.input,
                    t.cache_read,
                    t.cache_write,
                    t.output,
                    t.reasoning,
                    line.tiered_tokens,
                    line.base_rate_tier_untested_tokens,
                    line.api_nanos as f64 / 1e9
                );
            }
            for entry in &observation.unpriced_tokens {
                println!(
                    "  unpriced {} {} {} {}",
                    entry.reason, entry.model, entry.role, entry.tokens
                );
            }
            println!(
                "  delta% {} subscription ${:.2} fee ${:.2} api ${:.2} multiplier {:?} fee_multiplier {:?} outside_coverage {} low_res {}",
                observation.delta_percent,
                observation.subscription_nanos as f64 / 1e9,
                observation.fee_paid_nanos as f64 / 1e9,
                observation.api_nanos as f64 / 1e9,
                observation.multiplier,
                observation.fee_multiplier,
                observation.tokens_outside_quota_coverage,
                observation.low_resolution,
            );
        }
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
            overridden_facts: BTreeMap::new(),
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
            overridden_facts: BTreeMap::new(),
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
