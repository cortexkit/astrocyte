//! Effective-dated subscription declarations and subscription coverage resolution.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::catalog_client::CatalogClient;
use crate::fact::ChargeBasis;
use crate::pricing::{self, PricingError, RepriceOutcome, RepricePlan, ResolvedReprice};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionPeriod {
    Month,
    Year,
}

impl SubscriptionPeriod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Month => "month",
            Self::Year => "year",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "month" => Some(Self::Month),
            "year" => Some(Self::Year),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetSubscriptionRequest {
    pub account_ref: String,
    pub provider: String,
    pub plan_name: String,
    pub recurring_cost_minor: i64,
    pub currency: String,
    pub amount_exponent: u32,
    pub period: SubscriptionPeriod,
    pub effective_from: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseSubscriptionRequest {
    pub account_ref: String,
    pub provider: String,
    pub effective_to: i64,
}

/// Stated by the close operation itself, in its response, because an operator
/// who closes a plan expecting the ledger to correct itself will read silence
/// as repair. It is a field rather than a line of documentation for the same
/// reason the read coverage markers are: a caller that cannot see the
/// limitation in the payload will present a bounded plan as a repaired ledger.
pub const CLOSE_COVERAGE_NOTICE: &str = "COVERAGE IS ONE-WAY. Closing a plan bounds it at \
     effective_to so it covers no later event, but it does not uncover any event it already \
     covered: a covered charge is a priced charge, and repricing re-resolves only unpriced \
     ones, so every event this plan covered keeps its zero charge. That includes events \
     covered at instants after effective_to, which were priced while the plan was still \
     open and are left as they are. Correcting such an event is a separate operator act.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClosedSubscription {
    pub subscription: AccountSubscription,
    /// Always true, and present so a caller reads the limitation from the
    /// payload rather than having to know it.
    pub coverage_is_one_way: bool,
    pub coverage_notice: &'static str,
}

/// Stated by the set operation's response when the plan starts before the
/// instant it was set, for the reason [`CLOSE_COVERAGE_NOTICE`] is: an
/// operator who backdates a plan expecting the ledger to follow it will read
/// silence as "every event in that range now belongs to this plan".
pub const SET_COVERAGE_NOTICE: &str = "COVERAGE IS ONE-WAY. This plan starts before the \
     instant it was set. Events in that past range that no plan covered are repriced to \
     covered as the coverage backfill reaches them, but an event already covered keeps its \
     charge: repricing re-resolves only unpriced events. So if this plan succeeds an open \
     one, the earlier plan is bounded at effective_from, and every event it covered at or \
     after that instant stays covered by it. Correcting such an event is a separate operator \
     act.";

/// The set operation's response: the declared plan, its fields at the top
/// level as before, plus whether it was backdated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SetSubscription {
    #[serde(flatten)]
    pub subscription: AccountSubscription,
    /// `effective_from` is before the instant the plan was set.
    pub backdated: bool,
    /// The one-way coverage notice text, [`SET_COVERAGE_NOTICE`], when
    /// `backdated`; otherwise null.
    pub coverage_notice: Option<&'static str>,
}

impl SetSubscription {
    /// The response for `subscription`, set at `set_at_ms`.
    pub fn new(subscription: AccountSubscription, set_at_ms: i64) -> Self {
        let backdated = subscription.effective_from < set_at_ms;
        Self {
            subscription,
            backdated,
            coverage_notice: backdated.then_some(SET_COVERAGE_NOTICE),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountSubscription {
    pub account_ref: String,
    pub provider: String,
    pub plan_name: String,
    pub recurring_cost_minor: i64,
    pub currency: String,
    pub amount_exponent: u32,
    pub period: SubscriptionPeriod,
    pub effective_from: i64,
    pub effective_to: Option<i64>,
    pub operator: Option<String>,
}

impl AccountSubscription {
    pub fn recurring_cost_nanos(&self) -> Result<i64, SubscriptionError> {
        recurring_cost_nanos(self.recurring_cost_minor, self.amount_exponent)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionRefusal {
    Malformed,
    OverlappingEffectiveRange,
    RecurringCostOutOfRange,
    CoverageSuperseded,
    NoOpenSubscription,
    CloseBeforeEffectiveFrom,
}

impl SubscriptionRefusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::OverlappingEffectiveRange => "overlapping_effective_range",
            Self::RecurringCostOutOfRange => "recurring_cost_out_of_range",
            Self::CoverageSuperseded => "subscription_coverage_superseded",
            Self::NoOpenSubscription => "no_open_subscription",
            Self::CloseBeforeEffectiveFrom => "close_before_effective_from",
        }
    }
}

#[derive(Debug)]
pub enum SubscriptionError {
    Store(rusqlite::Error),
    Pricing(PricingError),
    Refused(SubscriptionRefusal),
}

impl SubscriptionError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Store(_) => "internal",
            Self::Pricing(PricingError::Refused(refusal)) => refusal.as_str(),
            Self::Pricing(_) => "internal",
            Self::Refused(refusal) => refusal.as_str(),
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Store(error) => format!("subscription store operation: {error}"),
            Self::Pricing(PricingError::Refused(refusal)) => {
                format!(
                    "subscription pricing operation refused: {}",
                    refusal.as_str()
                )
            }
            // Debug keeps the rusqlite variant and the SQLite primary and
            // extended result codes, which Display drops; the codes are what
            // tell a constraint violation apart from a busy or corrupt store.
            Self::Pricing(PricingError::Store(error)) => {
                format!("subscription pricing store operation: {error:?}")
            }
            // These inner errors have no Display, so render them with Debug: the cause (a
            // catalog refusal, an unreadable rate) is what an operator needs to act on.
            Self::Pricing(PricingError::Canon(error)) => {
                format!("subscription pricing canonicalization failed: {error:?}")
            }
            Self::Pricing(PricingError::Catalog(error)) => {
                format!("subscription pricing catalog operation failed: {error:?}")
            }
            Self::Refused(refusal) => {
                format!("subscription write refused: {}", refusal.as_str())
            }
        }
    }
}

impl From<rusqlite::Error> for SubscriptionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

impl From<PricingError> for SubscriptionError {
    fn from(error: PricingError) -> Self {
        Self::Pricing(error)
    }
}

pub fn recurring_cost_nanos(
    recurring_cost_minor: i64,
    amount_exponent: u32,
) -> Result<i64, SubscriptionError> {
    let power = 9_u32
        .checked_sub(amount_exponent)
        .ok_or(SubscriptionError::Refused(
            SubscriptionRefusal::RecurringCostOutOfRange,
        ))?;
    let scale = 10_i128
        .checked_pow(power)
        .ok_or(SubscriptionError::Refused(
            SubscriptionRefusal::RecurringCostOutOfRange,
        ))?;
    let amount =
        i128::from(recurring_cost_minor)
            .checked_mul(scale)
            .ok_or(SubscriptionError::Refused(
                SubscriptionRefusal::RecurringCostOutOfRange,
            ))?;
    i64::try_from(amount)
        .map_err(|_| SubscriptionError::Refused(SubscriptionRefusal::RecurringCostOutOfRange))
}

pub fn set_subscription_in_tx(
    tx: &Transaction<'_>,
    request: &SetSubscriptionRequest,
    operator: Option<&str>,
) -> Result<AccountSubscription, SubscriptionError> {
    validate_request(request, operator)?;
    recurring_cost_nanos(request.recurring_cost_minor, request.amount_exponent)?;

    let open_from = tx
        .query_row(
            "SELECT effective_from_ms FROM account_subscription
             WHERE provider=?1 AND account_ref=?2 AND effective_to_ms IS NULL",
            params![request.provider, request.account_ref],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    let closed_overlap = tx
        .query_row(
            "SELECT 1 FROM account_subscription
             WHERE provider=?1 AND account_ref=?2 AND effective_to_ms>?3
             LIMIT 1",
            params![
                request.provider,
                request.account_ref,
                request.effective_from
            ],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if open_from.is_some_and(|effective_from| effective_from >= request.effective_from)
        || closed_overlap
    {
        return Err(SubscriptionError::Refused(
            SubscriptionRefusal::OverlappingEffectiveRange,
        ));
    }

    tx.execute(
        "UPDATE account_subscription SET effective_to_ms=?3
         WHERE provider=?1 AND account_ref=?2 AND effective_to_ms IS NULL",
        params![
            request.provider,
            request.account_ref,
            request.effective_from
        ],
    )?;
    tx.execute(
        "INSERT INTO account_subscription (
             account_ref,provider,plan_name,recurring_cost_minor,currency,
             amount_exponent,period,effective_from_ms,effective_to_ms,operator
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,NULL,?9)",
        params![
            request.account_ref,
            request.provider,
            request.plan_name,
            request.recurring_cost_minor,
            request.currency,
            i64::from(request.amount_exponent),
            request.period.as_str(),
            request.effective_from,
            operator,
        ],
    )?;
    advance_plan_generation_in_tx(tx)?;

    Ok(AccountSubscription {
        account_ref: request.account_ref.clone(),
        provider: request.provider.clone(),
        plan_name: request.plan_name.clone(),
        recurring_cost_minor: request.recurring_cost_minor,
        currency: request.currency.clone(),
        amount_exponent: request.amount_exponent,
        period: request.period,
        effective_from: request.effective_from,
        effective_to: None,
        operator: operator.map(str::to_string),
    })
}

fn validate_request(
    request: &SetSubscriptionRequest,
    operator: Option<&str>,
) -> Result<(), SubscriptionError> {
    let currency_is_canonical = request.currency.len() == 3
        && request
            .currency
            .bytes()
            .all(|byte| byte.is_ascii_uppercase());
    if request.account_ref.is_empty()
        || request.provider.is_empty()
        || request.plan_name.is_empty()
        || request.recurring_cost_minor < 0
        || request.amount_exponent > 9
        || request.effective_from < 0
        || !currency_is_canonical
        || operator.is_some_and(str::is_empty)
    {
        return Err(SubscriptionError::Refused(SubscriptionRefusal::Malformed));
    }
    Ok(())
}

/// Retire the open plan for `(provider, account_ref)` by bounding it at
/// `effective_to`.
///
/// `set_subscription_in_tx` above also writes `effective_to_ms`, but only as a
/// side effect of declaring a SUCCESSOR plan, so without this there is always
/// exactly one open plan per key and "this subscription ended and there is no
/// plan now" cannot be recorded at all. That matters because the charge basis
/// arrives from the producer and is derived from the credential type rather
/// than from whether a subscription is actually paid: cancelling a plan while
/// keeping the login leaves the producer still reporting usage as included,
/// and an unbounded plan would cover every future event at zero indefinitely.
///
/// Closing bounds future coverage and nothing else -- see
/// [`CLOSE_COVERAGE_NOTICE`], which the response carries.
pub fn close_subscription_in_tx(
    tx: &Transaction<'_>,
    request: &CloseSubscriptionRequest,
) -> Result<ClosedSubscription, SubscriptionError> {
    validate_close_request(request)?;

    // Refusing an absent plan rather than reporting a no-op: an operator who
    // closes the wrong key needs to be told nothing was closed, and a silent
    // success reads as "the plan is now bounded".
    let open = tx
        .query_row(
            "SELECT account_ref,provider,plan_name,recurring_cost_minor,currency,
                    amount_exponent,period,effective_from_ms,effective_to_ms,operator
             FROM account_subscription
             WHERE provider=?1 AND account_ref=?2 AND effective_to_ms IS NULL",
            params![request.provider, request.account_ref],
            read_subscription,
        )
        .optional()?
        .ok_or(SubscriptionError::Refused(
            SubscriptionRefusal::NoOpenSubscription,
        ))?;

    // A plan cannot end before or at the instant it begins. Equality is refused
    // too: a plan bounded at its own start covers nothing, which is a deletion
    // wearing a retirement's name, and this table keeps closed history.
    if request.effective_to <= open.effective_from {
        return Err(SubscriptionError::Refused(
            SubscriptionRefusal::CloseBeforeEffectiveFrom,
        ));
    }

    tx.execute(
        "UPDATE account_subscription SET effective_to_ms=?3
         WHERE provider=?1 AND account_ref=?2 AND effective_to_ms IS NULL",
        params![request.provider, request.account_ref, request.effective_to],
    )?;
    advance_plan_generation_in_tx(tx)?;

    Ok(ClosedSubscription {
        subscription: AccountSubscription {
            effective_to: Some(request.effective_to),
            ..open
        },
        coverage_is_one_way: true,
        coverage_notice: CLOSE_COVERAGE_NOTICE,
    })
}

fn validate_close_request(request: &CloseSubscriptionRequest) -> Result<(), SubscriptionError> {
    if request.account_ref.is_empty() || request.provider.is_empty() || request.effective_to < 0 {
        return Err(SubscriptionError::Refused(SubscriptionRefusal::Malformed));
    }
    Ok(())
}

pub fn subscription_in_force(
    conn: &Connection,
    provider: &str,
    account_ref: &str,
    instant_ms: i64,
) -> Result<Option<AccountSubscription>, SubscriptionError> {
    conn.query_row(
        "SELECT account_ref,provider,plan_name,recurring_cost_minor,currency,
                amount_exponent,period,effective_from_ms,effective_to_ms,operator
         FROM account_subscription
         WHERE provider=?1 AND account_ref=?2
           AND effective_from_ms<=?3
           AND (effective_to_ms IS NULL OR effective_to_ms>?3)",
        params![provider, account_ref, instant_ms],
        read_subscription,
    )
    .optional()
    .map_err(SubscriptionError::Store)
}

/// Events whose current charge head says `no_subscription_in_force` but whose
/// instant does fall inside a declared plan's effective range.
///
/// Declaring a plan changes nothing that was already recorded, and the pricing
/// pass looks only at measurements that have no charge head at all, so without
/// this list an event that was recorded before its plan was declared would keep
/// saying no plan covers it for as long as it exists. The effective-range test
/// here is deliberately the same half-open comparison `subscription_in_force`
/// makes directly above -- a plan must be in force at the event's instant, not
/// merely in force now -- so:
///
///   * an event this returns resolves to `covered` and then leaves the answer,
///     which makes the list drain rather than repeat;
///   * an event no plan covers at its own instant never enters the list, so it
///     is never rewritten with the reason it already carries;
///   * with no plan declared the join is empty and so is the answer, which is
///     what makes this free for an installation that has declared none.
///
/// An event with no recorded instant never joins either: every comparison
/// against a NULL instant is NULL, and an undated event is unpriced for that
/// reason instead.
///
/// `limit` bounds one batch, taken in event-id order from just after `after`
/// (from the start when `after` is `None`). The caller passes the last id of
/// its previous batch, so successive batches move through the backlog even
/// when some of the events they return fail to reprice: a failed event keeps
/// its uncovered head and stays in the set, and a batch that always started
/// from the beginning would return the same failed prefix on every pass and
/// never reach the events behind it.
pub fn coverage_backfill_event_ids(
    conn: &Connection,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<String>, SubscriptionError> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    // Every string sorts at or after the empty one, so `>= ''` is the start.
    let (comparison, cursor) = match after {
        Some(after) => (">", after),
        None => (">=", ""),
    };
    let mut statement = conn.prepare(&format!(
        "SELECT DISTINCT event.event_id
         FROM priced_charge AS head
         CROSS JOIN usage_event AS event ON event.event_key=head.event_key
         CROSS JOIN account_subscription AS plan
           ON plan.provider=event.provider
          AND plan.account_ref=event.account_ref
          AND plan.effective_from_ms<=event.occurred_at_ms
          AND (plan.effective_to_ms IS NULL
               OR plan.effective_to_ms>event.occurred_at_ms)
         WHERE head.outcome='unpriced'
           AND head.unpriced_reason='no_subscription_in_force'
           AND NOT EXISTS (
               SELECT 1 FROM priced_charge AS child
               WHERE child.supersedes=head.charge_key
           )
           AND event.event_id {comparison} ?2
         ORDER BY event.event_id
         LIMIT ?1"
    ))?;
    let event_ids = statement
        .query_map(params![limit, cursor], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(event_ids)
}

/// Record that the declared plans changed, so the next pricing pass runs the
/// coverage backfill again. Runs in the plan write's own transaction: a plan
/// committed without this would never be backfilled, and the reverse would
/// only cost one wasted backfill query.
fn advance_plan_generation_in_tx(tx: &Transaction<'_>) -> Result<(), SubscriptionError> {
    let updated = tx.execute(
        "UPDATE pricing_selection_state SET plan_generation=plan_generation+1 WHERE id=1",
        [],
    )?;
    if updated != 1 {
        // The row is created by the migration that created the table; its
        // absence means the store is not the schema this code expects.
        return Err(SubscriptionError::Store(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    Ok(())
}

/// The plan generation the coverage backfill still has to drain, or `None`
/// when it has drained every plan write so far and has nothing to look for.
pub fn coverage_backfill_due(conn: &Connection) -> Result<Option<i64>, SubscriptionError> {
    let (plan_generation, drained) = conn.query_row(
        "SELECT plan_generation,backfill_drained_generation
         FROM pricing_selection_state WHERE id=1",
        [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )?;
    Ok((drained < plan_generation).then_some(plan_generation))
}

/// Record that the backfill drained `generation` completely. Changes nothing
/// if a plan write has moved the generation since `generation` was read, so
/// that write is still backfilled on the next pass. Returns whether the mark
/// was recorded.
pub fn mark_coverage_backfill_drained_in_tx(
    tx: &Transaction<'_>,
    generation: i64,
) -> Result<bool, SubscriptionError> {
    let updated = tx.execute(
        "UPDATE pricing_selection_state SET backfill_drained_generation=?1
         WHERE id=1 AND plan_generation=?1",
        [generation],
    )?;
    Ok(updated == 1)
}

fn read_subscription(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountSubscription> {
    let amount_exponent =
        u32::try_from(row.get::<_, i64>(5)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let period = SubscriptionPeriod::parse(&row.get::<_, String>(6)?)
        .ok_or(rusqlite::Error::InvalidQuery)?;
    Ok(AccountSubscription {
        account_ref: row.get(0)?,
        provider: row.get(1)?,
        plan_name: row.get(2)?,
        recurring_cost_minor: row.get(3)?,
        currency: row.get(4)?,
        amount_exponent,
        period,
        effective_from: row.get(7)?,
        effective_to: row.get(8)?,
        operator: row.get(9)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CoverageExpectation {
    NotApplicable,
    Missing {
        provider: String,
        account_ref: String,
        instant_ms: i64,
    },
    Covered {
        provider: String,
        account_ref: String,
        instant_ms: i64,
        effective_from: i64,
        currency: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRepricePlan {
    event_id: String,
    pricing: RepricePlan,
    coverage: CoverageExpectation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSubscriptionReprice {
    pricing: ResolvedReprice,
}

pub fn read_reprice(
    conn: &Connection,
    event_id: &str,
) -> Result<SubscriptionRepricePlan, SubscriptionError> {
    let tx = conn.unchecked_transaction()?;
    let plan = read_reprice_in_tx(&tx, event_id)?;
    tx.commit()?;
    Ok(plan)
}

pub fn read_reprice_in_tx(
    tx: &Transaction<'_>,
    event_id: &str,
) -> Result<SubscriptionRepricePlan, SubscriptionError> {
    read_plan_in_tx(tx, event_id, None)
}

pub fn read_basis_correction(
    conn: &Connection,
    request: &crate::ingest::BasisCorrectRequest,
) -> Result<SubscriptionRepricePlan, SubscriptionError> {
    let tx = conn.unchecked_transaction()?;
    let plan = read_plan_in_tx(&tx, &request.event_id, Some(request))?;
    tx.commit()?;
    Ok(plan)
}

fn read_plan_in_tx(
    tx: &Transaction<'_>,
    event_id: &str,
    correction: Option<&crate::ingest::BasisCorrectRequest>,
) -> Result<SubscriptionRepricePlan, SubscriptionError> {
    let pricing = match correction {
        Some(request) => pricing::read_basis_correction_in_tx(tx, request)?,
        None => pricing::read_reprice_in_tx(tx, event_id)?,
    };

    let (provider, account_ref, instant_ms, charge_basis) =
        pricing.subscription_coverage_identity();
    let coverage = match (provider, account_ref, instant_ms, charge_basis) {
        (
            Some(provider),
            Some(account_ref),
            Some(instant_ms),
            ChargeBasis::SubscriptionIncluded,
        ) if !provider.is_empty() && !account_ref.is_empty() => {
            let provider = provider.to_string();
            let account_ref = account_ref.to_string();
            match subscription_in_force(tx, &provider, &account_ref, instant_ms)? {
                Some(subscription) => CoverageExpectation::Covered {
                    provider,
                    account_ref,
                    instant_ms,
                    effective_from: subscription.effective_from,
                    currency: subscription.currency,
                },
                None => CoverageExpectation::Missing {
                    provider,
                    account_ref,
                    instant_ms,
                },
            }
        }
        _ => CoverageExpectation::NotApplicable,
    };
    Ok(SubscriptionRepricePlan {
        event_id: event_id.to_string(),
        pricing,
        coverage,
    })
}

pub fn resolve_reprice(
    plan: &SubscriptionRepricePlan,
    catalog: &CatalogClient,
    reasoning_billed_as_output: &[String],
) -> Result<ResolvedSubscriptionReprice, SubscriptionError> {
    resolve_reprice_with_catalog(plan, Some(catalog), reasoning_billed_as_output)
}

pub fn resolve_reprice_without_catalog(
    plan: &SubscriptionRepricePlan,
) -> Result<ResolvedSubscriptionReprice, SubscriptionError> {
    resolve_reprice_with_catalog(plan, None, &[])
}

fn resolve_reprice_with_catalog(
    plan: &SubscriptionRepricePlan,
    catalog: Option<&CatalogClient>,
    reasoning_billed_as_output: &[String],
) -> Result<ResolvedSubscriptionReprice, SubscriptionError> {
    // Coverage wins before the catalog is consulted: a subscription in force at
    // the event's instant already answers what the charge was (zero, and the
    // plan's currency is authoritative), so asking a vendor rate table would be
    // asking a question already answered and billing a covered call twice over.
    // A covered arm that reached the catalog would also make an operator action
    // depend on a peer being up.
    let pricing = match (&plan.coverage, catalog) {
        (CoverageExpectation::Covered { currency, .. }, _) => {
            pricing::resolve_subscription_coverage(&plan.pricing, currency)?
        }
        (
            CoverageExpectation::NotApplicable | CoverageExpectation::Missing { .. },
            Some(catalog),
        ) => pricing::resolve_reprice(&plan.pricing, catalog, reasoning_billed_as_output)?,
        (CoverageExpectation::NotApplicable | CoverageExpectation::Missing { .. }, None) => {
            pricing::resolve_reprice_without_catalog(&plan.pricing)?
        }
    };
    Ok(ResolvedSubscriptionReprice { pricing })
}

pub fn commit_reprice_in_tx(
    tx: &Transaction<'_>,
    plan: &SubscriptionRepricePlan,
    resolved: &ResolvedSubscriptionReprice,
    recorded_at_ms: i64,
    origin: pricing::PricingOrigin,
) -> Result<RepriceOutcome, SubscriptionError> {
    // Coverage was read outside this transaction, because resolving it may call
    // the catalog and network calls must not run inside one. So the plan's view
    // of which subscription was in force can be stale by the time it commits —
    // an operator may have closed or re-dated a plan in between. Re-check it
    // here and refuse rather than write money computed against a plan that no
    // longer applies.
    if !coverage_expectation_still_holds(tx, &plan.coverage)? {
        return Err(SubscriptionError::Refused(
            SubscriptionRefusal::CoverageSuperseded,
        ));
    }
    pricing::commit_reprice_in_tx(tx, &plan.pricing, &resolved.pricing, recorded_at_ms, origin)
        .map_err(SubscriptionError::Pricing)
}

fn coverage_expectation_still_holds(
    conn: &Connection,
    expectation: &CoverageExpectation,
) -> Result<bool, SubscriptionError> {
    let (provider, account_ref, instant_ms) = match expectation {
        CoverageExpectation::NotApplicable => return Ok(true),
        CoverageExpectation::Missing {
            provider,
            account_ref,
            instant_ms,
        }
        | CoverageExpectation::Covered {
            provider,
            account_ref,
            instant_ms,
            ..
        } => (provider, account_ref, *instant_ms),
    };
    let current = subscription_in_force(conn, provider, account_ref, instant_ms)?;
    match (expectation, current) {
        (CoverageExpectation::Missing { .. }, None) => Ok(true),
        (
            CoverageExpectation::Covered {
                effective_from,
                currency,
                ..
            },
            Some(subscription),
        ) => Ok(
            subscription.effective_from == *effective_from && subscription.currency == *currency
        ),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
    use rusqlite::params;
    use serde_json::json;

    use super::*;
    use crate::catalog_client::{CatalogCallError, CatalogConnection};
    use crate::store::{initialize_connection, MIGRATIONS};

    /// Declares a plan the way the served subscription-set operation does:
    /// through `set_subscription_in_tx` inside one transaction that commits on
    /// success.
    fn set_subscription(
        conn: &mut Connection,
        request: &SetSubscriptionRequest,
        operator: Option<&str>,
    ) -> Result<AccountSubscription, SubscriptionError> {
        let tx = conn.transaction()?;
        let subscription = set_subscription_in_tx(&tx, request, operator)?;
        tx.commit()?;
        Ok(subscription)
    }

    /// Closes a plan the way the served subscription-close operation does:
    /// through `close_subscription_in_tx` inside one transaction that commits
    /// on success.
    fn close_subscription(
        conn: &mut Connection,
        request: &CloseSubscriptionRequest,
    ) -> Result<ClosedSubscription, SubscriptionError> {
        let tx = conn.transaction()?;
        let closed = close_subscription_in_tx(&tx, request)?;
        tx.commit()?;
        Ok(closed)
    }

    /// Reprices one event with the same read, resolve and commit steps the
    /// served reprice operation composes, including the coverage re-check at
    /// commit.
    fn reprice_event(
        conn: &mut Connection,
        event_id: &str,
        catalog: &CatalogClient,
        recorded_at_ms: i64,
        origin: pricing::PricingOrigin,
    ) -> Result<RepriceOutcome, SubscriptionError> {
        let plan = read_reprice(conn, event_id)?;
        let resolved = resolve_reprice(&plan, catalog, &[])?;
        let tx = conn.transaction()?;
        let outcome = commit_reprice_in_tx(&tx, &plan, &resolved, recorded_at_ms, origin)?;
        tx.commit()?;
        Ok(outcome)
    }

    struct NoCallCatalog {
        calls: AtomicUsize,
    }

    impl CatalogConnection for NoCallCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, crate::catalog_client::CatalogCallError>
        {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            if request.at_ms == Some(0) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "fusiform's history begins at 1".into(),
                });
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(CatalogCallError {
                code: "unexpected_catalog_call".into(),
                message: "subscription pricing must not consult the catalog".into(),
            })
        }
    }

    fn catalog() -> (CatalogClient, Arc<NoCallCatalog>) {
        let connection = Arc::new(NoCallCatalog {
            calls: AtomicUsize::new(0),
        });
        let client = CatalogClient::connect(connection.clone()).unwrap();
        (client, connection)
    }

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn request(
        plan_name: &str,
        currency: &str,
        amount_exponent: u32,
        period: SubscriptionPeriod,
        effective_from: i64,
    ) -> SetSubscriptionRequest {
        SetSubscriptionRequest {
            account_ref: "account-a".into(),
            provider: "provider-a".into(),
            plan_name: plan_name.into(),
            recurring_cost_minor: 500,
            currency: currency.into(),
            amount_exponent,
            period,
            effective_from,
        }
    }

    fn close_request(effective_to: i64) -> CloseSubscriptionRequest {
        CloseSubscriptionRequest {
            account_ref: "account-a".into(),
            provider: "provider-a".into(),
            effective_to,
        }
    }

    /// The whole stored plan table as (row count, plan name, bound), so a
    /// refusal test can assert nothing was written AND nothing was modified
    /// rather than only counting rows.
    fn stored_plans(conn: &Connection) -> (i64, Option<String>, Option<i64>) {
        conn.query_row(
            "SELECT COUNT(*),MAX(plan_name),MAX(effective_to_ms) FROM account_subscription",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
    }

    fn reprice_outcome(conn: &mut Connection, event_id: &str) -> (String, Option<String>) {
        let (catalog, _) = catalog();
        let _ = reprice_event(
            conn,
            event_id,
            &catalog,
            1_000,
            pricing::PricingOrigin::PricingPass,
        );
        conn.query_row(
            "SELECT outcome,unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    fn count_subscriptions(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM account_subscription", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn record_subscription_event(conn: &Connection, event_id: &str, occurred_at_ms: Option<i64>) {
        conn.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_lineage,producer_ref,occurred_at_ms,
                 recorded_at_ms,provider,account_ref,charge_basis,pricing_generation
             ) VALUES (?1,'producer','lineage',?1,?2,900,'provider-a','account-a',
                       'subscription_included',0)",
            params![event_id, occurred_at_ms],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO measurement (
                 event_key,role,unit,kind,source,quantity_units,quantity_exponent
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','producer',100,0)",
            [event_id],
        )
        .unwrap();
    }

    unsafe extern "C" fn capture_sql_statement(
        _mask: std::os::raw::c_uint,
        context: *mut std::ffi::c_void,
        _statement: *mut std::ffi::c_void,
        sql: *mut std::ffi::c_void,
    ) -> std::os::raw::c_int {
        let statements = unsafe { &mut *context.cast::<Vec<String>>() };
        let sql = unsafe { std::ffi::CStr::from_ptr(sql.cast()) };
        statements.push(sql.to_string_lossy().into_owned());
        0
    }

    fn start_sql_trace(conn: &Connection, statements: &mut Vec<String>) {
        let result = unsafe {
            rusqlite::ffi::sqlite3_trace_v2(
                conn.handle(),
                rusqlite::ffi::SQLITE_TRACE_STMT as u32,
                Some(capture_sql_statement),
                (statements as *mut Vec<String>).cast(),
            )
        };
        assert_eq!(result, rusqlite::ffi::SQLITE_OK);
    }

    fn stop_sql_trace(conn: &Connection) {
        let result = unsafe {
            rusqlite::ffi::sqlite3_trace_v2(conn.handle(), 0_u32, None, std::ptr::null_mut())
        };
        assert_eq!(result, rusqlite::ffi::SQLITE_OK);
    }

    #[test]
    fn reprice_plan_reads_the_usage_event_once() {
        let conn = conn();
        record_subscription_event(&conn, "event-a", Some(150));
        let mut statements = Vec::new();
        start_sql_trace(&conn, &mut statements);

        read_reprice(&conn, "event-a").unwrap();

        stop_sql_trace(&conn);
        let event_reads = statements
            .iter()
            .filter(|sql| sql.contains("FROM usage_event"))
            .count();
        assert_eq!(
            event_reads, 1,
            "reprice planning should reuse the event row already read for pricing"
        );
    }

    #[test]
    fn pricing_refusal_message_is_stable_and_keeps_its_code() {
        let error = SubscriptionError::Pricing(PricingError::Refused(
            pricing::PricingRefusal::AlreadyPriced,
        ));

        assert_eq!(error.code(), "already_priced");
        assert_eq!(
            error.message(),
            "subscription pricing operation refused: already_priced"
        );
    }

    fn stored_pricing(
        conn: &Connection,
        event_id: &str,
    ) -> (Option<String>, i64, String, Option<String>) {
        conn.query_row(
            "SELECT e.currency,e.pricing_generation,p.outcome,p.unpriced_reason
             FROM usage_event AS e
             JOIN priced_charge AS p ON p.event_key=e.event_key
             WHERE e.event_id=?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
    }

    #[derive(Debug, PartialEq, Eq)]
    struct UnpricedShape {
        rows: i64,
        outcome: String,
        reason: String,
        charge_nanos: Option<i64>,
        rate_units: Option<i64>,
        catalog_source: Option<String>,
        resolved_at_ms: Option<i64>,
        catalog_entry: Option<String>,
    }

    fn assert_no_subscription_unpriced_shape(conn: &Connection, event_id: &str) {
        let stored = conn
            .query_row(
                "SELECT COUNT(*),outcome,unpriced_reason,charge_nanos,rate_units,
                        catalog_source,resolved_at_ms,catalog_entry
                 FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [event_id],
                |row| {
                    Ok(UnpricedShape {
                        rows: row.get(0)?,
                        outcome: row.get(1)?,
                        reason: row.get(2)?,
                        charge_nanos: row.get(3)?,
                        rate_units: row.get(4)?,
                        catalog_source: row.get(5)?,
                        resolved_at_ms: row.get(6)?,
                        catalog_entry: row.get(7)?,
                    })
                },
            )
            .unwrap();
        assert_eq!(
            stored,
            UnpricedShape {
                rows: 1,
                outcome: "unpriced".into(),
                reason: "no_subscription_in_force".into(),
                charge_nanos: None,
                rate_units: None,
                catalog_source: None,
                resolved_at_ms: None,
                catalog_entry: None,
            }
        );
    }

    /// `effective_to_ms` is the mechanism and coverage is the claim, so this
    /// asserts the repriced OUTCOME of events after the bound, not only the
    /// column. The instant equal to the close is included because coverage is
    /// half-open (`effective_to_ms > instant`) and a fixture testing only a
    /// strictly-later instant leaves that boundary untested.
    #[test]
    fn closing_a_plan_bounds_it_and_later_events_stop_resolving_to_covered() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("cancelled", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();

        let closed = close_subscription(&mut conn, &close_request(200)).unwrap();

        assert_eq!(closed.subscription.effective_to, Some(200));
        assert_eq!(closed.subscription.plan_name, "cancelled");
        assert!(closed.coverage_is_one_way);
        assert_eq!(closed.coverage_notice, CLOSE_COVERAGE_NOTICE);
        assert_eq!(
            stored_plans(&conn),
            (1, Some("cancelled".into()), Some(200))
        );
        assert!(subscription_in_force(&conn, "provider-a", "account-a", 200)
            .unwrap()
            .is_none());
        for (event_id, instant) in [("at-the-bound", 200), ("after-the-bound", 250)] {
            record_subscription_event(&conn, event_id, Some(instant));
            assert_eq!(
                reprice_outcome(&mut conn, event_id),
                ("unpriced".into(), Some("no_subscription_in_force".into())),
                "{event_id}"
            );
        }
    }

    /// Closing is a BOUND, not a deletion. An implementation that retired the
    /// plan wholesale -- or bounded it at its own start -- would still stop
    /// later events resolving to covered, so only an event INSIDE the window
    /// distinguishes the two, which is the only reason this arm exists.
    #[test]
    fn an_event_before_the_close_instant_still_resolves_to_covered() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("cancelled", "JPY", 0, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        close_subscription(&mut conn, &close_request(200)).unwrap();
        record_subscription_event(&conn, "inside-the-window", Some(150));

        assert_eq!(
            reprice_outcome(&mut conn, "inside-the-window"),
            ("covered".into(), None)
        );
        assert_eq!(
            stored_pricing(&conn, "inside-the-window"),
            (Some("JPY".into()), 1, "covered".into(), None)
        );
    }

    /// Both arms are needed: against an empty table the refusal could be
    /// reading whether the TABLE has rows rather than whether the key has an
    /// OPEN plan, so the second arm supplies a key that has history and no
    /// open plan.
    #[test]
    fn closing_with_no_open_plan_refuses_without_writing_or_modifying_a_row() {
        let mut conn = conn();

        let empty = close_subscription(&mut conn, &close_request(200));
        assert!(matches!(
            empty,
            Err(SubscriptionError::Refused(
                SubscriptionRefusal::NoOpenSubscription
            ))
        ));
        assert_eq!(stored_plans(&conn), (0, None, None));

        set_subscription(
            &mut conn,
            &request("already-closed", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        close_subscription(&mut conn, &close_request(200)).unwrap();

        let again = close_subscription(&mut conn, &close_request(300));
        assert!(matches!(
            again,
            Err(SubscriptionError::Refused(
                SubscriptionRefusal::NoOpenSubscription
            ))
        ));
        assert_eq!(
            stored_plans(&conn),
            (1, Some("already-closed".into()), Some(200))
        );
    }

    /// Both the equal and the earlier instant are driven: a fixture supplying
    /// only an earlier one leaves `<=` and `<` indistinguishable, and equality
    /// is the case that would mint a plan covering nothing.
    #[test]
    fn closing_at_or_before_effective_from_refuses_without_modifying_the_row() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("retained", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();

        for effective_to in [100, 99, 0] {
            let result = close_subscription(&mut conn, &close_request(effective_to));
            assert!(
                matches!(
                    result,
                    Err(SubscriptionError::Refused(
                        SubscriptionRefusal::CloseBeforeEffectiveFrom
                    ))
                ),
                "{effective_to}"
            );
            assert_eq!(
                stored_plans(&conn),
                (1, Some("retained".into()), None),
                "{effective_to}"
            );
        }
    }

    /// Coverage is one-way. An event priced as covered stays covered after the
    /// plan that covered it is closed, even when the close puts that event's
    /// instant outside the plan's range -- repricing re-resolves unpriced
    /// charges only and refuses the rest. Stated as a test so a later reader
    /// does not assume closing repairs the ledger.
    #[test]
    fn an_event_covered_before_the_close_stays_covered_afterwards() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("cancelled", "JPY", 0, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        record_subscription_event(&conn, "already-covered", Some(150));
        assert_eq!(
            reprice_outcome(&mut conn, "already-covered"),
            ("covered".into(), None)
        );

        close_subscription(&mut conn, &close_request(120)).unwrap();

        // The close really did put the event outside the plan's range ...
        assert!(subscription_in_force(&conn, "provider-a", "account-a", 150)
            .unwrap()
            .is_none());
        // ... and the already-covered charge is still covered regardless.
        let (catalog, _) = catalog();
        let result = reprice_event(
            &mut conn,
            "already-covered",
            &catalog,
            2_000,
            pricing::PricingOrigin::OperatorReprice,
        );
        assert!(matches!(
            result,
            Err(SubscriptionError::Pricing(PricingError::Refused(
                crate::pricing::PricingRefusal::AlreadyPriced
            )))
        ));
        assert_eq!(
            stored_pricing(&conn, "already-covered"),
            (Some("JPY".into()), 1, "covered".into(), None)
        );
        let covered: (i64, i64) = conn
            .query_row(
                "SELECT charge_nanos,basis_covered FROM priced_charge
                 WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='already-covered')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(covered, (0, 1));
    }

    #[test]
    fn close_request_rejects_unknown_and_malformed_fields_without_stored_state() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("retained", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();

        // `effective_from` is the set operation's field; accepting it here
        // would silently close at an instant the caller did not name.
        for payload in [
            json!({"account_ref":"account-a","provider":"provider-a","effective_from":200}),
            json!({"account_ref":"account-a","provider":"provider-a"}),
            json!({"account_ref":"account-a","provider":"provider-a","effective_to":200,"operator":"forged"}),
        ] {
            assert!(
                serde_json::from_value::<CloseSubscriptionRequest>(payload.clone()).is_err(),
                "{payload}"
            );
        }
        for malformed in [
            CloseSubscriptionRequest {
                account_ref: String::new(),
                provider: "provider-a".into(),
                effective_to: 200,
            },
            CloseSubscriptionRequest {
                account_ref: "account-a".into(),
                provider: String::new(),
                effective_to: 200,
            },
            CloseSubscriptionRequest {
                account_ref: "account-a".into(),
                provider: "provider-a".into(),
                effective_to: -1,
            },
        ] {
            assert!(matches!(
                close_subscription(&mut conn, &malformed),
                Err(SubscriptionError::Refused(SubscriptionRefusal::Malformed))
            ));
        }
        assert_eq!(stored_plans(&conn), (1, Some("retained".into()), None));
    }

    #[test]
    fn jpy_exponent_zero_preserves_scale_and_operator_write_path_identity() {
        let mut conn = conn();
        let subscription = set_subscription(
            &mut conn,
            &request("jpy-plan", "JPY", 0, SubscriptionPeriod::Month, 100),
            Some("operator-a"),
        )
        .unwrap();

        assert_eq!(
            subscription.recurring_cost_nanos().unwrap(),
            500_000_000_000
        );
        let stored: (i64, i64, String) = conn
            .query_row(
                "SELECT recurring_cost_minor,amount_exponent,operator
                 FROM account_subscription",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (500, 0, "operator-a".into()));
    }

    #[test]
    fn monthly_subscription_uses_declared_mid_month_anchor_not_calendar_month_start() {
        let mut conn = conn();
        let march_fourteenth_utc = 1_773_446_400_000_i64;
        set_subscription(
            &mut conn,
            &request(
                "mid-month",
                "USD",
                2,
                SubscriptionPeriod::Month,
                march_fourteenth_utc,
            ),
            None,
        )
        .unwrap();

        assert!(
            subscription_in_force(&conn, "provider-a", "account-a", march_fourteenth_utc - 1)
                .unwrap()
                .is_none()
        );
        let active = subscription_in_force(&conn, "provider-a", "account-a", march_fourteenth_utc)
            .unwrap()
            .unwrap();
        assert_eq!(active.effective_from, march_fourteenth_utc);
        assert_eq!(active.period, SubscriptionPeriod::Month);
    }

    #[test]
    fn successor_closes_prior_and_half_open_boundaries_select_one_plan() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("first", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        set_subscription(
            &mut conn,
            &request("second", "USD", 2, SubscriptionPeriod::Year, 200),
            None,
        )
        .unwrap();

        let first = subscription_in_force(&conn, "provider-a", "account-a", 199)
            .unwrap()
            .unwrap();
        let second = subscription_in_force(&conn, "provider-a", "account-a", 200)
            .unwrap()
            .unwrap();
        assert_eq!(
            (first.plan_name.as_str(), first.effective_to),
            ("first", Some(200))
        );
        assert_eq!(
            (second.plan_name.as_str(), second.effective_to),
            ("second", None)
        );
        assert_eq!(count_subscriptions(&conn), 2);
    }

    #[test]
    fn overlapping_effective_range_is_refused_without_changing_stored_state() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("retained", "USD", 2, SubscriptionPeriod::Month, 200),
            None,
        )
        .unwrap();

        for effective_from in [200, 199] {
            let result = set_subscription(
                &mut conn,
                &request(
                    "overlap",
                    "USD",
                    2,
                    SubscriptionPeriod::Month,
                    effective_from,
                ),
                None,
            );
            assert!(matches!(
                result,
                Err(SubscriptionError::Refused(
                    SubscriptionRefusal::OverlappingEffectiveRange
                ))
            ));
            let stored: (i64, Option<i64>, String) = conn
                .query_row(
                    "SELECT COUNT(*),MAX(effective_to_ms),MAX(plan_name)
                     FROM account_subscription",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(stored, (1, None, "retained".into()));
        }
    }

    #[test]
    fn successor_insert_failure_rolls_back_the_prior_effective_to_close() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("retained", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_successor_after_close
             BEFORE INSERT ON account_subscription
             WHEN NEW.plan_name='force-failure'
              AND EXISTS (
                  SELECT 1 FROM account_subscription
                  WHERE provider=NEW.provider AND account_ref=NEW.account_ref
                    AND effective_to_ms=NEW.effective_from_ms
              )
             BEGIN SELECT RAISE(ABORT,'forced successor failure after close'); END;",
        )
        .unwrap();

        let result = set_subscription(
            &mut conn,
            &request("force-failure", "USD", 2, SubscriptionPeriod::Month, 200),
            None,
        );
        assert!(matches!(result, Err(SubscriptionError::Store(_))));
        let stored: (i64, Option<i64>, String) = conn
            .query_row(
                "SELECT COUNT(*),MAX(effective_to_ms),MAX(plan_name)
                 FROM account_subscription",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (1, None, "retained".into()));
    }

    #[test]
    fn malformed_declaration_values_refuse_without_stored_state() {
        let mut conn = conn();
        let mut requests = Vec::new();
        let mut empty_account = request("plan", "USD", 2, SubscriptionPeriod::Month, 100);
        empty_account.account_ref.clear();
        requests.push(empty_account);
        let mut empty_provider = request("plan", "USD", 2, SubscriptionPeriod::Month, 100);
        empty_provider.provider.clear();
        requests.push(empty_provider);
        let mut empty_plan = request("plan", "USD", 2, SubscriptionPeriod::Month, 100);
        empty_plan.plan_name.clear();
        requests.push(empty_plan);
        let mut negative_cost = request("plan", "USD", 2, SubscriptionPeriod::Month, 100);
        negative_cost.recurring_cost_minor = -1;
        requests.push(negative_cost);
        requests.push(request("plan", "usd", 2, SubscriptionPeriod::Month, 100));
        requests.push(request("plan", "USD", 10, SubscriptionPeriod::Month, 100));
        requests.push(request("plan", "USD", 2, SubscriptionPeriod::Month, -1));

        for malformed in requests {
            let result = set_subscription(&mut conn, &malformed, None);
            assert!(matches!(
                result,
                Err(SubscriptionError::Refused(SubscriptionRefusal::Malformed))
            ));
            assert_eq!(count_subscriptions(&conn), 0);
        }
        let result = set_subscription(
            &mut conn,
            &request("plan", "USD", 2, SubscriptionPeriod::Month, 100),
            Some(""),
        );
        assert!(matches!(
            result,
            Err(SubscriptionError::Refused(SubscriptionRefusal::Malformed))
        ));
        assert_eq!(count_subscriptions(&conn), 0);
    }

    #[test]
    fn recurring_cost_conversion_overflow_refuses_without_stored_state() {
        let mut conn = conn();
        let mut overflow = request("overflow", "JPY", 0, SubscriptionPeriod::Year, 100);
        overflow.recurring_cost_minor = i64::MAX;
        let result = set_subscription(&mut conn, &overflow, None);
        assert!(matches!(
            result,
            Err(SubscriptionError::Refused(
                SubscriptionRefusal::RecurringCostOutOfRange
            ))
        ));
        assert_eq!(count_subscriptions(&conn), 0);
    }

    #[test]
    fn absent_amount_exponent_is_malformed_without_stored_state() {
        let conn = conn();
        let result = serde_json::from_value::<SetSubscriptionRequest>(json!({
            "account_ref": "account-a",
            "provider": "provider-a",
            "plan_name": "plan",
            "recurring_cost_minor": 500,
            "currency": "USD",
            "period": "month",
            "effective_from": 100
        }));
        assert!(result.is_err());
        assert_eq!(count_subscriptions(&conn), 0);
    }

    #[test]
    fn payload_supplied_operator_is_malformed_without_stored_state() {
        let conn = conn();
        let result = serde_json::from_value::<SetSubscriptionRequest>(json!({
            "account_ref": "account-a",
            "provider": "provider-a",
            "plan_name": "plan",
            "recurring_cost_minor": 500,
            "currency": "USD",
            "amount_exponent": 2,
            "period": "month",
            "effective_from": 100,
            "operator": "forged"
        }));
        assert!(result.is_err());
        assert_eq!(count_subscriptions(&conn), 0);
    }

    #[test]
    fn unknown_period_vocabulary_is_malformed_without_stored_state() {
        let conn = conn();
        for period in ["monthly", "P1M", ""] {
            let result = serde_json::from_value::<SetSubscriptionRequest>(json!({
                "account_ref": "account-a",
                "provider": "provider-a",
                "plan_name": "plan",
                "recurring_cost_minor": 500,
                "currency": "USD",
                "amount_exponent": 2,
                "period": period,
                "effective_from": 100
            }));
            assert!(result.is_err(), "{period}");
            assert_eq!(count_subscriptions(&conn), 0);
        }
    }

    #[test]
    fn storage_constraints_refuse_invalid_subscription_rows_without_effect() {
        let conn = conn();
        for values in [
            "('', 'provider', 'plan', 1, 'USD', 2, 'month', 100, NULL, NULL)",
            "('account', '', 'plan', 1, 'USD', 2, 'month', 100, NULL, NULL)",
            "('account', 'provider', '', 1, 'USD', 2, 'month', 100, NULL, NULL)",
            "('account', 'provider', 'plan', -1, 'USD', 2, 'month', 100, NULL, NULL)",
            "('account', 'provider', 'plan', 1, 'usd', 2, 'month', 100, NULL, NULL)",
            "('account', 'provider', 'plan', 1, 'USD', 10, 'month', 100, NULL, NULL)",
            "('account', 'provider', 'plan', 1, 'USD', 2, 'P1M', 100, NULL, NULL)",
            "('account', 'provider', 'plan', 1, 'USD', 2, 'month', -1, NULL, NULL)",
            "('account', 'provider', 'plan', 1, 'USD', 2, 'month', 100, 100, NULL)",
            "('account', 'provider', 'plan', 1, 'USD', 2, 'month', 100, NULL, '')",
        ] {
            let result = conn.execute(
                &format!(
                    "INSERT INTO account_subscription (
                         account_ref,provider,plan_name,recurring_cost_minor,currency,
                         amount_exponent,period,effective_from_ms,effective_to_ms,operator
                     ) VALUES {values}"
                ),
                [],
            );
            assert!(result.is_err(), "{values}");
            assert_eq!(count_subscriptions(&conn), 0, "{values}");
        }
    }

    #[test]
    fn storage_overlap_and_identity_guards_refuse_without_effect() {
        let conn = conn();
        conn.execute(
            "INSERT INTO account_subscription (
                 account_ref,provider,plan_name,recurring_cost_minor,currency,
                 amount_exponent,period,effective_from_ms,effective_to_ms
             ) VALUES ('account-a','provider-a','first',1,'USD',2,'month',100,300)",
            [],
        )
        .unwrap();
        for values in [
            "('account-a','provider-a','duplicate',1,'USD',2,'month',100,200)",
            "('account-a','provider-a','overlap',1,'USD',2,'month',200,400)",
            "('account-a','provider-a','open-overlap',1,'USD',2,'month',250,NULL)",
        ] {
            let result = conn.execute(
                &format!(
                    "INSERT INTO account_subscription (
                         account_ref,provider,plan_name,recurring_cost_minor,currency,
                         amount_exponent,period,effective_from_ms,effective_to_ms
                     ) VALUES {values}"
                ),
                [],
            );
            assert!(result.is_err(), "{values}");
            assert_eq!(count_subscriptions(&conn), 1, "{values}");
        }
    }

    #[test]
    fn storage_update_and_delete_guards_preserve_subscription_history() {
        let conn = conn();
        conn.execute(
            "INSERT INTO account_subscription (
                 account_ref,provider,plan_name,recurring_cost_minor,currency,
                 amount_exponent,period,effective_from_ms
             ) VALUES ('account-a','provider-a','retained',1,'USD',2,'month',100)",
            [],
        )
        .unwrap();
        assert!(conn
            .execute("UPDATE account_subscription SET plan_name='rewritten'", [],)
            .is_err());
        assert!(conn
            .execute("UPDATE account_subscription SET effective_to_ms=100", [])
            .is_err());
        conn.execute("UPDATE account_subscription SET effective_to_ms=200", [])
            .unwrap();
        assert!(conn
            .execute("UPDATE account_subscription SET effective_to_ms=300", [])
            .is_err());
        assert!(conn
            .execute("DELETE FROM account_subscription", [])
            .is_err());
        let stored: (i64, String, Option<i64>) = conn
            .query_row(
                "SELECT COUNT(*),MAX(plan_name),MAX(effective_to_ms)
                 FROM account_subscription",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (1, "retained".into(), Some(200)));
    }

    #[test]
    fn subscription_in_force_covers_at_event_instant_without_catalog_io() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("active", "JPY", 0, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        record_subscription_event(&conn, "covered-event", Some(150));
        let (catalog, connection) = catalog();

        reprice_event(
            &mut conn,
            "covered-event",
            &catalog,
            1_000,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();

        assert_eq!(connection.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            stored_pricing(&conn, "covered-event"),
            (Some("JPY".into()), 1, "covered".into(), None)
        );
        let covered: (i64, i64) = conn
            .query_row(
                "SELECT charge_nanos,basis_covered FROM priced_charge
                 WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='covered-event')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(covered, (0, 1));
    }

    #[test]
    fn declared_reasoning_is_covered_without_catalog_io() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("active", "JPY", 0, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        record_subscription_event(&conn, "covered-thinking", Some(150));
        conn.execute("INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
            VALUES ((SELECT event_key FROM usage_event WHERE event_id='covered-thinking'),'reasoning','token','billed','usage.reasoning',1000,0)", []).unwrap();
        let (catalog, connection) = catalog();
        let plan = read_reprice(&conn, "covered-thinking").unwrap();
        let resolved = resolve_reprice(&plan, &catalog, &["provider-a".into()]).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(
            &tx,
            &plan,
            &resolved,
            1_000,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(connection.calls.load(Ordering::SeqCst), 0);
        let rows: Vec<(String, String, i64, i64, Option<String>)> = conn.prepare(
            "SELECT role,outcome,charge_nanos,basis_covered,catalog_entry FROM priced_charge ORDER BY role"
        ).unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)))
            .unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(
            rows,
            ["input", "reasoning"].map(|role| (role.into(), "covered".into(), 0, 1, None))
        );
    }

    #[test]
    fn plan_starting_after_event_is_unpriced_no_subscription_in_force() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("future", "USD", 2, SubscriptionPeriod::Month, 200),
            None,
        )
        .unwrap();
        record_subscription_event(&conn, "historical-event", Some(150));
        let (catalog, connection) = catalog();

        reprice_event(
            &mut conn,
            "historical-event",
            &catalog,
            1_000,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();

        assert_eq!(connection.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            stored_pricing(&conn, "historical-event"),
            (
                None,
                1,
                "unpriced".into(),
                Some("no_subscription_in_force".into())
            )
        );
        assert_no_subscription_unpriced_shape(&conn, "historical-event");
    }

    #[test]
    fn absent_subscription_records_named_unpriced_without_catalog_io() {
        let mut conn = conn();
        record_subscription_event(&conn, "no-plan", Some(150));
        let (catalog, connection) = catalog();

        reprice_event(
            &mut conn,
            "no-plan",
            &catalog,
            1_000,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();

        assert_eq!(connection.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            stored_pricing(&conn, "no-plan"),
            (
                None,
                1,
                "unpriced".into(),
                Some("no_subscription_in_force".into())
            )
        );
        assert_no_subscription_unpriced_shape(&conn, "no-plan");
    }

    #[test]
    fn undated_subscription_event_is_unpriced_without_plan_or_catalog_resolution() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("active", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        record_subscription_event(&conn, "undated", None);
        let (catalog, connection) = catalog();

        reprice_event(
            &mut conn,
            "undated",
            &catalog,
            1_000,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();

        assert_eq!(connection.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            stored_pricing(&conn, "undated"),
            (None, 1, "unpriced".into(), Some("undated_event".into()))
        );
    }

    #[test]
    fn covered_currency_mismatch_refuses_without_charge_or_event_change() {
        let mut conn = conn();
        set_subscription(
            &mut conn,
            &request("jpy", "JPY", 0, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();
        record_subscription_event(&conn, "currency-mismatch", Some(150));
        conn.execute(
            "UPDATE usage_event SET currency='EUR' WHERE event_id='currency-mismatch'",
            [],
        )
        .unwrap();
        let (catalog, _) = catalog();

        let result = reprice_event(
            &mut conn,
            "currency-mismatch",
            &catalog,
            1_000,
            pricing::PricingOrigin::PricingPass,
        );
        assert!(matches!(
            result,
            Err(SubscriptionError::Pricing(PricingError::Refused(_)))
        ));
        let stored: (String, i64, i64) = conn
            .query_row(
                "SELECT currency,pricing_generation,
                        (SELECT COUNT(*) FROM priced_charge WHERE event_id=usage_event.event_id)
                 FROM usage_event WHERE event_id='currency-mismatch'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, ("EUR".into(), 0, 0));
    }

    #[test]
    fn subscription_change_between_resolve_and_commit_refuses_without_pricing_effect() {
        let mut conn = conn();
        record_subscription_event(&conn, "contended", Some(150));
        let (catalog, _) = catalog();
        let plan = read_reprice(&conn, "contended").unwrap();
        let resolved = resolve_reprice(&plan, &catalog, &[]).unwrap();
        set_subscription(
            &mut conn,
            &request("late-declaration", "USD", 2, SubscriptionPeriod::Month, 100),
            None,
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let result = commit_reprice_in_tx(
            &tx,
            &plan,
            &resolved,
            1_000,
            pricing::PricingOrigin::PricingPass,
        );
        assert!(matches!(
            result,
            Err(SubscriptionError::Refused(
                SubscriptionRefusal::CoverageSuperseded
            ))
        ));
        drop(tx);
        let stored: (Option<String>, i64, i64) = conn
            .query_row(
                "SELECT currency,pricing_generation,
                        (SELECT COUNT(*) FROM priced_charge WHERE event_id=usage_event.event_id)
                 FROM usage_event WHERE event_id='contended'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (None, 0, 0));
    }

    #[test]
    fn pricing_catalog_failure_message_keeps_its_cause() {
        let error = SubscriptionError::Pricing(PricingError::Catalog(
            crate::catalog_client::CatalogClientError::BoundaryDiscovery(
                "fusiform refused: injected cause".to_string(),
            ),
        ));
        assert_eq!(error.code(), "internal");
        assert!(
            error.message().contains("injected cause"),
            "the catalog failure's cause must reach the message: {}",
            error.message()
        );
    }

    #[test]
    fn pricing_store_failure_message_keeps_the_sqlite_codes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t(x UNIQUE); INSERT INTO t VALUES (1);")
            .unwrap();
        let store_error = conn.execute("INSERT INTO t VALUES (1)", []).unwrap_err();
        let error = SubscriptionError::Pricing(PricingError::Store(store_error));
        assert_eq!(error.code(), "internal");
        let message = error.message();
        for detail in [
            "SqliteFailure",
            "ConstraintViolation",
            "extended_code: 2067",
            "UNIQUE constraint failed: t.x",
        ] {
            assert!(
                message.contains(detail),
                "the store failure's {detail} must reach the message: {message}"
            );
        }
    }
}
