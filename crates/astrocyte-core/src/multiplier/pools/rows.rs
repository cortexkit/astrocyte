use std::collections::BTreeMap;

use fusiform_protocol::PlanPricesResponse;
use serde_json::json;

use crate::subscription::PlanLedger;

use super::super::{
    clean, fusiform_price, utc_day, MultiplierError, Observation, PlanPriceUsed, ResolvedRates,
    RoleTokens, ERROR_DEFLATING,
};
use super::{PlannedPool, PoolDayCapture, PoolDiagnostic, PoolLogin, PoolModelIdentities};

const POOL_ERROR_INFLATING: &str = "Estimate, not a marginal tariff. Bounded boundary pairs are time-interpolated; unseen pre-reset burn and unrecorded traffic remain unknown.";

/// Assemble rows from the captured provider day. Use the same `plan.prices`
/// response (dated at the interval end) for pool coverage and monthly prices.
/// Assembly makes no store or catalog reads.
pub fn compute_pool_observations(
    capture: &PoolDayCapture,
    pools: &[PlannedPool<'_>],
    rates: &ResolvedRates,
    identities: &PoolModelIdentities,
    served: &PlanPricesResponse,
    snapshot_at_ms: i64,
) -> Result<Vec<Observation>, MultiplierError> {
    let start = capture.readings.interval_start_ms;
    let end = capture.readings.interval_end_ms;
    let day = utc_day(end)?;
    let mut rows = Vec::with_capacity(pools.len());
    for pool in pools {
        let mut prices = BTreeMap::new();
        for run in &pool.reading_runs {
            prices
                .entry(run.login)
                .or_insert_with(|| price_login(served, pool.plan, run.login, end));
        }
        let mut row = empty_row(
            pool,
            &day,
            (start, end),
            snapshot_at_ms,
            rates.resolved_at_ms,
        );
        row.plan_prices = prices.values().cloned().collect();
        if let Some(reason) = pool
            .diagnostic
            .filter(|reason| *reason != PoolDiagnostic::WindowTie)
        {
            row.eligibility_reasons.push(reason.code().into());
        } else {
            let selected = capture
                .select_calls_for_logins(pool, identities, |login| {
                    prices
                        .get(login)
                        .is_some_and(|price| price.monthly_nanos.is_some())
                })
                .ok_or_else(|| {
                    MultiplierError::InvalidObservation(
                        "a measured pool must have known coverage".into(),
                    )
                })?;
            row.pool_member_models = selected.pool_member_models;
            clean::measure_pool(
                &mut row,
                &pool.reading_runs,
                &prices,
                &selected.calls,
                rates,
            )?;
            if pool.diagnostic == Some(PoolDiagnostic::WindowTie) {
                row.multiplier = None;
                row.fee_multiplier = None;
                row.routing_eligible = false;
                row.eligibility_reasons = vec![PoolDiagnostic::WindowTie.code().into()];
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

fn price_login(
    served: &PlanPricesResponse,
    plan: &str,
    login: &PoolLogin,
    at_ms: i64,
) -> PlanPriceUsed {
    let mut used = PlanPriceUsed {
        account_ref: if login.ledger == PlanLedger::Account {
            login.id.clone()
        } else {
            String::new()
        },
        credential_ref: (login.ledger == PlanLedger::Credential).then(|| login.id.clone()),
        plan: plan.into(),
        source: None,
        monthly_nanos: None,
        review_by_ms: None,
        plan_price_overdue: false,
        source_ref: None,
        unpriceable_reason: None,
        refusal_text: None,
    };
    fusiform_price(served, plan, at_ms, &mut used);
    used
}

// Leave identity, dated facts and intervals populated even when the readings
// lack the scope or duration needed to compute the measurement fields.
pub(in crate::multiplier) fn empty_row(
    pool: &PlannedPool<'_>,
    day: &str,
    (start, end): (i64, i64),
    snapshot_at_ms: i64,
    catalog_resolved_at_ms: i64,
) -> Observation {
    Observation {
        plan: Some(pool.plan.into()),
        routing_eligible: false,
        eligibility_reasons: Vec::new(),
        clean_hours_share: 0.0,
        saturated_share: 0.0,
        gap_share: 0.0,
        unpriced_share: 0.0,
        unconfigured_accounts: Vec::new(),
        source_backlog: Vec::new(),
        unattributed_event_share: 0.0,
        zero_measurement_events: 0,
        missing_quantity_events: 0,
        quota_provider: pool.quota_provider.into(),
        observation_day: day.into(),
        snapshot_at_ms,
        interval_start_ms: start,
        interval_end_ms: end,
        token_interval_start_ms: Some(start),
        token_interval_end_ms: Some(end),
        window_pool_key: pool.window_pool_key.into(),
        window_minutes: pool.window_minutes,
        pool_scope: Some(pool.pool_scope),
        pool_models: pool.pool_models.map(<[String]>::to_vec),
        pool_models_at_ms: pool.pool_models_at_ms,
        pool_member_models: pool.pool_models.map(|_| Vec::new()),
        catalog_provider: pool.catalog_provider.map(str::to_owned),
        tokens: RoleTokens::default(),
        api_nanos: 0,
        api_nanos_by_model: Vec::new(),
        unpriced_tokens: Vec::new(),
        tokens_outside_quota_coverage: 0,
        base_rate_tier_untested_tokens: 0,
        subscription_nanos: 0,
        fee_paid_nanos: 0,
        delta_percent: 0.0,
        accounts_included: Vec::new(),
        accounts_excluded: Vec::new(),
        excluded_pairs: json!([]),
        low_resolution: true,
        scope_rules: Vec::new(),
        plan_prices: Vec::new(),
        catalog_resolved_at_ms,
        api_twins: BTreeMap::new(),
        reasoning_billed_as_output: Vec::new(),
        error_deflating: ERROR_DEFLATING.into(),
        error_inflating: POOL_ERROR_INFLATING.into(),
        multiplier: None,
        fee_multiplier: None,
    }
}
