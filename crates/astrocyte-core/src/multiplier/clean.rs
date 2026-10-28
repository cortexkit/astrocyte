//! Reset-aware, matched account-hour calibration. Do not pool readings across
//! plans: a point on one plan is not interchangeable with a point on another.
use super::*;

// Hourly coverage permits no sample gap above 15 minutes; longer gaps can hide
// resets. The October 2026 proportionality study used the same coverage bound.
const MAX_GAP_MS: i64 = 15 * 60_000;
pub(super) const ANTIGRAVITY_WRONG_VAULT_CUTOFF_NS: i64 = 1_792_523_280_000_000_000;
// Insula reset horizons jitter by less than a second (e.g. .7 versus .0).
const RESET_TOLERANCE_MS: i64 = 1_000;
// At least half of all account-hour buckets must be clean, with at least 24
// clean buckets total. One jointly clean hour is insufficient for calibration.
const MIN_CLEAN_SHARE: f64 = 0.5;
const MIN_CLEAN_HOURS: usize = 24;
// Codex loses 19% to saturation; Claude loses 40.9%. Permit the former, not
// the latter. Apply the same conservative budget to gaps and unpriced hours.
const MAX_EXCLUDED_SHARE: f64 = 0.25;
// A one-point endpoint error is at most 5% of twenty accumulated points.
const MIN_POINTS: f64 = 20.0;
// A positive quantity with unknown pricing may represent premium burn even
// with few tokens. No positive quantity is assumed free.
const MATERIAL_TOKENS: i64 = 1;
// Codex has 2.2% unmatched events. Do not invent a cross-plan allocation; allow
// at most 5% omitted events and disclose the omission even below the budget.
const MAX_UNATTRIBUTED_SHARE: f64 = 0.05;

#[derive(Debug, Clone, PartialEq, Default)]
pub(super) struct CounterHour {
    pub points: f64,
    pub covered_ms: i64,
    pub saturated: bool,
    pub reset_unknown: bool,
}

struct Sample {
    at: i64,
    used: f64,
    reset: Option<i64>,
}

pub(super) fn account_delta(
    conn: &Connection,
    provider: &str,
    account: &str,
    window: &WindowSelector,
    start: i64,
    end: i64,
) -> rusqlite::Result<AccountDelta> {
    let mut statement = conn.prepare_cached(
        "SELECT fetched_at_instant, COALESCE(raw_used_percent, used_percent), resets_at, source
         FROM window_sample WHERE provider=?1 AND account_ref=?2 AND pool_key=?3
         AND window_minutes=?4 AND fetched_at_instant>=?5 AND fetched_at_instant<=?6
         ORDER BY fetched_at_instant, rowid",
    )?;
    let rows = statement.query_map(
        params![
            provider,
            account,
            window.pool_key,
            window.window_minutes,
            clamp_nanos(millis_to_nanos(
                start.saturating_sub(window.window_minutes.saturating_mul(60_000))
            )),
            clamp_nanos(millis_to_nanos(end.saturating_add(MAX_GAP_MS)))
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        },
    )?;
    let mut samples = Vec::new();
    let mut latest_reset: Option<i64> = None;
    let mut stale_reset_readings = 0_u64;
    for row in rows {
        let (at_ns, used, reset, source) = row?;
        // Fixed historical exclusion, not a tuning knob: before 2026-10-20
        // 19:08Z Antigravity vault queried Google's wrong production pool.
        // QTA confirms that later vault and oauth readings share one counter.
        if provider == "antigravity"
            && source.as_deref() == Some("vault")
            && at_ns < ANTIGRAVITY_WRONG_VAULT_CUTOFF_NS
        {
            continue;
        }
        let reset = reset
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(&text).ok())
            .map(|at| at.timestamp_millis());
        if let (Some(latest), Some(reset)) = (latest_reset, reset) {
            // A cached reading from the previous allowance cannot raise the
            // current period's high-water mark or certify current coverage.
            if latest.saturating_sub(reset) > RESET_TOLERANCE_MS {
                if at_ns >= clamp_nanos(millis_to_nanos(start))
                    && at_ns < clamp_nanos(millis_to_nanos(end))
                {
                    stale_reset_readings += 1;
                }
                continue;
            }
        }
        if let Some(reset) = reset {
            latest_reset = Some(latest_reset.map_or(reset, |latest| latest.max(reset)));
        }
        samples.push(Sample {
            at: at_ns / NANOS_PER_MS,
            used,
            reset,
        });
    }
    let mut result = AccountDelta {
        account_ref: account.into(),
        delta_percent: 0.0,
        readings: 0,
        decrease_boundaries: 0,
        fetched_at_backwards: 0,
        stale_reset_readings,
        reset_boundaries: 0,
        coverage_ms: None,
        hours: BTreeMap::new(),
    };
    let mut high = samples.first().map_or(0.0, |sample| sample.used);
    let mut horizon = samples.first().and_then(|sample| sample.reset);
    for sample in &samples {
        if sample.at >= start && sample.at < end {
            result.readings += 1;
        }
    }
    for pair in samples.windows(2) {
        let (previous, current) = (&pair[0], &pair[1]);
        if current.at <= previous.at {
            continue;
        }
        let reset = match (horizon, current.reset) {
            (Some(old), Some(new)) => new.saturating_sub(old) > RESET_TOLERANCE_MS,
            _ => false,
        };
        if reset {
            high = 0.0;
            horizon = current.reset;
        }
        if horizon.is_none() {
            horizon = current.reset;
        }
        let rise = (current.used - high).max(0.0);
        high = high.max(current.used);
        if current.at >= start && current.at < end && current.used < previous.used {
            result.decrease_boundaries += 1;
        }
        // Allocate increases between adjacent readings to their overlapping
        // hourly buckets in proportion to elapsed time, assuming constant burn.
        // New-period first values belong to the later reading's timestamp.
        let from = previous.at.max(start);
        let to = current.at.min(end);
        if reset && current.at >= start && current.at < end {
            result.reset_boundaries += 1;
            result
                .hours
                .entry(current.at.div_euclid(HOUR_MS) * HOUR_MS)
                .or_default()
                .points += rise;
            result.delta_percent += rise;
        }
        if from >= to {
            continue;
        }
        let gap = current.at - previous.at;
        let mut at = from;
        while at < to {
            let hour = at.div_euclid(HOUR_MS) * HOUR_MS;
            let until = to.min(hour.saturating_add(HOUR_MS));
            let bucket = result.hours.entry(hour).or_default();
            if gap <= MAX_GAP_MS {
                bucket.covered_ms += until - at;
                bucket.saturated |= previous.used >= 100.0 || current.used >= 100.0;
                bucket.reset_unknown |= previous.reset.is_none() || current.reset.is_none();
            }
            let points = if reset {
                0.0
            } else {
                rise * (until - at) as f64 / gap as f64
            };
            bucket.points += points;
            result.delta_percent += points;
            at = until;
        }
    }
    if let (Some(first), Some(last)) = (samples.first(), samples.last()) {
        let from = first.at.max(start);
        let to = last.at.saturating_add(1).min(end);
        if from < to {
            result.coverage_ms = Some((from, to));
        }
    }
    Ok(result)
}

/// Prices only matched clean account-hours from the owned planning snapshot.
/// Unknown account plans invalidate every plan row, rather than silently
/// leaving their list value in the numerator with no denominator.
pub fn compute_plan_observations(
    conn: &Connection,
    config: &MultiplierConfig,
    plan: &ObservationPlan,
    rates: &ResolvedRates,
    prices: &BTreeMap<String, PlanPriceUsed>,
    snapshot_at_ms: i64,
    reasoning_billed_as_output: &[String],
) -> Result<Vec<Observation>, MultiplierError> {
    let plans: BTreeSet<_> = plan
        .provider
        .accounts
        .values()
        .map(|account| account.plan.clone())
        .collect();
    let lookup = ModelLookup::new(rates);
    let rules = ValuationRules::from_config(config, reasoning_billed_as_output);
    let mut bad_hours = BTreeSet::new();
    for event in &plan.events {
        let mut unpriced = BTreeMap::new();
        let resolution =
            event.tokens.model.as_deref().and_then(|model| {
                lookup.get(rules.catalog_provider(&event.tokens.provider), model)
            });
        value_event(
            &rules,
            resolution,
            &event.tokens,
            &mut BTreeMap::new(),
            &mut unpriced,
            &mut RoleTokens::default(),
        )?;
        let missing = unpriced
            .keys()
            .any(|(reason, _, _)| reason == "missing_quantity");
        let unknown = event.tokens.roles.is_empty() || missing;
        if unknown
            || unpriced
                .values()
                .any(|(tokens, _)| *tokens >= MATERIAL_TOKENS)
        {
            let hour = event.occurred_at_ms.div_euclid(HOUR_MS) * HOUR_MS;
            match event
                .account_ref
                .as_ref()
                .filter(|account| plan.provider.accounts.contains_key(*account))
            {
                Some(account) => {
                    bad_hours.insert((account.clone(), hour));
                }
                None => {
                    for account in plan.provider.accounts.keys() {
                        bad_hours.insert((account.clone(), hour));
                    }
                }
            }
        }
    }
    let first_hour = plan.interval_start_ms.div_euclid(HOUR_MS) * HOUR_MS;
    let hours: Vec<_> = (first_hour..plan.interval_end_ms)
        .step_by(usize::try_from(HOUR_MS).expect("hour fits usize"))
        .collect();
    let clean = |account: &str, hour: i64| {
        plan.accounts
            .iter()
            .find(|row| row.account_ref == account)
            .and_then(|row| row.hours.get(&hour))
            .is_some_and(|counter| {
                counter.covered_ms == HOUR_MS && !counter.saturated && !counter.reset_unknown
            })
            && !bad_hours.contains(&(account.to_string(), hour))
    };
    let mut output = Vec::new();
    for name in &plans {
        let members: BTreeSet<_> = plan
            .provider
            .accounts
            .iter()
            .filter(|(_, account)| &account.plan == name)
            .map(|(account, _)| account.clone())
            .collect();
        let mut filtered = plan.clone();
        filtered
            .accounts
            .retain(|account| members.contains(&account.account_ref));
        filtered
            .provider
            .accounts
            .retain(|account, _| members.contains(account));
        let mut clean_count = 0_usize;
        let mut saturated = 0_usize;
        let mut gaps = 0_usize;
        let mut unpriced = 0_usize;
        let mut unknown_reset = false;
        for account in &mut filtered.accounts {
            account.delta_percent = 0.0;
            for hour in &hours {
                let counter = account.hours.get(hour);
                saturated += usize::from(counter.is_some_and(|counter| counter.saturated));
                gaps += usize::from(counter.is_none_or(|counter| counter.covered_ms != HOUR_MS));
                unknown_reset |= counter.is_some_and(|counter| counter.reset_unknown);
                unpriced += usize::from(bad_hours.contains(&(account.account_ref.clone(), *hour)));
                if clean(&account.account_ref, *hour) {
                    clean_count += 1;
                    account.delta_percent += counter.map_or(0.0, |counter| counter.points);
                }
            }
        }
        let mut unmatched = 0_usize;
        filtered.events.retain(|event| {
            let hour = event.occurred_at_ms.div_euclid(HOUR_MS) * HOUR_MS;
            match event
                .account_ref
                .as_ref()
                .filter(|account| plan.provider.accounts.contains_key(*account))
            {
                Some(account) => members.contains(account) && clean(account, hour),
                None => {
                    unmatched += 1;
                    plans.len() == 1
                        && plan
                            .provider
                            .accounts
                            .keys()
                            .all(|account| clean(account, hour))
                }
            }
        });
        filtered.token_interval_ms = Some((plan.interval_start_ms, plan.interval_end_ms));
        let plan_prices: BTreeMap<_, _> = prices
            .iter()
            .filter(|(account, _)| members.contains(*account))
            .map(|(account, price)| (account.clone(), price.clone()))
            .collect();
        let mut row = compute_observation(
            conn,
            config,
            &filtered,
            rates,
            &plan_prices,
            snapshot_at_ms,
            reasoning_billed_as_output,
        )?;
        // Preserve diagnostics for excluded hours too. A clean Spark estimate
        // must not make an unpriced premium model disappear from the report.
        let mut diagnostics = BTreeMap::new();
        let mut zero_measurements = 0_u64;
        let mut missing_quantities = 0_u64;
        for event in plan.events.iter().filter(|event| {
            event.account_ref.as_ref().is_none_or(|account| {
                !plan.provider.accounts.contains_key(account) || members.contains(account)
            })
        }) {
            zero_measurements += u64::from(event.tokens.roles.is_empty());
            missing_quantities += u64::from(
                event
                    .tokens
                    .roles
                    .iter()
                    .any(|(_, _, _, quantity, exponent)| quantity.is_none() || exponent.is_none()),
            );
            let resolution = event.tokens.model.as_deref().and_then(|model| {
                lookup.get(rules.catalog_provider(&event.tokens.provider), model)
            });
            value_event(
                &rules,
                resolution,
                &event.tokens,
                &mut BTreeMap::new(),
                &mut diagnostics,
                &mut RoleTokens::default(),
            )?;
        }
        let unknown_quantity = missing_quantities > 0 || zero_measurements > 0;
        row.unpriced_tokens = diagnostics
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
            .collect();
        row.plan = Some(name.clone());
        row.zero_measurement_events = zero_measurements;
        row.missing_quantity_events = missing_quantities;
        row.unattributed_event_share = unmatched as f64 / plan.events.len().max(1) as f64;
        let total = (members.len() * hours.len()).max(1) as f64;
        row.clean_hours_share = clean_count as f64 / total;
        row.saturated_share = saturated as f64 / total;
        row.gap_share = gaps as f64 / total;
        row.unpriced_share = unpriced as f64 / total;
        row.eligibility_reasons.clear();
        let unresolved_price = row
            .accounts_excluded
            .iter()
            .any(|account| account["reason"] != "account_not_configured");
        for (bad, reason) in [
            (
                !row.unconfigured_accounts.is_empty(),
                "unconfigured_accounts",
            ),
            (unresolved_price, "unpriced_plan"),
            (
                row.clean_hours_share < MIN_CLEAN_SHARE || clean_count < MIN_CLEAN_HOURS,
                "insufficient_clean_hours",
            ),
            (row.saturated_share > MAX_EXCLUDED_SHARE, "saturation"),
            (row.gap_share > MAX_EXCLUDED_SHARE, "gaps"),
            (row.unpriced_share > MAX_EXCLUDED_SHARE, "unpriced_usage"),
            (missing_quantities > 0, "missing_quantity"),
            (zero_measurements > 0, "missing_measurements"),
            (unknown_reset, "unknown_reset"),
            (
                plans.len() > 1
                    && unmatched as f64 / plan.events.len().max(1) as f64 > MAX_UNATTRIBUTED_SHARE,
                "unattributed_plan_usage",
            ),
            (row.delta_percent < MIN_POINTS, "insufficient_points"),
            (
                row.multiplier
                    .is_none_or(|ratio| !ratio.is_finite() || ratio <= 0.0),
                "nonpositive_ratio",
            ),
        ] {
            if bad {
                row.eligibility_reasons.push(reason.into());
            }
        }
        if unknown_quantity
            || unresolved_price
            || !row.unconfigured_accounts.is_empty()
            || (row.api_nanos == 0
                && row
                    .unpriced_tokens
                    .iter()
                    .any(|unpriced| unpriced.tokens > 0))
        {
            row.multiplier = None;
            row.fee_multiplier = None;
        }
        row.routing_eligible = row.eligibility_reasons.is_empty();
        row.error_inflating = "Estimate, not a marginal tariff. Bounded boundary pairs are time-interpolated; unseen pre-reset burn and unrecorded traffic remain unknown. Unattributed usage is omitted when plans differ.".into();
        output.push(row);
    }
    Ok(output)
}
