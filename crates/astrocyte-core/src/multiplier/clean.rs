//! Reset-aware, matched account-hour calibration. Do not pool readings across
//! plans: a point on one plan is not interchangeable with a point on another.
use crate::subscription::PlanLedger;

use super::calibration::{CalibrationBin, MIN_ACTIVE_DAYS};
use super::measurement::{
    measure_quota, measure_usage, record_unpriced, unpriced_tokens, CounterFee,
};
use super::pools::{PoolCall, PoolLogin, PoolReadingRun};
use super::*;

// The ten-minute backstop leaves five minutes of margin for changed-counter
// pairs. Equal values in the same known period can bridge longer quiet gaps.
pub(super) const MAX_GAP_MS: i64 = 15 * 60_000;
pub(super) const ANTIGRAVITY_WRONG_VAULT_CUTOFF_NS: i64 = 1_792_523_280_000_000_000;
// On one development store in October 2026, within-period reset times varied
// by up to 312 seconds; separate periods were at least 85,737 seconds apart.
// Ten minutes admits that jitter without inventing a rollover or stale cache.
pub(crate) const RESET_TOLERANCE_MS: i64 = 10 * 60_000;
// A full day of matched account-hours and twenty points are the minimum sample;
// excluded exposure is reported, not used as an unsupported share floor.
pub(super) const MIN_CLEAN_HOURS: usize = 24;
// On one development store in October 2026, saturation occupied 19% of Codex
// account-time and 40.9% of Claude account-time. The 25% veto limits selected
// heavy-use exposure; other exclusions use sample adequacy and stability.
pub(super) const MAX_SATURATED_SHARE: f64 = 0.25;
// A one-point endpoint error is at most 5% of twenty accumulated points.
pub(super) const MIN_POINTS: f64 = 20.0;
// A positive quantity with unknown pricing may represent premium burn even
// with few tokens. No positive quantity is assumed free.
pub(super) const MATERIAL_TOKENS: i64 = 1;

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

fn quiet_pair(previous: &Sample, current: &Sample, tolerance: i64) -> bool {
    previous.used.total_cmp(&current.used).is_eq()
        && matches!((previous.reset, current.reset), (Some(old), Some(new))
            if (i128::from(new) - i128::from(old)).abs() <= i128::from(tolerance))
}

/// How far a window's reset time must move forward before the readings belong
/// to a new allowance period. Quota history uses the same rule, so a chart's
/// reset marks agree with the periods the multiplier measures.
pub(crate) fn reset_tolerance_ms(window_minutes: i64) -> i64 {
    // A genuinely short window still needs distinct periods: the tolerance
    // cannot merge a rollover of a one-minute pool, for example.
    RESET_TOLERANCE_MS.min(window_minutes.saturating_mul(60_000) / 2)
}

// Six-hour bins reduce sensitivity to integer-point rounding and the mismatch
// between quota-consumption times and completed-run timestamps in hourly bins.
const CALIBRATION_BIN_MS: i64 = 6 * HOUR_MS;

fn calibration_bin(bins: &mut BTreeMap<i64, CalibrationBin>, at_ms: i64) -> &mut CalibrationBin {
    let start_ms = at_ms.div_euclid(CALIBRATION_BIN_MS) * CALIBRATION_BIN_MS;
    bins.entry(start_ms).or_insert(CalibrationBin {
        start_ms,
        ..CalibrationBin::default()
    })
}

pub(super) fn historical_sample_excluded(provider: &str, source: Option<&str>, at_ns: i64) -> bool {
    // Before 2026-10-20 19:08Z Antigravity vault queried Google's wrong pool.
    // Later vault and oauth readings sample the same counter.
    provider == "antigravity"
        && source == Some("vault")
        && at_ns < ANTIGRAVITY_WRONG_VAULT_CUTOFF_NS
}

/// Measure one continuous run. A change of plan ends the counter series, so
/// neither its high-water mark nor its reset horizon carries into a later run.
pub(super) fn measure_run<'a>(
    login: &str,
    readings: impl IntoIterator<Item = (i64, f64, Option<&'a str>)>,
    tolerance: i64,
    (start, end): (i64, i64),
) -> AccountDelta {
    let mut samples = Vec::new();
    let mut latest_reset: Option<i64> = None;
    let mut stale_reset_readings = 0_u64;
    for (at_ns, used, reset) in readings {
        let reset = reset
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
            .map(|at| at.timestamp_millis());
        if let (Some(latest), Some(reset)) = (latest_reset, reset) {
            // A cached reading from the previous allowance cannot raise the
            // current period's high-water mark or certify current coverage.
            if latest.saturating_sub(reset) > tolerance {
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
        account_ref: login.into(),
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
            result.coverage_ms = Some(match result.coverage_ms {
                Some((from, to)) => (
                    from.min(sample.at),
                    to.max(sample.at.saturating_add(1).min(end)),
                ),
                None => (sample.at, sample.at.saturating_add(1).min(end)),
            });
        }
    }
    for pair in samples.windows(2) {
        let (previous, current) = (&pair[0], &pair[1]);
        if current.at <= previous.at {
            continue;
        }
        let reset = match (horizon, current.reset) {
            (Some(old), Some(new)) => new.saturating_sub(old) > tolerance,
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
        // A reading past its own reset cannot certify coverage for that period.
        let covered = current.reset.is_none_or(|at| current.at <= at)
            && (reset || previous.reset.is_none_or(|at| previous.at <= at))
            && (gap <= MAX_GAP_MS || (!reset && quiet_pair(previous, current, tolerance)));
        if covered {
            result.coverage_ms = Some(match result.coverage_ms {
                Some((old_from, old_to)) => (old_from.min(from), old_to.max(to)),
                None => (from, to),
            });
        }
        let mut at = from;
        while at < to {
            let hour = at.div_euclid(HOUR_MS) * HOUR_MS;
            let until = to.min(hour.saturating_add(HOUR_MS));
            let bucket = result.hours.entry(hour).or_default();
            if covered {
                bucket.covered_ms += until - at;
                bucket.saturated |= previous.used >= 100.0 || current.used >= 100.0;
                bucket.reset_unknown |= previous.reset.is_none() || current.reset.is_none();
            }
            let points = if reset || !covered {
                0.0
            } else {
                rise * (until - at) as f64 / gap as f64
            };
            bucket.points += points;
            result.delta_percent += points;
            at = until;
        }
    }
    result
}

struct PoolCounter {
    delta: AccountDelta,
    segments: u64,
}

impl PoolCounter {
    fn add_run(&mut self, run: AccountDelta) {
        self.segments += run.segments();
        let delta = &mut self.delta;
        delta.delta_percent += run.delta_percent;
        delta.readings += run.readings;
        delta.decrease_boundaries += run.decrease_boundaries;
        delta.fetched_at_backwards += run.fetched_at_backwards;
        delta.stale_reset_readings += run.stale_reset_readings;
        delta.reset_boundaries += run.reset_boundaries;
        if let Some((from, to)) = run.coverage_ms {
            delta.coverage_ms = Some(delta.coverage_ms.map_or((from, to), |(old_from, old_to)| {
                (old_from.min(from), old_to.max(to))
            }));
        }
        for (hour, next) in run.hours {
            let counter = delta.hours.entry(hour).or_default();
            counter.points += next.points;
            counter.covered_ms += next.covered_ms;
            counter.saturated |= next.saturated;
            counter.reset_unknown |= next.reset_unknown;
        }
    }
}

#[derive(Default)]
struct Quality {
    clean_count: usize,
    saturated: usize,
    gaps: usize,
    unpriced_hours: usize,
    unknown_reset: bool,
}

/// Gate and value calls already narrowed to this plan, pool and the logins
/// with priced counters. Other plans or pools cannot change its calibration.
pub(super) fn measure_pool(
    row: &mut Observation,
    reading_runs: &[PoolReadingRun<'_>],
    prices: &BTreeMap<&PoolLogin, PlanPriceUsed>,
    calls: &[&PoolCall],
    rates: &ResolvedRates,
) -> Result<(), MultiplierError> {
    let interval = (row.interval_start_ms, row.interval_end_ms);
    let lookup = ModelLookup::new(rates);
    let mut bad_hours = BTreeSet::new();
    let mut diagnostics = BTreeMap::new();
    let mut numerators = Vec::with_capacity(calls.len());
    let mut fee_calls = Vec::new();
    for call in calls {
        let event = &call.tokens;
        let resolution = event
            .model
            .as_deref()
            .and_then(|model| lookup.get_at(&event.provider, model, call.occurred_at_ms));
        let value = crate::valuation::value_event(resolution, event)?;
        let zero_measurements = event.roles.is_empty();
        let missing_quantity = value
            .unpriced
            .iter()
            .any(|token| token.reason == "missing_quantity");
        row.zero_measurement_events += u64::from(zero_measurements);
        row.missing_quantity_events += u64::from(
            event
                .roles
                .iter()
                .any(|(_, _, _, quantity, exponent)| quantity.is_none() || exponent.is_none()),
        );
        if zero_measurements
            || missing_quantity
            || value
                .unpriced
                .iter()
                .any(|token| token.quantity >= MATERIAL_TOKENS)
        {
            // Each call is keyed by its account or, when it has no account,
            // its paying credential. A bad hour vetoes only that login.
            bad_hours.insert((
                &call.login,
                call.occurred_at_ms.div_euclid(HOUR_MS) * HOUR_MS,
            ));
        }
        if !zero_measurements
            && event
                .roles
                .iter()
                .all(|(_, _, _, quantity, exponent)| quantity.is_some() && exponent.is_some())
        {
            fee_calls.push((event, call.occurred_at_ms));
        }
        numerators.push(value.numerator);
        record_unpriced(event, value.unpriced, &mut diagnostics)?;
    }

    let mut counters = BTreeMap::<_, PoolCounter>::new();
    for run in reading_runs {
        let delta = measure_run(
            &run.login.id,
            run.readings.iter().map(|r| {
                (
                    r.fetched_at_instant_ns,
                    r.used_percent,
                    r.resets_at.as_deref(),
                )
            }),
            reset_tolerance_ms(row.window_minutes),
            interval,
        );
        match counters.entry(run.login) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().add_run(delta)
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PoolCounter {
                    segments: delta.segments(),
                    delta,
                });
            }
        }
    }

    let mut quality = Quality::default();
    let mut bins = BTreeMap::new();
    let mut clean_hours = BTreeSet::new();
    let hours: Vec<_> = (interval.0.div_euclid(HOUR_MS) * HOUR_MS..interval.1)
        .step_by(usize::try_from(HOUR_MS).expect("hour fits usize"))
        .collect();
    let mut priced_count = 0;
    for (&login, counter) in &mut counters {
        if prices[login].monthly_nanos.is_none() {
            continue;
        }
        priced_count += 1;
        counter.delta.delta_percent = 0.0;
        for &hour in &hours {
            let sample = counter.delta.hours.get(&hour);
            let bad = bad_hours.contains(&(login, hour));
            quality.unpriced_hours += usize::from(bad);
            quality.gaps += usize::from(sample.is_none_or(|sample| sample.covered_ms != HOUR_MS));
            if let Some(sample) = sample {
                quality.saturated += usize::from(sample.saturated);
                quality.unknown_reset |= sample.reset_unknown;
                if !bad
                    && sample.covered_ms == HOUR_MS
                    && !sample.saturated
                    && !sample.reset_unknown
                {
                    clean_hours.insert((login, hour));
                    quality.clean_count += 1;
                    counter.delta.delta_percent += sample.points;
                    let bin = calibration_bin(&mut bins, hour);
                    bin.clean_account_hours += 1;
                    bin.points += sample.points;
                }
            }
        }
    }

    let mut clean_calls = Vec::new();
    for (call, numerator) in calls.iter().zip(numerators) {
        let hour = call.occurred_at_ms.div_euclid(HOUR_MS) * HOUR_MS;
        // Each call already names its login, so multiple credentials do not
        // prevent a call from matching one counter.
        if clean_hours.contains(&(&call.login, hour)) {
            let bin = calibration_bin(&mut bins, hour);
            bin.api_numerator = bin
                .api_numerator
                .checked_add(numerator)
                .ok_or(MoneyError::OutOfRange)?;
            clean_calls.push((&call.tokens, call.occurred_at_ms));
        }
    }
    let token_interval = Some(interval);
    let usage = measure_usage(clean_calls, token_interval, &lookup)?;
    let quota = measure_quota(
        row.window_minutes,
        token_interval,
        counters.iter().map(|(&login, counter)| CounterFee {
            counter: &counter.delta,
            identity_field: match login.ledger {
                PlanLedger::Account => "account_ref",
                PlanLedger::Credential => "credential_ref",
            },
            price: prices.get(login),
            segments: counter.segments,
        }),
    )?;
    row.multiplier = usage.ratio(quota.subscription_nanos);
    row.fee_multiplier =
        measure_usage(fee_calls, token_interval, &lookup)?.ratio(quota.fee_paid_nanos);
    row.tokens = usage.tokens;
    row.api_nanos = usage.api_nanos;
    row.api_nanos_by_model = usage.api_nanos_by_model;
    row.base_rate_tier_untested_tokens = usage.base_rate_tier_untested_tokens;
    row.tokens_outside_quota_coverage = usage.tokens_outside_quota_coverage;
    row.unpriced_tokens = unpriced_tokens(diagnostics);
    row.subscription_nanos = quota.subscription_nanos;
    row.fee_paid_nanos = quota.fee_paid_nanos;
    row.delta_percent = quota.delta_percent;
    row.accounts_included = quota.accounts_included;
    row.accounts_excluded = quota.accounts_excluded;
    row.excluded_pairs = quota.excluded_pairs;
    row.low_resolution = quota.low_resolution;

    let bins: Vec<_> = bins.into_values().collect();
    let grain = if priced_count == 1 {
        "single_account_scope_six_hour"
    } else {
        "provider_plan_six_hour"
    };
    let stability = super::calibration::assess(&bins, grain);
    let calibration = json!({"summary": &stability, "bins": &bins});
    for login in &mut row.accounts_included {
        login["plan_calibration"] = calibration.clone();
    }
    let total = (priced_count * hours.len()).max(1) as f64;
    row.clean_hours_share = quality.clean_count as f64 / total;
    row.saturated_share = quality.saturated as f64 / total;
    row.gap_share = quality.gaps as f64 / total;
    row.unpriced_share = quality.unpriced_hours as f64 / total;
    let unresolved_price = !row.accounts_excluded.is_empty();
    for (bad, reason) in [
        (unresolved_price, "unpriced_plan"),
        (
            quality.clean_count < MIN_CLEAN_HOURS,
            "insufficient_clean_hours",
        ),
        (row.saturated_share > MAX_SATURATED_SHARE, "saturation"),
        (quality.unknown_reset, "unknown_reset"),
        (row.delta_percent < MIN_POINTS, "insufficient_points"),
        (
            stability.active_days < MIN_ACTIVE_DAYS,
            "insufficient_stability_days",
        ),
        (
            stability.active_days >= MIN_ACTIVE_DAYS && !stability.passes_width_gate(),
            "unstable_estimate",
        ),
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
    if unresolved_price {
        row.multiplier = None;
        row.fee_multiplier = None;
    }
    // No clean hours means no measured quota, not zero usage.
    if quality.clean_count == 0
        || (row.api_nanos == 0
            && row
                .unpriced_tokens
                .iter()
                .any(|token| token.tokens > 0 && token.reason != "missing_quantity"))
    {
        row.multiplier = None;
    }
    row.routing_eligible = row.eligibility_reasons.is_empty();
    Ok(())
}
