//! Conditional stability of matched list value and quota points. UTC days,
//! not individual hours or accounts, are the resampling units.
use super::*;

// The proportionality study used 300 UTC-day cluster resamples. Keep the seed
// fixed so historical recomputes and Windows/Linux reports are reproducible.
const BOOTSTRAP_RESAMPLES: usize = 300;
const BOOTSTRAP_SEED: u64 = 20_261_028;
// At least two training days must remain when one UTC day is held out.
pub(super) const MIN_ACTIVE_DAYS: usize = 3;
// The October 2026 matched pro_200 cohort has about 10–15% conditional interval
// widths, versus about 62–100% for the latest pro_100/Claude/Muse cohorts. A 50% span is an
// explicit uncertainty budget, not a fitted provider tariff or a share floor.
pub(super) const MAX_RELATIVE_WIDTH: f64 = 0.5;
const DAY_MS: i64 = 24 * HOUR_MS;
const NUMERATOR_PER_DOLLAR: f64 = 1_000_000_000_000_000.0;

#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct CalibrationBin {
    pub start_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_slot: Option<usize>,
    pub clean_account_hours: u64,
    pub points: f64,
    // Persist the exact rate-times-token numerator as a string: a JSON integer
    // need not fit i64, and statistical bins must not change row money rounding.
    #[serde(serialize_with = "serialize_numerator")]
    pub api_numerator: i128,
}

fn serialize_numerator<S: serde::Serializer>(
    value: &i128,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct Stability {
    pub method: &'static str,
    pub validation_grain: &'static str,
    pub resamples: usize,
    pub seed: u64,
    pub minimum_active_days: usize,
    pub maximum_relative_width: f64,
    pub active_days: usize,
    pub active_bins: usize,
    pub clean_account_hours: u64,
    pub points: f64,
    pub dollars_per_point: Option<f64>,
    pub bootstrap_95_low: Option<f64>,
    pub bootstrap_95_high: Option<f64>,
    pub bootstrap_relative_width: Option<f64>,
    pub held_out_ratio_rmse_points: Option<f64>,
    pub held_out_ratio_relative_rmse: Option<f64>,
    pub held_out_ratio_r2: Option<f64>,
    pub held_out_ols_rmse_points: Option<f64>,
    pub held_out_test_bins: usize,
    pub held_out_invalid_folds: usize,
}

impl Stability {
    pub fn passes_width_gate(&self) -> bool {
        self.bootstrap_relative_width
            .is_some_and(|width| width <= MAX_RELATIVE_WIDTH)
    }
}

#[derive(Clone, Copy, Default)]
struct Day {
    dollars: f64,
    points: f64,
}

fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn percentile(samples: &[f64], numerator: usize) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    // 2.5% = 1/40; 97.5% = 39/40. Integer ranks avoid lossy index casts.
    let rank = (samples.len() - 1) * numerator;
    let lower = rank / 40;
    let upper = (lower + 1).min(samples.len() - 1);
    if !samples[lower].is_finite() || !samples[upper].is_finite() {
        return None;
    }
    finite(samples[lower] + (samples[upper] - samples[lower]) * (rank % 40) as f64 / 40.0)
}

fn random_index(state: &mut u64, length: usize) -> usize {
    // SplitMix64 is only a deterministic resampling stream, not a security RNG.
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut bits = *state;
    bits = (bits ^ (bits >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    bits = (bits ^ (bits >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    bits ^= bits >> 31;
    let length = u64::try_from(length).expect("day count fits u64");
    usize::try_from(bits % length).expect("resampled index is below the slice length")
}

pub(super) fn assess(bins: &[CalibrationBin], grain: &'static str) -> Stability {
    let mut days: BTreeMap<i64, Day> = BTreeMap::new();
    let mut active = Vec::new();
    let mut clean_hours = 0_u64;
    for bin in bins {
        clean_hours = clean_hours.saturating_add(bin.clean_account_hours);
        if bin.points <= 0.0 && bin.api_numerator == 0 {
            continue;
        }
        let dollars = bin.api_numerator as f64 / NUMERATOR_PER_DOLLAR;
        let day = days.entry(bin.start_ms.div_euclid(DAY_MS)).or_default();
        day.dollars += dollars;
        day.points += bin.points;
        active.push((bin.start_ms.div_euclid(DAY_MS), dollars, bin.points));
    }
    let total = days.values().fold(Day::default(), |sum, day| Day {
        dollars: sum.dollars + day.dollars,
        points: sum.points + day.points,
    });
    let center = (total.points > 0.0)
        .then(|| total.dollars / total.points)
        .and_then(finite);
    let mut result = Stability {
        method: "utc_day_block_bootstrap_v1",
        validation_grain: grain,
        resamples: BOOTSTRAP_RESAMPLES,
        seed: BOOTSTRAP_SEED,
        minimum_active_days: MIN_ACTIVE_DAYS,
        maximum_relative_width: MAX_RELATIVE_WIDTH,
        active_days: days.len(),
        active_bins: active.len(),
        clean_account_hours: clean_hours,
        points: total.points,
        dollars_per_point: center,
        bootstrap_95_low: None,
        bootstrap_95_high: None,
        bootstrap_relative_width: None,
        held_out_ratio_rmse_points: None,
        held_out_ratio_relative_rmse: None,
        held_out_ratio_r2: None,
        held_out_ols_rmse_points: None,
        held_out_test_bins: 0,
        held_out_invalid_folds: 0,
    };
    if !days.is_empty() {
        let blocks: Vec<_> = days.values().collect();
        let mut state = BOOTSTRAP_SEED;
        let mut sampled = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
        for _ in 0..BOOTSTRAP_RESAMPLES {
            let mut sum = Day::default();
            for _ in 0..blocks.len() {
                let day = blocks[random_index(&mut state, blocks.len())];
                sum.dollars += day.dollars;
                sum.points += day.points;
            }
            sampled.push(if sum.points > 0.0 {
                sum.dollars / sum.points
            } else {
                f64::INFINITY
            });
        }
        sampled.sort_by(f64::total_cmp);
        result.bootstrap_95_low = percentile(&sampled, 1);
        result.bootstrap_95_high = percentile(&sampled, 39);
        if let (Some(low), Some(high), Some(center)) =
            (result.bootstrap_95_low, result.bootstrap_95_high, center)
        {
            if low > 0.0 && center > 0.0 {
                result.bootstrap_relative_width = finite((high - low) / center);
            }
        }
    }
    let mut squared_error = 0.0;
    let mut ols_squared_error = 0.0;
    let mut observed = Vec::new();
    for (held_day, held) in &days {
        let training = Day {
            dollars: total.dollars - held.dollars,
            points: total.points - held.points,
        };
        if training.dollars <= 0.0 || training.points <= 0.0 {
            result.held_out_invalid_folds += 1;
            continue;
        }
        let ratio = training.points / training.dollars;
        let (mut cross, mut squared) = (0.0, 0.0);
        for (day, dollars, points) in &active {
            if day != held_day {
                cross += dollars * points;
                squared += dollars * dollars;
            }
        }
        let ols = if squared > 0.0 {
            cross / squared
        } else {
            ratio
        };
        for (day, dollars, points) in &active {
            if day == held_day {
                squared_error += (points - ratio * dollars).powi(2);
                ols_squared_error += (points - ols * dollars).powi(2);
                observed.push(*points);
            }
        }
    }
    result.held_out_test_bins = observed.len();
    if !observed.is_empty() {
        let n = observed.len() as f64;
        let mean = observed.iter().sum::<f64>() / n;
        let variation = observed
            .iter()
            .map(|points| (points - mean).powi(2))
            .sum::<f64>();
        result.held_out_ratio_rmse_points = finite((squared_error / n).sqrt());
        result.held_out_ols_rmse_points = finite((ols_squared_error / n).sqrt());
        if mean > 0.0 {
            result.held_out_ratio_relative_rmse = finite((squared_error / n).sqrt() / mean);
        }
        if variation > 0.0 {
            result.held_out_ratio_r2 = finite(1.0 - squared_error / variation);
        }
    }
    result
}
