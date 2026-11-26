//! Calendar fees and exact per-record allocation, independent of report grouping.
use super::{PlanBook, PlanMember};
use crate::report::ReportError;
use crate::subscription::recurring_cost_nanos;
use chrono::{Datelike, NaiveDate, Utc};
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{Signed, ToPrimitive, Zero};
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub const DAY_MS: i64 = 86_400_000;

/// Exact nanodollars, or an exact dimensionless allocation coefficient. Day
/// weights can have coprime denominators, so multi-day fee sums use unbounded
/// integers until the final conversion to nanodollars.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fraction(BigRational);

impl Default for Fraction {
    fn default() -> Self {
        Self(BigRational::zero())
    }
}

impl Fraction {
    pub fn new(numerator: i128, denominator: i128) -> Result<Self, ReportError> {
        if denominator <= 0 {
            return Err(ReportError::Arithmetic);
        }
        Ok(Self(BigRational::new(numerator.into(), denominator.into())))
    }

    pub fn numerator(&self) -> &BigInt {
        self.0.numer()
    }

    pub fn denominator(&self) -> &BigInt {
        self.0.denom()
    }

    pub fn checked_add(&self, other: &Self) -> Result<Self, ReportError> {
        Ok(Self(&self.0 + &other.0))
    }

    pub fn checked_mul(&self, other: &Self) -> Result<Self, ReportError> {
        Ok(Self(&self.0 * &other.0))
    }

    pub fn round(&self) -> Result<i64, ReportError> {
        let mut whole = self.numerator() / self.denominator();
        let rem = self.numerator() % self.denominator();
        let double = rem.abs() * 2_u8;
        if double > *self.denominator()
            || (double == *self.denominator() && !(&whole % 2_u8).is_zero())
        {
            whole += self.numerator().signum();
        }
        whole.to_i64().ok_or(ReportError::Arithmetic)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolDay {
    pub day_start_ms: i64,
    pub pool: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DayMember {
    pub day_start_ms: i64,
    pub member: PlanMember,
}

impl DayMember {
    fn pool_day(&self) -> PoolDay {
        PoolDay {
            day_start_ms: self.day_start_ms,
            pool: self.member.plan.clone(),
        }
    }
}

/// Exact list-value numerators in the valuer's common scale. Include zero-weight
/// entries for plan calls too: they establish which pending days touch a row.
#[derive(Clone, Debug, Default)]
pub struct Weights(pub BTreeMap<DayMember, i128>);

impl Weights {
    pub fn checked_add(&mut self, key: DayMember, numerator: i128) -> Result<(), ReportError> {
        if key.day_start_ms.rem_euclid(DAY_MS) != 0 || numerator < 0 {
            return Err(ReportError::Arithmetic);
        }
        let value = self.0.entry(key).or_default();
        *value = value
            .checked_add(numerator)
            .ok_or(ReportError::Arithmetic)?;
        Ok(())
    }
}

/// The reader supplies weights over the **whole UTC day, unfiltered**, using
/// current valuation cells or per-call reduction of summary-dirty hours with
/// the same cached rates and plan resolver. Fallback weights do not override
/// currentness: every pool/day with a non-current hour holding pool calls must
/// appear in `not_current`. A pending day contributes neither spread nor
/// unspread fees.
#[derive(Clone, Debug, Default)]
pub struct DayWeights {
    pub weights: Weights,
    pub not_current: BTreeSet<PoolDay>,
}

#[derive(Clone, Debug)]
pub struct FeeDay {
    pub key: DayMember,
    pub fee: Fraction,
    active_from_ms: i64,
    active_to_ms: i64,
}

#[derive(Clone, Debug)]
pub struct FeeDays {
    pub start_ms: i64,
    pub end_ms: i64,
    pub days: Vec<FeeDay>,
    /// Distinct overlapping records, including records with no ledger calls.
    pub non_usd: BTreeSet<PlanMember>,
}

fn period_days(day: i64, period: &str) -> Result<i64, ReportError> {
    let date = chrono::DateTime::<Utc>::from_timestamp_millis(day)
        .ok_or(ReportError::Arithmetic)?
        .date_naive();
    let (start, end) = match period {
        "month" => {
            let start = date.with_day(1).ok_or(ReportError::Arithmetic)?;
            let (year, month) = if date.month() == 12 {
                (date.year() + 1, 1)
            } else {
                (date.year(), date.month() + 1)
            };
            (
                start,
                NaiveDate::from_ymd_opt(year, month, 1).ok_or(ReportError::Arithmetic)?,
            )
        }
        "year" => (
            NaiveDate::from_ymd_opt(date.year(), 1, 1).ok_or(ReportError::Arithmetic)?,
            NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).ok_or(ReportError::Arithmetic)?,
        ),
        _ => return Err(ReportError::Integrity("invalid subscription period")),
    };
    Ok((end - start).num_days())
}

impl FeeDays {
    /// Read a single snapshot with the valuation reader. Fees come only from the
    /// two subscription ledgers, never multiplier observations or call cells.
    /// Each day retains its full fee; report-window overlap is applied only to
    /// unspread fees, not to the full-day allocation denominators.
    pub fn load(conn: &Connection, start_ms: i64, end_ms: i64) -> Result<Self, ReportError> {
        Self::from_book(&PlanBook::load(conn)?, start_ms, end_ms)
    }

    pub(super) fn from_book(
        book: &PlanBook,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Self, ReportError> {
        if start_ms > end_ms {
            return Err(ReportError::Arithmetic);
        }
        if start_ms == end_ms {
            return Ok(Self {
                start_ms,
                end_ms,
                days: Vec::new(),
                non_usd: BTreeSet::new(),
            });
        }
        let first_day = start_ms
            .div_euclid(DAY_MS)
            .checked_mul(DAY_MS)
            .ok_or(ReportError::Arithmetic)?;
        let last_day = (end_ms - 1)
            .div_euclid(DAY_MS)
            .checked_mul(DAY_MS)
            .ok_or(ReportError::Arithmetic)?;
        let day_end = last_day
            .checked_add(DAY_MS)
            .ok_or(ReportError::Arithmetic)?;
        let mut result = Self {
            start_ms,
            end_ms,
            days: Vec::new(),
            non_usd: BTreeSet::new(),
        };
        for record in &book.records {
            let (from, to, minor, exponent, currency, period) = (
                record.id.effective_from_ms,
                record.until_ms,
                record.minor,
                record.exponent,
                &record.currency,
                &record.period,
            );
            if from >= day_end || to.is_some_and(|to| to <= first_day) {
                continue;
            }
            let member = record.member();
            if currency != "USD" {
                if from < end_ms && to.is_none_or(|to| to > start_ms) {
                    result.non_usd.insert(member);
                }
                continue;
            }
            let nanos =
                recurring_cost_nanos(minor, exponent).map_err(|_| ReportError::Arithmetic)?;
            let mut day = first_day.max(from.div_euclid(DAY_MS) * DAY_MS);
            while day < day_end && to.is_none_or(|to| day < to) {
                let next = day.checked_add(DAY_MS).ok_or(ReportError::Arithmetic)?;
                let active_from_ms = day.max(from);
                let active_to_ms = next.min(to.unwrap_or(next));
                let span = active_to_ms - active_from_ms;
                let fee = Fraction::new(i128::from(nanos), i128::from(period_days(day, period)?))?
                    .checked_mul(&Fraction::new(i128::from(span), i128::from(DAY_MS))?)?;
                result.days.push(FeeDay {
                    key: DayMember {
                        day_start_ms: day,
                        member: member.clone(),
                    },
                    fee,
                    active_from_ms,
                    active_to_ms,
                });
                day = next;
            }
        }
        Ok(result)
    }

    /// Row weights supplied later must cover only the row's matching calls;
    /// neither filters nor sub-day windows may narrow these day denominators.
    pub fn allocate(&self, day: &DayWeights, now_ms: i64) -> Result<Allocation, ReportError> {
        if day
            .weights
            .0
            .iter()
            .any(|(key, weight)| *weight < 0 || key.day_start_ms.rem_euclid(DAY_MS) != 0)
        {
            return Err(ReportError::Arithmetic);
        }
        let mut result = Allocation {
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            now_ms,
            not_current: day.not_current.clone(),
            coefficients: BTreeMap::new(),
            unspread: Vec::new(),
            non_usd: self.non_usd.clone(),
        };
        for fee in &self.days {
            if result.pending(&fee.key.pool_day())? {
                continue;
            }
            let weight = day.weights.0.get(&fee.key).copied().unwrap_or(0);
            if weight > 0 {
                let share = result.coefficients.entry(fee.key.clone()).or_default();
                *share = share.checked_add(&fee.fee.checked_mul(&Fraction::new(1, weight)?)?)?;
            } else {
                result.unspread.push(fee.clone());
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowShare {
    pub fee: Fraction,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Deduced {
    pub currency: &'static str,
    pub nanos: i64,
    pub pending_days: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_rule_share: Option<f64>,
}

impl RowShare {
    /// Add recorded USD only after all exact fee fractions for the row have been
    /// summed. Non-USD rows deliberately have no deduced cost.
    pub fn deduced(
        &self,
        currency: &str,
        recorded_nanos: i64,
        pending_days: usize,
    ) -> Result<Option<Deduced>, ReportError> {
        if currency != "USD" {
            return Ok(None);
        }
        let nanos = self
            .fee
            .checked_add(&Fraction::new(i128::from(recorded_nanos), 1)?)?
            .round()?;
        Ok(Some(Deduced {
            currency: "USD",
            nanos,
            pending_days,
            by_rule_share: None,
        }))
    }
}

#[derive(Clone, Debug)]
pub struct Allocation {
    start_ms: i64,
    end_ms: i64,
    now_ms: i64,
    not_current: BTreeSet<PoolDay>,
    coefficients: BTreeMap<DayMember, Fraction>,
    unspread: Vec<FeeDay>,
    non_usd: BTreeSet<PlanMember>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeTotals {
    /// None when a report filters any dimension other than plan/plan_account.
    pub deduced_unspread_nanos: Option<i64>,
    pub deduced_non_usd_plans: usize,
    pub deduced_conflict_plans: usize,
}

impl Allocation {
    fn pending(&self, day: &PoolDay) -> Result<bool, ReportError> {
        Ok(day
            .day_start_ms
            .checked_add(DAY_MS)
            .ok_or(ReportError::Arithmetic)?
            > self.now_ms
            || self.not_current.contains(day))
    }

    /// The row contains only its own matching calls, in the same numerator scale
    /// as the whole-day weights. Keep zero-weight entries to count pending days.
    /// Sum exact shares across all cells before calling `RowShare::deduced`.
    pub fn row(&self, weights: &Weights) -> Result<(RowShare, BTreeSet<i64>), ReportError> {
        let mut share = RowShare::default();
        let mut pending = BTreeSet::new();
        for (key, weight) in &weights.0 {
            if *weight < 0 {
                return Err(ReportError::Arithmetic);
            }
            if key.member.record.is_some() && self.pending(&key.pool_day())? {
                pending.insert(key.day_start_ms);
            }
            if let Some(coefficient) = self.coefficients.get(key) {
                let weight = Fraction::new(*weight, 1)?;
                share.fee = share.fee.checked_add(&coefficient.checked_mul(&weight)?)?;
            }
        }
        Ok((share, pending))
    }

    /// The report reader supplies record matching for plan/plan_account filters. If any other
    /// dimension is filtered, pass `include_unspread = false`.
    pub fn totals(
        &self,
        include_unspread: bool,
        matches: impl Fn(&PlanMember) -> bool,
    ) -> Result<FeeTotals, ReportError> {
        Ok(FeeTotals {
            deduced_unspread_nanos: if include_unspread {
                Some(self.unspread_exact(&matches)?.round()?)
            } else {
                None
            },
            deduced_non_usd_plans: self.non_usd.iter().filter(|member| matches(member)).count(),
            // The store rejects overlapping fee records for a provider and identity.
            deduced_conflict_plans: 0,
        })
    }

    /// Exact overlap-prorated unspread amount, retained for combining windows
    /// before the single final half-even rounding step.
    pub fn unspread_exact(
        &self,
        matches: impl Fn(&PlanMember) -> bool,
    ) -> Result<Fraction, ReportError> {
        let mut total = Fraction::default();
        for fee in &self.unspread {
            if !matches(&fee.key.member) {
                continue;
            }
            let from = self.start_ms.max(fee.active_from_ms);
            let to = self.end_ms.min(fee.active_to_ms);
            if to <= from {
                continue;
            }
            total = total.checked_add(&fee.fee.checked_mul(&Fraction::new(
                i128::from(to - from),
                i128::from(fee.active_to_ms - fee.active_from_ms),
            )?)?)?;
        }
        Ok(total)
    }
}
