use super::*;
use crate::report::analytics::UnitQuantity;
use std::collections::btree_map::Entry;

type Group = (BTreeMap<String, Option<String>>, Option<String>);

pub(super) fn fold(
    daily: Vec<SpendRow>,
    bounds: Option<(i64, i64)>,
) -> Result<Vec<SpendRow>, ReportError> {
    let (mut start, mut end) = bounds.unwrap_or((i64::MAX, i64::MIN));
    let mut groups: BTreeMap<Group, RowSum> = BTreeMap::new();
    for mut row in daily {
        // Without request edges, all groups use the same envelope of
        // selected day buckets, including any future-dated buckets.
        if bounds.is_none() {
            start = start.min(row.bucket_start_ms);
            end = end.max(row.bucket_end_ms);
        }
        let key = (std::mem::take(&mut row.grouping), row.currency.take());
        match groups.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(RowSum::new(row)?);
            }
            Entry::Occupied(mut slot) => slot.get_mut().merge(row)?,
        }
    }
    groups
        .into_iter()
        .map(|(group, row)| row.finish(group, start, end))
        .collect()
}

#[derive(Default)]
struct Units(BTreeMap<(String, String), (DecimalSum, u64)>);

impl Units {
    fn merge(&mut self, units: Vec<UnitQuantity>) -> Result<(), ReportError> {
        for unit in units {
            let (sum, unknown) = self.0.entry((unit.role, unit.unit)).or_default();
            if let Some(q) = unit.quantity {
                sum.add(q.units, q.exponent)?;
            }
            add_count(unknown, unit.unknown_quantities)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<Vec<UnitQuantity>, ReportError> {
        self.0
            .into_iter()
            .map(|((role, unit), (sum, unknown_quantities))| {
                let quantity = if sum.initialized {
                    Some(crate::runs::Quantity {
                        units: money(sum.units)?,
                        exponent: sum.exponent,
                    })
                } else {
                    None
                };
                Ok(UnitQuantity {
                    role,
                    unit,
                    quantity,
                    unknown_quantities,
                })
            })
            .collect()
    }
}

fn money(nanos: i128) -> Result<i64, ReportError> {
    i64::try_from(nanos).map_err(|_| ReportError::Arithmetic)
}

fn count_days(total: &mut usize, count: usize) -> Result<(), ReportError> {
    *total = total.checked_add(count).ok_or(ReportError::Arithmetic)?;
    Ok(())
}

fn count_map(
    total: &mut BTreeMap<String, u64>,
    counts: BTreeMap<String, u64>,
) -> Result<(), ReportError> {
    for (key, count) in counts {
        add_count(total.entry(key).or_default(), count)?;
    }
    Ok(())
}

fn token(total: &mut Option<u64>, day: Option<u64>) -> Result<(), ReportError> {
    if let Some(count) = day {
        add_count(total.get_or_insert(0), count)?;
    }
    Ok(())
}

struct RowSum {
    row: SpendRow,
    units: Units,
    // Recorded money can be signed; let later days cancel earlier ones
    // before checking that the final count fits the wire integer type.
    recorded: i128,
    metered: i128,
    deduced: i128,
}

impl RowSum {
    fn new(mut row: SpendRow) -> Result<Self, ReportError> {
        let mut units = Units::default();
        units.merge(std::mem::take(&mut row.usage.units))?;
        row.quantities = Quantities::default();
        Ok(Self {
            units,
            recorded: i128::from(row.total_nanos),
            metered: i128::from(row.metered_nanos),
            deduced: row
                .valuation
                .deduced
                .as_ref()
                .map_or(0, |d| i128::from(d.nanos)),
            row,
        })
    }

    fn merge(&mut self, day: SpendRow) -> Result<(), ReportError> {
        let row = &mut self.row;
        sum(&mut self.recorded, i128::from(day.total_nanos))?;
        sum(&mut self.metered, i128::from(day.metered_nanos))?;
        add_count(&mut row.events_total, day.events_total)?;
        add_count(&mut row.events_priced, day.events_priced)?;
        add_count(&mut row.events_unpriced, day.events_unpriced)?;
        add_count(&mut row.cost_sample_n, day.cost_sample_n)?;
        add_count(
            &mut row.zero_measurement_events,
            day.zero_measurement_events,
        )?;
        add_count(&mut row.measurements_total, day.measurements_total)?;
        count_map(
            &mut row.unpriced_events_by_reason,
            day.unpriced_events_by_reason,
        )?;
        count_map(
            &mut row.unpriced_measurements_by_reason,
            day.unpriced_measurements_by_reason,
        )?;

        // Add the displayed counts, not the internal quantities: rendering
        // again would change inclusive output and absent fractional tokens.
        let a = &mut row.usage.tokens;
        let b = day.usage.tokens;
        token(&mut a.counts.input, b.counts.input)?;
        token(&mut a.counts.output, b.counts.output)?;
        token(&mut a.counts.reasoning, b.counts.reasoning)?;
        token(&mut a.counts.cache_read, b.counts.cache_read)?;
        token(&mut a.counts.cache_write, b.counts.cache_write)?;
        token(&mut a.cache_write_5m, b.cache_write_5m)?;
        token(&mut a.cache_write_1h, b.cache_write_1h)?;
        self.units.merge(day.usage.units)?;
        count_map(
            &mut row.usage.unknown_quantities,
            day.usage.unknown_quantities,
        )?;

        let a = &mut row.valuation.list_value;
        let b = day.valuation.list_value;
        a.nanos = checked_add(a.nanos, b.nanos)?;
        a.base_rate_tier_untested_tokens = checked_add(
            a.base_rate_tier_untested_tokens,
            b.base_rate_tier_untested_tokens,
        )?;
        count_days(&mut a.not_yet_valued_hours, b.not_yet_valued_hours)?;
        merge_unvalued(&mut a.unvalued, &b.unvalued)?;
        if let Some(b) = day.valuation.deduced {
            sum(&mut self.deduced, i128::from(b.nanos))?;
            let a = row
                .valuation
                .deduced
                .as_mut()
                .ok_or(ReportError::Integrity("range day currency changed"))?;
            count_days(&mut a.pending_days, b.pending_days)?;
        }
        Ok(())
    }

    fn finish(
        mut self,
        (grouping, currency): Group,
        start: i64,
        end: i64,
    ) -> Result<SpendRow, ReportError> {
        let row = &mut self.row;
        row.grouping = grouping;
        row.currency = currency;
        row.bucket_start_ms = start;
        row.bucket_end_ms = end;
        row.total_nanos = money(self.recorded)?;
        row.metered_nanos = money(self.metered)?;
        row.usage.units = self.units.finish()?;
        if let Some(deduced) = &mut row.valuation.deduced {
            deduced.nanos = money(self.deduced)?;
        }
        Ok(self.row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fees::{Fraction, RowShare};

    #[test]
    fn range_keeps_daily_fee_rounding_and_sums_pending_days() {
        let mut rows = Vec::new();
        for (day, fee) in [(0, (1, 3)), (1, (2, 5))] {
            let share = RowShare {
                fee: Fraction::new(fee.0, fee.1).unwrap(),
            };
            let key = BucketKey {
                start_ms: day * DAY_MS,
                end_ms: (day + 1) * DAY_MS,
                grouping: Vec::new(),
                currency: Some("USD".into()),
            };
            let mut row = render_row(key, &Accumulator::default()).unwrap();
            row.valuation.deduced = share.deduced("USD", 0, 1).unwrap();
            rows.push(row);
        }
        let rows = fold(rows, None).unwrap();
        let deduced = rows[0].valuation.deduced.as_ref().unwrap();
        assert_eq!(
            deduced.nanos, 0,
            "both day fees round down; their sum would round up"
        );
        assert_eq!(deduced.pending_days, 2);
        assert!(deduced.by_rule_share.is_none());
    }
}
