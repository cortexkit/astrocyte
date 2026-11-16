//! Additive spend-report valuations from hourly caches and bounded hour reductions.
use super::*;
use crate::money::round_nano_num;
use crate::multiplier::{QuotaProviderConfig, WindowSelector};
use fees::{Allocation, DayMember, DayWeights, FeeDays, PoolDay, Weights, DAY_MS};

pub(crate) mod attribution_report;

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, ReportError> {
    serde_json::from_str(value).map_err(|_| ReportError::Integrity("invalid valuation payload"))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ListValue {
    pub currency: &'static str,
    pub nanos: i64,
    pub unvalued: Unvalued,
    pub not_yet_valued_hours: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub base_rate_tier_untested_tokens: i64,
}
fn is_zero(value: &i64) -> bool {
    *value == 0
}
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct RowValuation {
    pub list_value: ListValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduced: Option<fees::Deduced>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct TotalValuation {
    #[serde(flatten)]
    pub row: RowValuation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduced_unspread_nanos: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduced_non_usd_plans: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduced_conflict_plans: Option<usize>,
}
#[derive(Default)]
struct Accumulator {
    spend: SpendBucket,
    numerator: i128,
    unvalued: Unvalued,
    pending: BTreeSet<i64>,
    base_tokens: i64,
    weights: Weights,
}
impl Accumulator {
    fn value(
        &self,
        allocation: &Allocation,
        currency: Option<&str>,
        recorded: i64,
    ) -> Result<RowValuation, ReportError> {
        let (share, pending) = allocation.row(&self.weights)?;
        Ok(RowValuation {
            list_value: ListValue {
                currency: "USD",
                nanos: round_nano_num(self.numerator).map_err(|_| ReportError::Arithmetic)?,
                unvalued: self.unvalued.clone(),
                not_yet_valued_hours: self.pending.len(),
                base_rate_tier_untested_tokens: self.base_tokens,
            },
            deduced: share.deduced(currency.unwrap_or_default(), recorded, pending.len())?,
        })
    }
    fn observe(
        &mut self,
        cell: &ValuedCell,
        hour: Hour,
        pending: bool,
        member: PlanMember,
        key: &Key,
    ) -> Result<(), ReportError> {
        self.spend.population.merge(&cell.recorded.cell)?;
        sum(&mut self.spend.total, cell.recorded.cell.total)?;
        if key.0[Grouping::ChargeBasis as usize].as_deref() == Some("metered_api") {
            sum(&mut self.spend.metered, cell.recorded.cell.total)?;
        }
        for ((unit, role, exponent), quantity) in &cell.recorded.quantities {
            self.spend.quantities.observe(
                role,
                unit,
                if quantity.stated_observations == 0 {
                    None
                } else {
                    Some((quantity.units, *exponent))
                },
                quantity
                    .observations
                    .checked_sub(quantity.stated_observations)
                    .ok_or(ReportError::Arithmetic)?,
            )?;
        }
        if pending {
            self.pending.insert(hour.number);
        } else {
            sum(&mut self.numerator, cell.numerator)?;
            merge_unvalued(&mut self.unvalued, &cell.unvalued)?;
            self.base_tokens = checked_add(
                self.base_tokens,
                cell.recorded.base_rate_tier_untested_tokens,
            )?;
        }
        self.weights.checked_add(
            DayMember {
                day_start_ms: day_start(interval(hour)?.0),
                member,
            },
            if pending { 0 } else { cell.numerator },
        )
    }
}
fn day_start(at: i64) -> i64 {
    at.div_euclid(DAY_MS) * DAY_MS
}
fn is_plan(dimension: &Grouping) -> bool {
    matches!(dimension, Grouping::Plan | Grouping::PlanAccount)
}
fn dimension(key: &Key, member: &PlanMember, dimension: Grouping) -> Option<String> {
    match dimension {
        Grouping::Plan => Some(member.plan.clone()),
        Grouping::PlanAccount => member.plan_account.clone(),
        _ => key.0[dimension as usize].clone(),
    }
}
fn matches_plan(parsed: &ParsedReport, member: &PlanMember) -> bool {
    parsed
        .filters
        .iter()
        .filter(|(d, _)| is_plan(d))
        .all(|(d, values)| {
            values.contains(&match d {
                Grouping::Plan => Some(member.plan.clone()),
                _ => member.plan_account.clone(),
            })
        })
}
fn selects(parsed: &ParsedReport, key: &Key, member: &PlanMember) -> bool {
    parsed
        .filters
        .iter()
        .all(|(d, values)| values.contains(&dimension(key, member, *d)))
}
fn bucket(
    parsed: &ParsedReport,
    at: i64,
    key: &Key,
    member: &PlanMember,
) -> Result<BucketKey, ReportError> {
    let (start_ms, end_ms) = analytics::bounds(at, parsed.timezone, parsed.grain)?;
    Ok(BucketKey {
        start_ms,
        end_ms,
        currency: key.1.clone(),
        grouping: parsed
            .grouping
            .iter()
            .map(|d| (d.name().into(), dimension(key, member, *d)))
            .collect(),
    })
}
pub(super) fn stored_config(
    conn: &Connection,
) -> Result<(MultiplierConfig, Vec<String>), ReportError> {
    let encoded: String =
        conn.query_row("SELECT config FROM valuation_state WHERE id=1", [], |r| {
            r.get(0)
        })?;
    if encoded.is_empty() {
        return Ok((
            MultiplierConfig {
                api_twins: BTreeMap::new(),
                plan_price_overrides: BTreeMap::new(),
                quota_providers: BTreeMap::new(),
            },
            Vec::new(),
        ));
    }
    // Use the builder's stored snapshot, not a freshly loaded module config: it
    // keeps built cells, dirty-hour reductions and the fee pool map consistent.
    type ConfigSnapshot = (
        BTreeMap<String, String>,
        BTreeMap<String, Vec<ScopeRule>>,
        Vec<String>,
    );
    let (api_twins, scopes, reasoning): ConfigSnapshot = decode(&encoded)?;
    let config = MultiplierConfig {
        api_twins,
        quota_providers: scopes
            .into_iter()
            .map(|(pool, scope)| {
                (
                    pool,
                    QuotaProviderConfig {
                        scope,
                        window: WindowSelector {
                            pool_key: String::new(),
                            window_minutes: 0,
                        },
                    },
                )
            })
            .collect(),
        plan_price_overrides: BTreeMap::new(),
    };
    Ok((config, reasoning))
}
fn stored_cells(conn: &Connection, hour: Hour) -> Result<Cells, ReportError> {
    let mut stmt = conn.prepare("SELECT summary_key,plan,plan_account,plan_record,recorded,list_numerator,unvalued FROM valuation_cell WHERE kind=?1 AND hour_number=?2")?;
    let mut rows = stmt.query(params![hour.kind, hour.number])?;
    let mut cells = Cells::new();
    while let Some(row) = rows.next()? {
        cells.insert(
            (
                decode(&row.get::<_, String>(0)?)?,
                PlanMember {
                    plan: row.get(1)?,
                    plan_account: row.get(2)?,
                    record: row
                        .get::<_, Option<String>>(3)?
                        .map(|s| decode(&s))
                        .transpose()?,
                },
            ),
            ValuedCell {
                recorded: decode(&row.get::<_, String>(4)?)?,
                numerator: parse_exact(row.get(5)?)?,
                unvalued: decode(&row.get::<_, String>(6)?)?,
            },
        );
    }
    Ok(cells)
}
fn pending_cells(data: Data) -> Cells {
    let mut cells = Cells::new();
    for (key, cell) in data.cells {
        cells
            .entry((key, PlanMember::non_pool("pending")))
            .or_default()
            .recorded
            .cell = cell;
    }
    for ((key, quantity_key), quantity) in data.quantities {
        cells
            .entry((key, PlanMember::non_pool("pending")))
            .or_default()
            .recorded
            .quantities
            .push((quantity_key, quantity));
    }
    cells
}
fn live(
    conn: &Connection,
    filter: &str,
    config: &MultiplierConfig,
    reasoning: &[String],
    book: &PlanBook,
) -> Result<(Cells, bool), ReportError> {
    let mut cells = Cells::new();
    let (_, fetched) = reduce_with_book(conn, filter, config, reasoning, book, &mut cells)?;
    Ok((cells, fetched))
}

fn summary_members(
    book: &PlanBook,
    config: &MultiplierConfig,
    key: &Key,
    hour: Hour,
    record_starts: &BTreeSet<i64>,
) -> Result<BTreeSet<PlanMember>, ReportError> {
    let (from, to) = interval(hour)?;
    let tokens = EventTokens {
        producer: key.0[Grouping::Producer as usize]
            .clone()
            .unwrap_or_default(),
        provider: key.0[Grouping::Provider as usize]
            .clone()
            .unwrap_or_default(),
        model: key.0[Grouping::Model as usize].clone(),
        mode: None,
        roles: Vec::new(),
    };
    let instants = [from, to]
        .into_iter()
        .chain(record_starts.range(from..=to).copied());
    let mut members: BTreeSet<_> = instants
        .map(|at| {
            plan_member(
                book,
                config,
                &tokens,
                key.0[Grouping::ChargeBasis as usize]
                    .as_deref()
                    .unwrap_or_default(),
                key.0[Grouping::Account as usize].as_deref(),
                key.2.as_deref(),
                Some(at),
            )
        })
        .collect::<Result<_, _>>()?;
    if key.0[Grouping::Account as usize].is_none()
        && key.2.is_none()
        && key.0[Grouping::ChargeBasis as usize].as_deref() != Some("metered_api")
        && book.possible_credential(&tokens.provider, from, to)
    {
        // Summaries retain the resolved payer only for subscription-included
        // calls. Other non-metered calls may still match a credential plan.
        // Conservatively defer that provider's pool until valuation catches up:
        // delaying a spread cannot assign money to the wrong calls.
        let pools = pools_for_provider(&tokens.provider, config);
        if pools.len() == 1 {
            for pool in pools {
                members.insert(PlanMember {
                    plan: pool,
                    plan_account: Some("account_unknown".into()),
                    record: None,
                });
            }
        }
    }
    Ok(members)
}
fn add_pending(day: &mut DayWeights, at: i64, member: &PlanMember) {
    if member.plan_account.is_some() && member.plan != "pool_conflict" {
        day.not_current.insert(PoolDay {
            day_start_ms: day_start(at),
            pool: member.plan.clone(),
        });
    }
}
fn row_key(row: &SpendRow, grouping: &[Grouping]) -> BucketKey {
    BucketKey {
        start_ms: row.bucket_start_ms,
        end_ms: row.bucket_end_ms,
        currency: row.currency.clone(),
        grouping: grouping
            .iter()
            .map(|d| (d.name().into(), row.grouping[d.name()].clone()))
            .collect(),
    }
}
fn render_row(key: BucketKey, accumulator: &Accumulator) -> Result<SpendRow, ReportError> {
    let cell = &accumulator.spend.population;
    Ok(SpendRow {
        valuation: Default::default(),
        usage: accumulator.spend.quantities.render()?,
        quantities: accumulator.spend.quantities.clone(),
        bucket_start_ms: key.start_ms,
        bucket_end_ms: key.end_ms,
        grouping: key.grouping.into_iter().collect(),
        currency: key.currency,
        total_nanos: i64::try_from(accumulator.spend.total).map_err(|_| ReportError::Arithmetic)?,
        metered_nanos: i64::try_from(accumulator.spend.metered)
            .map_err(|_| ReportError::Arithmetic)?,
        events_total: cell.events,
        events_priced: cell.priced,
        events_unpriced: cell
            .events
            .checked_sub(cell.priced)
            .ok_or(ReportError::Arithmetic)?,
        cost_sample_n: cell.priced,
        zero_measurement_events: cell.zero_measurements,
        measurements_total: cell.measurements,
        unpriced_events_by_reason: cell.reasons.clone(),
        unpriced_measurements_by_reason: cell.declines.clone(),
    })
}
fn empty_total(currency: Option<String>) -> SpendTotal {
    SpendTotal {
        valuation: Default::default(),
        usage: Default::default(),
        currency,
        total_nanos: 0,
        metered_nanos: 0,
        events_total: 0,
        events_priced: 0,
        events_unpriced: 0,
        cost_sample_n: 0,
        zero_measurement_events: 0,
    }
}
fn recorded_totals(rows: &[SpendRow]) -> Result<Vec<SpendTotal>, ReportError> {
    let mut totals: BTreeMap<Option<String>, (SpendTotal, Quantities)> = BTreeMap::new();
    for row in rows {
        let (total, quantities) = totals
            .entry(row.currency.clone())
            .or_insert_with(|| (empty_total(row.currency.clone()), Quantities::default()));
        quantities.merge(&row.quantities)?;
        total.total_nanos = checked_add(total.total_nanos, row.total_nanos)?;
        total.metered_nanos = checked_add(total.metered_nanos, row.metered_nanos)?;
        add_count(&mut total.events_total, row.events_total)?;
        add_count(&mut total.events_priced, row.events_priced)?;
        add_count(&mut total.events_unpriced, row.events_unpriced)?;
        add_count(&mut total.cost_sample_n, row.cost_sample_n)?;
        add_count(
            &mut total.zero_measurement_events,
            row.zero_measurement_events,
        )?;
    }
    totals
        .into_values()
        .map(|(mut total, quantities)| {
            total.usage = quantities.render()?;
            Ok(total)
        })
        .collect()
}

pub(crate) fn report(
    conn: &Connection,
    request: &ReportRequest,
    now_ms: i64,
) -> Result<SpendReport, ReportError> {
    let parsed = ParsedReport::new(request)?;
    let plan_request =
        parsed.grouping.iter().any(is_plan) || parsed.filters.iter().any(|(d, _)| is_plan(d));
    let mut recorded_request = request.clone();
    recorded_request
        .grouping
        .retain(|name| name != "plan" && name != "plan_account");
    recorded_request
        .filters
        .retain(|name, _| name != "plan" && name != "plan_account");
    let mut report = recorded_spend_report(conn, &recorded_request, now_ms)?;
    let (config, reasoning) = stored_config(conn)?;
    let (from, to) = if let Some(range) = parsed.range {
        range
    } else {
        let first: Option<i64> = conn.query_row(
            "SELECT MIN(hour_number) FROM summary_hour WHERE kind=0",
            [],
            |r| r.get(0),
        )?;
        let first = first
            .map(|h| h.checked_mul(HOUR_MS).ok_or(ReportError::Calendar))
            .transpose()?
            .unwrap_or(now_ms);
        let end = analytics::bounds(now_ms, parsed.timezone, parsed.grain)?.1;
        (
            analytics::bounds(first, parsed.timezone, parsed.grain)?
                .0
                .min(end),
            end,
        )
    };
    let plans = PlanBook::load(conn)?;
    let book = FeeDays::from_book(&plans, &config, from, to)?;
    let record_starts = book
        .days
        .iter()
        .filter_map(|fee| fee.key.member.record.as_ref().map(|r| r.effective_from_ms))
        .collect::<BTreeSet<_>>();
    let mut day = DayWeights::default();
    let mut buckets: BTreeMap<BucketKey, Accumulator> = BTreeMap::new();
    let mut totals: BTreeMap<Option<String>, Accumulator> = BTreeMap::new();
    let mut dictionary = Values::new(conn);
    let mut stmt = conn.prepare("SELECT s.hour_number,s.revision,s.built_revision,s.format_version,EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=s.kind AND d.hour_number=s.hour_number),v.state='built' AND v.rate_generation=g.rate_generation AND v.rate_generation=r.generation AND v.config_generation=g.config_generation AND v.plan_generation=g.plan_generation AND NOT EXISTS(SELECT 1 FROM rate_dirty_hour d WHERE d.hour_number=s.hour_number) FROM summary_hour s LEFT JOIN valuation_hour v USING(kind,hour_number) CROSS JOIN valuation_state g CROSS JOIN rate_cache_state r WHERE s.kind=0 AND (?1 OR s.hour_number BETWEEN ?2 AND ?3) ORDER BY s.hour_number")?;
    let mut hours = stmt.query(params![
        parsed.range.is_none(),
        day_start(from).div_euclid(HOUR_MS),
        (to - 1).div_euclid(DAY_MS) * 24 + 23
    ])?;
    while let Some(row) = hours.next()? {
        let hour = Hour {
            kind: 0,
            number: row.get(0)?,
        };
        let (start, last) = interval(hour)?;
        let end = last.checked_add(1).ok_or(ReportError::Calendar)?;
        let summary_clean = eligible(row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?);
        let current = summary_clean && row.get::<_, Option<bool>>(5)?.unwrap_or(false);
        let (cells, fetched) = if current {
            (stored_cells(conn, hour)?, true)
        } else if !summary_clean {
            live(conn, &hour.filter(), &config, &reasoning, &plans)?
        } else {
            (
                pending_cells(stored_recorded(conn, hour, &mut dictionary)?.0),
                false,
            )
        };
        let pending = !fetched;
        let in_fee_window = start >= day_start(from)
            && start
                < to.checked_add(DAY_MS - 1)
                    .ok_or(ReportError::Calendar)?
                    .div_euclid(DAY_MS)
                    * DAY_MS;
        let mut inferred: BTreeMap<Key, BTreeSet<PlanMember>> = BTreeMap::new();
        for ((key, member), cell) in &cells {
            let members = if member.plan == "pending" {
                summary_members(&plans, &config, key, hour, &record_starts)?
            } else {
                BTreeSet::from([member.clone()])
            };
            if in_fee_window {
                for member in &members {
                    if current {
                        day.weights.checked_add(
                            DayMember {
                                day_start_ms: day_start(start),
                                member: member.clone(),
                            },
                            cell.numerator,
                        )?;
                    } else {
                        add_pending(&mut day, start, member);
                    }
                }
            }
            inferred.entry(key.clone()).or_default().extend(members);
        }
        let mut at = start;
        while at < end {
            let bounds = analytics::bounds(at, parsed.timezone, parsed.grain)?;
            let until = end.min(bounds.1);
            let in_report = parsed.range.is_none_or(|(from, to)| at >= from && at < to);
            if in_report {
                // Only a calendar seam needs a finer read than the hourly payload.
                let split;
                let selected = if at == start && until == end {
                    &cells
                } else {
                    let filter = format!("occurred_at_ms>={at} AND occurred_at_ms<{until}");
                    split = if pending && summary_clean {
                        let mut data = Data::default();
                        data.observe(conn, &filter)?;
                        pending_cells(data)
                    } else {
                        live(conn, &filter, &config, &reasoning, &plans)?.0
                    };
                    &split
                };
                for ((key, member), cell) in selected {
                    let display = if pending {
                        PlanMember::non_pool("pending")
                    } else {
                        member.clone()
                    };
                    if !selects(&parsed, key, &display) {
                        continue;
                    }
                    let accumulator = buckets
                        .entry(bucket(&parsed, at, key, &display)?)
                        .or_default();
                    let total = totals.entry(key.1.clone()).or_default();
                    for acc in [accumulator, total] {
                        acc.observe(cell, hour, pending, member.clone(), key)?;
                        if pending {
                            for member in inferred.get(key).into_iter().flatten() {
                                acc.weights.checked_add(
                                    DayMember {
                                        day_start_ms: day_start(start),
                                        member: member.clone(),
                                    },
                                    0,
                                )?;
                            }
                        }
                    }
                }
            }
            at = until;
        }
    }
    let allocation = book.allocate(&day, now_ms)?;
    if plan_request {
        report.rows = buckets
            .iter()
            .map(|(key, acc)| render_row(key.clone(), acc))
            .collect::<Result<_, _>>()?;
        report.totals = recorded_totals(&report.rows)?;
    }
    for row in &mut report.rows {
        if let Some(acc) = buckets.get(&row_key(row, &parsed.grouping)) {
            row.valuation = acc.value(&allocation, row.currency.as_deref(), row.total_nanos)?;
        } else {
            row.valuation = Accumulator::default().value(
                &allocation,
                row.currency.as_deref(),
                row.total_nanos,
            )?;
        }
    }
    if !report
        .totals
        .iter()
        .any(|t| t.currency.as_deref() == Some("USD"))
        && book
            .days
            .iter()
            .any(|d| matches_plan(&parsed, &d.key.member) && d.fee != fees::Fraction::default())
    {
        report.totals.push(empty_total(Some("USD".into())));
        report.totals.sort_by(|a, b| a.currency.cmp(&b.currency));
    }
    let fee_totals = allocation
        .totals(parsed.filters.iter().all(|(d, _)| is_plan(d)), |member| {
            matches_plan(&parsed, member)
        })?;
    for total in &mut report.totals {
        let empty = Accumulator::default();
        let acc = totals.get(&total.currency).unwrap_or(&empty);
        total.valuation.row =
            acc.value(&allocation, total.currency.as_deref(), total.total_nanos)?;
        if total.currency.as_deref() == Some("USD") {
            total.valuation.deduced_unspread_nanos = fee_totals.deduced_unspread_nanos;
            total.valuation.deduced_non_usd_plans = Some(fee_totals.deduced_non_usd_plans);
            total.valuation.deduced_conflict_plans = Some(fee_totals.deduced_conflict_plans);
        }
    }
    Ok(report)
}
