//! Attribute spend without changing summary_cell or valuation_cell report keys.
use super::*;
use crate::report::hourly::attribution::{
    Attribution, AttributionKey, Payload, ProjectedValue, ValuationProjection,
};

pub(crate) const CELLS_SQL:&str="SELECT currency,attribution,provider,model,plan_key,recorded,list_numerator,unvalued FROM attribution_valuation_cell WHERE kind=?1 AND hour_number=?2";
pub(crate) const HOURS_SQL:&str="SELECT s.hour_number,s.revision,s.built_revision,s.format_version,EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=s.kind AND d.hour_number=s.hour_number),v.state='built' AND v.config_generation=g.config_generation AND NOT EXISTS(SELECT 1 FROM rate_dirty_hour d WHERE d.hour_number=s.hour_number),a.summary_revision=s.revision AND a.summary_attribution_revision=a.revision AND a.valuation_revision=v.revision AND a.valuation_attribution_revision=a.revision,s.events_total FROM summary_hour s LEFT JOIN valuation_hour v USING(kind,hour_number) LEFT JOIN attribution_hour a USING(kind,hour_number) CROSS JOIN valuation_state g WHERE s.kind=0 AND s.hour_number BETWEEN ?1 AND ?2 ORDER BY s.hour_number";

struct Request<'a> {
    raw: &'a ReportRequest,
    parsed: ParsedReport,
}
impl<'a> Request<'a> {
    fn new(raw: &'a ReportRequest) -> Result<Self, ReportError> {
        let mut ordinary = raw.clone();
        ordinary
            .grouping
            .retain(|name| matches!(name.as_str(), "provider" | "model"));
        ordinary
            .filters
            .retain(|name, _| matches!(name.as_str(), "provider" | "model"));
        Ok(Self {
            raw,
            parsed: ParsedReport::new(&ordinary)?,
        })
    }
    fn non_attribution(&self, key: &AttributionKey) -> bool {
        self.raw
            .filters
            .iter()
            .filter(|(name, _)| matches!(name.as_str(), "provider" | "model"))
            .all(|(name, values)| values.contains(&dimension(key, name)))
    }
    fn attribution(&self, key: &AttributionKey) -> bool {
        self.raw
            .filters
            .iter()
            .filter(|(name, _)| analytics::is_attribution_dimension(name))
            .all(|(name, values)| values.contains(&dimension(key, name)))
    }
    fn bucket(&self, at: i64, key: &AttributionKey) -> Result<BucketKey, ReportError> {
        let (start_ms, end_ms) = analytics::bounds(at, self.parsed.timezone, self.parsed.grain)?;
        Ok(BucketKey {
            start_ms,
            end_ms,
            currency: key.currency.clone(),
            grouping: self
                .raw
                .grouping
                .iter()
                .map(|name| (name.clone(), dimension(key, name)))
                .collect(),
        })
    }
    fn range(&self, conn: &Connection, now: i64) -> Result<(i64, i64), ReportError> {
        if let Some(range) = self.parsed.range {
            return Ok(range);
        }
        let first: Option<i64> = conn.query_row(
            "SELECT MIN(hour_number) FROM summary_hour WHERE kind=0",
            [],
            |r| r.get(0),
        )?;
        let first = first
            .map(|hour| hour.checked_mul(HOUR_MS).ok_or(ReportError::Calendar))
            .transpose()?
            .unwrap_or(now);
        let end = analytics::bounds(now, self.parsed.timezone, self.parsed.grain)?.1;
        Ok((
            analytics::bounds(first, self.parsed.timezone, self.parsed.grain)?
                .0
                .min(end),
            end,
        ))
    }
}
fn dimension(key: &AttributionKey, name: &str) -> Option<String> {
    match name {
        "alfonso" => &key.attribution.alfonso,
        "parent_alfonso" => &key.attribution.parent_alfonso,
        "workload" => &key.attribution.workload,
        "project_id" => &key.attribution.project_id,
        "workspace" => &key.attribution.workspace,
        "provider" => &key.provider,
        "model" => &key.model,
        _ => return None,
    }
    .clone()
}
fn ordinary_key(_key: &AttributionKey) -> Key {
    // The common accumulator only reads charge_basis from this key. Its exact
    // metered subtotal is supplied separately by the attribution payload.
    (std::array::from_fn(|_| None), None, None)
}

type ReportCells = Vec<((AttributionKey, PlanMember), ProjectedValue)>;
struct Dictionary<'c> {
    values: Values<'c>,
    tuples: HashMap<i64, Attribution>,
    members: HashMap<String, PlanMember>,
    dimensions: [bool; 2],
}
impl Dictionary<'_> {
    fn tuple(&mut self, id: i64) -> Result<Attribution, ReportError> {
        if let Some(tuple) = self.tuples.get(&id) {
            return Ok(tuple.clone());
        }
        let tuple: Attribution = self
            .values
            .get(id)?
            .map(|encoded| decode(&encoded))
            .transpose()?
            .unwrap_or_default();
        self.tuples.insert(id, tuple.clone());
        Ok(tuple)
    }
    fn cells(&mut self, conn: &Connection, hour: Hour) -> Result<ReportCells, ReportError> {
        let mut stmt = conn.prepare_cached(CELLS_SQL)?;
        let mut rows = stmt.query(params![hour.kind, hour.number])?;
        let mut cells = Vec::new();
        while let Some(row) = rows.next()? {
            let key = AttributionKey {
                currency: self.values.get(row.get(0)?)?,
                attribution: self.tuple(row.get(1)?)?,
                provider: if self.dimensions[0] {
                    self.values.get(row.get(2)?)?
                } else {
                    None
                },
                model: if self.dimensions[1] {
                    self.values.get(row.get(3)?)?
                } else {
                    None
                },
            };
            let encoded_member = row.get::<_, String>(4)?;
            let member = if let Some(member) = self.members.get(&encoded_member) {
                member.clone()
            } else {
                let member: PlanMember = decode(&encoded_member)?;
                self.members.insert(encoded_member, member.clone());
                member
            };
            let payload: Payload = decode(&row.get::<_, String>(5)?)?;
            let numerator = row
                .get::<_, String>(6)?
                .parse()
                .map_err(|_| ReportError::Integrity("invalid attribution numerator"))?;
            let unvalued = decode(&row.get::<_, String>(7)?)?;
            cells.push((
                (key, member),
                ProjectedValue {
                    value: ValuedCell {
                        recorded: payload.recorded,
                        numerator,
                        unvalued,
                    },
                    metered: payload.metered,
                    ..Default::default()
                },
            ));
        }
        Ok(cells)
    }
}
struct Live<'c> {
    conn: &'c Connection,
    config: &'c MultiplierConfig,
    reasoning: &'c [String],
    plans: &'c PlanBook,
    resolver: crate::attribution::Resolver,
}
impl Live<'_> {
    fn cells(&mut self, filter: &str) -> Result<(ReportCells, bool), ReportError> {
        let mut ordinary = Cells::new();
        let mut projection = ValuationProjection::new();
        let (_, fetched) = reduce_both(
            self.conn,
            filter,
            self.config,
            self.reasoning,
            self.plans,
            &mut ordinary,
            Some((&mut projection, &mut self.resolver)),
        )?;
        Ok((projection.into_iter().collect(), fetched))
    }
}
fn population(cells: &ReportCells) -> Result<u64, ReportError> {
    cells.iter().try_fold(0_u64, |sum, (_, cell)| {
        sum.checked_add(cell.value.recorded.cell.events)
            .ok_or(ReportError::Arithmetic)
    })
}
fn members(
    cell: &ProjectedValue,
    member: &PlanMember,
    pending: bool,
    live: &Live<'_>,
    hour: Hour,
    starts: &BTreeSet<i64>,
) -> Result<BTreeSet<PlanMember>, ReportError> {
    if !pending {
        return Ok(BTreeSet::from([member.clone()]));
    }
    let mut members = BTreeSet::new();
    for key in &cell.original_keys {
        members.extend(summary_members(live.plans, live.config, key, hour, starts)?);
    }
    if members.is_empty() {
        members.insert(member.clone());
    }
    Ok(members)
}

#[derive(Default)]
struct Output {
    buckets: BTreeMap<BucketKey, Accumulator>,
    totals: BTreeMap<Option<String>, Accumulator>,
    coverage: AttributionCoverage,
    staleness: Cell,
}
impl Output {
    fn observe(
        &mut self,
        request: &Request<'_>,
        at: i64,
        hour: Hour,
        cells: &ReportCells,
        pending: bool,
        inferred: &BTreeMap<AttributionKey, BTreeSet<PlanMember>>,
    ) -> Result<(), ReportError> {
        for ((key, member), cell) in cells {
            if !request.non_attribution(key) {
                continue;
            }
            let population = cell.value.recorded.cell.events;
            let count = if key.attribution == Attribution::default() {
                &mut self.coverage.unattributed_calls
            } else {
                &mut self.coverage.attributed_calls
            };
            add_count(count, population)?;
            if !request.attribution(key) {
                continue;
            }
            self.staleness.merge(&cell.value.recorded.cell)?;
            let ordinary = ordinary_key(key);
            let row = self.buckets.entry(request.bucket(at, key)?).or_default();
            let total = self.totals.entry(key.currency.clone()).or_default();
            for acc in [row, total] {
                acc.observe(&cell.value, hour, pending, member.clone(), &ordinary)?;
                sum(&mut acc.spend.metered, cell.metered)?;
                if pending {
                    for member in inferred.get(key).into_iter().flatten() {
                        acc.weights.checked_add(
                            DayMember {
                                day_start_ms: day_start(interval(hour)?.0),
                                member: member.clone(),
                            },
                            0,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn report(
    conn: &Connection,
    raw: &ReportRequest,
    now: i64,
) -> Result<SpendReport, ReportError> {
    let request = Request::new(raw)?;
    let (from, to) = request.range(conn, now)?;
    let (config, reasoning) = stored_config(conn)?;
    let plans = PlanBook::load(conn)?;
    let book = FeeDays::from_book(&plans, &config, from, to)?;
    let starts = book
        .days
        .iter()
        .filter_map(|fee| {
            fee.key
                .member
                .record
                .as_ref()
                .map(|record| record.effective_from_ms)
        })
        .collect();
    let mut dictionary = Dictionary {
        values: Values::new(conn),
        tuples: HashMap::new(),
        members: HashMap::new(),
        dimensions: ["provider", "model"].map(|dimension| {
            raw.grouping
                .iter()
                .chain(raw.filters.keys())
                .any(|name| name == dimension)
        }),
    };
    let mut live = Live {
        conn,
        config: &config,
        reasoning: &reasoning,
        plans: &plans,
        resolver: crate::attribution::Resolver::default(),
    };
    let mut output = Output::default();
    let mut day = DayWeights::default();
    let first = day_start(from).div_euclid(HOUR_MS);
    let fee_last = (to - 1).div_euclid(DAY_MS) * 24 + 23;
    let last = if request.parsed.range.is_none() {
        conn.query_row(
            "SELECT MAX(hour_number) FROM summary_hour WHERE kind=0",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )?
        .unwrap_or(fee_last)
        .max(fee_last)
    } else {
        fee_last
    };
    let mut stmt = conn.prepare_cached(HOURS_SQL)?;
    let mut hours = stmt.query(params![first, last])?;
    while let Some(row) = hours.next()? {
        let hour = Hour {
            kind: 0,
            number: row.get(0)?,
        };
        let (start, last) = interval(hour)?;
        let end = last.checked_add(1).ok_or(ReportError::Calendar)?;
        let clean = eligible(row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?);
        let main_current = clean && row.get::<_, Option<bool>>(5)?.unwrap_or(false);
        let split_current = main_current && row.get::<_, Option<bool>>(6)?.unwrap_or(false);
        let cached = if split_current {
            Some(dictionary.cells(conn, hour)?)
        } else {
            None
        };
        let expected = row.get::<_, u64>(7)?;
        let (cells, fetched) = match cached {
            Some(cells) if population(&cells)? == expected => (cells, true),
            _ => live.cells(&hour.filter())?,
        };
        let pending = if clean && !main_current {
            true
        } else {
            !fetched
        };
        let mut inferred: BTreeMap<AttributionKey, BTreeSet<PlanMember>> = BTreeMap::new();
        for ((key, member), cell) in &cells {
            if main_current {
                if hour.number <= fee_last {
                    day.weights.checked_add(
                        DayMember {
                            day_start_ms: day_start(start),
                            member: member.clone(),
                        },
                        cell.value.numerator,
                    )?;
                }
                continue;
            }
            let members = members(cell, member, clean && !main_current, &live, hour, &starts)?;
            for member in &members {
                if hour.number > fee_last {
                    continue;
                }
                add_pending(&mut day, start, member);
            }
            inferred.entry(key.clone()).or_default().extend(members);
        }
        let mut at = start;
        while at < end {
            let bounds = analytics::bounds(at, request.parsed.timezone, request.parsed.grain)?;
            let until = end.min(bounds.1);
            if request.parsed.range.is_none() || (at >= from && at < to) {
                if at == start && until == end {
                    output.observe(&request, at, hour, &cells, pending, &inferred)?;
                } else {
                    // Calendar seams need only the portion of this one hour;
                    // fee denominators above still use the complete UTC day's
                    // usage for each subscription pool, before any filters.
                    let (split, _) =
                        live.cells(&format!("occurred_at_ms>={at} AND occurred_at_ms<{until}"))?;
                    output.observe(&request, at, hour, &split, pending, &inferred)?;
                }
            }
            at = until;
        }
    }
    let allocation = book.allocate(&day, now)?;
    let mut rows = Vec::with_capacity(output.buckets.len());
    for (key, acc) in &output.buckets {
        let mut row = render_row(key.clone(), acc)?;
        row.valuation = acc.value(&allocation, row.currency.as_deref(), row.total_nanos)?;
        rows.push(row);
    }
    let mut totals = recorded_totals(&rows)?;
    if !totals
        .iter()
        .any(|total| total.currency.as_deref() == Some("USD"))
        && book
            .days
            .iter()
            .any(|day| day.fee != fees::Fraction::default())
    {
        totals.push(empty_total(Some("USD".into())));
        totals.sort_by(|a, b| a.currency.cmp(&b.currency));
    }
    let fee_totals = allocation.totals(raw.filters.is_empty(), |_| true)?;
    for total in &mut totals {
        let empty = Accumulator::default();
        let acc = output.totals.get(&total.currency).unwrap_or(&empty);
        total.valuation.row =
            acc.value(&allocation, total.currency.as_deref(), total.total_nanos)?;
        if total.currency.as_deref() == Some("USD") {
            total.valuation.row.deduced =
                usd_total_deduced(&allocation, &output.totals, total.total_nanos)?;
            total.valuation.deduced_unspread_nanos = fee_totals.deduced_unspread_nanos;
            total.valuation.deduced_non_usd_plans = Some(fee_totals.deduced_non_usd_plans);
            total.valuation.deduced_conflict_plans = Some(fee_totals.deduced_conflict_plans);
        }
    }
    let mut undated = UndatedCoverage {
        events_undated: 0,
        events_report_total: output.staleness.events,
        reasons: BTreeMap::new(),
    };
    if request.parsed.range.is_none() {
        let (cells, _) = live.cells("occurred_at_ms IS NULL")?;
        for ((key, _), cell) in cells {
            if request.non_attribution(&key) && request.attribution(&key) {
                add_count(&mut undated.events_undated, cell.value.recorded.cell.events)?;
                output.staleness.merge(&cell.value.recorded.cell)?;
            }
        }
        undated.events_report_total = output.staleness.events;
        if undated.events_undated > 0 {
            undated
                .reasons
                .insert("missing_occurred_at".into(), undated.events_undated);
        }
    }
    Ok(SpendReport {
        totals,
        rows,
        undated,
        ingest_freshness: load_ingest_freshness(conn)?,
        rate_staleness: output.staleness.staleness(now),
        hierarchy_resolution: None,
        attribution_coverage: Some(output.coverage),
    })
}
