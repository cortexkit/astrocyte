//! Independent list-value partitions. Recorded spend uses the summary reducer unchanged.
use super::*;
use crate::subscription::plan_lookup_key;
use crate::valuation::{list_value_event, EventTokens, Unvalued};

// Older cache keys encoded pricing settings. Never decode those keys or serve
// their cached values, even before the refresher has started the rebuild.
const VALUATION_FORMAT: &str = "served-rates-v1";

pub mod fees;
mod plans;
pub use plans::PlanBook;
pub mod surface;

/// The immutable primary key of a record in either subscription ledger.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlanRecordId {
    pub ledger: crate::subscription::PlanLedger,
    pub provider: String,
    pub identity: String,
    pub effective_from_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlanMember {
    pub plan: String,
    pub plan_account: Option<String>,
    pub record: Option<PlanRecordId>,
}

impl PlanMember {
    fn non_pool(plan: &str) -> Self {
        Self {
            plan: plan.into(),
            plan_account: None,
            record: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Recorded {
    #[serde(default)]
    pub(super) base_rate_tier_untested_tokens: i64,
    pub(super) cell: Cell,
    pub(super) quantities: Vec<(QuantityKey, Quantity)>,
    pub(super) money: Vec<((i64, String), Money)>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ValuedCell {
    pub(super) recorded: Recorded,
    pub(super) numerator: i128,
    pub(super) unvalued: Unvalued,
}

type Cells = BTreeMap<(Key, PlanMember), ValuedCell>;

/// Membership deliberately matches ledger coverage: a present account selects
/// account plans, and only a NULL account selects the exact paying credential.
/// An empty account selects neither ledger; it is not an absent account.
pub fn plan_member(
    book: &PlanBook,
    provider: &str,
    basis: &str,
    account: Option<&str>,
    paying_credential: Option<&str>,
    at_ms: Option<i64>,
) -> PlanMember {
    if basis == "metered_api" {
        return PlanMember::non_pool("api");
    }
    let (Some((ledger, identity)), Some(at)) = (plan_lookup_key(account, paying_credential), at_ms)
    else {
        return PlanMember::non_pool("none");
    };
    let mut records = book.active(ledger, provider, identity, at);
    match (records.next(), records.next()) {
        (Some(_), Some(_)) => PlanMember::non_pool("pool_conflict"),
        (Some(record), None) => record.member(),
        (None, _) => PlanMember::non_pool("none"),
    }
}

fn merge_unvalued(target: &mut Unvalued, value: &Unvalued) -> Result<(), ReportError> {
    add_count(&mut target.measurements, value.measurements)?;
    target.tokens = target
        .tokens
        .checked_add(value.tokens)
        .ok_or(ReportError::Arithmetic)?;
    for (reason, count) in &value.by_reason {
        add_count(target.by_reason.entry(*reason).or_default(), *count)?;
    }
    Ok(())
}

/// Exercise the ordinary reduction without building attribution projections.
/// The caller holds a read snapshot covering rates, plans and ledger rows.
#[cfg(test)]
pub(crate) fn reduce(
    conn: &Connection,
    filter: &str,
    cells: &mut Cells,
) -> Result<(usize, bool), ReportError> {
    reduce_with_book(conn, filter, &PlanBook::load(conn)?, cells)
}

fn measurement_sql(filter: &str) -> String {
    let quantity = reported_quantity_sql();
    format!(
        "SELECT m.event_key,m.role,m.unit,m.source,m.quantity_units,m.quantity_exponent,
         CASE WHEN {DERIVED_UNPRICED_SQL} THEN 'unpriced' ELSE h.outcome END,{quantity}
         FROM measurement m JOIN usage_event e ON e.event_key=m.event_key
         LEFT JOIN priced_charge h ON h.event_key=m.event_key AND h.role=m.role AND h.unit=m.unit
         AND NOT EXISTS(SELECT 1 FROM priced_charge s WHERE s.supersedes=h.charge_key)
         WHERE m.event_key IN (SELECT event_key FROM usage_event WHERE {filter})
         ORDER BY m.event_key,m.role,m.unit"
    )
}

fn reduce_with_book(
    conn: &Connection,
    filter: &str,
    book: &PlanBook,
    cells: &mut Cells,
) -> Result<(usize, bool), ReportError> {
    reduce_both(conn, filter, book, cells, None)
}

fn reduce_both(
    conn: &Connection,
    filter: &str,
    book: &PlanBook,
    cells: &mut Cells,
    projection: Option<(
        &mut attribution::ValuationProjection,
        &mut crate::attribution::Resolver,
    )>,
) -> Result<(usize, bool), ReportError> {
    let (mut projection, mut maps) = projection.map_or((None, None), |(projection, maps)| {
        (Some(projection), Some(maps))
    });
    use crate::multiplier::ModelResolution;
    use crate::rate_era::ModelTimeline;
    use crate::valuation::MeasuredRole;
    let ledger = load_scoped_ledger(conn, &DIMENSIONS, Some(filter))?;
    let stamps = if projection.is_some() {
        attribution::stamps(conn, filter)?
    } else {
        attribution::ScopeStamps::default()
    };
    // Bulk reads stay scoped by the chunk's time/key bounds. In particular, do
    // not pass per-member event-key lists through the runs reducer: its forced
    // time index would scan unrelated history and valuation needs no runs data.
    type ReportedMeasurement = (MeasuredRole, bool, Option<i64>);
    let mut measurements: BTreeMap<i64, Vec<ReportedMeasurement>> = BTreeMap::new();
    let mut stmt = conn.prepare(&measurement_sql(filter))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        measurements.entry(row.get(0)?).or_default().push((
            (
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ),
            row.get::<_, Option<String>>(6)?.as_deref() == Some("covered"),
            row.get(7)?,
        ));
    }
    let mut payers = BTreeMap::new();
    let mut stmt = conn.prepare(&format!("SELECT e.event_key,COALESCE(e.resolved_credential,e.auth_selection),b.event_key IS NOT NULL,b.mode,e.media_facts FROM usage_event e LEFT JOIN broca_attempt_detail b ON b.event_key=e.event_key WHERE e.event_key IN (SELECT event_key FROM usage_event WHERE {filter})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        payers.insert(
            row.get::<_, i64>(0)?,
            (
                row.get::<_, Option<String>>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, Option<String>>(3)?,
                crate::media::read_facts(row, 4)?,
            ),
        );
    }
    let mut timelines = BTreeMap::new();
    for event in &ledger.events {
        let key = key(&ledger, event);
        if let Some(model) = &key.0[Grouping::Model as usize] {
            let provider = key.0[Grouping::Provider as usize]
                .clone()
                .unwrap_or_default();
            timelines
                .entry((provider.clone(), model.clone()))
                .or_insert_with(|| ModelTimeline::load(conn, &provider, model));
        }
    }
    let mut members: BTreeMap<(PlanMember, attribution::Attribution), Data> = BTreeMap::new();
    let mut event_members = BTreeMap::new();
    let mut fetched = true;
    let valuation_error = ModelResolution::Unpriceable {
        reason: "valuation_error".into(),
    };
    for event in &ledger.events {
        let key = key(&ledger, event);
        let (tuple, disagrees) = if let Some(resolver) = maps.as_deref_mut() {
            attribution::resolve(resolver, conn, &ledger, event, &stamps)?
        } else {
            (attribution::Attribution::default(), false)
        };
        let measured = measurements.remove(&event.key).unwrap_or_default();
        let (payer, broca_attempt, mode, media) = payers.remove(&event.key).unwrap_or_default();
        let tokens = EventTokens {
            producer: key.0[Grouping::Producer as usize]
                .clone()
                .unwrap_or_default(),
            provider: key.0[Grouping::Provider as usize]
                .clone()
                .unwrap_or_default(),
            model: key.0[Grouping::Model as usize].clone(),
            mode,
            broca_attempt,
            roles: measured
                .iter()
                .filter(|((_, unit, _, _, _), _, _)| {
                    !crate::catalog_client::RATE_UNITS.contains(&unit.as_str())
                })
                .map(|(role, _, _)| role.clone())
                .collect(),
        };
        let member = plan_member(
            book,
            &tokens.provider,
            key.0[Grouping::ChargeBasis as usize]
                .as_deref()
                .unwrap_or_default(),
            key.0[Grouping::Account as usize].as_deref(),
            payer.as_deref(),
            event.occurred_at_ms,
        );
        let data = members.entry((member.clone(), tuple.clone())).or_default();
        data.observe_event(&ledger, event)?;
        if let Some(projected) = projection.as_deref_mut().filter(|_| disagrees) {
            let cell = projected
                .entry((
                    attribution::AttributionKey::new(&key, &tuple),
                    member.clone(),
                ))
                .or_default();
            add_count(&mut cell.stamp_disagreement_calls, 1)?;
        }
        for ((role, unit, _, _, exponent), covered, units) in &measured {
            data.observe_quantity(
                &key,
                event.occurred_at_ms.is_some() && event.is_fully_priced(),
                (
                    unit.clone(),
                    role.clone(),
                    *units,
                    exponent.filter(|_| units.is_some()),
                ),
                *covered,
            )?;
        }
        event_members.insert(event.key, (key.clone(), member.clone(), tuple.clone()));
        let timeline = tokens
            .model
            .as_ref()
            .and_then(|model| timelines.get(&(tokens.provider.clone(), model.clone())));
        let (resolution, unit_rates) = match (timeline, event.occurred_at_ms) {
            (Some(Ok(Some(timeline))), Some(at)) => {
                (Some(timeline.at(at)), Some(timeline.unit_rates_at(at)))
            }
            (Some(Err(_)), Some(_)) => (Some(&valuation_error), None),
            _ => (None, None),
        };
        if tokens.model.is_some() && (resolution.is_none() || event.occurred_at_ms.is_none()) {
            fetched = false;
            continue;
        }
        let mut value =
            list_value_event(resolution, &tokens).map_err(|_| ReportError::Arithmetic)?;
        for ((role, unit, _, raw_units, raw_exponent), _, _) in measured {
            let Some((units, exponent)) =
                raw_units.zip(raw_exponent).filter(|(units, _)| *units > 0)
            else {
                continue;
            };
            if !crate::catalog_client::RATE_UNITS.contains(&unit.as_str()) {
                continue;
            }
            use crate::valuation::UnvaluedReason;
            let result = unit_rates
                .ok_or(if tokens.model.is_none() {
                    UnvaluedReason::MissingModel
                } else {
                    UnvaluedReason::ValuationError
                })
                .and_then(|rates| {
                    crate::pricing::unit_list_nanos(
                        rates,
                        &role,
                        &unit,
                        (units, exponent),
                        media.as_ref(),
                        tokens.mode.as_deref(),
                    )
                });
            match result {
                Ok(nanos) => sum(
                    &mut value.numerator,
                    i128::from(nanos) * crate::money::NUMERATOR_SCALE,
                )?,
                Err(reason) => value
                    .unvalued
                    .count(&unit, units, reason)
                    .map_err(|_| ReportError::Arithmetic)?,
            }
        }
        let apply = |cell: &mut ValuedCell| -> Result<(), ReportError> {
            cell.recorded.base_rate_tier_untested_tokens = checked_add(
                cell.recorded.base_rate_tier_untested_tokens,
                value.base_rate_tier_untested_tokens,
            )?;
            sum(&mut cell.numerator, value.numerator)?;
            merge_unvalued(&mut cell.unvalued, &value.unvalued)?;
            Ok(())
        };
        apply(cells.entry((key.clone(), member.clone())).or_default())?;
        if let Some(projected) = projection.as_deref_mut() {
            apply(
                &mut projected
                    .entry((attribution::AttributionKey::new(&key, &tuple), member))
                    .or_default()
                    .value,
            )?;
        }
    }
    let mut stmt = conn.prepare(&format!("SELECT event_key,currency,amount_nanos FROM adjustment WHERE event_key IN (SELECT event_key FROM usage_event WHERE {filter})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (key, member, tuple) = event_members
            .get(&row.get::<_, i64>(0)?)
            .ok_or(ReportError::Integrity("valuation adjustment event absent"))?;
        let money = members
            .get_mut(&(member.clone(), tuple.clone()))
            .ok_or(ReportError::Integrity("valuation member absent"))?
            .money
            .entry((key.clone(), 1, row.get(1)?))
            .or_default();
        sum(&mut money.nanos, i128::from(row.get::<_, i64>(2)?))?;
        add_count(&mut money.contributions, 1)?;
    }
    for ((member, tuple), data) in members {
        for (key, recorded) in data.cells {
            if let Some(projected) = projection.as_deref_mut() {
                let cell = projected
                    .entry((
                        attribution::AttributionKey::new(&key, &tuple),
                        member.clone(),
                    ))
                    .or_default();
                if key.0[Grouping::ChargeBasis as usize].as_deref() == Some("metered_api") {
                    sum(&mut cell.metered, recorded.total)?;
                }
                cell.original_keys.insert(key.clone());
                cell.value.recorded.cell.merge(&recorded)?;
            }
            cells
                .entry((key, member.clone()))
                .or_default()
                .recorded
                .cell
                .merge(&recorded)?;
        }
        for ((key, quantity_key), quantity) in data.quantities {
            if let Some(projected) = projection.as_deref_mut() {
                merge_quantity(
                    &mut projected
                        .entry((
                            attribution::AttributionKey::new(&key, &tuple),
                            member.clone(),
                        ))
                        .or_default()
                        .value
                        .recorded
                        .quantities,
                    &quantity_key,
                    &quantity,
                )?;
            }
            merge_quantity(
                &mut cells
                    .entry((key, member.clone()))
                    .or_default()
                    .recorded
                    .quantities,
                &quantity_key,
                &quantity,
            )?;
        }
        for ((key, kind, currency), money) in data.money {
            if let Some(projected) = projection.as_deref_mut() {
                merge_money(
                    &mut projected
                        .entry((
                            attribution::AttributionKey::new(&key, &tuple),
                            member.clone(),
                        ))
                        .or_default()
                        .value
                        .recorded
                        .money,
                    &(kind, currency.clone()),
                    &money,
                )?;
            }
            merge_money(
                &mut cells
                    .entry((key, member.clone()))
                    .or_default()
                    .recorded
                    .money,
                &(kind, currency),
                &money,
            )?;
        }
    }
    Ok((ledger.events.len(), fetched))
}

pub(super) fn merge_quantity(
    values: &mut Vec<(QuantityKey, Quantity)>,
    key: &QuantityKey,
    value: &Quantity,
) -> Result<(), ReportError> {
    if let Some((_, target)) = values.iter_mut().find(|(candidate, _)| candidate == key) {
        sum(&mut target.units, value.units)?;
        sum(&mut target.covered_units, value.covered_units)?;
        add_count(&mut target.observations, value.observations)?;
        add_count(&mut target.stated_observations, value.stated_observations)?;
        add_count(&mut target.covered_observations, value.covered_observations)?;
    } else {
        values.push((key.clone(), value.clone()));
        values.sort_by(|a, b| a.0.cmp(&b.0));
    }
    Ok(())
}
pub(super) fn merge_money(
    values: &mut Vec<((i64, String), Money)>,
    key: &(i64, String),
    value: &Money,
) -> Result<(), ReportError> {
    if let Some((_, target)) = values.iter_mut().find(|(candidate, _)| candidate == key) {
        sum(&mut target.nanos, value.nanos)?;
        add_count(&mut target.contributions, value.contributions)?;
    } else {
        values.push((key.clone(), value.clone()));
        values.sort_by(|a, b| a.0.cmp(&b.0));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Generations {
    rate: i64,
    config: i64,
    plan: i64,
}
fn generations(conn: &Connection) -> Result<Generations, ReportError> {
    Ok(conn.query_row(
        "SELECT rate_generation,config_generation,plan_generation FROM valuation_state WHERE id=1",
        [],
        |r| {
            Ok(Generations {
                rate: r.get(0)?,
                config: r.get(1)?,
                plan: r.get(2)?,
            })
        },
    )?)
}

struct Pending {
    hour: Hour,
    revision: i64,
    attribution_revision: i64,
    attribution: attribution::ValuationProjection,
    attribution_resolver: crate::attribution::Resolver,
    attribution_deferred: bool,
    main: bool,
    generations: Generations,
    after: Option<i64>,
    cells: Cells,
    fetched: bool,
    ready: bool,
}

/// Independent cursor and queue; callers assign this builder its own time budget.
#[derive(Default)]
pub struct ValuationMaintenance {
    pending: Option<Pending>,
    after: Option<Hour>,
}

impl ValuationMaintenance {
    /// Idle cycles use a reader snapshot and need no writer transaction.
    pub fn needs_prepare(conn: &Connection) -> Result<bool, ReportError> {
        Ok(conn.query_row("SELECT config<>?1 OR rate_generation<>(SELECT generation FROM rate_cache_state WHERE id=1) FROM valuation_state WHERE id=1",[VALUATION_FORMAT],|r|r.get(0))?)
    }

    /// Synchronize generations and consume indexed rate invalidations atomically.
    pub fn prepare(tx: &Transaction<'_>) -> Result<(), ReportError> {
        if tx.execute("UPDATE valuation_state SET config=?1,config_generation=config_generation+1 WHERE id=1 AND config<>?1",[VALUATION_FORMAT])? != 0 {
            tx.execute(
                "UPDATE valuation_hour SET state='dirty',revision=revision+1",
                [],
            )?;
        }
        let old = generations(tx)?;
        let rate: i64 = tx.query_row(
            "SELECT generation FROM rate_cache_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if rate != old.rate {
            // Publishing even an unfetched hour requires a current summary.
            // Rate invalidation finds those hours by model in summary_cell.
            tx.execute("UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0 AND hour_number IN (SELECT hour_number FROM rate_dirty_hour)",[])?;
            tx.execute(
                "UPDATE valuation_state SET rate_generation=?1 WHERE id=1",
                [rate],
            )?;
            tx.execute("DELETE FROM rate_dirty_hour", [])?;
        }
        Ok(())
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn read_chunk(&mut self, conn: &Connection) -> Result<bool, ReportError> {
        if self.pending.is_none() {
            let read = |after: Option<Hour>| -> rusqlite::Result<Option<Hour>> {
                let main=conn.query_row(&format!("SELECT v.kind,v.hour_number FROM valuation_hour v JOIN summary_hour s USING(kind,hour_number) WHERE v.state='dirty' AND s.built_revision=s.revision AND s.format_version={FORMAT} AND (?1 IS NULL OR (v.kind,v.hour_number)>(?1,?2)) ORDER BY v.kind,v.hour_number LIMIT 1"),params![after.map(|h|h.kind),after.map(|h|h.number)],|r|Ok(Hour {kind:r.get(0)?,number:r.get(1)?})).optional()?;
                if main.is_some() {
                    return Ok(main);
                }
                conn.query_row(&format!("SELECT a.kind,a.hour_number FROM attribution_hour a INDEXED BY attribution_valuation_stale JOIN summary_hour s USING(kind,hour_number) WHERE (a.valuation_revision IS NULL OR a.valuation_attribution_revision IS NOT a.revision) AND s.built_revision=s.revision AND s.format_version={FORMAT} AND (?1 IS NULL OR (a.kind,a.hour_number)>(?1,?2)) ORDER BY a.kind,a.hour_number LIMIT 1"),params![after.map(|h|h.kind),after.map(|h|h.number)],|r|Ok(Hour {kind:r.get(0)?,number:r.get(1)?})).optional()
            };
            let hour = read(self.after)?.or(read(None)?);
            let Some(hour) = hour else {
                return Ok(false);
            };
            self.after = Some(hour);
            self.pending = Some(Pending {
                hour,
                revision: valuation_revision(conn, hour)?,
                attribution_revision: attribution::own_revision(conn, hour)?,
                attribution: attribution::ValuationProjection::new(),
                attribution_resolver: crate::attribution::Resolver::default(),
                attribution_deferred: false,
                main: conn.query_row(
                    "SELECT state='dirty' FROM valuation_hour WHERE kind=?1 AND hour_number=?2",
                    params![hour.kind, hour.number],
                    |r| r.get(0),
                )?,
                generations: generations(conn)?,
                after: None,
                cells: Cells::new(),
                fetched: true,
                ready: false,
            });
        }
        let mut pending = self
            .pending
            .take()
            .ok_or(ReportError::Integrity("valuation build absent"))?;
        let attribution_changed =
            pending.attribution_revision != attribution::own_revision(conn, pending.hour)?;
        if pending.revision != valuation_revision(conn, pending.hour)?
            || (attribution_changed && !pending.main)
            || pending.generations != generations(conn)?
            || pending.generations.rate != cache_generation(conn)?
            || !summary_current(conn, pending.hour)?
        {
            return Ok(false);
        }
        pending.attribution_deferred |= attribution_changed;
        let last = conn.query_row(&page_end_sql(pending.hour, pending.after), [], |r| {
            r.get::<_, Option<i64>>(0)
        })?;
        let count = if let Some(last) = last {
            let (count, fetched) = reduce_both(
                conn,
                &chunk_filter(pending.hour, pending.after, last),
                &PlanBook::load(conn)?,
                &mut pending.cells,
                Some((&mut pending.attribution, &mut pending.attribution_resolver)),
            )?;
            pending.fetched &= fetched;
            count
        } else {
            0
        };
        pending.after = last;
        pending.ready = count < CHUNK;
        // Bound publication just as summaries do. An oversized hour remains
        // pending rather than monopolizing the writer with an unbounded commit.
        let main_rows = pending
            .cells
            .values()
            .map(|cell| 1 + cell.recorded.quantities.len() + cell.recorded.money.len())
            .sum::<usize>();
        pending.attribution_deferred |=
            pending.main && main_rows + pending.attribution.len() > MAX_PUBLICATION_ROWS;
        if (if pending.main {
            main_rows
        } else {
            pending.attribution.len()
        }) > MAX_PUBLICATION_ROWS
        {
            return Ok(false);
        }
        let ready = pending.ready;
        self.pending = Some(pending);
        Ok(ready)
    }

    pub fn publish(&mut self, tx: &Transaction<'_>) -> Result<bool, ReportError> {
        let Some(pending) = self.pending.take() else {
            return Ok(false);
        };
        let attribution_changed =
            pending.attribution_revision != attribution::own_revision(tx, pending.hour)?;
        if !pending.ready
            || pending.revision != valuation_revision(tx, pending.hour)?
            || (attribution_changed && !pending.main)
            || pending.generations != generations(tx)?
            || pending.generations.rate != cache_generation(tx)?
            || !summary_current(tx, pending.hour)?
        {
            return Ok(false);
        }
        let h = pending.hour;
        if pending.main {
            tx.execute(
                "DELETE FROM valuation_cell WHERE kind=?1 AND hour_number=?2",
                params![h.kind, h.number],
            )?;
            if pending.fetched {
                for ((key, member), cell) in pending.cells {
                    tx.execute(
                        "INSERT INTO valuation_cell VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                        params![
                            h.kind,
                            h.number,
                            encode(&(&key, &member))?,
                            encode(&key)?,
                            member.plan,
                            member.plan_account,
                            member.record.as_ref().map(encode).transpose()?,
                            encode(&cell.recorded)?,
                            cell.numerator.to_string(),
                            encode(&cell.unvalued)?
                        ],
                    )?;
                }
            }
            tx.execute("UPDATE valuation_hour SET rate_generation=?3,config_generation=?4,plan_generation=?5,state=?6 WHERE kind=?1 AND hour_number=?2",params![h.kind,h.number,pending.generations.rate,pending.generations.config,pending.generations.plan,if pending.fetched {"built"} else {"not_yet_valued"}])?;
        }
        if !pending.attribution_deferred && !attribution_changed {
            attribution::replace_valuation(
                tx,
                h,
                pending.revision,
                pending.attribution_revision,
                pending.attribution,
                pending.fetched,
            )?;
        }
        Ok(true)
    }
}

fn cache_generation(conn: &Connection) -> Result<i64, ReportError> {
    Ok(conn.query_row(
        "SELECT generation FROM rate_cache_state WHERE id=1",
        [],
        |r| r.get(0),
    )?)
}

// The model-to-hour invalidation lookup uses summary cells. Persisting a
// valuation ahead of its summary could hide a newly added model from that lookup.
fn summary_current(conn: &Connection, hour: Hour) -> Result<bool, ReportError> {
    Ok(conn.query_row("SELECT built_revision IS revision AND format_version IS ?3 FROM summary_hour WHERE kind=?1 AND hour_number=?2",params![hour.kind,hour.number,FORMAT],|r|r.get(0))?)
}

fn valuation_revision(conn: &Connection, hour: Hour) -> Result<i64, ReportError> {
    Ok(conn.query_row(
        "SELECT revision FROM valuation_hour WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
        |r| r.get(0),
    )?)
}

#[cfg(test)]
mod tests;
