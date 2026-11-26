//! Independent projections share ledger reads and revision checks, not report keys.
use super::valuation::{PlanMember, Recorded, ValuedCell};
use super::*;
pub(crate) use crate::attribution::Attribution;
use crate::broca::ScopeStamp;
use serde::{Deserialize, Serialize};

pub(super) const DELETE_CELLS_SQL: &str =
    "DELETE FROM attribution_cell WHERE kind=?1 AND hour_number=?2";

impl Attribution {
    pub fn dictionary_value(&self) -> Result<Option<String>, ReportError> {
        // The fixed null dictionary row represents Not attributed, avoiding a
        // second spelling for the all-null tuple in persisted summary keys.
        if self == &Self::default() {
            Ok(None)
        } else {
            encode(self).map(Some)
        }
    }
}

#[derive(Default)]
pub(super) struct ScopeStamps {
    broca: HashMap<i64, (ScopeStamp, String)>,
    media: HashMap<i64, String>,
}

pub(super) fn stamps_sql(filter: &str) -> String {
    format!(
        "SELECT s.event_key,s.agent_id,s.agent_run_id,s.flow_id,s.call_kind
             FROM (SELECT event_key FROM usage_event WHERE {filter}) e
             CROSS JOIN broca_scope_stamp s ON s.event_key=e.event_key"
    )
}

pub(super) fn stamps(conn: &Connection, filter: &str) -> Result<ScopeStamps, ReportError> {
    let mut statement = conn.prepare(&stamps_sql(filter))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get(0)?,
            (
                ScopeStamp {
                    agent_id: row.get(1)?,
                    agent_run_id: row.get(2)?,
                    flow_id: row.get(3)?,
                },
                row.get(4)?,
            ),
        ))
    })?;
    let broca = rows.collect::<rusqlite::Result<_>>()?;
    let mut statement = conn.prepare(&format!(
        "SELECT c.event_key,c.agent_id FROM (SELECT event_key FROM usage_event WHERE {filter}) e
         CROSS JOIN media_caller c ON c.event_key=e.event_key WHERE c.agent_id IS NOT NULL"
    ))?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(ScopeStamps {
        broca,
        media: rows.collect::<rusqlite::Result<_>>()?,
    })
}

pub(super) fn resolve(
    resolver: &mut crate::attribution::Resolver,
    conn: &Connection,
    ledger: &Ledger,
    event: &LedgerEvent,
    stamps: &ScopeStamps,
) -> Result<(Attribution, bool), ReportError> {
    let tuple = resolver
        .resolve(
            conn,
            event.attribution_runner.map(|id| ledger.values.name(id)),
            event.session.map(|id| ledger.values.name(id)),
            event.run_ref.map(|id| ledger.values.name(id)),
            event.flow_ref.map(|id| ledger.values.name(id)),
            event.occurred_at_ms,
        )
        .map_err(|_| ReportError::Integrity("invalid attribution evidence"))?;
    match stamps.broca.get(&event.key) {
        Some((stamp, kind)) => resolver
            .apply_stamp(conn, tuple, stamp, kind, event.occurred_at_ms)
            .map_err(|_| ReportError::Integrity("invalid attribution evidence")),
        None => Ok(match stamps.media.get(&event.key) {
            Some(agent) => tuple.with_media_caller(agent),
            None => (tuple, false),
        }),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct AttributionKey {
    pub(super) currency: Option<String>,
    pub(super) attribution: Attribution,
    pub(super) provider: Option<String>,
    pub(super) model: Option<String>,
}
impl AttributionKey {
    pub(super) fn new(key: &Key, attribution: &Attribution) -> Self {
        Self {
            currency: key.1.clone(),
            attribution: attribution.clone(),
            provider: key.0[Grouping::Provider as usize].clone(),
            model: key.0[Grouping::Model as usize].clone(),
        }
    }
    fn ids(
        &self,
        tx: &Transaction<'_>,
        values: &mut HashMap<Option<String>, i64>,
    ) -> Result<[i64; 4], ReportError> {
        let mut ids = [0; 4];
        let tuple = self.attribution.dictionary_value()?;
        for (id, text) in ids
            .iter_mut()
            .zip([&self.currency, &tuple, &self.provider, &self.model])
        {
            *id = value_id(tx, values, text)?;
        }
        Ok(ids)
    }
}

#[derive(Default)]
pub(super) struct Projection {
    pub(super) cells: BTreeMap<AttributionKey, Payload>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "ReadPayload")]
pub(super) struct Payload {
    #[serde(flatten)]
    pub(super) recorded: Recorded,
    // The projection no longer has charge_basis in its key. Preserve its
    // exact metered portion before combining different charge bases.
    #[serde(with = "exact")]
    pub(super) metered: i128,
    #[serde(skip_serializing_if = "crate::report::is_zero_calls")]
    pub(super) stamp_disagreement_calls: u64,
}

// Decode known fields directly rather than buffering the entire nested JSON
// through serde flatten. The stored representation stays byte-identical.
#[derive(Deserialize)]
struct ReadPayload {
    #[serde(default)]
    base_rate_tier_untested_tokens: i64,
    cell: Cell,
    quantities: Vec<(QuantityKey, Quantity)>,
    money: Vec<((i64, String), Money)>,
    #[serde(with = "exact")]
    metered: i128,
    #[serde(default)]
    stamp_disagreement_calls: u64,
}
impl From<ReadPayload> for Payload {
    fn from(value: ReadPayload) -> Self {
        Self {
            recorded: Recorded {
                base_rate_tier_untested_tokens: value.base_rate_tier_untested_tokens,
                cell: value.cell,
                quantities: value.quantities,
                money: value.money,
            },
            metered: value.metered,
            stamp_disagreement_calls: value.stamp_disagreement_calls,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct ProjectedValue {
    pub(super) value: ValuedCell,
    pub(super) metered: i128,
    pub(super) stamp_disagreement_calls: u64,
    pub(super) original_keys: BTreeSet<Key>,
}
impl Projection {
    fn merge(
        &mut self,
        tuple: &Attribution,
        disagrees: bool,
        data: &Data,
    ) -> Result<(), ReportError> {
        for (key, cell) in &data.cells {
            let projected = self
                .cells
                .entry(AttributionKey::new(key, tuple))
                .or_default();
            if key.0[Grouping::ChargeBasis as usize].as_deref() == Some("metered_api") {
                sum(&mut projected.metered, cell.total)?;
            }
            projected.recorded.cell.merge(cell)?;
            if disagrees {
                add_count(&mut projected.stamp_disagreement_calls, cell.events)?;
            }
        }
        for ((key, qkey), q) in &data.quantities {
            valuation::merge_quantity(
                &mut self
                    .cells
                    .entry(AttributionKey::new(key, tuple))
                    .or_default()
                    .recorded
                    .quantities,
                qkey,
                q,
            )?;
        }
        for ((key, kind, currency), money) in &data.money {
            valuation::merge_money(
                &mut self
                    .cells
                    .entry(AttributionKey::new(key, tuple))
                    .or_default()
                    .recorded
                    .money,
                &(*kind, currency.clone()),
                money,
            )?;
        }
        Ok(())
    }
    pub(super) fn len(&self) -> usize {
        self.cells.len()
    }
    pub(super) fn replace(
        &self,
        tx: &Transaction<'_>,
        hour: Hour,
        main_revision: i64,
        own_revision: i64,
    ) -> Result<(), ReportError> {
        tx.execute(DELETE_CELLS_SQL, params![hour.kind, hour.number])?;
        let mut values = HashMap::new();
        for (key, cell) in &self.cells {
            let [currency, tuple, provider, model] = key.ids(tx, &mut values)?;
            tx.execute(
                "INSERT INTO attribution_cell VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    hour.kind,
                    hour.number,
                    currency,
                    tuple,
                    provider,
                    model,
                    encode(cell)?
                ],
            )?;
        }
        tx.execute("UPDATE attribution_hour SET summary_revision=?3,summary_attribution_revision=?4 WHERE kind=?1 AND hour_number=?2",params![hour.kind,hour.number,main_revision,own_revision])?;
        Ok(())
    }
}

pub(super) type ValuationProjection = BTreeMap<(AttributionKey, PlanMember), ProjectedValue>;
pub(super) fn replace_valuation(
    tx: &Transaction<'_>,
    hour: Hour,
    main_revision: i64,
    own_revision: i64,
    cells: ValuationProjection,
    fetched: bool,
) -> Result<(), ReportError> {
    tx.execute(
        "DELETE FROM attribution_valuation_cell WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
    )?;
    let mut values = HashMap::new();
    if fetched {
        for ((key, member), cell) in cells {
            let [currency, tuple, provider, model] = key.ids(tx, &mut values)?;
            let recorded = Payload {
                recorded: cell.value.recorded,
                metered: cell.metered,
                stamp_disagreement_calls: cell.stamp_disagreement_calls,
            };
            tx.execute("INSERT INTO attribution_valuation_cell VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![hour.kind,hour.number,currency,tuple,provider,model,encode(&member)?,member.plan,member.plan_account,member.record.as_ref().map(encode).transpose()?,encode(&recorded)?,cell.value.numerator.to_string(),encode(&cell.value.unvalued)?])?;
        }
    }
    tx.execute("UPDATE attribution_hour SET valuation_revision=?3,valuation_attribution_revision=?4 WHERE kind=?1 AND hour_number=?2",params![hour.kind,hour.number,main_revision,own_revision])?;
    Ok(())
}

pub(super) fn own_revision(conn: &Connection, hour: Hour) -> Result<i64, ReportError> {
    Ok(conn
        .query_row(
            "SELECT revision FROM attribution_hour WHERE kind=?1 AND hour_number=?2",
            params![hour.kind, hour.number],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}
pub(super) fn summary_current(conn: &Connection, hour: Hour) -> Result<bool, ReportError> {
    Ok(conn.query_row(&format!("SELECT built_revision=revision AND format_version={FORMAT} AND NOT EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=s.kind AND d.hour_number=s.hour_number) FROM summary_hour s WHERE kind=?1 AND hour_number=?2"),params![hour.kind,hour.number],|r|r.get::<_,Option<bool>>(0)).optional()?.flatten().unwrap_or(false))
}
pub(super) fn next_summary(
    conn: &Connection,
    after: Option<Hour>,
) -> Result<Option<Hour>, ReportError> {
    let read = |after: Option<Hour>| {
        conn.query_row("SELECT kind,hour_number FROM attribution_hour INDEXED BY attribution_summary_stale WHERE (summary_revision IS NULL OR summary_attribution_revision IS NOT revision) AND (?1 IS NULL OR (kind,hour_number)>(?1,?2)) ORDER BY kind,hour_number LIMIT 1",params![after.map(|h|h.kind),after.map(|h|h.number)],|r|Ok(Hour{kind:r.get(0)?,number:r.get(1)?})).optional()
    };
    Ok(read(after)?.or(read(None)?))
}

pub(super) fn observe(
    data: &mut Data,
    projection: &mut Projection,
    conn: &Connection,
    filter: &str,
    resolver: &mut crate::attribution::Resolver,
) -> Result<usize, ReportError> {
    let ledger = load_scoped_ledger(conn, &DIMENSIONS, Some(filter))?;
    let stamps = stamps(conn, filter)?;
    let mut groups: BTreeMap<(Attribution, bool), Data> = BTreeMap::new();
    let mut events = HashMap::new();
    for event in &ledger.events {
        let tuple = resolve(resolver, conn, &ledger, event, &stamps)?;
        let key = groups
            .entry(tuple.clone())
            .or_default()
            .observe_event(&ledger, event)?;
        events.insert(
            event.key,
            (
                tuple,
                key,
                event.occurred_at_ms.is_some() && event.is_fully_priced(),
            ),
        );
    }
    let selected = format!("SELECT event_key FROM usage_event WHERE {filter}");
    let quantity = reported_quantity_sql();
    let mut stmt = conn.prepare(&format!(
        "SELECT m.event_key,m.unit,m.role,{quantity},m.quantity_exponent,
         CASE WHEN {DERIVED_UNPRICED_SQL} THEN 'unpriced' ELSE h.outcome END
         FROM measurement m JOIN usage_event e ON e.event_key=m.event_key
         LEFT JOIN priced_charge h ON h.event_key=m.event_key AND h.role=m.role AND h.unit=m.unit
         AND NOT EXISTS(SELECT 1 FROM priced_charge s WHERE s.supersedes=h.charge_key)
         WHERE m.event_key IN ({selected})"
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (tuple, key, complete) =
            events
                .get(&row.get::<_, i64>(0)?)
                .ok_or(ReportError::Integrity(
                    "attribution measurement event absent",
                ))?;
        groups
            .get_mut(tuple)
            .ok_or(ReportError::Integrity("attribution group absent"))?
            .observe_quantity(
                key,
                *complete,
                (row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?),
                row.get::<_, Option<String>>(5)?.as_deref() == Some("covered"),
            )?;
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT event_key,currency,amount_nanos FROM adjustment WHERE event_key IN ({selected})"
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (tuple, key, _) = events
            .get(&row.get::<_, i64>(0)?)
            .ok_or(ReportError::Integrity(
                "attribution adjustment event absent",
            ))?;
        let money = groups
            .get_mut(tuple)
            .ok_or(ReportError::Integrity("attribution group absent"))?
            .money
            .entry((key.clone(), 1, row.get(1)?))
            .or_default();
        sum(&mut money.nanos, i128::from(row.get::<_, i64>(2)?))?;
        add_count(&mut money.contributions, 1)?;
    }
    for ((tuple, disagrees), group) in groups {
        projection.merge(&tuple, disagrees, &group)?;
        add_count(&mut data.events, group.events)?;
        min(&mut data.first, group.first);
        max(&mut data.last, group.last);
        for (key, cell) in group.cells {
            data.cells.entry(key).or_default().merge(&cell)?;
        }
        for (key, q) in group.quantities {
            let to = data.quantities.entry(key).or_default();
            sum(&mut to.units, q.units)?;
            add_count(&mut to.observations, q.observations)?;
            add_count(&mut to.stated_observations, q.stated_observations)?;
            sum(&mut to.covered_units, q.covered_units)?;
            add_count(&mut to.covered_observations, q.covered_observations)?;
        }
        for (key, m) in group.money {
            let to = data.money.entry(key).or_default();
            sum(&mut to.nanos, m.nanos)?;
            add_count(&mut to.contributions, m.contributions)?;
        }
        for (key, n) in group.frequencies {
            add_count(data.frequencies.entry(key).or_default(), n)?;
        }
    }
    crate::runs::observe(conn, filter, &mut data.runs)?;
    Ok(ledger.events.len())
}

#[cfg(test)]
pub(super) fn bump(conn: &Connection, hour: Hour) {
    conn.execute(
        "UPDATE attribution_hour SET revision=revision+1 WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
    )
    .unwrap();
}
