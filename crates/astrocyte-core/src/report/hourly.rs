//! Rebuildable hour partitions; source facts remain authoritative.
#[cfg(test)]
mod copy_bench;
use super::*;
use rusqlite::Transaction;
use serde::Deserialize;
use std::collections::HashMap;

const FORMAT: i64 = crate::runs::FORMAT;
const CHUNK: usize = 5_000;
const MAX_PUBLICATION_ROWS: usize = 50_000;
const HOUR_MS: i64 = 3_600_000;

/// How long after one full-ledger audit pass completes the next one starts.
///
/// A pass re-reads every event, measurement and charge in the store, so it
/// costs disk reads in proportion to the whole ledger. It is only a backstop:
/// the triggers mark hours dirty in the same transaction as every write, so a
/// mismatch means a write path that bypassed them. Daily keeps such a miss
/// bounded to about a day without re-reading the store every hour.
const AUDIT_INTERVAL_MS: i64 = 24 * HOUR_MS;
const DIMENSIONS: [Grouping; 8] = [
    Grouping::Provider,
    Grouping::Model,
    Grouping::Account,
    Grouping::AuthSelection,
    Grouping::ChargeBasis,
    Grouping::WorkClass,
    Grouping::Project,
    Grouping::Producer,
];

type Key = ([Option<String>; 8], Option<String>);
type QuantityKey = (String, String, i32);
type FrequencyKey = (String, Option<String>, String, i64);

mod exact {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Hour {
    kind: i64,
    number: i64,
}
impl Hour {
    fn filter(self) -> String {
        if self.kind == 1 {
            return "occurred_at_ms IS NULL".into();
        }
        let from = i128::from(self.number) * i128::from(HOUR_MS);
        let to = from + i128::from(HOUR_MS);
        match (i64::try_from(from), i64::try_from(to)) {
            (Ok(from), Ok(to)) => format!("occurred_at_ms>={from} AND occurred_at_ms<{to}"),
            _ => format!(
                "occurred_at_ms/3600000-(occurred_at_ms%3600000<0)={}",
                self.number
            ),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Cell {
    events: u64,
    priced: u64,
    measurements: u64,
    zero_measurements: u64,
    budget_uncertain: u64,
    priced_measurements: u64,
    head_contributions: u64,
    oldest: Option<i64>,
    #[serde(with = "exact")]
    heads: i128,
    #[serde(with = "exact")]
    adjustments: i128,
    #[serde(with = "exact")]
    total: i128,
    reasons: BTreeMap<String, u64>,
    declines: BTreeMap<String, u64>,
    work_excluded: BTreeMap<String, u64>,
    work_unknown: u64,
    subscription_complete: u64,
    subscription_missing_account: u64,
    covered_measurements: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Quantity {
    units: i128,
    observations: u64,
    covered_units: i128,
    covered_observations: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Money {
    nanos: i128,
    contributions: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Data {
    runs: crate::runs::Cells,
    cells: BTreeMap<Key, Cell>,
    quantities: BTreeMap<(Key, QuantityKey), Quantity>,
    money: BTreeMap<(Key, i64, String), Money>,
    frequencies: BTreeMap<FrequencyKey, u64>,
    events: u64,
    first: Option<i64>,
    last: Option<i64>,
}
fn sum(left: &mut i128, right: i128) -> Result<(), ReportError> {
    *left = left.checked_add(right).ok_or(ReportError::Arithmetic)?;
    Ok(())
}
fn min(left: &mut Option<i64>, right: Option<i64>) {
    if let Some(right) = right {
        *left = Some(left.map_or(right, |left| left.min(right)));
    }
}
fn max(left: &mut Option<i64>, right: Option<i64>) {
    if let Some(right) = right {
        *left = Some(left.map_or(right, |left| left.max(right)));
    }
}
fn encode<T: Serialize>(value: &T) -> Result<String, ReportError> {
    serde_json::to_string(value).map_err(|_| ReportError::Integrity("invalid summary payload"))
}
fn decode(value: &str) -> Result<Cell, ReportError> {
    serde_json::from_str(value).map_err(|_| ReportError::Integrity("invalid summary payload"))
}
fn key(ledger: &Ledger, event: &LedgerEvent) -> Key {
    (
        event
            .dimensions
            .map(|value| ledger.values.optional_name(value)),
        ledger.values.optional_name(event.currency),
    )
}
fn is_subscription(key: &Key) -> bool {
    key.0[Grouping::ChargeBasis as usize].as_deref() == Some("subscription_included")
}

impl Data {
    fn observe(&mut self, conn: &Connection, filter: &str) -> Result<usize, ReportError> {
        let ledger = load_scoped_ledger(conn, &DIMENSIONS, Some(filter))?;
        let mut event_keys = HashMap::new();
        for event in &ledger.events {
            let key = key(&ledger, event);
            event_keys.insert(
                event.key,
                (
                    key.clone(),
                    event.occurred_at_ms.is_some() && event.is_fully_priced(),
                ),
            );
            let cell = self.cells.entry(key.clone()).or_default();
            add_count(&mut self.events, 1)?;
            add_count(&mut cell.events, 1)?;
            add_count(&mut cell.measurements, event.measurement_rows())?;
            add_count(
                &mut cell.zero_measurements,
                u64::from(event.measurement_rows() == 0),
            )?;
            add_count(&mut cell.priced, u64::from(event.is_fully_priced()))?;
            add_count(
                &mut cell.budget_uncertain,
                u64::from(event.examined == 0 || !event.declined.is_empty()),
            )?;
            add_count(&mut cell.priced_measurements, event.priced_measurements)?;
            let contributions = event
                .priced_measurements
                .checked_add(event.covered_measurements)
                .ok_or(ReportError::Arithmetic)?;
            add_count(&mut cell.head_contributions, contributions)?;
            min(&mut cell.oldest, event.oldest_resolved_at_ms);
            sum(&mut cell.heads, event.priced_nanos)?;
            sum(&mut cell.adjustments, event.adjustment_nanos)?;
            sum(&mut cell.total, i128::from(event.report_total()?))?;
            min(&mut self.first, event.occurred_at_ms);
            max(&mut self.last, event.occurred_at_ms);
            if !event.is_fully_priced() {
                add_count(
                    cell.reasons
                        .entry(event.exclusion_reason().label(&ledger.reasons))
                        .or_default(),
                    1,
                )?;
            }
            for (reason, count) in &event.declined {
                add_count(
                    cell.declines
                        .entry(ledger.reasons.name(*reason).into())
                        .or_default(),
                    *count,
                )?;
            }
            if event.occurred_at_ms.is_some() && event.is_fully_priced() {
                if let Some(currency) = &key.1 {
                    let producer = key.0[Grouping::Producer as usize]
                        .clone()
                        .ok_or(ReportError::Integrity("summary producer absent"))?;
                    add_count(
                        self.frequencies
                            .entry((
                                producer,
                                key.0[Grouping::WorkClass as usize].clone(),
                                currency.clone(),
                                event.current_head_total()?,
                            ))
                            .or_default(),
                        1,
                    )?;
                } else {
                    add_count(&mut cell.work_unknown, 1)?;
                }
                if is_subscription(&key) {
                    add_count(&mut cell.subscription_complete, 1)?;
                    add_count(&mut cell.covered_measurements, event.covered_measurements)?;
                }
            } else {
                add_count(
                    cell.work_excluded
                        .entry(event.exclusion_reason().label(&ledger.reasons))
                        .or_default(),
                    1,
                )?;
            }
            if is_subscription(&key)
                && event.occurred_at_ms.is_some()
                && key.0[Grouping::Account as usize]
                    .as_deref()
                    .is_none_or(str::is_empty)
            {
                add_count(&mut cell.subscription_missing_account, 1)?;
            }
            if contributions > 0 {
                let currency = key
                    .1
                    .clone()
                    .ok_or(ReportError::Integrity("summary head currency absent"))?;
                let money = self.money.entry((key.clone(), 0, currency)).or_default();
                sum(&mut money.nanos, event.priced_nanos)?;
                add_count(&mut money.contributions, contributions)?;
            }
        }
        let selected = format!("SELECT event_key FROM usage_event WHERE {filter}");
        let mut statement=conn.prepare(&format!("SELECT m.event_key,m.unit,m.role,m.quantity_units,m.quantity_exponent,h.outcome
            FROM measurement m LEFT JOIN priced_charge h ON h.event_key=m.event_key AND h.role=m.role AND h.unit=m.unit
            AND NOT EXISTS(SELECT 1 FROM priced_charge s WHERE s.supersedes=h.charge_key)
            WHERE m.event_key IN ({selected})"))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let event_key: i64 = row.get(0)?;
            let (key, complete) = event_keys
                .get(&event_key)
                .ok_or(ReportError::Integrity("summary measurement event absent"))?;
            let units: Option<i64> = row.get(3)?;
            let exponent: Option<i32> = row.get(4)?;
            let quantity = self
                .quantities
                .entry((
                    key.clone(),
                    (row.get(1)?, row.get(2)?, exponent.unwrap_or(1)),
                ))
                .or_default();
            add_count(&mut quantity.observations, 1)?;
            if let Some(units) = units {
                sum(&mut quantity.units, i128::from(units))?;
            }
            if *complete
                && is_subscription(key)
                && row.get::<_, Option<String>>(5)?.as_deref() == Some("covered")
            {
                add_count(&mut quantity.covered_observations, 1)?;
                if let Some(units) = units {
                    sum(&mut quantity.covered_units, i128::from(units))?;
                }
            }
        }
        let mut statement=conn.prepare(&format!("SELECT event_key,currency,amount_nanos FROM adjustment WHERE event_key IN ({selected})"))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let event_key: i64 = row.get(0)?;
            let (key, _) = event_keys
                .get(&event_key)
                .ok_or(ReportError::Integrity("summary adjustment event absent"))?;
            let money = self.money.entry((key.clone(), 1, row.get(1)?)).or_default();
            sum(&mut money.nanos, i128::from(row.get::<_, i64>(2)?))?;
            add_count(&mut money.contributions, 1)?;
        }
        crate::runs::observe(conn, filter, &mut self.runs)?;
        Ok(ledger.events.len())
    }
}

/// Dictionary values looked up by id on first use and remembered.
///
/// The dictionary keeps every value ever interned and is never pruned, so a
/// read that loaded all of it would cost all history even for one hour. Each
/// id is a primary-key lookup instead, and a read pays for the distinct values
/// it touches. An id with no row is the same integrity error as before.
struct Values<'c> {
    conn: &'c Connection,
    known: HashMap<i64, Option<String>>,
}
impl<'c> Values<'c> {
    fn new(conn: &'c Connection) -> Self {
        Self {
            conn,
            known: HashMap::new(),
        }
    }
    fn get(&mut self, id: i64) -> Result<Option<String>, ReportError> {
        if let Some(value) = self.known.get(&id) {
            return Ok(value.clone());
        }
        let value: Option<String> = self
            .conn
            .prepare_cached("SELECT value FROM summary_value WHERE value_id=?1")?
            .query_row([id], |row| row.get(0))
            .optional()?
            .ok_or(ReportError::Integrity("summary dictionary value absent"))?;
        self.known.insert(id, value.clone());
        Ok(value)
    }
}
fn value(values: &mut Values<'_>, id: i64) -> Result<Option<String>, ReportError> {
    values.get(id)
}
/// Finds or interns `text`, remembering ids already seen by this publication.
/// NULL is always id 0 (the dictionary's fixed first row).
fn value_id(
    tx: &Transaction<'_>,
    values: &mut HashMap<Option<String>, i64>,
    text: &Option<String>,
) -> Result<i64, ReportError> {
    if let Some(id) = values.get(text) {
        return Ok(*id);
    }
    let Some(present) = text else {
        return Ok(0);
    };
    if let Some(id) = tx
        .prepare_cached("SELECT value_id FROM summary_value WHERE value=?1")?
        .query_row([present], |row| row.get::<_, i64>(0))
        .optional()?
    {
        values.insert(text.clone(), id);
        return Ok(id);
    }
    tx.execute("INSERT INTO summary_value(value) VALUES(?1)", [text])?;
    let id = tx.last_insert_rowid();
    values.insert(text.clone(), id);
    Ok(id)
}
fn parse_exact(value: String) -> Result<i128, ReportError> {
    value
        .parse()
        .map_err(|_| ReportError::Integrity("summary integer invalid"))
}
fn stored(conn: &Connection, hour: Hour) -> Result<Data, ReportError> {
    let mut values = Values::new(conn);
    let mut data = Data::default();
    let mut keys = HashMap::new();
    let mut statement=conn.prepare("SELECT cell_id,provider,model,account,auth_selection,charge_basis,work_class,project,producer,currency,payload
        FROM summary_cell WHERE kind=?1 AND hour_number=?2")?;
    let mut rows = statement.query(params![hour.kind, hour.number])?;
    while let Some(row) = rows.next()? {
        let mut dims = [const { None }; 8];
        for (column, dim) in dims.iter_mut().enumerate() {
            *dim = value(&mut values, row.get(column + 1)?)?;
        }
        let key = (dims, value(&mut values, row.get(9)?)?);
        let cell = decode(&row.get::<_, String>(10)?)?;
        add_count(&mut data.events, cell.events)?;
        keys.insert(row.get::<_, i64>(0)?, key.clone());
        data.cells.insert(key, cell);
    }
    let bounds = conn
        .query_row(
            "SELECT first_ms,last_ms FROM summary_hour WHERE kind=?1 AND hour_number=?2",
            params![hour.kind, hour.number],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((first, last)) = bounds {
        data.first = first;
        data.last = last;
    }
    let mut statement=conn.prepare("SELECT q.cell_id,u.value,r.value,q.exponent,q.units,q.observations,q.covered_units,q.covered_observations
        FROM summary_quantity q JOIN summary_cell c USING(cell_id) JOIN summary_value u ON u.value_id=q.unit JOIN summary_value r ON r.value_id=q.role
        WHERE c.kind=?1 AND c.hour_number=?2")?;
    let mut rows = statement.query(params![hour.kind, hour.number])?;
    while let Some(row) = rows.next()? {
        let key = keys
            .get(&row.get::<_, i64>(0)?)
            .ok_or(ReportError::Integrity("summary quantity cell absent"))?
            .clone();
        data.quantities.insert(
            (key, (row.get(1)?, row.get(2)?, row.get(3)?)),
            Quantity {
                units: parse_exact(row.get(4)?)?,
                observations: row.get(5)?,
                covered_units: parse_exact(row.get(6)?)?,
                covered_observations: row.get(7)?,
            },
        );
    }
    let mut statement=conn.prepare("SELECT m.cell_id,m.kind,v.value,m.nanos,m.contributions FROM summary_money m JOIN summary_cell c USING(cell_id)
        JOIN summary_value v ON v.value_id=m.currency WHERE c.kind=?1 AND c.hour_number=?2")?;
    let mut rows = statement.query(params![hour.kind, hour.number])?;
    while let Some(row) = rows.next()? {
        let key = keys
            .get(&row.get::<_, i64>(0)?)
            .ok_or(ReportError::Integrity("summary money cell absent"))?
            .clone();
        data.money.insert(
            (key, row.get(1)?, row.get(2)?),
            Money {
                nanos: parse_exact(row.get(3)?)?,
                contributions: row.get(4)?,
            },
        );
    }
    let mut statement=conn.prepare("SELECT producer,work_class,currency,nanos,samples FROM summary_cost_frequency WHERE kind=?1 AND hour_number=?2")?;
    let mut rows = statement.query(params![hour.kind, hour.number])?;
    while let Some(row) = rows.next()? {
        let producer = value(&mut values, row.get(0)?)?
            .ok_or(ReportError::Integrity("summary producer absent"))?;
        let currency = value(&mut values, row.get(2)?)?
            .ok_or(ReportError::Integrity("summary currency absent"))?;
        data.frequencies.insert(
            (
                producer,
                value(&mut values, row.get(1)?)?,
                currency,
                row.get(3)?,
            ),
            row.get(4)?,
        );
    }
    data.runs = crate::runs::stored(conn, hour.kind, hour.number)?;
    Ok(data)
}
fn replace(
    tx: &Transaction<'_>,
    hour: Hour,
    revision: i64,
    data: &Data,
) -> Result<(), ReportError> {
    let mut values = HashMap::new();
    tx.execute(
        "DELETE FROM summary_cell WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
    )?;
    tx.execute(
        "DELETE FROM summary_cost_frequency WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
    )?;
    let mut cells = BTreeMap::new();
    for (key, cell) in &data.cells {
        let mut ids = [0_i64; 9];
        for (id, text) in ids
            .iter_mut()
            .zip(key.0.iter().chain(std::iter::once(&key.1)))
        {
            *id = value_id(tx, &mut values, text)?;
        }
        tx.execute("INSERT INTO summary_cell(kind,hour_number,provider,model,account,auth_selection,charge_basis,work_class,project,producer,currency,payload)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![hour.kind,hour.number,ids[0],ids[1],ids[2],ids[3],ids[4],ids[5],ids[6],ids[7],ids[8],encode(cell)?])?;
        cells.insert(key.clone(), tx.last_insert_rowid());
    }
    for ((key, (unit, role, exponent)), quantity) in &data.quantities {
        let unit = value_id(tx, &mut values, &Some(unit.clone()))?;
        let role = value_id(tx, &mut values, &Some(role.clone()))?;
        tx.execute(
            "INSERT INTO summary_quantity VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                cells[key],
                unit,
                role,
                exponent,
                quantity.units.to_string(),
                quantity.observations,
                quantity.covered_units.to_string(),
                quantity.covered_observations
            ],
        )?;
    }
    for ((key, kind, currency), money) in &data.money {
        let currency = value_id(tx, &mut values, &Some(currency.clone()))?;
        tx.execute(
            "INSERT INTO summary_money VALUES(?1,?2,?3,?4,?5)",
            params![
                cells[key],
                kind,
                currency,
                money.nanos.to_string(),
                money.contributions
            ],
        )?;
    }
    for ((producer, work_class, currency, nanos), samples) in &data.frequencies {
        let producer = value_id(tx, &mut values, &Some(producer.clone()))?;
        let class = value_id(tx, &mut values, work_class)?;
        let currency = value_id(tx, &mut values, &Some(currency.clone()))?;
        tx.execute(
            "INSERT INTO summary_cost_frequency VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                hour.kind,
                hour.number,
                producer,
                class,
                currency,
                nanos,
                samples
            ],
        )?;
    }
    crate::runs::replace(tx, hour.kind, hour.number, &data.runs)?;
    tx.execute("UPDATE summary_hour SET built_revision=?3,format_version=?4,events_total=?5,first_ms=?6,last_ms=?7 WHERE kind=?1 AND hour_number=?2",params![hour.kind,hour.number,revision,FORMAT,data.events,data.first,data.last])?;
    tx.execute(
        "DELETE FROM summary_dirty_hour WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
    )?;
    Ok(())
}

struct Build {
    hour: Hour,
    revision: i64,
    after: Option<i64>,
    data: Data,
    audit: bool,
    ready: bool,
    deferred: bool,
    chunks: u64,
    spent_ms: u64,
    max_chunk_ms: u64,
    started_at_ms: i64,
}
/// Chunked fresh reductions. Read chunks and publication use separate transactions;
/// the persistent revision fences out any source write between those snapshots.
#[derive(Default)]
pub struct SummaryMaintenance {
    pending: Option<Build>,
    prefer_audit: bool,
    finish_audit: bool,
    retry_after_ms: i64,
    dirty_after: Option<Hour>,
    stale_after: Option<Hour>,
    #[cfg(test)]
    max_publication_rows: Option<usize>,
}
#[derive(Debug, Default)]
pub struct SummaryOutcome {
    pub published: bool,
    pub audited: bool,
    pub mismatch: bool,
    pub examined: u64,
    pub audit_completed: bool,
    pub mismatches: u64,
    pub hours: u64,
    pub hour: Option<(i64, i64)>,
    pub deferred: bool,
    pub chunks: u64,
    pub spent_ms: u64,
    pub max_chunk_ms: u64,
    pub audit_elapsed_ms: i64,
}
fn revision(conn: &Connection, hour: Hour) -> Result<i64, ReportError> {
    Ok(conn
        .query_row(
            "SELECT revision FROM summary_hour WHERE kind=?1 AND hour_number=?2",
            params![hour.kind, hour.number],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}
/// The key of the last event in the next chunk of `hour` after the key
/// cursor: the `CHUNK`th of the hour's later events in key order, or the
/// hour's final one. Null when none is left.
///
/// The selection is pinned to the time index. Given only
/// `event_key>cursor ORDER BY event_key LIMIT`, SQLite prefers the key range
/// because it already yields key order, and then tests the time of every later
/// event in the ledger until the page fills: on the last chunk of an old hour
/// that is all history after it. The time index reads only the hour's events.
fn page_end_sql(hour: Hour, after: Option<i64>) -> String {
    let time = hour.filter();
    let after = after.map_or_else(String::new, |key| format!(" AND event_key>{key}"));
    format!(
        "SELECT MAX(event_key) FROM (SELECT event_key FROM usage_event INDEXED BY usage_event_summary_time
            WHERE ({time}){after} ORDER BY event_key LIMIT {CHUNK})"
    )
}
/// The rows of one chunk: the hour's events with keys after the cursor up to
/// and including the page end, which are exactly the page `page_end_sql`
/// chose. Bounded on both sides, the key range holds no event after the page.
fn chunk_filter(hour: Hour, after: Option<i64>, last: i64) -> String {
    let after = after.map_or_else(String::new, |key| format!(" AND event_key>{key}"));
    format!(
        "({}){after} AND event_key<={last} ORDER BY event_key",
        hour.filter()
    )
}
fn stale_hour_sql(after: Option<Hour>) -> String {
    // SQLite can use the partial index only when the format is a literal
    // matching its predicate, not a bound parameter.
    let after = after.map_or_else(String::new, |hour| {
        format!(" AND (kind,hour_number)>({},{})", hour.kind, hour.number)
    });
    format!(
        "SELECT kind,hour_number FROM summary_hour INDEXED BY summary_hour_stale
         WHERE (built_revision IS NOT revision OR format_version IS NOT {FORMAT}){after}
         ORDER BY kind,hour_number LIMIT 1"
    )
}
fn next_stale(conn: &Connection, after: Option<Hour>) -> Result<Option<Hour>, ReportError> {
    let read = |after| {
        conn.query_row(&stale_hour_sql(after), [], |row| {
            Ok(Hour {
                kind: row.get(0)?,
                number: row.get(1)?,
            })
        })
        .optional()
    };
    let next = read(after)?;
    Ok(if next.is_none() && after.is_some() {
        read(None)?
    } else {
        next
    })
}
fn next_audit(conn: &Connection) -> Result<Option<Hour>, ReportError> {
    let (kind, number): (Option<i64>, Option<i64>) = conn.query_row(
        "SELECT after_kind,after_hour FROM summary_audit_state WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let after = match (kind, number) {
        (Some(kind), Some(number)) => Some(Hour { kind, number }),
        (None, None) => None,
        _ => return Err(ReportError::Integrity("summary audit cursor invalid")),
    };
    let mut candidates = Vec::new();
    if let Some(hour)=conn.query_row("SELECT kind,hour_number FROM summary_hour WHERE ?1 IS NULL OR kind>?1 OR (kind=?1 AND hour_number>?2)
        ORDER BY kind,hour_number LIMIT 1",params![after.map(|h|h.kind),after.map(|h|h.number)],|row|Ok(Hour{kind:row.get(0)?,number:row.get(1)?})).optional()? {candidates.push(hour);}
    if after.is_none_or(|hour| hour.kind == 0) {
        let minimum = after.map(|hour| (i128::from(hour.number) + 1) * i128::from(HOUR_MS));
        if minimum.is_none_or(|minimum| minimum <= i128::from(i64::MAX)) {
            let bound = minimum.and_then(|minimum| i64::try_from(minimum).ok());
            let time = match bound {
                Some(bound)=>conn.query_row("SELECT occurred_at_ms FROM usage_event WHERE occurred_at_ms>=?1 ORDER BY occurred_at_ms LIMIT 1",[bound],|row|row.get::<_,i64>(0)).optional()?,
                None=>conn.query_row("SELECT occurred_at_ms FROM usage_event WHERE occurred_at_ms IS NOT NULL ORDER BY occurred_at_ms LIMIT 1",[],|row|row.get::<_,i64>(0)).optional()?,
            };
            if let Some(time) = time {
                candidates.push(Hour {
                    kind: 0,
                    number: time.div_euclid(HOUR_MS),
                });
            }
        }
    }
    if after.is_none_or(|hour| hour.kind < 1)
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM usage_event WHERE occurred_at_ms IS NULL)",
            [],
            |row| row.get::<_, bool>(0),
        )?
    {
        candidates.push(Hour { kind: 1, number: 0 });
    }
    Ok(candidates.into_iter().min())
}
impl SummaryMaintenance {
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
    pub fn defer_after_error(&mut self, now_ms: i64) {
        self.pending = None;
        self.finish_audit = false;
        self.retry_after_ms = now_ms.saturating_add(HOUR_MS);
    }
    /// The caller holds a read snapshot for this chunk, never across ticks.
    pub fn read_chunk(&mut self, conn: &Connection, now_ms: i64) -> Result<bool, ReportError> {
        if now_ms < self.retry_after_ms {
            return Ok(false);
        }
        let started = std::time::Instant::now();
        if self.pending.is_none() {
            let due = conn.query_row(
                "SELECT next_due_ms<=?1 FROM summary_audit_state WHERE id=1",
                [now_ms],
                |row| row.get::<_, bool>(0),
            )?;
            let mut dirty=conn.query_row("SELECT kind,hour_number FROM summary_dirty_hour WHERE ?1 IS NULL OR kind>?1 OR (kind=?1 AND hour_number>?2) ORDER BY kind,hour_number LIMIT 1",params![self.dirty_after.map(|hour|hour.kind),self.dirty_after.map(|hour|hour.number)],|row|Ok(Hour{kind:row.get(0)?,number:row.get(1)?})).optional()?;
            if dirty.is_none() {
                dirty=conn.query_row("SELECT kind,hour_number FROM summary_dirty_hour ORDER BY kind,hour_number LIMIT 1",[],|row|Ok(Hour{kind:row.get(0)?,number:row.get(1)?})).optional()?;
            }
            let next = match dirty {
                Some(hour) => Some(hour),
                None => next_stale(conn, self.stale_after)?,
            };
            let audit = due && (self.prefer_audit || next.is_none());
            let hour = if audit { next_audit(conn)? } else { next };
            let Some(hour) = hour else {
                self.finish_audit = audit;
                return Ok(audit);
            };
            if !audit {
                if dirty.is_some() {
                    self.dirty_after = Some(hour);
                } else {
                    // A deferred hour stays stale; rotate past it so later
                    // hours can rebuild within the publication row budget.
                    self.stale_after = Some(hour);
                }
            }
            self.pending = Some(Build {
                hour,
                revision: revision(conn, hour)?,
                after: None,
                data: Data::default(),
                audit,
                ready: false,
                deferred: false,
                chunks: 0,
                spent_ms: 0,
                max_chunk_ms: 0,
                started_at_ms: now_ms,
            });
        }
        let mut build = self
            .pending
            .take()
            .ok_or(ReportError::Integrity("summary build absent"))?;
        if build.revision != revision(conn, build.hour)? {
            return Ok(false);
        }
        let last = conn.query_row(&page_end_sql(build.hour, build.after), [], |row| {
            row.get::<_, Option<i64>>(0)
        })?;
        // An empty page reduces nothing, so it is not read.
        let count = match last {
            Some(last) => build
                .data
                .observe(conn, &chunk_filter(build.hour, build.after, last))?,
            None => 0,
        };
        build.after = last;
        let max_publication_rows = MAX_PUBLICATION_ROWS;
        #[cfg(test)]
        let max_publication_rows = self.max_publication_rows.unwrap_or(max_publication_rows);
        build.deferred = !build.audit
            && (build.data.cells.len()
                + build.data.quantities.len()
                + build.data.money.len()
                + build.data.frequencies.len()
                + build.data.runs.len()
                > max_publication_rows);
        build.ready = count < CHUNK || build.deferred;
        let spent =
            u64::try_from(started.elapsed().as_millis()).map_err(|_| ReportError::Arithmetic)?;
        add_count(&mut build.chunks, 1)?;
        add_count(&mut build.spent_ms, spent)?;
        build.max_chunk_ms = build.max_chunk_ms.max(spent);
        let ready = build.ready;
        self.pending = Some(build);
        Ok(ready)
    }
    /// Publication and dirty removal are one ordered writer transaction.
    pub fn publish(
        &mut self,
        tx: &Transaction<'_>,
        now_ms: i64,
    ) -> Result<SummaryOutcome, ReportError> {
        let started = std::time::Instant::now();
        if self.finish_audit {
            self.finish_audit = false;
            let (examined, mismatches, hours,chunks,spent_ms,max_chunk_ms,started_at) = tx.query_row(
                "SELECT examined,mismatches,hours,chunks,spent_ms,max_chunk_ms,started_at_ms FROM summary_audit_state WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get::<_,Option<i64>>(6)?)),
            )?;
            tx.execute("UPDATE summary_audit_state SET after_kind=NULL,after_hour=NULL,next_due_ms=?1,examined=0,mismatches=0,hours=0,chunks=0,spent_ms=0,max_chunk_ms=0,started_at_ms=NULL WHERE id=1",[now_ms.saturating_add(AUDIT_INTERVAL_MS)])?;
            return Ok(SummaryOutcome {
                audited: true,
                audit_completed: true,
                examined,
                mismatches,
                hours,
                chunks,
                spent_ms,
                max_chunk_ms,
                audit_elapsed_ms: started_at.map_or(0, |start| now_ms.saturating_sub(start).max(0)),
                ..SummaryOutcome::default()
            });
        }
        let Some(build) = self.pending.take() else {
            return Ok(SummaryOutcome::default());
        };
        if !build.ready || build.revision != revision(tx, build.hour)? {
            return Ok(SummaryOutcome::default());
        }
        let mut outcome = SummaryOutcome {
            examined: build.data.events,
            hour: Some((build.hour.kind, build.hour.number)),
            chunks: build.chunks,
            spent_ms: build.spent_ms,
            max_chunk_ms: build.max_chunk_ms,
            ..SummaryOutcome::default()
        };
        if build.deferred {
            outcome.deferred = true;
            self.prefer_audit = true;
            return Ok(outcome);
        }
        if build.audit {
            let dirty = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM summary_dirty_hour WHERE kind=?1 AND hour_number=?2)",
                params![build.hour.kind, build.hour.number],
                |row| row.get::<_, bool>(0),
            )?;
            let metadata=tx.query_row("SELECT built_revision,format_version,events_total FROM summary_hour WHERE kind=?1 AND hour_number=?2",params![build.hour.kind,build.hour.number],|row|Ok((row.get::<_,Option<i64>>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,u64>(2)?))).optional().ok().flatten();
            // A malformed derived payload is a rebuild request, never a zero total.
            let cache_matches = metadata
                == Some((Some(build.revision), Some(FORMAT), build.data.events))
                && stored(tx, build.hour).is_ok_and(|cached| cached == build.data);
            // Known revision or format staleness is ordinary rebuild work,
            // not evidence that a clean summary differs from base facts.
            let known_stale = metadata.is_some_and(|(revision, format, _)| {
                revision != Some(build.revision) || format != Some(FORMAT)
            });
            if !dirty && !known_stale && (build.revision == 0 || !cache_matches) {
                tx.execute(
                    "INSERT INTO summary_hour(kind,hour_number,revision) VALUES(?1,?2,1)
                    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1",
                    params![build.hour.kind, build.hour.number],
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO summary_dirty_hour VALUES(?1,?2)",
                    params![build.hour.kind, build.hour.number],
                )?;
                outcome.mismatch = true;
            }
            let spent = u64::try_from(started.elapsed().as_millis())
                .map_err(|_| ReportError::Arithmetic)?;
            add_count(&mut outcome.spent_ms, spent)?;
            outcome.max_chunk_ms = outcome.max_chunk_ms.max(spent);
            tx.execute("UPDATE summary_audit_state SET after_kind=?1,after_hour=?2,examined=examined+?3,mismatches=mismatches+?4,hours=hours+1,chunks=chunks+?5,spent_ms=spent_ms+?6,max_chunk_ms=MAX(max_chunk_ms,?7),started_at_ms=COALESCE(started_at_ms,?8) WHERE id=1",params![build.hour.kind,build.hour.number,build.data.events,i64::from(outcome.mismatch),outcome.chunks,outcome.spent_ms,outcome.max_chunk_ms,build.started_at_ms])?;
            outcome.audited = true;
            self.prefer_audit = false;
        } else {
            replace(tx, build.hour, build.revision, &build.data)?;
            outcome.published = true;
            self.prefer_audit = true;
        }
        Ok(outcome)
    }
}

struct Frame<'c> {
    cells: Vec<(i64, Hour, Key, Cell)>,
    clean: BTreeSet<Hour>,
    dirty: String,
    live: Data,
    values: Values<'c>,
}
fn merge_map(
    target: &mut BTreeMap<String, u64>,
    source: &BTreeMap<String, u64>,
) -> Result<(), ReportError> {
    for (name, count) in source {
        add_count(target.entry(name.clone()).or_default(), *count)?;
    }
    Ok(())
}
impl Cell {
    fn merge(&mut self, other: &Self) -> Result<(), ReportError> {
        macro_rules! counts {($($field:ident),*)=>{$(add_count(&mut self.$field,other.$field)?;)*};}
        counts!(
            events,
            priced,
            measurements,
            zero_measurements,
            budget_uncertain,
            priced_measurements,
            head_contributions,
            work_unknown,
            subscription_complete,
            subscription_missing_account,
            covered_measurements
        );
        sum(&mut self.heads, other.heads)?;
        sum(&mut self.adjustments, other.adjustments)?;
        sum(&mut self.total, other.total)?;
        min(&mut self.oldest, other.oldest);
        merge_map(&mut self.reasons, &other.reasons)?;
        merge_map(&mut self.declines, &other.declines)?;
        merge_map(&mut self.work_excluded, &other.work_excluded)?;
        Ok(())
    }
    fn staleness(&self, now_ms: i64) -> RateStaleness {
        RateStaleness {
            as_of_ms: now_ms,
            oldest_resolved_at_ms: self.oldest,
            age_ms: self.oldest.map(|old| now_ms.saturating_sub(old)),
            priced_measurements: self.priced_measurements,
            measurements_total: self.measurements,
        }
    }
}
fn eligible(revision: i64, built: Option<i64>, version: Option<i64>, dirty: bool) -> bool {
    built == Some(revision) && version == Some(FORMAT) && !dirty
}
fn disjunction(parts: &[String]) -> String {
    match parts.len() {
        0 => "0".into(),
        1 => parts[0].clone(),
        count => {
            let middle = count / 2;
            format!(
                "({}) OR ({})",
                disjunction(&parts[..middle]),
                disjunction(&parts[middle..])
            )
        }
    }
}
/// Reads the clean summary hours, with live reductions of the dirty ones.
///
/// `select` names the cells the caller reads; only those are kept. Every cell
/// of every clean hour is still decoded and checked: an undecodable payload,
/// an unknown dictionary id or cell events that don't add up to the hour's
/// `events_total` mean the summary was changed outside `replace`, and such an
/// hour is reduced from the ledger instead (or the whole read errors and the
/// caller falls back to the ledger). No report request carries a time range,
/// so the check covers every hour; skipping hours without a selected cell
/// would miss a selected cell that was deleted out of band.
fn frame<'c>(
    conn: &'c Connection,
    mut seam: impl FnMut(Hour, &Key) -> Result<bool, ReportError>,
    live_data: bool,
    select: impl Fn(&Key) -> bool,
) -> Result<Option<Frame<'c>>, ReportError> {
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='summary_hour')",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Ok(None);
    }
    let mut hours = BTreeMap::new();
    let mut clean = BTreeSet::new();
    let mut statement=conn.prepare("SELECT h.kind,h.hour_number,h.events_total,h.revision,h.built_revision,h.format_version,d.kind
        FROM summary_hour h LEFT JOIN summary_dirty_hour d USING(kind,hour_number)")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let hour = Hour {
            kind: row.get(0)?,
            number: row.get(1)?,
        };
        hours.insert(hour, row.get::<_, u64>(2)?);
        if eligible(
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get::<_, Option<i64>>(6)?.is_some(),
        ) {
            clean.insert(hour);
        }
    }
    if clean.is_empty() {
        return Ok(None);
    }
    let mut values = Values::new(conn);
    let mut cells = Vec::new();
    let mut observed = BTreeMap::new();
    let mut split = BTreeSet::new();
    let mut statement=conn.prepare("SELECT c.cell_id,c.kind,c.hour_number,c.provider,c.model,c.account,c.auth_selection,c.charge_basis,c.work_class,c.project,c.producer,c.currency,c.payload
        FROM summary_cell c")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let hour = Hour {
            kind: row.get(1)?,
            number: row.get(2)?,
        };
        if !clean.contains(&hour) {
            continue;
        }
        let mut dims = [const { None }; 8];
        for (column, dim) in dims.iter_mut().enumerate() {
            *dim = value(&mut values, row.get(column + 3)?)?;
        }
        let key = (dims, value(&mut values, row.get(11)?)?);
        let payload: String = row.get(12)?;
        let cell = decode(&payload)?;
        add_count(observed.entry(hour).or_default(), cell.events)?;
        if seam(hour, &key)? {
            split.insert(hour);
        }
        if select(&key) {
            cells.push((row.get(0)?, hour, key, cell));
        }
    }
    for hour in &clean {
        if observed.get(hour).copied().unwrap_or(0) != hours[hour] {
            split.insert(*hour);
        }
    }
    for hour in split {
        clean.remove(&hour);
    }
    if clean.is_empty() {
        return Ok(None);
    }
    cells.retain(|(_, hour, _, _)| clean.contains(hour));
    let dirty_hours: Vec<_> = hours
        .keys()
        .filter(|hour| !clean.contains(hour))
        .copied()
        .collect();
    let dirty = if dirty_hours.is_empty() {
        "0".into()
    } else {
        let parts = dirty_hours
            .into_iter()
            .map(|hour| format!("({})", hour.filter()))
            .collect::<Vec<_>>();
        disjunction(&parts)
    };
    if dirty != "0" {
        let dirty_events = conn.query_row(
            &format!("SELECT COUNT(*) FROM usage_event WHERE {dirty}"),
            [],
            |row| row.get::<_, u64>(0),
        )?;
        let clean_events = clean.iter().try_fold(0_u64, |sum, hour| {
            sum.checked_add(hours[hour]).ok_or(ReportError::Arithmetic)
        })?;
        if u128::from(dirty_events) > u128::from(clean_events) * 3 {
            return Ok(None);
        }
    }
    let mut live = Data::default();
    if live_data && dirty != "0" {
        live.observe(conn, &dirty)?;
    }
    Ok(Some(Frame {
        cells,
        clean,
        dirty,
        live,
        values,
    }))
}
fn totals(frame: &Frame) -> Result<Cell, ReportError> {
    let mut total = Cell::default();
    for (_, _, _, cell) in &frame.cells {
        total.merge(cell)?;
    }
    for cell in frame.live.cells.values() {
        total.merge(cell)?;
    }
    Ok(total)
}

pub(super) fn coverage(
    conn: &Connection,
    now_ms: i64,
) -> Result<Option<LedgerCoverage>, ReportError> {
    let Some(frame) = frame(conn, |_, _| Ok(false), true, |_| true)? else {
        return Ok(None);
    };
    let total = totals(&frame)?;
    let undated = frame
        .cells
        .iter()
        .filter(|(_, hour, _, _)| hour.kind == 1)
        .try_fold(0_u64, |sum, (_, _, _, cell)| {
            sum.checked_add(cell.events).ok_or(ReportError::Arithmetic)
        })?;
    let live_undated = if frame.dirty == "0" {
        0
    } else {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM usage_event WHERE occurred_at_ms IS NULL AND ({})",
                frame.dirty
            ),
            [],
            |row| row.get::<_, u64>(0),
        )?
    };
    Ok(Some(LedgerCoverage {
        thalamus_payer: load_thalamus_payer_coverage(conn)?,
        set_aside_by_source: load_set_aside_coverage(conn)?,
        skipped_local_by_source: load_skipped_local_coverage(conn)?,
        events_total: total.events,
        events_unpriced: total
            .events
            .checked_sub(total.priced)
            .ok_or(ReportError::Arithmetic)?,
        events_undated: undated
            .checked_add(live_undated)
            .ok_or(ReportError::Arithmetic)?,
        zero_measurement_events: total.zero_measurements,
        unpriced_events_by_reason: total.reasons.clone(),
        unpriced_measurements_by_reason: total.declines.clone(),
        ingest_freshness: load_ingest_freshness(conn)?,
        rate_staleness: total.staleness(now_ms),
        capacity: CapacityCoverage {
            labeled_windows: crate::capacity::windows(conn)?,
            provider_readings: load_provider_capacity_readings(conn)?,
        },
    }))
}
fn weighted_rank(values: &BTreeMap<i64, u64>, percentile: usize) -> Option<i64> {
    let n = values
        .values()
        .try_fold(0_u64, |sum, count| sum.checked_add(*count))?;
    let n = usize::try_from(n).ok()?;
    if n == 0 {
        return None;
    }
    let rank = u64::try_from(percentile.checked_mul(n)?.div_ceil(100).max(1)).ok()?;
    let mut count = 0_u64;
    for (value, n) in values {
        count = count.checked_add(*n)?;
        if count >= rank {
            return Some(*value);
        }
    }
    None
}
pub(super) fn workclass(conn: &Connection) -> Result<Option<WorkClassReport>, ReportError> {
    let Some(mut frame) = frame(conn, |_, _| Ok(false), true, |_| true)? else {
        return Ok(None);
    };
    type Group = (String, Option<String>, Option<String>);
    let mut groups: BTreeMap<Group, Cell> = BTreeMap::new();
    for (_, _, key, cell) in &frame.cells {
        let producer = key.0[Grouping::Producer as usize]
            .clone()
            .ok_or(ReportError::Integrity("summary producer absent"))?;
        groups
            .entry((
                producer,
                key.0[Grouping::WorkClass as usize].clone(),
                key.1.clone(),
            ))
            .or_default()
            .merge(cell)?;
    }
    for (key, cell) in &frame.live.cells {
        let producer = key.0[Grouping::Producer as usize]
            .clone()
            .ok_or(ReportError::Integrity("summary producer absent"))?;
        groups
            .entry((
                producer,
                key.0[Grouping::WorkClass as usize].clone(),
                key.1.clone(),
            ))
            .or_default()
            .merge(cell)?;
    }
    let mut frequencies: BTreeMap<Group, BTreeMap<i64, u64>> = BTreeMap::new();
    let mut add = |key: FrequencyKey, n: u64| -> Result<(), ReportError> {
        add_count(
            frequencies
                .entry((key.0, key.1, Some(key.2)))
                .or_default()
                .entry(key.3)
                .or_default(),
            n,
        )
    };
    for (key, n) in &frame.live.frequencies {
        add(key.clone(), *n)?;
    }
    let mut statement=conn.prepare("SELECT kind,hour_number,producer,work_class,currency,nanos,samples FROM summary_cost_frequency")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        if frame.clean.contains(&Hour {
            kind: row.get(0)?,
            number: row.get(1)?,
        }) {
            let producer = value(&mut frame.values, row.get(2)?)?
                .ok_or(ReportError::Integrity("summary producer absent"))?;
            let currency = value(&mut frame.values, row.get(4)?)?
                .ok_or(ReportError::Integrity("summary currency absent"))?;
            add(
                (
                    producer,
                    value(&mut frame.values, row.get(3)?)?,
                    currency,
                    row.get(5)?,
                ),
                row.get(6)?,
            )?;
        }
    }
    let mut labels: BTreeMap<String, LabelCounts> = BTreeMap::new();
    let mut result = Vec::new();
    for ((producer, work_class, currency), cell) in groups {
        let label = labels.entry(producer.clone()).or_default();
        add_count(&mut label.events_total, cell.events)?;
        if work_class.as_deref().is_some_and(|label| !label.is_empty()) {
            add_count(&mut label.events_labelled, cell.events)?;
        }
        let samples = frequencies
            .remove(&(producer.clone(), work_class.clone(), currency.clone()))
            .unwrap_or_default();
        let n = samples.values().try_fold(0_u64, |sum, n| {
            sum.checked_add(*n).ok_or(ReportError::Arithmetic)
        })?;
        let mut reasons = cell.work_excluded;
        if cell.work_unknown > 0 {
            add_count(
                reasons.entry("currency_unknown".into()).or_default(),
                cell.work_unknown,
            )?;
        }
        result.push(WorkClassRow {
            producer,
            work_class,
            currency,
            events_total: cell.events,
            cost_sample_n: n,
            excluded_from_sample: ExcludedSample {
                events: cell.events.checked_sub(n).ok_or(ReportError::Arithmetic)?,
                of_events: cell.events,
                reasons,
            },
            p50_nanos: weighted_rank(&samples, 50),
            p90_nanos: weighted_rank(&samples, 90),
        });
    }
    let producers = labels
        .into_iter()
        .map(|(producer, label)| ProducerLabelCoverage {
            producer,
            events_total: label.events_total,
            events_labelled: label.events_labelled,
            events_unlabelled: label.events_total - label.events_labelled,
            reports_labels: label.events_labelled > 0,
        })
        .collect();
    Ok(Some(WorkClassReport {
        rows: result,
        producers,
    }))
}

fn interval(hour: Hour) -> Result<(i64, i64), ReportError> {
    let start = i128::from(hour.number) * i128::from(HOUR_MS);
    Ok((
        i64::try_from(start).map_err(|_| ReportError::Calendar)?,
        i64::try_from(start + i128::from(HOUR_MS) - 1).map_err(|_| ReportError::Calendar)?,
    ))
}
fn calendar_bucket(ms: i64, timezone: Tz, grain: Grain) -> Result<(i64, i64), ReportError> {
    bucket_bounds_for_date(local_date(ms, timezone)?, timezone, grain)
}
#[derive(Default)]
struct SpendBucket {
    population: Cell,
    total: i128,
    metered: i128,
}
pub(super) fn spend(
    conn: &Connection,
    request: &ReportRequest,
    now_ms: i64,
) -> Result<Option<SpendReport>, ReportError> {
    use chrono::Offset;
    let grain = Grain::parse(&request.grain)
        .ok_or_else(|| ReportError::Refused(ReportRefusal::UnknownGrain(request.grain.clone())))?;
    let timezone: Tz = request.timezone.parse().map_err(|_| {
        ReportError::Refused(ReportRefusal::UnknownTimezone(request.timezone.clone()))
    })?;
    let grouping = parse_grouping(&request.grouping)?;
    let mut seams = BTreeMap::new();
    let Some(frame) = frame(
        conn,
        |hour, _| {
            if hour.kind == 1 {
                return Ok(false);
            }
            if let Some(seam) = seams.get(&hour) {
                return Ok(*seam);
            }
            let (from, to) = interval(hour)?;
            let start = DateTime::<Utc>::from_timestamp_millis(from)
                .ok_or(ReportError::Calendar)?
                .with_timezone(&timezone);
            let end = DateTime::<Utc>::from_timestamp_millis(to)
                .ok_or(ReportError::Calendar)?
                .with_timezone(&timezone);
            // An offset transition splits an hour even when its endpoints share a local date.
            let seam = start.offset().fix() != end.offset().fix()
                || calendar_bucket(from, timezone, grain)? != calendar_bucket(to, timezone, grain)?;
            seams.insert(hour, seam);
            Ok(seam)
        },
        false,
        |_| true,
    )?
    else {
        return Ok(None);
    };
    let base = spend_report_base(conn, request, now_ms, Some(&frame.dirty))?;
    let mut buckets: BTreeMap<BucketKey, SpendBucket> = BTreeMap::new();
    for row in base.rows {
        let key = BucketKey {
            start_ms: row.bucket_start_ms,
            end_ms: row.bucket_end_ms,
            grouping: grouping
                .iter()
                .map(|dimension| {
                    (
                        dimension.name().to_string(),
                        row.grouping[dimension.name()].clone(),
                    )
                })
                .collect(),
            currency: row.currency,
        };
        let bucket = buckets.entry(key).or_default();
        sum(&mut bucket.total, i128::from(row.total_nanos))?;
        sum(&mut bucket.metered, i128::from(row.metered_nanos))?;
        let cell = Cell {
            events: row.events_total,
            priced: row.events_priced,
            measurements: row.measurements_total,
            zero_measurements: row.zero_measurement_events,
            reasons: row.unpriced_events_by_reason,
            declines: row.unpriced_measurements_by_reason,
            ..Cell::default()
        };
        bucket.population.merge(&cell)?;
    }
    let mut undated = base.undated.events_undated;
    let mut events = base.undated.events_report_total;
    let mut staleness = Cell {
        measurements: base.rate_staleness.measurements_total,
        priced_measurements: base.rate_staleness.priced_measurements,
        oldest: base.rate_staleness.oldest_resolved_at_ms,
        ..Cell::default()
    };
    for (_, hour, key, cell) in frame.cells {
        add_count(&mut events, cell.events)?;
        add_count(&mut staleness.measurements, cell.measurements)?;
        add_count(&mut staleness.priced_measurements, cell.priced_measurements)?;
        min(&mut staleness.oldest, cell.oldest);
        if hour.kind == 1 {
            add_count(&mut undated, cell.events)?;
            continue;
        }
        let (start_ms, end_ms) = calendar_bucket(interval(hour)?.0, timezone, grain)?;
        let bucket = buckets
            .entry(BucketKey {
                start_ms,
                end_ms,
                grouping: grouping
                    .iter()
                    .map(|dimension| {
                        (
                            dimension.name().to_string(),
                            key.0[*dimension as usize].clone(),
                        )
                    })
                    .collect(),
                currency: key.1,
            })
            .or_default();
        sum(&mut bucket.total, cell.total)?;
        if key.0[Grouping::ChargeBasis as usize].as_deref() == Some("metered_api") {
            sum(&mut bucket.metered, cell.total)?;
        }
        bucket.population.merge(&cell)?;
    }
    let mut rows = Vec::new();
    let mut headline: BTreeMap<Option<String>, SpendTotal> = BTreeMap::new();
    for (key, bucket) in buckets {
        let cell = bucket.population;
        let row = SpendRow {
            bucket_start_ms: key.start_ms,
            bucket_end_ms: key.end_ms,
            grouping: key.grouping.into_iter().collect(),
            currency: key.currency,
            total_nanos: i64::try_from(bucket.total).map_err(|_| ReportError::Arithmetic)?,
            metered_nanos: i64::try_from(bucket.metered).map_err(|_| ReportError::Arithmetic)?,
            events_total: cell.events,
            events_priced: cell.priced,
            events_unpriced: cell
                .events
                .checked_sub(cell.priced)
                .ok_or(ReportError::Arithmetic)?,
            cost_sample_n: cell.priced,
            zero_measurement_events: cell.zero_measurements,
            measurements_total: cell.measurements,
            unpriced_events_by_reason: cell.reasons,
            unpriced_measurements_by_reason: cell.declines,
        };
        let total = headline
            .entry(row.currency.clone())
            .or_insert_with(|| SpendTotal {
                currency: row.currency.clone(),
                total_nanos: 0,
                metered_nanos: 0,
                events_total: 0,
                events_priced: 0,
                events_unpriced: 0,
                cost_sample_n: 0,
                zero_measurement_events: 0,
            });
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
        rows.push(row);
    }
    let mut reasons = BTreeMap::new();
    if undated > 0 {
        reasons.insert("missing_occurred_at".into(), undated);
    }
    Ok(Some(SpendReport {
        totals: headline.into_values().collect(),
        rows,
        undated: UndatedCoverage {
            events_undated: undated,
            events_report_total: events,
            reasons,
        },
        ingest_freshness: base.ingest_freshness,
        rate_staleness: staleness.staleness(now_ms),
        hierarchy_resolution: base.hierarchy_resolution,
    }))
}

fn plan_key(plan: &SubscriptionView, time: i64) -> Result<SubscriptionBucketKey, ReportError> {
    let (start, end) = subscription_period_bounds(plan, time)?;
    Ok(SubscriptionBucketKey {
        provider: plan.provider.clone(),
        account_ref: plan.account_ref.clone(),
        start_ms: start,
        end_ms: plan.effective_to_ms.map_or(end, |to| to.min(end)),
        plan_cost_nanos: recurring_cost_nanos(plan.recurring_cost_minor, plan.amount_exponent)
            .map_err(|_| ReportError::Arithmetic)?,
    })
}
pub(super) fn subscription(
    conn: &Connection,
    request: &SubscriptionReportRequest,
) -> Result<Option<SubscriptionReport>, ReportError> {
    let plans = load_subscriptions(conn)?;
    let index = SubscriptionIndex::new(&plans);
    let mut seams = BTreeMap::new();
    let Some(mut frame) = frame(
        conn,
        |hour, key| {
            if hour.kind == 1 || !is_subscription(key) {
                return Ok(false);
            }
            let (Some(provider), Some(account)) = (
                &key.0[Grouping::Provider as usize],
                &key.0[Grouping::Account as usize],
            ) else {
                return Ok(false);
            };
            let cache = (hour, provider.clone(), account.clone());
            if let Some(seam) = seams.get(&cache) {
                return Ok(*seam);
            }
            let (start, end) = interval(hour)?;
            let boundary = plans
                .iter()
                .filter(|plan| plan.provider == *provider && plan.account_ref == *account)
                .any(|plan| {
                    (plan.effective_from_ms > start && plan.effective_from_ms <= end)
                        || plan
                            .effective_to_ms
                            .is_some_and(|to| to > start && to <= end)
                });
            let from = index
                .find(provider, account, start)
                .map(|plan| plan_key(plan, start))
                .transpose()?;
            let to = index
                .find(provider, account, end)
                .map(|plan| plan_key(plan, end))
                .transpose()?;
            let seam = boundary || from != to;
            seams.insert(cache, seam);
            Ok(seam)
        },
        false,
        is_subscription,
    )?
    else {
        return Ok(None);
    };
    let base = subscription_report_base(conn, request, Some(&frame.dirty))?;
    let mut coverage = base.coverage;
    let mut rows: BTreeMap<SubscriptionBucketKey, SubscriptionAccumulator> = BTreeMap::new();
    for row in base.rows {
        let accumulator = rows
            .entry(SubscriptionBucketKey {
                provider: row.provider,
                account_ref: row.account_ref,
                start_ms: row.period_start_ms,
                end_ms: row.period_end_ms,
                plan_cost_nanos: row.plan_cost_nanos,
            })
            .or_default();
        add_count(&mut accumulator.covered_events, row.covered_events)?;
        add_count(
            &mut accumulator.covered_measurements,
            row.covered_measurements,
        )?;
        for quantity in row.quantities {
            accumulator
                .quantities
                .entry((quantity.unit, quantity.role))
                .or_default()
                .add(quantity.quantity_units, quantity.quantity_exponent)?;
        }
    }
    // Ordered by cell id so quantities are added in the same order a scan of
    // summary_quantity's primary key would visit them.
    let mut attribution = BTreeMap::new();
    for (id, hour, key, cell) in frame.cells {
        if !is_subscription(&key) {
            continue;
        }
        add_count(&mut coverage.events_total, cell.events)?;
        add_count(
            &mut coverage.zero_measurement_events,
            cell.zero_measurements,
        )?;
        if hour.kind == 1 {
            add_count(&mut coverage.events_undated, cell.events)?;
            continue;
        }
        add_count(
            &mut coverage.events_without_account,
            cell.subscription_missing_account,
        )?;
        add_count(
            &mut coverage.events_unpriced,
            cell.events
                .checked_sub(cell.subscription_complete)
                .ok_or(ReportError::Arithmetic)?,
        )?;
        if cell.subscription_complete == 0 {
            continue;
        }
        let plan = match (
            &key.0[Grouping::Provider as usize],
            &key.0[Grouping::Account as usize],
        ) {
            (Some(provider), Some(account)) => index.find(provider, account, interval(hour)?.0),
            _ => None,
        };
        let Some(plan) = plan else {
            add_count(&mut coverage.events_unpriced, cell.subscription_complete)?;
            continue;
        };
        let bucket = plan_key(plan, interval(hour)?.0)?;
        let accumulator = rows.entry(bucket.clone()).or_default();
        add_count(&mut accumulator.covered_events, cell.subscription_complete)?;
        add_count(
            &mut accumulator.covered_measurements,
            cell.covered_measurements,
        )?;
        attribution.insert(id, bucket);
    }
    // Only the attributed cells' quantities are read, each by its primary key,
    // rather than every covered quantity ever summarised.
    let mut statement=conn.prepare("SELECT unit,role,exponent,covered_units FROM summary_quantity WHERE cell_id=?1 AND covered_observations>0 AND exponent<=0")?;
    for (cell_id, bucket) in &attribution {
        let mut quantities = statement.query([cell_id])?;
        while let Some(quantity) = quantities.next()? {
            let unit = value(&mut frame.values, quantity.get(0)?)?
                .ok_or(ReportError::Integrity("summary unit absent"))?;
            let role = value(&mut frame.values, quantity.get(1)?)?
                .ok_or(ReportError::Integrity("summary role absent"))?;
            let units = i64::try_from(parse_exact(quantity.get(3)?)?)
                .map_err(|_| ReportError::Arithmetic)?;
            rows.get_mut(bucket)
                .ok_or(ReportError::Integrity("subscription bucket absent"))?
                .quantities
                .entry((unit, role))
                .or_default()
                .add(units, quantity.get(2)?)?;
        }
    }
    let rows = rows
        .into_iter()
        .map(|(key, accumulator)| -> Result<_, ReportError> {
            let quantities = accumulator
                .quantities
                .into_iter()
                .map(|((unit, role), sum)| {
                    Ok(CoveredQuantity {
                        unit,
                        role,
                        quantity_units: i64::try_from(sum.units)
                            .map_err(|_| ReportError::Arithmetic)?,
                        quantity_exponent: sum.exponent,
                    })
                })
                .collect::<Result<_, ReportError>>()?;
            Ok(SubscriptionRow {
                provider: key.provider,
                account_ref: key.account_ref,
                period_start_ms: key.start_ms,
                period_end_ms: key.end_ms,
                plan_cost_nanos: key.plan_cost_nanos,
                covered_events: accumulator.covered_events,
                covered_measurements: accumulator.covered_measurements,
                quantities,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(Some(SubscriptionReport {
        rows,
        coverage,
        ingest_freshness: base.ingest_freshness,
    }))
}

fn compatible_limb_population(contributions: u64) -> bool {
    // A low limb has magnitude at most 2^32-1; 2^31 contributions cannot
    // overflow SQLite's signed SUM, regardless of their signs or row order.
    contributions <= 2_147_483_648
}
pub(super) fn verdict(
    conn: &Connection,
    request: &crate::verdict::VerdictRequest,
) -> Result<Option<crate::verdict::VerdictResponse>, ReportError> {
    let filtered =
        request.project_root.is_some() || request.provider.is_some() || request.model.is_some();
    let selected = |key: &Key| {
        request
            .project_root
            .as_ref()
            .is_none_or(|value| key.0[Grouping::Project as usize].as_ref() == Some(value))
            && request
                .provider
                .as_ref()
                .is_none_or(|value| key.0[Grouping::Provider as usize].as_ref() == Some(value))
            && request
                .model
                .as_ref()
                .is_none_or(|value| key.0[Grouping::Model as usize].as_ref() == Some(value))
    };
    let Some(mut frame) = frame(conn, |_, _| Ok(false), true, |key| selected(key))? else {
        return Ok(None);
    };
    let mut total = 0_u64;
    let mut uncertain = 0_u64;
    let mut cells = BTreeSet::new();
    let mut money: BTreeMap<String, i128> = BTreeMap::new();
    let mut contributions = 0_u64;
    for (id, _, key, cell) in &frame.cells {
        if selected(key) {
            cells.insert(*id);
            add_count(&mut total, cell.events)?;
            add_count(&mut uncertain, cell.budget_uncertain)?;
        }
    }
    for (key, cell) in &frame.live.cells {
        if selected(key) {
            add_count(&mut total, cell.events)?;
            add_count(&mut uncertain, cell.budget_uncertain)?;
        }
    }
    for ((key, _, currency), amount) in &frame.live.money {
        if selected(key) {
            sum(money.entry(currency.clone()).or_default(), amount.nanos)?;
            add_count(&mut contributions, amount.contributions)?;
        }
    }
    if filtered {
        // A filtered verdict reads the money of its own cells by primary key,
        // in cell order, instead of scanning every summarised money row.
        let mut statement = conn
            .prepare("SELECT currency,nanos,contributions FROM summary_money WHERE cell_id=?1")?;
        for id in &cells {
            let mut rows = statement.query([id])?;
            while let Some(row) = rows.next()? {
                let currency = value(&mut frame.values, row.get(0)?)?
                    .ok_or(ReportError::Integrity("summary money currency absent"))?;
                sum(
                    money.entry(currency).or_default(),
                    parse_exact(row.get(1)?)?,
                )?;
                add_count(&mut contributions, row.get(2)?)?;
            }
        }
    } else {
        // Unfiltered, every clean cell is selected, so one scan is cheaper
        // than a lookup per cell.
        let mut statement =
            conn.prepare("SELECT cell_id,currency,nanos,contributions FROM summary_money")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            if cells.contains(&row.get::<_, i64>(0)?) {
                let currency = value(&mut frame.values, row.get(1)?)?
                    .ok_or(ReportError::Integrity("summary money currency absent"))?;
                sum(
                    money.entry(currency).or_default(),
                    parse_exact(row.get(2)?)?,
                )?;
                add_count(&mut contributions, row.get(3)?)?;
            }
        }
    }
    if !compatible_limb_population(contributions) {
        return Ok(None);
    }
    let consumed_nanos = money
        .into_iter()
        .map(|(currency, nanos)| {
            Ok((
                currency,
                i64::try_from(nanos).map_err(|_| ReportError::Arithmetic)?,
            ))
        })
        .collect::<Result<_, ReportError>>()?;
    Ok(Some(crate::verdict::VerdictResponse {
        decision: crate::verdict::VerdictDecision::Allow,
        consumed_nanos,
        est_cost_nanos: request.est_cost_nanos,
        limit_nanos: None,
        remaining_nanos: None,
        uncertainty: crate::verdict::VerdictUncertainty {
            unpriced_events: uncertain,
            events_total: total,
        },
        uncertainty_scope: "machine",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fact::ChargeBasis;
    use crate::test_support::conn;
    use crate::{ingest, pricing, subscription, verdict};

    #[test]
    fn media_skipped_local_coverage_is_live_on_base_and_hourly_paths() {
        let mut conn = conn();
        assert!(
            ledger_coverage_base(&conn, &LedgerCoverageRequest {}, 10_000)
                .unwrap()
                .skipped_local_by_source
                .is_empty()
        );
        record(
            &mut conn,
            "media-coverage-anchor",
            Some(1_000),
            ChargeBasis::MeteredApi,
            None,
        );
        catch_up(&mut conn);
        let hourly = coverage(&conn, 10_000)
            .unwrap()
            .expect("hourly summaries are used");
        assert!(hourly.skipped_local_by_source.is_empty());
        conn.execute(
            "INSERT INTO media_feed_cursor VALUES('colliculus','18446744073709551615',3)",
            [],
        )
        .unwrap();
        let direct = ledger_coverage_base(&conn, &LedgerCoverageRequest {}, 10_000).unwrap();
        let hourly = coverage(&conn, 10_000)
            .unwrap()
            .expect("hourly summaries are still used");
        assert_eq!(direct.skipped_local_by_source.get("colliculus"), Some(&3));
        assert_eq!(hourly.skipped_local_by_source.get("colliculus"), Some(&3));
    }

    fn record(
        conn: &mut Connection,
        id: &str,
        time: Option<i64>,
        basis: ChargeBasis,
        work: Option<&str>,
    ) -> String {
        let request:ingest::EventRecordRequest=serde_json::from_value(serde_json::json!({
            "producer":"fixture","producer_ref":id,"provider":"provider","model":"model","account_ref":"account",
            "work_class":work,"project_root":"/fixture/project","charge_basis":basis.as_str(),"occurred_at_ms":time,
            "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":12,"quantity_exponent":0},
                {"role":"output","unit":"token","kind":"billed","source":"fixture","quantity_units":0,"quantity_exponent":0}]
        })).unwrap();
        ingest::record_event(conn, &request, 1).unwrap().event_id
    }

    fn observed_record(conn: &mut Connection, time: i64) {
        let request = serde_json::from_value(serde_json::json!({
            "producer":"fixture","producer_ref":"readings","provider":"provider",
            "model":"model","account_ref":"account","project_root":"/fixture/project",
            "occurred_at_ms":time,"charge_basis":"metered_api","measurements":[
                {"role":"duration","unit":"second","kind":"observed","source":"fixture","quantity_units":0,"quantity_exponent":0},
                {"role":"credits","unit":"credit","kind":"observed","source":"fixture","quantity_units":12,"quantity_exponent":0}
            ]
        })).unwrap();
        ingest::record_event(conn, &request, 1).unwrap();
    }

    fn add_reading(conn: &Connection, id: &str) {
        conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
            SELECT event_key,'duration','second','observed','fixture',0,0 FROM usage_event WHERE event_id=?1", [id]).unwrap();
    }
    // Explicit priced_charge amounts keep report tests independent of catalog lookups and pricing arithmetic.
    fn priced(conn: &mut Connection, id: &str, currency: &str, nanos: i64) {
        let tx = conn.transaction().unwrap();
        tx.execute(
            "UPDATE usage_event SET pricing_generation=1,currency=?2 WHERE event_id=?1",
            params![id, currency],
        )
        .unwrap();
        for (role, nanos) in [("input", nanos), ("output", 0)] {
            tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token',1,'priced','metered_api',0,?3,?4,1,9,'none','fixture',1,'{}','pricing_pass',1)",params![id,role,currency,nanos]).unwrap();
        }
        tx.commit().unwrap();
    }
    fn adjust(conn: &mut Connection, id: &str, currency: &str, nanos: i64, correction: &str) {
        let tx = conn.transaction().unwrap();
        ingest::record_adjustment_in_tx(
            &tx,
            &ingest::AdjustmentRecordRequest {
                event_id: id.into(),
                currency: currency.into(),
                amount_nanos: nanos,
                reason: ingest::AdjustmentReason::Refund,
                occurred_at_ms: 2,
                note: None,
            },
            &ingest::AdjustmentWriteContext::correction_ingest(correction.into()),
            3,
        )
        .unwrap();
        tx.commit().unwrap();
    }
    fn step(conn: &mut Connection, maintenance: &mut SummaryMaintenance) -> SummaryOutcome {
        let tx = conn.unchecked_transaction().unwrap();
        let ready = maintenance.read_chunk(&tx, 10_000).unwrap();
        tx.commit().unwrap();
        if !ready {
            return SummaryOutcome::default();
        }
        let tx = conn.transaction().unwrap();
        let result = maintenance.publish(&tx, 10_000).unwrap();
        tx.commit().unwrap();
        result
    }
    fn dirty(conn: &Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM summary_dirty_hour", [], |row| {
            row.get(0)
        })
        .unwrap()
    }
    fn catch_up(conn: &mut Connection) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut maintenance = SummaryMaintenance::default();
        while dirty(conn) > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "summary fill failed to progress"
            );
            step(conn, &mut maintenance);
        }
    }
    fn request(timezone: &str, grain: &str, grouping: &[&str]) -> ReportRequest {
        ReportRequest {
            timezone: timezone.into(),
            grain: grain.into(),
            grouping: grouping.iter().map(|value| (*value).into()).collect(),
        }
    }
    fn equal<T: Serialize>(actual: &T, expected: &T) {
        assert_eq!(
            serde_json::to_vec(actual).unwrap(),
            serde_json::to_vec(expected).unwrap()
        );
    }
    fn compare(conn: &Connection) {
        for timezone in [
            "UTC",
            "Europe/Madrid",
            "Asia/Kolkata",
            "America/St_Johns",
            "Pacific/Chatham",
        ] {
            for grain in ["day", "week", "month"] {
                for grouping in [
                    &[][..],
                    &["provider"][..],
                    &["model"][..],
                    &["account"][..],
                    &["auth_selection"][..],
                    &["charge_basis"][..],
                    &["work_class"][..],
                    &["project"][..],
                    &[
                        "provider",
                        "model",
                        "account",
                        "auth_selection",
                        "charge_basis",
                        "work_class",
                        "project",
                    ][..],
                    &[
                        "project",
                        "work_class",
                        "charge_basis",
                        "auth_selection",
                        "account",
                        "model",
                        "provider",
                    ][..],
                ] {
                    let request = request(timezone, grain, grouping);
                    equal(
                        &spend_report(conn, &request, 10_000).unwrap(),
                        &spend_report_base(conn, &request, 10_000, None).unwrap(),
                    );
                }
            }
        }
        equal(
            &subscription_report(conn, &SubscriptionReportRequest {}).unwrap(),
            &subscription_report_base(conn, &SubscriptionReportRequest {}, None).unwrap(),
        );
        equal(
            &work_class_report(conn, &WorkClassReportRequest {}).unwrap(),
            &work_class_report_base(conn, &WorkClassReportRequest {}).unwrap(),
        );
        equal(
            &ledger_coverage(conn, &LedgerCoverageRequest {}, 10_000).unwrap(),
            &ledger_coverage_base(conn, &LedgerCoverageRequest {}, 10_000).unwrap(),
        );
        for project in [None, Some("/fixture/project"), Some("absent")] {
            for provider in [None, Some("provider"), Some("absent")] {
                for model in [None, Some("model"), Some("absent")] {
                    let request = verdict::VerdictRequest {
                        project_root: project.map(str::to_owned),
                        provider: provider.map(str::to_owned),
                        model: model.map(str::to_owned),
                        est_cost_nanos: Some(123),
                    };
                    equal(
                        &verdict::evaluate(conn, &request).unwrap(),
                        &verdict::evaluate_base(conn, &request).unwrap(),
                    );
                }
            }
        }
    }

    #[test]
    fn observed_only_cells_reject_old_formats_and_rebuild_without_changing_coverage() {
        let mut conn = conn();
        observed_record(&mut conn, 1000);
        for (id, time) in [
            ("mixed-one", HOUR_MS + 1000),
            ("mixed-two", 2 * HOUR_MS + 1000),
        ] {
            let event = record(
                &mut conn,
                id,
                Some(time),
                ChargeBasis::MeteredApi,
                Some("coding"),
            );
            priced(&mut conn, &event, "USD", 50);
            add_reading(&conn, &event);
        }
        compare(&conn);
        catch_up(&mut conn);
        compare(&conn);
        let coverage = coverage(&conn, 10_000).unwrap().unwrap();
        assert_eq!(
            coverage.unpriced_events_by_reason,
            BTreeMap::from([("observed_only".into(), 1)])
        );
        assert_eq!(coverage.zero_measurement_events, 0);
        assert_eq!(coverage.rate_staleness.measurements_total, 8);

        conn.execute("UPDATE summary_hour SET format_version=?1", [FORMAT - 1])
            .unwrap();
        let stale: i64 = conn
            .query_row(
                "SELECT count(*) FROM summary_hour WHERE format_version<>?1",
                [FORMAT],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale, 3);
        conn.execute("UPDATE summary_cell SET payload=json_set(payload,'$.reasons',json('{\"not_yet_priced\":1}'),'$.zero_measurements',1)", []).unwrap();
        compare(&conn);
        conn.execute(
            "UPDATE summary_audit_state SET next_due_ms=?1,after_kind=NULL,after_hour=NULL",
            [i64::MAX],
        )
        .unwrap();
        assert_eq!(dirty(&conn), 0);
        let audit_before: (i64, i64) = conn
            .query_row(
                "SELECT hours,mismatches FROM summary_audit_state WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let mut maintenance = SummaryMaintenance::default();
        for remaining in (0..stale).rev() {
            let outcome = step(&mut conn, &mut maintenance);
            assert!(
                outcome.published,
                "a stale hour must rebuild before the audit is due"
            );
            assert!(!outcome.audited);
            assert!(!outcome.mismatch);
            compare(&conn);
            let stale: i64 = conn
                .query_row(
                    "SELECT count(*) FROM summary_hour WHERE format_version<>?1",
                    [FORMAT],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stale, remaining);
        }
        let stale: i64 = conn
            .query_row(
                "SELECT count(*) FROM summary_hour WHERE format_version<>?1",
                [FORMAT],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale, 0);
        let audit_after: (i64, i64) = conn
            .query_row(
                "SELECT hours,mismatches FROM summary_audit_state WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(audit_after, audit_before);
        compare(&conn);
    }

    #[test]
    fn dirty_hours_rebuild_before_unmarked_format_and_revision_stale_hours() {
        let mut conn = conn();
        for (id, time) in [("format", 0), ("dirty", HOUR_MS), ("revision", 2 * HOUR_MS)] {
            let event = record(&mut conn, id, Some(time), ChargeBasis::MeteredApi, None);
            priced(&mut conn, &event, "USD", 100);
        }
        catch_up(&mut conn);
        conn.execute("UPDATE summary_audit_state SET next_due_ms=?1", [i64::MAX])
            .unwrap();
        conn.execute(
            "UPDATE summary_hour SET format_version=?1 WHERE hour_number=0",
            [FORMAT - 1],
        )
        .unwrap();
        conn.execute(
            "UPDATE summary_hour SET built_revision=revision-1 WHERE hour_number=2",
            [],
        )
        .unwrap();
        let event: String = conn
            .query_row(
                "SELECT event_id FROM usage_event WHERE occurred_at_ms=?1",
                [HOUR_MS],
                |row| row.get(0),
            )
            .unwrap();
        adjust(&mut conn, &event, "USD", 25, "dirty-adjustment");
        assert_eq!(dirty(&conn), 1);
        let mut maintenance = SummaryMaintenance::default();
        for number in [1, 0, 2] {
            let outcome = step(&mut conn, &mut maintenance);
            assert!(outcome.published);
            assert_eq!(outcome.hour, Some((0, number)));
            assert!(!outcome.audited);
            assert!(!outcome.mismatch);
            let metadata: (i64, i64, i64) = conn.query_row("SELECT revision,built_revision,format_version FROM summary_hour WHERE kind=0 AND hour_number=?1", [number], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
            assert_eq!(metadata.0, metadata.1);
            assert_eq!(metadata.2, FORMAT);
            compare(&conn);
        }
        assert_eq!(dirty(&conn), 0);
        assert_eq!(
            stored(&conn, Hour { kind: 0, number: 1 })
                .unwrap()
                .cells
                .values()
                .next()
                .unwrap()
                .total,
            125
        );
    }

    #[test]
    fn audit_does_not_count_known_format_or_revision_staleness_as_mismatches() {
        for change in [
            format!("format_version={}", FORMAT - 1),
            format!("format_version={}", FORMAT + 1),
            "format_version=NULL".into(),
            "built_revision=revision-1".into(),
            "built_revision=NULL".into(),
        ] {
            let mut conn = conn();
            record(
                &mut conn,
                "stale",
                Some(1000),
                ChargeBasis::MeteredApi,
                None,
            );
            catch_up(&mut conn);
            conn.execute_batch(&format!("UPDATE summary_hour SET {change}; UPDATE summary_audit_state SET next_due_ms=0,after_kind=NULL,after_hour=NULL,mismatches=0;")).unwrap();
            // A due audit can encounter an old summary before maintenance rebuilds it.
            let mut maintenance = SummaryMaintenance {
                prefer_audit: true,
                ..SummaryMaintenance::default()
            };
            let audit = step(&mut conn, &mut maintenance);
            assert!(audit.audited);
            assert!(
                !audit.mismatch,
                "known staleness is not an audit mismatch: {change}"
            );
            let mismatches: i64 = conn
                .query_row(
                    "SELECT mismatches FROM summary_audit_state WHERE id=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(mismatches, 0);
            assert_eq!(dirty(&conn), 0);
            let rebuild = step(&mut conn, &mut maintenance);
            assert!(rebuild.published);
            assert!(!rebuild.audited);
            assert!(!rebuild.mismatch);
            let metadata: (i64, i64, i64) = conn
                .query_row(
                    "SELECT revision,built_revision,format_version FROM summary_hour",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(metadata.0, metadata.1);
            assert_eq!(metadata.2, FORMAT);
        }
    }

    #[test]
    fn dirty_hours_use_base_money_while_workclass_keeps_head_only_samples() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "one",
            Some(1000),
            ChargeBasis::MeteredApi,
            Some("coding"),
        );
        priced(&mut conn, &id, "USD", 100);
        let other = record(
            &mut conn,
            "two",
            Some(HOUR_MS + 1000),
            ChargeBasis::MeteredApi,
            None,
        );
        priced(&mut conn, &other, "JPY", 300);
        observed_record(&mut conn, 2 * HOUR_MS + 1000);
        add_reading(&conn, &id);
        catch_up(&mut conn);
        assert!(spend(&conn, &request("UTC", "day", &[]), 10_000)
            .unwrap()
            .is_some());
        assert!(coverage(&conn, 10_000).unwrap().is_some());
        assert!(workclass(&conn).unwrap().is_some());
        compare(&conn);
        adjust(&mut conn, &id, "USD", -25, "refund");
        let report = spend(&conn, &request("UTC", "day", &[]), 10_000)
            .unwrap()
            .unwrap();
        assert_eq!(
            report
                .totals
                .iter()
                .find(|row| row.currency.as_deref() == Some("USD"))
                .unwrap()
                .total_nanos,
            75
        );
        assert_eq!(dirty(&conn), 1);
        assert_eq!(
            workclass(&conn)
                .unwrap()
                .unwrap()
                .rows
                .iter()
                .find(|row| row.currency.as_deref() == Some("USD"))
                .unwrap()
                .p50_nanos,
            Some(100)
        );
        compare(&conn);
        catch_up(&mut conn);
        compare(&conn);
    }

    #[test]
    fn write_after_reduction_cannot_publish_a_stale_revision() {
        let mut conn = conn();
        let id = record(&mut conn, "one", Some(1000), ChargeBasis::MeteredApi, None);
        priced(&mut conn, &id, "USD", 100);
        catch_up(&mut conn);
        adjust(&mut conn, &id, "USD", 10, "first");
        let mut maintenance = SummaryMaintenance::default();
        let tx = conn.unchecked_transaction().unwrap();
        assert!(maintenance.read_chunk(&tx, 10_000).unwrap());
        tx.commit().unwrap();
        adjust(&mut conn, &id, "USD", 20, "second");
        let tx = conn.transaction().unwrap();
        let outcome = maintenance.publish(&tx, 10_000).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            stored(&conn, Hour { kind: 0, number: 0 })
                .unwrap()
                .cells
                .values()
                .next()
                .unwrap()
                .total,
            100
        );
        assert!(
            !outcome.published,
            "a changed revision must not be published"
        );
        assert_eq!(dirty(&conn), 1);
        assert_eq!(
            spend_report(&conn, &request("UTC", "day", &[]), 10_000)
                .unwrap()
                .totals[0]
                .total_nanos,
            130
        );
    }

    #[test]
    fn half_hour_timezone_and_plan_seams_match_the_base_reader() {
        let mut conn = conn();
        let day = Utc
            .with_ymd_and_hms(2026, 3, 28, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        for (name, time, amount) in [
            ("before", day + 18 * HOUR_MS + 29 * 60_000, 100),
            ("after", day + 18 * HOUR_MS + 31 * 60_000, 200),
            ("whole", day + 20 * HOUR_MS, 300),
        ] {
            let id = record(&mut conn, name, Some(time), ChargeBasis::MeteredApi, None);
            priced(&mut conn, &id, "USD", amount);
        }
        catch_up(&mut conn);
        let request = request("Asia/Kolkata", "day", &[]);
        let actual = spend(&conn, &request, 10_000).unwrap().unwrap();
        equal(
            &actual,
            &spend_report_base(&conn, &request, 10_000, None).unwrap(),
        );
        assert_eq!(actual.rows.len(), 2);
        assert_eq!(actual.rows[0].total_nanos, 100);
        assert_eq!(actual.rows[1].total_nanos, 500);
        let tx = conn.transaction().unwrap();
        subscription::set_subscription_in_tx(
            &tx,
            &subscription::SetSubscriptionRequest {
                provider: "provider".into(),
                account_ref: "account".into(),
                plan_name: "fixture".into(),
                recurring_cost_minor: 100,
                currency: "USD".into(),
                amount_exponent: 2,
                period: subscription::SubscriptionPeriod::Month,
                effective_from: day + 30 * 60_000,
            },
            None,
        )
        .unwrap();
        tx.commit().unwrap();
        for (name, time) in [
            ("sub-before", day + 29 * 60_000),
            ("sub-after", day + 31 * 60_000),
            ("sub-whole", day + HOUR_MS),
        ] {
            let id = record(
                &mut conn,
                name,
                Some(time),
                ChargeBasis::SubscriptionIncluded,
                None,
            );
            let plan = subscription::read_reprice(&conn, &id).unwrap();
            let resolved = subscription::resolve_reprice_without_catalog(&plan).unwrap();
            let tx = conn.transaction().unwrap();
            subscription::commit_reprice_in_tx(
                &tx,
                &plan,
                &resolved,
                3,
                pricing::PricingOrigin::PricingPass,
            )
            .unwrap();
            tx.commit().unwrap();
        }
        catch_up(&mut conn);
        let actual = subscription(&conn, &SubscriptionReportRequest {})
            .unwrap()
            .unwrap();
        equal(
            &actual,
            &subscription_report_base(&conn, &SubscriptionReportRequest {}, None).unwrap(),
        );
        assert_eq!(actual.coverage.events_unpriced, 1);
        assert_eq!(actual.rows[0].covered_events, 2);
        assert_eq!(actual.rows[0].covered_measurements, 4);
    }

    #[test]
    fn audit_finds_an_unmarked_adjustment_and_restores_report_parity() {
        let mut conn = conn();
        let id = record(&mut conn, "one", Some(1000), ChargeBasis::MeteredApi, None);
        priced(&mut conn, &id, "USD", 100);
        catch_up(&mut conn);
        let trigger: String = conn
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name='summary_adjustment_insert'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        conn.execute_batch("DROP TRIGGER summary_adjustment_insert")
            .unwrap();
        adjust(&mut conn, &id, "USD", -25, "unmarked");
        assert_eq!(dirty(&conn), 0);
        assert_eq!(
            spend(&conn, &request("UTC", "day", &[]), 10_000)
                .unwrap()
                .unwrap()
                .totals[0]
                .total_nanos,
            100
        );
        let mut maintenance = SummaryMaintenance::default();
        let outcome = step(&mut conn, &mut maintenance);
        assert_eq!(
            spend_report(&conn, &request("UTC", "day", &[]), 10_000)
                .unwrap()
                .totals[0]
                .total_nanos,
            75
        );
        assert!(
            outcome.audited && outcome.mismatch,
            "the independent clean-hour audit must diagnose stale money"
        );
        assert_eq!(dirty(&conn), 1);
        catch_up(&mut conn);
        assert_eq!(
            stored(&conn, Hour { kind: 0, number: 0 })
                .unwrap()
                .cells
                .values()
                .next()
                .unwrap()
                .total,
            75
        );
        conn.execute_batch(&trigger).unwrap();
        compare(&conn);
    }

    fn reprice(conn: &mut Connection, id: &str) {
        let plan = subscription::read_reprice(conn, id).unwrap();
        let resolved = subscription::resolve_reprice_without_catalog(&plan).unwrap();
        let tx = conn.transaction().unwrap();
        subscription::commit_reprice_in_tx(
            &tx,
            &plan,
            &resolved,
            4,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn audit_walk_resumes_and_discovers_a_base_hour_missing_from_all_summary_tables() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "known",
            Some(1000),
            ChargeBasis::MeteredApi,
            None,
        );
        priced(&mut conn, &id, "USD", 100);
        catch_up(&mut conn);
        let trigger: String = conn
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name='summary_event_insert'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        conn.execute_batch("DROP TRIGGER summary_event_insert")
            .unwrap();
        record(
            &mut conn,
            "unmarked",
            Some(HOUR_MS + 1000),
            ChargeBasis::Unknown,
            None,
        );
        assert_eq!(dirty(&conn), 0);
        assert_eq!(coverage(&conn, 10_000).unwrap().unwrap().events_total, 1);
        let mut first = SummaryMaintenance::default();
        let result = step(&mut conn, &mut first);
        assert!(result.audited && !result.mismatch);
        assert_eq!(
            conn.query_row("SELECT after_hour FROM summary_audit_state", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
        let mut restarted = SummaryMaintenance::default();
        let result = step(&mut conn, &mut restarted);
        assert!(result.audited && result.mismatch);
        assert_eq!(dirty(&conn), 1);
        assert_eq!(
            ledger_coverage(&conn, &LedgerCoverageRequest {}, 10_000)
                .unwrap()
                .events_total,
            2
        );
        step(&mut conn, &mut restarted);
        let result = step(&mut conn, &mut restarted);
        assert!(result.audit_completed);
        assert_eq!(result.examined, 2);
        assert_eq!(result.hours, 2);
        assert_eq!(result.mismatches, 1);
        // The next whole-ledger pass waits a day, because a pass re-reads the
        // whole store.
        assert_eq!(
            conn.query_row("SELECT next_due_ms FROM summary_audit_state", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            10_000 + 24 * HOUR_MS
        );
        conn.execute_batch(&trigger).unwrap();
        compare(&conn);
    }
    fn declare(conn: &mut Connection, from: i64) {
        let tx = conn.transaction().unwrap();
        subscription::set_subscription_in_tx(
            &tx,
            &subscription::SetSubscriptionRequest {
                provider: "provider".into(),
                account_ref: "account".into(),
                plan_name: "fixture".into(),
                recurring_cost_minor: 100,
                currency: "USD".into(),
                amount_exponent: 2,
                period: subscription::SubscriptionPeriod::Month,
                effective_from: from,
            },
            None,
        )
        .unwrap();
        tx.commit().unwrap();
    }
    fn correct_basis(conn: &mut Connection, id: &str) {
        let plan = subscription::read_basis_correction(
            conn,
            &ingest::BasisCorrectRequest {
                event_id: id.into(),
                corrected_basis: ChargeBasis::Unknown,
                reason: "fixture correction".into(),
            },
        )
        .unwrap();
        let resolved = subscription::resolve_reprice_without_catalog(&plan).unwrap();
        let tx = conn.transaction().unwrap();
        ingest::correct_basis_in_tx(&tx, &plan, &resolved, 5).unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn coverage_backfill_and_basis_correction_replace_the_whole_clean_partition() {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "subscription",
            Some(HOUR_MS + 1000),
            ChargeBasis::SubscriptionIncluded,
            None,
        );
        reprice(&mut conn, &id);
        catch_up(&mut conn);
        assert_eq!(
            subscription_report(&conn, &SubscriptionReportRequest {})
                .unwrap()
                .coverage
                .events_unpriced,
            1
        );
        declare(&mut conn, HOUR_MS / 2);
        assert_eq!(
            subscription::coverage_backfill_event_ids(&conn, None, 100)
                .unwrap()
                .as_slice(),
            std::slice::from_ref(&id)
        );
        reprice(&mut conn, &id);
        assert_eq!(dirty(&conn), 1);
        let report = subscription_report(&conn, &SubscriptionReportRequest {}).unwrap();
        assert_eq!(report.rows[0].covered_events, 1);
        assert_eq!(report.coverage.events_unpriced, 0);
        catch_up(&mut conn);
        compare(&conn);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM priced_charge", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            4
        );
        assert_eq!(conn.query_row("SELECT count(*) FROM summary_cell c JOIN summary_value v ON v.value_id=c.currency WHERE v.value='USD'",[],|row|row.get::<_,i64>(0)).unwrap(),1);
        correct_basis(&mut conn, &id);
        assert_eq!(dirty(&conn), 1);
        compare(&conn);
        catch_up(&mut conn);
        compare(&conn);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM priced_charge", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            6
        );
        let bases:Vec<String>=conn.prepare("SELECT v.value FROM summary_cell c JOIN summary_value v ON v.value_id=c.charge_basis").unwrap().query_map([],|row|row.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
        assert_eq!(bases, ["unknown"]);
    }

    #[test]
    fn budget_compatibility_uses_the_signed_sqlite_limb_bound() {
        let bound = 2_147_483_648_u64;
        assert!(compatible_limb_population(bound));
        assert!(!compatible_limb_population(bound + 1));
        let maximum_limb = 4_294_967_295_i128;
        assert!(i128::from(bound) * maximum_limb <= i128::from(i64::MAX));
        assert!(i128::from(bound + 1) * maximum_limb > i128::from(i64::MAX));
    }

    #[test]
    fn oversized_cached_budget_population_returns_to_the_original_source_sum() {
        let mut conn = conn();
        let id = record(&mut conn, "one", Some(1000), ChargeBasis::MeteredApi, None);
        priced(&mut conn, &id, "USD", 100);
        catch_up(&mut conn);
        // A synthetic cached population exercises the overflow fence without billions of rows.
        conn.execute(
            "UPDATE summary_money SET contributions=2147483649,nanos='200'",
            [],
        )
        .unwrap();
        let request = verdict::VerdictRequest {
            project_root: None,
            provider: None,
            model: None,
            est_cost_nanos: None,
        };
        assert_eq!(
            verdict::evaluate(&conn, &request).unwrap().consumed_nanos["USD"],
            100
        );
        assert!(super::verdict(&conn, &request).unwrap().is_none());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM priced_charge", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT nanos FROM summary_money", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "200"
        );
    }

    #[test]
    fn thalamus_payer_coverage_is_live_on_base_and_hourly_paths() {
        let mut conn = conn();
        let request:ingest::EventRecordRequest=serde_json::from_value(serde_json::json!({"producer":"fixture","producer_ref":"payer-coverage","charge_basis":"unknown","occurred_at_ms":1000,"measurements":[]})).unwrap();
        ingest::record_event(&mut conn, &request, 1).unwrap();
        conn.execute(
            "INSERT INTO thalamus_payer_coverage VALUES ('late','attempt-a')",
            [],
        )
        .unwrap();
        let before = ledger_coverage_base(&conn, &LedgerCoverageRequest {}, 10_000).unwrap();
        assert_eq!(before.thalamus_payer.as_ref().unwrap().late_payer_rows, 1);
        catch_up(&mut conn);
        assert!(coverage(&conn, 10_000).unwrap().is_some());
        conn.execute(
            "INSERT INTO thalamus_payer_coverage VALUES ('late','attempt-b')",
            [],
        )
        .unwrap();
        let hourly = ledger_coverage(&conn, &LedgerCoverageRequest {}, 10_000).unwrap();
        let base = ledger_coverage_base(&conn, &LedgerCoverageRequest {}, 10_000).unwrap();
        assert_eq!(hourly.thalamus_payer, base.thalamus_payer);
        assert_eq!(hourly.thalamus_payer.unwrap().late_payer_rows, 2);
    }

    #[test]
    fn examined_declined_zero_is_report_priced_but_budget_uncertain() {
        let mut conn = conn();
        let mut request:ingest::EventRecordRequest=serde_json::from_value(serde_json::json!({"producer":"fixture","producer_ref":"zero","charge_basis":"unknown","occurred_at_ms":1000,
            "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":0,"quantity_exponent":0}]})).unwrap();
        request.work_class = Some(String::new());
        let id = ingest::record_event(&mut conn, &request, 1)
            .unwrap()
            .event_id;
        reprice(&mut conn, &id);
        catch_up(&mut conn);
        let report = ledger_coverage(&conn, &LedgerCoverageRequest {}, 10_000).unwrap();
        assert_eq!(report.events_unpriced, 0);
        assert_eq!(
            report.unpriced_measurements_by_reason["charge_basis_unknown"],
            1
        );
        let verdict = verdict::evaluate(
            &conn,
            &verdict::VerdictRequest {
                project_root: None,
                provider: None,
                model: None,
                est_cost_nanos: None,
            },
        )
        .unwrap();
        assert_eq!(verdict.uncertainty.unpriced_events, 1);
        assert!(verdict.consumed_nanos.is_empty());
        let report = work_class_report(&conn, &WorkClassReportRequest {}).unwrap();
        assert_eq!(report.rows[0].cost_sample_n, 0);
        assert_eq!(
            report.rows[0].excluded_from_sample.reasons["currency_unknown"],
            1
        );
        compare(&conn);
    }

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            self.0
        }
        fn pick<T: Copy>(&mut self, values: &[T]) -> T {
            values[usize::try_from(self.next() % u64::try_from(values.len()).unwrap()).unwrap()]
        }
    }
    #[test]
    fn random_fixtures_are_byte_identical_when_clean_dirty_repriced_or_half_filled() {
        for seed in 1..=6_u64 {
            let mut conn = conn();
            let mut random = Random(seed);
            let (month, day) = match seed % 4 {
                0 => (10, 24),
                1 => (3, 7),
                2 => (3, 28),
                _ => (4, 4),
            };
            let base = Utc
                .with_ymd_and_hms(2026, month, day, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_millis();
            observed_record(&mut conn, base + 3 * HOUR_MS);
            declare(&mut conn, base + 30 * 60_000);
            let paid = record(
                &mut conn,
                "bootstrap-usd",
                Some(base + HOUR_MS),
                ChargeBasis::MeteredApi,
                Some("coding"),
            );
            priced(&mut conn, &paid, "USD", 500);
            let yen = record(
                &mut conn,
                "bootstrap-jpy",
                Some(base + 2 * HOUR_MS),
                ChargeBasis::MeteredApi,
                None,
            );
            priced(&mut conn, &yen, "JPY", 300);
            for index in 0..72_i64 {
                let basis = random.pick(&[
                    ChargeBasis::MeteredApi,
                    ChargeBasis::SubscriptionIncluded,
                    ChargeBasis::Unknown,
                ]);
                let time = if index % 13 == 0 {
                    None
                } else {
                    Some(
                        base + index * 8 * HOUR_MS
                            + i64::try_from(random.next() % u64::try_from(HOUR_MS).unwrap())
                                .unwrap(),
                    )
                };
                let input = random.pick(&[Some(0_i64), Some(12), Some(123), None]);
                let exponent = if input.is_some() {
                    Some(random.pick(&[0_i32, -1, -2]))
                } else {
                    None
                };
                let measurements = if index % 17 == 0 {
                    vec![]
                } else {
                    vec![
                        serde_json::json!({"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":input,"quantity_exponent":exponent}),
                        serde_json::json!({"role":"output","unit":"token","kind":"billed","source":"fixture","quantity_units":0,"quantity_exponent":0}),
                    ]
                };
                let request:ingest::EventRecordRequest=serde_json::from_value(serde_json::json!({"producer":random.pick(&["fixture","other-fixture"]),"producer_ref":format!("{seed}-{index}"),
                    "provider":random.pick(&[Some("provider"),Some("other-provider"),None]),"model":random.pick(&[Some("model"),Some("other-model"),None]),
                    "account_ref":random.pick(&[Some("account"),Some("other-account"),None]),"auth_selection":random.pick(&[Some("api-key"),Some(""),None]),
                    "project_root":random.pick(&[Some("/fixture/project"),Some("/fixture/other"),None]),"work_class":random.pick(&[Some("coding"),Some("review"),Some(""),None]),
                    "occurred_at_ms":time,"charge_basis":basis.as_str(),"measurements":measurements})).unwrap();
                let id = ingest::record_event(&mut conn, &request, 1)
                    .unwrap()
                    .event_id;
                if request.measurements.is_empty() {
                    continue;
                }
                if basis == ChargeBasis::MeteredApi && time.is_some() && index % 5 == 0 {
                    priced(
                        &mut conn,
                        &id,
                        random.pick(&["USD", "JPY"]),
                        if input == Some(0) {
                            0
                        } else {
                            i64::try_from(random.next() % 500).unwrap()
                        },
                    );
                } else {
                    reprice(&mut conn, &id);
                }
                if index % 7 == 0 {
                    add_reading(&conn, &id);
                }
            }
            compare(&conn);
            catch_up(&mut conn);
            assert!(spend(
                &conn,
                &request("UTC", "month", &["provider", "model"]),
                10_000
            )
            .unwrap()
            .is_some());
            compare(&conn);
            adjust(&mut conn, &paid, "USD", -25, &format!("refund-{seed}"));
            compare(&conn);
            correct_basis(&mut conn, &paid);
            compare(&conn);
            catch_up(&mut conn);
            compare(&conn);
            conn.execute("INSERT OR IGNORE INTO summary_dirty_hour SELECT kind,hour_number FROM (SELECT kind,hour_number,ROW_NUMBER() OVER(ORDER BY kind,hour_number) AS ordinal FROM summary_hour) WHERE ordinal%2=0",[]).unwrap();
            assert!(coverage(&conn, 10_000).unwrap().is_some());
            compare(&conn);
            catch_up(&mut conn);
            compare(&conn);
        }
    }

    fn query_plan(conn: &Connection, sql: &str) -> String {
        let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let mut rows = statement.query([]).unwrap();
        let mut text = String::new();
        while let Some(row) = rows.next().unwrap() {
            text.push_str(&row.get::<_, String>(3).unwrap());
            text.push('\n');
        }
        text
    }

    #[test]
    fn stale_hour_lookup_uses_the_partial_index_and_current_stores_stay_idle() {
        for hours in [200, 20_000] {
            let mut conn = conn();
            conn.execute_batch(&format!(
                "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<{hours}-1)
                 INSERT INTO summary_hour(kind,hour_number,revision,built_revision,format_version)
                 SELECT 0,i,1,1,{FORMAT} FROM n;
                 UPDATE summary_audit_state SET next_due_ms={};",
                i64::MAX
            ))
            .unwrap();
            for after in [
                None,
                Some(Hour {
                    kind: 0,
                    number: hours / 2,
                }),
            ] {
                let plan = query_plan(&conn, &stale_hour_sql(after));
                assert!(plan.contains("summary_hour_stale"), "{plan}");
                assert!(
                    !plan.lines().any(|line| line.trim() == "SCAN summary_hour"),
                    "{plan}"
                );
                assert!(!plan.contains("TEMP B-TREE"), "{plan}");
                if after.is_some() {
                    assert!(plan.contains("SEARCH summary_hour"), "{plan}");
                }
            }
            let mut maintenance = SummaryMaintenance::default();
            let counter = crate::store::VmCounter::start(&conn);
            let outcome = step(&mut conn, &mut maintenance);
            let steps = counter.steps();
            assert!(
                steps < 200,
                "idle step used {steps} VM steps for {hours} current hours"
            );
            assert!(!outcome.published);
            assert!(!outcome.audited);
            assert!(!maintenance.has_pending());

            conn.execute(
                "UPDATE summary_hour SET format_version=NULL WHERE hour_number=?1",
                [hours - 1],
            )
            .unwrap();
            let counter = crate::store::VmCounter::start(&conn);
            let stale: (i64, i64) = conn
                .query_row(&stale_hour_sql(None), [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .unwrap();
            let steps = counter.steps();
            assert_eq!(stale, (0, hours - 1));
            assert!(
                steps < 100,
                "stale lookup used {steps} VM steps for {hours} hours"
            );
            let outcome = step(&mut conn, &mut maintenance);
            assert!(outcome.published);
            assert_eq!(outcome.hour, Some(stale));
            assert!(!outcome.mismatch);
            let counter = crate::store::VmCounter::start(&conn);
            assert!(!step(&mut conn, &mut maintenance).published);
            let steps = counter.steps();
            assert!(
                steps < 200,
                "idle wrapped step used {steps} VM steps for {hours} current hours"
            );
        }
    }

    #[test]
    fn deferred_stale_hour_does_not_block_later_stale_hours() {
        let mut conn = conn();
        for (id, time, work) in [
            ("large-one", 0, "coding"),
            ("large-two", 0, "analysis"),
            ("later-one", HOUR_MS, "coding"),
            ("later-two", 2 * HOUR_MS, "coding"),
        ] {
            record(
                &mut conn,
                id,
                Some(time),
                ChargeBasis::MeteredApi,
                Some(work),
            );
        }
        catch_up(&mut conn);
        conn.execute("UPDATE summary_hour SET format_version=?1", [FORMAT - 1])
            .unwrap();
        conn.execute("UPDATE summary_audit_state SET next_due_ms=?1", [i64::MAX])
            .unwrap();
        assert_eq!(dirty(&conn), 0);
        // One cell and its two quantities fit; the two-cell hour must defer.
        let mut maintenance = SummaryMaintenance {
            max_publication_rows: Some(3),
            ..SummaryMaintenance::default()
        };
        for number in 0..3 {
            let outcome = step(&mut conn, &mut maintenance);
            assert_eq!(outcome.hour, Some((0, number)));
            assert_eq!(outcome.deferred, number == 0);
            assert_eq!(outcome.published, number != 0);
            assert!(!outcome.audited);
            assert!(!outcome.mismatch);
            compare(&conn);
        }
        let versions: Vec<(i64, i64)> = conn
            .prepare(
                "SELECT hour_number,format_version FROM summary_hour ORDER BY kind,hour_number",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(versions, vec![(0, FORMAT - 1), (1, FORMAT), (2, FORMAT)]);
        let retried = step(&mut conn, &mut maintenance);
        assert_eq!(retried.hour, Some((0, 0)));
        assert!(retried.deferred);
    }

    /// Fills one old hour with more events than one chunk holds, then adds
    /// `later` events in a much later hour, all through plain inserts so the
    /// fixture is quick at any size.
    fn busy_old_hour(later: i64) -> Connection {
        let conn = conn();
        let events = i64::try_from(CHUNK).unwrap() + 10;
        conn.execute_batch(&format!(
            "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<{events}-1)
             INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,charge_basis)
             SELECT 'old-'||i,'fixture','old-'||i,i,1,'metered_api' FROM n;
             WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<{later}-1)
             INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,charge_basis)
             SELECT 'new-'||i,'fixture','new-'||i,{}+i,1,'metered_api' FROM n;",
            1_000 * HOUR_MS
        ))
        .unwrap();
        conn
    }

    #[test]
    fn format_stale_hours_rebuild_with_the_same_chunk_budget() {
        let mut conn = busy_old_hour(10);
        catch_up(&mut conn);
        conn.execute("UPDATE summary_audit_state SET next_due_ms=?1", [i64::MAX])
            .unwrap();
        conn.execute(
            "UPDATE summary_hour SET format_version=?1 WHERE hour_number=0",
            [FORMAT - 1],
        )
        .unwrap();
        assert_eq!(dirty(&conn), 0);
        let mut maintenance = SummaryMaintenance::default();
        assert!(!step(&mut conn, &mut maintenance).published);
        let pending = maintenance.pending.as_ref().unwrap();
        assert_eq!(pending.data.events, u64::try_from(CHUNK).unwrap());
        assert!(!pending.audit);
        let version: i64 = conn
            .query_row(
                "SELECT format_version FROM summary_hour WHERE hour_number=0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, FORMAT - 1);
        let outcome = step(&mut conn, &mut maintenance);
        assert!(outcome.published);
        assert_eq!(outcome.chunks, 2);
        assert!(!outcome.audited);
        assert!(!outcome.mismatch);
        assert_eq!(
            stored(&conn, Hour { kind: 0, number: 0 }).unwrap().events,
            u64::try_from(CHUNK).unwrap() + 10
        );
        let version: i64 = conn
            .query_row(
                "SELECT format_version FROM summary_hour WHERE hour_number=0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, FORMAT);
    }

    #[test]
    fn resumed_hour_chunk_reads_its_hour_not_the_later_ledger() {
        let mut measured = Vec::new();
        for later in [1_000, 5_000] {
            let conn = busy_old_hour(later);
            let mut maintenance = SummaryMaintenance::default();
            // The first chunk takes the first CHUNK events of the old hour.
            assert!(!maintenance.read_chunk(&conn, 10_000).unwrap());
            assert_eq!(
                maintenance.pending.as_ref().unwrap().hour,
                Hour { kind: 0, number: 0 }
            );
            let counter = crate::store::VmCounter::start(&conn);
            let ready = maintenance.read_chunk(&conn, 10_000).unwrap();
            measured.push(counter.steps());
            let build = maintenance.pending.as_ref().unwrap();
            assert!(ready);
            assert_eq!(build.data.events, u64::try_from(CHUNK).unwrap() + 10);
            assert_eq!(build.after, Some(i64::try_from(CHUNK).unwrap() + 10));
        }
        // 4,000 more later events must not cost the resumed chunk anything.
        assert!(
            measured[1] < measured[0] + 100,
            "resumed chunk VM steps grew with later history: {measured:?}"
        );
        let conn = busy_old_hour(10);
        let hour = Hour { kind: 0, number: 0 };
        let plan = query_plan(&conn, &page_end_sql(hour, Some(5_000)));
        assert!(plan.contains("usage_event_summary_time"), "{plan}");
        assert!(!plan.contains("rowid>"), "{plan}");
        assert!(!plan.contains("SCAN usage_event"), "{plan}");
        // Each reduction statement reads the page through a range closed on
        // both sides, either the key range or the hour's time range.
        let plan = query_plan(
            &conn,
            &format!(
                "SELECT event_key FROM usage_event WHERE {}",
                chunk_filter(hour, Some(5_000), 5_010)
            ),
        );
        assert!(
            plan.contains("(rowid>? AND rowid<?)")
                || plan
                    .contains("usage_event_summary_time (occurred_at_ms>? AND occurred_at_ms<?)"),
            "{plan}"
        );
        assert!(!plan.contains("SCAN usage_event"), "{plan}");
    }

    fn unused_dictionary_values(conn: &Connection, count: i64) {
        conn.execute_batch(&format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<{count})
             INSERT INTO summary_value(value) SELECT 'unused-'||i FROM n;"
        ))
        .unwrap();
    }

    #[test]
    fn publishing_and_reading_one_hour_load_only_the_dictionary_values_it_uses() {
        let mut publish = Vec::new();
        let mut read = Vec::new();
        let mut published = Vec::new();
        for unused in [100, 2_000] {
            let mut conn = conn();
            let id = record(&mut conn, "one", Some(1_000), ChargeBasis::MeteredApi, None);
            priced(&mut conn, &id, "USD", 100);
            unused_dictionary_values(&conn, unused);
            let counter = crate::store::VmCounter::start(&conn);
            catch_up(&mut conn);
            publish.push(counter.steps());
            let hour = Hour { kind: 0, number: 0 };
            let counter = crate::store::VmCounter::start(&conn);
            let data = stored(&conn, hour).unwrap();
            read.push(counter.steps());
            assert_eq!(data.events, 1);
            published.push(format!("{data:?}"));
            compare(&conn);
        }
        assert_eq!(published[0], published[1]);
        assert!(publish[1] < publish[0] + 100, "publication: {publish:?}");
        assert!(read[1] < read[0] + 100, "stored hour read: {read:?}");
    }

    fn record_project(conn: &mut Connection, id: &str, project: &str) -> String {
        let request: ingest::EventRecordRequest = serde_json::from_value(serde_json::json!({
            "producer":"fixture","producer_ref":id,"provider":"provider","model":"model","account_ref":"account",
            "project_root":project,"charge_basis":"metered_api","occurred_at_ms":1_000,
            "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":12,"quantity_exponent":0},
                {"role":"output","unit":"token","kind":"billed","source":"fixture","quantity_units":0,"quantity_exponent":0}]
        }))
        .unwrap();
        ingest::record_event(conn, &request, 1).unwrap().event_id
    }
    fn project_request(project: Option<&str>) -> verdict::VerdictRequest {
        verdict::VerdictRequest {
            project_root: project.map(str::to_owned),
            provider: None,
            model: None,
            est_cost_nanos: None,
        }
    }
    /// One clean hour holding a selected and an unselected project cell. The
    /// unselected cell then gets `extra` money and quantity rows in as many
    /// new dictionary values: summary history a filtered report never uses.
    fn unselected_summary_history(extra: i64) -> Connection {
        let mut conn = conn();
        let selected = record_project(&mut conn, "selected", "/fixture/selected");
        priced(&mut conn, &selected, "USD", 100);
        let other = record_project(&mut conn, "other", "/fixture/other");
        priced(&mut conn, &other, "USD", 50);
        catch_up(&mut conn);
        compare(&conn);
        unused_dictionary_values(&conn, extra);
        conn.execute_batch(
            "INSERT INTO summary_money SELECT c.cell_id,0,v.value_id,'0',1
               FROM summary_cell c JOIN summary_value p ON p.value_id=c.project, summary_value v
              WHERE p.value='/fixture/other' AND v.value LIKE 'unused-%';
             INSERT INTO summary_quantity SELECT c.cell_id,v.value_id,v.value_id,0,'1',1,'1',1
               FROM summary_cell c JOIN summary_value p ON p.value_id=c.project, summary_value v
              WHERE p.value='/fixture/other' AND v.value LIKE 'unused-%';",
        )
        .unwrap();
        conn
    }

    #[test]
    fn filtered_and_scalar_reports_skip_unselected_summary_history() {
        let mut verdicts = Vec::new();
        let mut subscriptions = Vec::new();
        let mut coverages = Vec::new();
        for extra in [100, 2_000] {
            let conn = unselected_summary_history(extra);
            let request = project_request(Some("/fixture/selected"));
            let counter = crate::store::VmCounter::start(&conn);
            let hourly = super::verdict(&conn, &request)
                .unwrap()
                .expect("summaries used");
            verdicts.push(counter.steps());
            equal(&hourly, &verdict::evaluate_base(&conn, &request).unwrap());
            assert_eq!(hourly.consumed_nanos["USD"], 100);

            let counter = crate::store::VmCounter::start(&conn);
            let report = super::subscription(&conn, &SubscriptionReportRequest {})
                .unwrap()
                .expect("summaries used");
            subscriptions.push(counter.steps());
            equal(
                &report,
                &subscription_report_base(&conn, &SubscriptionReportRequest {}, None).unwrap(),
            );

            let counter = crate::store::VmCounter::start(&conn);
            coverage(&conn, 10_000).unwrap().expect("summaries used");
            coverages.push(counter.steps());

            // Only the selected cell is kept, though both were read and checked.
            let frame = frame(
                &conn,
                |_, _| Ok(false),
                true,
                |key| key.0[Grouping::Project as usize].as_deref() == Some("/fixture/selected"),
            )
            .unwrap()
            .unwrap();
            assert_eq!(frame.cells.len(), 1);
        }
        assert!(verdicts[1] < verdicts[0] + 100, "verdict: {verdicts:?}");
        assert!(
            subscriptions[1] < subscriptions[0] + 100,
            "subscription: {subscriptions:?}"
        );
        assert!(coverages[1] < coverages[0] + 100, "coverage: {coverages:?}");
    }

    #[test]
    fn deleting_a_selected_cell_out_of_band_still_reads_the_ledger_for_it() {
        let mut conn = conn();
        let selected = record_project(&mut conn, "selected", "/fixture/selected");
        priced(&mut conn, &selected, "USD", 100);
        let other = record_project(&mut conn, "other", "/fixture/other");
        priced(&mut conn, &other, "USD", 50);
        catch_up(&mut conn);
        let request = project_request(Some("/fixture/selected"));
        assert_eq!(
            super::verdict(&conn, &request)
                .unwrap()
                .unwrap()
                .consumed_nanos["USD"],
            100
        );
        // A write that bypasses `replace`: the hour's cells no longer add up
        // to its events_total, so the hour must be reduced from the ledger.
        conn.execute_batch(
            "DELETE FROM summary_cell WHERE project=(SELECT value_id FROM summary_value WHERE value='/fixture/selected')",
        )
        .unwrap();
        let response = verdict::evaluate(&conn, &request).unwrap();
        assert_eq!(response.consumed_nanos["USD"], 100);
        assert_eq!(response.uncertainty.events_total, 1);
        equal(&response, &verdict::evaluate_base(&conn, &request).unwrap());
    }
}
