//! Session, run and flow reads over current measurement charges. The list shares
//! the hourly report's invalidation, bounded rebuild, publication and audit.
use std::collections::BTreeMap;

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::report::ReportError;

pub(crate) const FORMAT: i64 = 2;
const HOUR_MS: i64 = 3_600_000;
const MAX_RANGE: i64 = 31 * 24 * HOUR_MS;
const HARNESSES: &[&str] = &[
    "broca",
    "claude-code",
    "codex",
    "gemini",
    "goose",
    "kimi",
    "omp",
    "opencode",
    "pi",
];

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunKey {
    Session { harness: String, session: String },
    Run { harness: String, run_id: String },
    Flow { harness: String, flow_id: String },
}
impl RunKey {
    fn harness(&self) -> &str {
        match self {
            Self::Session { harness, .. }
            | Self::Run { harness, .. }
            | Self::Flow { harness, .. } => harness,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        let valid = match self {
            Self::Session { harness, .. } => {
                harness != "broca" && HARNESSES.contains(&harness.as_str())
            }
            Self::Run { harness, .. } | Self::Flow { harness, .. } => harness == "broca",
        };
        if valid {
            Ok(())
        } else {
            Err(Error::Refusal("invalid_request"))
        }
    }
    fn pricing_unit(&self) -> &'static str {
        match self {
            Self::Session { harness, .. } if harness == "goose" => "session_total",
            Self::Session { .. } => "step",
            _ => "run_segment",
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Refusal(&'static str),
    Store(ReportError),
}
impl From<ReportError> for Error {
    fn from(value: ReportError) -> Self {
        Self::Store(value)
    }
}
impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Self::Store(value.into())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    pub from_ms: i64,
    pub to_ms: i64,
    pub harness: Option<String>,
    pub limit: Option<u32>,
    pub after: Option<String>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetailRequest {
    pub key: RunKey,
    pub limit: Option<u32>,
    pub after: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tokens {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<u64>,
}
fn add<T: Copy + std::ops::Add<Output = T>>(
    left: &mut T,
    right: T,
    checked: impl FnOnce(T, T) -> Option<T>,
) -> Result<(), ReportError> {
    *left = checked(*left, right).ok_or(ReportError::Arithmetic)?;
    Ok(())
}
fn count(left: &mut u64, right: u64) -> Result<(), ReportError> {
    add(left, right, u64::checked_add)
}
fn optional(left: &mut Option<u64>, right: Option<u64>) -> Result<(), ReportError> {
    if let Some(right) = right {
        let mut value = left.unwrap_or(0);
        count(&mut value, right)?;
        *left = Some(value);
    }
    Ok(())
}
impl Tokens {
    fn merge(&mut self, other: &Self) -> Result<(), ReportError> {
        optional(&mut self.input, other.input)?;
        optional(&mut self.output, other.output)?;
        optional(&mut self.reasoning, other.reasoning)?;
        optional(&mut self.cache_read, other.cache_read)?;
        optional(&mut self.cache_write, other.cache_write)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cost {
    pub priced: BTreeMap<String, i64>,
    pub covered_measurements: u64,
    pub unpriced_measurements: u64,
    pub unpriced_by_reason: BTreeMap<String, u64>,
    pub pending_measurements: u64,
}
impl Cost {
    fn merge(&mut self, other: &Self) -> Result<(), ReportError> {
        for (currency, nanos) in &other.priced {
            add(
                self.priced.entry(currency.clone()).or_default(),
                *nanos,
                i64::checked_add,
            )?;
        }
        for (reason, n) in &other.unpriced_by_reason {
            count(
                self.unpriced_by_reason.entry(reason.clone()).or_default(),
                *n,
            )?;
        }
        count(&mut self.covered_measurements, other.covered_measurements)?;
        count(&mut self.unpriced_measurements, other.unpriced_measurements)?;
        count(&mut self.pending_measurements, other.pending_measurements)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    pub tokens: Tokens,
    pub cost: Cost,
}
impl Totals {
    fn merge(&mut self, other: &Self) -> Result<(), ReportError> {
        self.tokens.merge(&other.tokens)?;
        self.cost.merge(&other.cost)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Model {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub steps: u64,
    pub tokens: Tokens,
    pub cost: Cost,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRow {
    pub key: RunKey,
    pub first_at_ms: i64,
    pub last_at_ms: i64,
    pub steps: u64,
    pub tokens: Tokens,
    pub cost: Cost,
    pub pricing_unit: String,
    pub models: Vec<Model>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_class: Option<String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeTotals {
    pub rows: u64,
    pub steps: u64,
    pub tokens: Tokens,
    pub cost: Cost,
}
#[derive(Clone, Debug, Serialize)]
pub struct ListReply {
    pub rows: Vec<RunRow>,
    pub totals: RangeTotals,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quantity {
    pub units: i64,
    pub exponent: i32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Measurement {
    pub role: String,
    pub unit: String,
    pub quantity: Option<Quantity>,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_nanos: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct DetailItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_ref: Option<String>,
    pub occurred_at_ms: Option<i64>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub measurements: Vec<Measurement>,
}
#[derive(Clone, Debug, Serialize)]
pub struct DetailReply {
    pub key: RunKey,
    pub pricing_unit: String,
    pub items: Vec<DetailItem>,
    pub totals: Totals,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

type CellKey = (RunKey, Option<String>, Option<String>);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Cell {
    first: i64,
    last: i64,
    steps: u64,
    totals: Totals,
    session: Option<String>,
    work_class: Option<String>,
}
pub(crate) type Cells = BTreeMap<CellKey, Cell>;

struct Event {
    id: i64,
    key: Option<RunKey>,
    reference: String,
    time: Option<i64>,
    provider: Option<String>,
    model: Option<String>,
    session: Option<String>,
    work_class: Option<String>,
    measurements: Vec<Measurement>,
}
impl Event {
    fn totals(&self) -> Result<Totals, ReportError> {
        let mut totals = Totals::default();
        for m in &self.measurements {
            if let Some(q) = &m.quantity {
                if let Some(value) = whole_tokens(q)
                    .filter(|_| m.unit == "token" || m.unit.starts_with("token_cache_"))
                {
                    let target = match m.role.as_str() {
                        "input" => Some(&mut totals.tokens.input),
                        "output" => Some(&mut totals.tokens.output),
                        "reasoning" => Some(&mut totals.tokens.reasoning),
                        "cache_read" => Some(&mut totals.tokens.cache_read),
                        "cache_write" => Some(&mut totals.tokens.cache_write),
                        _ => None,
                    };
                    if let Some(target) = target {
                        optional(target, Some(value))?;
                    }
                }
            }
            match m.state.as_str() {
                "priced" => add(
                    totals
                        .cost
                        .priced
                        .entry(
                            m.currency
                                .clone()
                                .ok_or(ReportError::Integrity("priced currency absent"))?,
                        )
                        .or_default(),
                    m.amount_nanos
                        .ok_or(ReportError::Integrity("priced amount absent"))?,
                    i64::checked_add,
                )?,
                "covered" => count(&mut totals.cost.covered_measurements, 1)?,
                "unpriced" => {
                    count(&mut totals.cost.unpriced_measurements, 1)?;
                    count(
                        totals
                            .cost
                            .unpriced_by_reason
                            .entry(
                                m.reason
                                    .clone()
                                    .ok_or(ReportError::Integrity("unpriced reason absent"))?,
                            )
                            .or_default(),
                        1,
                    )?;
                }
                "pending" => count(&mut totals.cost.pending_measurements, 1)?,
                _ => return Err(ReportError::Integrity("unknown charge state")),
            }
        }
        // The feed adds reasoning only when output was reported by that step.
        if totals.tokens.output.is_some() {
            optional(&mut totals.tokens.output, totals.tokens.reasoning)?;
        }
        Ok(totals)
    }
}

fn event_key(
    producer: String,
    session: Option<String>,
    run: Option<String>,
    flow: Option<String>,
) -> Option<RunKey> {
    if producer == "broca" {
        run.map(|run_id| RunKey::Run {
            harness: producer.clone(),
            run_id,
        })
        .or_else(|| {
            flow.map(|flow_id| RunKey::Flow {
                harness: producer,
                flow_id,
            })
        })
    } else if HARNESSES.contains(&producer.as_str()) {
        session.map(|session| RunKey::Session {
            harness: producer,
            session,
        })
    } else {
        None
    }
}

fn base_sql(filter: &str, index: &str) -> String {
    format!("WITH selected AS MATERIALIZED (SELECT * FROM usage_event INDEXED BY {index} WHERE {filter})
        SELECT e.event_key,e.producer,e.session,e.run_ref,e.flow_ref,e.producer_ref,e.occurred_at_ms,e.provider,e.model,e.work_class,
        m.role,m.unit,m.quantity_units,m.quantity_exponent,h.outcome,h.charge_nanos,h.currency,h.unpriced_reason
        FROM selected e LEFT JOIN measurement m ON m.event_key=e.event_key
        LEFT JOIN priced_charge h ON h.event_key=m.event_key AND h.role=m.role AND h.unit=m.unit
            AND NOT EXISTS(SELECT 1 FROM priced_charge child WHERE child.supersedes=h.charge_key)
        ORDER BY e.event_key,m.role,m.unit")
}

fn visit(
    conn: &Connection,
    filter: &str,
    index: &str,
    values: &[&dyn rusqlite::ToSql],
    mut observe: impl FnMut(Event) -> Result<(), ReportError>,
) -> Result<(), ReportError> {
    let mut stmt = conn.prepare(&base_sql(filter, index))?;
    let mut rows = stmt.query(values)?;
    let mut current: Option<Event> = None;
    while let Some(row) = rows.next()? {
        let id = row.get(0)?;
        if current.as_ref().is_none_or(|event| event.id != id) {
            if let Some(event) = current.take() {
                observe(event)?;
            }
            let session: Option<String> = row.get(2)?;
            let key = event_key(row.get(1)?, session.clone(), row.get(3)?, row.get(4)?);
            current = Some(Event {
                id,
                key,
                reference: row.get(5)?,
                time: row.get(6)?,
                provider: row.get(7)?,
                model: row.get(8)?,
                session,
                work_class: row.get(9)?,
                measurements: Vec::new(),
            });
        }
        if let Some(role) = row.get::<_, Option<String>>(10)? {
            let state: String = row
                .get::<_, Option<String>>(14)?
                .unwrap_or_else(|| "pending".into());
            let priced = state == "priced";
            let quantity = match (row.get(12)?, row.get(13)?) {
                (Some(units), Some(exponent)) => Some(Quantity { units, exponent }),
                _ => None,
            };
            current
                .as_mut()
                .ok_or(ReportError::Integrity("event absent"))?
                .measurements
                .push(Measurement {
                    role,
                    unit: row.get(11)?,
                    quantity,
                    amount_nanos: if priced { row.get(15)? } else { None },
                    currency: if priced { row.get(16)? } else { None },
                    reason: if state == "unpriced" {
                        row.get(17)?
                    } else {
                        None
                    },
                    state,
                });
        }
    }
    if let Some(event) = current {
        observe(event)?;
    }
    Ok(())
}

fn merge_cell(left: &mut Cell, other: &Cell) -> Result<(), ReportError> {
    left.first = left.first.min(other.first);
    left.last = left.last.max(other.last);
    count(&mut left.steps, other.steps)?;
    left.totals.merge(&other.totals)?;
    left.session = left.session.clone().max(other.session.clone());
    left.work_class = left.work_class.clone().max(other.work_class.clone());
    Ok(())
}
fn insert(cells: &mut Cells, key: CellKey, cell: Cell) -> Result<(), ReportError> {
    if let Some(left) = cells.get_mut(&key) {
        merge_cell(left, &cell)
    } else {
        cells.insert(key, cell);
        Ok(())
    }
}
pub(crate) fn observe(
    conn: &Connection,
    filter: &str,
    cells: &mut Cells,
) -> Result<(), ReportError> {
    visit(conn, filter, "usage_event_summary_time", &[], |event| {
        if let Some(key) = &event.key {
            let time = event.time.unwrap_or(0);
            insert(
                cells,
                (key.clone(), event.provider.clone(), event.model.clone()),
                Cell {
                    first: time,
                    last: time,
                    steps: 1,
                    totals: event.totals()?,
                    session: event.session,
                    work_class: event.work_class,
                },
            )?;
        }
        Ok(())
    })
}

const CELL_COLUMNS: &str = "run_key,provider,model,first_ms,last_ms,steps,input,output,reasoning,cache_read,cache_write,priced,covered,unpriced,pending,reasons,session,work_class";
fn parsed<T: serde::de::DeserializeOwned>(
    row: &rusqlite::Row<'_>,
    column: usize,
) -> Result<T, ReportError> {
    serde_json::from_str(&row.get::<_, String>(column)?)
        .map_err(|_| ReportError::Integrity("invalid run summary value"))
}
fn cell_row(row: &rusqlite::Row<'_>) -> Result<(CellKey, Cell), ReportError> {
    Ok((
        (parsed(row, 0)?, parsed(row, 1)?, parsed(row, 2)?),
        Cell {
            first: row.get(3)?,
            last: row.get(4)?,
            steps: row.get(5)?,
            totals: Totals {
                tokens: Tokens {
                    input: stored_count(row, 6)?,
                    output: stored_count(row, 7)?,
                    reasoning: stored_count(row, 8)?,
                    cache_read: stored_count(row, 9)?,
                    cache_write: stored_count(row, 10)?,
                },
                cost: Cost {
                    priced: parsed(row, 11)?,
                    covered_measurements: row.get(12)?,
                    unpriced_measurements: row.get(13)?,
                    pending_measurements: row.get(14)?,
                    unpriced_by_reason: parsed(row, 15)?,
                },
            },
            session: row.get(16)?,
            work_class: row.get(17)?,
        },
    ))
}
pub(crate) fn stored(conn: &Connection, kind: i64, hour: i64) -> Result<Cells, ReportError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {CELL_COLUMNS} FROM summary_run_cell WHERE kind=?1 AND hour_number=?2"
    ))?;
    let mut rows = stmt.query(params![kind, hour])?;
    let mut cells = Cells::new();
    while let Some(row) = rows.next()? {
        let (key, cell) = cell_row(row)?;
        cells.insert(key, cell);
    }
    Ok(cells)
}
pub(crate) fn replace(
    conn: &Connection,
    kind: i64,
    hour: i64,
    cells: &Cells,
) -> Result<(), ReportError> {
    conn.execute(
        "DELETE FROM summary_run_cell WHERE kind=?1 AND hour_number=?2",
        params![kind, hour],
    )?;
    let mut stmt = conn.prepare("INSERT INTO summary_run_cell VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)")?;
    for ((key, provider, model), cell) in cells {
        let json = |value| {
            serde_json::to_string(value).map_err(|_| ReportError::Integrity("invalid run cell"))
        };
        let tokens = &cell.totals.tokens;
        let cost = &cell.totals.cost;
        stmt.execute(params![
            kind,
            hour,
            json(key)?,
            serde_json::to_string(provider).unwrap(),
            serde_json::to_string(model).unwrap(),
            cell.first,
            cell.last,
            cell.steps,
            count_value(tokens.input),
            count_value(tokens.output),
            count_value(tokens.reasoning),
            count_value(tokens.cache_read),
            count_value(tokens.cache_write),
            serde_json::to_string(&cost.priced).unwrap(),
            cost.covered_measurements,
            cost.unpriced_measurements,
            cost.pending_measurements,
            serde_json::to_string(&cost.unpriced_by_reason).unwrap(),
            cell.session,
            cell.work_class
        ])?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListCursor {
    version: String,
    from_ms: i64,
    to_ms: i64,
    harness: Option<String>,
    last_at_ms: i64,
    key: RunKey,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DetailCursor {
    version: String,
    key: RunKey,
    time: Option<i64>,
    event: i64,
}
fn encode<T: Serialize>(value: &T) -> Result<String, Error> {
    Ok(hex::encode(serde_json::to_vec(value).map_err(|_| {
        ReportError::Integrity("invalid runs cursor")
    })?))
}
fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, Error> {
    let bytes = hex::decode(value).map_err(|_| Error::Refusal("invalid_request"))?;
    serde_json::from_slice(&bytes).map_err(|_| Error::Refusal("invalid_request"))
}
fn limit(requested: Option<u32>, default: u32, max: u32) -> Result<usize, Error> {
    let value = requested.unwrap_or(default);
    if value == 0 || value > max {
        return Err(Error::Refusal("invalid_request"));
    }
    usize::try_from(value).map_err(|_| Error::Refusal("invalid_request"))
}

struct Frame {
    clean: String,
    fallback: Cells,
}
fn range_frame(conn: &Connection, request: &ListRequest) -> Result<Frame, ReportError> {
    let first = request.from_ms.div_euclid(HOUR_MS);
    let last = (request.to_ms - 1).div_euclid(HOUR_MS);
    let mut stmt=conn.prepare("SELECT hour_number,built_revision=revision AND format_version=?3 AND NOT EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=0 AND d.hour_number=h.hour_number) FROM summary_hour h WHERE kind=0 AND hour_number BETWEEN ?1 AND ?2")?;
    let metadata: BTreeMap<i64, bool> = stmt
        .query_map(params![first, last, FORMAT], |r| {
            Ok((r.get(0)?, r.get::<_, Option<bool>>(1)?.unwrap_or(false)))
        })?
        .collect::<Result<_, _>>()?;
    let mut clean = Vec::new();
    let mut partial_hours = 0;
    let mut dirty_hours = 0;
    let mut fallback = Cells::new();
    for hour in first..=last {
        let from = i128::from(hour) * i128::from(HOUR_MS);
        let to = from + i128::from(HOUR_MS);
        if from >= i128::from(request.from_ms)
            && to <= i128::from(request.to_ms)
            && metadata.get(&hour) == Some(&true)
        {
            clean.push(hour.to_string());
        } else {
            if from < i128::from(request.from_ms) || to > i128::from(request.to_ms) {
                partial_hours += 1;
            } else {
                dirty_hours += 1;
            }
            let from = from.max(i128::from(request.from_ms));
            let to = to.min(i128::from(request.to_ms));
            let mut filter = format!("occurred_at_ms>={from} AND occurred_at_ms<{to}");
            if let Some(harness) = &request.harness {
                filter.push_str(&format!(" AND producer='{harness}'"));
            }
            observe(conn, &filter, &mut fallback)?;
        }
    }
    let clean = if clean.is_empty() {
        "0".into()
    } else {
        let low = clean
            .first()
            .unwrap()
            .parse::<i64>()
            .map_err(|_| ReportError::Arithmetic)?;
        let high = clean
            .last()
            .unwrap()
            .parse::<i64>()
            .map_err(|_| ReportError::Arithmetic)?;
        let excluded: Vec<_> = (low..=high)
            .filter(|h| metadata.get(h) != Some(&true))
            .map(|h| h.to_string())
            .collect();
        let holes = if excluded.is_empty() {
            String::new()
        } else {
            format!(" AND c.hour_number NOT IN ({})", excluded.join(","))
        };
        format!("c.kind=0 AND c.hour_number BETWEEN {low} AND {high}{holes}")
    };
    // Harness is the producer in a session key, not usage_event.harness.
    let clean = if let Some(h) = &request.harness {
        format!("({clean}) AND json_extract(c.run_key,'$.harness')='{h}'")
    } else {
        clean
    };
    if std::env::var_os("ASTROCYTE_RUNS_PROFILE").is_some() {
        eprintln!(
            "{}",
            serde_json::json!({"partial_hours":partial_hours,"dirty_or_missing_hours":dirty_hours})
        );
    }
    Ok(Frame { clean, fallback })
}

fn summed(conn: &Connection, filter: &str) -> Result<(u64, Totals), ReportError> {
    let mut stmt=conn.prepare(&format!("SELECT steps,input,output,reasoning,cache_read,cache_write,priced,covered,unpriced,pending,reasons FROM summary_run_cell c WHERE {filter}"))?;
    let mut rows = stmt.query([])?;
    let mut steps = 0;
    let mut totals = Totals::default();
    while let Some(row) = rows.next()? {
        count(&mut steps, row.get(0)?)?;
        totals.merge(&Totals {
            tokens: Tokens {
                input: stored_count(row, 1)?,
                output: stored_count(row, 2)?,
                reasoning: stored_count(row, 3)?,
                cache_read: stored_count(row, 4)?,
                cache_write: stored_count(row, 5)?,
            },
            cost: Cost {
                priced: parsed(row, 6)?,
                covered_measurements: row.get(7)?,
                unpriced_measurements: row.get(8)?,
                pending_measurements: row.get(9)?,
                unpriced_by_reason: parsed(row, 10)?,
            },
        })?;
    }
    Ok((steps, totals))
}
fn key_text(key: &RunKey) -> Result<String, ReportError> {
    serde_json::to_string(key).map_err(|_| ReportError::Integrity("invalid run key"))
}
fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn page_rows(conn: &Connection, cells: Cells) -> Result<Vec<RunRow>, Error> {
    use rusqlite::OptionalExtension;
    let mut rows: BTreeMap<RunKey, RunRow> = BTreeMap::new();
    for ((key, provider, model), cell) in cells {
        let row = rows.entry(key.clone()).or_insert_with(|| RunRow {
            pricing_unit: key.pricing_unit().into(),
            key,
            first_at_ms: cell.first,
            last_at_ms: cell.last,
            steps: 0,
            tokens: Tokens::default(),
            cost: Cost::default(),
            models: Vec::new(),
            session: None,
            work_class: None,
        });
        row.first_at_ms = row.first_at_ms.min(cell.first);
        row.last_at_ms = row.last_at_ms.max(cell.last);
        count(&mut row.steps, cell.steps)?;
        row.tokens.merge(&cell.totals.tokens)?;
        row.cost.merge(&cell.totals.cost)?;
        if matches!(row.key, RunKey::Run { .. }) {
            row.session = row.session.clone().max(cell.session);
        }
        if matches!(row.key, RunKey::Flow { .. }) {
            row.work_class = row.work_class.clone().max(cell.work_class);
        }
        row.models.push(Model {
            provider,
            model,
            steps: cell.steps,
            tokens: cell.totals.tokens,
            cost: cell.totals.cost,
        });
    }
    let mut rows: Vec<_> = rows.into_values().collect();
    rows.sort_by_cached_key(|r| {
        (
            std::cmp::Reverse(r.last_at_ms),
            key_text(&r.key).unwrap_or_default(),
        )
    });
    for row in &mut rows {
        if let RunKey::Run { run_id, .. } = &row.key {
            // The label can be on a segment outside the requested time range.
            row.session = conn.query_row("SELECT session FROM usage_event INDEXED BY usage_event_run_time WHERE run_ref=?1 AND producer='broca' AND session IS NOT NULL LIMIT 1", [run_id], |r| r.get(0)).optional()?;
        }
    }
    Ok(rows)
}

/// The caller holds one read transaction across the whole operation.
pub fn list(conn: &Connection, request: &ListRequest) -> Result<ListReply, Error> {
    use std::collections::BTreeSet;
    if request.from_ms >= request.to_ms {
        return Err(Error::Refusal("invalid_range"));
    }
    if i128::from(request.to_ms) - i128::from(request.from_ms) > i128::from(MAX_RANGE) {
        return Err(Error::Refusal("range_too_large"));
    }
    if request
        .harness
        .as_ref()
        .is_some_and(|h| !HARNESSES.contains(&h.as_str()))
    {
        return Err(Error::Refusal("invalid_request"));
    }
    let n = limit(request.limit, 50, 200)?;
    let after: Option<ListCursor> = request.after.as_deref().map(decode).transpose()?;
    if after.as_ref().is_some_and(|c| {
        c.version != "usage-runs-v1/list"
            || c.from_ms != request.from_ms
            || c.to_ms != request.to_ms
            || c.harness != request.harness
    }) {
        return Err(Error::Refusal("invalid_request"));
    }
    let after_key = after.as_ref().map(|c| key_text(&c.key)).transpose()?;
    let eligible = |last: i64, key: &str| {
        after.as_ref().is_none_or(|c| {
            last < c.last_at_ms || (last == c.last_at_ms && Some(key) > after_key.as_deref())
        })
    };
    let mut probe = Probe::new(conn);
    let frame = range_frame(conn, request)?;
    probe.phase("boundary");
    let (mut steps, mut total) = summed(conn, &frame.clean)?;
    for cell in frame.fallback.values() {
        count(&mut steps, cell.steps)?;
        total.merge(&cell.totals)?;
    }
    probe.phase("aggregate");
    let mut row_count:u64=conn.query_row(&format!("SELECT count(*) FROM (SELECT run_key FROM summary_run_cell c INDEXED BY summary_run_key_time WHERE {} GROUP BY run_key)",frame.clean),[],|r|r.get(0))?;
    probe.phase("distinct_keys");
    let mut fallback_last: BTreeMap<String, i64> = BTreeMap::new();
    for ((key, _, _), cell) in &frame.fallback {
        fallback_last
            .entry(key_text(key)?)
            .and_modify(|t| *t = (*t).max(cell.last))
            .or_insert(cell.last);
    }
    let maxima = fallback_maxima(conn, &frame.clean, fallback_last.keys())?;
    let mut candidates = BTreeMap::new();
    for (key, last) in &mut fallback_last {
        if let Some(clean) = maxima.get(key) {
            *last = (*last).max(*clean);
        } else {
            count(&mut row_count, 1)?;
        }
        if eligible(*last, key) {
            candidates.insert(key.clone(), *last);
        }
    }
    if probe.enabled {
        eprintln!(
            "{}",
            serde_json::json!({"fallback_keys":fallback_last.len()})
        );
    }
    probe.phase("fallback_maxima");
    let resume = after.as_ref().map_or_else(
        || "1".into(),
        |c| {
            format!(
                "(c.last_ms<{} OR (c.last_ms={} AND c.run_key>{}))",
                c.last_at_ms,
                c.last_at_ms,
                quoted(after_key.as_deref().unwrap_or_default())
            )
        },
    );
    let sql=format!("SELECT c.run_key,c.last_ms FROM summary_run_cell c INDEXED BY summary_run_activity WHERE ({}) AND {resume} AND NOT EXISTS(SELECT 1 FROM summary_run_cell newer INDEXED BY summary_run_key_time WHERE newer.run_key=c.run_key AND ({}) AND newer.last_ms>c.last_ms) ORDER BY c.last_ms DESC,c.run_key",frame.clean,frame.clean.replace("c.","newer."));
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut seen = BTreeSet::new();
    while let Some(row) = rows.next()? {
        let key: String = row.get(0)?;
        if fallback_last.contains_key(&key) || !seen.insert(key.clone()) {
            continue;
        }
        candidates.insert(key, row.get(1)?);
        if seen.len() > n {
            break;
        }
    }
    let mut candidates: Vec<_> = candidates.into_iter().collect();
    candidates.sort_by(|(ak, at), (bk, bt)| bt.cmp(at).then(ak.cmp(bk)));
    let more = candidates.len() > n;
    candidates.truncate(n);
    probe.phase("candidates");
    let selected: BTreeSet<_> = candidates.iter().map(|(k, _)| k.as_str()).collect();
    let mut cells = Cells::new();
    for (key, cell) in frame.fallback {
        if selected.contains(key_text(&key.0)?.as_str()) {
            insert(&mut cells, key, cell)?;
        }
    }
    let mut stmt=conn.prepare(&format!("SELECT {CELL_COLUMNS} FROM summary_run_cell c INDEXED BY summary_run_key_time WHERE c.run_key=?1 AND ({})",frame.clean))?;
    for (key, _) in &candidates {
        let mut rows = stmt.query([key])?;
        while let Some(row) = rows.next()? {
            let (key, cell) = cell_row(row)?;
            insert(&mut cells, key, cell)?;
        }
    }
    let rows = page_rows(conn, cells)?;
    let next = if more {
        rows.last()
            .map(|row| {
                encode(&ListCursor {
                    version: "usage-runs-v1/list".into(),
                    from_ms: request.from_ms,
                    to_ms: request.to_ms,
                    harness: request.harness.clone(),
                    last_at_ms: row.last_at_ms,
                    key: row.key.clone(),
                })
            })
            .transpose()?
    } else {
        None
    };
    probe.phase("page");
    Ok(ListReply {
        rows,
        totals: RangeTotals {
            rows: row_count,
            steps,
            tokens: total.tokens,
            cost: total.cost,
        },
        next,
    })
}

/// Detail pages use the key/time index. Whole-key totals come from clean hour
/// cells and fresh reductions of only the dirty hours in the same snapshot.
/// Null timestamps sort before dated items, as in SQLite's ascending order.
pub fn detail(conn: &Connection, request: &DetailRequest) -> Result<DetailReply, Error> {
    request.key.validate()?;
    let n = limit(request.limit, 200, 1000)?;
    let after: Option<DetailCursor> = request.after.as_deref().map(decode).transpose()?;
    if after
        .as_ref()
        .is_some_and(|c| c.version != "usage-runs-v1/detail" || c.key != request.key)
    {
        return Err(Error::Refusal("invalid_request"));
    }
    let (filter, index, value): (&str, &str, &str) = match &request.key {
        RunKey::Session { session, .. } => (
            "session=?1 AND producer=?2",
            "usage_event_session_time",
            session,
        ),
        RunKey::Run { run_id, .. } => {
            ("run_ref=?1 AND producer=?2", "usage_event_run_time", run_id)
        }
        RunKey::Flow { flow_id, .. } => (
            "flow_ref=?1 AND producer=?2",
            "usage_event_flow_time",
            flow_id,
        ),
    };
    let values: &[&dyn rusqlite::ToSql] = &[&value, &request.key.harness()];
    let found: bool = conn.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM usage_event INDEXED BY {index} WHERE {filter})"),
        values,
        |r| r.get(0),
    )?;
    if !found {
        return Err(Error::Refusal("not_found"));
    }
    let mut probe = Probe::new(conn);
    let clean=format!("c.run_key={} AND EXISTS(SELECT 1 FROM summary_hour h WHERE h.kind=c.kind AND h.hour_number=c.hour_number AND h.built_revision=h.revision AND h.format_version={FORMAT} AND NOT EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=h.kind AND d.hour_number=h.hour_number))",quoted(&key_text(&request.key)?));
    let (_, mut totals) = summed(conn, &clean)?;
    let mut stmt=conn.prepare("SELECT kind,hour_number FROM summary_hour h WHERE built_revision IS NOT revision OR format_version IS NOT ?1 OR EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=h.kind AND d.hour_number=h.hour_number)")?;
    let hours = stmt
        .query_map([FORMAT], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    for (kind, hour) in hours {
        let time = if kind == 1 {
            "occurred_at_ms IS NULL".into()
        } else {
            let from = i128::from(hour) * i128::from(HOUR_MS);
            format!(
                "occurred_at_ms>={from} AND occurred_at_ms<{}",
                from + i128::from(HOUR_MS)
            )
        };
        visit(
            conn,
            &format!("({filter}) AND ({time})"),
            index,
            values,
            |event| totals.merge(&event.totals()?),
        )?;
    }
    probe.phase("detail_totals");
    let resume = after.as_ref().map_or_else(
        || "1".into(),
        |c| match c.time {
            Some(t) => format!(
                "(occurred_at_ms>{t} OR (occurred_at_ms={t} AND event_key>{}))",
                c.event
            ),
            None => format!(
                "(occurred_at_ms IS NOT NULL OR (occurred_at_ms IS NULL AND event_key>{}))",
                c.event
            ),
        },
    );
    let page = format!(
        "({filter}) AND {resume} ORDER BY occurred_at_ms,event_key LIMIT {}",
        n + 1
    );
    let mut items = BTreeMap::new();
    visit(conn, &page, index, values, |event| {
        let segment = request.key.harness() == "broca";
        items.insert(
            (event.time, event.id),
            DetailItem {
                step_ref: if segment {
                    None
                } else {
                    Some(event.reference.clone())
                },
                segment_ref: if segment { Some(event.reference) } else { None },
                occurred_at_ms: event.time,
                provider: event.provider,
                model: event.model,
                measurements: event.measurements,
            },
        );
        Ok(())
    })?;
    let more = items.len() > n;
    if more {
        items.pop_last();
    }
    let next = if more {
        items
            .last_key_value()
            .map(|((time, event), _)| {
                encode(&DetailCursor {
                    version: "usage-runs-v1/detail".into(),
                    key: request.key.clone(),
                    time: *time,
                    event: *event,
                })
            })
            .transpose()?
    } else {
        None
    };
    probe.phase("detail_page");
    Ok(DetailReply {
        key: request.key.clone(),
        pricing_unit: request.key.pricing_unit().into(),
        items: items.into_values().collect(),
        totals,
        next,
    })
}

fn fallback_maxima_sql(clean: &str, keys: &[String]) -> String {
    let values = keys
        .iter()
        .map(|k| format!("({})", quoted(k)))
        .collect::<Vec<_>>()
        .join(",");
    // CROSS JOIN keeps the small key set outermost even when SQLite has no
    // collected table statistics (sqlite_stat1) to estimate join populations.
    // INDEXED BY prevents MAX's activity-index shortcut from walking other keys.
    format!(
        "WITH keys(run_key) AS (VALUES {values}) SELECT keys.run_key,MAX(c.last_ms)
        FROM keys CROSS JOIN summary_run_cell c INDEXED BY summary_run_key_time
        WHERE c.run_key=keys.run_key AND ({clean}) GROUP BY keys.run_key"
    )
}
fn fallback_maxima<'a>(
    conn: &Connection,
    clean: &str,
    keys: impl Iterator<Item = &'a String>,
) -> Result<BTreeMap<String, i64>, ReportError> {
    let keys: Vec<_> = keys.cloned().collect();
    let mut maxima = BTreeMap::new();
    // Bound SQL size for a heavily dirty store; every batch still seeks by key.
    for batch in keys.chunks(500) {
        let mut stmt = conn.prepare(&fallback_maxima_sql(clean, batch))?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        for row in rows {
            let (key, last) = row?;
            maxima.insert(key, last);
        }
    }
    Ok(maxima)
}

// Opt-in native diagnostics; normal served reads never install a progress hook.
// The hook owns a stable allocation and is removed even on an early error.
struct Probe<'a> {
    conn: &'a Connection,
    enabled: bool,
    steps: Box<u64>,
    started: std::time::Instant,
}
unsafe extern "C" fn progress(pointer: *mut std::ffi::c_void) -> std::ffi::c_int {
    unsafe {
        *pointer.cast::<u64>() += 1;
    }
    0
}
impl<'a> Probe<'a> {
    fn new(conn: &'a Connection) -> Self {
        let mut probe = Self {
            conn,
            enabled: std::env::var_os("ASTROCYTE_RUNS_PROFILE").is_some(),
            steps: Box::new(0),
            started: std::time::Instant::now(),
        };
        if probe.enabled {
            unsafe {
                rusqlite::ffi::sqlite3_progress_handler(
                    conn.handle(),
                    1,
                    Some(progress),
                    std::ptr::from_mut(probe.steps.as_mut()).cast(),
                );
            }
        }
        probe
    }
    fn phase(&mut self, name: &str) {
        if self.enabled {
            eprintln!(
                "{}",
                serde_json::json!({"phase":name,"vm_steps":*self.steps,"milliseconds":self.started.elapsed().as_secs_f64()*1000.0})
            );
        }
        *self.steps = 0;
        self.started = std::time::Instant::now();
    }
}
impl Drop for Probe<'_> {
    fn drop(&mut self) {
        if self.enabled {
            unsafe {
                rusqlite::ffi::sqlite3_progress_handler(
                    self.conn.handle(),
                    0,
                    None,
                    std::ptr::null_mut(),
                );
            }
        }
    }
}

fn whole_tokens(quantity: &Quantity) -> Option<u64> {
    let units = u64::try_from(quantity.units).ok()?;
    if quantity.exponent <= 0 {
        let divisor = 10_u64.checked_pow(quantity.exponent.unsigned_abs())?;
        if units % divisor == 0 {
            Some(units / divisor)
        } else {
            None
        }
    } else {
        units.checked_mul(10_u64.checked_pow(quantity.exponent.unsigned_abs())?)
    }
}
// SQLite INTEGER is signed. A nonnumeric prefix preserves rare wider u64
// aggregates as exact text instead of coercing decimal text to a lossy REAL.
fn count_value(value: Option<u64>) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match value {
        None => Value::Null,
        Some(n) => match i64::try_from(n) {
            Ok(n) => Value::Integer(n),
            Err(_) => Value::Text(format!("u{n}")),
        },
    }
}
fn stored_count(row: &rusqlite::Row<'_>, column: usize) -> Result<Option<u64>, ReportError> {
    use rusqlite::types::ValueRef;
    match row.get_ref(column)? {
        ValueRef::Null => Ok(None),
        ValueRef::Integer(n) => Ok(Some(u64::try_from(n).map_err(|_| ReportError::Arithmetic)?)),
        ValueRef::Text(text) => Ok(Some(
            std::str::from_utf8(text)
                .ok()
                .and_then(|s| s.strip_prefix('u'))
                .and_then(|s| s.parse().ok())
                .ok_or(ReportError::Integrity("invalid wide token count"))?,
        )),
        _ => Err(ReportError::Integrity("invalid token count")),
    }
}

#[cfg(test)]
mod tests;
