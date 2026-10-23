//! Cache diagnostics: the model steps the harness readers recorded, per
//! session, with the cache verdicts derived from them.
//!
//! Every harness step is already a ledger event (`usage_event` plus its
//! `measurement` rows), and its finish reason and context window sit in
//! `step_detail`. Nothing derived here is stored: steps can arrive late
//! (OpenCode holds in-flight messages back) and stored rows cannot be
//! corrected, so a stored verdict could be frozen wrong. The verdict rules
//! are the Magic Context dashboard's, with one deliberate difference: they
//! run over the whole session's steps in time order, never over the window a
//! caller asked for, so a step's verdict does not depend on the page size.

use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};

/// Producers whose events are one model step each. Goose records one total
/// per session and BROCA's steps come from its own facts, so neither is here.
pub const CACHE_HARNESSES: [&str; 7] = [
    "claude-code",
    "codex",
    "gemini",
    "kimi",
    "omp",
    "opencode",
    "pi",
];

/// [`CACHE_HARNESSES`] as an SQL list. A test keeps the two equal.
macro_rules! harnesses_sql {
    () => {
        "('claude-code','codex','gemini','kimi','omp','opencode','pi')"
    };
}

/// A prompt that shrank by at least this many tokens since the previous
/// cache-reporting step is a drop. Magic Context's reclaims are tens of
/// thousands of tokens; this stays well above per-step accounting noise.
pub const DROP_MIN_TOKENS: u64 = 15_000;
/// Retention at or above this is `stable`.
pub const STABLE_MIN_RETENTION: f64 = 0.95;
/// Retention at or above this, and below [`STABLE_MIN_RETENTION`], is
/// `warning`; below it is `bust`.
pub const WARNING_MIN_RETENTION: f64 = 0.80;

pub const STEPS_DEFAULT_LIMIT: u32 = 200;
pub const STEPS_MAX_LIMIT: u32 = 1_000;
pub const SESSIONS_DEFAULT_LIMIT: u32 = 50;
pub const SESSIONS_MAX_LIMIT: u32 = 200;
/// The default `usage.cache.sessions` window, and how recent a session must
/// be to be kept in the serving side's memory.
pub const RECENT_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;
/// The oldest `since_ms` `usage.cache.sessions` accepts, measured back from
/// now. The reply reads every step in the window, so the window is bounded.
pub const SESSIONS_MAX_WINDOW_MS: i64 = 7 * RECENT_WINDOW_MS;

/// The connection-private list of events that gained a `step_detail` row
/// after they were recorded. See `ingest::note_late_step_detail`.
pub(crate) const CREATE_LATE_STEP_DETAIL: &str =
    "CREATE TEMP TABLE IF NOT EXISTS step_detail_late (event_key INTEGER PRIMARY KEY)";

/// How a producer's cache reports are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheModel {
    /// The provider caches a prefix and reports what it wrote (Anthropic's
    /// cache writes) or the uncached input that becomes cacheable. Retention
    /// is measured against what the previous step should have left cached.
    Explicit,
    /// Codex: OpenAI's implicit prefix cache never reports writes, and the
    /// reader stores the uncached part of the input, so retention is the
    /// cached share of this step's own prompt.
    Implicit,
}

impl CacheModel {
    pub fn of(producer: &str) -> Self {
        if producer == "codex" {
            Self::Implicit
        } else {
            Self::Explicit
        }
    }
}

/// The token counts the verdict rules read. `None` is a count the reader
/// did not store, which is not a reported zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepCounts {
    pub input: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
}

impl StepCounts {
    /// Input plus cache read plus cache write; an absent count adds nothing.
    pub fn prompt(&self) -> u64 {
        self.input
            .unwrap_or(0)
            .saturating_add(self.cache_read.unwrap_or(0))
            .saturating_add(self.cache_write.unwrap_or(0))
    }

    /// A step reports its cache when its reader stored a cache read.
    pub fn reports_cache(&self) -> bool {
        self.cache_read.is_some()
    }
}

/// What the verdict rules need to know about the steps before a run of
/// steps: whether there are any, and the last one that reported its cache.
/// Folding every earlier step of the session with [`Lead::advance`] gives the
/// lead of the next one, so a window's lead carries the whole session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lead {
    pub earlier_steps: bool,
    pub baseline: Option<StepCounts>,
}

impl Lead {
    pub fn advance(&mut self, step: &StepCounts) {
        self.earlier_steps = true;
        if step.reports_cache() {
            self.baseline = Some(*step);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The session's first step: there is nothing to compare it with.
    ColdStart,
    Stable,
    Warning,
    Bust,
    /// Nothing was read from the cache although the previous step had a
    /// cache to read from.
    FullBust,
    /// The step reports no cache read, or (for a prefix cache) no step of
    /// its session ever read from the cache, so its cache health cannot be
    /// judged.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Derived {
    pub prompt: u64,
    pub retention: Option<f64>,
    pub verdict: Verdict,
    pub prompt_dropped: bool,
}

/// Derive every step's values, in time order, from the steps' counts and the
/// lead of the first one.
///
/// `session_reads_cache` says whether any dated step of the whole session
/// read a positive number of cached tokens. A prefix-cache session that
/// never did (ollama-cloud reports a zero read everywhere) does not expose
/// its cache, and every step is `unknown` rather than a false bust.
pub fn derive(
    model: CacheModel,
    session_reads_cache: bool,
    lead: Lead,
    steps: &[StepCounts],
) -> Vec<Derived> {
    let mut lead = lead;
    steps
        .iter()
        .map(|step| {
            let derived = derive_step(model, session_reads_cache, &lead, step);
            lead.advance(step);
            derived
        })
        .collect()
}

fn derive_step(
    model: CacheModel,
    session_reads_cache: bool,
    lead: &Lead,
    step: &StepCounts,
) -> Derived {
    let prompt = step.prompt();
    let (verdict, retention) = match step.cache_read {
        None => (Verdict::Unknown, None),
        Some(_) if model == CacheModel::Explicit && !session_reads_cache => {
            (Verdict::Unknown, None)
        }
        Some(_) if !lead.earlier_steps => (Verdict::ColdStart, None),
        Some(read) => match model {
            CacheModel::Implicit => {
                let prompt = read.saturating_add(step.input.unwrap_or(0));
                if prompt == 0 {
                    (Verdict::Unknown, None)
                } else {
                    retention_verdict(read, prompt)
                }
            }
            CacheModel::Explicit => match lead.baseline {
                // No earlier step reported its cache: no baseline.
                None => (Verdict::Stable, None),
                Some(previous) => {
                    let previous_read = previous.cache_read.unwrap_or(0);
                    if previous_read == 0 {
                        // No cache was established on the previous step, so a
                        // low read now is not a loss.
                        (Verdict::Stable, None)
                    } else {
                        // What the previous step added to the cacheable prefix:
                        // its cache write when it reported one, otherwise its
                        // uncached input. Generated output is not counted: the
                        // host may not resend those bytes.
                        let growth = match previous.cache_write {
                            Some(write) if write > 0 => write,
                            _ => previous.input.unwrap_or(0),
                        };
                        retention_verdict(read, previous_read.saturating_add(growth))
                    }
                }
            },
        },
    };
    let prompt_dropped = lead.baseline.is_some_and(|previous| {
        previous
            .prompt()
            .checked_sub(prompt)
            .is_some_and(|drop| drop >= DROP_MIN_TOKENS)
    });
    Derived {
        prompt,
        retention,
        verdict,
        prompt_dropped,
    }
}

/// The verdict for `read` cached tokens against `expected` (never zero).
fn retention_verdict(read: u64, expected: u64) -> (Verdict, Option<f64>) {
    if read == 0 {
        return (Verdict::FullBust, Some(0.0));
    }
    #[allow(clippy::cast_precision_loss)] // A ratio of token counts; rounding is harmless.
    let retention = read as f64 / expected as f64;
    let verdict = if retention >= STABLE_MIN_RETENTION {
        Verdict::Stable
    } else if retention >= WARNING_MIN_RETENTION {
        Verdict::Warning
    } else {
        Verdict::Bust
    };
    (verdict, Some(retention))
}

/// One model step as the ledger stores it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepRow {
    /// The step's `event_key`. Keys grow in commit order, so a client can ask
    /// for the steps recorded after one it has seen.
    pub cursor: i64,
    /// The harness's own id for the step (`producer_ref`).
    pub step_ref: String,
    pub occurred_at_ms: i64,
    pub input: Option<u64>,
    pub cache_read: Option<u64>,
    /// Every cache-write measurement summed; Claude Code stores its 5-minute
    /// and 1-hour writes separately.
    pub cache_write: Option<u64>,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
    pub provider: Option<String>,
    pub model: Option<String>,
    /// `step_detail.finish`, else the event's `terminal_status` (Claude Code
    /// and Oh My Pi store their stop reason there).
    pub finish: Option<String>,
    pub context_limit: Option<i64>,
}

impl StepRow {
    pub fn counts(&self) -> StepCounts {
        StepCounts {
            input: self.input,
            cache_read: self.cache_read,
            cache_write: self.cache_write,
        }
    }
}

/// A step recorded by a harness producer, for the serving side's memory.
/// `step` is `None` for an undated step, which is counted, not placed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedStep {
    pub producer: String,
    pub session: String,
    pub step: Option<StepRow>,
}

/// The newest steps of one session, oldest first, with what the verdict
/// rules need from the rest of the session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSteps {
    pub rows: Vec<StepRow>,
    /// The lead of `rows[0]`: whether earlier dated steps exist, and the last
    /// of them that reported its cache.
    pub lead: Lead,
    pub reads_cache: bool,
    pub undated: u64,
}

/// A step's token count from its measurements: the sum over every unit of
/// the role, counting only plain token counts (exponent 0). NULL when the
/// reader stored none, which keeps an absent count apart from a zero.
macro_rules! role_sum {
    ($role:literal) => {
        concat!(
            "(SELECT SUM(m.quantity_units) FROM measurement m WHERE m.event_key=e.event_key AND m.role='",
            $role,
            "' AND m.quantity_exponent=0)"
        )
    };
}

/// The columns [`step_row`] reads, from `usage_event e` left-joined to
/// `step_detail d`.
macro_rules! step_columns {
    () => {
        concat!(
            "e.event_key,e.producer_ref,e.occurred_at_ms,e.provider,e.model,",
            "COALESCE(d.finish,e.terminal_status),d.context_limit,",
            role_sum!("input"),
            ",",
            role_sum!("cache_read"),
            ",",
            role_sum!("cache_write"),
            ",",
            role_sum!("output"),
            ",",
            role_sum!("reasoning"),
            " FROM usage_event e LEFT JOIN step_detail d ON d.event_key=e.event_key"
        )
    };
}

/// The newest dated steps of one session: `?1` session, `?2` producer,
/// `?3` limit.
const WINDOW_SQL: &str = concat!(
    "SELECT ",
    step_columns!(),
    " WHERE e.session=?1 AND e.producer=?2 AND e.occurred_at_ms IS NOT NULL",
    " ORDER BY e.occurred_at_ms DESC, e.event_key DESC LIMIT ?3"
);
/// Whether the session has a dated step before `(?3 time, ?4 key)`.
const EARLIER_SQL: &str = "SELECT EXISTS (SELECT 1 FROM usage_event e
     WHERE e.session=?1 AND e.producer=?2 AND e.occurred_at_ms IS NOT NULL
       AND (e.occurred_at_ms,e.event_key)<(?3,?4))";
/// The last dated step before `(?3 time, ?4 key)` with a plain token count
/// written by a cache-read measurement.
const BASELINE_SQL: &str = concat!(
    "SELECT ",
    step_columns!(),
    " WHERE e.session=?1 AND e.producer=?2 AND e.occurred_at_ms IS NOT NULL",
    " AND (e.occurred_at_ms,e.event_key)<(?3,?4)",
    " AND EXISTS (SELECT 1 FROM measurement m WHERE m.event_key=e.event_key",
    " AND m.role='cache_read' AND m.quantity_exponent=0)",
    " ORDER BY e.occurred_at_ms DESC, e.event_key DESC LIMIT 1"
);
const READS_CACHE_SQL: &str = "SELECT EXISTS (SELECT 1 FROM usage_event e
     JOIN measurement m ON m.event_key=e.event_key
     WHERE e.session=?1 AND e.producer=?2 AND e.occurred_at_ms IS NOT NULL
       AND m.role='cache_read' AND m.quantity_exponent=0 AND m.quantity_units>0)";
const UNDATED_SQL: &str = "SELECT COUNT(*) FROM usage_event
     WHERE session=?1 AND producer=?2 AND occurred_at_ms IS NULL";
/// `?1` since, `?2` one producer or NULL, `?3` limit. The unary `+` keeps
/// SQLite from walking a producer's whole history through the producer
/// index: the time range on `usage_event_summary_time` is the narrow one.
const ACTIVE_SESSIONS_SQL: &str = concat!(
    "SELECT producer,session,MAX(occurred_at_ms) FROM usage_event",
    " WHERE occurred_at_ms>=?1 AND +session IS NOT NULL AND +producer IN ",
    harnesses_sql!(),
    " AND (?2 IS NULL OR +producer=?2)",
    " GROUP BY producer,session ORDER BY 3 DESC, producer, session LIMIT ?3"
);
/// The `+` keeps the scan on the `event_key` range, as for
/// [`ACTIVE_SESSIONS_SQL`].
const RECORDED_AFTER_SQL: &str = concat!(
    "SELECT e.producer,e.session,",
    step_columns!(),
    " WHERE e.event_key>?1 AND +e.session IS NOT NULL AND +e.producer IN ",
    harnesses_sql!(),
    " ORDER BY e.event_key"
);
const BY_KEY_SQL: &str = concat!(
    "SELECT e.producer,e.session,",
    step_columns!(),
    " WHERE e.event_key=?1 AND e.session IS NOT NULL AND e.producer IN ",
    harnesses_sql!()
);
/// The dated steps of one session at or after `?3`, oldest first.
const SINCE_SQL: &str = concat!(
    "SELECT ",
    step_columns!(),
    " WHERE e.session=?1 AND e.producer=?2 AND e.occurred_at_ms>=?3",
    " ORDER BY e.occurred_at_ms, e.event_key"
);
const PROJECT_ROOT_SQL: &str = "SELECT project_root FROM usage_event
     WHERE session=?1 AND producer=?2 AND occurred_at_ms>=?3 AND project_root IS NOT NULL
     ORDER BY occurred_at_ms DESC, event_key DESC LIMIT 1";

fn count(row: &Row<'_>, column: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(column)?
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
        })
        .transpose()
}

/// Read one step from the columns of `step_columns!`, starting at `at`.
/// `occurred_at_ms` is `None` for an undated step.
fn step_parts(row: &Row<'_>, at: usize) -> rusqlite::Result<(Option<i64>, StepRow)> {
    let occurred_at_ms = row.get::<_, Option<i64>>(at + 2)?;
    Ok((
        occurred_at_ms,
        StepRow {
            cursor: row.get(at)?,
            step_ref: row.get(at + 1)?,
            occurred_at_ms: occurred_at_ms.unwrap_or(0),
            provider: row.get(at + 3)?,
            model: row.get(at + 4)?,
            finish: row.get(at + 5)?,
            context_limit: row.get(at + 6)?,
            input: count(row, at + 7)?,
            cache_read: count(row, at + 8)?,
            cache_write: count(row, at + 9)?,
            output: count(row, at + 10)?,
            reasoning: count(row, at + 11)?,
        },
    ))
}

fn step_row(row: &Row<'_>) -> rusqlite::Result<StepRow> {
    step_parts(row, 0).map(|(_, step)| step)
}

/// The newest `limit` dated steps of one session, oldest first, with the
/// lead of the first, whether the session ever read from its cache, and how
/// many of its steps are undated.
pub fn load_session(
    conn: &Connection,
    producer: &str,
    session: &str,
    limit: usize,
) -> rusqlite::Result<SessionSteps> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let mut statement = conn.prepare_cached(WINDOW_SQL)?;
    let mut rows = statement
        .query_map(params![session, producer, limit], step_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.reverse();
    let lead = match rows.first() {
        Some(first) => lead_before(conn, producer, session, first)?,
        None => Lead::default(),
    };
    Ok(SessionSteps {
        rows,
        lead,
        reads_cache: session_reads_cache(conn, producer, session)?,
        undated: undated_steps(conn, producer, session)?,
    })
}

/// The lead of `step`, read from the session's earlier dated steps.
fn lead_before(
    conn: &Connection,
    producer: &str,
    session: &str,
    step: &StepRow,
) -> rusqlite::Result<Lead> {
    let earlier_steps = conn.prepare_cached(EARLIER_SQL)?.query_row(
        params![session, producer, step.occurred_at_ms, step.cursor],
        |row| row.get::<_, bool>(0),
    )?;
    if !earlier_steps {
        return Ok(Lead::default());
    }
    let baseline = conn
        .prepare_cached(BASELINE_SQL)?
        .query_row(
            params![session, producer, step.occurred_at_ms, step.cursor],
            step_row,
        )
        .optional()?;
    Ok(Lead {
        earlier_steps,
        baseline: baseline.map(|step| step.counts()),
    })
}

/// Whether any dated step of the session read a positive number of cached
/// tokens.
fn session_reads_cache(conn: &Connection, producer: &str, session: &str) -> rusqlite::Result<bool> {
    conn.prepare_cached(READS_CACHE_SQL)?
        .query_row(params![session, producer], |row| row.get(0))
}

fn undated_steps(conn: &Connection, producer: &str, session: &str) -> rusqlite::Result<u64> {
    let undated: i64 = conn
        .prepare_cached(UNDATED_SQL)?
        .query_row(params![session, producer], |row| row.get(0))?;
    Ok(u64::try_from(undated).unwrap_or(0))
}

/// Sessions of the harness producers (or of one) with a dated step at or
/// after `since_ms`, most recently active first: `(producer, session, last
/// activity)`.
pub fn active_sessions(
    conn: &Connection,
    since_ms: i64,
    producer: Option<&str>,
    limit: usize,
) -> rusqlite::Result<Vec<(String, String, i64)>> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let mut statement = conn.prepare_cached(ACTIVE_SESSIONS_SQL)?;
    let sessions = statement
        .query_map(params![since_ms, producer, limit], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect();
    sessions
}

/// Every harness step recorded after `after_event_key`, in key order.
pub fn steps_recorded_after(
    conn: &Connection,
    after_event_key: i64,
) -> rusqlite::Result<Vec<RecordedStep>> {
    let mut statement = conn.prepare_cached(RECORDED_AFTER_SQL)?;
    let steps = statement
        .query_map([after_event_key], recorded_step)?
        .collect();
    steps
}

/// The harness steps with these event keys, in key order. Keys of other
/// producers' events are skipped.
pub fn steps_by_key(conn: &Connection, event_keys: &[i64]) -> rusqlite::Result<Vec<RecordedStep>> {
    let mut statement = conn.prepare_cached(BY_KEY_SQL)?;
    let mut steps = Vec::new();
    for key in event_keys {
        if let Some(step) = statement.query_row([key], recorded_step).optional()? {
            steps.push(step);
        }
    }
    Ok(steps)
}

fn recorded_step(row: &Row<'_>) -> rusqlite::Result<RecordedStep> {
    let (occurred_at_ms, step) = step_parts(row, 2)?;
    Ok(RecordedStep {
        producer: row.get(0)?,
        session: row.get(1)?,
        step: occurred_at_ms.map(|_| step),
    })
}

/// The largest `event_key` in the store, or 0 when it has no event.
pub fn last_event_key(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COALESCE(MAX(event_key),0) FROM usage_event",
        [],
        |row| row.get(0),
    )
}

/// Take the events that gained a `step_detail` row after they were
/// recorded, on this connection, since the last call. See
/// `ingest::note_late_step_detail`. Runs outside any transaction, after the
/// commit that wrote them.
pub fn take_late_step_details(conn: &Connection) -> rusqlite::Result<Vec<i64>> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM temp.sqlite_master WHERE name='step_detail_late')",
        [],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(Vec::new());
    }
    let keys = conn
        .prepare("SELECT event_key FROM temp.step_detail_late ORDER BY event_key")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<i64>>>()?;
    conn.execute("DELETE FROM temp.step_detail_late", [])?;
    Ok(keys)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheSessionsRequest {
    /// Steps at or after this time count; defaults to the last 24 hours.
    #[serde(default)]
    pub since_ms: Option<i64>,
    /// One harness producer; defaults to all of them.
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheStepsRequest {
    pub harness: String,
    pub session: String,
    #[serde(default)]
    pub limit: Option<u32>,
    /// Only steps of the window with a greater cursor are returned.
    #[serde(default)]
    pub after_cursor: Option<i64>,
}

/// A `usage.cache.steps` request that passed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepsQuery {
    pub producer: String,
    pub session: String,
    pub limit: usize,
    pub after_cursor: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheRefusal {
    /// The request decoded but a value is invalid (an empty session, a zero
    /// limit, a negative time).
    InvalidRequest(String),
    /// The harness is not one whose events are model steps.
    UnknownHarness(String),
    /// The limit or the time window is larger than the operation serves.
    RangeTooLarge(String),
}

impl CacheRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "invalid_request",
            Self::UnknownHarness(_) => "unknown_harness",
            Self::RangeTooLarge(_) => "range_too_large",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::InvalidRequest(message) | Self::RangeTooLarge(message) => message.clone(),
            Self::UnknownHarness(harness) => format!(
                "harness '{harness}' is not one of {}",
                CACHE_HARNESSES.join(", ")
            ),
        }
    }
}

#[derive(Debug)]
pub enum CacheError {
    Refused(CacheRefusal),
    Store(rusqlite::Error),
}

impl From<rusqlite::Error> for CacheError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

impl From<CacheRefusal> for CacheError {
    fn from(refusal: CacheRefusal) -> Self {
        Self::Refused(refusal)
    }
}

fn known_harness(harness: &str) -> Result<(), CacheRefusal> {
    if CACHE_HARNESSES.contains(&harness) {
        Ok(())
    } else {
        Err(CacheRefusal::UnknownHarness(harness.to_string()))
    }
}

fn limit(requested: Option<u32>, default: u32, max: u32) -> Result<usize, CacheRefusal> {
    let limit = requested.unwrap_or(default);
    if limit == 0 {
        return Err(CacheRefusal::InvalidRequest(
            "limit must be at least 1".into(),
        ));
    }
    if limit > max {
        return Err(CacheRefusal::RangeTooLarge(format!(
            "limit {limit} is above the maximum of {max}"
        )));
    }
    Ok(usize::try_from(limit).unwrap_or(usize::MAX))
}

impl CacheStepsRequest {
    pub fn validate(self) -> Result<StepsQuery, CacheRefusal> {
        known_harness(&self.harness)?;
        if self.session.is_empty() {
            return Err(CacheRefusal::InvalidRequest(
                "session must not be empty".into(),
            ));
        }
        Ok(StepsQuery {
            limit: limit(self.limit, STEPS_DEFAULT_LIMIT, STEPS_MAX_LIMIT)?,
            producer: self.harness,
            session: self.session,
            after_cursor: self.after_cursor,
        })
    }
}

/// One step of a `usage.cache.steps` reply. Absent counts and names are
/// omitted, never zero.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CacheStep {
    pub cursor: i64,
    pub step_ref: String,
    pub occurred_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<i64>,
    pub prompt: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<f64>,
    pub verdict: Verdict,
    pub prompt_dropped: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StepsReply {
    pub harness: String,
    pub session: String,
    pub steps: Vec<CacheStep>,
    /// Whether the session has dated steps older than the window.
    pub earlier_steps: bool,
    /// Steps without a time; they are not placed on the timeline.
    pub undated_steps: u64,
}

/// The reply for a window of a session's steps (oldest first) whose first
/// step has `lead`. Both the memory and the store path build their reply
/// here, from the same rows, so they cannot disagree.
pub fn steps_reply(
    query: &StepsQuery,
    window: Vec<StepRow>,
    lead: Lead,
    reads_cache: bool,
    undated_steps: u64,
) -> StepsReply {
    let counts = window.iter().map(StepRow::counts).collect::<Vec<_>>();
    let derived = derive(CacheModel::of(&query.producer), reads_cache, lead, &counts);
    let steps = window
        .into_iter()
        .zip(derived)
        .filter(|(row, _)| query.after_cursor.map_or(true, |after| row.cursor > after))
        .map(|(row, derived)| CacheStep {
            cursor: row.cursor,
            step_ref: row.step_ref,
            occurred_at_ms: row.occurred_at_ms,
            input: row.input,
            cache_read: row.cache_read,
            cache_write: row.cache_write,
            output: row.output,
            reasoning: row.reasoning,
            provider: row.provider,
            model: row.model,
            finish: row.finish,
            context_limit: row.context_limit,
            prompt: derived.prompt,
            retention: derived.retention,
            verdict: derived.verdict,
            prompt_dropped: derived.prompt_dropped,
        })
        .collect();
    StepsReply {
        harness: query.producer.clone(),
        session: query.session.clone(),
        steps,
        earlier_steps: lead.earlier_steps,
        undated_steps,
    }
}

/// `usage.cache.steps` answered from the store.
pub fn steps_from_store(conn: &Connection, query: &StepsQuery) -> rusqlite::Result<StepsReply> {
    let loaded = load_session(conn, &query.producer, &query.session, query.limit)?;
    Ok(steps_reply(
        query,
        loaded.rows,
        loaded.lead,
        loaded.reads_cache,
        loaded.undated,
    ))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CacheSession {
    pub harness: String,
    pub session: String,
    /// The project root of the newest step in the window that names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_root: Option<String>,
    /// Dated steps in the window.
    pub steps: u64,
    /// Cached tokens over all prompt tokens (read, write and uncached input)
    /// of the window's steps that report their cache; omitted when none do.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hit_ratio: Option<f64>,
    /// Window steps whose verdict is `bust` or `full_bust`.
    pub busts: u64,
    pub last_activity_ms: i64,
    /// The newest positive context limit among the window's steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionsReply {
    pub since_ms: i64,
    pub sessions: Vec<CacheSession>,
}

/// `usage.cache.sessions`: sessions with a step at or after `since_ms`,
/// most recently active first.
pub fn cache_sessions(
    conn: &Connection,
    request: CacheSessionsRequest,
    now_ms: i64,
) -> Result<SessionsReply, CacheError> {
    if let Some(harness) = &request.harness {
        known_harness(harness)?;
    }
    let limit = limit(request.limit, SESSIONS_DEFAULT_LIMIT, SESSIONS_MAX_LIMIT)?;
    let since_ms = request
        .since_ms
        .unwrap_or_else(|| now_ms.saturating_sub(RECENT_WINDOW_MS));
    if since_ms < 0 {
        return Err(CacheRefusal::InvalidRequest("since_ms must not be negative".into()).into());
    }
    if since_ms < now_ms.saturating_sub(SESSIONS_MAX_WINDOW_MS) {
        return Err(CacheRefusal::RangeTooLarge(format!(
            "since_ms reaches more than {} days back",
            SESSIONS_MAX_WINDOW_MS / RECENT_WINDOW_MS
        ))
        .into());
    }
    let mut sessions = Vec::new();
    for (producer, session, last_activity_ms) in
        active_sessions(conn, since_ms, request.harness.as_deref(), limit)?
    {
        sessions.push(session_summary(
            conn,
            producer,
            session,
            since_ms,
            last_activity_ms,
        )?);
    }
    Ok(SessionsReply { since_ms, sessions })
}

fn session_summary(
    conn: &Connection,
    producer: String,
    session: String,
    since_ms: i64,
    last_activity_ms: i64,
) -> rusqlite::Result<CacheSession> {
    let rows = conn
        .prepare_cached(SINCE_SQL)?
        .query_map(params![session, producer, since_ms], step_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let lead = match rows.first() {
        Some(first) => lead_before(conn, &producer, &session, first)?,
        None => Lead::default(),
    };
    let counts = rows.iter().map(StepRow::counts).collect::<Vec<_>>();
    let derived = derive(
        CacheModel::of(&producer),
        session_reads_cache(conn, &producer, &session)?,
        lead,
        &counts,
    );
    let busts = derived
        .iter()
        .filter(|derived| matches!(derived.verdict, Verdict::Bust | Verdict::FullBust))
        .count();
    let (read, prompt) = counts.iter().filter(|counts| counts.reports_cache()).fold(
        (0_u64, 0_u64),
        |(read, prompt), counts| {
            (
                read.saturating_add(counts.cache_read.unwrap_or(0)),
                prompt.saturating_add(counts.prompt()),
            )
        },
    );
    #[allow(clippy::cast_precision_loss)] // A ratio of token counts; rounding is harmless.
    let hit_ratio = (prompt > 0).then(|| read as f64 / prompt as f64);
    let project_root = conn
        .prepare_cached(PROJECT_ROOT_SQL)?
        .query_row(params![session, producer, since_ms], |row| row.get(0))
        .optional()?;
    Ok(CacheSession {
        project_root,
        steps: u64::try_from(rows.len()).unwrap_or(u64::MAX),
        hit_ratio,
        busts: u64::try_from(busts).unwrap_or(u64::MAX),
        last_activity_ms,
        context_limit: rows
            .iter()
            .rev()
            .find_map(|row| row.context_limit.filter(|limit| *limit > 0)),
        harness: producer,
        session,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fact::ChargeBasis;
    use crate::ingest::{
        record_event_in_tx, DeliveryPath, EventRecordRequest, MeasurementInput, StepDetail,
    };
    use crate::store::{initialize_connection, MIGRATIONS};

    fn counts(input: Option<u64>, read: Option<u64>, write: Option<u64>) -> StepCounts {
        StepCounts {
            input,
            cache_read: read,
            cache_write: write,
        }
    }

    /// `(verdict, retention to four places, prompt_dropped)` of each step.
    fn run(
        model: CacheModel,
        reads_cache: bool,
        steps: &[StepCounts],
    ) -> Vec<(Verdict, Option<String>, bool)> {
        derive(model, reads_cache, Lead::default(), steps)
            .into_iter()
            .map(|derived| {
                (
                    derived.verdict,
                    derived.retention.map(|retention| format!("{retention:.4}")),
                    derived.prompt_dropped,
                )
            })
            .collect()
    }

    /// The verdict of the second step after `previous`, in a session that
    /// reads its cache.
    fn after(previous: StepCounts, current: StepCounts) -> (Verdict, Option<String>, bool) {
        run(CacheModel::Explicit, true, &[previous, current])[1].clone()
    }

    fn r(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    /// Every rule of the dashboard's prefix-cache verdict, at its edges.
    #[test]
    fn prefix_cache_verdicts_cover_every_band_and_edge() {
        use Verdict::*;
        // Expected read after a step with read 1,000 and no write or input:
        // exactly 1,000.
        let warm = counts(Some(0), Some(1_000), Some(0));
        let table = [
            (
                counts(Some(0), Some(950), None),
                (Stable, r("0.9500"), false),
            ),
            (
                counts(Some(0), Some(949), None),
                (Warning, r("0.9490"), false),
            ),
            (
                counts(Some(0), Some(800), None),
                (Warning, r("0.8000"), false),
            ),
            (counts(Some(0), Some(799), None), (Bust, r("0.7990"), false)),
            (
                counts(Some(5), Some(0), None),
                (FullBust, r("0.0000"), false),
            ),
            (
                counts(Some(0), Some(1_100), None),
                (Stable, r("1.1000"), false),
            ),
            (counts(Some(3), None, Some(9)), (Unknown, None, false)),
        ];
        for (current, expected) in table {
            assert_eq!(after(warm, current), expected, "{current:?}");
        }

        // Growth is the previous cache write when it is positive...
        let wrote = counts(Some(500), Some(1_000), Some(200));
        assert_eq!(after(wrote, counts(Some(0), Some(1_200), None)).0, Stable);
        assert_eq!(
            after(wrote, counts(Some(0), Some(1_000), None)),
            (Warning, r("0.8333"), false)
        );
        // ...and otherwise the previous uncached input.
        for write in [Some(0), None] {
            let uncached = counts(Some(500), Some(1_000), write);
            assert_eq!(
                after(uncached, counts(Some(0), Some(1_000), None)),
                (Bust, r("0.6667"), false)
            );
        }

        // A previous read of zero is no baseline: stable, without a ratio.
        assert_eq!(
            after(
                counts(Some(9), Some(0), Some(50)),
                counts(Some(0), Some(0), None)
            ),
            (Stable, None, false)
        );
        // The first step of the session is a cold start.
        assert_eq!(
            run(CacheModel::Explicit, true, &[counts(None, Some(10), None)]),
            [(ColdStart, None, false)]
        );
        // A step without a cache report is unknown and is no baseline: the
        // step after it is compared with the last step that reported.
        assert_eq!(
            run(
                CacheModel::Explicit,
                true,
                &[
                    counts(Some(1), Some(1_000), Some(0)),
                    counts(Some(1), None, None),
                    counts(Some(1), Some(0), None),
                ]
            ),
            [
                (ColdStart, None, false),
                (Unknown, None, false),
                (FullBust, r("0.0000"), false)
            ]
        );
        // Only unknown steps before: no baseline, so stable.
        assert_eq!(
            run(
                CacheModel::Explicit,
                true,
                &[counts(Some(1), None, None), counts(Some(1), Some(7), None)]
            ),
            [(Unknown, None, false), (Stable, None, false)]
        );
        // A session that never read from its cache does not expose it.
        assert_eq!(
            run(
                CacheModel::Explicit,
                false,
                &[
                    counts(Some(9), Some(0), None),
                    counts(Some(9), Some(0), None)
                ]
            ),
            [(Unknown, None, false), (Unknown, None, false)]
        );
    }

    /// A drop is a prompt at least 15,000 tokens smaller than the previous
    /// cache-reporting step's, whatever the step's own verdict.
    #[test]
    fn a_prompt_drop_is_at_least_fifteen_thousand_tokens() {
        let previous = counts(Some(5_000), Some(15_000), Some(0));
        assert!(
            after(previous, counts(Some(5_000), Some(0), None)).2,
            "exactly 15,000"
        );
        assert!(
            !after(previous, counts(Some(5_001), Some(0), None)).2,
            "14,999"
        );
        assert!(
            after(previous, counts(Some(4_000), None, None)).2,
            "an unknown step drops too"
        );
        assert!(
            !run(
                CacheModel::Explicit,
                true,
                &[counts(Some(1), Some(1), None)]
            )[0]
            .2,
            "the first step has nothing to drop from"
        );
    }

    /// Codex: retention is the cached share of the step's own prompt.
    #[test]
    fn implicit_cache_verdicts_use_the_steps_own_prompt() {
        use Verdict::*;
        let steps = [
            counts(Some(100), Some(0), None),
            counts(Some(5), Some(95), None),
            counts(Some(20), Some(80), None),
            counts(Some(21), Some(79), None),
            counts(Some(10), Some(0), None),
            counts(Some(0), Some(0), None),
            counts(Some(10), None, None),
            counts(None, Some(10), None),
        ];
        // A session that never read from its cache is still judged.
        assert_eq!(
            run(CacheModel::Implicit, false, &steps),
            [
                (ColdStart, None, false),
                (Stable, r("0.9500"), false),
                (Warning, r("0.8000"), false),
                (Bust, r("0.7900"), false),
                (FullBust, r("0.0000"), false),
                (Unknown, None, false),
                (Unknown, None, false),
                (Stable, r("1.0000"), false),
            ]
        );
    }

    /// A long session with every kind of step.
    fn varied_steps(count: u64) -> Vec<StepCounts> {
        (0..count)
            .map(|index| {
                let read = match index % 11 {
                    3 => None,
                    7 => Some(0),
                    _ => Some(20_000 + (index * 7_919) % 40_000),
                };
                counts(
                    (index % 13 != 5).then_some((index * 37) % 900),
                    read,
                    Some(if index % 4 == 0 { 300 } else { 0 }),
                )
            })
            .collect()
    }

    /// The values of a step do not depend on where a window starts: the last
    /// 500 of 1,000 steps derive the same alone (with their lead) as within
    /// all 1,000.
    #[test]
    fn derived_values_do_not_depend_on_the_window() {
        let steps = varied_steps(1_000);
        for model in [CacheModel::Explicit, CacheModel::Implicit] {
            let whole = derive(model, true, Lead::default(), &steps);
            let mut lead = Lead::default();
            for step in &steps[..500] {
                lead.advance(step);
            }
            assert_eq!(derive(model, true, lead, &steps[500..]), whole[500..]);
            let verdicts = whole
                .iter()
                .map(|d| d.verdict)
                .collect::<std::collections::BTreeSet<_>>();
            assert!(
                verdicts.len() >= 4,
                "the fixture exercises several verdicts: {verdicts:?}"
            );
        }
    }

    #[test]
    fn the_sql_harness_list_is_the_harness_list() {
        let listed = format!("('{}')", CACHE_HARNESSES.join("','"));
        assert_eq!(listed, harnesses_sql!());
    }

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn token(role: &str, unit: &str, units: Option<u64>) -> MeasurementInput {
        let units = units.map(|units| i64::try_from(units).unwrap());
        MeasurementInput {
            role: role.into(),
            unit: unit.into(),
            kind: "billed".into(),
            source: format!("usage.{role}.{unit}"),
            quantity_units: units,
            quantity_exponent: units.map(|_| 0),
        }
    }

    struct Step {
        producer: &'static str,
        session: &'static str,
        step_ref: String,
        at: Option<i64>,
        counts: StepCounts,
        detail: StepDetail,
        project_root: Option<&'static str>,
    }

    fn step(
        producer: &'static str,
        session: &'static str,
        index: u64,
        at: Option<i64>,
        counts: StepCounts,
    ) -> Step {
        Step {
            producer,
            session,
            step_ref: format!("{session}:{index}"),
            at,
            counts,
            detail: StepDetail::default(),
            project_root: None,
        }
    }

    fn record(conn: &mut Connection, steps: &[Step]) {
        let tx = conn.transaction().unwrap();
        for step in steps {
            let mut measurements = vec![token("output", "token", Some(7))];
            measurements.extend(step.counts.input.map(|n| token("input", "token", Some(n))));
            measurements.extend(
                step.counts
                    .cache_read
                    .map(|n| token("cache_read", "token", Some(n))),
            );
            if let Some(write) = step.counts.cache_write {
                // Claude Code's split write: both lifetimes are summed.
                measurements.push(token("cache_write", "token_cache_5m", Some(write / 2)));
                measurements.push(token(
                    "cache_write",
                    "token_cache_1h",
                    Some(write - write / 2),
                ));
            }
            let request = EventRecordRequest {
                producer: step.producer.into(),
                producer_lineage: None,
                producer_ref: step.step_ref.clone(),
                prior_event_ref: None,
                harness: None,
                project_root: step.project_root.map(str::to_string),
                session: Some(step.session.into()),
                run_ref: None,
                flow_ref: None,
                work_class: None,
                occurred_at_ms: step.at,
                emitted_at_ms: None,
                terminal_status: None,
                attempt_ordinal: None,
                vendor_request_id: None,
                provider: Some("anthropic".into()),
                model: Some("claude-sonnet-4".into()),
                account_ref: None,
                auth_selection: None,
                charge_basis: ChargeBasis::Unknown,
                charge_basis_mapping_version: None,
                charge_basis_overrides: None,
                measurements,
                step_detail: step.detail.clone(),
            };
            record_event_in_tx(&tx, &request, 1, DeliveryPath::Read).unwrap();
        }
        tx.commit().unwrap();
    }

    fn query(producer: &str, session: &str, limit: usize, after_cursor: Option<i64>) -> StepsQuery {
        StepsQuery {
            producer: producer.into(),
            session: session.into(),
            limit,
            after_cursor,
        }
    }

    /// A 1,000-step session among other sessions and producers.
    fn long_session_store() -> Connection {
        let mut conn = conn();
        let steps = varied_steps(1_000)
            .into_iter()
            .enumerate()
            .flat_map(|(index, counts)| {
                let index = u64::try_from(index).unwrap();
                let at = i64::try_from(index).unwrap() * 1_000;
                [
                    step("claude-code", "long", index, Some(at), counts),
                    step("claude-code", "other", index, Some(at), counts),
                    step("opencode", "long", index, Some(at + 1), counts),
                ]
            })
            .collect::<Vec<_>>();
        record(&mut conn, &steps);
        conn
    }

    /// Asked for 500 or for 1,000 steps, the store gives the newest 500 the
    /// same values: the lead of a window comes from the whole session.
    #[test]
    fn store_steps_are_the_same_whatever_the_window() {
        let conn = long_session_store();
        let whole = steps_from_store(&conn, &query("claude-code", "long", 1_000, None)).unwrap();
        let newest = steps_from_store(&conn, &query("claude-code", "long", 500, None)).unwrap();
        assert_eq!(whole.steps.len(), 1_000);
        assert!(!whole.earlier_steps);
        assert!(newest.earlier_steps);
        assert_eq!(newest.steps, whole.steps[500..]);
        assert_eq!(whole.steps[0].verdict, Verdict::ColdStart);
        assert!(whole.steps.iter().all(|s| s.step_ref.starts_with("long:")));
        // Oldest first, in time order.
        assert!(whole
            .steps
            .windows(2)
            .all(|w| w[0].occurred_at_ms < w[1].occurred_at_ms));
    }

    /// A step's row carries the five counts, the summed split cache write,
    /// the finish (detail first, then terminal status) and the context limit.
    #[test]
    fn a_stored_step_reads_back_with_its_detail() {
        let mut conn = conn();
        let mut first = step("pi", "s", 0, Some(10), counts(Some(3), Some(5), Some(7)));
        first.detail = StepDetail {
            finish: Some("toolUse".into()),
            context_limit: Some(200_000),
        };
        let second = step("pi", "s", 1, Some(20), counts(None, None, None));
        let undated = step("pi", "s", 2, None, counts(Some(1), Some(1), None));
        record(&mut conn, &[first, second, undated]);
        let reply = steps_from_store(&conn, &query("pi", "s", 10, None)).unwrap();
        let json = serde_json::to_value(&reply).unwrap();
        assert_eq!(json["undated_steps"], 1);
        assert_eq!(
            json["steps"][0],
            serde_json::json!({
                "cursor": reply.steps[0].cursor, "step_ref": "s:0", "occurred_at_ms": 10,
                "input": 3, "cache_read": 5, "cache_write": 7, "output": 7,
                "provider": "anthropic", "model": "claude-sonnet-4",
                "finish": "toolUse", "context_limit": 200_000,
                "prompt": 15, "verdict": "cold_start", "prompt_dropped": false,
            })
        );
        // Absent counts are omitted, never zero.
        assert_eq!(
            json["steps"][1],
            serde_json::json!({
                "cursor": reply.steps[1].cursor, "step_ref": "s:1", "occurred_at_ms": 20,
                "output": 7, "provider": "anthropic", "model": "claude-sonnet-4",
                "prompt": 0, "verdict": "unknown", "prompt_dropped": false,
            })
        );

        // A terminal status (Claude Code's stop reason) is the finish when
        // there is no detail.
        let tx = conn.transaction().unwrap();
        record_event_in_tx(
            &tx,
            &EventRecordRequest {
                terminal_status: Some("end_turn".into()),
                ..cc_request("cc-1")
            },
            1,
            DeliveryPath::Read,
        )
        .unwrap();
        tx.commit().unwrap();
        let reply = steps_from_store(&conn, &query("claude-code", "cc", 10, None)).unwrap();
        assert_eq!(reply.steps[0].finish.as_deref(), Some("end_turn"));
    }

    fn cc_request(producer_ref: &str) -> EventRecordRequest {
        EventRecordRequest {
            producer: "claude-code".into(),
            producer_lineage: None,
            producer_ref: producer_ref.into(),
            prior_event_ref: None,
            harness: None,
            project_root: None,
            session: Some("cc".into()),
            run_ref: None,
            flow_ref: None,
            work_class: None,
            occurred_at_ms: Some(5),
            emitted_at_ms: None,
            terminal_status: None,
            attempt_ordinal: None,
            vendor_request_id: None,
            provider: None,
            model: None,
            account_ref: None,
            auth_selection: None,
            charge_basis: ChargeBasis::Unknown,
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements: vec![],
            step_detail: StepDetail::default(),
        }
    }

    /// `after_cursor` keeps the window's steps with a greater cursor; their
    /// values are still those of the whole window.
    #[test]
    fn after_cursor_filters_the_window() {
        let conn = long_session_store();
        let window = steps_from_store(&conn, &query("claude-code", "long", 10, None)).unwrap();
        let cursor = window.steps[6].cursor;
        let after =
            steps_from_store(&conn, &query("claude-code", "long", 10, Some(cursor))).unwrap();
        assert_eq!(after.steps, window.steps[7..]);
        let none = steps_from_store(
            &conn,
            &query("claude-code", "long", 10, Some(window.steps[9].cursor)),
        )
        .unwrap();
        assert!(none.steps.is_empty());
    }

    fn plan(conn: &Connection, sql: &str) -> String {
        let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let values = [
            rusqlite::types::Value::Text("long".into()),
            rusqlite::types::Value::Text("claude-code".into()),
            rusqlite::types::Value::Integer(200),
        ];
        let count = statement.parameter_count();
        statement
            .query_map(rusqlite::params_from_iter(&values[..count]), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The steps query walks one session's index entries newest first, with
    /// no sort, and its cost does not grow with the rest of the store.
    #[test]
    fn a_session_window_reads_its_own_index_range() {
        let window_sql = WINDOW_SQL;
        let conn = long_session_store();
        // The session list walks the recent time range and the memory's
        // catch-up the new keys, never a producer's whole history.
        let active_plan = plan(&conn, ACTIVE_SESSIONS_SQL);
        assert!(
            active_plan.contains("USING INDEX usage_event_summary_time"),
            "{active_plan}"
        );
        let recorded_plan = plan(&conn, RECORDED_AFTER_SQL);
        assert!(
            recorded_plan.contains("USING INTEGER PRIMARY KEY (rowid>?)"),
            "{recorded_plan}"
        );
        assert!(!recorded_plan.contains("TEMP B-TREE"), "{recorded_plan}");
        let window_plan = plan(&conn, window_sql);
        assert!(
            window_plan.contains("USING INDEX usage_event_session_time"),
            "{window_plan}"
        );
        assert!(!window_plan.contains("TEMP B-TREE"), "{window_plan}");

        let steps_of = |conn: &Connection| {
            let mut statement = conn.prepare(window_sql).unwrap();
            let rows = statement
                .query_map(params!["long", "claude-code", 200], |_| Ok(()))
                .unwrap()
                .count();
            assert_eq!(rows, 200);
            statement.get_status(rusqlite::StatementStatus::VmStep)
        };
        let before = steps_of(&conn);
        let mut conn = conn;
        let noise = (0..2_000_u64)
            .map(|index| {
                step(
                    "claude-code",
                    "noise",
                    index,
                    Some(500_000),
                    counts(Some(1), Some(1), None),
                )
            })
            .collect::<Vec<_>>();
        record(&mut conn, &noise);
        assert_eq!(
            steps_of(&conn),
            before,
            "another session's steps cost nothing"
        );
    }

    #[test]
    fn sessions_report_hit_ratio_busts_limit_and_activity() {
        let mut conn = conn();
        let mut steps = vec![
            // cold start, then a bust (500 of an expected 1,000+100).
            step(
                "claude-code",
                "a",
                0,
                Some(1_000),
                counts(Some(100), Some(1_000), Some(0)),
            ),
            step(
                "claude-code",
                "a",
                1,
                Some(2_000),
                counts(Some(100), Some(500), Some(0)),
            ),
            // Unknown: no cache report; not in the hit ratio.
            step(
                "claude-code",
                "a",
                2,
                Some(3_000),
                counts(Some(9_999), None, None),
            ),
            step(
                "codex",
                "b",
                0,
                Some(1_500),
                counts(Some(10), Some(90), None),
            ),
        ];
        steps[0].project_root = Some("/old");
        steps[1].project_root = Some("/work");
        steps[1].detail.context_limit = Some(200_000);
        steps[2].detail.context_limit = Some(0);
        record(&mut conn, &steps);
        let reply = cache_sessions(
            &conn,
            CacheSessionsRequest {
                since_ms: Some(0),
                harness: None,
                limit: None,
            },
            1_000,
        )
        .unwrap();
        let json = serde_json::to_value(&reply).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"since_ms": 0, "sessions": [
                {"harness": "claude-code", "session": "a", "project_root": "/work", "steps": 3,
                 "hit_ratio": 1_500.0 / 1_700.0, "busts": 1, "last_activity_ms": 3_000,
                 "context_limit": 200_000},
                {"harness": "codex", "session": "b", "steps": 1, "hit_ratio": 0.9, "busts": 0,
                 "last_activity_ms": 1_500},
            ]})
        );
        // The window starts at since_ms, but verdicts still see the steps
        // before it: the bust stays a bust.
        let windowed = cache_sessions(
            &conn,
            CacheSessionsRequest {
                since_ms: Some(1_600),
                harness: Some("claude-code".into()),
                limit: Some(1),
            },
            1_600,
        )
        .unwrap();
        assert_eq!(windowed.sessions.len(), 1);
        assert_eq!(windowed.sessions[0].steps, 2);
        assert_eq!(windowed.sessions[0].busts, 1);
    }

    #[test]
    fn sessions_and_steps_refuse_bad_values_with_typed_codes() {
        let conn = conn();
        let now = 10 * RECENT_WINDOW_MS;
        let refuse = |request: CacheSessionsRequest| match cache_sessions(&conn, request, now) {
            Err(CacheError::Refused(refusal)) => refusal.code(),
            other => panic!("{other:?}"),
        };
        let base = CacheSessionsRequest {
            since_ms: None,
            harness: None,
            limit: None,
        };
        assert_eq!(
            refuse(CacheSessionsRequest {
                harness: Some("goose".into()),
                ..base.clone()
            }),
            "unknown_harness"
        );
        assert_eq!(
            refuse(CacheSessionsRequest {
                limit: Some(0),
                ..base.clone()
            }),
            "invalid_request"
        );
        assert_eq!(
            refuse(CacheSessionsRequest {
                limit: Some(SESSIONS_MAX_LIMIT + 1),
                ..base.clone()
            }),
            "range_too_large"
        );
        assert_eq!(
            refuse(CacheSessionsRequest {
                since_ms: Some(-1),
                ..base.clone()
            }),
            "invalid_request"
        );
        assert_eq!(
            refuse(CacheSessionsRequest {
                since_ms: Some(now - SESSIONS_MAX_WINDOW_MS - 1),
                ..base.clone()
            }),
            "range_too_large"
        );
        assert!(cache_sessions(
            &conn,
            CacheSessionsRequest {
                since_ms: Some(now - SESSIONS_MAX_WINDOW_MS),
                ..base
            },
            now
        )
        .is_ok());

        let steps = |harness: &str, session: &str, limit: Option<u32>| {
            CacheStepsRequest {
                harness: harness.into(),
                session: session.into(),
                limit,
                after_cursor: None,
            }
            .validate()
            .map(|query| query.limit)
            .map_err(|refusal| refusal.code())
        };
        assert_eq!(steps("broca", "s", None), Err("unknown_harness"));
        assert_eq!(steps("pi", "", None), Err("invalid_request"));
        assert_eq!(steps("pi", "s", Some(0)), Err("invalid_request"));
        assert_eq!(
            steps("pi", "s", Some(STEPS_MAX_LIMIT + 1)),
            Err("range_too_large")
        );
        assert_eq!(steps("pi", "s", None), Ok(200));
        assert_eq!(steps("pi", "s", Some(STEPS_MAX_LIMIT)), Ok(1_000));
    }

    /// Every statement the cache-diagnostics reads prepare.
    const ALL_SQL: [&str; 10] = [
        WINDOW_SQL,
        EARLIER_SQL,
        BASELINE_SQL,
        READS_CACHE_SQL,
        UNDATED_SQL,
        SINCE_SQL,
        PROJECT_ROOT_SQL,
        ACTIVE_SESSIONS_SQL,
        RECORDED_AFTER_SQL,
        BY_KEY_SQL,
    ];

    /// VM steps SQLite ran in the cache-diagnostics statements while `work`
    /// ran on `conn`. The reads prepare through the connection's statement
    /// cache, so the cached statements' counters cover all of their runs.
    fn vm_steps<T>(conn: &Connection, work: impl FnOnce() -> T) -> (T, u64) {
        use rusqlite::StatementStatus::VmStep;
        for sql in ALL_SQL {
            conn.prepare_cached(sql).unwrap().reset_status(VmStep);
        }
        let result = work();
        let steps = ALL_SQL
            .iter()
            .map(|sql| u64::try_from(conn.prepare_cached(sql).unwrap().get_status(VmStep)).unwrap())
            .sum();
        (result, steps)
    }

    /// Builds a store of 2,050,000 harness events with four measurements
    /// each (10,000 sessions of 200 steps over about four weeks, plus one
    /// 50,000-step session), applies migration 27 to it and prints the
    /// migration time, the query plans and the VM steps of the
    /// cache-diagnostics reads. Run on the build server:
    /// `cargo test --workspace --lib -- --ignored --nocapture measure_cache_reads`.
    #[test]
    #[ignore = "builds a 2,050,000-event store; run by hand to measure"]
    fn measure_cache_reads_on_a_two_million_event_store() {
        use std::time::Instant;
        let path = std::env::temp_dir().join(format!("cache-measure-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..26] {
            conn.execute_batch(migration.statements).unwrap();
        }
        // 2026-10-01T00:00:00Z; sessions start four minutes apart, steps
        // are 30 seconds apart, the long session's 5 seconds apart.
        let base: i64 = 1_790_812_800_000;
        let started = Instant::now();
        conn.execute_batch(&format!(
            "BEGIN;
             WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<1999999)
             INSERT INTO usage_event (event_id,producer,producer_ref,session,occurred_at_ms,
                                      recorded_at_ms,charge_basis,provider,model)
             SELECT 'e'||i,
                    CASE (i/200)%3 WHEN 0 THEN 'opencode' WHEN 1 THEN 'pi' ELSE 'claude-code' END,
                    'r'||i,'s'||(i/200),{base}+(i/200)*240000+(i%200)*30000,1,'unknown',
                    'anthropic','claude-sonnet-4'
             FROM n;
             WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<49999)
             INSERT INTO usage_event (event_id,producer,producer_ref,session,occurred_at_ms,
                                      recorded_at_ms,charge_basis,provider,model)
             SELECT 'big'||i,'opencode','big'||i,'big',{base}+2400000000-250000000+i*5000,1,
                    'unknown','anthropic','claude-sonnet-4'
             FROM n;
             INSERT INTO measurement SELECT event_key,'input','token','billed','x',event_key%900,0 FROM usage_event;
             INSERT INTO measurement SELECT event_key,'cache_read','token','billed','x',20000+event_key%40000,0 FROM usage_event;
             INSERT INTO measurement SELECT event_key,'cache_write','token','billed','x',event_key%500,0 FROM usage_event;
             INSERT INTO measurement SELECT event_key,'output','token','billed','x',event_key%300,0 FROM usage_event;
             COMMIT;"
        ))
        .unwrap();
        println!(
            "built 2,050,000 events and 8,200,000 measurements in {:?}",
            started.elapsed()
        );

        let started = Instant::now();
        conn.execute_batch(MIGRATIONS[26].statements).unwrap();
        println!(
            "migration 27 (step_detail, usage_event_session_time) took {:?}",
            started.elapsed()
        );

        let explain = |label: &str, sql: &str| {
            let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            let count = statement.parameter_count();
            let values: Vec<rusqlite::types::Value> = (0..count)
                .map(|index| match index {
                    0 => rusqlite::types::Value::Text("big".into()),
                    1 => rusqlite::types::Value::Text("opencode".into()),
                    _ => rusqlite::types::Value::Integer(base),
                })
                .collect();
            let plan = statement
                .query_map(rusqlite::params_from_iter(values), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            println!("plan {label}:\n  {}", plan.join("\n  "));
        };
        for (label, sql) in [
            ("steps window", WINDOW_SQL),
            ("earlier steps", EARLIER_SQL),
            ("lead baseline", BASELINE_SQL),
            ("session reads cache", READS_CACHE_SQL),
            ("undated count", UNDATED_SQL),
            ("sessions window", SINCE_SQL),
            ("project root", PROJECT_ROOT_SQL),
            ("active sessions", ACTIVE_SESSIONS_SQL),
            ("steps recorded after", RECORDED_AFTER_SQL),
            ("step by key", BY_KEY_SQL),
        ] {
            explain(label, sql);
        }

        let now = base + 2_400_000_000;
        for (label, producer, session, limit) in [
            ("typical session, 200 steps", "pi", "s9997", 200),
            ("long session, 200 steps", "opencode", "big", 200),
            ("long session, 1000 steps", "opencode", "big", 1_000),
        ] {
            let started = Instant::now();
            let (reply, steps) = vm_steps(&conn, || {
                steps_from_store(&conn, &query(producer, session, limit, None)).unwrap()
            });
            println!(
                "usage.cache.steps {label}: {} steps returned, {steps} VM steps, {:?}",
                reply.steps.len(),
                started.elapsed()
            );
        }
        let started = Instant::now();
        let (reply, steps) = vm_steps(&conn, || {
            cache_sessions(
                &conn,
                CacheSessionsRequest {
                    since_ms: None,
                    harness: None,
                    limit: None,
                },
                now,
            )
            .unwrap()
        });
        println!(
            "usage.cache.sessions last 24h, limit 50: {} sessions, {steps} VM steps, {:?}",
            reply.sessions.len(),
            started.elapsed()
        );
        let started = Instant::now();
        let (sessions, steps) = vm_steps(&conn, || {
            active_sessions(&conn, now - RECENT_WINDOW_MS, None, 200).unwrap()
        });
        println!(
            "memory rebuild session list: {} sessions, {steps} VM steps, {:?}",
            sessions.len(),
            started.elapsed()
        );
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
