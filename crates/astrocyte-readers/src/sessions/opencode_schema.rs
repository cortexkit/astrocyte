// Copied from tokscale v4.17.0,
// crates/tokscale-core/src/sessions/opencode_schema.rs at
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
// ASTROCYTE DIVERGENCE: this is the OpenCode `message`-table subset only. The
// shared MiCode/Kilo variants, legacy JSON path, fingerprint merge cache, cost
// attribution, and session metadata joins are intentionally omitted.

//! SQLite driver for OpenCode's current `message` schema.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use rusqlite::{params, Connection, ErrorCode, Statement, StatementStatus};
use serde::Deserialize;

use super::opencode::{OpenCodeMessage, OpenCodeTotals};
use crate::TokenBreakdown;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeCursor {
    pub lineage: String,
    pub ordinal: i64,
}

/// How much SQLite work one read did, summed over every statement it ran.
/// These are SQLite's own per-statement counters, so they are deterministic
/// for a given database and do not depend on the machine's speed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenCodeScanCost {
    /// Virtual-machine instructions executed (SQLITE_STMTSTATUS_VM_STEP).
    pub vm_steps: u64,
    /// Forward steps taken by full table or index scans
    /// (SQLITE_STMTSTATUS_FULLSCAN_STEP). A query that walks the whole
    /// `message` table adds roughly one per row here.
    pub fullscan_steps: u64,
}

impl OpenCodeScanCost {
    pub(super) fn add_statement(&mut self, statement: &Statement<'_>) {
        let read = |status| u64::try_from(statement.reset_status(status)).unwrap_or(0);
        self.vm_steps = self.vm_steps.saturating_add(read(StatementStatus::VmStep));
        self.fullscan_steps = self
            .fullscan_steps
            .saturating_add(read(StatementStatus::FullscanStep));
    }

    pub fn add_assign_saturating(&mut self, other: &Self) {
        self.vm_steps = self.vm_steps.saturating_add(other.vm_steps);
        self.fullscan_steps = self.fullscan_steps.saturating_add(other.fullscan_steps);
    }
}

/// Run a single-row query and add its statement's SQLite work to `cost`.
pub(super) fn counted_query_row<T, P: rusqlite::Params>(
    connection: &Connection,
    cost: &mut OpenCodeScanCost,
    sql: &str,
    params: P,
    map: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let mut statement = crate::read_io::prepare_cached(connection, sql)?;
    let result = statement.query_row(params, map);
    cost.add_statement(&statement);
    result
}

/// SQL for an assistant message row. Only these can carry usage.
const ASSISTANT_ROW: &str = "json_extract(data,'$.role')='assistant'";
/// SQL for a row that has a token object at all.
const HAS_TOKENS: &str = "json_extract(data,'$.tokens') IS NOT NULL";
/// SQL for a row whose token figures are final. The full page read explains
/// why either marker is enough.
const TOKENS_FINAL: &str = "(json_extract(data,'$.time.completed') IS NOT NULL
     OR json_extract(data,'$.finish') IS NOT NULL)";

/// CASE short-circuits JSON extraction even for a malformed or BLOB value.
/// Invalid rows must still be returned so the caller can retain their locators.
fn eligible_row(final_only: bool) -> String {
    let valid = if final_only {
        format!("{ASSISTANT_ROW} AND {HAS_TOKENS} AND {TOKENS_FINAL}")
    } else {
        ASSISTANT_ROW.to_string()
    };
    format!("CASE WHEN typeof(data)!='text' OR NOT json_valid(data) THEN 1 ELSE ({valid}) END")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeSetAside {
    pub id: String,
    pub reason: String,
}

/// The id of the row with the smallest rowid: a cheap fingerprint of the
/// store's front. A different store at the same path, or a store whose
/// oldest rows were deleted, almost always has a different row there, so
/// comparing it between reads finds a replacement from the content itself,
/// without trusting file metadata. Both lookups use the rowid B-tree; the
/// plan (asserted by `the_front_row_probe_searches_by_rowid_and_never_scans`)
/// is:
///
///   SEARCH message USING INTEGER PRIMARY KEY (rowid=?)
///   SCALAR SUBQUERY 1: SEARCH message
const FRONT_ROW_SQL: &str = "SELECT id FROM message WHERE rowid=(SELECT MIN(rowid) FROM message)";

/// Read the front-row fingerprint; `None` for an empty table.
fn read_front_row_id(
    connection: &Connection,
    cost: &mut OpenCodeScanCost,
    stop: &AtomicBool,
) -> Result<Option<String>, OpenCodeReadError> {
    counted_query_row(connection, cost, FRONT_ROW_SQL, [], |row| {
        row.get::<_, String>(0)
    })
    .map(Some)
    .or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        other => Err(other),
    })
    .map_err(|error| sqlite_error(stop, "read OpenCode front row", error))
}

/// What a tail read should look at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenCodeTailRequest<'a> {
    /// Examine rows whose rowid is larger than this. `None` means no tail
    /// read has happened yet: start `seed_rows` below the largest rowid.
    pub after_rowid: Option<i64>,
    pub seed_rows: i64,
    /// Rowids of assistant rows an earlier tail read saw with tokens that
    /// were not final yet. Each is looked up again by rowid.
    pub open_rowids: &'a [i64],
    /// At most this many new assistant rows are examined; the next read
    /// continues after the last one.
    pub limit: usize,
}

/// An assistant row whose tokens were not final when the tail read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenCodeOpenRow {
    pub rowid: i64,
    pub updated_at_ms: i64,
}

/// The result of one tail read.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenCodeTailPage {
    pub set_aside: Vec<OpenCodeSetAside>,
    /// The largest rowid in `message` when the read's snapshot began.
    pub max_rowid: i64,
    /// New rows were examined after this rowid (the resolved start).
    pub start_rowid: i64,
    /// Every new row up to and including this rowid has been examined.
    pub scanned_through_rowid: i64,
    /// Rows with final tokens, from new rows and from re-read open rows.
    pub messages: Vec<OpenCodeMessage>,
    /// Assistant rows, new or re-read, whose tokens are still not final.
    pub open_rows: Vec<OpenCodeOpenRow>,
    /// The id at the smallest rowid, in this read's snapshot.
    pub front_row_id: Option<String>,
    pub cost: OpenCodeScanCost,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenCodePage {
    pub set_aside: Vec<OpenCodeSetAside>,
    pub lineage: String,
    pub consumed_ordinal: i64,
    pub high_water_ordinal: i64,
    pub raw: OpenCodeTotals,
    pub raw_after_cursor: OpenCodeTotals,
    pub emitted: OpenCodeTotals,
    pub emitted_after_cursor: OpenCodeTotals,
    pub messages: Vec<OpenCodeMessage>,
    /// The id at the smallest rowid, in the same snapshot as the page. A
    /// later tail read that finds a different one is reading another store.
    pub front_row_id: Option<String>,
    pub cost: OpenCodeScanCost,
}

/// Why a page read returned no page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCodeReadError {
    /// The caller's stop flag was set and the read was abandoned. Nothing was
    /// wrong with the store; the same page is simply read again on the next
    /// start.
    Stopped,
    /// The read failed.
    Failed(String),
}

impl From<String> for OpenCodeReadError {
    fn from(reason: String) -> Self {
        Self::Failed(reason)
    }
}

impl std::fmt::Display for OpenCodeReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => formatter.write_str("OpenCode read stopped"),
            Self::Failed(reason) => formatter.write_str(reason),
        }
    }
}

/// How often the interrupt watcher looks at the stop flag. A stop is noticed
/// within this interval, and the running statement aborts at SQLite's next
/// virtual-machine step after that.
const INTERRUPT_POLL: Duration = Duration::from_millis(20);

/// Map a SQLite error. SQLITE_INTERRUPT is a stop only when our own stop flag
/// is set: an interrupt nobody here asked for is a real failure and must not be
/// swallowed as a quiet shutdown.
pub(super) fn sqlite_error(
    stop: &AtomicBool,
    context: &str,
    error: rusqlite::Error,
) -> OpenCodeReadError {
    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted)
        && stop.load(Ordering::Relaxed)
    {
        OpenCodeReadError::Stopped
    } else {
        OpenCodeReadError::Failed(format!("{context}: {error}"))
    }
}

#[derive(Debug, Deserialize)]
struct StoredMessage {
    role: Option<String>,
    #[serde(rename = "modelID")]
    model_id: Option<String>,
    #[serde(rename = "providerID")]
    provider_id: Option<String>,
    tokens: Option<StoredTokens>,
    time: Option<StoredTime>,
    agent: Option<String>,
    #[serde(default, deserialize_with = "deserialize_path")]
    path: Option<StoredPath>,
    /// The step's finish reason (`stop`, `tool-calls`, ...). The page query
    /// already reads `finish` as a marker that the tokens are final; this
    /// keeps its value for cache diagnostics. Decoded leniently so a
    /// non-string value cannot make a message that decoded before fail now.
    #[serde(default, deserialize_with = "super::lenient::string")]
    finish: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StoredTokens {
    // ASTROCYTE DIVERGENCE: every numeric bucket is optional. An absent value
    // is not a producer-reported zero and must remain absent through ingestion.
    input: Option<u64>,
    output: Option<u64>,
    reasoning: Option<u64>,
    cache: Option<StoredCache>,
}

#[derive(Debug, Deserialize)]
struct StoredCache {
    read: Option<u64>,
    write: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct StoredTime {
    created: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StoredPath {
    root: Option<String>,
}

fn deserialize_path<'de, D>(deserializer: D) -> Result<Option<StoredPath>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let root = value
        .get("root")
        .and_then(|root| root.as_str())
        .map(str::to_string);
    Ok(Some(StoredPath { root }))
}

pub(super) fn read_page(
    db_path: &Path,
    since: Option<&OpenCodeCursor>,
    limit: usize,
    stop: &AtomicBool,
) -> Result<OpenCodePage, OpenCodeReadError> {
    if limit == 0 {
        return Err("OpenCode page limit must be positive".to_string().into());
    }
    with_read_snapshot(db_path, stop, |connection| {
        read_snapshot(connection, since, limit, stop)
    })
}

pub(super) fn read_tail(
    db_path: &Path,
    request: &OpenCodeTailRequest<'_>,
    stop: &AtomicBool,
) -> Result<OpenCodeTailPage, OpenCodeReadError> {
    if request.limit == 0 {
        return Err("OpenCode tail limit must be positive".to_string().into());
    }
    OpenCodeTailReader::open(db_path)?.read(db_path, request, stop)
}

/// A read-only tail connection. Each read ends its snapshot before returning,
/// leaving the producer free to checkpoint its WAL between reads.
#[derive(Debug)]
pub struct OpenCodeTailReader {
    connection: Connection,
}

impl OpenCodeTailReader {
    pub fn open(db_path: &Path) -> Result<Self, OpenCodeReadError> {
        Ok(Self {
            connection: super::sqlite_read::open_read_connection(
                readonly_uri(db_path)?,
                db_path,
                "OpenCode",
            )?,
        })
    }

    pub fn read(
        &self,
        db_path: &Path,
        request: &OpenCodeTailRequest<'_>,
        stop: &AtomicBool,
    ) -> Result<OpenCodeTailPage, OpenCodeReadError> {
        if request.limit == 0 {
            return Err("OpenCode tail limit must be positive".to_string().into());
        }
        in_read_snapshot(&self.connection, db_path, stop, |connection| {
            read_tail_snapshot(connection, request, stop)
        })
    }
}

/// Open `db_path` read-only, run `read` inside one read transaction, and make
/// `stop` able to abort a statement that is already running.
pub(super) fn with_read_snapshot<T>(
    db_path: &Path,
    stop: &AtomicBool,
    read: impl FnOnce(&Connection) -> Result<T, OpenCodeReadError>,
) -> Result<T, OpenCodeReadError> {
    if stop.load(Ordering::Relaxed) {
        return Err(OpenCodeReadError::Stopped);
    }

    let connection =
        super::sqlite_read::open_read_connection(readonly_uri(db_path)?, db_path, "OpenCode")?;
    in_read_snapshot(&connection, db_path, stop, read)
}

pub(super) fn in_read_snapshot<T>(
    connection: &Connection,
    db_path: &Path,
    stop: &AtomicBool,
    read: impl FnOnce(&Connection) -> Result<T, OpenCodeReadError>,
) -> Result<T, OpenCodeReadError> {
    if stop.load(Ordering::Relaxed) {
        return Err(OpenCodeReadError::Stopped);
    }
    crate::read_io::execute(connection, "BEGIN DEFERRED")
        .map_err(|error| sqlite_error(stop, "begin OpenCode read snapshot", error))?;

    // A single statement of the full page read can run for minutes on a large
    // store: the page query has no usable index, so SQLite finishes the whole
    // scan and sort before it yields a first row, and a stop flag checked
    // between rows is never reached. The watcher thread therefore interrupts
    // the statement itself. It keeps interrupting while the flag is set,
    // because an interrupt issued between two statements does not carry over
    // to the next one. The snapshot is read-only, so an interrupted read
    // leaves nothing behind in either store.
    let interrupt = connection.get_interrupt_handle();
    let (read_done, watcher_exit) = mpsc::channel::<()>();
    let result = std::thread::scope(|scope| {
        // The watcher ends when `read_done` is dropped (the channel
        // disconnects), which happens as soon as the read returns.
        scope.spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) =
                watcher_exit.recv_timeout(INTERRUPT_POLL)
            {
                if stop.load(Ordering::Relaxed) {
                    interrupt.interrupt();
                }
            }
        });
        let result = read(connection);
        drop(read_done);
        result
    });
    let finish = crate::read_io::execute(
        connection,
        if result.is_ok() { "COMMIT" } else { "ROLLBACK" },
    );
    match (result, finish) {
        (Ok(page), Ok(())) => Ok(page),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(sqlite_error(
            stop,
            &format!("finish OpenCode read snapshot {}", db_path.display()),
            error,
        )),
    }
}

fn read_snapshot(
    connection: &Connection,
    since: Option<&OpenCodeCursor>,
    limit: usize,
    stop: &AtomicBool,
) -> Result<OpenCodePage, OpenCodeReadError> {
    // The oldest message id is immutable while the store's front remains
    // intact and changes when a fresh store starts with different contents. If
    // OpenCode prunes from the front, this deliberately changes lineage and
    // causes a conservative full replay; content-derived event ids make that
    // extra work safe, while retaining the old cursor could skip live rows.
    let mut cost = OpenCodeScanCost::default();
    let lineage = counted_query_row(
        connection,
        &mut cost,
        "SELECT id FROM message ORDER BY time_created,id LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )
    .or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok("empty".to_string()),
        other => Err(other),
    })
    .map_err(|error| sqlite_error(stop, "read OpenCode lineage", error))?;
    let high_water_ordinal = counted_query_row(
        connection,
        &mut cost,
        "SELECT COALESCE(MAX(time_updated),0) FROM message",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map_err(|error| sqlite_error(stop, "read OpenCode high water", error))?;

    let represented = since.filter(|cursor| cursor.lineage == lineage);
    if represented.is_some_and(|cursor| cursor.ordinal > high_water_ordinal) {
        return Err(format!(
            "OpenCode high water regressed within lineage {lineage}: stored {}, offered {high_water_ordinal}",
            represented.expect("checked above").ordinal
        )
        .into());
    }
    let lower_bound = represented.map_or(0, |cursor| cursor.ordinal);
    // The two lower bounds below must use the same comparison. `time_updated`
    // is not unique: on 2026-09-20 the live store had 1,241,262 messages but
    // only 1,235,249 distinct update timestamps, so 6,013 rows shared an
    // ordinal and a page cutoff can land inside a tie. Mixing inclusive and
    // exclusive bounds makes boundary selection and row scanning disagree,
    // which can skip rows or perform unintended duplicate work. Both are kept
    // inclusive as the conservative policy because a boundary re-read becomes
    // a free ExactRedelivery under content-derived identity. The test
    // `tied_timestamp_boundary_is_inclusive_so_no_message_is_skipped` fences
    // one-sided edits to this pair; changing both bounds to exclusive is also
    // safe and should update that test's expected boundary replay behavior.
    let page_offset = i64::try_from(limit.saturating_sub(1))
        .map_err(|_| "OpenCode page limit exceeds SQLite integer range".to_string())?;

    // The cutoff counts every row, including assistant rows whose tokens are
    // not final yet, so the cursor can move past an in-flight message. That
    // is safe because of how OpenCode writes the row. Every message write
    // sets `time_updated` to `Date.now()` at write time: drizzle applies the
    // column's `$onUpdate` hook to the insert and to the upsert's `set`. The
    // write that makes the tokens final happens after this read saw the row
    // in flight. This cursor is at most the snapshot's MAX(time_updated),
    // which is no later than the wall clock at read time. So the final write
    // lands at a `time_updated` no lower than the cursor. It may be equal
    // (millisecond resolution), which the inclusive lower bound catches. The
    // test `completion_at_the_cursor_timestamp_is_still_ingested` fences that
    // equal case.
    //
    // The alternative, stopping the cutoff at the first row that is not final
    // yet, is unusable. Messages whose process died before any final write
    // stay non-final forever. On 2026-09-22 the live store held 586 such
    // assistant rows, the oldest last updated 195 days earlier, and they
    // would pin the cursor there permanently.
    //
    // Two limits of a wall-clock cursor apply to every row, not only to
    // in-flight ones: a backwards clock step, and a writer that computed
    // `Date.now()` before waiting on another process's write lock. Either can
    // commit a `time_updated` below a cursor that was already read.
    let cutoff = counted_query_row(
        connection,
        &mut cost,
        "SELECT time_updated
         FROM message
         WHERE time_updated>=?1 AND time_updated<=?2
         ORDER BY time_updated,id
         LIMIT 1 OFFSET ?3",
        params![lower_bound, high_water_ordinal, page_offset],
        |row| row.get::<_, i64>(0),
    )
    .or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok(high_water_ordinal),
        other => Err(other),
    })
    .map_err(|error| sqlite_error(stop, "select OpenCode page boundary", error))?;

    // The ledger stores measurements write-once, so a message is selected
    // only once its token figure is final. While a turn streams, OpenCode
    // keeps the row with all-zero tokens. Ingesting that row would store the
    // zeros, and the later completed figure would then be quarantined as a
    // conflicting redelivery. A row that is not yet final is skipped here and
    // picked up by a later drain. The comment on the page cutoff explains why
    // the cursor may still move past it.
    //
    // Either marker below means the tokens are final:
    // - `time.completed` is set by the processor's cleanup, which is the last
    //   write of a turn.
    // - `finish` is NOT a terminal marker. It means the tokens are final. In
    //   OpenCode v1.18.30, packages/opencode/src/session/processor.ts, the
    //   `finish-step` handler (L457-470) sets `finish` and assigns the step's
    //   final `tokens` in the same persisted update. `cleanup()` (L609-610)
    //   later adds only `time.completed`, and no field this reader maps comes
    //   from it. Each assistant message holds one model step: prompt.ts
    //   creates a new message per loop iteration, and `streamText` is called
    //   with the AI SDK default `stopWhen = stepCountIs(1)`. Checking this
    //   marker keeps the tokens of a message whose process died between
    //   finish-step and cleanup; such a message never gains `time.completed`.
    //   If OpenCode ever rewrites tokens after finish-step (a stream retry
    //   or a multi-step message), the rewrite arrives as a quarantined
    //   redelivery, a visible failure, rather than a silently lost figure.
    //   Re-check the cited lines when upgrading OpenCode.
    //
    // The predicate text is shared with the tail read (`TOKENS_FINAL`), so
    // both paths select exactly the same rows as final.
    let mut statement = connection
        .prepare(&format!(
            "SELECT id,session_id,time_updated,data
             FROM message
             WHERE time_updated>=?1 AND time_updated<=?2
               AND {}
             ORDER BY time_updated,id",
            eligible_row(true)
        ))
        .map_err(|error| sqlite_error(stop, "prepare OpenCode message scan", error))?;
    let mut rows = statement
        .query(params![lower_bound, cutoff])
        .map_err(|error| sqlite_error(stop, "query OpenCode messages", error))?;

    let lineage_reset = since.is_some_and(|cursor| cursor.lineage != lineage);
    let mut page = OpenCodePage {
        set_aside: Vec::new(),
        lineage,
        consumed_ordinal: cutoff,
        high_water_ordinal,
        raw: OpenCodeTotals::default(),
        raw_after_cursor: OpenCodeTotals::default(),
        emitted: OpenCodeTotals::default(),
        emitted_after_cursor: OpenCodeTotals::default(),
        messages: Vec::new(),
        front_row_id: None,
        cost: OpenCodeScanCost::default(),
    };

    while let Some(row) = rows
        .next()
        .map_err(|error| sqlite_error(stop, "step OpenCode message scan", error))?
    {
        // Decoding rows is ordinary Rust work that the SQLite interrupt does
        // not reach. Abandoning the partial page here is safe: the caller
        // commits nothing without a whole page, and the next start reads the
        // same page again.
        if stop.load(Ordering::Relaxed) {
            return Err(OpenCodeReadError::Stopped);
        }
        let id = row
            .get::<_, String>(0)
            .map_err(|error| format!("read OpenCode message id: {error}"))?;
        let session_id = row
            .get::<_, String>(1)
            .map_err(|error| format!("read OpenCode session id for {id}: {error}"))?;
        let updated_at_ms = row
            .get::<_, i64>(2)
            .map_err(|error| format!("read OpenCode update time for {id}: {error}"))?;
        let data = match row.get::<_, String>(3) {
            Ok(data) => data,
            Err(error) => {
                page.set_aside.push(OpenCodeSetAside {
                    id,
                    reason: format!("read OpenCode data: {error}"),
                });
                continue;
            }
        };
        let after_cursor =
            represented.is_none_or(|cursor| updated_at_ms > cursor.ordinal) || lineage_reset;

        let mut bytes = data.into_bytes();
        let stored: StoredMessage = match simd_json::from_slice(&mut bytes) {
            Ok(stored) => stored,
            Err(error) => {
                page.set_aside.push(OpenCodeSetAside {
                    id,
                    reason: format!("decode OpenCode data: {error}"),
                });
                page.raw.records = page.raw.records.saturating_add(1);
                if after_cursor {
                    page.raw_after_cursor.records = page.raw_after_cursor.records.saturating_add(1);
                }
                continue;
            }
        };
        let Some(tokens) = stored.tokens.as_ref() else {
            continue;
        };
        let breakdown = token_breakdown(tokens);
        page.raw.add_tokens(&breakdown);
        if after_cursor {
            page.raw_after_cursor.add_tokens(&breakdown);
        }

        let Some(message) = map_stored(id.clone(), session_id, updated_at_ms, stored, breakdown)
        else {
            page.set_aside.push(OpenCodeSetAside {
                id,
                reason: "OpenCode usage lacks a usable time, provider or model".into(),
            });
            continue;
        };
        page.emitted.add_tokens(&message.tokens);
        if after_cursor {
            page.emitted_after_cursor.add_tokens(&message.tokens);
        }
        page.messages.push(message);
    }

    drop(rows);
    cost.add_statement(&statement);
    // Read last, inside the same snapshot. The stop tests' slow fixture
    // makes `message` a view, which has no rowid; it never gets this far
    // because those tests interrupt the page statement above.
    page.front_row_id = read_front_row_id(connection, &mut cost, stop)?;
    page.cost = cost;
    Ok(page)
}

fn token_breakdown(tokens: &StoredTokens) -> TokenBreakdown {
    TokenBreakdown {
        input: tokens.input,
        output: tokens.output,
        cache_read: tokens.cache.as_ref().and_then(|cache| cache.read),
        cache_write: tokens.cache.as_ref().and_then(|cache| cache.write),
        reasoning: tokens.reasoning,
    }
}

/// Read rows by rowid instead of by `time_updated`. The rowid is the table's
/// own B-tree key, so a range starting at a known rowid and a lookup of one
/// rowid cost O(log n) plus the rows returned, whatever the table's size.
/// OpenCode inserts a message once and later rewrites it in place with
/// `ON CONFLICT DO UPDATE`, which keeps its rowid, and SQLite gives a new row
/// a rowid above the current largest one. So new rows appear above the last
/// rowid examined, and the only later writes that matter to the ledger, a
/// row's tokens becoming final, land on rows already known to be open.
///
/// This read never decides the durable cursor and is not a complete view:
/// see `OpenCodeSource` for what it can miss and what catches that.
fn read_tail_snapshot(
    connection: &Connection,
    request: &OpenCodeTailRequest<'_>,
    stop: &AtomicBool,
) -> Result<OpenCodeTailPage, OpenCodeReadError> {
    let mut cost = OpenCodeScanCost::default();
    let max_rowid = counted_query_row(
        connection,
        &mut cost,
        "SELECT COALESCE(MAX(rowid),0) FROM message",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map_err(|error| sqlite_error(stop, "read OpenCode largest rowid", error))?;
    let start_rowid = request
        .after_rowid
        .unwrap_or_else(|| max_rowid.saturating_sub(request.seed_rows).max(0));
    let limit = i64::try_from(request.limit)
        .map_err(|_| "OpenCode tail limit exceeds SQLite integer range".to_string())?;
    let columns = format!(
        "SELECT rowid,id,session_id,time_updated,data,(CASE WHEN typeof(data)='text' AND json_valid(data) THEN ({HAS_TOKENS} AND {TOKENS_FINAL}) ELSE 0 END)
         FROM message"
    );
    let mut page = OpenCodeTailPage {
        set_aside: Vec::new(),
        max_rowid,
        start_rowid,
        scanned_through_rowid: start_rowid.max(max_rowid),
        messages: Vec::new(),
        open_rows: Vec::new(),
        front_row_id: read_front_row_id(connection, &mut cost, stop)?,
        cost: OpenCodeScanCost::default(),
    };

    let mut new_rows = crate::read_io::prepare_cached(
        connection,
        &format!(
            "{columns}
             WHERE rowid>?1 AND rowid<=?2 AND {}
             ORDER BY rowid
             LIMIT ?3",
            eligible_row(false)
        ),
    )
    .map_err(|error| sqlite_error(stop, "prepare OpenCode tail scan", error))?;
    let mut rows = new_rows
        .query(params![start_rowid, max_rowid, limit])
        .map_err(|error| sqlite_error(stop, "query OpenCode tail scan", error))?;
    let mut returned = 0usize;
    while let Some(row) = rows
        .next()
        .map_err(|error| sqlite_error(stop, "step OpenCode tail scan", error))?
    {
        let rowid = take_tail_row(row, &mut page, stop)?;
        returned += 1;
        if returned == request.limit {
            page.scanned_through_rowid = rowid;
        }
    }
    drop(rows);
    cost.add_statement(&new_rows);

    if request
        .open_rowids
        .iter()
        .any(|rowid| *rowid <= start_rowid)
    {
        let mut lookup = crate::read_io::prepare_cached(
            connection,
            &format!("{columns} WHERE rowid=?1 AND {}", eligible_row(false)),
        )
        .map_err(|error| sqlite_error(stop, "prepare OpenCode open-row lookup", error))?;
        // Rows above the start were already examined by the range scan above.
        for &rowid in request
            .open_rowids
            .iter()
            .filter(|&&rowid| rowid <= start_rowid)
        {
            let mut rows = lookup
                .query([rowid])
                .map_err(|error| sqlite_error(stop, "query OpenCode open row", error))?;
            if let Some(row) = rows
                .next()
                .map_err(|error| sqlite_error(stop, "step OpenCode open row", error))?
            {
                take_tail_row(row, &mut page, stop)?;
            }
        }
        cost.add_statement(&lookup);
    }
    page.cost = cost;
    Ok(page)
}

/// Sort one tail row into the page: a final row becomes a message, a row
/// that is not final yet becomes an open row. Returns the row's rowid.
fn take_tail_row(
    row: &rusqlite::Row<'_>,
    page: &mut OpenCodeTailPage,
    stop: &AtomicBool,
) -> Result<i64, OpenCodeReadError> {
    // As in the full page read: decoding is Rust work the SQLite interrupt
    // does not reach, and the caller commits nothing from a partial read.
    if stop.load(Ordering::Relaxed) {
        return Err(OpenCodeReadError::Stopped);
    }
    let rowid = row
        .get::<_, i64>(0)
        .map_err(|error| format!("read OpenCode rowid: {error}"))?;
    let id = row
        .get::<_, String>(1)
        .map_err(|error| format!("read OpenCode message id at rowid {rowid}: {error}"))?;
    let session_id = row
        .get::<_, String>(2)
        .map_err(|error| format!("read OpenCode session id for {id}: {error}"))?;
    let updated_at_ms = row
        .get::<_, i64>(3)
        .map_err(|error| format!("read OpenCode update time for {id}: {error}"))?;
    let data = match row.get::<_, String>(4) {
        Ok(data) => data,
        Err(error) => {
            page.set_aside.push(OpenCodeSetAside {
                id,
                reason: format!("read OpenCode data: {error}"),
            });
            return Ok(rowid);
        }
    };
    let mut bytes = data.into_bytes();
    let stored: StoredMessage = match simd_json::from_slice(&mut bytes) {
        Ok(stored) => stored,
        Err(error) => {
            page.set_aside.push(OpenCodeSetAside {
                id,
                reason: format!("decode OpenCode data: {error}"),
            });
            return Ok(rowid);
        }
    };
    let is_final = row
        .get::<_, bool>(5)
        .map_err(|error| format!("read OpenCode finality for {id}: {error}"))?;
    if !is_final {
        page.open_rows.push(OpenCodeOpenRow {
            rowid,
            updated_at_ms,
        });
        return Ok(rowid);
    }
    if let Some(breakdown) = stored.tokens.as_ref().map(token_breakdown) {
        if let Some(message) = map_stored(id.clone(), session_id, updated_at_ms, stored, breakdown)
        {
            page.messages.push(message);
        } else {
            page.set_aside.push(OpenCodeSetAside {
                id,
                reason: "OpenCode usage lacks a usable time, provider or model".into(),
            });
        }
    }
    Ok(rowid)
}

fn map_stored(
    id: String,
    session_id: String,
    updated_at_ms: i64,
    stored: StoredMessage,
    tokens: TokenBreakdown,
) -> Option<OpenCodeMessage> {
    if stored.role.as_deref() != Some("assistant") {
        return None;
    }
    let occurred_at_ms = stored.time?.created?;
    if !occurred_at_ms.is_finite() || occurred_at_ms < 0.0 || occurred_at_ms > i64::MAX as f64 {
        return None;
    }
    let occurred_at_ms = if occurred_at_ms >= i64::MAX as f64 {
        i64::MAX
    } else {
        // The check above keeps this finite timestamp in the i64 range.
        #[allow(clippy::cast_possible_truncation)]
        let occurred_at_ms = occurred_at_ms as i64;
        occurred_at_ms
    };

    Some(OpenCodeMessage {
        id,
        session_id,
        updated_at_ms,
        occurred_at_ms,
        // ASTROCYTE DIVERGENCE: use the path OpenCode stored on this message.
        // The session directory is only a session-level coordinate and can name
        // a different location than the project where this message occurred.
        project_root: stored.path.and_then(|path| path.root),
        // ASTROCYTE DIVERGENCE: preserve provider/model ids verbatim so the
        // ledger retains OpenCode's identities instead of substituted display aliases.
        provider_id: stored.provider_id?,
        model_id: stored.model_id?,
        // ASTROCYTE DIVERGENCE: retain the exact producer label, including
        // U+200B prefixes. Normalizing visually identical labels at ingest
        // destroys evidence that OpenCode declared distinct work classes.
        agent: stored.agent,
        finish: stored.finish,
        // OpenCode reports output and reasoning as separate buckets. Subtracting
        // reasoning would corrupt turns where reasoning exceeds output.
        tokens,
    })
}

pub(super) fn readonly_uri(path: &Path) -> Result<String, String> {
    let raw = path
        .to_str()
        .ok_or_else(|| format!("OpenCode database path is not UTF-8: {}", path.display()))?;
    Ok(crate::sqlite_uri::sqlite_readonly_uri(raw))
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "realistic-size page-query measurement"]
    fn set_aside_page_query_cost_before_and_after() {
        use std::time::Instant;
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE message(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,time_created INTEGER NOT NULL,time_updated INTEGER NOT NULL,data TEXT NOT NULL); CREATE INDEX message_session_time_created_id_idx ON message(session_id,time_created,id);").unwrap();
        let tx = conn.transaction().unwrap();
        {
            let mut insert = tx
                .prepare("INSERT INTO message VALUES (?1,'s',?2,?2,?3)")
                .unwrap();
            for i in 0..1_834_282 {
                let data = if i % 3 == 0 {
                    r#"{"role":"user"}"#
                } else {
                    r#"{"role":"assistant","tokens":{"input":100,"output":20},"finish":"stop","time":{"created":1}}"#
                };
                insert
                    .execute(params![format!("msg-{i:08}"), i, data])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        let old = format!("{ASSISTANT_ROW} AND {HAS_TOKENS} AND {TOKENS_FINAL}");
        for (name, predicate) in [("before", old), ("after", eligible_row(true))] {
            let mut stmt = conn.prepare(&format!("SELECT id,session_id,time_updated,data FROM message WHERE time_updated>=?1 AND time_updated<=?2 AND {predicate} ORDER BY time_updated,id")).unwrap();
            let start = Instant::now();
            let records = stmt
                .query_map(params![1_000_000, 1_009_999], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let mut cost = OpenCodeScanCost::default();
            cost.add_statement(&stmt);
            assert_eq!(records.len(), 6667);
            eprintln!("PAGE COST {name}: rows=1834282 selected={} elapsed_ms={} vm_steps={} fullscan_steps={}", records.len(), start.elapsed().as_millis(), cost.vm_steps, cost.fullscan_steps);
        }
    }
    #[test]
    fn forward_slash_unc_uri_keeps_the_server_in_the_filename() {
        assert_eq!(
            super::readonly_uri(std::path::Path::new("//server/share/x.db")).unwrap(),
            "file:%2F%2Fserver/share/x.db?mode=ro"
        );
    }

    use super::*;

    fn interrupted() -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
            None,
        )
    }

    /// The front-row probe runs on every tick, so it must stay a lookup by
    /// rowid. This builds OpenCode's schema, including its one index on
    /// `message`, and checks the planner's choice.
    #[test]
    fn the_front_row_probe_searches_by_rowid_and_never_scans() {
        let temp = tempfile::TempDir::new().unwrap();
        let connection = Connection::open(temp.path().join("opencode.db")).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE message (
                     id text PRIMARY KEY,
                     session_id text NOT NULL,
                     time_created integer NOT NULL,
                     time_updated integer NOT NULL,
                     data text NOT NULL
                 );
                 CREATE INDEX message_session_time_created_id_idx
                     ON message (session_id,time_created,id);",
            )
            .unwrap();
        let mut plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {FRONT_ROW_SQL}"))
            .unwrap();
        let details = plan
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|detail| detail.starts_with("SEARCH message USING INTEGER PRIMARY KEY")),
            "{details:?}"
        );
        assert!(
            details.iter().all(|detail| !detail.contains("SCAN")),
            "{details:?}"
        );
    }

    /// Something other than our stop can interrupt the connection. Reporting
    /// that as a stop would hide a real failure behind a quiet shutdown, so
    /// only an interrupt that arrives while our own flag is set counts as one.
    #[test]
    fn an_interrupt_is_a_stop_only_when_our_stop_flag_is_set() {
        assert_eq!(
            sqlite_error(&AtomicBool::new(true), "step", interrupted()),
            OpenCodeReadError::Stopped
        );
        assert!(matches!(
            sqlite_error(&AtomicBool::new(false), "step", interrupted()),
            OpenCodeReadError::Failed(reason) if reason.starts_with("step: ")
        ));
        assert!(matches!(
            sqlite_error(
                &AtomicBool::new(true),
                "step",
                rusqlite::Error::QueryReturnedNoRows
            ),
            OpenCodeReadError::Failed(_)
        ));
    }
}
