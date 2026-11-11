//! Read-only OpenCode 2 `session_message` usage. The `session_v2` marker is
//! required because OpenCode 1 also created a `session_message` table.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;

use super::opencode::{OpenCodeCursor, OpenCodeMessage, OpenCodeReadError, OpenCodeScanCost};
use super::opencode_schema::{
    counted_query_row, in_read_snapshot, readonly_uri, sqlite_error, with_read_snapshot,
    OpenCodeOpenRow, OpenCodeSetAside, OpenCodeTailRequest,
};
use crate::TokenBreakdown;

/// One quiet hour bounds streaming latency without dropping billed calls whose
/// process died before settlement. Recent incomplete token snapshots are not final.
pub const QUIET_WINDOW: i64 = 60 * 60 * 1_000;

/// Per-session finality and copied-history boundary for a shared store.
#[derive(Clone, Copy, Default)]
pub struct SessionPolicy {
    pub settled: bool,
    pub imported_through_seq: Option<u64>,
}

#[derive(Debug, Default)]
pub struct Page {
    pub messages: Vec<OpenCodeMessage>,
    pub set_aside: Vec<OpenCodeSetAside>,
    pub open_rows: Vec<OpenCodeOpenRow>,
    pub skipped_legacy: usize,
    pub skipped_imported: usize,
    pub lineage: String,
    pub consumed: i64,
    pub high_water: i64,
    pub front_id: Option<String>,
    pub max_rowid: i64,
    pub start_rowid: i64,
    pub through_rowid: i64,
    pub cost: OpenCodeScanCost,
    /// Work spent discovering incomplete rows below the timestamp cursor.
    pub incomplete_cost: OpenCodeScanCost,
    pub open_row_floor: i64,
}

#[derive(Deserialize)]
struct Message {
    model: Option<Model>,
    agent: Option<String>,
    tokens: Option<Tokens>,
    time: Option<Time>,
    #[serde(default, deserialize_with = "super::lenient::string")]
    finish: Option<String>,
}

#[derive(Deserialize)]
struct Model {
    #[serde(rename = "providerID")]
    provider: String,
    id: String,
}

#[derive(Deserialize)]
struct Time {
    created: Option<f64>,
    completed: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct Tokens {
    input: Option<u64>,
    output: Option<u64>,
    reasoning: Option<u64>,
    cache: Option<Cache>,
}

#[derive(Deserialize)]
struct Cache {
    read: Option<u64>,
    write: Option<u64>,
}

fn table_exists(
    connection: &Connection,
    cost: &mut OpenCodeScanCost,
    name: &str,
) -> rusqlite::Result<bool> {
    counted_query_row(
        connection,
        cost,
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |row| row.get(0),
    )
}

fn begin_page(
    connection: &Connection,
    stop: &AtomicBool,
    check_marker: bool,
) -> Result<Option<Page>, OpenCodeReadError> {
    let mut page = Page::default();
    if check_marker
        && !table_exists(connection, &mut page.cost, "session_v2")
            .map_err(|e| sqlite_error(stop, "detect OpenCode 2", e))?
    {
        return Ok(None);
    }
    page.front_id = counted_query_row(
        connection,
        &mut page.cost,
        "SELECT id FROM session_message WHERE rowid=(SELECT MIN(rowid) FROM session_message)",
        [],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| sqlite_error(stop, "read OpenCode 2 front row", e))?;
    page.lineage = page.front_id.clone().unwrap_or_else(|| "empty".into());
    page.max_rowid = counted_query_row(
        connection,
        &mut page.cost,
        "SELECT COALESCE(MAX(rowid),0) FROM session_message",
        [],
        |row| row.get(0),
    )
    .map_err(|e| sqlite_error(stop, "read OpenCode 2 largest rowid", e))?;
    Ok(Some(page))
}

/// Only a full pass moves the durable update-time cursor. Old incomplete rows
/// are revisited even below it: merely aging past the quiet window does not
/// change `time_updated`, including when the reader restarts while they wait.
pub fn read_page(
    path: &Path,
    cursor: Option<&OpenCodeCursor>,
    limit: usize,
    now_ms: i64,
    stop: &AtomicBool,
) -> Result<Option<Page>, OpenCodeReadError> {
    read_page_with_settled_sessions(path, cursor, None, limit, now_ms, stop, &|_| false)
}

/// Read a full page with immediate finality for sessions that can no longer write.
/// Other sessions retain the ordinary quiet window, including unlisted sessions.
pub fn read_page_with_settled_sessions(
    path: &Path,
    cursor: Option<&OpenCodeCursor>,
    open_row_floor: Option<i64>,
    limit: usize,
    now_ms: i64,
    stop: &AtomicBool,
    settled: &dyn Fn(&str) -> bool,
) -> Result<Option<Page>, OpenCodeReadError> {
    read_page_with_sessions(
        path,
        cursor,
        open_row_floor,
        limit,
        now_ms,
        stop,
        &|session| SessionPolicy {
            settled: settled(session),
            ..SessionPolicy::default()
        },
    )
}

/// Read a full page, consuming copied history without emitting or retaining it.
pub fn read_page_with_sessions(
    path: &Path,
    cursor: Option<&OpenCodeCursor>,
    open_row_floor: Option<i64>,
    limit: usize,
    now_ms: i64,
    stop: &AtomicBool,
    sessions: &dyn Fn(&str) -> SessionPolicy,
) -> Result<Option<Page>, OpenCodeReadError> {
    with_read_snapshot(path, stop, |connection| {
        let Some(mut page) = begin_page(connection, stop, true)? else {
            return Ok(None);
        };
        page.high_water = counted_query_row(
            connection,
            &mut page.cost,
            "SELECT COALESCE(MAX(time_updated),0) FROM session_message",
            [],
            |row| row.get(0),
        )
        .map_err(|e| sqlite_error(stop, "read OpenCode 2 high water", e))?;
        let represented = cursor.filter(|cursor| cursor.lineage == page.lineage);
        // A new row can reuse a deleted rowid below this floor. The timestamp
        // branch remains unbounded by rowid and discovers it before the cursor
        // passes it: new writes must be stamped at or after the cursor, the
        // same clock assumption the existing timestamp cursor requires.
        // A replaced store or an upgrade without a floor needs one discovery.
        let floor = represented.and(open_row_floor).unwrap_or(i64::MIN);
        let lower = represented.map_or(0, |cursor| cursor.ordinal);
        if lower > page.high_water {
            // OpenCode deletes sessions. When the deleted sessions held every
            // row at or above the cursor, the maximum falls below the cursor
            // while the front row (and so the lineage) is unchanged. The store
            // then holds no row newer than the cursor. Keep the high water at
            // the cursor, as its durable row requires, but still check older
            // incomplete calls: they may now be quiet or their session done.
            // Completed rows below the cursor are not re-read. A later write
            // at or above the cursor is read normally.
            page.high_water = lower;
        }
        let offset = i64::try_from(
            limit
                .checked_sub(1)
                .ok_or_else(|| "OpenCode 2 page limit is zero".to_string())?,
        )
        .map_err(|_| "OpenCode 2 page limit exceeds SQLite integer range".to_string())?;
        // Include every row tied at the cutoff. Both range bounds are inclusive,
        // so a completion sharing a cursor timestamp cannot be lost.
        page.consumed = counted_query_row(
            connection,
            &mut page.cost,
            "SELECT time_updated FROM session_message WHERE time_updated>=?1
             ORDER BY time_updated,seq LIMIT 1 OFFSET ?2",
            params![lower, offset],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| sqlite_error(stop, "select OpenCode 2 boundary", e))?
        .unwrap_or(page.high_water);
        if page.consumed == lower && lower < page.high_water {
            // A whole page of rows shares the cursor's own update time, so the
            // boundary above lands on the cursor again and would never move.
            // That happens when OpenCode writes many rows in one millisecond,
            // as when it imports a long session. Move the boundary to the next
            // update time instead: the page then reads every tied row plus the
            // rows at that next time, and the cursor passes the tie.
            page.consumed = counted_query_row(
                connection,
                &mut page.cost,
                "SELECT MIN(time_updated) FROM session_message WHERE time_updated>?1",
                params![lower],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(|e| sqlite_error(stop, "select OpenCode 2 boundary past a tie", e))?
            .unwrap_or(page.high_water);
        }
        let legacy = table_exists(connection, &mut page.cost, "message")
            .map_err(|e| sqlite_error(stop, "detect OpenCode 1 table", e))?;
        // Discover old incomplete rows separately so their JSON work can be
        // measured independently of the unindexed scalar timestamp scans.
        let mut incomplete = connection
            .prepare(
                "SELECT rowid FROM session_message
                 WHERE rowid>=?3 AND type='assistant' AND time_updated<?1 AND time_updated<=?2
                   AND CASE WHEN typeof(data)='text' AND json_valid(data)
                     THEN json_extract(data,'$.time.completed') IS NULL ELSE 0 END",
            )
            .map_err(|e| sqlite_error(stop, "prepare OpenCode 2 incomplete rows", e))?;
        let old_open = incomplete
            .query_map(params![lower, page.consumed, floor], |row| {
                row.get::<_, i64>(0)
            })
            .and_then(Iterator::collect::<rusqlite::Result<Vec<_>>>)
            .map_err(|e| sqlite_error(stop, "read OpenCode 2 incomplete rows", e))?;
        page.incomplete_cost.add_statement(&incomplete);
        page.cost.add_assign_saturating(&page.incomplete_cost);
        let old_open = serde_json::to_string(&old_open)
            .map_err(|e| format!("encode OpenCode 2 incomplete rowids: {e}"))?;
        let mut statement = connection
            .prepare(
                "SELECT rowid,id,session_id,time_updated,data,seq FROM session_message
             WHERE type='assistant' AND time_updated<=?2 AND
               (time_updated>=?1 OR rowid IN (SELECT value FROM json_each(?3)))
             ORDER BY seq",
            )
            .map_err(|e| sqlite_error(stop, "prepare OpenCode 2 page", e))?;
        let mut rows = statement
            .query(params![lower, page.consumed, old_open])
            .map_err(|e| sqlite_error(stop, "query OpenCode 2 page", e))?;
        while let Some(row) = rows
            .next()
            .map_err(|e| sqlite_error(stop, "step OpenCode 2 page", e))?
        {
            take_row(connection, row, legacy, now_ms, stop, &mut page, sessions)?;
        }
        drop(rows);
        page.cost.add_statement(&statement);
        page.open_row_floor = page
            .open_rows
            .iter()
            .map(|row| row.rowid)
            .min()
            .unwrap_or(page.max_rowid);
        Ok(Some(page))
    })
}

/// Whether the store at `path` has OpenCode 1's `message` table. An OpenCode 2
/// store need not have one, and the OpenCode 1 reader fails on a store
/// without it. Opened read-only like every other read here.
pub fn has_v1_message_table(path: &Path, stop: &AtomicBool) -> Result<bool, OpenCodeReadError> {
    with_read_snapshot(path, stop, |connection| {
        table_exists(connection, &mut OpenCodeScanCost::default(), "message")
            .map_err(|e| sqlite_error(stop, "detect OpenCode 1 table", e))
    })
}

/// A rowid range plus point lookups for rows still in flight. No update-time
/// query or full table scan runs on this path; idle cost is independent of history.
pub fn read_tail(
    path: &Path,
    request: &OpenCodeTailRequest<'_>,
    now_ms: i64,
    stop: &AtomicBool,
) -> Result<Option<Page>, OpenCodeReadError> {
    with_read_snapshot(path, stop, |connection| {
        read_tail_snapshot(connection, request, now_ms, stop, true, &|_| {
            SessionPolicy::default()
        })
    })
}

/// Retain the read-only connection and statements, never the read transaction.
pub struct TailReader {
    connection: Connection,
}

impl TailReader {
    pub fn open(path: &Path, stop: &AtomicBool) -> Result<Option<Self>, OpenCodeReadError> {
        let connection =
            super::sqlite_read::open_read_connection(readonly_uri(path)?, path, "OpenCode 2")?;
        // After an error, the path may name a store without the v2 schema.
        let present = in_read_snapshot(&connection, path, stop, |connection| {
            table_exists(connection, &mut OpenCodeScanCost::default(), "session_v2")
                .map_err(|error| sqlite_error(stop, "detect OpenCode 2", error))
        })?;
        Ok(present.then_some(Self { connection }))
    }

    /// Full ticks check the marker. Fast reads may skip that query only after
    /// a full tick established that this store contains the v2 schema.
    pub fn read(
        &self,
        path: &Path,
        request: &OpenCodeTailRequest<'_>,
        now_ms: i64,
        stop: &AtomicBool,
        check_marker: bool,
    ) -> Result<Option<Page>, OpenCodeReadError> {
        self.read_with_settled_sessions(path, request, now_ms, stop, check_marker, &|_| false)
    }

    /// Read a tail with the same per-session finality as a full page.
    pub fn read_with_settled_sessions(
        &self,
        path: &Path,
        request: &OpenCodeTailRequest<'_>,
        now_ms: i64,
        stop: &AtomicBool,
        check_marker: bool,
        settled: &dyn Fn(&str) -> bool,
    ) -> Result<Option<Page>, OpenCodeReadError> {
        self.read_with_sessions(path, request, now_ms, stop, check_marker, &|session| {
            SessionPolicy {
                settled: settled(session),
                ..SessionPolicy::default()
            }
        })
    }

    /// Read a tail with the same copied-history and finality rules as a full page.
    pub fn read_with_sessions(
        &self,
        path: &Path,
        request: &OpenCodeTailRequest<'_>,
        now_ms: i64,
        stop: &AtomicBool,
        check_marker: bool,
        sessions: &dyn Fn(&str) -> SessionPolicy,
    ) -> Result<Option<Page>, OpenCodeReadError> {
        in_read_snapshot(&self.connection, path, stop, |connection| {
            read_tail_snapshot(connection, request, now_ms, stop, check_marker, sessions)
        })
    }
}

fn read_tail_snapshot(
    connection: &Connection,
    request: &OpenCodeTailRequest<'_>,
    now_ms: i64,
    stop: &AtomicBool,
    check_marker: bool,
    sessions: &dyn Fn(&str) -> SessionPolicy,
) -> Result<Option<Page>, OpenCodeReadError> {
    let Some(mut page) = begin_page(connection, stop, check_marker)? else {
        return Ok(None);
    };
    page.start_rowid = request
        .after_rowid
        .unwrap_or_else(|| page.max_rowid.saturating_sub(request.seed_rows).max(0));
    page.through_rowid = page.start_rowid.max(page.max_rowid);
    let limit = i64::try_from(request.limit)
        .map_err(|_| "OpenCode 2 tail limit out of range".to_string())?;
    if limit == 0 {
        return Err("OpenCode 2 tail limit is zero".to_string().into());
    }
    let legacy = table_exists(connection, &mut page.cost, "message")
        .map_err(|e| sqlite_error(stop, "detect OpenCode 1 table", e))?;
    // Range over all types so a run of user rows also advances the tail.
    let mut statement = crate::read_io::prepare_cached(
        connection,
        "SELECT rowid,id,session_id,time_updated,data,seq,type FROM session_message
             WHERE rowid>?1 AND rowid<=?2 ORDER BY rowid LIMIT ?3",
    )
    .map_err(|e| sqlite_error(stop, "prepare OpenCode 2 tail", e))?;
    let mut rows = statement
        .query(params![page.start_rowid, page.max_rowid, limit])
        .map_err(|e| sqlite_error(stop, "query OpenCode 2 tail", e))?;
    let mut returned = 0;
    while let Some(row) = rows
        .next()
        .map_err(|e| sqlite_error(stop, "step OpenCode 2 tail", e))?
    {
        returned += 1;
        if returned == request.limit {
            page.through_rowid = row
                .get(0)
                .map_err(|e| sqlite_error(stop, "read OpenCode 2 rowid", e))?;
        }
        if row
            .get::<_, String>(6)
            .map_err(|e| sqlite_error(stop, "read OpenCode 2 type", e))?
            == "assistant"
        {
            take_row(connection, row, legacy, now_ms, stop, &mut page, sessions)?;
        }
    }
    drop(rows);
    page.cost.add_statement(&statement);
    if request
        .open_rowids
        .iter()
        .any(|&rowid| rowid <= page.start_rowid)
    {
        let mut lookup = crate::read_io::prepare_cached(connection,
            "SELECT rowid,id,session_id,time_updated,data,seq FROM session_message WHERE rowid=?1 AND type='assistant'",
        ).map_err(|e| sqlite_error(stop, "prepare OpenCode 2 pending lookup", e))?;
        let start = page.start_rowid;
        for rowid in request.open_rowids.iter().filter(|&&id| id <= start) {
            let mut rows = lookup
                .query([rowid])
                .map_err(|e| sqlite_error(stop, "query OpenCode 2 pending row", e))?;
            if let Some(row) = rows
                .next()
                .map_err(|e| sqlite_error(stop, "step OpenCode 2 pending row", e))?
            {
                take_row(connection, row, legacy, now_ms, stop, &mut page, sessions)?;
            }
        }
        page.cost.add_statement(&lookup);
    }
    Ok(Some(page))
}

fn take_row(
    connection: &Connection,
    row: &rusqlite::Row<'_>,
    legacy: bool,
    now_ms: i64,
    stop: &AtomicBool,
    page: &mut Page,
    sessions: &dyn Fn(&str) -> SessionPolicy,
) -> Result<(), OpenCodeReadError> {
    if stop.load(Ordering::Relaxed) {
        return Err(OpenCodeReadError::Stopped);
    }
    let rowid: i64 = row
        .get(0)
        .map_err(|e| sqlite_error(stop, "read OpenCode 2 rowid", e))?;
    let id: String = row
        .get(1)
        .map_err(|e| sqlite_error(stop, "read OpenCode 2 id", e))?;
    let decoded = (|| -> Result<Option<OpenCodeMessage>, String> {
        let session_id: String = row.get(2).map_err(|e| format!("read session: {e}"))?;
        let policy = sessions(&session_id);
        if let Some(through) = policy.imported_through_seq {
            let seq: i64 = row.get(5).map_err(|e| format!("read sequence: {e}"))?;
            if i128::from(seq) <= i128::from(through) {
                // Copied rows cannot be new spend, even if their data is
                // incomplete or malformed. Do not let them pin an open floor.
                page.skipped_imported += 1;
                return Ok(None);
            }
        }
        let updated_at_ms = row.get(3).map_err(|e| format!("read update time: {e}"))?;
        let mut bytes = row
            .get::<_, String>(4)
            .map_err(|e| format!("read data: {e}"))?
            .into_bytes();
        let message: Message =
            simd_json::from_slice(&mut bytes).map_err(|e| format!("decode data: {e}"))?;
        let completed_at_ms = message
            .time
            .as_ref()
            .and_then(|time| time.completed.as_ref())
            .and_then(serde_json::Value::as_i64)
            .filter(|time| *time >= 0);
        let completed = message
            .time
            .as_ref()
            .is_some_and(|time| time.completed.is_some());
        let quiet = now_ms.saturating_sub(updated_at_ms) >= QUIET_WINDOW;
        if !completed && !quiet && !policy.settled {
            page.open_rows.push(OpenCodeOpenRow {
                rowid,
                updated_at_ms,
            });
            return Ok(None);
        }
        let Some(tokens) = message.tokens else {
            return Ok(None);
        };
        // A retained v1 row owns this call, even if the migrated projection
        // has lost coordinates or normalized its finish spelling.
        if legacy
            && counted_query_row(
                connection,
                &mut page.cost,
                "SELECT EXISTS(SELECT 1 FROM message WHERE id=?1)",
                [&id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|e| format!("check legacy owner: {e}"))?
        {
            page.skipped_legacy += 1;
            return Ok(None);
        }
        let created = message
            .time
            .and_then(|time| time.created)
            .ok_or("usage lacks creation time")?;
        if !created.is_finite() || created < 0.0 || created > i64::MAX as f64 {
            return Err("usage creation time is out of range".into());
        }
        // The finite, nonnegative timestamp was checked against i64's range.
        #[allow(clippy::cast_possible_truncation)]
        let occurred_at_ms = created as i64;
        let model = message.model.ok_or("usage lacks provider/model")?;
        Ok(Some(OpenCodeMessage {
            id: id.clone(),
            session_id,
            updated_at_ms,
            occurred_at_ms,
            completed_at_ms,
            // V2 assistant data has no root. session_v2.directory/path may
            // move and may name a nested location, not the original worktree.
            project_root: None,
            provider_id: model.provider,
            model_id: model.id,
            agent: message.agent,
            // Only settlement has a producer finish. An aged interrupted call
            // carries no invented finish, even if an intermediate row had one.
            finish: if completed { message.finish } else { None },
            tokens: TokenBreakdown {
                input: tokens.input,
                output: tokens.output,
                reasoning: tokens.reasoning,
                cache_read: tokens.cache.as_ref().and_then(|cache| cache.read),
                cache_write: tokens.cache.as_ref().and_then(|cache| cache.write),
            },
        }))
    })();
    match decoded {
        Ok(Some(message)) => page.messages.push(message),
        Ok(None) => {}
        Err(reason) => page.set_aside.push(OpenCodeSetAside { id, reason }),
    }
    Ok(())
}
