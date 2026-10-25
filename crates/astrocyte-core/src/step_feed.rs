//! The live step feed behind `usage.steps.read` and `usage.steps.subscribe`.
//!
//! The feed is the `step_feed` table: one row per emission of a served cache
//! step, keyed by an `AUTOINCREMENT` feed cursor that grows in commit order
//! and is never reused. After each committed write, [`append_changes`]
//! recomputes the served rows of every session that gained or changed steps
//! and appends the rows that differ from the newest row stored for the same
//! `harness` and `step_ref`. So a step whose served fields change (a finish
//! reason that arrives late, or a verdict recomputed because an earlier step
//! of its session arrived late) is appended again at a new cursor, and the
//! newest row for an identity wins.
//!
//! The feed keeps the rows committed in the last 24 hours. A read from a
//! cursor older than that is refused `cursor_expired`, and the client reloads
//! per-session history with `usage.cache.steps`.

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::cache_diagnostics::{self as cache, CacheStep};

/// How long the feed keeps a row after it was committed, and how old a step
/// may be when it is committed and still enter the feed.
pub const FEED_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;
pub const READ_DEFAULT_LIMIT: u32 = 200;
pub const READ_MAX_LIMIT: u32 = 1_000;
/// Expired rows deleted per append, so one append never stalls the writer
/// on a large backlog of deletions; later appends delete the rest.
const PRUNE_BATCH: i64 = 1_000;

/// Where a feed row's step came from.
///
/// Every row today is `File`: a step read from a harness store or a runner's
/// durable log. Steps a runner announces on the bus (`step.finished.v1`) will
/// enter as `runner` rows once the bus grants astrocyte its `live` root; the
/// durable read of the same step then appends its `file` row, which wins as
/// the newer row for the identity. The table's `source` column already
/// accepts both values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedSource {
    File,
}

impl FeedSource {
    fn wire_name(self) -> &'static str {
        match self {
            Self::File => "file",
        }
    }
}

/// `usage.steps.read` params.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedReadRequest {
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// One `usage.steps.read` reply.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FeedPage {
    pub rows: Vec<Value>,
    pub next: Option<String>,
    pub through: String,
}

/// A refused read, with the detail `cursor_expired` carries.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedRefusal {
    pub code: &'static str,
    pub message: String,
    pub detail: Option<Value>,
}

#[derive(Debug)]
pub enum FeedError {
    Refused(FeedRefusal),
    Store(rusqlite::Error),
}

impl From<rusqlite::Error> for FeedError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

fn refused(code: &'static str, message: String) -> FeedError {
    FeedError::Refused(FeedRefusal {
        code,
        message,
        detail: None,
    })
}

/// The opaque string form of a feed cursor.
pub fn encode_cursor(cursor: i64) -> String {
    format!("c{cursor}")
}

/// The feed cursor a string names, or `None` when it is not one astrocyte
/// writes.
fn decode_cursor(text: &str) -> Option<i64> {
    let digits = text.strip_prefix('c')?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The newest feed cursor ever committed, including cursors retention has
/// deleted; 0 before the first row.
pub fn through(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='step_feed'),0)",
        [],
        |row| row.get(0),
    )
}

/// The served row of `step` without its cursor, as stored in `row_json`.
fn row_body(
    producer: &str,
    session: &str,
    source: FeedSource,
    step: &CacheStep,
) -> serde_json::Result<String> {
    let Value::Object(mut row) = serde_json::to_value(step)? else {
        unreachable!("a cache step serializes as an object");
    };
    // The cache cursor is the store's; the feed row carries its own.
    row.remove("cursor");
    row.insert("harness".into(), Value::from(producer));
    row.insert("source".into(), Value::from(source.wire_name()));
    row.insert("session".into(), Value::from(session));
    serde_json::to_string(&Value::Object(row))
}

/// The newest stored row body for an identity, if any.
fn newest_body(
    conn: &Connection,
    harness: &str,
    step_ref: &str,
) -> rusqlite::Result<Option<String>> {
    conn.prepare_cached(
        "SELECT row_json FROM step_feed WHERE harness=?1 AND step_ref=?2
         ORDER BY feed_cursor DESC LIMIT 1",
    )?
    .query_row(params![harness, step_ref], |row| row.get(0))
    .optional()
}

/// Bring the feed up to date with what has been committed. Must run after
/// the commit that recorded the steps, on the writer's connection, inside
/// one transaction or savepoint of its own: it reads the cache cursor the
/// feed has caught up to, appends rows for every changed step, and records
/// the new cursor. `late_keys` are the events that gained a `step_detail`
/// row after they were recorded. Returns how many rows were appended.
pub fn append_changes(
    conn: &Connection,
    now_ms: i64,
    late_keys: &[i64],
) -> rusqlite::Result<usize> {
    let caught_up: i64 = conn.query_row(
        "SELECT cache_high_water FROM step_feed_state WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    let newest = cache::last_event_key(conn)?;
    let window_start = now_ms.saturating_sub(FEED_WINDOW_MS);
    let mut appended = 0;
    for changed in cache::changed_sessions(conn, caught_up, newest, late_keys)? {
        let (producer, session) = (changed.producer.as_str(), changed.session.as_str());
        // A session whose steps had never read the cache before, and now do,
        // can change the verdict of every earlier step: recompute the whole
        // window. Otherwise only the changed steps and the ones after them
        // can change.
        let flipped = !cache::session_read_cache_through(conn, producer, session, caught_up)?
            && cache::session_reads_cache(conn, producer, session)?;
        let since = if flipped {
            window_start
        } else {
            changed.earliest_ms.max(window_start)
        };
        for step in cache::session_steps_since(conn, producer, session, since)? {
            let body = row_body(producer, session, FeedSource::File, &step)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            if newest_body(conn, producer, &step.step_ref)?.as_deref() == Some(body.as_str()) {
                continue;
            }
            conn.prepare_cached(
                "INSERT INTO step_feed(harness,step_ref,source,session,committed_at_ms,row_json)
                 VALUES (?1,?2,?3,?4,?5,?6)",
            )?
            .execute(params![
                producer,
                step.step_ref,
                FeedSource::File.wire_name(),
                session,
                now_ms,
                body
            ])?;
            appended += 1;
        }
    }
    conn.execute(
        "UPDATE step_feed_state SET cache_high_water=?1 WHERE id=1",
        [newest],
    )?;
    prune(conn, window_start)?;
    Ok(appended)
}

/// Delete up to `PRUNE_BATCH` rows committed before `window_start`, and
/// record the newest cursor deleted so reads from before it are refused.
fn prune(conn: &Connection, window_start: i64) -> rusqlite::Result<()> {
    let newest_expired: Option<i64> = conn.query_row(
        "SELECT MAX(feed_cursor) FROM (SELECT feed_cursor FROM step_feed
          WHERE committed_at_ms<?1 ORDER BY feed_cursor LIMIT ?2)",
        params![window_start, PRUNE_BATCH],
        |row| row.get(0),
    )?;
    let Some(newest_expired) = newest_expired else {
        return Ok(());
    };
    conn.execute(
        "DELETE FROM step_feed WHERE feed_cursor<=?1 AND committed_at_ms<?2",
        params![newest_expired, window_start],
    )?;
    conn.execute(
        "UPDATE step_feed_state SET pruned_through=MAX(pruned_through,?1) WHERE id=1",
        [newest_expired],
    )?;
    Ok(())
}

/// One `usage.steps.read` page, read in one snapshot. Superseded rows (an
/// identity with a newer row) are left out; the newer row is in this page or
/// a later one.
pub fn read(conn: &Connection, request: FeedReadRequest) -> Result<FeedPage, FeedError> {
    let limit = request.limit.unwrap_or(READ_DEFAULT_LIMIT);
    if limit == 0 {
        return Err(refused(
            "invalid_request",
            "limit must be at least 1".into(),
        ));
    }
    if limit > READ_MAX_LIMIT {
        return Err(refused(
            "range_too_large",
            format!("limit {limit} is above the maximum of {READ_MAX_LIMIT}"),
        ));
    }
    let through = through(conn)?;
    let pruned_through: i64 = conn.query_row(
        "SELECT pruned_through FROM step_feed_state WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    let after = match request.after.as_deref() {
        None => pruned_through,
        Some(text) => match decode_cursor(text) {
            Some(cursor) if cursor <= through => cursor,
            _ => {
                return Err(refused(
                    "invalid_request",
                    format!("after {text:?} is not a cursor astrocyte issued"),
                ))
            }
        },
    };
    if after < pruned_through {
        return Err(FeedError::Refused(FeedRefusal {
            code: "cursor_expired",
            message: "after is older than the rows the feed still holds; reload history with \
                      usage.cache.steps and read from through"
                .into(),
            detail: Some(serde_json::json!({ "through": encode_cursor(through) })),
        }));
    }
    let fetch = i64::from(limit) + 1;
    let mut statement = conn.prepare_cached(
        "SELECT f.feed_cursor,f.row_json FROM step_feed f
          WHERE f.feed_cursor>?1 AND f.feed_cursor<=?2
            AND NOT EXISTS (SELECT 1 FROM step_feed n WHERE n.harness=f.harness
                AND n.step_ref=f.step_ref AND n.feed_cursor>f.feed_cursor AND n.feed_cursor<=?2)
          ORDER BY f.feed_cursor LIMIT ?3",
    )?;
    let mut rows = statement
        .query_map(params![after, through, fetch], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
    rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    let next = if more {
        rows.last().map(|(cursor, _)| encode_cursor(*cursor))
    } else {
        None
    };
    let rows = rows
        .into_iter()
        .map(|(cursor, body)| {
            let mut row: Map<String, Value> = serde_json::from_str(&body).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            row.insert("cursor".into(), Value::from(encode_cursor(cursor)));
            Ok(Value::Object(row))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(FeedPage {
        rows,
        next,
        through: encode_cursor(through),
    })
}
