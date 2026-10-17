// Copied from tokscale v4.17.0 (tag commit
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7),
// crates/tokscale-core/src/sessions/goose.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
//
// ASTROCYTE DIVERGENCE (whole file): upstream turns each Goose session row
// into one `UnifiedMessage` for reporting. This copy keeps that unit (one
// row per session, from the `accumulated_*` columns), the `model_name`
// lookup in `model_config_json` and the `created_at` parse, and adds: a
// read-only snapshot opened with `mode=ro` and `query_only`, counters read
// strictly from their SQLite type (absent stays absent, a non-integer or
// negative value is counted, never zeroed), the cache-read and cache-write
// columns upstream does not read, the session's working directory and last
// update time. Dropped: the fallback from an absent accumulated counter to
// the last call's counter (one call's usage is not the session's), the
// reasoning "inferred" from `total - input - output` (a guess, and the
// measured total has no such gap), the skip of all-zero sessions (a
// reported zero is a measurement), the date-only `created_at` form (midnight
// would be an invented time), and provider inference from the model name.

//! Goose session reader.
//!
//! Reads `sessions` rows of Goose's SQLite store
//! (`$XDG_DATA_HOME/goose/sessions/sessions.db`, else
//! `~/.local/share/goose/sessions/sessions.db`), which Goose owns: the store
//! is opened read-only and never written, indexed or migrated.
//!
//! A [`GooseSession`] is ONE SESSION'S ACCUMULATED USAGE, NOT ONE CALL.
//! Goose keeps no per-call usage: `messages.tokens` is NULL in all 26
//! measured message rows, and only the `sessions` row carries running
//! totals (`accumulated_*_tokens`) beside the last call's counters. Those
//! totals grow while a session is live or resumed, so the caller must decide
//! when a session is settled enough to record.
//!
//! Token convention, measured 2026-09-24 on the development machine (1
//! session, too few to establish one): `accumulated_total_tokens ==
//! accumulated_input_tokens + accumulated_output_tokens` (122,726 is
//! 119,008 plus 3,718). `accumulated_cache_read_tokens` (43,520) is not added on top,
//! but whether it is inside the input count or outside the total altogether
//! is NOT established, so beside a non-zero cache read or cache write the
//! `input` role is underivable. No reasoning count exists; `output` is
//! stored as reported.

use super::normalize_workspace_key;
use super::roles::DerivedRole;
use rusqlite::types::Value;
use rusqlite::Row;
use serde::Deserialize;
use std::path::Path;

/// The `model_config_json` field this reader uses.
#[derive(Debug, Deserialize)]
struct GooseModelConfig {
    model_name: Option<String>,
}

/// `model_name` from `model_config_json`, or `None` when the column is
/// absent, not that JSON, or names no model. Never guessed from messages.
fn parse_model_config(json: Option<&str>) -> Option<String> {
    let config: GooseModelConfig = serde_json::from_str(json?).ok()?;
    let name = config.model_name?.trim().to_string();
    (!name.is_empty()).then_some(name)
}

/// A Goose timestamp column: RFC 3339, or SQLite's `CURRENT_TIMESTAMP`
/// form `YYYY-MM-DD HH:MM:SS` in UTC. `None` for anything else.
fn parse_timestamp_ms(text: Option<&str>) -> Option<i64> {
    let text = text?;
    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(text) {
        return Some(timestamp.timestamp_millis());
    }
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|timestamp| timestamp.and_utc().timestamp_millis())
}

/// Accumulated usage of one session in disjoint roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GooseSessionUsage {
    /// `accumulated_input_tokens`; underivable beside a non-zero cache read
    /// or cache write, whose place relative to it is unmeasured.
    pub input: DerivedRole,
    /// `accumulated_cache_read_tokens` verbatim.
    pub cache_read: Option<u64>,
    /// `accumulated_cache_write_tokens` verbatim.
    pub cache_write: Option<u64>,
    /// `accumulated_output_tokens` verbatim.
    pub output: Option<u64>,
}

/// One Goose session's accumulated usage. Not one call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GooseSession {
    /// `sessions.id` (for example `20260711_1`).
    pub session_id: String,
    /// `working_dir`, normalized.
    pub project_root: Option<String>,
    /// `provider_name` verbatim.
    pub provider: Option<String>,
    /// `model_config_json.model_name` verbatim.
    pub model: Option<String>,
    /// `created_at`; absent when it does not parse. Never a file mtime.
    pub created_at_ms: Option<i64>,
    /// `updated_at`: when Goose last changed the session. The caller's quiet
    /// window is measured from it.
    pub updated_at_ms: Option<i64>,
    pub usage: GooseSessionUsage,
}

/// Counts for one read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GooseReadCounts {
    /// `sessions` rows read.
    pub rows: u64,
    /// Rows with a counter that is not a non-negative integer (a real, a
    /// text, a negative). Not read further and not ingested.
    pub unreadable_rows: u64,
    /// Rows where every accumulated counter is NULL: no usage recorded.
    pub rows_without_usage: u64,
    /// Rows whose `input` is [`DerivedRole::Underivable`].
    pub underivable_roles: u64,
}

/// Result of one [`read_goose_sessions`] call.
#[derive(Debug, Default)]
pub struct GooseRead {
    /// Every session with usage, ordered by id.
    pub sessions: Vec<GooseSession>,
    pub counts: GooseReadCounts,
}

/// A counter column: `Ok(None)` for NULL, `Err` for anything that is not a
/// non-negative integer.
fn counter(row: &Row<'_>, column: &str) -> rusqlite::Result<Result<Option<u64>, ()>> {
    Ok(match row.get::<_, Value>(column)? {
        Value::Null => Ok(None),
        Value::Integer(value) => u64::try_from(value).map(Some).map_err(|_| ()),
        _ => Err(()),
    })
}

fn readonly_uri(path: &Path) -> Result<String, String> {
    let raw = path
        .to_str()
        .ok_or_else(|| format!("Goose database path is not UTF-8: {}", path.display()))?;
    Ok(crate::sqlite_uri::sqlite_readonly_uri(raw))
}

const SESSIONS_QUERY: &str = "SELECT id, working_dir, created_at, updated_at, provider_name,
        model_config_json, accumulated_input_tokens, accumulated_output_tokens,
        accumulated_cache_read_tokens, accumulated_cache_write_tokens
     FROM sessions ORDER BY id";

/// Reads every session row of the Goose store at `db_path` in one read-only
/// snapshot. `Err` when the store cannot be opened or queried (a missing
/// file included: the caller checks presence first).
pub fn read_goose_sessions(db_path: &Path) -> Result<GooseRead, String> {
    // ASTROCYTE DIVERGENCE: upstream's `sqlite_for_each_row` opens the store
    // with its default flags. Goose owns this store, so it is opened with
    // `mode=ro` and `query_only`, the same way the OpenCode reader opens
    // OpenCode's, and read inside one deferred transaction for a consistent
    // snapshot.
    let connection =
        super::sqlite_read::open_read_snapshot(readonly_uri(db_path)?, db_path, "Goose")?;
    let mut statement = connection
        .prepare(SESSIONS_QUERY)
        .map_err(|error| format!("prepare Goose sessions query: {error}"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| format!("query Goose sessions: {error}"))?;
    let mut read = GooseRead::default();
    loop {
        let row = match rows.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(error) => return Err(format!("read Goose session row: {error}")),
        };
        read_row(row, &mut read).map_err(|error| format!("read Goose session row: {error}"))?;
    }
    Ok(read)
}

fn read_row(row: &Row<'_>, read: &mut GooseRead) -> rusqlite::Result<()> {
    read.counts.rows += 1;
    let counters = [
        "accumulated_input_tokens",
        "accumulated_output_tokens",
        "accumulated_cache_read_tokens",
        "accumulated_cache_write_tokens",
    ]
    .map(|column| counter(row, column));
    let mut values = [None; 4];
    for (slot, value) in values.iter_mut().zip(counters) {
        match value? {
            Ok(value) => *slot = value,
            Err(()) => {
                read.counts.unreadable_rows += 1;
                return Ok(());
            }
        }
    }
    let [input, output, cache_read, cache_write] = values;
    if values.iter().all(Option::is_none) {
        read.counts.rows_without_usage += 1;
        return Ok(());
    }
    let text = |column: &str| -> rusqlite::Result<Option<String>> {
        Ok(match row.get::<_, Value>(column)? {
            Value::Text(text) => Some(text),
            _ => None,
        })
    };
    let Some(session_id) = text("id")?.filter(|id| !id.is_empty()) else {
        read.counts.unreadable_rows += 1;
        return Ok(());
    };
    let cache_unmeasured =
        cache_read.is_some_and(|read| read > 0) || cache_write.is_some_and(|write| write > 0);
    let usage = GooseSessionUsage {
        input: DerivedRole::verbatim(input).unless(cache_unmeasured),
        cache_read,
        cache_write,
        output,
    };
    read.counts.underivable_roles += u64::from(usage.input == DerivedRole::Underivable);
    read.sessions.push(GooseSession {
        session_id,
        project_root: text("working_dir")?
            .as_deref()
            .and_then(normalize_workspace_key),
        provider: text("provider_name")?.filter(|provider| !provider.trim().is_empty()),
        model: parse_model_config(text("model_config_json")?.as_deref()),
        created_at_ms: parse_timestamp_ms(text("created_at")?.as_deref()),
        updated_at_ms: parse_timestamp_ms(text("updated_at")?.as_deref()),
        usage,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    /// The `sessions` table as Goose (schema version 14) creates it.
    const SESSIONS_SCHEMA: &str = "CREATE TABLE sessions (
        id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '',
        user_set_name BOOLEAN DEFAULT FALSE, session_type TEXT NOT NULL DEFAULT 'user',
        working_dir TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, extension_data TEXT DEFAULT '{}',
        total_tokens INTEGER, input_tokens INTEGER, output_tokens INTEGER,
        cache_read_tokens INTEGER, cache_write_tokens INTEGER,
        accumulated_total_tokens INTEGER, accumulated_input_tokens INTEGER,
        accumulated_output_tokens INTEGER, accumulated_cache_read_tokens INTEGER,
        accumulated_cache_write_tokens INTEGER, accumulated_cost REAL, schedule_id TEXT,
        recipe_json TEXT, user_recipe_values_json TEXT, provider_name TEXT,
        model_config_json TEXT, goose_mode TEXT NOT NULL DEFAULT 'auto',
        archived_at TIMESTAMP, project_id TEXT)";

    fn store(rows: &[&str]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(SESSIONS_SCHEMA).unwrap();
        for values in rows {
            connection
                .execute_batch(&format!(
                    "INSERT INTO sessions (id,working_dir,created_at,updated_at,provider_name,
                     model_config_json,accumulated_total_tokens,accumulated_input_tokens,
                     accumulated_output_tokens,accumulated_cache_read_tokens,
                     accumulated_cache_write_tokens) VALUES ({values})"
                ))
                .unwrap();
        }
        (dir, path)
    }

    const CONFIG: &str = r#"'{"model_name":"gpt-5.5","context_limit":1050000}'"#;

    #[test]
    fn a_session_row_is_its_accumulated_usage() {
        let (_dir, path) = store(&[&format!(
            "'20260711_1','/Users/u/','2026-07-11 14:17:30','2026-07-11 14:54:30','chatgpt_codex',{CONFIG},122726,119008,3718,0,NULL"
        )]);
        let read = read_goose_sessions(&path).unwrap();
        assert_eq!(read.sessions.len(), 1);
        let session = &read.sessions[0];
        assert_eq!(session.session_id, "20260711_1");
        assert_eq!(session.project_root.as_deref(), Some("/Users/u"));
        assert_eq!(session.provider.as_deref(), Some("chatgpt_codex"));
        assert_eq!(session.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(session.created_at_ms, Some(1_783_779_450_000));
        assert_eq!(session.updated_at_ms, Some(1_783_781_670_000));
        assert_eq!(
            session.usage,
            GooseSessionUsage {
                input: DerivedRole::Measured(119008),
                cache_read: Some(0),
                cache_write: None,
                output: Some(3718),
            }
        );
    }

    /// A NULL counter is omitted, never zero; a session with no counters at
    /// all is counted, not emitted; an unusable model config leaves the
    /// model absent.
    #[test]
    fn absent_counters_are_omitted_not_zero() {
        let (_dir, path) = store(&[
            "'a','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,'not json',NULL,10,NULL,NULL,NULL",
            "'b','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,NULL,NULL,NULL,NULL,NULL,NULL",
        ]);
        let read = read_goose_sessions(&path).unwrap();
        assert_eq!(read.sessions.len(), 1);
        assert_eq!(read.counts.rows_without_usage, 1);
        let session = &read.sessions[0];
        assert_eq!(session.model, None);
        assert_eq!(session.provider, None);
        assert_eq!(session.usage.output, None);
        assert_eq!(session.usage.cache_read, None);
        assert_eq!(session.usage.input, DerivedRole::Measured(10));
    }

    /// A counter that is not a non-negative integer is counted and its row
    /// not ingested, never read as zero. (A numeric text such as `'12'` is
    /// stored as an integer by the column's INTEGER affinity, so the text
    /// case uses a non-numeric one.)
    #[test]
    fn a_malformed_counter_is_counted_not_zeroed() {
        let (_dir, path) = store(&[
            "'a','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,NULL,NULL,-5,1,NULL,NULL",
            "'b','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,NULL,NULL,1.5,1,NULL,NULL",
            "'c','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,NULL,NULL,'12 tokens',1,NULL,NULL",
            "'d','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,NULL,NULL,7,1,NULL,NULL",
        ]);
        let read = read_goose_sessions(&path).unwrap();
        assert_eq!(read.counts.unreadable_rows, 3);
        assert_eq!(read.sessions.len(), 1);
        assert_eq!(read.sessions[0].session_id, "d");
    }

    /// Where a cache read or write sits relative to the input count is not
    /// measured, so beside a non-zero one the input role is underivable.
    #[test]
    fn a_non_zero_cache_counter_leaves_input_underivable() {
        let (_dir, path) = store(&[
            "'a','/w','2026-07-11 14:17:30','2026-07-11 14:54:30',NULL,NULL,122726,119008,3718,43520,NULL",
        ]);
        let read = read_goose_sessions(&path).unwrap();
        let usage = read.sessions[0].usage;
        assert_eq!(usage.input, DerivedRole::Underivable);
        assert_eq!(usage.cache_read, Some(43520));
        assert_eq!(read.counts.underivable_roles, 1);
    }

    /// Reading leaves the store byte-for-byte unchanged, and a timestamp in
    /// neither accepted form is absent rather than invented.
    #[test]
    fn reading_never_writes_the_store_and_odd_times_are_absent() {
        let (_dir, path) =
            store(&["'a','/w','2026-07-11','not a time',NULL,NULL,NULL,1,1,NULL,NULL"]);
        let before = std::fs::read(&path).unwrap();
        let read = read_goose_sessions(&path).unwrap();
        assert_eq!(read.sessions[0].created_at_ms, None);
        assert_eq!(read.sessions[0].updated_at_ms, None);
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
