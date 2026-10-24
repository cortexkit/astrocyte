// Copied from tokscale v4.17.0,
// crates/tokscale-core/src/sessions/opencode.rs at
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
// ASTROCYTE DIVERGENCE: only the SQLite path is retained. OpenCode archives
// legacy storage/message JSON files on this host, and Astrocyte needs a durable
// database cursor rather than tokscale's process-local JSON/SQLite merge cache.

//! OpenCode SQLite session reader.

use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::TokenBreakdown;

pub use super::opencode_schema::{
    OpenCodeCursor, OpenCodeOpenRow, OpenCodePage, OpenCodeReadError, OpenCodeScanCost,
    OpenCodeTailPage, OpenCodeTailReader, OpenCodeTailRequest,
};

/// A usage-bearing assistant message read from OpenCode's `message` table.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenCodeMessage {
    pub id: String,
    pub session_id: String,
    pub updated_at_ms: i64,
    pub occurred_at_ms: i64,
    pub project_root: Option<String>,
    pub provider_id: String,
    pub model_id: String,
    pub agent: Option<String>,
    /// `data.finish` verbatim, when it is a string. Kept for cache
    /// diagnostics only; it is not part of the ledger event.
    pub finish: Option<String>,
    pub tokens: TokenBreakdown,
}

/// Population totals used to reconcile the SQL predicate, reader, and ledger.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenCodeTotals {
    pub records: u64,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub reasoning: u64,
}

impl OpenCodeTotals {
    pub(crate) fn add_tokens(&mut self, tokens: &TokenBreakdown) {
        self.records = self.records.saturating_add(1);
        self.input = self.input.saturating_add(tokens.input.unwrap_or(0));
        self.output = self.output.saturating_add(tokens.output.unwrap_or(0));
        self.cache_read = self
            .cache_read
            .saturating_add(tokens.cache_read.unwrap_or(0));
        self.cache_write = self
            .cache_write
            .saturating_add(tokens.cache_write.unwrap_or(0));
        self.reasoning = self.reasoning.saturating_add(tokens.reasoning.unwrap_or(0));
    }

    pub fn add_assign_saturating(&mut self, other: &Self) {
        self.records = self.records.saturating_add(other.records);
        self.input = self.input.saturating_add(other.input);
        self.output = self.output.saturating_add(other.output);
        self.cache_read = self.cache_read.saturating_add(other.cache_read);
        self.cache_write = self.cache_write.saturating_add(other.cache_write);
        self.reasoning = self.reasoning.saturating_add(other.reasoning);
    }
}

/// Read one resumable page. The URI is opened with `mode=ro`; it deliberately
/// does not use SQLite's `immutable=1`, because a running OpenCode keeps recent
/// commits in the WAL.
///
/// Setting `stop` aborts the read, including a statement already running, and
/// returns `OpenCodeReadError::Stopped`.
pub fn read_opencode_page(
    db_path: &Path,
    since: Option<&OpenCodeCursor>,
    limit: usize,
    stop: &AtomicBool,
) -> Result<OpenCodePage, OpenCodeReadError> {
    super::opencode_schema::read_page(db_path, since, limit, stop)
}

/// Read the newest rows and re-read known open rows by rowid, at a cost that
/// does not grow with the size of the `message` table. Opened like
/// `read_opencode_page`, and `stop` aborts it the same way.
pub fn read_opencode_tail(
    db_path: &Path,
    request: &OpenCodeTailRequest<'_>,
    stop: &AtomicBool,
) -> Result<OpenCodeTailPage, OpenCodeReadError> {
    super::opencode_schema::read_tail(db_path, request, stop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    fn database() -> (TempDir, std::path::PathBuf, Connection) {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE message (
                     id TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL,
                     time_created INTEGER NOT NULL,
                     time_updated INTEGER NOT NULL,
                     data TEXT NOT NULL
                 );
                 CREATE TABLE session (
                     id TEXT PRIMARY KEY,
                     directory TEXT NOT NULL,
                     tokens_output INTEGER NOT NULL
                 );",
            )
            .unwrap();
        (temp, path, connection)
    }

    // The reader ingests only messages whose tokens are final, so without
    // `finish` and `time.completed` this fixture would be an in-flight turn.
    fn payload(agent: &str, root: &str, output: u64, reasoning: u64) -> String {
        serde_json::json!({
            "role": "assistant",
            "agent": agent,
            "modelID": "claude-sonnet-4",
            "providerID": "anthropic",
            "path": {"root": root},
            "time": {"created": 1_700_000_000_001_i64, "completed": 1_700_000_000_002_i64},
            "finish": "stop",
            "tokens": {
                "input": 3,
                "output": output,
                "reasoning": reasoning,
                "cache": {"read": 5, "write": 7}
            }
        })
        .to_string()
    }

    fn insert_message(connection: &Connection, data: &str) {
        connection
            .execute(
                "INSERT INTO message (id,session_id,time_created,time_updated,data)
                 VALUES ('msg-1','session-1',10,20,?1)",
                [data],
            )
            .unwrap();
    }

    #[test]
    fn opencode_reasoning_is_emitted_as_given_instead_of_subtracted_from_output() {
        let (_temp, path, connection) = database();
        insert_message(&connection, &payload("build", "/payload/root", 4, 9));
        drop(connection);

        let page = read_opencode_page(&path, None, 100, &NEVER_STOP).unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].tokens.output, Some(4));
        assert_eq!(page.messages[0].tokens.reasoning, Some(9));
    }

    #[test]
    fn zero_width_space_agent_round_trips_byte_identical() {
        let (_temp, path, connection) = database();
        let agent = "\u{200b}Alfonso - CTO";
        insert_message(&connection, &payload(agent, "/payload/root", 4, 9));
        drop(connection);

        let page = read_opencode_page(&path, None, 100, &NEVER_STOP).unwrap();
        let stored = page.messages[0].agent.as_deref().unwrap();
        assert_eq!(stored.as_bytes(), agent.as_bytes());
    }

    #[test]
    fn project_root_comes_from_message_path_root_not_session_directory() {
        let (_temp, path, connection) = database();
        connection
            .execute(
                "INSERT INTO session (id,directory,tokens_output)
                 VALUES ('session-1','/wrong/session/directory',999999)",
                [],
            )
            .unwrap();
        insert_message(
            &connection,
            &payload("build", "/producer/declared/root", 4, 9),
        );
        drop(connection);

        let page = read_opencode_page(&path, None, 100, &NEVER_STOP).unwrap();
        assert_eq!(
            page.messages[0].project_root.as_deref(),
            Some("/producer/declared/root")
        );
    }

    #[test]
    fn session_token_rollups_are_never_read() {
        let (_temp, path, connection) = database();
        connection
            .execute(
                "INSERT INTO session (id,directory,tokens_output)
                 VALUES ('session-1','/wrong/session/directory',999999)",
                [],
            )
            .unwrap();
        insert_message(&connection, &payload("build", "/payload/root", 7, 11));
        drop(connection);

        let page = read_opencode_page(&path, None, 100, &NEVER_STOP).unwrap();
        assert_eq!(page.raw.output, 7);
        assert_eq!(page.emitted.output, 7);
        assert_eq!(page.messages[0].tokens.output, Some(7));
    }

    #[test]
    fn readonly_uri_observes_committed_wal_rows() {
        let (_temp, path, connection) = database();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        insert_message(&connection, &payload("build", "/payload/root", 7, 11));

        let page = read_opencode_page(&path, None, 100, &NEVER_STOP).unwrap();
        assert_eq!(page.messages.len(), 1);
    }
}
