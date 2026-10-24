//! OpenCode 2 records each v2-only call once. A recorded call is final: later
//! changes to its source row are ignored, not compared or quarantined. Retained
//! v1 rows remain the v1 reader's responsibility, including migrated copies.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_core::fact::{event_id, ChargeBasis};
use astrocyte_core::ingest::{EventRecordRequest, RecordDisposition, StepDetail};
use astrocyte_readers::sessions::opencode::{
    OpenCodeCursor, OpenCodeMessage, OpenCodeReadError, OpenCodeScanCost, OpenCodeTailRequest,
};
use astrocyte_readers::sessions::opencode_v2::{self, Page};
use rusqlite::params;

use super::harness_drain::{self, HarnessSource};
use super::{SetAsideRecord, SourceDrainError};
use crate::state::SharedState;

/// Separate update-time cursor and health, but the existing `opencode` config
/// key and ledger producer govern both generations.
pub(crate) const SOURCE_KEY: &str = "opencode-v2";
const FULL_INTERVAL: i64 = 60 * 60 * 1_000;
const OPEN_CAP: usize = 4_096;
const PAGE_LIMIT: usize = 2_000;

#[derive(Default)]
struct Cache {
    last_full: Option<i64>,
    backlog: bool,
    front: Option<String>,
    tail: Option<i64>,
    open: BTreeMap<i64, i64>,
}

#[derive(Debug, Default)]
pub(crate) struct Outcome {
    pub recorded: usize,
    pub skipped_legacy: usize,
    pub skipped_recorded: usize,
    pub absent: bool,
    pub full: bool,
    pub cost: OpenCodeScanCost,
}

pub(crate) struct OpenCodeV2Source {
    root: Result<PathBuf, String>,
    cache: Mutex<Cache>,
}

pub(crate) fn registered() -> OpenCodeV2Source {
    #[cfg(any(test, feature = "test-support"))]
    let root = Ok(std::env::temp_dir().join(format!(
        "astrocyte-test-without-opencode-v2-{}/opencode.db",
        std::process::id()
    )));
    #[cfg(not(any(test, feature = "test-support")))]
    let root =
        crate::harness_paths::opencode(&cortexkit_paths::SystemEnv).map_err(|e| e.to_string());
    OpenCodeV2Source {
        root,
        cache: Mutex::new(Cache::default()),
    }
}

impl OpenCodeV2Source {
    #[cfg(test)]
    fn at(path: impl Into<PathBuf>) -> Self {
        Self {
            root: Ok(path.into()),
            cache: Mutex::new(Cache::default()),
        }
    }

    pub(crate) fn drain(
        &self,
        state: &SharedState,
        now: i64,
        stop: &AtomicBool,
    ) -> Result<Outcome, SourceDrainError> {
        if stop.load(Ordering::Relaxed) {
            return Err(SourceDrainError::Stopped);
        }
        let path = self
            .root
            .as_ref()
            .map_err(|reason| SourceDrainError::Failed(reason.clone()))?;
        match std::fs::metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return self.absent(state)
            }
            Err(error) => return Err(format!("inspect OpenCode 2: {error}").into()),
            Ok(_) => {}
        }
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        let mut full = cache.backlog
            || cache
                .last_full
                .is_none_or(|last| now < last || now.saturating_sub(last) >= FULL_INTERVAL);
        let pending = cache.open.keys().copied().collect::<Vec<_>>();
        let mut extra_cost = OpenCodeScanCost::default();
        let page = if full {
            self.full_page(state, path, now, stop)?
        } else {
            let page = opencode_v2::read_tail(
                path,
                &OpenCodeTailRequest {
                    after_rowid: cache.tail,
                    seed_rows: 2_000,
                    open_rowids: &pending,
                    limit: PAGE_LIMIT,
                },
                now,
                stop,
            )
            .map_err(read_error)?;
            match page {
                Some(page) if page.front_id != cache.front || page.max_rowid < page.start_rowid => {
                    // A replaced/front-pruned store invalidates rowids. Commit
                    // none of the suspect tail and resolve the durable lineage.
                    extra_cost = page.cost;
                    cache.tail = None;
                    cache.open.clear();
                    full = true;
                    self.full_page(state, path, now, stop)?
                }
                page => page,
            }
        };
        let Some(page) = page else {
            *cache = Cache::default();
            return self.absent(state);
        };
        harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
        let outcome = commit(state, &page, full, now, stop)?;
        if full {
            if cache.front != page.front_id {
                cache.tail = None;
                cache.open.clear();
            }
            cache.front.clone_from(&page.front_id);
            cache.backlog = page.consumed < page.high_water;
            if !cache.backlog {
                cache.last_full = Some(now);
            }
        } else {
            cache.tail = Some(page.through_rowid);
            for rowid in pending {
                cache.open.remove(&rowid);
            }
        }
        for row in &page.open_rows {
            cache.open.insert(row.rowid, row.updated_at_ms);
        }
        // Evicted pending rows are rediscovered by the next full pass, even
        // below its durable cursor. They can never pin that cursor forever.
        while cache.open.len() > OPEN_CAP {
            cache.open.pop_first();
        }
        let mut outcome = outcome;
        outcome.cost.add_assign_saturating(&extra_cost);
        Ok(outcome)
    }

    fn absent(&self, state: &SharedState) -> Result<Outcome, SourceDrainError> {
        harness_drain::remove_runtime_row(state, SOURCE_KEY)?;
        Ok(Outcome {
            absent: true,
            ..Outcome::default()
        })
    }

    fn full_page(
        &self,
        state: &SharedState,
        path: &std::path::Path,
        now: i64,
        stop: &AtomicBool,
    ) -> Result<Option<Page>, SourceDrainError> {
        let cursor = state.with_conn(|conn| {
            conn.query_row(
                "SELECT lineage,consumed_ordinal FROM source_runtime WHERE source_key=?1",
                [SOURCE_KEY],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                    ))
                },
            )
            .optional()
            .map(|row| {
                row.and_then(|(lineage, ordinal)| {
                    Some(OpenCodeCursor {
                        lineage: lineage?,
                        ordinal: ordinal?,
                    })
                })
            })
            .map_err(|e| e.to_string())
        })?;
        opencode_v2::read_page(path, cursor.as_ref(), PAGE_LIMIT, now, stop).map_err(read_error)
    }
}

use rusqlite::OptionalExtension;

impl HarnessSource for OpenCodeV2Source {
    fn source_key(&self) -> &'static str {
        SOURCE_KEY
    }
    fn drain_for_health(
        &self,
        state: &SharedState,
        now: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, now, stop)?;
        tracing::debug!(target: "drain", recorded = outcome.recorded,
            skipped_legacy = outcome.skipped_legacy, skipped_recorded = outcome.skipped_recorded,
            absent = outcome.absent, full = outcome.full, vm_steps = outcome.cost.vm_steps,
            "OpenCode 2 drain");
        Ok(None)
    }
}

fn read_error(error: OpenCodeReadError) -> SourceDrainError {
    match error {
        OpenCodeReadError::Stopped => SourceDrainError::Stopped,
        OpenCodeReadError::Failed(reason) => SourceDrainError::Failed(reason),
    }
}

fn commit(
    state: &SharedState,
    page: &Page,
    full: bool,
    now: i64,
    stop: &AtomicBool,
) -> Result<Outcome, SourceDrainError> {
    let mut stopped = false;
    let result = state.ordered_write(|tx| {
        let mut outcome = Outcome {
            full,
            cost: page.cost,
            skipped_legacy: page.skipped_legacy,
            ..Outcome::default()
        };
        let mut evidence = page
            .set_aside
            .iter()
            .map(|row| SetAsideRecord {
                locator: format!("session_message:{}", row.id),
                reason: row.reason.clone(),
            })
            .collect::<Vec<_>>();
        for message in &page.messages {
            if stop.load(Ordering::Relaxed) {
                stopped = true;
                return Err("OpenCode 2 ingest stopped".into());
            }
            let id = event_id("opencode", None, &message.id)
                .map_err(|e| format!("derive OpenCode identity: {e:?}"))?;
            let already_recorded: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM usage_event WHERE event_id=?1)",
                    [&id],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if already_recorded {
                outcome.skipped_recorded += 1;
                continue;
            }
            match map_message(message.clone()) {
                Ok(request) => {
                    let result = astrocyte_core::ingest::record_event_in_tx(
                        tx,
                        &request,
                        now,
                        astrocyte_core::ingest::DeliveryPath::Read,
                    )
                    .map_err(|e| format!("record OpenCode 2: {e:?}"))?;
                    if result.disposition == RecordDisposition::Recorded {
                        outcome.recorded += 1;
                    }
                }
                Err(reason) => evidence.push(SetAsideRecord {
                    locator: format!("session_message:{}", message.id),
                    reason,
                }),
            }
        }
        if stop.load(Ordering::Relaxed) {
            stopped = true;
            return Err("OpenCode 2 ingest stopped".into());
        }
        let newly_seen = harness_drain::record_set_aside_in_tx(tx, SOURCE_KEY, &evidence, now)?;
        if full {
            tx.execute(
                "UPDATE source_runtime SET lineage=?2,consumed_ordinal=?3,
                high_water_ordinal=?4,last_success_ms=?5,pending_known=0,pending_unknown=?6
                WHERE source_key=?1",
                params![
                    SOURCE_KEY,
                    page.lineage,
                    page.consumed,
                    page.high_water,
                    now,
                    i64::from(page.consumed < page.high_water)
                ],
            )
            .map_err(|e| e.to_string())?;
        } else {
            tx.execute(
                "UPDATE source_runtime SET last_success_ms=?2 WHERE source_key=?1",
                params![SOURCE_KEY, now],
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(((outcome, newly_seen), true))
    });
    match result {
        Ok((outcome, newly_seen)) => {
            harness_drain::warn_set_aside(SOURCE_KEY, &newly_seen);
            Ok(outcome)
        }
        Err(_) if stopped => Err(SourceDrainError::Stopped),
        Err(reason) => Err(SourceDrainError::Failed(reason)),
    }
}

fn map_message(message: OpenCodeMessage) -> Result<EventRecordRequest, String> {
    let measurements = [
        ("input", "usage.input", message.tokens.input),
        ("output", "usage.output", message.tokens.output),
        ("cache_read", "usage.cacheRead", message.tokens.cache_read),
        (
            "cache_write",
            "usage.cacheWrite",
            message.tokens.cache_write,
        ),
        // OpenCode output already excludes reasoning; never subtract it again.
        ("reasoning", "usage.reasoning", message.tokens.reasoning),
    ]
    .into_iter()
    .filter_map(|(role, source, quantity)| quantity.map(|q| (role, source, q)))
    .map(|(role, source, q)| harness_drain::measurement(role, source, Some(q)))
    .collect::<Result<Vec<_>, _>>()?;
    Ok(EventRecordRequest {
        producer: "opencode".into(),
        producer_lineage: None,
        producer_ref: message.id,
        prior_event_ref: None,
        harness: None,
        project_root: message.project_root,
        session: Some(message.session_id),
        run_ref: None,
        flow_ref: None,
        work_class: message.agent,
        occurred_at_ms: Some(message.occurred_at_ms),
        emitted_at_ms: None,
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider: Some(message.provider_id),
        model: Some(message.model_id),
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        charge_basis: ChargeBasis::Unknown,
        measurements,
        step_detail: StepDetail {
            finish: message.finish,
            context_limit: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_state;
    use astrocyte_readers::sessions::opencode_v2::QUIET_WINDOW;
    use rusqlite::Connection;
    use serde_json::{json, Value};
    use tempfile::TempDir;

    static STOP: AtomicBool = AtomicBool::new(false);

    fn database() -> (TempDir, PathBuf, Connection) {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE session_v2(id TEXT PRIMARY KEY,directory TEXT,path TEXT);
            INSERT INTO session_v2 VALUES ('session','/work/subdir','/work');
            CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                type TEXT NOT NULL,seq INTEGER NOT NULL,time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,data TEXT NOT NULL);
            CREATE TABLE message(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,time_updated INTEGER NOT NULL,data TEXT NOT NULL);",
        )
        .unwrap();
        (temp, path, conn)
    }

    fn payload(created: i64, completed: bool) -> Value {
        let mut value = json!({
            "id":"ignored-embedded-id", "type":"assistant", "agent":"\u{200b}build",
            "model":{"providerID":"anthropic","id":"claude-sonnet-4","variant":"default"},
            "time":{"created":created}, "finish":"stop",
            "tokens":{"input":3,"output":13,"reasoning":17,"cache":{"read":5,"write":7}}
        });
        if completed {
            value["time"]["completed"] = json!(created + 1);
        }
        value
    }

    fn insert(conn: &Connection, id: &str, updated: i64, data: &Value) {
        conn.execute(
            "INSERT INTO session_message VALUES (?1,'session','assistant',
            (SELECT COALESCE(MAX(seq),0)+1 FROM session_message),?2,?2,?3)",
            params![id, updated, data.to_string()],
        )
        .unwrap();
    }

    fn count(state: &SharedState, table: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    fn quantity(state: &SharedState, role: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT SUM(quantity_units) FROM measurement WHERE role=?1",
                    [role],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    fn snapshot(state: &SharedState) -> Vec<Vec<rusqlite::types::Value>> {
        state
            .with_conn(|conn| {
                let mut result = Vec::new();
                for table in ["usage_event", "measurement", "step_detail"] {
                    let mut stmt = conn
                        .prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))
                        .unwrap();
                    let columns = stmt.column_count();
                    let rows = stmt
                        .query_map([], |row| {
                            (0..columns)
                                .map(|i| row.get(i))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        })
                        .unwrap();
                    result.extend(rows.map(Result::unwrap));
                }
                Ok(result)
            })
            .unwrap()
    }

    fn runtime(state: &SharedState) -> (String, i64, i64) {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,high_water_ordinal
            FROM source_runtime WHERE source_key='opencode-v2'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    #[test]
    fn completed_failed_and_missing_tokens_preserve_v2_buckets() {
        let (_temp, path, conn) = database();
        insert(&conn, "completed", 10, &payload(10, true));
        let mut failed = payload(20, true);
        failed["finish"] = json!("error");
        insert(&conn, "failed", 20, &failed);
        failed.as_object_mut().unwrap().remove("tokens");
        insert(&conn, "failed-no-tokens", 30, &failed);
        let mut no_tokens = payload(40, true);
        no_tokens.as_object_mut().unwrap().remove("tokens");
        insert(&conn, "no-tokens", 40, &no_tokens);
        let mut no_finish = payload(50, true);
        no_finish["finish"] = Value::Null;
        insert(&conn, "null-finish", 50, &no_finish);
        let state = test_state();
        let result = OpenCodeV2Source::at(path)
            .drain(&state, 100, &STOP)
            .unwrap();
        assert_eq!(result.recorded, 3);
        assert_eq!(count(&state, "usage_event"), 3);
        assert_eq!(quantity(&state, "input"), 9);
        assert_eq!(quantity(&state, "output"), 39);
        assert_eq!(quantity(&state, "reasoning"), 51);
        assert_eq!(quantity(&state, "cache_read"), 15);
        assert_eq!(quantity(&state, "cache_write"), 21);
        state
            .with_conn(|conn| {
                let rooted: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM usage_event WHERE project_root IS NOT NULL",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(rooted, 0);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            super::super::harness_drain::step_details(&state),
            vec![
                ("completed".into(), Some("stop".into()), None),
                ("failed".into(), Some("error".into()), None),
            ]
        );
        state.with_conn(|conn| {
            let row: (String, String, String, String, Option<String>) = conn.query_row(
                "SELECT session,work_class,provider,model,producer_lineage FROM usage_event LIMIT 1", [],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
            assert_eq!(row, ("session".into(),"\u{200b}build".into(),"anthropic".into(),"claude-sonnet-4".into(),None));
            Ok(())
        }).unwrap();
    }

    #[test]
    fn streamed_or_finish_is_not_v2_completion() {
        let (_temp, path, conn) = database();
        let mut data = payload(10, false);
        data["time"]["streamed"] = json!(11);
        insert(&conn, "streaming", 10, &data);
        let state = test_state();
        let source = OpenCodeV2Source::at(&path);
        assert_eq!(source.drain(&state, 100, &STOP).unwrap().recorded, 0);
        assert_eq!(count(&state, "usage_event"), 0);
        data["time"]["completed"] = json!(110);
        data["tokens"]["output"] = json!(29);
        conn.execute(
            "UPDATE session_message SET data=?1,time_updated=110 WHERE id='streaming'",
            [data.to_string()],
        )
        .unwrap();
        assert_eq!(source.drain(&state, 120, &STOP).unwrap().recorded, 1);
        assert_eq!(quantity(&state, "output"), 29);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
    }

    #[test]
    fn v2_quiet_window_crosses_between_drains() {
        let (_temp, path, conn) = database();
        let mut data = payload(10, false);
        data["finish"] = Value::Null;
        insert(&conn, "interrupted", 10, &data);
        let state = test_state();
        let source = OpenCodeV2Source::at(path);
        assert_eq!(
            source
                .drain(&state, 10 + QUIET_WINDOW - 1, &STOP)
                .unwrap()
                .recorded,
            0
        );
        assert_eq!(count(&state, "usage_event"), 0);
        assert_eq!(
            source
                .drain(&state, 10 + QUIET_WINDOW, &STOP)
                .unwrap()
                .recorded,
            1
        );
        assert_eq!(quantity(&state, "output"), 13);
        assert!(super::super::harness_drain::step_details(&state).is_empty());
    }

    #[test]
    fn v2_malformed_finish_preserves_usage() {
        let (_temp, path, conn) = database();
        let mut data = payload(10, true);
        data["finish"] = json!({"reason":"stop"});
        insert(&conn, "malformed-finish", 10, &data);
        let state = test_state();
        assert_eq!(
            OpenCodeV2Source::at(path)
                .drain(&state, 100, &STOP)
                .unwrap()
                .recorded,
            1
        );
        assert_eq!(quantity(&state, "output"), 13);
        assert!(super::super::harness_drain::step_details(&state).is_empty());
    }

    #[test]
    fn v2_completion_at_the_cursor_timestamp_is_ingested() {
        let (_temp, path, conn) = database();
        let mut data = payload(10, false);
        data["finish"] = Value::Null;
        insert(&conn, "pending", 10, &data);
        insert(&conn, "done", 20, &payload(20, true));
        let state = test_state();
        assert_eq!(
            OpenCodeV2Source::at(&path)
                .drain(&state, 100, &STOP)
                .unwrap()
                .recorded,
            1
        );
        data["time"]["completed"] = json!(20);
        conn.execute(
            "UPDATE session_message SET time_updated=20,data=?1 WHERE id='pending'",
            [data.to_string()],
        )
        .unwrap();
        assert_eq!(
            OpenCodeV2Source::at(path)
                .drain(&state, 110, &STOP)
                .unwrap()
                .recorded,
            1
        );
        assert_eq!(count(&state, "usage_event"), 2);
    }

    #[test]
    fn v2_interrupted_completion_is_ignored_with_a_counted_skip() {
        let (_temp, path, conn) = database();
        insert(&conn, "interrupted", 10, &payload(10, false));
        let state = test_state();
        let source = OpenCodeV2Source::at(path);
        assert_eq!(
            source
                .drain(&state, 10 + QUIET_WINDOW, &STOP)
                .unwrap()
                .recorded,
            1
        );
        let before = snapshot(&state);
        let mut data = payload(10, true);
        data["tokens"]["output"] = json!(1000);
        conn.execute(
            "UPDATE session_message SET data=?1,time_updated=?2",
            params![data.to_string(), QUIET_WINDOW + 20],
        )
        .unwrap();
        let outcome = source.drain(&state, QUIET_WINDOW + 30, &STOP).unwrap();
        assert_eq!(outcome.skipped_recorded, 1);
        assert_eq!(outcome.recorded, 0);
        assert_eq!(count(&state, "usage_event"), 1);
        assert_eq!(snapshot(&state), before);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
        assert!(super::super::harness_drain::step_details(&state).is_empty());
    }

    fn legacy(conn: &Connection) {
        let data = json!({"role":"assistant","agent":"\u{200b}build",
            "providerID":"anthropic","modelID":"claude-sonnet-4","path":{"root":"/original/root"},
            "time":{"created":9,"completed":11},"finish":"stop",
            "tokens":{"input":3,"output":13,"reasoning":17,"cache":{"read":5,"write":7}}});
        conn.execute(
            "INSERT INTO message VALUES ('migrated','session',10,11,?1)",
            [data.to_string()],
        )
        .unwrap();
    }

    #[test]
    fn v2_migrated_copy_with_v1_owner_produces_no_event_or_quarantine() {
        let (_temp, path, conn) = database();
        legacy(&conn);
        insert(&conn, "migrated", 11, &payload(10, true));
        let state = test_state();
        let result = OpenCodeV2Source::at(&path)
            .drain(&state, 100, &STOP)
            .unwrap();
        assert_eq!(result.recorded, 0);
        assert_eq!(result.skipped_legacy, 1);
        assert_eq!(count(&state, "usage_event"), 0);
        super::super::opencode_source::OpenCodeSource::at(path)
            .drain(&state, 100, &STOP)
            .unwrap();
        assert_eq!(count(&state, "usage_event"), 1);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
    }

    #[test]
    fn v2_migrated_copy_after_v1_deletion_keeps_ledger_bytes() {
        let (_temp, path, conn) = database();
        legacy(&conn);
        let state = test_state();
        super::super::opencode_source::OpenCodeSource::at(&path)
            .drain(&state, 100, &STOP)
            .unwrap();
        let before = snapshot(&state);
        conn.execute("DELETE FROM message", []).unwrap();
        insert(&conn, "migrated", 11, &payload(10, true));
        let outcome = OpenCodeV2Source::at(path)
            .drain(&state, 200, &STOP)
            .unwrap();
        assert_eq!(outcome.skipped_recorded, 1);
        assert_eq!(outcome.recorded, 0);
        assert_eq!(snapshot(&state), before);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
    }

    #[test]
    fn v2_cursor_and_pending_rows_survive_restarts() {
        let (_temp, path, conn) = database();
        let mut pending = payload(10, false);
        pending["finish"] = Value::Null;
        insert(&conn, "pending", 10, &pending);
        insert(&conn, "done", 50, &payload(50, true));
        let state = test_state();
        super::super::harness_drain::ensure_runtime_row(&state, "opencode").unwrap();
        assert_eq!(
            OpenCodeV2Source::at(&path)
                .drain(&state, 100, &STOP)
                .unwrap()
                .recorded,
            1
        );
        assert_eq!(runtime(&state).1, 50);
        let restarted = OpenCodeV2Source::at(&path);
        let result = restarted.drain(&state, 10 + QUIET_WINDOW, &STOP).unwrap();
        assert_eq!(result.recorded, 1);
        assert_eq!(result.skipped_recorded, 1);
        assert_eq!(count(&state, "usage_event"), 2);
        assert_eq!(runtime(&state).1, 50);
        let before = runtime(&state);
        restarted.drain(&state, 20 + QUIET_WINDOW, &STOP).unwrap();
        assert_eq!(runtime(&state), before);
        let result = OpenCodeV2Source::at(path)
            .drain(&state, 30 + QUIET_WINDOW, &STOP)
            .unwrap();
        assert_eq!(result.recorded, 0);
        assert_eq!(result.skipped_recorded, 2);
        state
            .with_conn(|conn| {
                let cursor: Option<i64> = conn
                    .query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='opencode'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(cursor, None);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn v2_marker_config_and_health_keep_the_v1_path_unchanged() {
        let (_temp, path, conn) = database();
        legacy(&conn);
        insert(&conn, "new", 10, &payload(10, true));
        conn.execute("DROP TABLE session_v2", []).unwrap();
        let source = OpenCodeV2Source::at(&path);
        let state = test_state();
        let initial_runtime_rows = count(&state, "source_runtime");
        assert!(source.drain(&state, 100, &STOP).unwrap().absent);
        assert_eq!(count(&state, "source_runtime"), initial_runtime_rows);
        assert_eq!(count(&state, "usage_event"), 0);
        super::super::opencode_source::OpenCodeSource::at(&path)
            .drain(&state, 100, &STOP)
            .unwrap();
        assert_eq!(count(&state, "usage_event"), 1);
        conn.execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY)", [])
            .unwrap();
        let mut config = state.desired_config();
        config.harness_ingest.exclude = vec!["opencode".into()];
        state.publish_desired(config, 1);
        state.mark_producer_degraded(SOURCE_KEY, "old failure".into());
        super::super::run_iteration_with_harness(&state, &[], &source, &STOP);
        assert_eq!(count(&state, "usage_event"), 1);
        assert!(state.producer_degraded_reason(SOURCE_KEY).is_none());
        let mut config = state.desired_config();
        config.harness_ingest.exclude.clear();
        state.publish_desired(config, 2);
        super::super::run_iteration_with_harness(&state, &[], &source, &STOP);
        assert_eq!(count(&state, "usage_event"), 2);
        assert!(state.producer_degraded_reason(SOURCE_KEY).is_none());
        conn.execute("DROP TABLE session_message", []).unwrap();
        let restarted = OpenCodeV2Source::at(&path);
        super::super::run_iteration_with_harness(&state, &[], &restarted, &STOP);
        assert!(state.producer_degraded_reason(SOURCE_KEY).is_some());
        conn.execute("DROP TABLE session_v2", []).unwrap();
        super::super::run_iteration_with_harness(&state, &[], &restarted, &STOP);
        assert!(state.producer_degraded_reason(SOURCE_KEY).is_none());
        assert_eq!(count(&state, "source_runtime"), initial_runtime_rows + 1);
    }

    #[test]
    fn v2_set_aside_advances_full_and_tail_past_unreadable_rows() {
        let (_temp, path, conn) = database();
        insert(&conn, "good", 10, &payload(10, true));
        insert(&conn, "malformed", 20, &payload(20, true));
        conn.execute(
            "UPDATE session_message SET data='{bad' WHERE id='malformed'",
            [],
        )
        .unwrap();
        insert(&conn, "overflow", 30, &payload(30, true));
        let mut overflow = payload(30, true);
        overflow["tokens"]["input"] = json!(1_u64 << 63);
        conn.execute(
            "UPDATE session_message SET data=?1 WHERE id='overflow'",
            [overflow.to_string()],
        )
        .unwrap();
        let state = test_state();
        let source = OpenCodeV2Source::at(path);
        assert_eq!(source.drain(&state, 100, &STOP).unwrap().recorded, 1);
        assert_eq!(runtime(&state).1, 30);
        insert(&conn, "blob", 40, &payload(40, true));
        conn.execute(
            "UPDATE session_message SET data=CAST(data AS BLOB) WHERE id='blob'",
            [],
        )
        .unwrap();
        insert(&conn, "tail-good", 50, &payload(50, true));
        assert_eq!(source.drain(&state, 110, &STOP).unwrap().recorded, 1);
        assert_eq!(count(&state, "usage_event"), 2);
        super::super::harness_drain::assert_set_aside(&state, SOURCE_KEY, 3);
        assert_eq!(runtime(&state).1, 30);
        assert_eq!(source.drain(&state, 120, &STOP).unwrap().recorded, 0);
        super::super::harness_drain::assert_set_aside(&state, SOURCE_KEY, 3);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
    }

    #[test]
    fn v2_only_calls_reach_served_cache_steps_and_sessions_once() {
        let (_temp, path, conn) = database();
        let now = crate::state::now_ms();
        insert(&conn, "completed", now, &payload(now, true));
        insert(
            &conn,
            "interrupted",
            now - QUIET_WINDOW - 1,
            &payload(now, false),
        );
        let state = test_state();
        let source = OpenCodeV2Source::at(path);
        super::super::run_iteration_with_harness(&state, &[], &source, &STOP);
        let request = json!({"harness":"opencode","session":"session"});
        let steps =
            crate::ops::dispatch(&state, crate::ops::OP_CACHE_STEPS, request.clone()).unwrap();
        let rows = steps["steps"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        let completed = rows
            .iter()
            .find(|row| row["step_ref"] == "completed")
            .unwrap();
        assert_eq!(completed["finish"], "stop");
        let interrupted = rows
            .iter()
            .find(|row| row["step_ref"] == "interrupted")
            .unwrap();
        assert!(interrupted["finish"].is_null());
        let sessions = crate::ops::dispatch(
            &state,
            crate::ops::OP_CACHE_SESSIONS,
            json!({"harness":"opencode","since_ms":now - 60_000}),
        )
        .unwrap();
        let sessions = sessions["sessions"].as_array().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["session"], "session");
        assert_eq!(sessions[0]["steps"], 2);
        state.rebuild_recent_steps();
        assert_eq!(
            crate::ops::dispatch(&state, crate::ops::OP_CACHE_STEPS, request).unwrap(),
            steps
        );
        let before = snapshot(&state);
        let outcome = source.drain(&state, now + 1, &STOP).unwrap();
        assert_eq!(outcome.recorded, 0);
        assert_eq!(outcome.skipped_recorded, 2);
        assert_eq!(snapshot(&state), before);
    }

    #[test]
    fn v2_idle_vm_work_is_constant_and_tail_never_moves_the_cursor() {
        let measure = |history: i64| {
            let (_temp, path, mut conn) = database();
            let tx = conn.transaction().unwrap();
            tx.execute(
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<?1)
                INSERT INTO session_message SELECT 'user-'||x,'session','user',x,x,x,'{}' FROM n",
                [history],
            )
            .unwrap();
            tx.commit().unwrap();
            let state = test_state();
            let source = OpenCodeV2Source::at(path);
            let mut now = 100_000;
            loop {
                source.drain(&state, now, &STOP).unwrap();
                now += 1;
                let (_, consumed, high) = runtime(&state);
                if consumed == high {
                    break;
                }
                assert!(now < 100_100);
            }
            source.drain(&state, now, &STOP).unwrap();
            let before = runtime(&state);
            let idle = source.drain(&state, now + 1, &STOP).unwrap();
            assert!(!idle.full);
            // sqlite_master probes scan four schema rows, not message history.
            assert_eq!(idle.cost.fullscan_steps, 4);
            assert!(idle.cost.vm_steps < 500);
            assert_eq!(runtime(&state), before);
            insert(
                &conn,
                "new-assistant",
                history + 1,
                &payload(history + 1, true),
            );
            let new = source.drain(&state, now + 2, &STOP).unwrap();
            assert_eq!(new.recorded, 1);
            assert_eq!(new.cost.fullscan_steps, idle.cost.fullscan_steps);
            assert!(new.cost.vm_steps > idle.cost.vm_steps);
            assert!(new.cost.vm_steps < idle.cost.vm_steps + 150);
            assert_eq!(runtime(&state), before);
            println!(
                "history={history}: idle VM steps={}, new-row VM steps={}",
                idle.cost.vm_steps, new.cost.vm_steps
            );
            idle.cost.vm_steps
        };
        assert_eq!(measure(100), measure(20_000));
    }
}
