//! Goose's session store producer and ledger ingest mapping.
//!
//! Reads Goose's platform-specific session database read-only through
//! `astrocyte_readers::sessions::goose`. Goose owns that store; nothing here
//! writes, indexes or migrates it.
//!
//! A Goose event is ONE SESSION'S ACCUMULATED USAGE, NOT ONE CALL: Goose
//! keeps no per-call usage. A session is recorded once its own `updated_at`
//! is at least [`GOOSE_QUIET_WINDOW_MS`] old on the drain's clock, so a live
//! session's growing totals are not recorded mid-flight. A session recorded
//! after its quiet window that later grows (Goose resumed it) is re-emitted
//! with the new totals; the ledger then quarantines that redelivery. The
//! quarantine is the correct and visible outcome: it is a real change in
//! what Goose claims about the session, and it must never be overwritten or
//! re-summed into a second event.

use std::collections::HashMap;
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
#[cfg(any(test, feature = "test-support"))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_readers::sessions::goose::{read_goose_sessions, GooseReadCounts, GooseSession};

use crate::refresher::harness_drain::{
    derived_measurement, ensure_runtime_row, health_reason, record_requests,
    record_source_discovery, remove_runtime_row, verbatim_measurement, HarnessSource,
};
use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const GOOSE_SOURCE_KEY: &str = "goose";

/// How long a session's `updated_at` must be in the past before its
/// accumulated usage is recorded. A day: Goose sessions are interactive, and
/// one resumed within a day of its last change is still being used.
pub(crate) const GOOSE_QUIET_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GooseDrainOutcome {
    /// `sessions.db` does not exist: no Goose lane.
    pub root_absent: bool,
    /// The store was queried this drain, including successful idle reads.
    pub store_read: bool,
    pub counts: GooseReadCounts,
    /// Sessions changed within the quiet window: not recorded yet.
    pub sessions_live: usize,
    /// Sessions without a readable `updated_at`, whose quietness cannot be
    /// judged. Not recorded; none measured.
    pub sessions_without_update_time: usize,
    pub sessions_emitted: usize,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
}

impl GooseDrainOutcome {
    /// Usage seen and not recorded for a reason other than waiting out the
    /// quiet window.
    pub fn usage_not_ingested(&self) -> u64 {
        self.counts.unreadable_rows + self.sessions_without_update_time as u64
    }
}

#[derive(Debug, Clone)]
enum GooseStore {
    Path(PathBuf),
    Unresolved(String),
}

#[derive(Debug, Clone, Default)]
struct GooseCache {
    /// What was last committed for each session, so a session re-read
    /// unchanged is not sent again. That matters for a quarantined session:
    /// every re-send of its changed totals would add another rejected
    /// delivery row.
    sent: HashMap<String, EventRecordRequest>,
}

/// A process-local view over Goose's session store.
pub(crate) struct GooseSource {
    store: GooseStore,
    quiet_window_ms: i64,
    cache: Mutex<GooseCache>,
}

impl GooseSource {
    #[cfg_attr(
        any(test, feature = "test-support"),
        expect(
            dead_code,
            reason = "tests inject an environment instead of reading process variables"
        )
    )]
    pub(crate) fn from_environment() -> Self {
        Self::from_env(&cortexkit_paths::SystemEnv)
    }

    pub(crate) fn from_env(env: &impl cortexkit_paths::EnvSource) -> Self {
        Self {
            store: match crate::harness_paths::goose(env) {
                Ok(path) => GooseStore::Path(path),
                Err(error) => GooseStore::Unresolved(error.to_string()),
            },
            quiet_window_ms: GOOSE_QUIET_WINDOW_MS,
            cache: Mutex::new(GooseCache::default()),
        }
    }

    /// XDG-only fixture layout keeps existing drain/health tests independent of
    /// the host platform. Production discovery uses `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>, xdg_data_home: Option<OsString>) -> Self {
        let configured = xdg_data_home.filter(|value| !value.to_string_lossy().trim().is_empty());
        let data = match (configured, home) {
            (Some(dir), _) if Path::new(&dir).is_absolute() => Ok(PathBuf::from(dir)),
            (Some(dir), _) => Err(format!(
                "XDG_DATA_HOME is relative ({dir:?}); refusing to resolve the Goose store"
            )),
            (None, Some(home)) if Path::new(&home).is_absolute() => {
                Ok(PathBuf::from(home).join(".local").join("share"))
            }
            (None, Some(home)) => Err(format!(
                "HOME is relative ({home:?}); refusing to resolve the Goose store"
            )),
            (None, None) => Err("HOME is unset; cannot resolve the Goose store".to_string()),
        };
        Self {
            store: match data {
                Ok(data) => GooseStore::Path(data.join("goose/sessions/sessions.db")),
                Err(reason) => GooseStore::Unresolved(reason),
            },
            quiet_window_ms: GOOSE_QUIET_WINDOW_MS,
            cache: Mutex::new(GooseCache::default()),
        }
    }

    /// A source over the store file at `db_path`.
    #[cfg(test)]
    pub(crate) fn at(db_path: impl Into<PathBuf>) -> Self {
        Self {
            store: GooseStore::Path(db_path.into()),
            quiet_window_ms: GOOSE_QUIET_WINDOW_MS,
            cache: Mutex::new(GooseCache::default()),
        }
    }

    /// Read the store and commit every session past its quiet window.
    ///
    /// The store is read on every drain, never skipped on an unchanged
    /// length or mtime: file metadata is not a reliable change signal, and
    /// the store is small enough that reading it costs nothing. Every drain
    /// also commits, even with nothing to record, because that commit is
    /// what marks the source current for health. Setting `stop` abandons the drain and
    /// returns `SourceDrainError::Stopped` having committed nothing.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<GooseDrainOutcome, SourceDrainError> {
        let db_path = match &self.store {
            GooseStore::Path(path) => path,
            GooseStore::Unresolved(reason) => {
                ensure_runtime_row(state, GOOSE_SOURCE_KEY)?;
                return Err(reason.clone().into());
            }
        };
        match std::fs::metadata(db_path) {
            Ok(_) => {
                record_source_discovery(state, GOOSE_SOURCE_KEY, db_path.display(), true);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                record_source_discovery(state, GOOSE_SOURCE_KEY, db_path.display(), false);
                remove_runtime_row(state, GOOSE_SOURCE_KEY)?;
                return Ok(GooseDrainOutcome {
                    root_absent: true,
                    ..GooseDrainOutcome::default()
                });
            }
            Err(error) => {
                ensure_runtime_row(state, GOOSE_SOURCE_KEY)?;
                return Err(format!("inspect Goose store {}: {error}", db_path.display()).into());
            }
        }
        ensure_runtime_row(state, GOOSE_SOURCE_KEY)?;

        let mut outcome = GooseDrainOutcome::default();
        if stop.load(Ordering::Relaxed) {
            return Err(SourceDrainError::Stopped);
        }

        let mut sent = self
            .cache
            .lock()
            .map_err(|_| "Goose cache lock poisoned".to_string())?
            .sent
            .clone();
        let read = read_goose_sessions(db_path)?;
        outcome.store_read = true;
        outcome.counts = read.counts;
        let mut requests = Vec::new();
        let mut sent_now = Vec::new();
        for session in read.sessions {
            match session.updated_at_ms {
                None => outcome.sessions_without_update_time += 1,
                Some(updated) if recorded_at_ms.saturating_sub(updated) < self.quiet_window_ms => {
                    outcome.sessions_live += 1;
                }
                Some(_) => {
                    let request = map_session(session)?;
                    if sent.get(&request.producer_ref) != Some(&request) {
                        sent_now.push((request.producer_ref.clone(), request.clone()));
                        requests.push(request);
                    }
                }
            }
        }
        outcome.sessions_emitted = requests.len();

        let tally = record_requests(state, GOOSE_SOURCE_KEY, &requests, recorded_at_ms, &|| {
            stop.load(Ordering::Relaxed)
        })?;
        outcome.rows_recorded = tally.recorded;
        outcome.exact_redeliveries = tally.exact_redeliveries;
        outcome.quarantined_redeliveries = tally.quarantined_redeliveries;

        *self
            .cache
            .lock()
            .map_err(|_| "Goose cache lock poisoned".to_string())? = GooseCache {
            sent: {
                sent.extend(sent_now);
                sent
            },
        };
        Ok(outcome)
    }
}

impl HarnessSource for GooseSource {
    fn source_key(&self) -> &'static str {
        GOOSE_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok(health_reason(
            "Goose",
            outcome.quarantined_redeliveries,
            outcome.usage_not_ingested(),
        ))
    }
}

fn map_session(session: GooseSession) -> Result<EventRecordRequest, String> {
    let usage = session.usage;
    // These quantities are ONE SESSION'S ACCUMULATED USAGE, not one call's.
    // The input quantity is the sum of every call's input, not a request's
    // context size, so nothing may select an `.above_context` price tier
    // from it. That is safe today only because the event carries
    // `charge_basis = unknown` and so never prices; anyone who later
    // declares a charge basis for Goose must price it without tier
    // selection.
    let mut measurements = Vec::with_capacity(4);
    measurements.extend(derived_measurement(
        "input",
        "sessions.accumulated_input_tokens",
        usage.input,
    )?);
    measurements.extend(verbatim_measurement(
        "cache_read",
        "sessions.accumulated_cache_read_tokens",
        usage.cache_read,
    )?);
    measurements.extend(verbatim_measurement(
        "cache_write",
        "sessions.accumulated_cache_write_tokens",
        usage.cache_write,
    )?);
    measurements.extend(verbatim_measurement(
        "output",
        "sessions.accumulated_output_tokens",
        usage.output,
    )?);

    Ok(EventRecordRequest {
        producer: GOOSE_SOURCE_KEY.to_string(),
        producer_lineage: Some(session.session_id.clone()),
        // One event per session: the session id is the whole identity.
        producer_ref: session.session_id.clone(),
        prior_event_ref: None,
        harness: None,
        project_root: session.project_root,
        session: Some(session.session_id),
        run_ref: None,
        work_class: None,
        // The session's start, as upstream dates it.
        occurred_at_ms: session.created_at_ms,
        emitted_at_ms: None,
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider: session.provider,
        model: session.model,
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        // No column states how Goose authenticated; `provider_name` (for
        // example `chatgpt_codex`) names a provider integration, not who
        // paid. See the tier note above before changing this.
        charge_basis: ChargeBasis::Unknown,
        measurements,
    })
}

// The SQLite reconciliation fixtures keep all token counts within i64.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::run_iteration_with_harness;
    use crate::state::test_state;
    use rusqlite::Connection;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);
    /// 2026-07-11 14:54:30 UTC, the fixture session's `updated_at`.
    const UPDATED_MS: i64 = 1_783_781_670_000;
    const QUIET: i64 = UPDATED_MS + GOOSE_QUIET_WINDOW_MS;

    const SCHEMA: &str = "CREATE TABLE sessions (
        id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', working_dir TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_tokens INTEGER, input_tokens INTEGER, output_tokens INTEGER,
        cache_read_tokens INTEGER, cache_write_tokens INTEGER,
        accumulated_total_tokens INTEGER, accumulated_input_tokens INTEGER,
        accumulated_output_tokens INTEGER, accumulated_cache_read_tokens INTEGER,
        accumulated_cache_write_tokens INTEGER, accumulated_cost REAL,
        provider_name TEXT, model_config_json TEXT)";

    fn store(dir: &Path) -> PathBuf {
        let path = dir.join("goose/sessions/sessions.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection
            .execute_batch(
                r#"INSERT INTO sessions (id,working_dir,created_at,updated_at,provider_name,
                   model_config_json,input_tokens,output_tokens,accumulated_total_tokens,
                   accumulated_input_tokens,accumulated_output_tokens,
                   accumulated_cache_read_tokens)
                   VALUES ('20260711_1','/Users/u','2026-07-11 14:17:30','2026-07-11 14:54:30',
                   'chatgpt_codex','{"model_name":"gpt-5.5"}',12964,316,122726,119008,3718,0)"#,
            )
            .unwrap();
        path
    }

    /// Goose resumes a session: its totals grow and `updated_at` moves.
    fn grow(path: &Path, updated_at: &str) {
        Connection::open(path)
            .unwrap()
            .execute(
                "UPDATE sessions SET accumulated_input_tokens=accumulated_input_tokens+100,
                 accumulated_output_tokens=accumulated_output_tokens+10, updated_at=?1",
                [updated_at],
            )
            .unwrap();
    }

    fn drain(source: &GooseSource, state: &SharedState, at: i64) -> GooseDrainOutcome {
        source.drain(state, at, &NEVER_STOP).unwrap()
    }

    fn count(state: &SharedState, sql: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(sql, [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn quantities(state: &SharedState) -> BTreeMap<String, Option<i64>> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT measurement.role,measurement.quantity_units
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='goose'",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(|error| error.to_string())?
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map_err(|error| error.to_string());
                rows
            })
            .unwrap()
    }

    /// A session still inside its quiet window is not recorded; once quiet
    /// it becomes one event carrying its accumulated usage, dated at its
    /// start, with the session's model, provider and working directory.
    #[test]
    fn a_session_is_one_event_once_its_quiet_window_has_passed() {
        let temp = TempDir::new().unwrap();
        let path = store(temp.path());
        let state = test_state();
        let source = GooseSource::at(&path);
        let live = drain(&source, &state, QUIET - 1);
        assert_eq!((live.sessions_live, live.rows_recorded), (1, 0));
        let quiet = drain(&source, &state, QUIET);
        assert_eq!((quiet.sessions_emitted, quiet.rows_recorded), (1, 1));
        assert_eq!(
            quantities(&state),
            BTreeMap::from([
                ("cache_read".to_string(), Some(0)),
                ("input".to_string(), Some(119008)),
                ("output".to_string(), Some(3718)),
            ])
        );
        let row: (String, String, String, String, String, i64, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT producer_lineage,producer_ref,provider,model,project_root,
                            occurred_at_ms,charge_basis FROM usage_event WHERE producer='goose'",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    },
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            row,
            (
                "20260711_1".to_string(),
                "20260711_1".to_string(),
                "chatgpt_codex".to_string(),
                "gpt-5.5".to_string(),
                "/Users/u".to_string(),
                1_783_779_450_000,
                "unknown".to_string()
            )
        );
    }

    /// Every drain reads the store and marks the source current, even when
    /// nothing changed; a re-read writes no rows and no redelivery rows.
    /// Skipping an unchanged store once left `last_success_ms` behind and
    /// turned module health degraded.
    #[test]
    fn every_drain_marks_the_source_current_and_re_reads_write_no_rows() {
        let temp = TempDir::new().unwrap();
        let path = store(temp.path());
        let state = test_state();
        let source = GooseSource::at(&path);
        drain(&source, &state, QUIET);
        let unchanged = drain(&source, &state, QUIET + 1);
        assert!(unchanged.store_read);
        assert_eq!(
            (unchanged.rows_recorded, unchanged.sessions_emitted),
            (0, 0)
        );
        assert_eq!(
            count(
                &state,
                "SELECT last_success_ms FROM source_runtime WHERE source_key='goose'"
            ),
            QUIET + 1
        );
        let fresh = drain(&GooseSource::at(&path), &state, QUIET + 2);
        assert_eq!((fresh.rows_recorded, fresh.exact_redeliveries), (0, 1));
        assert_eq!(count(&state, "SELECT COUNT(*) FROM redelivery_log"), 0);
    }

    #[test]
    fn failed_read_keeps_committed_sent_claims_without_repeating_rejections() {
        let temp = TempDir::new().unwrap();
        let path = store(temp.path());
        let state = test_state();
        let source = GooseSource::at(&path);
        assert_eq!(drain(&source, &state, QUIET).rows_recorded, 1);
        grow(&path, "2026-07-11 14:54:30");
        assert_eq!(
            drain(&source, &state, QUIET + 1).quarantined_redeliveries,
            1
        );
        let stored = quantities(&state);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("ALTER TABLE sessions RENAME TO unavailable_sessions")
            .unwrap();
        assert!(source.drain(&state, QUIET + 2, &NEVER_STOP).is_err());
        connection
            .execute_batch("ALTER TABLE unavailable_sessions RENAME TO sessions")
            .unwrap();
        drop(connection);
        let recovered = drain(&source, &state, QUIET + 3);
        assert_eq!(
            recovered.sessions_emitted, 0,
            "failed read must not forget a committed claim"
        );
        assert_eq!(count(&state, "SELECT COUNT(*) FROM rejected_delivery"), 1);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='goose'"
            ),
            1
        );
        assert_eq!(quantities(&state), stored);
    }

    /// A session recorded after its quiet window that Goose later resumes
    /// changes what Goose claims about it. The re-emission is quarantined:
    /// one stored event, one quarantine, never a second stored or re-summed
    /// event, and the source's health shows it.
    #[test]
    fn a_session_that_grows_after_being_recorded_is_quarantined_not_re_summed() {
        let temp = TempDir::new().unwrap();
        let path = store(temp.path());
        let state = test_state();
        let source = GooseSource::at(&path);
        assert_eq!(drain(&source, &state, QUIET).rows_recorded, 1);

        grow(&path, "2026-07-20 10:00:00");
        let resumed_ms = 1_784_541_600_000;
        let still_live = drain(&source, &state, resumed_ms + 1);
        assert_eq!((still_live.sessions_live, still_live.rows_recorded), (1, 0));
        assert_eq!(still_live.quarantined_redeliveries, 0);

        let grown = source
            .drain_for_health(&state, resumed_ms + GOOSE_QUIET_WINDOW_MS, &NEVER_STOP)
            .unwrap();
        assert!(
            grown
                .as_deref()
                .is_some_and(|reason| reason.contains("1 Goose deliveries were quarantined")),
            "{grown:?}"
        );
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='goose'"
            ),
            1
        );
        assert_eq!(count(&state, "SELECT COUNT(*) FROM rejected_delivery"), 1);
        // Re-reading the grown store later does not re-send it.
        let later = drain(&source, &state, resumed_ms + GOOSE_QUIET_WINDOW_MS + 1);
        assert!(later.store_read);
        assert_eq!(later.sessions_emitted, 0);
        assert_eq!(count(&state, "SELECT COUNT(*) FROM rejected_delivery"), 1);
        assert_eq!(
            quantities(&state)["input"],
            Some(119008),
            "the stored event keeps the first claim"
        );
    }

    /// A malformed counter is counted and surfaced, never stored as zero.
    #[test]
    fn a_malformed_counter_is_counted_and_surfaced() {
        let temp = TempDir::new().unwrap();
        let path = store(temp.path());
        Connection::open(&path)
            .unwrap()
            .execute_batch("UPDATE sessions SET accumulated_output_tokens=-3")
            .unwrap();
        let state = test_state();
        let reason = GooseSource::at(&path)
            .drain_for_health(&state, QUIET, &NEVER_STOP)
            .unwrap();
        assert!(
            reason
                .as_deref()
                .is_some_and(|reason| reason.contains("1 usage records were not ingested")),
            "{reason:?}"
        );
        assert_eq!(count(&state, "SELECT COUNT(*) FROM usage_event"), 0);
    }

    #[test]
    fn absent_store_is_no_lane_but_an_unreadable_store_is_cold() {
        let temp = TempDir::new().unwrap();
        let state = test_state();
        let absent = GooseSource::from_home(Some(temp.path().as_os_str().to_owned()), None);
        assert!(drain(&absent, &state, 0).root_absent);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='goose'"
            ),
            0
        );
        let db = temp.path().join(".local/share/goose/sessions/sessions.db");
        std::fs::create_dir_all(&db).unwrap();
        let error = absent
            .drain(&state, 1, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Goose database"), "{error}");
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime
                 WHERE source_key='goose' AND last_success_ms IS NULL AND pending_unknown=1"
            ),
            1
        );
        let xdg = GooseSource::from_home(None, Some(temp.path().as_os_str().to_owned()));
        store(temp.path());
        assert_eq!(drain(&xdg, &state, QUIET).rows_recorded, 1);
        let relative = GooseSource::from_home(Some(OsString::from("relative")), None);
        assert!(relative.drain(&state, 3, &NEVER_STOP).is_err());
    }

    #[test]
    fn refresher_iteration_drains_goose_and_clears_its_health() {
        let temp = TempDir::new().unwrap();
        let path = store(temp.path());
        let state = test_state();
        // The refresher's clock is the real one, long after the fixture.
        run_iteration_with_harness(&state, &[], &GooseSource::at(&path), &NEVER_STOP);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='goose'"
            ),
            1
        );
        assert!(state.producer_degraded_reason("goose").is_none());
    }

    /// Run with `--ignored`. Reads the live store read-only (checked: its
    /// bytes are unchanged afterwards) into a scratch ledger and reconciles
    /// a raw SQL sum, the reader's sessions and the ledger's rows.
    #[test]
    #[ignore = "reads the mutable ~/.local/share/goose/sessions/sessions.db store"]
    fn live_goose_store_reconciles_raw_reader_and_ledger() {
        let path = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap()).join(".local/share")
            })
            .join("goose/sessions/sessions.db");
        let before = std::fs::read(&path).unwrap();
        let raw: [i64; 6] = Connection::open_with_flags(
            format!("file:{}?mode=ro", path.display()),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap()
        .query_row(
            "SELECT COUNT(*), TOTAL(accumulated_input_tokens), TOTAL(accumulated_output_tokens),
                    TOTAL(accumulated_cache_read_tokens), TOTAL(accumulated_cache_write_tokens),
                    SUM(accumulated_total_tokens = accumulated_input_tokens + accumulated_output_tokens)
             FROM sessions WHERE accumulated_input_tokens IS NOT NULL",
            [],
            |row| {
                Ok([
                    row.get(0)?,
                    row.get::<_, f64>(1)? as i64,
                    row.get::<_, f64>(2)? as i64,
                    row.get::<_, f64>(3)? as i64,
                    row.get::<_, f64>(4)? as i64,
                    row.get(5)?,
                ])
            },
        )
        .unwrap();

        let read = read_goose_sessions(&path).unwrap();
        let reader = read.sessions.iter().fold([0_i64; 5], |mut sum, session| {
            sum[0] += 1;
            // Underivable input carries no quantity; the raw value is
            // recovered only for the reconciliation below.
            sum[2] += session.usage.output.unwrap_or(0) as i64;
            sum[3] += session.usage.cache_read.unwrap_or(0) as i64;
            sum[4] += session.usage.cache_write.unwrap_or(0) as i64;
            sum
        });

        let state = test_state();
        let outcome = drain(&GooseSource::at(&path), &state, 4_000_000_000_000);
        let stored = quantities(&state);
        eprintln!("raw [sessions,input,output,cache_read,cache_write,total_identity]: {raw:?}");
        eprintln!(
            "reader counts {:?} sessions {:?}",
            read.counts, read.sessions
        );
        eprintln!("ledger: {stored:?} outcome {outcome:?}");

        // Never a vacuous pass.
        assert!(raw[0] > 0 && raw[1] > 0 && raw[2] > 0);
        assert_eq!(raw[5], raw[0], "accumulated total = input + output");
        assert_eq!(read.counts.unreadable_rows, 0);
        assert_eq!(
            [reader[0], reader[2], reader[3], reader[4]],
            [raw[0], raw[2], raw[3], raw[4]]
        );
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='goose'"
            ),
            raw[0]
        );
        assert_eq!(stored.get("output").copied().flatten(), Some(raw[2]));
        // Input is stored as a quantity only when no cache counter is
        // reported beside it; otherwise it is present without a quantity.
        let input_measured: i64 = read
            .sessions
            .iter()
            .filter_map(|session| session.usage.input.quantity())
            .map(|quantity| quantity as i64)
            .sum();
        assert_eq!(
            stored.get("input").copied().flatten().unwrap_or(0),
            input_measured
        );
        assert!(stored.contains_key("input"));
        assert_eq!(outcome.quarantined_redeliveries, 0);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "the store was not written"
        );
    }
}
