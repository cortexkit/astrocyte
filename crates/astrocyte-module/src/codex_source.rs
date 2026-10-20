//! Codex CLI's on-disk session producer and ledger ingest mapping.
//!
//! Reads `~/.codex/sessions/**/rollout-*.jsonl` and, when present,
//! `~/.codex/archived_sessions/`, where Codex moves older rollouts unchanged.
//! A moved file is read again under its new path; every call's identity and
//! content come from the file alone, so the second read is an exact
//! redelivery, not a new event.

use std::collections::{BTreeMap, HashMap};
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::{EventRecordRequest, MeasurementInput};
use astrocyte_readers::sessions::codex::{
    parse_codex_file_resumable, CodexCall, CodexDerived, CodexParseCounts, CodexPrefixCheck,
    CodexResumePoint,
};

use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const CODEX_SOURCE_KEY: &str = "codex";

/// The session directories below the Codex home, in read order.
const SESSION_DIRECTORIES: [&str; 2] = ["sessions", "archived_sessions"];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CodexDrainOutcome {
    /// Neither session directory exists: no Codex lane.
    pub root_absent: bool,
    /// Rollout files found, per session directory.
    pub files_by_directory: BTreeMap<&'static str, usize>,
    pub files_read: usize,
    /// Files read from their previous byte offset; a subset of `files_read`.
    pub files_resumed: usize,
    /// Reader counts summed over every file this drain read.
    pub counts: CodexParseCounts,
    /// Complete lines that could not be read, per file, for every file that
    /// had any this drain. Distinct from an unterminated tail, which is
    /// never consumed and never counted.
    pub skipped_lines_by_file: BTreeMap<PathBuf, u64>,
    /// Calls the reader handed over this drain.
    pub calls_emitted: usize,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
    /// Bytes fed to SHA-256 while reading rollouts: the deterministic
    /// steady-state cost counter, O(window + appended) per file.
    pub hashed_bytes: u64,
}

#[derive(Debug, Default)]
struct CodexCache {
    /// Where each file's next read may resume, with the parse state its
    /// consumed bytes produced. Taken out for a drain and put back only after
    /// the drain's events commit, so a failed or stopped drain leaves no
    /// resume point behind and the next drain reads every file in full,
    /// which content-derived identity makes safe.
    resume: HashMap<PathBuf, CodexResumePoint>,
    /// When each file's consumed prefix was last verified end to end.
    prefix_verified_ms: HashMap<PathBuf, i64>,
    windows: HashMap<PathBuf, crate::refresher::harness_drain::WindowSchedule>,
}

#[derive(Debug, Clone)]
enum CodexHome {
    Path(PathBuf),
    Unresolved(String),
}

/// A process-local incremental view over Codex's rollout files.
pub(crate) struct CodexSource {
    home: CodexHome,
    cache: Mutex<CodexCache>,
}

impl CodexSource {
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
        let root = match crate::harness_paths::codex(env) {
            Ok(path) => CodexHome::Path(path),
            Err(error) => CodexHome::Unresolved(error.to_string()),
        };
        Self {
            home: root,
            cache: Mutex::new(CodexCache::default()),
        }
    }

    /// HOME-only fixture layout keeps existing drain/health tests independent of
    /// process variables. Production discovery uses `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>) -> Self {
        let home = match home {
            Some(home) if Path::new(&home).is_absolute() => {
                CodexHome::Path(PathBuf::from(home).join(".codex"))
            }
            Some(home) => CodexHome::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the Codex home"
            )),
            None => {
                CodexHome::Unresolved("HOME is unset; cannot resolve the Codex home".to_string())
            }
        };
        Self {
            home,
            cache: Mutex::new(CodexCache::default()),
        }
    }

    /// A source over `codex_home` (the directory holding `sessions/`).
    #[cfg(test)]
    pub(crate) fn at(codex_home: impl Into<PathBuf>) -> Self {
        Self {
            home: CodexHome::Path(codex_home.into()),
            cache: Mutex::new(CodexCache::default()),
        }
    }

    /// Read every rollout from where the last drain stopped and commit the
    /// calls read. Setting `stop` abandons the drain between files or between
    /// events and returns `SourceDrainError::Stopped` having committed
    /// nothing.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<CodexDrainOutcome, SourceDrainError> {
        let home = match &self.home {
            CodexHome::Path(home) => home,
            CodexHome::Unresolved(reason) => {
                crate::refresher::harness_drain::ensure_runtime_row(state, CODEX_SOURCE_KEY)?;
                return Err(reason.clone().into());
            }
        };

        let mut present = Vec::new();
        for directory in SESSION_DIRECTORIES {
            let root = home.join(directory);
            match fs::metadata(&root) {
                Ok(_) => present.push((directory, root)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    crate::refresher::harness_drain::ensure_runtime_row(state, CODEX_SOURCE_KEY)?;
                    return Err(
                        format!("inspect Codex session root {}: {error}", root.display()).into(),
                    );
                }
            }
        }
        if present.is_empty() {
            let paths = SESSION_DIRECTORIES
                .iter()
                .map(|directory| home.join(directory).display().to_string())
                .collect::<Vec<_>>()
                .join(",");
            crate::refresher::harness_drain::record_source_discovery(
                state,
                CODEX_SOURCE_KEY,
                &paths,
                false,
            );
            crate::refresher::harness_drain::remove_runtime_row(state, CODEX_SOURCE_KEY)?;
            return Ok(CodexDrainOutcome {
                root_absent: true,
                ..CodexDrainOutcome::default()
            });
        }
        let paths = present
            .iter()
            .map(|(_, root)| root.display().to_string())
            .collect::<Vec<_>>()
            .join(",");
        crate::refresher::harness_drain::record_source_discovery(
            state,
            CODEX_SOURCE_KEY,
            &paths,
            true,
        );
        crate::refresher::harness_drain::ensure_runtime_row(state, CODEX_SOURCE_KEY)?;

        let mut outcome = CodexDrainOutcome::default();
        let mut files = Vec::new();
        for (directory, root) in &present {
            let found = jsonl_files(root)
                .map_err(|error| format!("read Codex session root {}: {error}", root.display()))?;
            outcome.files_by_directory.insert(directory, found.len());
            files.extend(found);
        }
        let (mut previous_points, previous_verified, previous_windows) = {
            let cache = self
                .cache
                .lock()
                .map_err(|_| "Codex cache lock poisoned".to_string())?;
            (
                cache.resume.clone(),
                cache.prefix_verified_ms.clone(),
                cache.windows.clone(),
            )
        };

        let mut resume_points = HashMap::with_capacity(files.len());
        let mut verified_ms = HashMap::with_capacity(files.len());
        let mut windows = HashMap::with_capacity(files.len());
        let mut requests = Vec::new();
        let mut set_aside = Vec::new();
        for path in files {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            let stat = crate::refresher::harness_drain::FileStat::read(&path)
                .map_err(|error| format!("stat Codex session {}: {error}", path.display()))?;
            let previous = previous_points.remove(&path);
            let check = if crate::refresher::harness_drain::prefix_check(
                &path,
                previous.is_some(),
                previous_verified.get(&path).copied(),
                recorded_at_ms,
            ) == astrocyte_readers::sessions::jsonl_resume::PrefixCheck::Full
            {
                CodexPrefixCheck::Full
            } else {
                CodexPrefixCheck::Window
            };
            let window = previous_windows.get(&path);
            if let (Some(point), Some(window)) = (previous.as_ref(), window) {
                if window.defer(&stat, recorded_at_ms, check == CodexPrefixCheck::Full) {
                    resume_points.insert(path.clone(), point.clone());
                    if let Some(&verified) = previous_verified.get(&path) {
                        verified_ms.insert(path.clone(), verified);
                    }
                    windows.insert(path, window.clone());
                    continue;
                }
            }
            let previous_offset = previous.as_ref().map(CodexResumePoint::offset);
            let parse = parse_codex_file_resumable(&path, previous, check);
            if let Some(reason) = parse.io_error {
                return Err(format!("read Codex session file {}: {reason}", path.display()).into());
            }
            let offset = parse.resume.as_ref().map(CodexResumePoint::offset);
            windows.insert(
                path.clone(),
                crate::refresher::harness_drain::WindowSchedule::after_read(
                    window,
                    stat.clone(),
                    recorded_at_ms,
                    parse.resumed && offset == previous_offset,
                    offset == Some(stat.len),
                ),
            );
            set_aside.extend(parse.set_aside.into_iter().map(|(line, reason)| {
                crate::refresher::SetAsideRecord {
                    locator: format!("{path:?}:line:{line}"),
                    reason,
                }
            }));
            if check == CodexPrefixCheck::Full || !parse.resumed {
                verified_ms.insert(path.clone(), recorded_at_ms);
            } else if let Some(&verified) = previous_verified.get(&path) {
                verified_ms.insert(path.clone(), verified);
            }
            outcome.files_read += 1;
            if parse.resumed {
                outcome.files_resumed += 1;
            }
            outcome.hashed_bytes += parse.hashed_bytes;
            outcome.counts.add(&parse.counts);
            if parse.counts.skipped_lines > 0 {
                outcome
                    .skipped_lines_by_file
                    .insert(path.clone(), parse.counts.skipped_lines);
            }
            outcome.calls_emitted += parse.calls.len();
            if let Some(point) = parse.resume {
                resume_points.insert(path.clone(), point);
            }
            for call in parse.calls {
                let locator = format!("{path:?}:line:{}", call.source_line);
                match call
                    .set_aside_reason
                    .clone()
                    .map_or_else(|| map_call(call), Err)
                {
                    Ok(request) => requests.push(request),
                    Err(reason) => {
                        set_aside.push(crate::refresher::SetAsideRecord { locator, reason })
                    }
                }
            }
        }

        record_requests(
            state,
            &requests,
            &set_aside,
            recorded_at_ms,
            &mut outcome,
            &|| stop.load(Ordering::Relaxed),
        )?;

        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Codex cache lock poisoned".to_string())?;
        cache.resume = resume_points;
        cache.prefix_verified_ms = verified_ms;
        cache.windows = windows;
        Ok(outcome)
    }
}

impl crate::refresher::harness_drain::HarnessSource for CodexSource {
    fn source_key(&self) -> &'static str {
        CODEX_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok(crate::refresher::harness_drain::health_reason(
            "Codex",
            outcome.quarantined_redeliveries,
            outcome.counts.usage_not_ingested(),
        ))
    }
}

/// Sorted JSONL files, without following symlinks.
fn jsonl_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    crate::refresher::harness_drain::jsonl_files(root)
}

fn derived_measurement(
    role: &str,
    source: &str,
    derived: CodexDerived,
) -> Result<Option<MeasurementInput>, String> {
    use astrocyte_readers::sessions::roles::DerivedRole;
    let derived = match derived {
        CodexDerived::Absent => DerivedRole::Absent,
        CodexDerived::Measured(quantity) => DerivedRole::Measured(quantity),
        CodexDerived::Conflicted => DerivedRole::Conflicted,
        CodexDerived::Underivable => DerivedRole::Underivable,
    };
    crate::refresher::harness_drain::derived_measurement(role, source, derived)
        .map_err(|error| format!("Codex {error}"))
}

fn measurement(
    role: &str,
    source: &str,
    quantity: Option<u64>,
) -> Result<MeasurementInput, String> {
    crate::refresher::harness_drain::measurement(role, source, quantity)
        .map_err(|error| format!("Codex {error}"))
}

fn map_call(call: CodexCall) -> Result<EventRecordRequest, String> {
    let usage = call.usage;
    let mut measurements = Vec::with_capacity(5);
    measurements.extend(derived_measurement(
        "input",
        "last_token_usage.input_tokens-cached_input_tokens",
        usage.input,
    )?);
    for (role, source, quantity) in [
        (
            "cache_read",
            "last_token_usage.cached_input_tokens",
            usage.cache_read,
        ),
        (
            "cache_write",
            "last_token_usage.cache_write_input_tokens",
            usage.cache_write,
        ),
    ] {
        if let Some(quantity) = quantity {
            measurements.push(measurement(role, source, Some(quantity))?);
        }
    }
    measurements.extend(derived_measurement(
        "output",
        "last_token_usage.output_tokens-reasoning_output_tokens",
        usage.output,
    )?);
    if let Some(reasoning) = usage.reasoning {
        measurements.push(measurement(
            "reasoning",
            "last_token_usage.reasoning_output_tokens",
            Some(reasoning),
        )?);
    }

    Ok(EventRecordRequest {
        producer: CODEX_SOURCE_KEY.to_string(),
        // The conversation id. Measured stable across a resume (Codex appends
        // to the same rollout) and shared by a subagent fork, which carries
        // its parent's `session_id`, so a replayed response that escaped the
        // reader's replay check lands on the parent's identity.
        producer_lineage: Some(call.session_id.clone()),
        producer_ref: call.producer_ref,
        prior_event_ref: None,
        harness: None,
        project_root: call.project_root,
        session: Some(call.session_id),
        run_ref: None,
        work_class: None,
        occurred_at_ms: call.occurred_at_ms,
        emitted_at_ms: None,
        // A `token_count` is written only for a completed response; Codex
        // records no per-response status.
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider: call.provider,
        model: call.model,
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        // No rollout record says how Codex authenticated (ChatGPT sign-in or
        // API key); `rate_limits.plan_type` names a plan tier on some
        // records and is not read as a statement of who paid.
        charge_basis: ChargeBasis::Unknown,
        measurements,
    })
}

/// Commit good records, evidence and progress together; a stop rolls back all three.
fn record_requests(
    state: &SharedState,
    requests: &[EventRecordRequest],
    set_aside: &[crate::refresher::SetAsideRecord],
    recorded_at_ms: i64,
    outcome: &mut CodexDrainOutcome,
    stop_requested: &dyn Fn() -> bool,
) -> Result<(), SourceDrainError> {
    use crate::refresher::harness_drain::{
        mark_current_in_tx, record_events_in_tx, record_set_aside_in_tx, RecordTally,
    };
    let mut stopped = false;
    let committed = state.ordered_write(|tx| {
        if requests.is_empty() && stop_requested() {
            stopped = true;
            return Err("Codex ingest stopped".into());
        }
        let newly_seen = record_set_aside_in_tx(tx, CODEX_SOURCE_KEY, set_aside, recorded_at_ms)?;
        let dispositions = record_events_in_tx(
            tx,
            "Codex",
            requests,
            recorded_at_ms,
            stop_requested,
            &mut stopped,
        )?;
        mark_current_in_tx(tx, CODEX_SOURCE_KEY, "Codex", recorded_at_ms)?;
        Ok(((dispositions, newly_seen), true))
    });
    let (dispositions, newly_seen) = match committed {
        Ok(committed) => committed,
        Err(_) if stopped => return Err(SourceDrainError::Stopped),
        Err(reason) => return Err(SourceDrainError::Failed(reason)),
    };
    crate::refresher::harness_drain::warn_set_aside(CODEX_SOURCE_KEY, &newly_seen);
    let tally = RecordTally::from_dispositions(dispositions);
    outcome.rows_recorded = tally.recorded;
    outcome.exact_redeliveries = tally.exact_redeliveries;
    outcome.quarantined_redeliveries = tally.quarantined_redeliveries;
    Ok(())
}

// Test fixtures use fixed prefix windows, small line counts and nonnegative ledger totals.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::run_iteration_with_codex;
    use crate::state::test_state;
    use astrocyte_readers::sessions::codex::CODEX_PREFIX_WINDOW_BYTES;
    use std::collections::BTreeSet;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    #[test]
    fn set_aside_codex_overflow_keeps_good_calls_and_resume() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "sessions/a.jsonl", &parent_rollout());
        let state = test_state();
        let source = CodexSource::at(temp.path());
        drain(&source, &state, 100);
        append(
            &path,
            &token_count(6, [1_u64 << 63, 70, 0, 40, 12], [1_u64 << 63, 0, 0, 5, 0]),
        );
        append(
            &path,
            &token_count(7, [(1_u64 << 63) + 50, 70, 0, 45, 12], [50, 0, 0, 5, 0]),
        );
        let committed_offset = source.cache.lock().unwrap().resume[&path].offset();
        state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER refuse_evidence BEFORE INSERT ON set_aside_record BEGIN SELECT RAISE(ABORT,'test failure'); END;").map_err(|e| e.to_string())).unwrap();
        assert!(source.drain(&state, 200, &NEVER_STOP).is_err());
        assert_eq!(event_refs(&state).len(), 2);
        assert_eq!(
            source.cache.lock().unwrap().resume[&path].offset(),
            committed_offset
        );
        state
            .with_conn(|conn| {
                conn.execute_batch("DROP TRIGGER refuse_evidence")
                    .map_err(|e| e.to_string())
            })
            .unwrap();
        for now in [200, 300] {
            let result = source.drain(&state, now, &NEVER_STOP);
            assert!(
                result.is_ok(),
                "tick {now}: {result:?}; cache={:?}",
                source.cache.lock().unwrap()
            );
            assert_eq!(
                event_refs(&state),
                BTreeSet::from([
                    format!("{TURN}:1"),
                    format!("{TURN}:2"),
                    format!("{TURN}:4")
                ])
            );
            assert_eq!(
                source.cache.lock().unwrap().resume[&path].offset(),
                fs::metadata(&path).unwrap().len()
            );
            crate::refresher::harness_drain::assert_set_aside(&state, "codex", 1);
        }
    }

    #[test]
    fn set_aside_codex_undecidable_fork_does_not_duplicate_parent() {
        let temp = TempDir::new().unwrap();
        let usage = token_count(1, [100, 60, 0, 30, 12], [100, 60, 0, 30, 12]);
        write_file(
            temp.path(),
            "sessions/parent.jsonl",
            &(session_meta(THREAD, None) + &usage),
        );
        write_file(
            temp.path(),
            "sessions/child.jsonl",
            &(session_meta("child", Some(THREAD))
                + &usage
                + &turn(2, FORK_TURN)
                + &token_count(4, [150, 70, 0, 35, 12], [50, 10, 0, 5, 0])),
        );
        let source = CodexSource::at(temp.path());
        let state = test_state();
        for now in [100, 200] {
            drain(&source, &state, now);
            assert_eq!(
                event_refs(&state),
                BTreeSet::from([format!("{THREAD}:ordinal:1"), format!("{FORK_TURN}:1")])
            );
            crate::refresher::harness_drain::assert_set_aside(&state, "codex", 1);
        }
    }

    #[test]
    fn set_aside_good_codex_rows_are_byte_identical() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), "sessions/a.jsonl", &parent_rollout());
        let state = test_state();
        drain(&CodexSource::at(temp.path()), &state, 100);
        crate::refresher::harness_drain::assert_good_snapshot(
            &state,
            "codex",
            "7944ff0d36e02effa93219baa184dd7d762c97a21b748efed8a2db10141d31cc",
        );
    }

    #[cfg(unix)]
    #[test]
    fn set_aside_codex_unreadable_file_fails_drain_and_retains_resume() {
        use std::os::unix::fs::PermissionsExt;
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "sessions/a.jsonl", &parent_rollout());
        let state = test_state();
        let source = CodexSource::at(temp.path());
        drain(&source, &state, 100);
        let offset = source.cache.lock().unwrap().resume[&path].offset();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o0)).unwrap();
        assert!(
            fs::File::open(&path).is_err(),
            "permission fixture must be unreadable"
        );
        run_iteration_with_codex(&state, &[], &source, &NEVER_STOP);
        assert!(state.producer_degraded_reason("codex").is_some());
        assert_eq!(source.cache.lock().unwrap().resume[&path].offset(), offset);
        assert_eq!(event_refs(&state).len(), 2);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    const THREAD: &str = "019f48b9-8c8f-7351-8841-9387ce1e709f";
    const TURN: &str = "019f48b9-8d15-7612-a4ec-17a0ce833015";
    const FORK: &str = "019f48b9-cb62-7b40-9bb3-db9ad8062d69";
    const FORK_TURN: &str = "019f48b9-cb87-7b90-9834-5937afde433b";

    fn session_meta(id: &str, fork_parent: Option<&str>) -> String {
        let fork = fork_parent.map_or(String::new(), |parent| {
            format!(
                r#","forked_from_id":"{parent}","source":{{"subagent":{{"thread_spawn":{{"parent_thread_id":"{parent}"}}}}}}"#
            )
        });
        format!(
            r#"{{"timestamp":"2026-07-09T21:12:27.672Z","ordinal":0,"type":"session_meta","payload":{{"session_id":"{THREAD}","id":"{id}","cwd":"/work/project","model_provider":"openai"{fork}}}}}"#
        ) + "\n"
    }

    fn turn(ordinal: u64, turn: &str) -> String {
        [
            format!(
                r#"{{"timestamp":"2026-07-09T21:12:27.672Z","ordinal":{ordinal},"type":"event_msg","payload":{{"type":"task_started","turn_id":"{turn}"}}}}"#
            ),
            format!(
                r#"{{"timestamp":"2026-07-09T21:12:28.501Z","ordinal":{},"type":"turn_context","payload":{{"turn_id":"{turn}","cwd":"/work/project","model":"gpt-5.6-sol"}}}}"#,
                ordinal + 1
            ),
        ]
        .join("\n")
            + "\n"
    }

    /// `[input, cached, cache_write, output, reasoning]`.
    fn block(counters: [u64; 5]) -> String {
        let [input, cached, write, output, reasoning] = counters;
        format!(
            r#"{{"input_tokens":{input},"cached_input_tokens":{cached},"cache_write_input_tokens":{write},"output_tokens":{output},"reasoning_output_tokens":{reasoning},"total_tokens":{}}}"#,
            input + output
        )
    }

    fn token_count(ordinal: u64, total: [u64; 5], last: [u64; 5]) -> String {
        format!(
            r#"{{"timestamp":"2026-07-09T21:12:37.335Z","ordinal":{ordinal},"type":"event_msg","payload":{{"type":"token_count","info":{{"total_token_usage":{},"last_token_usage":{}}}}}}}"#,
            block(total),
            block(last)
        ) + "\n"
    }

    fn write_file(root: &Path, relative: &str, contents: &str) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();
        path
    }

    fn append(path: &Path, text: &str) {
        use std::io::Write;
        let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn drain(source: &CodexSource, state: &SharedState, at: i64) -> CodexDrainOutcome {
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

    fn event_refs(state: &SharedState) -> BTreeSet<String> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT producer_ref FROM usage_event WHERE producer='codex'")
                    .map_err(|error| error.to_string())?;
                let refs = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?
                    .collect::<Result<BTreeSet<String>, _>>()
                    .map_err(|error| error.to_string());
                refs
            })
            .unwrap()
    }

    /// `role -> (quantity, source)` of one event.
    fn measurements(
        state: &SharedState,
        producer_ref: &str,
    ) -> BTreeMap<String, (Option<i64>, String)> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT measurement.role,measurement.quantity_units,measurement.source
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='codex' AND usage_event.producer_ref=?1",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([producer_ref], |row| {
                        Ok((row.get(0)?, (row.get(1)?, row.get(2)?)))
                    })
                    .map_err(|error| error.to_string())?
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map_err(|error| error.to_string());
                rows
            })
            .unwrap()
    }

    fn quantities(state: &SharedState, producer_ref: &str) -> BTreeMap<String, Option<i64>> {
        measurements(state, producer_ref)
            .into_iter()
            .map(|(role, (quantity, _))| (role, quantity))
            .collect()
    }

    fn parent_rollout() -> String {
        [
            session_meta(THREAD, None),
            turn(1, TURN),
            token_count(3, [100, 60, 0, 30, 12], [100, 60, 0, 30, 12]),
            token_count(4, [100, 60, 0, 30, 12], [100, 60, 0, 30, 12]),
            token_count(5, [150, 70, 0, 35, 12], [50, 10, 0, 5, 0]),
        ]
        .concat()
    }

    /// One event per response, stored in disjoint roles: uncached input,
    /// cache read, cache write, visible output, reasoning. A repeated total
    /// is not a second event.
    #[test]
    fn each_response_is_one_event_in_disjoint_roles() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "sessions/2026/07/09/rollout-a.jsonl",
            &parent_rollout(),
        );
        let state = test_state();
        let outcome = drain(&CodexSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.counts.repeated_totals, 1);
        assert_eq!(outcome.rows_recorded, 2);
        assert_eq!(
            quantities(&state, &format!("{TURN}:1")),
            BTreeMap::from([
                ("cache_read".to_string(), Some(60)),
                ("cache_write".to_string(), Some(0)),
                ("input".to_string(), Some(40)),
                ("output".to_string(), Some(18)),
                ("reasoning".to_string(), Some(12)),
            ])
        );
        let (lineage, session, provider, model, project): (String, String, String, String, String) =
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT producer_lineage,session,provider,model,project_root
                     FROM usage_event WHERE producer='codex' AND producer_ref=?1",
                        [format!("{TURN}:2")],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                            ))
                        },
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap();
        assert_eq!(
            (lineage.as_str(), session.as_str(), provider.as_str()),
            (THREAD, THREAD, "openai")
        );
        assert_eq!(
            (model.as_str(), project.as_str()),
            ("gpt-5.6-sol", "/work/project")
        );
    }

    /// A conflicted role lands as a measurement without a quantity, so the
    /// event still counts, the contradiction is visible, and that role can
    /// never be priced.
    #[test]
    fn a_conflicted_role_is_stored_without_a_quantity() {
        let temp = TempDir::new().unwrap();
        let bad = [10, 60, 0, 30, 12];
        write_file(
            temp.path(),
            "sessions/rollout-a.jsonl",
            &[
                session_meta(THREAD, None),
                turn(1, TURN),
                token_count(3, bad, bad),
            ]
            .concat(),
        );
        let state = test_state();
        let outcome = drain(&CodexSource::at(temp.path()), &state, 0);
        assert_eq!(
            (outcome.rows_recorded, outcome.counts.input_conflicts),
            (1, 1)
        );
        let stored = measurements(&state, &format!("{TURN}:1"));
        assert_eq!(
            stored["input"],
            (
                None,
                "last_token_usage.input_tokens-cached_input_tokens:conflicted".to_string()
            )
        );
        assert_eq!(stored["output"].0, Some(18));
    }

    /// A subagent fork replays its parent's responses before its own. The
    /// ledger holds the parent's responses once and the fork's own once.
    #[test]
    fn a_fork_and_its_parent_do_not_count_the_parents_responses_twice() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "sessions/rollout-parent.jsonl",
            &parent_rollout(),
        );
        write_file(
            temp.path(),
            "sessions/rollout-fork.jsonl",
            &[
                session_meta(FORK, Some(THREAD)),
                turn(1, TURN),
                token_count(12, [100, 60, 0, 30, 12], [100, 60, 0, 30, 12]),
                turn(13, FORK_TURN),
                token_count(20, [180, 80, 0, 40, 13], [80, 20, 0, 10, 1]),
            ]
            .concat(),
        );
        let state = test_state();
        let outcome = drain(&CodexSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.counts.fork_replays_skipped, 1);
        assert_eq!(outcome.quarantined_redeliveries, 0);
        assert_eq!(
            event_refs(&state),
            BTreeSet::from([
                format!("{TURN}:1"),
                format!("{TURN}:2"),
                format!("{FORK_TURN}:1"),
            ])
        );
    }

    /// Re-reading, from scratch or from an archived copy, writes no new rows
    /// and no redelivery rows: a file reader is not a pushed producer.
    #[test]
    fn re_reads_and_archived_copies_write_no_rows() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), "sessions/rollout-a.jsonl", &parent_rollout());
        let state = test_state();
        let first = drain(&CodexSource::at(temp.path()), &state, 0);
        assert_eq!(first.rows_recorded, 2);

        write_file(
            temp.path(),
            "archived_sessions/rollout-a.jsonl",
            &parent_rollout(),
        );
        let again = drain(&CodexSource::at(temp.path()), &state, 1);
        assert_eq!(
            again.files_by_directory,
            BTreeMap::from([("archived_sessions", 1), ("sessions", 1)])
        );
        assert_eq!((again.rows_recorded, again.exact_redeliveries), (0, 4));
        assert_eq!(count(&state, "SELECT COUNT(*) FROM usage_event"), 2);
        assert_eq!(count(&state, "SELECT COUNT(*) FROM redelivery_log"), 0);
    }

    /// A corrupt complete line is counted per file and reading continues; an
    /// unterminated tail is a writer mid-append, ingested once it completes.
    #[test]
    fn a_corrupt_line_is_counted_and_a_partial_tail_waits() {
        let temp = TempDir::new().unwrap();
        let tail = token_count(9, [200, 90, 0, 50, 20], [50, 20, 0, 15, 8]);
        let (head, rest) = tail.split_at(tail.len() / 2);
        let path = write_file(
            temp.path(),
            "sessions/rollout-a.jsonl",
            &[parent_rollout(), "not json\n".to_string(), head.to_string()].concat(),
        );
        let state = test_state();
        let source = CodexSource::at(temp.path());
        let first = drain(&source, &state, 0);
        assert_eq!(
            first.skipped_lines_by_file,
            BTreeMap::from([(path.clone(), 1)])
        );
        assert_eq!(first.rows_recorded, 2);
        append(&path, rest);
        let second = drain(&source, &state, 1);
        assert_eq!((second.files_resumed, second.rows_recorded), (1, 1));
        assert!(second.skipped_lines_by_file.is_empty());
    }

    /// Per-tick cost scales with new bytes, not with the file: a grown file
    /// hashes one window plus what was appended, a quiet file one window.
    #[test]
    fn steady_state_tick_cost_is_bounded_by_the_window_and_the_appended_bytes() {
        let temp = TempDir::new().unwrap();
        let filler = r#"{"timestamp":"2026-07-09T21:12:28.000Z","ordinal":2,"type":"response_item","payload":{"type":"message","role":"user","content":[]}}"#.to_string() + "\n";
        let lines = (8 * CODEX_PREFIX_WINDOW_BYTES as usize) / filler.len() + 1;
        let contents = parent_rollout() + &filler.repeat(lines);
        let path = write_file(temp.path(), "sessions/rollout-a.jsonl", &contents);
        let state = test_state();
        let source = CodexSource::at(temp.path());
        drain(&source, &state, 0);

        let appended = token_count(90, [200, 90, 0, 50, 20], [50, 20, 0, 15, 8]);
        append(&path, &appended);
        let grown = drain(&source, &state, 1);
        assert_eq!((grown.files_resumed, grown.counts.lines), (1, 1));
        assert_eq!(grown.rows_recorded, 1);
        let bound = 2 * CODEX_PREFIX_WINDOW_BYTES + appended.len() as u64;
        assert!(
            grown.hashed_bytes <= bound,
            "{} > {bound}",
            grown.hashed_bytes
        );
        assert!(grown.hashed_bytes < contents.len() as u64 / 2);

        let quiet = drain(&source, &state, 2);
        assert_eq!((quiet.files_resumed, quiet.counts.lines), (1, 0));
        assert!(quiet.hashed_bytes <= 2 * CODEX_PREFIX_WINDOW_BYTES);
    }

    #[test]
    fn absent_root_is_no_lane_but_an_unreadable_root_is_cold() {
        let temp = TempDir::new().unwrap();
        let state = test_state();
        let absent = CodexSource::from_home(Some(temp.path().as_os_str().to_owned()));
        assert!(drain(&absent, &state, 0).root_absent);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='codex'"
            ),
            0
        );

        write_file(temp.path(), ".codex/sessions", "a file, not a directory");
        let error = absent
            .drain(&state, 1, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("read Codex session root"), "{error}");
        let cold: (i64, Option<i64>, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MAX(last_success_ms),MAX(pending_unknown)
                     FROM source_runtime WHERE source_key='codex'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(cold, (1, None, 1));

        let relative = CodexSource::from_home(Some(OsString::from("relative")));
        assert!(relative.drain(&state, 2, &NEVER_STOP).is_err());
    }

    #[test]
    fn refresher_iteration_drains_codex_and_clears_its_health() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), "sessions/rollout-a.jsonl", &parent_rollout());
        let state = test_state();
        run_iteration_with_codex(&state, &[], &CodexSource::at(temp.path()), &NEVER_STOP);
        assert_eq!(event_refs(&state).len(), 2);
        assert!(state.producer_degraded_reason("codex").is_none());
    }

    /// Per-class totals of a raw `serde_json` scan that shares no code with
    /// the reader. It de-duplicates responses by content instead of by the
    /// reader's rules: a response is keyed by its conversation and its
    /// running total, which a repeat and a fork's replay both reproduce and
    /// a new response never does. It also measures the token convention.
    #[derive(Debug, Default)]
    struct RawTotals {
        calls: u64,
        input: u64,
        cached: u64,
        cache_write: u64,
        output: u64,
        reasoning: u64,
        unparseable_lines: u64,
        usage_records: u64,
        total_identity_holds: u64,
        cached_exceeds_input: u64,
        reasoning_exceeds_output: u64,
        non_zero_cache_write: u64,
        non_zero_reasoning: u64,
    }

    fn raw_totals(files: &[PathBuf]) -> RawTotals {
        let mut totals = RawTotals::default();
        let mut seen = BTreeSet::new();
        for path in files {
            let bytes = fs::read(path).unwrap();
            let Some(end) = bytes.iter().rposition(|&byte| byte == b'\n') else {
                continue;
            };
            let mut session = None;
            for line in bytes[..end].split(|&byte| byte == b'\n') {
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) else {
                    totals.unparseable_lines += 1;
                    continue;
                };
                if value["type"] == "session_meta" && session.is_none() {
                    session = value["payload"]["session_id"].as_str().map(str::to_string);
                }
                let info = &value["payload"]["info"];
                if value["payload"]["type"] != "token_count" || !info.is_object() {
                    continue;
                }
                let (total, last) = (&info["total_token_usage"], &info["last_token_usage"]);
                for block in [total, last] {
                    let field = |name: &str| block[name].as_u64().unwrap();
                    totals.usage_records += 1;
                    if field("total_tokens") == field("input_tokens") + field("output_tokens") {
                        totals.total_identity_holds += 1;
                    }
                    if field("cached_input_tokens") > field("input_tokens") {
                        totals.cached_exceeds_input += 1;
                    }
                    if field("reasoning_output_tokens") > field("output_tokens") {
                        totals.reasoning_exceeds_output += 1;
                    }
                    if field("cache_write_input_tokens") > 0 {
                        totals.non_zero_cache_write += 1;
                    }
                    if field("reasoning_output_tokens") > 0 {
                        totals.non_zero_reasoning += 1;
                    }
                }
                if !seen.insert((session.clone(), total.to_string())) {
                    continue;
                }
                let field = |name: &str| last[name].as_u64().unwrap();
                totals.calls += 1;
                totals.input += field("input_tokens");
                totals.cached += field("cached_input_tokens");
                totals.cache_write += field("cache_write_input_tokens");
                totals.output += field("output_tokens");
                totals.reasoning += field("reasoning_output_tokens");
            }
        }
        totals
    }

    /// Run with `--ignored`. Reads the live corpus (never writes it) into an
    /// in-memory scratch store and reconciles three independent views: the
    /// raw scan above, the reader's emitted calls, and the ledger's rows.
    #[test]
    #[ignore = "reads the mutable ~/.codex corpus"]
    fn live_codex_corpus_reconciles_raw_reader_and_ledger() {
        let home = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap()).join(".codex"));
        let files: Vec<PathBuf> = SESSION_DIRECTORIES
            .iter()
            .map(|directory| home.join(directory))
            .filter(|root| root.is_dir())
            .flat_map(|root| jsonl_files(&root).unwrap())
            .collect();
        let raw = raw_totals(&files);

        let mut reader = RawTotals::default();
        let mut reader_counts = CodexParseCounts::default();
        for path in &files {
            let parse = parse_codex_file_resumable(path, None, CodexPrefixCheck::Full);
            reader_counts.add(&parse.counts);
            for call in parse.calls {
                let usage = call.usage;
                let input = usage.input.quantity().unwrap();
                let output = usage.output.quantity().unwrap();
                reader.calls += 1;
                reader.cached += usage.cache_read.unwrap();
                reader.input += input + usage.cache_read.unwrap();
                reader.cache_write += usage.cache_write.unwrap();
                reader.reasoning += usage.reasoning.unwrap();
                reader.output += output + usage.reasoning.unwrap();
            }
        }

        let state = test_state();
        let outcome = drain(&CodexSource::at(&home), &state, 4_000_000_000_000);
        let ledger_role = |role: &str| -> u64 {
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COALESCE(SUM(measurement.quantity_units),0)
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='codex' AND measurement.role=?1",
                        [role],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap() as u64
        };
        let ledger_events = count(
            &state,
            "SELECT COUNT(*) FROM usage_event WHERE producer='codex'",
        ) as u64;

        eprintln!("files: {} ({:?})", files.len(), outcome.files_by_directory);
        eprintln!("raw: {raw:?}");
        eprintln!("reader counts: {reader_counts:?}");
        eprintln!("reader totals (gross): {reader:?}");
        eprintln!(
            "ledger: events {ledger_events}, input {} cache_read {} cache_write {} output {} \
             reasoning {}; quarantines {}",
            ledger_role("input"),
            ledger_role("cache_read"),
            ledger_role("cache_write"),
            ledger_role("output"),
            ledger_role("reasoning"),
            outcome.quarantined_redeliveries,
        );

        // Never a vacuous pass.
        assert!(raw.calls > 0 && raw.input > 0 && raw.cached > 0 && raw.output > 0);
        assert!(raw.reasoning > 0);
        // The nested convention the reader relies on: if any of these fail,
        // stop and re-establish the convention before trusting the reader.
        assert_eq!(raw.cached_exceeds_input, 0);
        assert_eq!(raw.reasoning_exceeds_output, 0);
        assert_eq!(
            raw.non_zero_cache_write, 0,
            "cache-write convention unmeasured"
        );

        assert_eq!(reader_counts.skipped_lines, raw.unparseable_lines);
        assert_eq!(reader_counts.usage_not_ingested(), 0);
        assert_eq!(
            (
                reader.calls,
                reader.input,
                reader.cached,
                reader.cache_write
            ),
            (raw.calls, raw.input, raw.cached, raw.cache_write)
        );
        assert_eq!(
            (reader.output, reader.reasoning),
            (raw.output, raw.reasoning)
        );

        assert_eq!(outcome.quarantined_redeliveries, 0);
        assert_eq!(ledger_events, raw.calls);
        assert_eq!(
            ledger_role("input") + ledger_role("cache_read"),
            raw.input,
            "stored uncached input plus cache read is Codex's input_tokens"
        );
        assert_eq!(ledger_role("cache_read"), raw.cached);
        assert_eq!(ledger_role("cache_write"), raw.cache_write);
        assert_eq!(ledger_role("output") + ledger_role("reasoning"), raw.output);
        assert_eq!(ledger_role("reasoning"), raw.reasoning);
    }
}
