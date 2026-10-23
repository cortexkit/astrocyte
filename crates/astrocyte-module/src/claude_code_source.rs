//! Claude Code's on-disk session producer and ledger ingest mapping.
//!
//! Reads `~/.claude/projects/**/*.jsonl`, recursively: subagent transcripts
//! live one level deeper than their session, under `<session>/subagents/`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_readers::provider_identity::inferred_provider_from_model;
use astrocyte_readers::sessions::claudecode::{
    parse_claude_file_resumable, ClaudeCodeCall, ClaudeParseCounts, ClaudePrefixCheck,
    ClaudeResumePoint, CLAUDE_CODE_QUIET_WINDOW_MS,
};
use rusqlite::params;

use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const CLAUDE_CODE_SOURCE_KEY: &str = "claude-code";

/// The retired `thalamus` gateway recorded Claude Code API calls from the wire.
/// Its retained events store the transcript's `requestId` as `vendor_request_id`,
/// so overlap suppression prevents metering the same call through both producers.
const THALAMUS_PRODUCER: &str = "thalamus";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ClaudeCodeDrainOutcome {
    pub root_absent: bool,
    /// Session files found, keyed by depth below the projects root: 1 for a
    /// session transcript, 3 for a subagent transcript.
    pub files_by_depth: BTreeMap<usize, usize>,
    pub files_read: usize,
    /// Files read from their previous byte offset; a subset of `files_read`.
    pub files_resumed: usize,
    /// Reader counts summed over every file this drain read.
    pub counts: ClaudeParseCounts,
    /// Complete lines that could not be read, per file, for every file that
    /// had any this drain. Distinct from an unterminated tail, which is
    /// never consumed and never counted.
    pub skipped_lines_by_file: BTreeMap<PathBuf, u64>,
    /// Calls the reader handed over this drain.
    pub calls_emitted: usize,
    /// Calls seen but not yet complete, across all files, after this drain.
    pub calls_pending: usize,
    /// Calls not recorded because a `thalamus` event already carries their
    /// request id: the gateway already recorded the same call.
    pub thalamus_skips: usize,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
    pub quarantined_refs: BTreeSet<String>,
    /// Bytes fed to SHA-256 while reading session files: the deterministic
    /// steady-state cost counter, O(window + appended) per file.
    pub hashed_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ClaudeCodeCache {
    /// Where each file's next read may resume, with the call state its
    /// consumed bytes produced. Taken out for a drain and put back only after
    /// the drain's events commit, so a failed or stopped drain leaves no
    /// resume point behind and the next drain reads every file in full,
    /// which content-derived identity makes safe.
    resume: HashMap<PathBuf, ClaudeResumePoint>,
    /// When each file's consumed prefix was last verified end to end.
    prefix_verified_ms: HashMap<PathBuf, i64>,
    windows: HashMap<PathBuf, crate::refresher::harness_drain::WindowSchedule>,
}

#[derive(Debug, Clone)]
enum ClaudeCodeRoot {
    Path(PathBuf),
    Unresolved(String),
}

/// A process-local incremental view over Claude Code's session files.
pub(crate) struct ClaudeCodeSource {
    root: ClaudeCodeRoot,
    quiet_window_ms: i64,
    cache: Mutex<ClaudeCodeCache>,
}

impl ClaudeCodeSource {
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
        let root = match crate::harness_paths::claude_code(env) {
            Ok(path) => ClaudeCodeRoot::Path(path),
            Err(error) => ClaudeCodeRoot::Unresolved(error.to_string()),
        };
        Self {
            root,
            quiet_window_ms: CLAUDE_CODE_QUIET_WINDOW_MS,
            cache: Mutex::new(ClaudeCodeCache::default()),
        }
    }

    /// HOME-only fixture layout keeps existing drain/health tests independent of
    /// process variables. Production discovery uses `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>) -> Self {
        let root = match home {
            Some(home) if Path::new(&home).is_absolute() => {
                ClaudeCodeRoot::Path(PathBuf::from(home).join(".claude/projects"))
            }
            Some(home) => ClaudeCodeRoot::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the Claude Code projects root"
            )),
            None => ClaudeCodeRoot::Unresolved(
                "HOME is unset; cannot resolve the Claude Code projects root".to_string(),
            ),
        };
        Self {
            root,
            quiet_window_ms: CLAUDE_CODE_QUIET_WINDOW_MS,
            cache: Mutex::new(ClaudeCodeCache::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn at(root: impl Into<PathBuf>) -> Self {
        Self {
            root: ClaudeCodeRoot::Path(root.into()),
            quiet_window_ms: CLAUDE_CODE_QUIET_WINDOW_MS,
            cache: Mutex::new(ClaudeCodeCache::default()),
        }
    }

    /// Read every session file from where the last drain stopped and commit
    /// the calls that completed. Setting `stop` abandons the drain between
    /// files or between events and returns `SourceDrainError::Stopped` having
    /// committed nothing.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<ClaudeCodeDrainOutcome, SourceDrainError> {
        let root = match &self.root {
            ClaudeCodeRoot::Path(root) => root,
            ClaudeCodeRoot::Unresolved(reason) => {
                crate::refresher::harness_drain::ensure_runtime_row(state, CLAUDE_CODE_SOURCE_KEY)?;
                return Err(reason.clone().into());
            }
        };

        match fs::metadata(root) {
            Ok(_) => {
                crate::refresher::harness_drain::record_source_discovery(
                    state,
                    CLAUDE_CODE_SOURCE_KEY,
                    root.display(),
                    true,
                );
                crate::refresher::harness_drain::ensure_runtime_row(state, CLAUDE_CODE_SOURCE_KEY)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                crate::refresher::harness_drain::record_source_discovery(
                    state,
                    CLAUDE_CODE_SOURCE_KEY,
                    root.display(),
                    false,
                );
                crate::refresher::harness_drain::remove_runtime_row(state, CLAUDE_CODE_SOURCE_KEY)?;
                return Ok(ClaudeCodeDrainOutcome {
                    root_absent: true,
                    ..ClaudeCodeDrainOutcome::default()
                });
            }
            Err(error) => {
                crate::refresher::harness_drain::ensure_runtime_row(state, CLAUDE_CODE_SOURCE_KEY)?;
                return Err(format!(
                    "inspect Claude Code projects root {}: {error}",
                    root.display()
                )
                .into());
            }
        }

        let previous = self
            .cache
            .lock()
            .map_err(|_| "Claude Code cache lock poisoned".to_string())?
            .clone();
        let read = read_projects(
            root,
            &previous,
            recorded_at_ms,
            self.quiet_window_ms,
            None,
            stop,
        )?;
        let ClaudeCodeRead {
            mut outcome,
            requests,
            set_aside,
            cache: next_cache,
        } = read;
        record_requests(
            state,
            CLAUDE_CODE_SOURCE_KEY,
            &requests,
            &set_aside,
            recorded_at_ms,
            &mut outcome,
            &|| stop.load(Ordering::Relaxed),
        )?;

        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Claude Code cache lock poisoned".to_string())?;
        *cache = next_cache;
        Ok(outcome)
    }
}

pub(crate) struct ClaudeCodeRead {
    pub(crate) outcome: ClaudeCodeDrainOutcome,
    pub(crate) requests: Vec<EventRecordRequest>,
    pub(crate) set_aside: Vec<crate::refresher::SetAsideRecord>,
    pub(crate) cache: ClaudeCodeCache,
}

/// Read a Claude projects directory, returning proposed file offsets and call
/// state separately so failed ledger commits cannot skip calls on the next drain.
pub(crate) fn read_projects(
    root: &Path,
    previous: &ClaudeCodeCache,
    recorded_at_ms: i64,
    quiet_window_ms: i64,
    work_class: Option<&str>,
    stop: &AtomicBool,
) -> Result<ClaudeCodeRead, SourceDrainError> {
    let files = jsonl_files(root)
        .map_err(|error| format!("read Claude Code projects root {}: {error}", root.display()))?;
    let mut previous_points = previous.resume.clone();
    let previous_verified = &previous.prefix_verified_ms;

    let mut outcome = ClaudeCodeDrainOutcome::default();
    let mut resume_points = HashMap::with_capacity(files.len());
    let mut verified_ms = HashMap::with_capacity(files.len());
    let mut windows = HashMap::with_capacity(files.len());
    let mut requests = Vec::new();
    let mut set_aside = Vec::new();
    for (path, depth) in files {
        if stop.load(Ordering::Relaxed) {
            return Err(SourceDrainError::Stopped);
        }
        *outcome.files_by_depth.entry(depth).or_default() += 1;
        let stat = crate::refresher::harness_drain::FileStat::read(&path)
            .map_err(|error| format!("stat Claude Code session {}: {error}", path.display()))?;
        let point = previous_points.remove(&path);
        let check = if crate::refresher::harness_drain::prefix_check(
            &path,
            point.is_some(),
            previous_verified.get(&path).copied(),
            recorded_at_ms,
        ) == astrocyte_readers::sessions::jsonl_resume::PrefixCheck::Full
        {
            ClaudePrefixCheck::Full
        } else {
            ClaudePrefixCheck::Window
        };
        let window = previous.windows.get(&path);
        if let (Some(point), Some(window)) = (point.as_ref(), window) {
            if window.defer(&stat, recorded_at_ms, check == ClaudePrefixCheck::Full) {
                resume_points.insert(path.clone(), point.clone());
                if let Some(&verified) = previous_verified.get(&path) {
                    verified_ms.insert(path.clone(), verified);
                }
                windows.insert(path, window.clone());
                continue;
            }
        }
        let previous_offset = point.as_ref().map(ClaudeResumePoint::offset);
        let parse =
            parse_claude_file_resumable(&path, point, check, recorded_at_ms, quiet_window_ms);
        if let Some(reason) = parse.io_error {
            return Err(
                format!("read Claude Code session file {}: {reason}", path.display()).into(),
            );
        }
        let offset = parse.resume.as_ref().map(ClaudeResumePoint::offset);
        windows.insert(
            path.clone(),
            crate::refresher::harness_drain::WindowSchedule::after_read(
                window,
                stat.clone(),
                recorded_at_ms,
                parse.resumed && offset == previous_offset,
                offset == Some(stat.len) && parse.pending_calls == 0,
            ),
        );
        set_aside.extend(parse.set_aside.into_iter().map(|(line, reason)| {
            crate::refresher::SetAsideRecord {
                locator: format!("{path:?}:line:{line}"),
                reason,
            }
        }));
        if check == ClaudePrefixCheck::Full || !parse.resumed {
            verified_ms.insert(path.clone(), recorded_at_ms);
        } else if let Some(&verified) = previous_verified.get(&path) {
            verified_ms.insert(path.clone(), verified);
        }
        outcome.files_read += 1;
        if parse.resumed {
            outcome.files_resumed += 1;
        }
        outcome.hashed_bytes += parse.hashed_bytes;
        add_counts(&mut outcome.counts, &parse.counts);
        if parse.counts.skipped_lines > 0 {
            outcome
                .skipped_lines_by_file
                .insert(path.clone(), parse.counts.skipped_lines);
        }
        outcome.calls_pending += parse.pending_calls;
        outcome.calls_emitted += parse.calls.len();
        if let Some(point) = parse.resume {
            resume_points.insert(path.clone(), point);
        }
        for call in parse.calls {
            let locator = format!("{path:?}:line:{}", call.source_line);
            match map_call(call) {
                Ok(mut request) => {
                    request.work_class = work_class.map(str::to_owned);
                    requests.push(request);
                }
                Err(reason) => set_aside.push(crate::refresher::SetAsideRecord { locator, reason }),
            }
        }
    }

    Ok(ClaudeCodeRead {
        outcome,
        requests,
        set_aside,
        cache: ClaudeCodeCache {
            resume: resume_points,
            prefix_verified_ms: verified_ms,
            windows,
        },
    })
}

impl crate::refresher::harness_drain::HarnessSource for ClaudeCodeSource {
    fn source_key(&self) -> &'static str {
        CLAUDE_CODE_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok(
            (outcome.quarantined_redeliveries > 0 || outcome.counts.calls_refused > 0).then(|| {
                format!(
                    "{} Claude Code deliveries were quarantined and {} calls were refused",
                    outcome.quarantined_redeliveries, outcome.counts.calls_refused,
                )
            }),
        )
    }
}

fn add_counts(total: &mut ClaudeParseCounts, add: &ClaudeParseCounts) {
    total.lines += add.lines;
    total.skipped_lines += add.skipped_lines;
    total.usage_records += add.usage_records;
    total.records_without_request_id += add.records_without_request_id;
    total.synthetic_records += add.synthetic_records;
    total.calls_refused += add.calls_refused;
    total.calls_refused_after_emission += add.calls_refused_after_emission;
    total.replayed_records += add.replayed_records;
    total.replay_only_calls += add.replay_only_calls;
}

/// JSONL files and directory depths, preserving project/subagent ordering.
fn jsonl_files(root: &Path) -> std::io::Result<Vec<(PathBuf, usize)>> {
    crate::refresher::harness_drain::files_with_depth(root, &|path| {
        path.extension()
            .is_some_and(|extension| extension == "jsonl")
    })
}

fn map_call(call: ClaudeCodeCall) -> Result<EventRecordRequest, String> {
    // Role and unit follow the vocabulary `thalamus` events already use for
    // the same traffic, so one population is not described two ways. Kind
    // and source follow the other session readers: `billed` counters, with
    // the source naming the transcript field they came from.
    let usage = &call.usage;
    let measurements = [
        ("input", "token", "usage.input_tokens", usage.input),
        ("output", "token", "usage.output_tokens", usage.output),
        (
            "cache_read",
            "token",
            "usage.cache_read_input_tokens",
            usage.cache_read,
        ),
        (
            "cache_write",
            "token_cache_5m",
            "usage.cache_creation.ephemeral_5m_input_tokens",
            usage.cache_write_5m,
        ),
        (
            "cache_write",
            "token_cache_1h",
            "usage.cache_creation.ephemeral_1h_input_tokens",
            usage.cache_write_1h,
        ),
        // Only when the record had no 5m/1h split: stored without a lifetime,
        // like the other session readers' cache writes, rather than guessed.
        (
            "cache_write",
            "token",
            "usage.cache_creation_input_tokens",
            usage.cache_write_unsplit,
        ),
        // Claude Code reports no separate reasoning count; none is invented.
    ]
    .into_iter()
    .filter_map(|(role, unit, source, quantity)| {
        quantity.map(|quantity| (role, unit, source, quantity))
    })
    .map(|(role, unit, source, quantity)| {
        crate::refresher::harness_drain::measurement_with_unit(role, unit, source, Some(quantity))
            .map_err(|error| format!("Claude Code {error}"))
    })
    .collect::<Result<Vec<_>, String>>()?;

    let provider = call
        .model
        .as_deref()
        .and_then(inferred_provider_from_model)
        .map(str::to_string);
    Ok(EventRecordRequest {
        producer: CLAUDE_CODE_SOURCE_KEY.to_string(),
        // `requestId` is globally unique in the measured corpus (1,469
        // distinct ids, none in two files, one-to-one with `message.id`), so
        // a lineage would change identity without disambiguating anything.
        producer_lineage: None,
        producer_ref: call.request_id.clone(),
        prior_event_ref: None,
        harness: None,
        project_root: call.project_root,
        session: call.session_id,
        run_ref: None,
        flow_ref: None,
        work_class: None,
        occurred_at_ms: call.occurred_at_ms,
        emitted_at_ms: None,
        // Claude Code's stop_reason verbatim; absent for an interrupted call.
        terminal_status: call.stop_reason,
        attempt_ordinal: None,
        vendor_request_id: Some(call.request_id),
        provider,
        model: call.model,
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        // Nothing in a transcript says who paid for the call.
        charge_basis: ChargeBasis::Unknown,
        measurements,
        step_detail: Default::default(),
    })
}

/// Record every request in one transaction, skipping calls the `thalamus`
/// gateway already recorded.
///
/// Identity is per producer, so without the skip a call the gateway captured
/// on the wire would become a second event here. The gateway's events are
/// immutable and stay; the transcript's copy is the one not recorded.
pub(crate) fn record_requests(
    state: &SharedState,
    source_key: &str,
    requests: &[EventRecordRequest],
    set_aside: &[crate::refresher::SetAsideRecord],
    recorded_at_ms: i64,
    outcome: &mut ClaudeCodeDrainOutcome,
    stop_requested: &dyn Fn() -> bool,
) -> Result<(), SourceDrainError> {
    let mut stopped = false;
    let committed = state.ordered_write(|tx| {
        if requests.is_empty() && stop_requested() {
            stopped = true;
            return Err("Claude Code ingest stopped".into());
        }
        let newly_seen = crate::refresher::harness_drain::record_set_aside_in_tx(
            tx,
            source_key,
            set_aside,
            recorded_at_ms,
        )?;
        let mut dispositions = Vec::with_capacity(requests.len());
        let mut thalamus_skips = 0usize;
        let mut quarantined_refs = BTreeSet::new();
        for request in requests {
            if stop_requested() {
                stopped = true;
                return Err("Claude Code ingest stopped".to_string());
            }
            let recorded_by_gateway: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM usage_event
                                   WHERE producer=?1 AND vendor_request_id=?2)",
                    params![THALAMUS_PRODUCER, request.vendor_request_id],
                    |row| row.get(0),
                )
                .map_err(|error| format!("look up thalamus overlap: {error}"))?;
            if recorded_by_gateway {
                thalamus_skips += 1;
                continue;
            }
            let result = astrocyte_core::ingest::record_event_in_tx(
                tx,
                request,
                recorded_at_ms,
                astrocyte_core::ingest::DeliveryPath::Read,
            )
            .map_err(|error| format!("record Claude Code event: {error:?}"))?;
            if result.disposition
                == astrocyte_core::ingest::RecordDisposition::QuarantinedRedelivery
            {
                quarantined_refs.insert(request.producer_ref.clone());
            }
            dispositions.push(result.disposition);
        }
        crate::refresher::harness_drain::mark_current_in_tx(
            tx,
            source_key,
            "Claude Code",
            recorded_at_ms,
        )?;
        Ok((
            (dispositions, thalamus_skips, newly_seen, quarantined_refs),
            true,
        ))
    });
    let (dispositions, thalamus_skips, newly_seen, quarantined_refs) = match committed {
        Ok(committed) => committed,
        Err(_) if stopped => return Err(SourceDrainError::Stopped),
        Err(reason) => return Err(SourceDrainError::Failed(reason)),
    };
    crate::refresher::harness_drain::warn_set_aside(source_key, &newly_seen);
    outcome.thalamus_skips = thalamus_skips;
    let tally = crate::refresher::harness_drain::RecordTally::from_dispositions(dispositions);
    outcome.rows_recorded = tally.recorded;
    outcome.exact_redeliveries = tally.exact_redeliveries;
    outcome.quarantined_redeliveries = tally.quarantined_redeliveries;
    outcome.quarantined_refs = quarantined_refs;
    Ok(())
}

// Test fixtures use a fixed prefix window and a small number of lines.
#[allow(clippy::cast_possible_truncation)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::run_iteration_with_claude_code;
    use crate::state::test_state;
    use astrocyte_readers::sessions::claudecode::CLAUDE_PREFIX_WINDOW_BYTES;
    use std::collections::BTreeSet;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    #[test]
    fn set_aside_claude_overflow_keeps_good_calls_and_resume() {
        let temp = TempDir::new().unwrap();
        let path = write_file(
            temp.path(),
            "project/session.jsonl",
            &record("before", 5, Some("end_turn"), CWD),
        );
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());
        drain(&source, &state, 100);
        append(&path, &record("bad", 1_u64 << 63, Some("end_turn"), CWD));
        append(&path, &record("after", 7, Some("end_turn"), CWD));
        let committed_offset = source.cache.lock().unwrap().resume[&path].offset();
        state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER refuse_evidence BEFORE INSERT ON set_aside_record BEGIN SELECT RAISE(ABORT,'test failure'); END;").map_err(|e| e.to_string())).unwrap();
        assert!(source.drain(&state, 200, &NEVER_STOP).is_err());
        assert_eq!(event_refs(&state), BTreeSet::from(["before".into()]));
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
                BTreeSet::from(["before".into(), "after".into()])
            );
            assert_eq!(
                source.cache.lock().unwrap().resume[&path].offset(),
                fs::metadata(&path).unwrap().len()
            );
            crate::refresher::harness_drain::assert_set_aside(&state, "claude-code", 1);
        }
    }

    #[test]
    fn set_aside_good_claude_rows_are_byte_identical() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &(record("before", 5, Some("end_turn"), CWD)
                + &record("after", 7, Some("end_turn"), CWD)),
        );
        let state = test_state();
        drain(&ClaudeCodeSource::at(temp.path()), &state, 100);
        crate::refresher::harness_drain::assert_good_snapshot(
            &state,
            "claude-code",
            "b585886201701b5b0e2e3419fc7384183129d4719dbb026de0591cc8082f8665",
        );
    }

    #[cfg(unix)]
    #[test]
    fn set_aside_claude_unreadable_file_fails_drain_and_retains_resume() {
        use std::os::unix::fs::PermissionsExt;
        let temp = TempDir::new().unwrap();
        let path = write_file(
            temp.path(),
            "project/session.jsonl",
            &record("before", 5, Some("end_turn"), CWD),
        );
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());
        drain(&source, &state, 100);
        let offset = source.cache.lock().unwrap().resume[&path].offset();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o0)).unwrap();
        assert!(
            fs::File::open(&path).is_err(),
            "permission fixture must be unreadable"
        );
        run_iteration_with_claude_code(&state, &[], &source, &NEVER_STOP);
        assert!(state.producer_degraded_reason("claude-code").is_some());
        assert_eq!(source.cache.lock().unwrap().resume[&path].offset(), offset);
        assert_eq!(event_refs(&state), BTreeSet::from(["before".into()]));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    const WINDOW: i64 = CLAUDE_CODE_QUIET_WINDOW_MS;
    const CWD: &str = "/work/a.b-c";

    /// One per-block record of call `request`. Every call carries input 3,
    /// cache read 5, and a 5m/1h cache-write split of 2/5 (flat total 7).
    fn record(request: &str, output: u64, stop: Option<&str>, cwd: &str) -> String {
        let stop = stop.map_or("null".to_string(), |stop| format!("\"{stop}\""));
        format!(
            r#"{{"type":"assistant","timestamp":"2026-09-01T00:00:01.000Z","requestId":"{request}","sessionId":"session-1","cwd":"{cwd}","message":{{"id":"msg_{request}","model":"claude-opus-5","stop_reason":{stop},"usage":{{"input_tokens":3,"output_tokens":{output},"cache_read_input_tokens":5,"cache_creation_input_tokens":7,"cache_creation":{{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":5}}}}}}}}"#
        ) + "\n"
    }

    /// Writes `contents` at `root/<relative>` and returns the path.
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

    fn drain(source: &ClaudeCodeSource, state: &SharedState, at: i64) -> ClaudeCodeDrainOutcome {
        source.drain(state, at, &NEVER_STOP).unwrap()
    }

    fn event_refs(state: &SharedState) -> BTreeSet<String> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT producer_ref FROM usage_event WHERE producer='claude-code'")
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

    /// `(terminal_status, project_root, vendor_request_id)` of one event.
    fn event_columns(
        state: &SharedState,
        producer_ref: &str,
    ) -> (Option<String>, Option<String>, Option<String>) {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT terminal_status,project_root,vendor_request_id FROM usage_event
                     WHERE producer='claude-code' AND producer_ref=?1",
                    [producer_ref],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn measurements(state: &SharedState, producer_ref: &str) -> BTreeMap<(String, String), i64> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT measurement.role,measurement.unit,measurement.quantity_units
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='claude-code'
                           AND usage_event.producer_ref=?1",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([producer_ref], |row| {
                        Ok(((row.get(0)?, row.get(1)?), row.get(2)?))
                    })
                    .map_err(|error| error.to_string())?
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map_err(|error| error.to_string());
                rows
            })
            .unwrap()
    }

    fn measured(state: &SharedState, producer_ref: &str, role: &str, unit: &str) -> Option<i64> {
        measurements(state, producer_ref)
            .get(&(role.to_string(), unit.to_string()))
            .copied()
    }

    /// Claude Code writes one record per content block, each carrying the
    /// whole response's usage. Summing them would count the call once per
    /// block; the ledger must hold one event at the per-field maximum.
    #[test]
    fn per_block_copies_of_one_call_become_one_event_at_the_per_field_max() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &[
                record("req_1", 5, Some("tool_use"), CWD),
                record("req_1", 12, Some("tool_use"), CWD),
                record("req_1", 12, Some("tool_use"), CWD),
            ]
            .concat(),
        );
        let state = test_state();
        let outcome = drain(&ClaudeCodeSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.counts.usage_records, 3);
        assert_eq!(outcome.rows_recorded, 1);
        assert_eq!(event_refs(&state), BTreeSet::from(["req_1".to_string()]));
        assert_eq!(measured(&state, "req_1", "output", "token"), Some(12));
        assert_eq!(measured(&state, "req_1", "input", "token"), Some(3));
        assert_eq!(measured(&state, "req_1", "cache_read", "token"), Some(5));
    }

    /// Measured copies only ever grow. A later copy with a lower counter is
    /// the one shape meaning Claude Code changed how it writes usage, so the
    /// call is refused instead of stored at a maximum; other calls ingest.
    #[test]
    fn a_later_copy_with_a_lower_counter_refuses_the_call() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &[
                record("req_1", 12, None, CWD),
                record("req_1", 5, Some("end_turn"), CWD),
                record("req_2", 4, Some("end_turn"), CWD),
            ]
            .concat(),
        );
        let state = test_state();
        let outcome = drain(&ClaudeCodeSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.counts.calls_refused, 1);
        assert_eq!(outcome.rows_recorded, 1);
        assert_eq!(event_refs(&state), BTreeSet::from(["req_2".to_string()]));
    }

    /// Subagent transcripts carry `stop_reason` only on a call's last copy,
    /// with null on the earlier ones. "Any copy has it" is the completion
    /// rule; requiring it on every copy would leave the call waiting.
    #[test]
    fn a_subagent_call_with_stop_reason_only_on_its_last_copy_is_complete() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session-1/subagents/agent-a1.jsonl",
            &[
                record("req_1", 5, None, CWD),
                record("req_1", 5, None, CWD),
                record("req_1", 5, Some("tool_use"), CWD),
            ]
            .concat(),
        );
        let state = test_state();
        let outcome = drain(&ClaudeCodeSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.files_by_depth, BTreeMap::from([(3, 1)]));
        assert_eq!(outcome.calls_pending, 0);
        assert_eq!(outcome.rows_recorded, 1);
        assert_eq!(
            event_columns(&state, "req_1").0,
            Some("tool_use".to_string())
        );
    }

    /// A call that never got a stop_reason waits while its file might still
    /// grow, then is ingested exactly once after the file has been quiet for
    /// the window, with no terminal status.
    #[test]
    fn an_interrupted_call_is_ingested_once_after_the_quiet_window_with_null_status() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &record("req_1", 5, None, CWD),
        );
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());

        let first = drain(&source, &state, 1_000);
        assert_eq!((first.rows_recorded, first.calls_pending), (0, 1));
        let just_before = drain(&source, &state, 1_000 + WINDOW - 1);
        assert_eq!(
            (just_before.rows_recorded, just_before.calls_pending),
            (0, 1)
        );

        let quiet = drain(&source, &state, 1_000 + WINDOW);
        assert_eq!((quiet.rows_recorded, quiet.calls_pending), (1, 0));
        assert_eq!(event_columns(&state, "req_1").0, None);

        let later = drain(&source, &state, 1_000 + 3 * WINDOW);
        assert_eq!(later.calls_emitted, 0, "ingested once, never upgraded");
        assert_eq!((later.rows_recorded, later.exact_redeliveries), (0, 0));
    }

    /// A slow-but-live call: its second copy arrives just under one window
    /// after the first. It must still be live then, and complete normally
    /// when the copy with its stop_reason lands. Tightening the window below
    /// this gap ingests it early as interrupted, and the later completion
    /// then quarantines, which reddens this test. The gap is a literal, not
    /// derived from the constant, so changing the constant cannot move the
    /// fixture along with it.
    #[test]
    fn a_call_whose_copies_straddle_a_gap_just_under_the_window_stays_live() {
        const STRADDLING_GAP_MS: i64 = 10 * 60 * 1000 - 1;
        let temp = TempDir::new().unwrap();
        let path = write_file(
            temp.path(),
            "project/session-1/subagents/agent-a1.jsonl",
            &record("req_1", 5, None, CWD),
        );
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());
        drain(&source, &state, 1_000);
        let straddling = drain(&source, &state, 1_000 + STRADDLING_GAP_MS);
        assert_eq!(straddling.rows_recorded, 0, "still live inside the window");

        append(&path, &record("req_1", 9, Some("end_turn"), CWD));
        let completed = drain(&source, &state, 1_000 + STRADDLING_GAP_MS);
        let settled = drain(&source, &state, 1_000 + 4 * STRADDLING_GAP_MS);
        assert_eq!(completed.rows_recorded, 1);
        assert_eq!(
            completed.quarantined_redeliveries + settled.quarantined_redeliveries,
            0
        );
        assert_eq!(
            event_columns(&state, "req_1").0,
            Some("end_turn".to_string())
        );
    }

    /// A call the `thalamus` gateway already recorded (joined on its request
    /// id) is skipped, so the same call is not counted twice; the gateway's
    /// event stays. Every recorded event carries its request id.
    #[test]
    fn a_call_already_recorded_by_thalamus_is_skipped() {
        let state = test_state();
        seed_thalamus(&state, &["req_gateway"]);
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &[
                record("req_gateway", 5, Some("end_turn"), CWD),
                record("req_local", 5, Some("end_turn"), CWD),
            ]
            .concat(),
        );
        let outcome = drain(&ClaudeCodeSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.calls_emitted, 2);
        assert_eq!(outcome.thalamus_skips, 1);
        assert_eq!(outcome.rows_recorded, 1);
        assert_eq!(
            event_refs(&state),
            BTreeSet::from(["req_local".to_string()])
        );
        assert_eq!(
            event_columns(&state, "req_local").2,
            Some("req_local".to_string())
        );
    }

    /// 5-minute and 1-hour cache writes are priced differently, so they are
    /// two measurements, never one summed cache write.
    #[test]
    fn five_minute_and_one_hour_cache_writes_are_separate_measurements() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &record("req_1", 5, Some("end_turn"), CWD),
        );
        let state = test_state();
        drain(&ClaudeCodeSource::at(temp.path()), &state, 0);
        let cache_writes: Vec<_> = measurements(&state, "req_1")
            .into_iter()
            .filter(|((role, _), _)| role == "cache_write")
            .collect();
        assert_eq!(
            cache_writes,
            vec![
                (("cache_write".to_string(), "token_cache_1h".to_string()), 5),
                (("cache_write".to_string(), "token_cache_5m".to_string()), 2),
            ]
        );
    }

    /// The project directory's name collapses `/`, `-` and `.` into one
    /// character, so decoding it misreads paths like `/work/a.b-c`. Each
    /// event takes its root from its own record's cwd, so a session that
    /// moved between directories keeps one root per call.
    #[test]
    fn project_root_comes_from_each_records_cwd_not_the_directory_name() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "-work-a-b-c/session.jsonl",
            &[
                record("req_1", 5, Some("end_turn"), "/work/a.b-c"),
                record("req_2", 5, Some("end_turn"), "/other/x-y"),
            ]
            .concat(),
        );
        let state = test_state();
        drain(&ClaudeCodeSource::at(temp.path()), &state, 0);
        assert_eq!(
            event_columns(&state, "req_1").1,
            Some("/work/a.b-c".to_string())
        );
        assert_eq!(
            event_columns(&state, "req_2").1,
            Some("/other/x-y".to_string())
        );
    }

    /// A corrupt complete line is counted per file and reading continues past
    /// it. An unterminated tail is a writer mid-append: not consumed, not
    /// counted, and ingested once its newline lands.
    #[test]
    fn a_corrupt_complete_line_is_counted_and_a_partial_tail_is_not() {
        let temp = TempDir::new().unwrap();
        let tail = record("req_3", 5, Some("end_turn"), CWD);
        let (head, rest) = tail.split_at(tail.len() / 2);
        let path = write_file(
            temp.path(),
            "project/session.jsonl",
            &[
                record("req_1", 5, Some("end_turn"), CWD),
                "{\"type\":\"assistant\",not json\n".to_string(),
                record("req_2", 5, Some("end_turn"), CWD),
                head.to_string(),
            ]
            .concat(),
        );
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());
        let first = drain(&source, &state, 0);
        assert_eq!(
            first.skipped_lines_by_file,
            BTreeMap::from([(path.clone(), 1)])
        );
        assert_eq!(
            first.rows_recorded, 2,
            "the line after the corrupt one ingests"
        );

        append(&path, rest);
        let second = drain(&source, &state, 1);
        assert_eq!(second.files_resumed, 1);
        assert!(second.skipped_lines_by_file.is_empty());
        assert_eq!(second.rows_recorded, 1);
        assert_eq!(event_refs(&state).len(), 3);
    }

    /// Per-tick cost scales with new bytes, not with the file: a grown file
    /// hashes one window plus what was appended, and a quiet file one
    /// window. Asserted on the reader's byte and line counters, not time.
    #[test]
    fn steady_state_tick_cost_is_bounded_by_the_window_and_the_appended_bytes() {
        let temp = TempDir::new().unwrap();
        let filler = "{\"type\":\"last-prompt\",\"text\":\"padding padding padding\"}\n";
        let lines = (8 * CLAUDE_PREFIX_WINDOW_BYTES as usize) / filler.len() + 1;
        let contents = filler.repeat(lines) + &record("req_1", 5, Some("end_turn"), CWD);
        let path = write_file(temp.path(), "project/session.jsonl", &contents);
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());
        drain(&source, &state, 0);

        let appended = record("req_2", 5, Some("end_turn"), CWD);
        append(&path, &appended);
        let grown = drain(&source, &state, 1);
        assert_eq!((grown.files_resumed, grown.counts.lines), (1, 1));
        let bound = 2 * CLAUDE_PREFIX_WINDOW_BYTES + appended.len() as u64;
        assert!(
            grown.hashed_bytes <= bound,
            "{} > {bound}",
            grown.hashed_bytes
        );
        assert!(grown.hashed_bytes < contents.len() as u64 / 2);

        let quiet = drain(&source, &state, 2);
        assert_eq!((quiet.files_resumed, quiet.counts.lines), (1, 0));
        assert!(quiet.hashed_bytes <= 2 * CLAUDE_PREFIX_WINDOW_BYTES);
    }

    #[test]
    fn refresher_iteration_drains_claude_code_and_clears_its_health() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            "project/session.jsonl",
            &record("req_1", 5, Some("end_turn"), CWD),
        );
        let state = test_state();
        let source = ClaudeCodeSource::at(temp.path());
        run_iteration_with_claude_code(&state, &[], &source, &NEVER_STOP);
        assert_eq!(event_refs(&state).len(), 1);
        assert!(state.producer_degraded_reason("claude-code").is_none());
    }

    #[test]
    fn absent_projects_root_is_no_lane() {
        let temp = TempDir::new().unwrap();
        let state = test_state();
        let source = ClaudeCodeSource::from_home(Some(temp.path().as_os_str().to_owned()));
        assert!(drain(&source, &state, 0).root_absent);
        let relative = ClaudeCodeSource::from_home(Some(OsString::from("relative")));
        assert!(relative.drain(&state, 0, &NEVER_STOP).is_err());
    }

    /// Minimal `thalamus` events carrying `request_ids` as vendor request ids.
    fn seed_thalamus(state: &SharedState, request_ids: &[&str]) {
        state
            .ordered_write(|tx| {
                for (index, request_id) in request_ids.iter().enumerate() {
                    let request = EventRecordRequest {
                        producer: THALAMUS_PRODUCER.to_string(),
                        producer_lineage: Some("gateway-lineage".to_string()),
                        producer_ref: index.to_string(),
                        prior_event_ref: None,
                        harness: Some("claude-code".to_string()),
                        project_root: None,
                        session: None,
                        run_ref: None,
                        flow_ref: None,
                        work_class: None,
                        occurred_at_ms: None,
                        emitted_at_ms: None,
                        terminal_status: Some("completed".to_string()),
                        attempt_ordinal: None,
                        vendor_request_id: Some((*request_id).to_string()),
                        provider: Some("anthropic".to_string()),
                        model: None,
                        account_ref: None,
                        auth_selection: None,
                        charge_basis_mapping_version: None,
                        charge_basis_overrides: None,
                        charge_basis: ChargeBasis::Unknown,
                        measurements: Vec::new(),
                        step_detail: Default::default(),
                    };
                    astrocyte_core::ingest::record_event_in_tx(
                        tx,
                        &request,
                        0,
                        astrocyte_core::ingest::DeliveryPath::Read,
                    )
                    .map_err(|error| format!("{error:?}"))?;
                }
                Ok(((), true))
            })
            .unwrap();
    }

    /// Raw records (assistant records with usage and a request id, on
    /// complete lines) and their distinct request ids, counted by a plain
    /// JSON scan that shares no code with the reader.
    fn raw_population(files: &[PathBuf]) -> (u64, BTreeSet<String>) {
        let mut records = 0;
        let mut ids = BTreeSet::new();
        for path in files {
            let bytes = fs::read(path).unwrap();
            let complete = match bytes.iter().rposition(|&byte| byte == b'\n') {
                Some(end) => &bytes[..end],
                None => continue,
            };
            for line in complete.split(|&byte| byte == b'\n') {
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) else {
                    continue;
                };
                let request = value["requestId"].as_str();
                if value["type"] == "assistant"
                    && value["message"]["usage"].is_object()
                    && value["message"]["model"] != "<synthetic>"
                    && request.is_some_and(|id| !id.is_empty())
                {
                    records += 1;
                    ids.insert(request.unwrap().to_string());
                }
            }
        }
        (records, ids)
    }

    /// Run with `--release --ignored`. Reads the live corpus (never writes
    /// it) into an in-memory TEMPORARY store. To measure the gateway overlap,
    /// export thalamus request ids to a file first and point
    /// `ASTROCYTE_THALAMUS_REQUEST_IDS` at it (one id per line); this test
    /// never opens the production store.
    #[test]
    #[ignore = "reads the mutable ~/.claude/projects corpus"]
    fn live_claude_code_corpus_populations() {
        let root = std::env::var_os("CLAUDE_PROJECTS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap()).join(".claude/projects")
            });
        let state = test_state();
        let thalamus_ids: Vec<String> = std::env::var_os("ASTROCYTE_THALAMUS_REQUEST_IDS")
            .map(|path| {
                fs::read_to_string(path)
                    .unwrap()
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        seed_thalamus(
            &state,
            &thalamus_ids.iter().map(String::as_str).collect::<Vec<_>>(),
        );

        let files: Vec<PathBuf> = jsonl_files(&root)
            .unwrap()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        let (raw_records, raw_ids) = raw_population(&files);

        let source = ClaudeCodeSource::at(&root);
        let now = 4_000_000_000_000;
        let first = drain(&source, &state, now);
        // No file grows between these drains, so the second one sees every
        // call that never got a stop_reason as interrupted.
        let second = drain(&source, &state, now + CLAUDE_CODE_QUIET_WINDOW_MS);
        let ledger_events = event_refs(&state).len();
        let emitted = first.calls_emitted + second.calls_emitted;
        let thalamus_skips = first.thalamus_skips + second.thalamus_skips;
        let quarantines = first.quarantined_redeliveries + second.quarantined_redeliveries;
        let overlap = thalamus_ids
            .iter()
            .filter(|id| raw_ids.contains(id.as_str()))
            .count();

        eprintln!("files by depth: {:?}", first.files_by_depth);
        eprintln!(
            "raw records: {raw_records}; distinct request ids: {}",
            raw_ids.len()
        );
        eprintln!(
            "reader: usage records {}, lines {}, skipped lines {:?}, synthetic {}, \
             without request id {}, refused {}, emitted calls {emitted} \
             (first drain {}, after the quiet window {}), pending {}",
            first.counts.usage_records,
            first.counts.lines,
            first.skipped_lines_by_file,
            first.counts.synthetic_records,
            first.counts.records_without_request_id,
            first.counts.calls_refused,
            first.calls_emitted,
            second.calls_emitted,
            second.calls_pending,
        );
        eprintln!(
            "ledger events: {ledger_events}; thalamus ids {}, overlapping {overlap}, \
             skipped {thalamus_skips}; quarantines {quarantines}",
            thalamus_ids.len()
        );

        assert_eq!(quarantines, 0);
        assert_eq!(first.counts.usage_records, raw_records);
        assert_eq!(first.counts.calls_refused, 0);
        assert_eq!(second.calls_pending, 0);
        assert_eq!(emitted, raw_ids.len(), "one emitted call per request id");
        assert_eq!(thalamus_skips, overlap);
        assert_eq!(ledger_events, emitted - thalamus_skips);
    }
}
