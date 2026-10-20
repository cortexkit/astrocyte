//! Process-local read deadlines, published with the drain's committed resume points.

use std::path::Path;
use std::time::SystemTime;

use astrocyte_readers::file_identity::{inode_of, same_identity};

pub(crate) const DAILY_VERIFY_MS: i64 = 24 * 60 * 60 * 1000;
const QUIET_AFTER_MS: i64 = 60 * 60 * 1000;
const QUIET_CHECK_MS: i64 = 5 * 60 * 1000;

/// The next occurrence of a path's fixed daily phase, strictly after verification.
/// Inventory growth cannot move existing deadlines. The first wait is at most a
/// day, not a day plus a phase offset. Whole due files are never budget-delayed:
/// an oversized due file may exceed any soft per-drain byte budget.
pub(crate) fn full_due(path: &Path, verified: Option<i64>, now: i64, interval: i64) -> bool {
    verified.is_none_or(|verified| {
        let phase = phase(path, interval);
        let wait = interval - (verified.rem_euclid(interval) - phase).rem_euclid(interval);
        now < verified || now.saturating_sub(verified) >= wait
    })
}

pub(super) fn phase(path: &Path, interval: i64) -> i64 {
    let hash = path
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
        });
    i64::try_from(hash % u64::try_from(interval).expect("positive verification interval"))
        .expect("phase fits the interval")
}

#[derive(Debug, Clone)]
pub(crate) struct FileStat {
    pub(crate) len: u64,
    modified: Option<SystemTime>,
    identity: Option<(u64, u64)>,
}

impl FileStat {
    pub(crate) fn read(path: &Path) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let metadata = file.metadata()?;
        let stat = Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            identity: inode_of(&file, &metadata),
        };
        #[cfg(test)]
        {
            AFTER_STAT.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            if let Some(fake) = STAT_OVERRIDE.with(|value| value.borrow().clone()) {
                return Ok(fake);
            }
        }
        Ok(stat)
    }

    fn same(&self, other: &Self) -> bool {
        self.len == other.len
            && self.modified.is_some()
            && self.modified == other.modified
            && same_identity(self.identity, other.identity)
    }
}

#[cfg(test)]
thread_local! {
    // Simulate stale file metadata and a stop after stat, before the drain commits.
    static STAT_OVERRIDE: std::cell::RefCell<Option<FileStat>> = const { std::cell::RefCell::new(None) };
    static AFTER_STAT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub(crate) struct WindowSchedule {
    stat: FileStat,
    changed_ms: i64,
    checked_ms: i64,
    ready: bool,
}

impl WindowSchedule {
    pub(crate) fn defer(&self, stat: &FileStat, now: i64, full_due: bool) -> bool {
        // One observed quiet hour avoids backing off pauses in active sessions.
        // Stat can only wake a file early; it never renews the content deadline.
        self.ready
            && !full_due
            && self.stat.same(stat)
            && now >= self.checked_ms
            && now.saturating_sub(self.changed_ms) >= QUIET_AFTER_MS
            && now.saturating_sub(self.checked_ms) < QUIET_CHECK_MS
    }

    pub(crate) fn after_read(
        previous: Option<&Self>,
        stat: FileStat,
        now: i64,
        unchanged: bool,
        ready: bool,
    ) -> Self {
        let changed_ms = previous
            .filter(|previous| unchanged && previous.stat.same(&stat) && now >= previous.checked_ms)
            .map_or(now, |previous| previous.changed_ms);
        Self {
            stat,
            changed_ms,
            checked_ms: now,
            ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::{
        claude_code_source::ClaudeCodeSource, codex_source::CodexSource,
        gemini_source::GeminiSource, kimi_source::KimiSource, omp_source::OmpSource,
        pi_source::PiSource, SourceDrainError,
    };
    use crate::state::{test_state, SharedState};
    use std::sync::{atomic::AtomicBool, Arc};

    const DAY: i64 = 86_400_000;
    const HOUR: i64 = 3_600_000;
    const FIVE_MINUTES: i64 = 300_000;

    struct Tick {
        hashed: u64,
        recorded: usize,
        quarantined: usize,
    }

    enum Reader {
        Pi(PiSource),
        Claude(ClaudeCodeSource),
        Codex(CodexSource),
        Omp(OmpSource),
        Kimi(KimiSource),
        Gemini(GeminiSource),
    }

    impl Reader {
        fn key(&self) -> &str {
            match self {
                Self::Pi(_) => "pi",
                Self::Claude(_) => "claude-code",
                Self::Codex(_) => "codex",
                Self::Omp(_) => "omp",
                Self::Kimi(_) => "kimi",
                Self::Gemini(_) => "gemini",
            }
        }

        fn drain(
            &self,
            state: &SharedState,
            now: i64,
            stop: &AtomicBool,
        ) -> Result<Tick, SourceDrainError> {
            macro_rules! tick {
                ($source:expr) => {
                    $source.drain(state, now, stop).map(|outcome| Tick {
                        hashed: outcome.hashed_bytes,
                        recorded: outcome.rows_recorded,
                        quarantined: outcome.quarantined_redeliveries,
                    })
                };
            }
            match self {
                Self::Pi(source) => tick!(source),
                Self::Claude(source) => tick!(source),
                Self::Codex(source) => tick!(source),
                Self::Omp(source) => tick!(source),
                Self::Kimi(source) => tick!(source),
                Self::Gemini(source) => tick!(source),
            }
        }

        fn record(&self, id: &str, input: u64) -> String {
            match self {
                Self::Pi(_) => pi_record(id, input),
                Self::Claude(_) => format!("{{\"type\":\"assistant\",\"timestamp\":\"2026-09-01T00:00:01Z\",\"requestId\":\"{id}\",\"sessionId\":\"session\",\"cwd\":\"/work\",\"message\":{{\"id\":\"msg-{id}\",\"model\":\"claude-sonnet-4\",\"stop_reason\":\"end_turn\",\"usage\":{{\"input_tokens\":{input},\"cached_input_tokens\":0,\"output_tokens\":7,\"reasoning_output_tokens\":0}}}}}}\n"),
                Self::Codex(_) => format!("{{\"timestamp\":\"2026-09-01T00:00:01Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\",\"turn_id\":\"{id}\"}}}}\n{{\"type\":\"turn_context\",\"payload\":{{\"turn_id\":\"{id}\",\"model\":\"gpt-5\"}}}}\n{{\"timestamp\":\"2026-09-01T00:00:02Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":{{\"total_token_usage\":{{\"input_tokens\":{input},\"cached_input_tokens\":0,\"output_tokens\":7,\"reasoning_output_tokens\":0}},\"last_token_usage\":{{\"input_tokens\":{input},\"cached_input_tokens\":0,\"output_tokens\":7,\"reasoning_output_tokens\":0}}}}}}}}\n"),
                Self::Omp(_) => format!("{{\"type\":\"message\",\"id\":\"{id}\",\"timestamp\":\"2026-09-01T00:00:01Z\",\"message\":{{\"role\":\"assistant\",\"provider\":\"openai-codex\",\"model\":\"gpt-5\",\"usage\":{{\"input\":{input},\"output\":7}},\"stopReason\":\"stop\",\"responseId\":\"resp-{id}\"}}}}\n"),
                Self::Kimi(_) => format!("{{\"timestamp\":1771258960.612596,\"message\":{{\"type\":\"StatusUpdate\",\"payload\":{{\"token_usage\":{{\"input_other\":{input},\"output\":7,\"input_cache_read\":0,\"input_cache_creation\":0}},\"message_id\":\"{id}\"}}}}}}\n"),
                Self::Gemini(_) => format!("{{\"id\":\"{id}\",\"timestamp\":\"2026-09-01T00:00:01Z\",\"type\":\"gemini\",\"tokens\":{{\"input\":{input},\"output\":7,\"cached\":0,\"thoughts\":0,\"tool\":0}},\"model\":\"gemini-3-flash-preview\"}}\n"),
            }
        }
    }

    fn pi_record(id: &str, input: u64) -> String {
        format!("{{\"type\":\"message\",\"id\":\"entry-{id}\",\"timestamp\":\"2026-09-01T00:00:01Z\",\"message\":{{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-sonnet-4\",\"responseId\":\"{id}\",\"usage\":{{\"input\":{input},\"output\":7}}}}}}\n")
    }

    fn fixtures(root: &Path, large: bool) -> Vec<(Reader, std::path::PathBuf, String)> {
        let session = "{\"type\":\"session\",\"version\":3,\"id\":\"session\",\"cwd\":\"/work\",\"timestamp\":\"2026-09-01T00:00:00Z\"}\n";
        let mut result = Vec::new();
        for (reader, relative, header) in [
            (Reader::Pi(PiSource::at(root.join("pi"))), "pi/session.jsonl", session),
            (Reader::Claude(ClaudeCodeSource::at(root.join("claude"))), "claude/project/session.jsonl", ""),
            (Reader::Codex(CodexSource::at(root.join("codex"))), "codex/sessions/rollout.jsonl", "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session\",\"cwd\":\"/work\",\"model_provider\":\"openai\"}}\n"),
            (Reader::Omp(OmpSource::at(root.join("omp"))), "omp/session.jsonl", session),
            (Reader::Kimi(KimiSource::at(root.join("kimi"))), "kimi/project/session/wire.jsonl", ""),
            (Reader::Gemini(GeminiSource::at(root.join("gemini"))), "gemini/proj/chats/session-test.jsonl", "{\"sessionId\":\"session\",\"projectHash\":\"project\",\"kind\":\"main\"}\n"),
        ] {
            let path = root.join(relative);
            let mut contents = format!("{header}{}", reader.record("one", 11));
            if large {
                contents.push_str(&format!("{{\"type\":\"padding\",\"text\":\"{}\"}}\n", "x".repeat(131_072)));
            }
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &contents).unwrap();
            result.push((reader, path, contents));
        }
        result
    }

    fn success(state: &SharedState, key: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT last_success_ms FROM source_runtime WHERE source_key=?1",
                    [key],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn count(state: &SharedState, table: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn assert_changed_payload(state: &SharedState) {
        let changed: i64 = state.with_conn(|conn| conn.query_row(
            "SELECT COUNT(*) FROM rejected_delivery, json_tree(incoming_payload) WHERE json_tree.key='quantity_units' AND json_tree.atom=19",
            [], |row| row.get(0),
        ).map_err(|error| error.to_string())).unwrap();
        assert!(changed > 0, "the changed quantity must reach quarantine");
    }

    struct FakeStat;
    impl FakeStat {
        fn set(stat: FileStat) -> Self {
            STAT_OVERRIDE.with(|value| *value.borrow_mut() = Some(stat));
            Self
        }
    }
    impl Drop for FakeStat {
        fn drop(&mut self) {
            STAT_OVERRIDE.with(|value| *value.borrow_mut() = None);
        }
    }

    #[test]
    fn quiet_unexpired_tick_reads_no_content_but_commits_health() {
        for large in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            for (reader, path, _) in fixtures(temp.path(), large) {
                let state = test_state();
                let stop = AtomicBool::new(false);
                let start = phase(&path, DAY);
                assert_eq!(reader.drain(&state, start, &stop).unwrap().recorded, 1);
                assert!(reader.drain(&state, start + HOUR, &stop).unwrap().hashed > 0);
                let now = start + HOUR + 1;
                assert_eq!(
                    reader.drain(&state, now, &stop).unwrap().hashed,
                    0,
                    "{}",
                    reader.key()
                );
                assert_eq!(success(&state, reader.key()), now);
                assert_eq!(count(&state, "usage_event"), 1);
            }
        }
    }

    #[test]
    fn quiet_five_minute_bound_catches_hidden_rewrite_and_append() {
        for append in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            for (reader, path, contents) in fixtures(temp.path(), false) {
                let state = test_state();
                let stop = AtomicBool::new(false);
                let start = phase(&path, DAY);
                reader.drain(&state, start, &stop).unwrap();
                reader.drain(&state, start + HOUR, &stop).unwrap();
                let stat = FileStat::read(&path).unwrap();
                let changed = if append {
                    format!("{contents}{}", reader.record("two", 21))
                } else {
                    contents.replace(":11", ":19")
                };
                std::fs::write(&path, changed).unwrap();
                // Model a stale stat snapshot, including a hidden length change.
                // The actual reader must still discover the bytes at the deadline.
                let _fake = FakeStat::set(stat);
                assert_eq!(
                    reader
                        .drain(&state, start + HOUR + FIVE_MINUTES - 1, &stop)
                        .unwrap()
                        .hashed,
                    0
                );
                let due = reader
                    .drain(&state, start + HOUR + FIVE_MINUTES, &stop)
                    .unwrap();
                assert!(due.hashed > 0, "{}", reader.key());
                assert_eq!(due.recorded, usize::from(append));
                assert_eq!(
                    due.quarantined,
                    usize::from(!append),
                    "{} append={append}",
                    reader.key()
                );
                assert_eq!(count(&state, "usage_event"), if append { 2 } else { 1 });
                assert_eq!(count(&state, "rejected_delivery"), i64::from(!append));
                if !append {
                    assert_changed_payload(&state);
                }
                assert!(
                    reader
                        .drain(&state, start + HOUR + FIVE_MINUTES + 1, &stop)
                        .unwrap()
                        .hashed
                        > 0,
                    "a content change must restore active cadence"
                );
            }
        }
    }

    #[test]
    fn quiet_metadata_change_wakes_before_the_window_deadline() {
        for field in ["size", "mtime", "identity"] {
            let temp = tempfile::tempdir().unwrap();
            for (reader, path, contents) in fixtures(temp.path(), false) {
                let state = test_state();
                let stop = AtomicBool::new(false);
                let start = phase(&path, DAY);
                reader.drain(&state, start, &stop).unwrap();
                reader.drain(&state, start + HOUR, &stop).unwrap();
                let mut stat = FileStat::read(&path).unwrap();
                std::fs::write(&path, contents.replace(":11", ":19")).unwrap();
                match field {
                    "size" => stat.len += 1,
                    "mtime" => stat.modified = Some(SystemTime::UNIX_EPOCH),
                    _ => stat.identity = Some((u64::MAX, u64::MAX)),
                }
                let _fake = FakeStat::set(stat);
                let tick = reader.drain(&state, start + HOUR + 1, &stop).unwrap();
                assert!(tick.hashed > 0, "{}: {field}", reader.key());
                assert_eq!(tick.quarantined, 1, "{} {field}", reader.key());
                assert_eq!(count(&state, "rejected_delivery"), 1);
            }
        }
    }

    #[test]
    fn daily_literal_deadline_overrides_quiet_deferral_and_quarantines_old_bytes() {
        let temp = tempfile::tempdir().unwrap();
        for (reader, path, contents) in fixtures(temp.path(), true) {
            let state = test_state();
            let stop = AtomicBool::new(false);
            let start = phase(&path, DAY);
            reader.drain(&state, start, &stop).unwrap();
            let stat = FileStat::read(&path).unwrap();
            std::fs::write(&path, contents.replace(":11", ":19")).unwrap();
            let _fake = FakeStat::set(stat);
            let before = reader.drain(&state, start + DAY - 1, &stop).unwrap();
            assert_eq!(before.hashed, 65_536);
            assert_eq!(before.quarantined, 0);
            let due = reader.drain(&state, start + DAY, &stop).unwrap();
            assert!(due.hashed > 131_072, "{}", reader.key());
            assert_eq!(due.quarantined, 1, "{}", reader.key());
            assert_eq!(count(&state, "rejected_delivery"), 1);
            assert_changed_payload(&state);
            assert_eq!(count(&state, "usage_event"), 1);
        }
    }

    #[test]
    fn stopped_or_rolled_back_drain_does_not_spend_a_daily_check() {
        for stopped in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            for (reader, path, _) in fixtures(temp.path(), true) {
                let state = test_state();
                let stop = Arc::new(AtomicBool::new(false));
                let start = phase(&path, DAY);
                reader.drain(&state, start, &stop).unwrap();
                if stopped {
                    let stop = stop.clone();
                    AFTER_STAT.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move || {
                            stop.store(true, std::sync::atomic::Ordering::Relaxed)
                        }))
                    });
                } else {
                    state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER refuse_health BEFORE UPDATE ON source_runtime BEGIN SELECT RAISE(ABORT,'test rollback'); END;").map_err(|error| error.to_string())).unwrap();
                }
                assert!(reader.drain(&state, start + DAY, &stop).is_err());
                assert_eq!(success(&state, reader.key()), start);
                stop.store(false, std::sync::atomic::Ordering::Relaxed);
                state
                    .with_conn(|conn| {
                        conn.execute_batch("DROP TRIGGER IF EXISTS refuse_health")
                            .map_err(|error| error.to_string())
                    })
                    .unwrap();
                assert!(reader.drain(&state, start + DAY, &stop).unwrap().hashed > 131_072);
                assert_eq!(success(&state, reader.key()), start + DAY);
                assert_eq!(count(&state, "usage_event"), 1);
            }
        }
    }

    #[test]
    fn daily_checks_are_not_postponed_by_appends_on_every_tick() {
        let temp = tempfile::tempdir().unwrap();
        for (reader, path, mut contents) in fixtures(temp.path(), true) {
            let state = test_state();
            let stop = AtomicBool::new(false);
            let start = phase(&path, DAY);
            reader.drain(&state, start, &stop).unwrap();
            contents = contents.replace(":11", ":19");
            for tick in 1..=24 {
                contents.push_str(&reader.record(&format!("tick-{tick}"), 20 + tick));
                std::fs::write(&path, &contents).unwrap();
                let outcome = reader
                    .drain(&state, start + i64::try_from(tick).unwrap() * HOUR, &stop)
                    .unwrap();
                assert_eq!(outcome.recorded, 1, "{} tick {tick}", reader.key());
                assert_eq!(
                    outcome.quarantined,
                    usize::from(tick == 24),
                    "{} tick {tick}",
                    reader.key()
                );
            }
            assert_eq!(count(&state, "usage_event"), 25);
            assert_changed_payload(&state);
        }
    }

    #[test]
    fn clock_reversal_forces_content_verification_and_restart_replays_safely() {
        let temp = tempfile::tempdir().unwrap();
        for (reader, path, contents) in fixtures(temp.path(), true) {
            let state = test_state();
            let stop = AtomicBool::new(false);
            let start = phase(&path, DAY) + 2 * DAY;
            reader.drain(&state, start, &stop).unwrap();
            std::fs::write(&path, contents.replace(":11", ":19")).unwrap();
            let reversed = reader.drain(&state, start - 1, &stop).unwrap();
            assert_eq!(reversed.quarantined, 1, "{}", reader.key());
            assert_changed_payload(&state);
        }
        // Recreate each source with empty process-local caches over the same files.
        let original = fixtures(temp.path(), true);
        for (reader, _, _) in &original {
            let state = test_state();
            let stop = AtomicBool::new(false);
            reader.drain(&state, DAY, &stop).unwrap();
            let restarted = fixtures(temp.path(), true);
            let (new_reader, _, _) = restarted
                .iter()
                .find(|(new_reader, _, _)| new_reader.key() == reader.key())
                .unwrap();
            let replay = new_reader.drain(&state, DAY + 1, &stop).unwrap();
            assert!(replay.hashed > 131_072);
            assert_eq!(replay.recorded, 0);
            assert_eq!(replay.quarantined, 0);
            assert_eq!(count(&state, "usage_event"), 1);
        }
    }

    #[test]
    #[ignore = "synthetic 379-file logical-byte census; hashes about 115 GB, no live sessions"]
    fn synthetic_inventory_daily_read_bytes_before_and_after() {
        use astrocyte_readers::sessions::{
            jsonl_resume::PrefixCheck, omp::parse_omp_file_resumable,
        };
        use std::io::Write;

        fn distribute(files: &mut Vec<(u64, bool)>, count: u64, bytes: u64, active: u64) {
            for index in 0..count {
                files.push((
                    bytes / count + u64::from(index < bytes % count),
                    index < active,
                ));
            }
        }
        // Match the inventoried file-byte and capped 64 KiB-window totals,
        // file counts, six active windows, and Pi's two dominant files.
        // Sizes within the smaller groups are synthetic because only aggregate
        // metadata is available; no real session contents are needed.
        let mut files = vec![(589_407_728, true), (356_757_464, true)];
        distribute(&mut files, 33, 36_595_476, 0);
        distribute(&mut files, 194, 1_241_426, 0);
        distribute(&mut files, 101, 42_243_636, 2);
        distribute(&mut files, 35, 1_540_764, 0);
        distribute(&mut files, 6, 53_097_970, 2);
        distribute(&mut files, 1, 125_607, 0);
        distribute(&mut files, 4, 31_021, 0);
        distribute(&mut files, 1, 135_863, 0);
        distribute(&mut files, 2, 7_825, 0);
        assert_eq!(files.len(), 379);
        assert_eq!(files.iter().map(|(len, _)| len).sum::<u64>(), 1_081_184_780);
        assert_eq!(
            files.iter().map(|(len, _)| (*len).min(65_536)).sum::<u64>(),
            12_258_220
        );
        let temp = tempfile::tempdir().unwrap();
        let mut totals = [(0_u64, 0_u64); 2];
        for (index, (len, active)) in files.into_iter().enumerate() {
            // Scheduling is independent per file, so reuse one scratch file's
            // disk space instead of retaining a gigabyte of synthetic logs.
            let path = temp.path().join(format!("session-{index}.jsonl"));
            let mut file = std::fs::File::create(&path).unwrap();
            let mut remaining = len;
            while remaining > 0 {
                let mut chunk = remaining.min(32_768);
                if remaining > chunk && remaining - chunk < 64 {
                    chunk = remaining - 64;
                }
                let prefix = "{\"type\":\"custom\",\"text\":\"";
                let suffix = "\"}\n";
                let padding = usize::try_from(chunk).unwrap() - prefix.len() - suffix.len();
                write!(file, "{prefix}{}{suffix}", "x".repeat(padding)).unwrap();
                remaining -= chunk;
            }
            drop(file);
            for (slot, poll_ms) in [60_000_i64, 30_000].into_iter().enumerate() {
                let start = phase(&path, DAY) + 2 * DAY;
                let cold = parse_omp_file_resumable(&path, None, PrefixCheck::Full);
                let warm = parse_omp_file_resumable(&path, cold.resume, PrefixCheck::Full);
                let mut before = parse_omp_file_resumable(&path, None, PrefixCheck::Full).resume;
                let mut after = warm.resume;
                let stat = FileStat::read(&path).unwrap();
                let old =
                    WindowSchedule::after_read(None, stat.clone(), start - 2 * HOUR, false, true);
                let mut window = WindowSchedule::after_read(Some(&old), stat, start, true, true);
                let mut verified = start;
                for tick in 1..=DAY / poll_ms {
                    let now = start + tick * poll_ms;
                    let baseline = parse_omp_file_resumable(
                        &path,
                        before,
                        if tick * poll_ms % HOUR == 0 {
                            PrefixCheck::Full
                        } else {
                            PrefixCheck::Window
                        },
                    );
                    totals[slot].0 += baseline.hashed_bytes;
                    before = baseline.resume;
                    if active {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&path)
                            .unwrap()
                            .set_times(std::fs::FileTimes::new().set_modified(
                                SystemTime::UNIX_EPOCH
                                    + std::time::Duration::from_millis(u64::try_from(now).unwrap()),
                            ))
                            .unwrap();
                    }
                    let stat = FileStat::read(&path).unwrap();
                    let due = full_due(&path, Some(verified), now, DAY);
                    if !window.defer(&stat, now, due) {
                        let parsed = parse_omp_file_resumable(
                            &path,
                            after,
                            if due {
                                PrefixCheck::Full
                            } else {
                                PrefixCheck::Window
                            },
                        );
                        totals[slot].1 += parsed.hashed_bytes;
                        after = parsed.resume;
                        window = WindowSchedule::after_read(Some(&window), stat, now, true, true);
                        if due {
                            verified = now;
                        }
                    }
                }
            }
            std::fs::remove_file(path).unwrap();
        }
        for ((before, after), poll_ms) in totals.into_iter().zip([60_000_i64, 30_000]) {
            println!("synthetic files=379 F=1081184780 W=12258220 active_W=393216 poll_ms={poll_ms} before_bytes={before} after_bytes={after} saved_bytes={}", before - after);
            let ticks = u64::try_from(DAY / poll_ms).unwrap();
            assert_eq!(
                before,
                ticks * 12_258_220 + 24 * (1_081_184_780 - 12_258_220)
            );
            assert_eq!(
                after,
                1_081_184_780 + 287 * 11_865_004 + (ticks - 1) * 393_216
            );
        }
    }
}
