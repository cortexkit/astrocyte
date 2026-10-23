//! Oh My Pi's on-disk session producer and ledger ingest mapping.
//!
//! Reads every `.jsonl` below `~/.omp/agent/sessions/` at any depth: top-level
//! sessions and their subagent and advisor files, each a complete session
//! with its own header. Each assistant message with usage is one response;
//! the reader is `astrocyte_readers::sessions::omp`. The root is fixed and
//! not taken from `PI_CODING_AGENT_DIR`: Pi reads that same variable, so
//! honouring it here could point both sources at one tree.

#[cfg(test)]
use crate::refresher::harness_drain::files_below;
#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
#[cfg(any(test, feature = "test-support"))]
use std::path::Path;
#[cfg(any(test, feature = "test-support"))]
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use std::sync::Mutex;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_readers::sessions::omp::{
    parse_omp_file_resumable, OmpCall, OmpOutput, OmpParseCounts, OmpResumePoint,
};

use crate::refresher::harness_drain::{
    derived_measurement, drain_jsonl, health_reason, verbatim_measurement, HarnessSource,
    JsonlDrainOutcome, JsonlRead, ResumeCache, SourceRoot as OmpRoot,
};
use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const OMP_SOURCE_KEY: &str = "omp";

pub(crate) type OmpDrainOutcome = JsonlDrainOutcome<OmpParseCounts>;

/// A process-local incremental view over Oh My Pi's session files.
pub(crate) struct OmpSource {
    root: OmpRoot,
    cache: Mutex<ResumeCache<OmpResumePoint>>,
}

impl OmpSource {
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
        let root = match crate::harness_paths::omp(env) {
            Ok(path) => OmpRoot::Path(path),
            Err(error) => OmpRoot::Unresolved(error.to_string()),
        };
        Self {
            root,
            cache: Mutex::new(ResumeCache::default()),
        }
    }

    /// HOME-only fixture layout keeps existing drain/health tests independent of
    /// process variables. Production discovery uses `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>) -> Self {
        let root = match home {
            Some(home) if Path::new(&home).is_absolute() => OmpRoot::Path(
                PathBuf::from(home)
                    .join(".omp")
                    .join("agent")
                    .join("sessions"),
            ),
            Some(home) => OmpRoot::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the Oh My Pi sessions root"
            )),
            None => OmpRoot::Unresolved(
                "HOME is unset; cannot resolve the Oh My Pi sessions root".to_string(),
            ),
        };
        Self {
            root,
            cache: Mutex::new(ResumeCache::default()),
        }
    }

    /// A source over `sessions_root` (the directory holding session groups).
    #[cfg(test)]
    pub(crate) fn at(sessions_root: impl Into<PathBuf>) -> Self {
        Self {
            root: OmpRoot::Path(sessions_root.into()),
            cache: Mutex::new(ResumeCache::default()),
        }
    }

    /// Read and commit the session files; a stop rolls back the whole drain.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<OmpDrainOutcome, SourceDrainError> {
        drain_jsonl(
            state,
            OMP_SOURCE_KEY,
            "Oh My Pi",
            &self.root,
            &self.cache,
            recorded_at_ms,
            stop,
            &|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            },
            |path, previous, check| {
                let parse = parse_omp_file_resumable(path, previous, check);
                JsonlRead {
                    resumed: parse.resumed,
                    hashed_bytes: parse.hashed_bytes,
                    counts: parse.counts,
                    calls: parse.calls,
                    resume: parse.resume,
                }
            },
            map_call,
            |total, counts| {
                total.add(counts);
                counts.skipped_lines
            },
            astrocyte_readers::sessions::omp::OmpResumePoint::offset,
        )
    }
}

impl HarnessSource for OmpSource {
    fn source_key(&self) -> &'static str {
        OMP_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok(health_reason(
            "Oh My Pi",
            outcome.quarantined_redeliveries,
            outcome.counts.usage_not_ingested(),
        ))
    }
}

fn map_call(call: OmpCall) -> Result<EventRecordRequest, String> {
    let usage = call.usage;
    let mut measurements = Vec::with_capacity(5);
    measurements.extend(derived_measurement("input", "usage.input", usage.input)?);
    measurements.extend(verbatim_measurement(
        "cache_read",
        "usage.cacheRead",
        usage.cache_read,
    )?);
    measurements.extend(verbatim_measurement(
        "cache_write",
        "usage.cacheWrite",
        usage.cache_write,
    )?);
    measurements.extend(match usage.output {
        OmpOutput::Visible(visible) => {
            derived_measurement("output", "usage.output-reasoningTokens", visible)?
        }
        // No reasoning count: the gross output, under a source that says so,
        // and no reasoning role beside it.
        OmpOutput::Gross(output) => verbatim_measurement("output", "usage.output", output)?,
    });
    measurements.extend(verbatim_measurement(
        "reasoning",
        "usage.reasoningTokens",
        usage.reasoning,
    )?);

    Ok(EventRecordRequest {
        producer: OMP_SOURCE_KEY.to_string(),
        // The session header's id; the entry id is unique within it
        // (measured: 14 distinct entry ids over 14 usage records).
        producer_lineage: Some(call.session_id.clone()),
        producer_ref: call.entry_id,
        prior_event_ref: None,
        harness: None,
        project_root: call.project_root,
        session: Some(call.session_id),
        run_ref: None,
        flow_ref: None,
        work_class: None,
        occurred_at_ms: call.occurred_at_ms,
        emitted_at_ms: None,
        // Oh My Pi's `stopReason` verbatim (`stop`, `toolUse`, `aborted`).
        terminal_status: call.stop_reason,
        attempt_ordinal: None,
        vendor_request_id: call.response_id,
        provider: call.provider,
        model: call.model,
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        // No record states how Oh My Pi authenticated. The provider id
        // (`openai-codex`) names an API route, not who paid.
        charge_basis: ChargeBasis::Unknown,
        measurements,
        step_detail: Default::default(),
    })
}

// Test fixtures use fixed prefix windows, small line counts and nonnegative ledger totals.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::run_iteration_with_harness;
    use crate::state::test_state;
    use astrocyte_readers::sessions::jsonl_resume::{PrefixCheck, PREFIX_WINDOW_BYTES};
    use std::fs;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);
    const FILE: &str = "-work-p/2026-07-16T06-07-24-160Z_019f6989.jsonl";
    const SESSION: &str = "019f6989-7600-7000-a3a3-0194dcbc61cc";

    fn assistant(id: &str, usage: &str) -> String {
        format!(
            r#"{{"type":"message","id":"{id}","parentId":"p","timestamp":"2026-07-16T06:10:29.569Z","message":{{"role":"assistant","content":[],"provider":"openai-codex","model":"gpt-5.6-luna","usage":{usage},"stopReason":"toolUse","responseId":"resp_{id}"}}}}"#
        ) + "\n"
    }

    fn session() -> String {
        [
            format!(
                r#"{{"type":"session","version":3,"id":"{SESSION}","timestamp":"2026-07-16T06:07:24.160Z","cwd":"/work/p"}}"#
            ) + "\n",
            r#"{"type":"thinking_level_change","id":"t1","thinkingLevel":"high"}"#.to_string() + "\n",
            assistant(
                "a1",
                r#"{"input":858,"output":68,"cacheRead":28416,"cacheWrite":0,"totalTokens":29342,"reasoningTokens":30}"#,
            ),
            assistant(
                "a2",
                r#"{"input":1791,"output":26,"cacheRead":28416,"cacheWrite":0,"totalTokens":30233}"#,
            ),
        ]
        .concat()
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

    fn drain(source: &OmpSource, state: &SharedState, at: i64) -> OmpDrainOutcome {
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
                         WHERE usage_event.producer='omp' AND usage_event.producer_ref=?1",
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

    /// One event per assistant message: visible output and reasoning
    /// apart when reasoning is reported, gross output (named as such) when
    /// it is not, and the header's session and cwd on every event.
    #[test]
    fn each_assistant_message_is_one_event_in_disjoint_roles() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), FILE, &session());
        let state = test_state();
        let outcome = drain(&OmpSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.rows_recorded, 2);
        let quantity = |(quantity, _): &(Option<i64>, String)| *quantity;
        let first = measurements(&state, "a1");
        assert_eq!(
            first
                .iter()
                .map(|(role, m)| (role.as_str(), quantity(m)))
                .collect::<Vec<_>>(),
            [
                ("cache_read", Some(28416)),
                ("cache_write", Some(0)),
                ("input", Some(858)),
                ("output", Some(38)),
                ("reasoning", Some(30)),
            ]
        );
        let second = measurements(&state, "a2");
        assert_eq!(second["output"], (Some(26), "usage.output".to_string()));
        assert!(!second.contains_key("reasoning"));
        let (lineage, project, status, vendor): (String, String, String, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT producer_lineage,project_root,terminal_status,vendor_request_id
                     FROM usage_event WHERE producer='omp' AND producer_ref='a1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            (
                lineage.as_str(),
                project.as_str(),
                status.as_str(),
                vendor.as_str()
            ),
            (SESSION, "/work/p", "toolUse", "resp_a1")
        );
    }

    /// Re-reading from scratch writes no rows and no redelivery rows; an
    /// appended message is read from the stored offset; a corrupt line is
    /// counted per file.
    #[test]
    fn re_reads_write_no_rows_appends_resume_and_corrupt_lines_count() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), FILE, &session());
        let state = test_state();
        drain(&OmpSource::at(temp.path()), &state, 0);
        let again = drain(&OmpSource::at(temp.path()), &state, 1);
        assert_eq!((again.rows_recorded, again.exact_redeliveries), (0, 2));
        assert_eq!(count(&state, "SELECT COUNT(*) FROM redelivery_log"), 0);

        let source = OmpSource::at(temp.path());
        drain(&source, &state, 2);
        append(
            &path,
            &("not json\n".to_string() + &assistant("a3", r#"{"input":1,"output":1}"#)),
        );
        let grown = drain(&source, &state, 3);
        assert_eq!((grown.files_resumed, grown.counts.lines), (1, 2));
        assert_eq!(grown.rows_recorded, 1);
        assert_eq!(grown.skipped_lines_by_file, BTreeMap::from([(path, 1)]));
    }

    /// Per-tick cost scales with new bytes, not with the file.
    #[test]
    fn steady_state_tick_cost_is_bounded_by_the_window_and_the_appended_bytes() {
        let temp = TempDir::new().unwrap();
        let filler =
            r#"{"type":"custom","id":"f","data":{"k":"filler filler"}}"#.to_string() + "\n";
        let lines = (8 * PREFIX_WINDOW_BYTES as usize) / filler.len() + 1;
        let contents = session() + &filler.repeat(lines);
        let path = write_file(temp.path(), FILE, &contents);
        let state = test_state();
        let source = OmpSource::at(temp.path());
        drain(&source, &state, 0);
        let appended = assistant("a3", r#"{"input":1,"output":1}"#);
        append(&path, &appended);
        let grown = drain(&source, &state, 1);
        assert_eq!((grown.files_resumed, grown.rows_recorded), (1, 1));
        let bound = 2 * PREFIX_WINDOW_BYTES + appended.len() as u64;
        assert!(
            grown.hashed_bytes <= bound,
            "{} > {bound}",
            grown.hashed_bytes
        );
        let quiet = drain(&source, &state, 2);
        assert!(quiet.hashed_bytes <= 2 * PREFIX_WINDOW_BYTES);
    }

    #[test]
    fn absent_root_is_no_lane_but_an_unreadable_root_is_cold() {
        let temp = TempDir::new().unwrap();
        let state = test_state();
        let absent = OmpSource::from_home(Some(temp.path().as_os_str().to_owned()));
        assert!(drain(&absent, &state, 0).root_absent);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='omp'"
            ),
            0
        );
        write_file(
            temp.path(),
            ".omp/agent/sessions",
            "a file, not a directory",
        );
        let error = absent
            .drain(&state, 1, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("read Oh My Pi sessions root"), "{error}");
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime
                 WHERE source_key='omp' AND last_success_ms IS NULL AND pending_unknown=1"
            ),
            1
        );
        let relative = OmpSource::from_home(Some(OsString::from("relative")));
        assert!(relative.drain(&state, 2, &NEVER_STOP).is_err());
    }

    #[test]
    fn refresher_iteration_drains_omp_and_clears_its_health() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), FILE, &session());
        let state = test_state();
        run_iteration_with_harness(&state, &[], &OmpSource::at(temp.path()), &NEVER_STOP);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='omp'"
            ),
            2
        );
        assert!(state.producer_degraded_reason("omp").is_none());
    }

    /// Per-class totals of a raw `serde_json` scan sharing no code with the
    /// reader, plus the convention checks the reader's roles rely on.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct RawTotals {
        calls: u64,
        input: u64,
        cache_read: u64,
        cache_write: u64,
        /// Gross `output`, reasoning included.
        output: u64,
        reasoning: u64,
        unparseable_lines: u64,
    }

    #[derive(Debug, Default)]
    struct Convention {
        total_identity_holds: u64,
        non_zero_cache_read: u64,
        non_zero_cache_write: u64,
        with_reasoning: u64,
        reasoning_exceeds_output: u64,
    }

    fn raw_totals(files: &[PathBuf]) -> (RawTotals, Convention) {
        let mut totals = RawTotals::default();
        let mut convention = Convention::default();
        for path in files {
            let text = fs::read_to_string(path).unwrap();
            let Some(end) = text.rfind('\n') else {
                continue;
            };
            for line in text[..end].lines().filter(|line| !line.trim().is_empty()) {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                    totals.unparseable_lines += 1;
                    continue;
                };
                let usage = &value["message"]["usage"];
                if value["type"] != "message"
                    || value["message"]["role"] != "assistant"
                    || !usage.is_object()
                {
                    continue;
                }
                let field = |name: &str| usage[name].as_u64();
                let [input, output, read, write] =
                    ["input", "output", "cacheRead", "cacheWrite"].map(|name| field(name).unwrap());
                totals.calls += 1;
                totals.input += input;
                totals.output += output;
                totals.cache_read += read;
                totals.cache_write += write;
                if field("totalTokens") == Some(input + output + read + write) {
                    convention.total_identity_holds += 1;
                }
                convention.non_zero_cache_read += u64::from(read > 0);
                convention.non_zero_cache_write += u64::from(write > 0);
                if let Some(reasoning) = field("reasoningTokens") {
                    convention.with_reasoning += 1;
                    convention.reasoning_exceeds_output += u64::from(reasoning > output);
                    totals.reasoning += reasoning;
                }
            }
        }
        (totals, convention)
    }

    /// Run with `--ignored`. Reads the live corpus (never writes it) into a
    /// scratch store and reconciles the raw scan, the reader's calls and the
    /// ledger's rows.
    #[test]
    #[ignore = "reads the mutable ~/.omp/agent/sessions corpus"]
    fn live_omp_corpus_reconciles_raw_reader_and_ledger() {
        let root = PathBuf::from(std::env::var_os("HOME").unwrap()).join(".omp/agent/sessions");
        let files = files_below(&root, &|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .unwrap();
        let (raw, convention) = raw_totals(&files);

        let mut reader = RawTotals::default();
        let mut counts = OmpParseCounts::default();
        for path in &files {
            let parse = parse_omp_file_resumable(path, None, PrefixCheck::Full);
            counts.add(&parse.counts);
            for call in parse.calls {
                let usage = call.usage;
                reader.calls += 1;
                reader.input += usage.input.quantity().unwrap();
                reader.cache_read += usage.cache_read.unwrap();
                reader.cache_write += usage.cache_write.unwrap();
                reader.reasoning += usage.reasoning.unwrap_or(0);
                reader.output += match usage.output {
                    OmpOutput::Visible(visible) => {
                        visible.quantity().unwrap() + usage.reasoning.unwrap()
                    }
                    OmpOutput::Gross(output) => output.unwrap(),
                };
            }
        }
        reader.unparseable_lines = counts.skipped_lines;

        let state = test_state();
        let outcome = drain(&OmpSource::at(&root), &state, 4_000_000_000_000);
        let ledger_role = |role: &str| -> u64 {
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COALESCE(SUM(measurement.quantity_units),0)
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='omp' AND measurement.role=?1",
                        [role],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap() as u64
        };
        let ledger = RawTotals {
            calls: count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='omp'",
            ) as u64,
            input: ledger_role("input"),
            cache_read: ledger_role("cache_read"),
            cache_write: ledger_role("cache_write"),
            // Stored output is visible output where reasoning was reported;
            // adding the stored reasoning back gives the gross counter.
            output: ledger_role("output") + ledger_role("reasoning"),
            reasoning: ledger_role("reasoning"),
            unparseable_lines: outcome.counts.skipped_lines,
        };
        eprintln!("files: {}", files.len());
        eprintln!("raw: {raw:?}");
        eprintln!("convention: {convention:?}");
        eprintln!("reader: {reader:?} counts {counts:?}");
        eprintln!("ledger: {ledger:?}");

        // Never a vacuous pass.
        assert!(raw.calls > 0 && raw.input > 0 && raw.cache_read > 0 && raw.output > 0);
        assert!(raw.reasoning > 0);
        // The convention the reader relies on; if any fails, re-establish it.
        assert_eq!(convention.total_identity_holds, raw.calls);
        assert!(convention.non_zero_cache_read > 0);
        assert_eq!(convention.reasoning_exceeds_output, 0);
        assert_eq!(
            convention.non_zero_cache_write, 0,
            "cache-write convention unmeasured"
        );

        assert_eq!(counts.usage_not_ingested(), 0);
        assert_eq!(counts.gross_outputs, raw.calls - convention.with_reasoning);
        assert_eq!(reader, raw);
        assert_eq!(ledger, raw);
        assert_eq!(outcome.quarantined_redeliveries, 0);
    }
}
