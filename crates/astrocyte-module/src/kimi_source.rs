//! Kimi CLI's on-disk session producer and ledger ingest mapping.
//!
//! Reads every `wire.jsonl` below `~/.kimi/sessions/` (one per session, at
//! `<GROUP>/<SESSION_UUID>/wire.jsonl`). Each `StatusUpdate` is one
//! response; the reader is `astrocyte_readers::sessions::kimi`.

use super::harness_drain::DrainMode;
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
use astrocyte_readers::sessions::kimi::{
    parse_kimi_wire_resumable, KimiCall, KimiParseCounts, KimiResumePoint,
};

use crate::refresher::harness_drain::{
    derived_measurement, drain_jsonl, health_reason, verbatim_measurement, HarnessSource,
    JsonlDrainOutcome, JsonlRead, ResumeCache, SourceRoot as KimiRoot,
};
use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const KIMI_SOURCE_KEY: &str = "kimi";

pub(crate) type KimiDrainOutcome = JsonlDrainOutcome<KimiParseCounts>;

/// A process-local incremental view over Kimi CLI's `wire.jsonl` files.
pub(crate) struct KimiSource {
    root: KimiRoot,
    cache: Mutex<ResumeCache<KimiResumePoint>>,
}

impl KimiSource {
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
        let root = match crate::harness_paths::kimi(env) {
            Ok(path) => KimiRoot::Path(path),
            Err(error) => KimiRoot::Unresolved(error.to_string()),
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
            Some(home) if Path::new(&home).is_absolute() => {
                KimiRoot::Path(PathBuf::from(home).join(".kimi").join("sessions"))
            }
            Some(home) => KimiRoot::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the Kimi CLI sessions root"
            )),
            None => KimiRoot::Unresolved(
                "HOME is unset; cannot resolve the Kimi CLI sessions root".to_string(),
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
            root: KimiRoot::Path(sessions_root.into()),
            cache: Mutex::new(ResumeCache::default()),
        }
    }

    /// Read and commit the wire logs; a stop rolls back the whole drain.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<KimiDrainOutcome, SourceDrainError> {
        self.drain_in_lane(state, recorded_at_ms, stop, DrainMode::Full)
    }

    fn drain_in_lane(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
        mode: DrainMode,
    ) -> Result<KimiDrainOutcome, SourceDrainError> {
        drain_jsonl(
            state,
            KIMI_SOURCE_KEY,
            "Kimi CLI",
            &self.root,
            &self.cache,
            recorded_at_ms,
            stop,
            &|path| path.file_name().is_some_and(|name| name == "wire.jsonl"),
            |path, previous, check| {
                let parse = parse_kimi_wire_resumable(path, previous, check);
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
            astrocyte_readers::sessions::kimi::KimiResumePoint::offset,
            mode,
        )
    }
}

impl HarnessSource for KimiSource {
    fn drain_fast(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        self.drain_in_lane(state, at, stop, DrainMode::Fast)
            .map(|_| ())
    }

    fn source_key(&self) -> &'static str {
        KIMI_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok(health_reason(
            "Kimi CLI",
            outcome.quarantined_redeliveries,
            outcome.counts.usage_not_ingested(),
        ))
    }
}

fn map_call(call: KimiCall) -> Result<EventRecordRequest, String> {
    let usage = call.usage;
    let mut measurements = Vec::with_capacity(4);
    measurements.extend(derived_measurement(
        "input",
        "token_usage.input_other",
        usage.input,
    )?);
    measurements.extend(verbatim_measurement(
        "cache_read",
        "token_usage.input_cache_read",
        usage.cache_read,
    )?);
    measurements.extend(verbatim_measurement(
        "cache_write",
        "token_usage.input_cache_creation",
        usage.cache_write,
    )?);
    // Output as reported: Kimi reports no reasoning count, so any thinking
    // is inside it and no reasoning role is stored.
    measurements.extend(verbatim_measurement(
        "output",
        "token_usage.output",
        usage.output,
    )?);

    Ok(EventRecordRequest {
        producer: KIMI_SOURCE_KEY.to_string(),
        // The session directory's id. `message_id` is an OpenAI-style
        // completion id, unique per response on its own; the lineage keeps
        // a copy of a session under another group directory on the same
        // identity only if it is the same session.
        producer_lineage: Some(call.session_id.clone()),
        producer_ref: call.message_id,
        prior_event_ref: None,
        harness: None,
        // A wire record carries no working directory.
        project_root: None,
        session: Some(call.session_id),
        run_ref: None,
        flow_ref: None,
        work_class: None,
        occurred_at_ms: call.occurred_at_ms,
        emitted_at_ms: None,
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        // A wire record names neither. The configured default model in
        // `~/.kimi/config.toml` is a current setting, not a statement about
        // a past response, so it is not read.
        provider: None,
        model: None,
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        // No wire record says how Kimi CLI authenticated.
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
    use std::collections::BTreeSet;
    use std::fs;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);
    const WIRE: &str = "c63e/5777b661-8d37-4b53-a3f5-8635024bb016/wire.jsonl";
    const SESSION: &str = "5777b661-8d37-4b53-a3f5-8635024bb016";

    fn status(id: &str, counters: [u64; 4]) -> String {
        let [other, output, read, creation] = counters;
        format!(
            r#"{{"timestamp": 1771258960.612596, "message": {{"type": "StatusUpdate", "payload": {{"context_usage": 0.02, "token_usage": {{"input_other": {other}, "output": {output}, "input_cache_read": {read}, "input_cache_creation": {creation}}}, "message_id": "{id}"}}}}}}"#
        ) + "\n"
    }

    fn wire() -> String {
        [
            r#"{"type": "metadata", "protocol_version": "1.3"}"#.to_string() + "\n",
            status("chatcmpl-a", [1862, 12591, 4864, 0]),
            status("chatcmpl-b", [2455, 408, 16896, 0]),
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

    fn drain(source: &KimiSource, state: &SharedState, at: i64) -> KimiDrainOutcome {
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

    /// `role -> quantity` of one event.
    fn quantities(state: &SharedState, producer_ref: &str) -> BTreeMap<String, Option<i64>> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT measurement.role,measurement.quantity_units
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='kimi' AND usage_event.producer_ref=?1",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([producer_ref], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(|error| error.to_string())?
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map_err(|error| error.to_string());
                rows
            })
            .unwrap()
    }

    /// One event per `StatusUpdate`, in the disjoint roles Kimi reports,
    /// with the session directory as lineage and no invented model.
    #[test]
    fn each_status_update_is_one_event_in_disjoint_roles() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), WIRE, &wire());
        let state = test_state();
        let outcome = drain(&KimiSource::at(temp.path()), &state, 0);
        assert_eq!((outcome.files_read, outcome.rows_recorded), (1, 2));
        assert_eq!(
            quantities(&state, "chatcmpl-a"),
            BTreeMap::from([
                ("cache_read".to_string(), Some(4864)),
                ("cache_write".to_string(), Some(0)),
                ("input".to_string(), Some(1862)),
                ("output".to_string(), Some(12591)),
            ])
        );
        let (lineage, model, charge_basis): (String, Option<String>, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT producer_lineage,model,charge_basis FROM usage_event
                     WHERE producer='kimi' AND producer_ref='chatcmpl-b'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            (lineage.as_str(), model, charge_basis.as_str()),
            (SESSION, None, "unknown")
        );
    }

    /// Re-reading from scratch writes no new rows and no redelivery rows; an
    /// appended response is read from the stored offset.
    #[test]
    fn re_reads_write_no_rows_and_appends_resume() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), WIRE, &wire());
        let state = test_state();
        drain(&KimiSource::at(temp.path()), &state, 0);
        let again = drain(&KimiSource::at(temp.path()), &state, 1);
        assert_eq!((again.rows_recorded, again.exact_redeliveries), (0, 2));
        assert_eq!(count(&state, "SELECT COUNT(*) FROM redelivery_log"), 0);

        let source = KimiSource::at(temp.path());
        drain(&source, &state, 2);
        append(&path, &status("chatcmpl-c", [1, 2, 3, 0]));
        let grown = drain(&source, &state, 3);
        assert_eq!((grown.files_resumed, grown.counts.lines), (1, 1));
        assert_eq!(grown.rows_recorded, 1);
        assert_eq!(count(&state, "SELECT COUNT(*) FROM usage_event"), 3);
    }

    /// A corrupt complete line is counted per file and reading continues.
    #[test]
    fn a_corrupt_line_is_counted_per_file() {
        let temp = TempDir::new().unwrap();
        let path = write_file(
            temp.path(),
            WIRE,
            &(wire() + "not json\n" + &status("chatcmpl-c", [1, 2, 3, 0])),
        );
        let state = test_state();
        let outcome = drain(&KimiSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.skipped_lines_by_file, BTreeMap::from([(path, 1)]));
        assert_eq!(outcome.rows_recorded, 3);
    }

    /// Per-tick cost scales with new bytes, not with the file.
    #[test]
    fn steady_state_tick_cost_is_bounded_by_the_window_and_the_appended_bytes() {
        let temp = TempDir::new().unwrap();
        let filler = r#"{"timestamp": 1771258836.1, "message": {"type": "ContentPart", "payload": {"type": "text", "text": "x"}}}"#.to_string() + "\n";
        let lines = (8 * PREFIX_WINDOW_BYTES as usize) / filler.len() + 1;
        let contents = wire() + &filler.repeat(lines);
        let path = write_file(temp.path(), WIRE, &contents);
        let state = test_state();
        let source = KimiSource::at(temp.path());
        drain(&source, &state, 0);

        let appended = status("chatcmpl-c", [1, 2, 3, 0]);
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
        let absent = KimiSource::from_home(Some(temp.path().as_os_str().to_owned()));
        assert!(drain(&absent, &state, 0).root_absent);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='kimi'"
            ),
            0
        );

        write_file(temp.path(), ".kimi/sessions", "a file, not a directory");
        let error = absent
            .drain(&state, 1, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("read Kimi CLI sessions root"), "{error}");
        let cold: (i64, Option<i64>, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MAX(last_success_ms),MAX(pending_unknown)
                     FROM source_runtime WHERE source_key='kimi'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(cold, (1, None, 1));

        let relative = KimiSource::from_home(Some(OsString::from("relative")));
        assert!(relative.drain(&state, 2, &NEVER_STOP).is_err());
    }

    #[test]
    fn refresher_iteration_drains_kimi_and_clears_its_health() {
        let temp = TempDir::new().unwrap();
        write_file(temp.path(), WIRE, &wire());
        let state = test_state();
        run_iteration_with_harness(&state, &[], &KimiSource::at(temp.path()), &NEVER_STOP);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='kimi'"
            ),
            2
        );
        assert!(state.producer_degraded_reason("kimi").is_none());
    }

    /// Per-class totals of a raw `serde_json` scan sharing no code with the
    /// reader: one response per distinct `message_id`.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct RawTotals {
        calls: u64,
        input_other: u64,
        cache_read: u64,
        cache_creation: u64,
        output: u64,
        unparseable_lines: u64,
    }

    fn raw_totals(files: &[PathBuf]) -> RawTotals {
        let mut totals = RawTotals::default();
        let mut seen = BTreeSet::new();
        for path in files {
            let text = fs::read_to_string(path).unwrap();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                    totals.unparseable_lines += 1;
                    continue;
                };
                let payload = &value["message"]["payload"];
                if value["message"]["type"] != "StatusUpdate"
                    || !payload["token_usage"].is_object()
                    || !seen.insert(payload["message_id"].to_string())
                {
                    continue;
                }
                let field = |name: &str| payload["token_usage"][name].as_u64().unwrap();
                totals.calls += 1;
                totals.input_other += field("input_other");
                totals.cache_read += field("input_cache_read");
                totals.cache_creation += field("input_cache_creation");
                totals.output += field("output");
            }
        }
        totals
    }

    /// Run with `--ignored`. Reads the live corpus (never writes it) into a
    /// scratch store and reconciles the raw scan, the reader's calls and the
    /// ledger's rows.
    #[test]
    #[ignore = "reads the mutable ~/.kimi/sessions corpus"]
    fn live_kimi_corpus_reconciles_raw_reader_and_ledger() {
        let root = PathBuf::from(std::env::var_os("HOME").unwrap()).join(".kimi/sessions");
        let files = files_below(&root, &|path| {
            path.file_name().is_some_and(|name| name == "wire.jsonl")
        })
        .unwrap();
        let raw = raw_totals(&files);

        let mut reader = RawTotals::default();
        let mut counts = KimiParseCounts::default();
        for path in &files {
            let parse = parse_kimi_wire_resumable(path, None, PrefixCheck::Full);
            counts.add(&parse.counts);
            for call in parse.calls {
                reader.calls += 1;
                reader.input_other += call.usage.input.quantity().unwrap();
                reader.cache_read += call.usage.cache_read.unwrap();
                reader.cache_creation += call.usage.cache_write.unwrap();
                reader.output += call.usage.output.unwrap();
            }
        }
        reader.unparseable_lines = counts.skipped_lines;

        let state = test_state();
        let outcome = drain(&KimiSource::at(&root), &state, 4_000_000_000_000);
        let ledger_role = |role: &str| -> u64 {
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COALESCE(SUM(measurement.quantity_units),0)
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='kimi' AND measurement.role=?1",
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
                "SELECT COUNT(*) FROM usage_event WHERE producer='kimi'",
            ) as u64,
            input_other: ledger_role("input"),
            cache_read: ledger_role("cache_read"),
            cache_creation: ledger_role("cache_write"),
            output: ledger_role("output"),
            unparseable_lines: outcome.counts.skipped_lines,
        };
        eprintln!("files: {}", files.len());
        eprintln!("raw: {raw:?}");
        eprintln!("reader: {reader:?} counts {counts:?}");
        eprintln!("ledger: {ledger:?}");

        // Never a vacuous pass.
        assert!(raw.calls > 0 && raw.input_other > 0 && raw.cache_read > 0 && raw.output > 0);
        assert_eq!(counts.usage_not_ingested(), 0);
        assert_eq!(reader, raw);
        assert_eq!(ledger, raw);
        assert_eq!(outcome.quarantined_redeliveries, 0);
    }
}
