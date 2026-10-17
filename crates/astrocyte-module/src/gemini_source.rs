//! Gemini CLI's on-disk session producer and ledger ingest mapping.
//!
//! Reads chat recordings below `$GEMINI_CLI_HOME/.gemini/tmp/` (else
//! `~/.gemini/tmp/`) that the reader's layout filter accepts. A JSONL
//! recording is appended to and resumes from its byte offset. A JSON
//! recording is rewritten in full by Gemini CLI, so it has no append-only
//! prefix: it is re-read whole when its length, mtime or inode changed, and
//! on the hourly full-verification cadence its bytes are re-hashed to catch
//! a rewrite those three missed. An unchanged document needs only an opened-file
//! identity and metadata check, without reading its contents.

use std::collections::{BTreeMap, HashMap};
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_readers::file_identity::{inode_of, same_identity};
use astrocyte_readers::provider_identity::inferred_provider_from_model;
#[cfg(test)]
use astrocyte_readers::sessions::gemini::parse_gemini_json_document;
use astrocyte_readers::sessions::gemini::{
    is_gemini_chat_recording, parse_gemini_jsonl_resumable, read_gemini_json_document, GeminiCall,
    GeminiParseCounts, GeminiResumePoint,
};

use crate::refresher::harness_drain::{
    derived_measurement, files_below, health_reason, prefix_check, present_root, record_requests,
    verbatim_measurement, verified_after, HarnessSource, ResumeCache, SourceRoot as GeminiRoot,
    FULL_PREFIX_VERIFY_INTERVAL_MS,
};
use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const GEMINI_SOURCE_KEY: &str = "gemini";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GeminiDrainOutcome {
    /// The `tmp` directory does not exist: no Gemini CLI lane.
    pub root_absent: bool,
    /// Chat recordings found.
    pub files_found: usize,
    /// JSON documents parsed this drain (new or changed).
    pub documents_parsed: usize,
    /// JSON documents left alone because nothing about them changed.
    pub documents_unchanged: usize,
    /// JSONL recordings read, and how many of those resumed.
    pub jsonl_read: usize,
    pub jsonl_resumed: usize,
    /// Reader counts summed over every file this drain parsed.
    pub counts: GeminiParseCounts,
    /// Skipped lines or messages per file, for every file that had any.
    pub skipped_lines_by_file: BTreeMap<PathBuf, u64>,
    pub calls_emitted: usize,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
    /// Bytes fed to SHA-256: a JSONL resume costs its window plus what was
    /// appended; a changed JSON document costs the document.
    pub hashed_bytes: u64,
}

/// What a JSON document looked like when it was last parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentMark {
    len: u64,
    modified: Option<SystemTime>,
    inode: Option<(u64, u64)>,
    sha256: [u8; 32],
    verified_ms: i64,
}

#[derive(Debug, Default)]
struct GeminiCache {
    jsonl: ResumeCache<GeminiResumePoint>,
    documents: HashMap<PathBuf, DocumentMark>,
}

/// A process-local incremental view over Gemini CLI's chat recordings.
pub(crate) struct GeminiSource {
    root: GeminiRoot,
    cache: Mutex<GeminiCache>,
}

impl GeminiSource {
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
        let root = match crate::harness_paths::gemini(env) {
            Ok(path) => GeminiRoot::Path(path),
            Err(error) => GeminiRoot::Unresolved(error.to_string()),
        };
        Self {
            root,
            cache: Mutex::new(GeminiCache::default()),
        }
    }

    /// Legacy fixture layout for drain/health tests; the supplied path is already
    /// the .gemini directory. Production discovery uses the home-root override
    /// through `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>, gemini_cli_home: Option<OsString>) -> Self {
        let configured = gemini_cli_home.filter(|value| !value.to_string_lossy().trim().is_empty());
        let root = match (configured, home) {
            (Some(dir), _) if Path::new(&dir).is_absolute() => {
                GeminiRoot::Path(PathBuf::from(dir).join("tmp"))
            }
            (Some(dir), _) => GeminiRoot::Unresolved(format!(
                "GEMINI_CLI_HOME is relative ({dir:?}); refusing to resolve the Gemini CLI home"
            )),
            (None, Some(home)) if Path::new(&home).is_absolute() => {
                GeminiRoot::Path(PathBuf::from(home).join(".gemini").join("tmp"))
            }
            (None, Some(home)) => GeminiRoot::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the Gemini CLI home"
            )),
            (None, None) => GeminiRoot::Unresolved(
                "HOME is unset; cannot resolve the Gemini CLI home".to_string(),
            ),
        };
        Self {
            root,
            cache: Mutex::new(GeminiCache::default()),
        }
    }

    /// A source over `tmp_root` (the directory holding project directories).
    #[cfg(test)]
    pub(crate) fn at(tmp_root: impl Into<PathBuf>) -> Self {
        Self {
            root: GeminiRoot::Path(tmp_root.into()),
            cache: Mutex::new(GeminiCache::default()),
        }
    }

    /// Read every chat recording that is new or changed and commit the calls
    /// read. Setting `stop` abandons the drain and returns
    /// `SourceDrainError::Stopped` having committed nothing.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<GeminiDrainOutcome, SourceDrainError> {
        let Some(root) = present_root(state, GEMINI_SOURCE_KEY, &self.root, "Gemini CLI")? else {
            return Ok(GeminiDrainOutcome {
                root_absent: true,
                ..GeminiDrainOutcome::default()
            });
        };
        let files = files_below(root, &is_gemini_chat_recording)
            .map_err(|error| format!("read Gemini CLI root {}: {error}", root.display()))?;
        let previous = std::mem::take(
            &mut *self
                .cache
                .lock()
                .map_err(|_| "Gemini CLI cache lock poisoned".to_string())?,
        );
        let (mut previous_points, previous_verified) =
            (previous.jsonl.resume, previous.jsonl.prefix_verified_ms);
        let previous_documents = previous.documents;

        let mut outcome = GeminiDrainOutcome {
            files_found: files.len(),
            ..GeminiDrainOutcome::default()
        };
        let mut next = GeminiCache::default();
        let mut calls = Vec::new();
        for path in files {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            let counts = if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                let previous = previous_points.remove(&path);
                let verified = previous_verified.get(&path).copied();
                let check = prefix_check(previous.is_some(), verified, recorded_at_ms);
                let parse = parse_gemini_jsonl_resumable(&path, previous, check);
                if let Some(verified) =
                    verified_after(check, parse.resumed, verified, recorded_at_ms)
                {
                    next.jsonl.prefix_verified_ms.insert(path.clone(), verified);
                }
                outcome.jsonl_read += 1;
                outcome.jsonl_resumed += usize::from(parse.resumed);
                outcome.hashed_bytes += parse.hashed_bytes;
                if let Some(point) = parse.resume {
                    next.jsonl.resume.insert(path.clone(), point);
                }
                calls.extend(parse.calls);
                parse.counts
            } else {
                match read_document(
                    &path,
                    previous_documents.get(&path),
                    recorded_at_ms,
                    &mut outcome,
                ) {
                    Some((mark, parsed)) => {
                        next.documents.insert(path.clone(), mark);
                        match parsed {
                            Some((document_calls, counts)) => {
                                calls.extend(document_calls);
                                counts
                            }
                            None => GeminiParseCounts::default(),
                        }
                    }
                    None => GeminiParseCounts::default(),
                }
            };
            outcome.counts.add(&counts);
            if counts.skipped_lines > 0 {
                outcome
                    .skipped_lines_by_file
                    .insert(path.clone(), counts.skipped_lines);
            }
        }
        outcome.calls_emitted = calls.len();
        let requests = calls
            .into_iter()
            .map(map_call)
            .collect::<Result<Vec<_>, _>>()?;

        let tally = record_requests(state, GEMINI_SOURCE_KEY, &requests, recorded_at_ms, &|| {
            stop.load(Ordering::Relaxed)
        })?;
        outcome.rows_recorded = tally.recorded;
        outcome.exact_redeliveries = tally.exact_redeliveries;
        outcome.quarantined_redeliveries = tally.quarantined_redeliveries;

        *self
            .cache
            .lock()
            .map_err(|_| "Gemini CLI cache lock poisoned".to_string())? = next;
        Ok(outcome)
    }
}

type DocumentCalls = (Vec<GeminiCall>, GeminiParseCounts);

/// Read one JSON document if it may have changed. Returns the mark to keep
/// and, when the document was parsed, its calls and counts; `None` when the
/// file could not be read at all (it is read again next drain).
fn read_document(
    path: &Path,
    previous: Option<&DocumentMark>,
    recorded_at_ms: i64,
    outcome: &mut GeminiDrainOutcome,
) -> Option<(DocumentMark, Option<DocumentCalls>)> {
    let file = std::fs::File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    let (len, modified, inode) = (
        metadata.len(),
        metadata.modified().ok(),
        inode_of(&file, &metadata),
    );
    if let Some(previous) = previous {
        let same_stat = previous.len == len
            && previous.modified == modified
            && same_identity(inode, previous.inode);
        let verify_due =
            recorded_at_ms.saturating_sub(previous.verified_ms) >= FULL_PREFIX_VERIFY_INTERVAL_MS;
        if same_stat && !verify_due {
            outcome.documents_unchanged += 1;
            return Some((previous.clone(), None));
        }
    }
    let parse = read_gemini_json_document(path)?;
    outcome.hashed_bytes += parse.hashed_bytes;
    if let Some(previous) = previous {
        let same_stat = previous.len == len
            && previous.modified == modified
            && same_identity(inode, previous.inode);
        if same_stat && parse.sha256 == previous.sha256 {
            outcome.documents_unchanged += 1;
            return Some((
                DocumentMark {
                    verified_ms: recorded_at_ms,
                    ..previous.clone()
                },
                None,
            ));
        }
    }
    outcome.documents_parsed += 1;
    let mark = DocumentMark {
        len,
        modified,
        inode,
        sha256: parse.sha256,
        verified_ms: recorded_at_ms,
    };
    Some((mark, Some((parse.calls, parse.counts))))
}

impl HarnessSource for GeminiSource {
    fn source_key(&self) -> &'static str {
        GEMINI_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok(health_reason(
            "Gemini CLI",
            outcome.quarantined_redeliveries,
            outcome.counts.usage_not_ingested(),
        ))
    }
}

fn map_call(call: GeminiCall) -> Result<EventRecordRequest, String> {
    let usage = call.usage;
    let mut measurements = Vec::with_capacity(4);
    measurements.extend(derived_measurement(
        "input",
        "tokens.input-cached",
        usage.input,
    )?);
    measurements.extend(verbatim_measurement(
        "cache_read",
        "tokens.cached",
        usage.cache_read,
    )?);
    measurements.extend(derived_measurement(
        "output",
        "tokens.output",
        usage.output,
    )?);
    measurements.extend(verbatim_measurement(
        "reasoning",
        "tokens.thoughts",
        usage.reasoning,
    )?);

    // Gemini CLI records no provider; the Claude Code source infers one from
    // the model name the same way.
    let provider = call
        .model
        .as_deref()
        .and_then(inferred_provider_from_model)
        .map(str::to_string);
    Ok(EventRecordRequest {
        producer: GEMINI_SOURCE_KEY.to_string(),
        // The recording's `sessionId`, shared by every file that saves the
        // same session, so a copy is an exact redelivery, not a new event.
        producer_lineage: Some(call.session_id.clone()),
        producer_ref: call.message_id,
        prior_event_ref: None,
        harness: None,
        // A chat recording names no working directory.
        project_root: None,
        session: Some(call.session_id),
        run_ref: None,
        work_class: None,
        occurred_at_ms: call.occurred_at_ms,
        emitted_at_ms: None,
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider,
        model: call.model,
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        // No recording says how Gemini CLI authenticated.
        charge_basis: ChargeBasis::Unknown,
        measurements,
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
    const SESSION: &str = "2fd93fbe-0000-4000-8000-000000000001";
    const DOC: &str = "proj/chats/session-2026-02-19T22-18-2fd93fbe.json";
    const LINES: &str = "proj/chats/session-2026-06-16T21-45-57e27a2a.jsonl";

    fn message(id: &str, counters: [u64; 5]) -> String {
        let [input, output, cached, thoughts, tool] = counters;
        format!(
            r#"{{"id":"{id}","timestamp":"2026-02-19T22:20:48.450Z","type":"gemini","content":"ok","tokens":{{"input":{input},"output":{output},"cached":{cached},"thoughts":{thoughts},"tool":{tool},"total":{}}},"model":"gemini-3-flash-preview"}}"#,
            input + output + thoughts + tool
        )
    }

    fn document(messages: &[String]) -> String {
        format!(
            r#"{{"sessionId":"{SESSION}","projectHash":"665a","startTime":"s","lastUpdated":"l","messages":[{}]}}"#,
            messages.join(",")
        )
    }

    fn recording(messages: &[String]) -> String {
        let header = r#"{"sessionId":"57e27a2a-1","projectHash":"0f39","startTime":"s","lastUpdated":"l","kind":"main"}"#;
        [header.to_string()]
            .into_iter()
            .chain(messages.iter().cloned())
            .map(|line| line + "\n")
            .collect()
    }

    fn write_file(root: &Path, relative: &str, contents: &str) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();
        path
    }

    fn drain(source: &GeminiSource, state: &SharedState, at: i64) -> GeminiDrainOutcome {
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

    fn quantities(state: &SharedState, producer_ref: &str) -> BTreeMap<String, Option<i64>> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT measurement.role,measurement.quantity_units
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='gemini' AND usage_event.producer_ref=?1",
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

    /// One event per token message in either format, in disjoint roles; a
    /// provider inferred from the model; logs and other files ignored.
    #[test]
    fn each_token_message_is_one_event_in_disjoint_roles() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            DOC,
            &document(&[message("fc", [8075, 66, 6031, 23, 0])]),
        );
        write_file(
            temp.path(),
            LINES,
            &recording(&[message("g1", [100, 5, 60, 2, 0])]),
        );
        write_file(temp.path(), "proj/logs.json", "[]");
        let state = test_state();
        let outcome = drain(&GeminiSource::at(temp.path()), &state, 0);
        assert_eq!((outcome.files_found, outcome.rows_recorded), (2, 2));
        assert_eq!(
            quantities(&state, "fc"),
            BTreeMap::from([
                ("cache_read".to_string(), Some(6031)),
                ("input".to_string(), Some(2044)),
                ("output".to_string(), Some(66)),
                ("reasoning".to_string(), Some(23)),
            ])
        );
        let (lineage, provider): (String, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT producer_lineage,provider FROM usage_event
                     WHERE producer='gemini' AND producer_ref='fc'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!((lineage.as_str(), provider.as_str()), (SESSION, "google"));
    }

    #[test]
    fn production_home_override_drains_from_the_dot_gemini_layout() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join(".gemini/tmp");
        write_file(
            &root,
            DOC,
            &document(&[message("override", [10, 3, 4, 1, 0])]),
        );
        let env = crate::harness_paths::TestEnv::default()
            .set("GEMINI_CLI_HOME", temp.path().as_os_str());
        let state = test_state();
        let outcome = drain(&GeminiSource::from_env(&env), &state, 0);
        assert_eq!(outcome.rows_recorded, 1);
        assert_eq!(quantities(&state, "override")["input"], Some(6));
    }

    #[test]
    fn unknown_document_identity_forces_a_reparse() {
        let temp = TempDir::new().unwrap();
        let path = write_file(
            temp.path(),
            DOC,
            &document(&[message("m1", [10, 3, 4, 1, 0])]),
        );
        let mut outcome = GeminiDrainOutcome::default();
        let (mut mark, calls) = read_document(&path, None, 0, &mut outcome).unwrap();
        assert_eq!(calls.unwrap().0.len(), 1);
        mark.inode = None;
        let (_, calls) = read_document(&path, Some(&mark), 1, &mut outcome).unwrap();
        assert_eq!(calls.unwrap().0.len(), 1);
        assert_eq!(outcome.documents_parsed, 2);
        assert_eq!(outcome.documents_unchanged, 0);
    }

    /// An unchanged document is not re-parsed; a rewritten one is, and its
    /// earlier calls are exact redeliveries (no redelivery rows). A copy of
    /// the session in another file is the same events.
    #[test]
    fn a_rewritten_document_adds_only_its_new_calls() {
        let temp = TempDir::new().unwrap();
        let first = message("m1", [10, 3, 4, 1, 0]);
        let path = write_file(temp.path(), DOC, &document(std::slice::from_ref(&first)));
        let state = test_state();
        let source = GeminiSource::at(temp.path());
        assert_eq!(drain(&source, &state, 0).rows_recorded, 1);
        let quiet = drain(&source, &state, 1);
        assert_eq!((quiet.documents_unchanged, quiet.hashed_bytes), (1, 0));

        fs::write(&path, document(&[first, message("m2", [20, 3, 4, 0, 0])])).unwrap();
        write_file(
            temp.path(),
            "copy/chats/session-copy.json",
            &fs::read_to_string(&path).unwrap(),
        );
        let rewritten = drain(&source, &state, 2);
        assert_eq!(rewritten.documents_parsed, 2);
        assert_eq!(
            (rewritten.rows_recorded, rewritten.exact_redeliveries),
            (1, 3)
        );
        assert_eq!(count(&state, "SELECT COUNT(*) FROM redelivery_log"), 0);
        assert_eq!(rewritten.quarantined_redeliveries, 0);
    }

    /// The hourly verification re-hashes an unchanged document instead of
    /// trusting its stat forever, and still does not re-parse it.
    #[test]
    fn an_unchanged_document_is_re_hashed_on_the_verification_cadence() {
        let temp = TempDir::new().unwrap();
        let contents = document(&[message("m1", [10, 3, 4, 1, 0])]);
        write_file(temp.path(), DOC, &contents);
        let state = test_state();
        let source = GeminiSource::at(temp.path());
        drain(&source, &state, 0);
        let due = drain(&source, &state, FULL_PREFIX_VERIFY_INTERVAL_MS);
        assert_eq!(due.documents_unchanged, 1);
        assert_eq!(due.hashed_bytes, contents.len() as u64);
        assert_eq!(due.rows_recorded + due.exact_redeliveries, 0);
    }

    #[test]
    fn a_same_stat_rewrite_reads_and_hashes_the_document_once() {
        let temp = TempDir::new().unwrap();
        let original = document(&[message("m1", [10, 3, 4, 1, 0])]);
        let path = write_file(temp.path(), DOC, &original);
        let state = test_state();
        let source = GeminiSource::at(temp.path());
        drain(&source, &state, 0);

        let rewritten = original.replace("\"m1\"", "\"m2\"");
        assert_eq!(rewritten.len(), original.len());
        fs::write(&path, rewritten).unwrap();
        let file = fs::File::open(&path).unwrap();
        let metadata = file.metadata().unwrap();
        let mut cache = source.cache.lock().unwrap();
        let mark = cache.documents.get_mut(&path).unwrap();
        mark.len = metadata.len();
        mark.modified = metadata.modified().ok();
        mark.inode = inode_of(&file, &metadata);
        drop(cache);

        let outcome = drain(&source, &state, FULL_PREFIX_VERIFY_INTERVAL_MS);
        assert_eq!(outcome.documents_parsed, 1);
        assert_eq!(outcome.calls_emitted, 1);
        assert_eq!(outcome.hashed_bytes, metadata.len());
    }

    /// A JSONL recording resumes: a grown file costs its window plus the
    /// appended bytes, and only the new message lands.
    #[test]
    fn a_jsonl_recording_resumes_with_bounded_cost() {
        let temp = TempDir::new().unwrap();
        let filler = r#"{"id":"u","timestamp":"2026-06-16T21:45:21.315Z","type":"user","content":[{"text":"filler"}]}"#;
        let lines = (8 * PREFIX_WINDOW_BYTES as usize) / filler.len() + 1;
        let mut messages = vec![filler.to_string(); lines];
        messages.push(message("g1", [100, 5, 60, 2, 0]));
        let path = write_file(temp.path(), LINES, &recording(&messages));
        let state = test_state();
        let source = GeminiSource::at(temp.path());
        drain(&source, &state, 0);
        let appended = message("g2", [7, 1, 0, 0, 0]) + "\n";
        {
            use std::io::Write;
            let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(appended.as_bytes()).unwrap();
        }
        let grown = drain(&source, &state, 1);
        assert_eq!((grown.jsonl_resumed, grown.rows_recorded), (1, 1));
        let bound = 2 * PREFIX_WINDOW_BYTES + appended.len() as u64;
        assert!(
            grown.hashed_bytes <= bound,
            "{} > {bound}",
            grown.hashed_bytes
        );
    }

    /// A malformed message is counted per file; the document's other
    /// messages still land.
    #[test]
    fn a_malformed_message_is_counted_per_file() {
        let temp = TempDir::new().unwrap();
        let bad = message("bad", [1, 1, 0, 0, 0]).replace(r#""input":1,"#, r#""input":-1,"#);
        let path = write_file(
            temp.path(),
            DOC,
            &document(&[bad, message("ok", [1, 1, 0, 0, 0])]),
        );
        let state = test_state();
        let outcome = drain(&GeminiSource::at(temp.path()), &state, 0);
        assert_eq!(outcome.skipped_lines_by_file, BTreeMap::from([(path, 1)]));
        assert_eq!(outcome.rows_recorded, 1);
    }

    #[test]
    fn absent_root_is_no_lane_but_an_unreadable_root_is_cold() {
        let temp = TempDir::new().unwrap();
        let state = test_state();
        let absent = GeminiSource::from_home(Some(temp.path().as_os_str().to_owned()), None);
        assert!(drain(&absent, &state, 0).root_absent);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='gemini'"
            ),
            0
        );
        write_file(temp.path(), ".gemini/tmp", "a file, not a directory");
        let error = absent
            .drain(&state, 1, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("read Gemini CLI root"), "{error}");
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM source_runtime
                 WHERE source_key='gemini' AND last_success_ms IS NULL AND pending_unknown=1"
            ),
            1
        );
        let configured = GeminiSource::from_home(
            Some(OsString::from("/nonexistent-home")),
            Some(temp.path().join("custom").into_os_string()),
        );
        write_file(temp.path(), &format!("custom/tmp/{DOC}"), &document(&[]));
        assert_eq!(drain(&configured, &state, 2).files_found, 1);
        let relative = GeminiSource::from_home(Some(OsString::from("relative")), None);
        assert!(relative.drain(&state, 3, &NEVER_STOP).is_err());
    }

    #[test]
    fn refresher_iteration_drains_gemini_and_clears_its_health() {
        let temp = TempDir::new().unwrap();
        write_file(
            temp.path(),
            DOC,
            &document(&[message("m1", [10, 3, 4, 1, 0])]),
        );
        let state = test_state();
        run_iteration_with_harness(&state, &[], &GeminiSource::at(temp.path()), &NEVER_STOP);
        assert_eq!(
            count(
                &state,
                "SELECT COUNT(*) FROM usage_event WHERE producer='gemini'"
            ),
            1
        );
        assert!(state.producer_degraded_reason("gemini").is_none());
    }

    /// Per-class totals of a raw `serde_json` scan sharing no code with the
    /// reader: one response per distinct (session, message id), plus the
    /// convention checks the roles rely on.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct RawTotals {
        calls: u64,
        /// Gross `input`, cached included.
        input: u64,
        cached: u64,
        output: u64,
        thoughts: u64,
    }

    #[derive(Debug, Default)]
    struct Convention {
        token_records: u64,
        total_identity_holds: u64,
        non_zero_cached: u64,
        cached_added_on_top: u64,
        non_zero_thoughts: u64,
        non_zero_tool: u64,
        unparseable_documents: u64,
    }

    fn raw_totals(files: &[PathBuf]) -> (RawTotals, Convention) {
        let mut totals = RawTotals::default();
        let mut convention = Convention::default();
        let mut seen = BTreeSet::new();
        for path in files {
            let text = fs::read_to_string(path).unwrap();
            let mut messages = Vec::new();
            let mut session = None;
            if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                for line in text.lines().filter(|line| !line.trim().is_empty()) {
                    let value: serde_json::Value = serde_json::from_str(line).unwrap();
                    if session.is_none() {
                        session = value["sessionId"].as_str().map(str::to_string);
                    }
                    if let Some(set) = value["$set"]["messages"].as_array() {
                        messages.extend(set.iter().cloned());
                    } else {
                        messages.push(value);
                    }
                }
            } else {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                    convention.unparseable_documents += 1;
                    continue;
                };
                session = value["sessionId"].as_str().map(str::to_string);
                messages.extend(value["messages"].as_array().into_iter().flatten().cloned());
            }
            for message in messages {
                let tokens = &message["tokens"];
                if !tokens.is_object() {
                    continue;
                }
                let field = |name: &str| tokens[name].as_u64().unwrap();
                convention.token_records += 1;
                let (input, output, cached, thoughts, tool, total) = (
                    field("input"),
                    field("output"),
                    field("cached"),
                    field("thoughts"),
                    field("tool"),
                    field("total"),
                );
                convention.total_identity_holds +=
                    u64::from(total == input + output + thoughts + tool);
                convention.non_zero_cached += u64::from(cached > 0);
                convention.cached_added_on_top +=
                    u64::from(cached > 0 && total == input + output + thoughts + tool + cached);
                convention.non_zero_thoughts += u64::from(thoughts > 0);
                convention.non_zero_tool += u64::from(tool > 0);
                if !seen.insert((session.clone(), message["id"].to_string())) {
                    continue;
                }
                totals.calls += 1;
                totals.input += input;
                totals.cached += cached;
                totals.output += output;
                totals.thoughts += thoughts;
            }
        }
        (totals, convention)
    }

    /// Run with `--ignored`. Reads the live corpus (never writes it) into a
    /// scratch store and reconciles the raw scan, the reader's calls and the
    /// ledger's rows.
    #[test]
    #[ignore = "reads the mutable ~/.gemini/tmp corpus"]
    fn live_gemini_corpus_reconciles_raw_reader_and_ledger() {
        let root = std::env::var_os("GEMINI_CLI_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap()).join(".gemini"))
            .join("tmp");
        let files = files_below(&root, &is_gemini_chat_recording).unwrap();
        let (raw, convention) = raw_totals(&files);

        let mut reader = RawTotals::default();
        let mut counts = GeminiParseCounts::default();
        let mut seen = BTreeSet::new();
        for path in &files {
            let (calls, parse_counts) = if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                let parse = parse_gemini_jsonl_resumable(path, None, PrefixCheck::Full);
                (parse.calls, parse.counts)
            } else {
                let parse = parse_gemini_json_document(path).unwrap();
                (parse.calls, parse.counts)
            };
            counts.add(&parse_counts);
            for call in calls {
                // The same session saved in several files: count once, as
                // the ledger does.
                if !seen.insert((call.session_id.clone(), call.message_id.clone())) {
                    continue;
                }
                let usage = call.usage;
                reader.calls += 1;
                reader.cached += usage.cache_read.unwrap();
                reader.input += usage.input.quantity().unwrap() + usage.cache_read.unwrap();
                reader.output += usage.output.quantity().unwrap();
                reader.thoughts += usage.reasoning.unwrap();
            }
        }

        let state = test_state();
        let outcome = drain(&GeminiSource::at(&root), &state, 4_000_000_000_000);
        let ledger_role = |role: &str| -> u64 {
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COALESCE(SUM(measurement.quantity_units),0)
                         FROM measurement JOIN usage_event USING(event_key)
                         WHERE usage_event.producer='gemini' AND measurement.role=?1",
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
                "SELECT COUNT(*) FROM usage_event WHERE producer='gemini'",
            ) as u64,
            input: ledger_role("input") + ledger_role("cache_read"),
            cached: ledger_role("cache_read"),
            output: ledger_role("output"),
            thoughts: ledger_role("reasoning"),
        };
        eprintln!("files: {}", files.len());
        eprintln!("raw: {raw:?}");
        eprintln!("convention: {convention:?}");
        eprintln!("reader: {reader:?} counts {counts:?}");
        eprintln!("ledger: {ledger:?} outcome {outcome:?}");

        // Never a vacuous pass.
        assert!(raw.calls > 0 && raw.input > 0 && raw.cached > 0 && raw.output > 0);
        assert!(raw.thoughts > 0);
        // The convention the reader relies on; if any fails, re-establish it.
        assert_eq!(convention.total_identity_holds, convention.token_records);
        assert!(convention.non_zero_cached > 0);
        assert_eq!(convention.cached_added_on_top, 0);
        assert!(convention.non_zero_thoughts > 0);
        assert_eq!(convention.non_zero_tool, 0, "tool convention unmeasured");

        assert_eq!(counts.skipped_lines, 0);
        assert_eq!(
            counts.usage_not_ingested(),
            convention.unparseable_documents
        );
        assert_eq!(reader, raw);
        assert_eq!(ledger, raw);
        assert_eq!(outcome.quarantined_redeliveries, 0);
    }
}
