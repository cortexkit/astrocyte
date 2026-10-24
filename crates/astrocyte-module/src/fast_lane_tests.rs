use super::*;
use crate::state::{test_state, transaction_commits_for_test};
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

const NOW: i64 = 1_790_000_100_000;

pub(super) struct Corpus {
    pub(super) directory: TempDir,
    pub(super) opencode: Connection,
    pub(super) broca: Connection,
    pub(super) pi_path: PathBuf,
    pub(super) files: FileSources,
    pub(super) producers: Vec<Producer>,
    pub(super) stop: Arc<AtomicBool>,
}

impl Corpus {
    pub(super) fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let opencode_path = directory.path().join("opencode.db");
        let opencode = Connection::open(&opencode_path).unwrap();
        opencode
            .execute_batch(
                "PRAGMA journal_mode=WAL;
            CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, data TEXT NOT NULL);
            CREATE TABLE session_v2(id TEXT PRIMARY KEY);
            CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                type TEXT NOT NULL,seq INTEGER NOT NULL,time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,data TEXT NOT NULL);",
            )
            .unwrap();
        let broca_path = directory.path().join("broca.db");
        let broca = Connection::open(&broca_path).unwrap();
        broca
            .execute_batch(
                "PRAGMA journal_mode=WAL;
            CREATE TABLE export_meta(id INTEGER PRIMARY KEY,source_epoch INTEGER NOT NULL,
                source_instance_id TEXT NOT NULL, high_water INTEGER NOT NULL);
            INSERT INTO export_meta VALUES(1,1,'fast-lane',0);
            CREATE TABLE export_facts(export_seq INTEGER PRIMARY KEY, fact_id TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL, segment_json TEXT NOT NULL);",
            )
            .unwrap();
        let pi_root = directory.path().join("pi");
        fs::create_dir(&pi_root).unwrap();
        let pi_path = pi_root.join("session.jsonl");
        fs::write(&pi_path, "{\"type\":\"session\",\"id\":\"pi-session\",\"cwd\":\"/synthetic\",\"timestamp\":\"2026-09-21T00:00:00Z\"}\n").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let files = FileSources {
            opencode: Some(Arc::new(opencode_source::OpenCodeSource::at(
                &opencode_path,
            ))),
            pi: Some(Arc::new(pi_source::PiSource::at(pi_root))),
            harnesses: vec![Arc::new(opencode_v2_source::OpenCodeV2Source::at(
                &opencode_path,
            ))],
            ..FileSources::default()
        };
        let producers = vec![Producer {
            source_key: "broca".into(),
            client: Arc::new(broca_source::BrocaSource::at(broca_path, stop.clone())),
        }];
        Self {
            directory,
            opencode,
            broca,
            pi_path,
            files,
            producers,
            stop,
        }
    }

    pub(super) fn append(&self, index: i64) {
        self.append_opencode(index);
        self.append_pi(index);
        let mut fact: Value = serde_json::from_str(include_str!(
            "../tests/vectors/broca-model-step/model_step_fact.json"
        ))
        .unwrap();
        let id = format!("fast-fact-{index}");
        fact["fact_id"] = json!(id);
        fact["step_id"] = json!(index);
        self.broca
            .execute(
                "INSERT INTO export_facts VALUES(?1,?2,'run-golden',?3)",
                params![index, id, fact.to_string()],
            )
            .unwrap();
        self.broca
            .execute("UPDATE export_meta SET high_water=?1", [index])
            .unwrap();
    }

    pub(super) fn append_opencode(&self, index: i64) {
        let payload = json!({"role":"assistant","modelID":"claude-sonnet-4","providerID":"anthropic",
            "time":{"created":NOW + index,"completed":NOW + index + 1},"finish":"stop",
            "tokens":{"input":3,"output":13,"reasoning":2,"cache":{"read":5,"write":7}}});
        self.opencode
            .execute(
                "INSERT INTO message VALUES(?1,'oc-session',?2,?2,?3)",
                params![format!("message-{index}"), NOW + index, payload.to_string()],
            )
            .unwrap();
    }

    pub(super) fn append_pi(&self, index: i64) {
        let line = json!({"type":"message","id":format!("entry-{index}"),"timestamp":"2026-09-21T00:00:01Z",
            "message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4",
                "responseId":format!("response-{index}"),"stopReason":"stop",
                "usage":{"input":11,"output":7,"cacheRead":13,"cacheWrite":17,"reasoning":2,"totalTokens":48}}});
        writeln!(
            OpenOptions::new().append(true).open(&self.pi_path).unwrap(),
            "{line}"
        )
        .unwrap();
    }

    fn append_v2(&self, index: i64) {
        let payload = json!({"type":"assistant", "agent":"build",
            "model":{"providerID":"anthropic","id":"claude-sonnet-4"},
            "time":{"created":NOW + index,"completed":NOW + index + 1},"finish":"stop",
            "tokens":{"input":3,"output":13,"reasoning":17,"cache":{"read":5,"write":7}}});
        self.opencode
            .execute(
                "INSERT INTO session_message VALUES(?1,'oc-v2-session','assistant',?2,?3,?3,?4)",
                params![
                    format!("v2-message-{index}"),
                    index,
                    NOW + index,
                    payload.to_string()
                ],
            )
            .unwrap();
    }

    pub(super) fn full(&self, state: &SharedState, at: i64) {
        let _hold = hold_pricing_pass_for_test(state);
        run_iteration_all(
            state,
            &self.producers,
            self.files.opencode.as_deref(),
            self.files.pi.as_deref(),
            self.files.claude_code.as_deref(),
            self.files.codex.as_deref(),
            &self
                .files
                .harnesses
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            None,
            true,
            &mut PricingWalk::new(PricingBudget::PER_TICK),
            &self.stop,
        );
        // Source clocks are set explicitly so subsequent assertions do not depend on wall time.
        state
            .ordered_write(|tx| {
                tx.execute("UPDATE source_runtime SET last_success_ms=?1", [at])
                    .map_err(|e| e.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    fn due(&self, state: &SharedState) {
        let mut next = FAST_LANE_INTERVAL;
        fast_lane_if_due(
            state,
            &self.producers,
            &self.files,
            &self.stop,
            FAST_LANE_INTERVAL,
            Duration::from_secs(30),
            &mut next,
        );
        assert_eq!(next, FAST_LANE_INTERVAL * 2);
    }
}

fn runtime(state: &SharedState) -> Vec<Vec<SqlValue>> {
    rows(state, "source_runtime")
}

fn rows(state: &SharedState, table: &str) -> Vec<Vec<SqlValue>> {
    state
        .with_conn(|conn| {
            let mut statement = conn
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                .map_err(|e| e.to_string())?;
            let count = statement.column_count();
            let rows = statement
                .query_map([], |row| (0..count).map(|index| row.get(index)).collect())
                .map_err(|e| e.to_string())?;
            rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
        })
        .unwrap()
}

#[test]
fn fresh_opencode_broca_and_jsonl_steps_are_served_on_the_next_fast_interval() {
    let corpus = Corpus::new();
    let state = test_state();
    state.set_recent_steps_now_for_test(NOW);
    corpus.full(&state, NOW);
    corpus.append(1);
    let query = |harness: &str, session: &str| {
        crate::ops::dispatch(
            &state,
            crate::ops::OP_CACHE_STEPS,
            json!({"harness":harness,"session":session}),
        )
        .unwrap()
    };
    let mut next = FAST_LANE_INTERVAL;
    fast_lane_if_due(
        &state,
        &corpus.producers,
        &corpus.files,
        &corpus.stop,
        FAST_LANE_INTERVAL - Duration::from_nanos(1),
        Duration::from_secs(30),
        &mut next,
    );
    for (harness, session) in [
        ("opencode", "oc-session"),
        ("pi", "pi-session"),
        ("broca", "golden-session"),
    ] {
        assert_eq!(query(harness, session)["steps"], json!([]));
    }
    corpus.due(&state);
    for (harness, session, step, input, output) in [
        ("opencode", "oc-session", "message-1", 3, 13),
        ("pi", "pi-session", "pi:response:response-1", 11, 5),
        ("broca", "golden-session", "run-golden#1", 3, 512),
    ] {
        let reply = query(harness, session);
        assert_eq!(
            reply["steps"].as_array().unwrap().len(),
            1,
            "{harness}: {reply}"
        );
        assert_eq!(reply["steps"][0]["step_ref"], step);
        assert_eq!(reply["steps"][0]["input"], input);
        assert_eq!(reply["steps"][0]["output"], output);
        assert_eq!(reply["steps"][0]["finish"], "stop");
    }
    assert_eq!(
        rows(&state, "usage_event").len(),
        2,
        "BROCA diagnostics are not spend"
    );
}

#[test]
fn fresh_v2_only_completed_row_is_served_on_the_next_fast_interval() {
    let corpus = Corpus::new();
    let state = test_state();
    state.set_recent_steps_now_for_test(NOW);
    corpus.full(&state, NOW);
    let health = runtime(&state);
    let query = || {
        crate::ops::dispatch(
            &state,
            crate::ops::OP_CACHE_STEPS,
            json!({"harness":"opencode","session":"oc-v2-session"}),
        )
        .unwrap()
    };
    corpus.append_v2(1);
    let mut next = FAST_LANE_INTERVAL;
    fast_lane_if_due(
        &state,
        &corpus.producers,
        &corpus.files,
        &corpus.stop,
        FAST_LANE_INTERVAL - Duration::from_nanos(1),
        Duration::from_secs(30),
        &mut next,
    );
    assert_eq!(query()["steps"], json!([]));
    corpus.due(&state);
    let reply = query();
    assert_eq!(reply["steps"].as_array().unwrap().len(), 1, "{reply}");
    assert_eq!(reply["steps"][0]["step_ref"], "v2-message-1");
    assert_eq!(reply["steps"][0]["input"], 3);
    assert_eq!(reply["steps"][0]["output"], 13);
    assert_eq!(reply["steps"][0]["finish"], "stop");
    assert_eq!(rows(&state, "usage_event").len(), 1);
    assert_eq!(runtime(&state), health);
    corpus.due(&state);
    assert_eq!(query()["steps"], reply["steps"]);
    assert_eq!(rows(&state, "usage_event").len(), 1);
}

#[test]
fn absent_v2_marker_keeps_fast_cycles_query_free_until_a_full_tick() {
    use astrocyte_readers::read_io;
    let mut corpus = Corpus::new();
    corpus.files.opencode = None;
    corpus.files.pi = None;
    corpus
        .opencode
        .execute("DROP TABLE session_v2", [])
        .unwrap();
    corpus.append_v2(1);
    let state = test_state();
    corpus.full(&state, NOW);
    assert!(rows(&state, "usage_event").is_empty());
    let health = runtime(&state);
    for added_marker in [false, true] {
        if added_marker {
            corpus
                .opencode
                .execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY)", [])
                .unwrap();
        }
        read_io::reset_counts();
        run_fast_lane(&state, &[], &corpus.files, NOW, &corpus.stop);
        assert_eq!(read_io::counts(), read_io::Counts::default());
        assert!(rows(&state, "usage_event").is_empty());
        assert_eq!(runtime(&state), health);
    }
    corpus.full(&state, NOW + 30_000);
    assert_eq!(rows(&state, "usage_event").len(), 1);
}

#[test]
fn v2_fast_lane_never_runs_an_overdue_full_pass() {
    let mut corpus = Corpus::new();
    corpus.files.opencode = None;
    corpus.files.pi = None;
    let state = test_state();
    let at = current_time_ms();
    corpus.full(&state, at);
    let health = runtime(&state);
    corpus.append_v2(1);
    run_fast_lane(
        &state,
        &[],
        &corpus.files,
        at + 24 * 60 * 60 * 1000,
        &corpus.stop,
    );
    assert_eq!(rows(&state, "usage_event").len(), 1);
    assert_eq!(runtime(&state), health);
}

#[test]
fn opencode_exclusion_covers_both_fast_readers() {
    use astrocyte_readers::read_io;
    let corpus = Corpus::new();
    let state = test_state();
    corpus.full(&state, NOW);
    let mut config = state.desired_config();
    config.harness_ingest.exclude = vec!["opencode".into()];
    state.publish_desired(config, 1);
    let health = runtime(&state);
    corpus.append_opencode(1);
    corpus.append_v2(1);
    read_io::reset_counts();
    corpus.due(&state);
    assert_eq!(read_io::counts().connection_opens, 0);
    assert!(rows(&state, "usage_event").is_empty());
    assert_eq!(runtime(&state), health);
    let mut config = state.desired_config();
    config.harness_ingest.exclude.clear();
    state.publish_desired(config, 2);
    corpus.due(&state);
    assert_eq!(rows(&state, "usage_event").len(), 2);
}

#[test]
fn idle_fast_lane_commits_nothing_and_keeps_full_drain_health() {
    let corpus = Corpus::new();
    corpus.append(1);
    let state = test_state();
    corpus.full(&state, NOW);
    let health = runtime(&state);
    let events = rows(&state, "usage_event");
    let before = transaction_commits_for_test();
    corpus.due(&state);
    corpus.due(&state);
    assert_eq!(
        transaction_commits_for_test(),
        before,
        "idle tails must not COMMIT, including OpenCode's initial seed redeliveries"
    );
    assert_eq!(runtime(&state), health);
    assert_eq!(rows(&state, "usage_event"), events);
}

fn add_jsonl_inventory(corpus: &mut Corpus) -> usize {
    let root = corpus.directory.path();
    for (directory, file) in [
        ("claude/project", "session.jsonl"),
        ("codex/sessions/day", "rollout.jsonl"),
        ("gemini/chats", "session.jsonl"),
        ("omp/project", "session.jsonl"),
        ("kimi/group/session", "wire.jsonl"),
        (
            "cortexkit/thalamus-harness/claude-workers/worker/projects/project",
            "session.jsonl",
        ),
    ] {
        let directory = root.join(directory);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join(file), "{}\n").unwrap();
    }
    fs::write(root.join("cortexkit/thalamus-harness/claude-workers/roots.json"), json!({
        "version":1, "roots":[{"worker_id":"w-00000000000000000000000000000001", "path": root.join("cortexkit/thalamus-harness/claude-workers/worker"),
            "work_class":"subagent", "state":"active"}]
    }).to_string()).unwrap();
    corpus.files.claude_code = Some(Arc::new(claude_code_source::ClaudeCodeSource::at(
        root.join("claude"),
    )));
    corpus.files.codex = Some(Arc::new(codex_source::CodexSource::at(root.join("codex"))));
    let jsonl: Vec<Arc<dyn harness_drain::HarnessSource>> = vec![
        Arc::new(gemini_source::GeminiSource::at(root.join("gemini"))),
        Arc::new(omp_source::OmpSource::at(root.join("omp"))),
        Arc::new(kimi_source::KimiSource::at(root.join("kimi"))),
        Arc::new(claude_workers_source::ClaudeWorkersSource::from_data_home(
            root.into(),
        )),
    ];
    corpus.files.harnesses.extend(jsonl);
    7
}

#[test]
fn idle_fast_cycle_opens_nothing_and_stats_only_hot_files() {
    use astrocyte_readers::read_io;
    let mut corpus = Corpus::new();
    let hot_files = add_jsonl_inventory(&mut corpus);
    corpus.append(1);
    corpus.append_v2(1);
    let state = test_state();
    let at = current_time_ms();
    corpus.full(&state, at);
    run_fast_lane(&state, &corpus.producers, &corpus.files, at, &corpus.stop);
    let health = runtime(&state);
    let events = rows(&state, "usage_event");
    for _ in 0..3 {
        read_io::reset_counts();
        run_fast_lane(
            &state,
            &corpus.producers,
            &corpus.files,
            at + 2000,
            &corpus.stop,
        );
        let counts = read_io::counts();
        assert_eq!(counts.connection_opens, 0, "{counts:?}");
        assert_eq!(counts.statement_prepares, 0, "{counts:?}");
        assert_eq!(counts.directory_reads, 0, "{counts:?}");
        assert_eq!(counts.file_opens, 0, "{counts:?}");
        assert_eq!(
            counts.metadata_calls, hot_files,
            "each hot file is statted once: {counts:?}"
        );
    }
    assert_eq!(runtime(&state), health);
    assert_eq!(rows(&state, "usage_event"), events);
}

#[test]
fn new_opencode_row_reuses_prepared_statements() {
    use astrocyte_readers::read_io;
    let corpus = Corpus::new();
    corpus.append(1);
    let state = test_state();
    corpus.full(&state, NOW);
    run_fast_lane(&state, &corpus.producers, &corpus.files, NOW, &corpus.stop);
    corpus.append_opencode(2);
    read_io::reset_counts();
    run_fast_lane(
        &state,
        &corpus.producers,
        &corpus.files,
        NOW + 2000,
        &corpus.stop,
    );
    let counts = read_io::counts();
    assert_eq!(counts.connection_opens, 0, "{counts:?}");
    assert_eq!(counts.statement_prepares, 0, "{counts:?}");
    assert_eq!(rows(&state, "usage_event").len(), 3);
    assert_eq!(rows(&state, "step_detail").len(), 3);
    for connection in [&corpus.opencode, &corpus.broca] {
        let (busy, _, _): (i64, i64, i64) = connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(busy, 0, "a cached reader must not retain a read snapshot");
    }
}

#[test]
fn hot_files_expire_but_growth_keeps_them_hot_and_new_files_wait_for_full_tick() {
    use astrocyte_readers::read_io;
    let corpus = Corpus::new();
    corpus.append(1);
    let at = current_time_ms();
    let old = std::time::SystemTime::now() - Duration::from_secs(600);
    fs::File::open(&corpus.pi_path)
        .unwrap()
        .set_modified(old)
        .unwrap();
    let state = test_state();
    corpus.full(&state, at);
    corpus
        .files
        .pi
        .as_ref()
        .unwrap()
        .drain(&state, at, &corpus.stop)
        .unwrap();
    run_fast_lane(&state, &corpus.producers, &corpus.files, at, &corpus.stop);
    read_io::reset_counts();
    run_fast_lane(
        &state,
        &corpus.producers,
        &corpus.files,
        at + 300_001,
        &corpus.stop,
    );
    assert_eq!(
        read_io::counts().metadata_calls,
        0,
        "an old discovered file eventually leaves the hot set"
    );
    corpus.full(&state, at);
    corpus.append_pi(2);
    run_fast_lane(
        &state,
        &corpus.producers,
        &corpus.files,
        at + 2000,
        &corpus.stop,
    );
    read_io::reset_counts();
    run_fast_lane(
        &state,
        &corpus.producers,
        &corpus.files,
        at + 4000,
        &corpus.stop,
    );
    assert_eq!(read_io::counts().metadata_calls, 1);
    assert_eq!(rows(&state, "usage_event").len(), 3);
    corpus.append_pi(3);
    let bytes = fs::read_to_string(&corpus.pi_path)
        .unwrap()
        .replace("response-3", "new-session-response");
    fs::write(corpus.pi_path.parent().unwrap().join("new.jsonl"), bytes).unwrap();
    run_fast_lane(
        &state,
        &corpus.producers,
        &corpus.files,
        at + 6000,
        &corpus.stop,
    );
    assert_eq!(
        rows(&state, "usage_event").len(),
        4,
        "the existing file grows but the new session waits"
    );
    corpus.full(&state, at + 30_000);
    assert_eq!(rows(&state, "usage_event").len(), 5);
}

#[test]
fn fast_lane_event_bytes_match_full_drain() {
    let corpus = Corpus::new();
    let fast = test_state();
    let full = test_state();
    corpus.full(&fast, NOW);
    corpus.append(1);
    run_fast_lane(&fast, &corpus.producers, &corpus.files, NOW, &corpus.stop);
    let other = Corpus {
        files: FileSources {
            opencode: Some(Arc::new(opencode_source::OpenCodeSource::at(
                corpus.directory.path().join("opencode.db"),
            ))),
            pi: Some(Arc::new(pi_source::PiSource::at(
                corpus.pi_path.parent().unwrap(),
            ))),
            ..FileSources::default()
        },
        ..corpus
    };
    other
        .files
        .opencode
        .as_ref()
        .unwrap()
        .drain(&full, NOW, &other.stop)
        .unwrap();
    other
        .files
        .pi
        .as_ref()
        .unwrap()
        .drain(&full, NOW, &other.stop)
        .unwrap();
    drain_once(
        &full,
        other.producers[0].client.as_ref(),
        "broca",
        broca_source::PAGE_LIMIT,
        NOW,
    )
    .unwrap();
    for table in [
        "usage_event",
        "measurement",
        "step_detail",
        "broca_step",
        "pricing_pending",
    ] {
        assert!(
            !rows(&fast, table).is_empty(),
            "fixture must exercise {table}"
        );
        assert_eq!(
            rows(&fast, table),
            rows(&full, table),
            "{table} differs by lane"
        );
    }
}

#[test]
fn fast_lane_never_runs_due_full_passes_or_full_only_sources() {
    struct FullOnly(Arc<std::sync::atomic::AtomicUsize>);
    impl harness_drain::HarnessSource for FullOnly {
        fn source_key(&self) -> &'static str {
            "goose"
        }
        fn drain_for_health(
            &self,
            _: &SharedState,
            _: i64,
            _: &AtomicBool,
        ) -> Result<Option<String>, SourceDrainError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }
    let mut corpus = Corpus::new();
    let state = test_state();
    corpus.append(1);
    corpus
        .files
        .opencode
        .as_ref()
        .unwrap()
        .drain(&state, NOW, &corpus.stop)
        .unwrap();
    corpus
        .files
        .pi
        .as_ref()
        .unwrap()
        .drain(&state, NOW, &corpus.stop)
        .unwrap();
    let before = runtime(&state);
    let full_only = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    corpus
        .files
        .harnesses
        .push(Arc::new(FullOnly(full_only.clone())));
    corpus.append_opencode(2);
    // A full OpenCode pass is overdue, but only a normal tick may move its durable cursor.
    run_fast_lane(
        &state,
        &[],
        &corpus.files,
        NOW + 24 * 60 * 60 * 1000,
        &corpus.stop,
    );
    assert_eq!(rows(&state, "usage_event").len(), 3);
    assert_eq!(runtime(&state), before);
    assert_eq!(full_only.load(Ordering::Relaxed), 0);
}

#[test]
fn stopped_fast_lane_exits_without_draining_the_next_source() {
    use std::sync::mpsc;
    struct BlockingTail {
        entered: mpsc::Sender<()>,
        released: std::sync::Mutex<mpsc::Receiver<()>>,
    }
    impl harness_drain::HarnessSource for BlockingTail {
        fn source_key(&self) -> &'static str {
            "kimi"
        }
        fn drain_for_health(
            &self,
            _: &SharedState,
            _: i64,
            _: &AtomicBool,
        ) -> Result<Option<String>, SourceDrainError> {
            panic!("full drain called between ticks")
        }
        fn drain_fast(
            &self,
            _: &SharedState,
            _: i64,
            stop: &AtomicBool,
        ) -> Result<(), SourceDrainError> {
            self.entered.send(()).unwrap();
            self.released
                .lock()
                .unwrap()
                .recv_timeout(opencode_source::fixtures::STOP_BOUND)
                .unwrap();
            assert!(stop.load(Ordering::Relaxed));
            Err(SourceDrainError::Stopped)
        }
    }
    struct MustNotRead;
    impl SourceClient for MustNotRead {
        fn pull(&self, _: Option<SourceCursor>, _: u32) -> Result<ExportPage, PullError> {
            panic!("producer read after stop")
        }
    }
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let thread = std::thread::spawn(move || {
        let state = test_state();
        let before = transaction_commits_for_test();
        let sources = FileSources {
            harnesses: vec![Arc::new(BlockingTail {
                entered: entered_tx,
                released: std::sync::Mutex::new(release_rx),
            })],
            ..FileSources::default()
        };
        let producers = vec![Producer {
            source_key: "broca".into(),
            client: Arc::new(MustNotRead),
        }];
        let mut next = FAST_LANE_INTERVAL;
        fast_lane_if_due(
            &state,
            &producers,
            &sources,
            &thread_stop,
            FAST_LANE_INTERVAL,
            Duration::from_secs(30),
            &mut next,
        );
        assert_eq!(transaction_commits_for_test(), before);
        done_tx.send(()).unwrap();
    });
    entered_rx
        .recv_timeout(opencode_source::fixtures::STOP_BOUND)
        .unwrap();
    stop.store(true, Ordering::Relaxed);
    release_tx.send(()).unwrap();
    done_rx
        .recv_timeout(opencode_source::fixtures::STOP_BOUND)
        .unwrap();
    thread.join().unwrap();
}
