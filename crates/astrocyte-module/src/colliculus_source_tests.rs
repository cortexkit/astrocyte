use super::*;
use crate::refresher::harness_drain::HarnessSource;
use crate::state::test_state;
use crate::test_support::Background;
use std::sync::{Arc, Mutex};
use subc_protocol::FrameType;
use subc_protocol::{Flags, Frame, Priority};

type Reply = Arc<dyn Fn(&Value) -> (FrameType, Value) + Send + Sync>;

struct Peer {
    path: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    thread: Option<Background<()>>,
}

impl Peer {
    fn start(reply: Reply) -> Self {
        let (listener, path, mut connection) =
            crate::test_support::fake_daemon("media-feed", 41, 42);
        connection.daemon_ver = "0.39.0".into();
        subc_transport::connection_file::write_atomic(&path, &connection).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let capture = requests.clone();
        let (release, mut released) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                loop {
                    tokio::select! {
                        _ = &mut released => break,
                        accepted = listener.accept() => {
                            let (mut stream, _) = accepted.unwrap();
                            let (reply, capture, connection) = (reply.clone(), capture.clone(), connection.clone());
                            tokio::spawn(async move {
                                use subc_transport::{read_frame, write_frame};
                                use tokio::io::AsyncWriteExt;
                                if subc_transport::auth::authenticate_server(&mut stream, &connection.key, &connection.daemon_id, &connection.daemon_ver, BUDGET).await.is_err() { return; }
                                while let Ok(Some(request)) = read_frame(&mut stream).await {
                                    let value: Value = serde_json::from_slice(&request.body).unwrap();
                                    capture.lock().unwrap().push(value.clone());
                                    let (kind, mut value) = reply(&value);
                                    // Error frames require a message even when a
                                    // fixture cares only about the structural code.
                                    if kind == FrameType::Error && value.get("message").is_none() {
                                        value["message"] = json!("fixture refusal");
                                    }
                                    let response = Frame::build(kind, Flags::new(false, Priority::Passive, false), request.header.channel, request.header.epoch, request.header.corr, serde_json::to_vec(&value).unwrap()).unwrap();
                                    if write_frame(&mut stream, &response).await.is_err() || stream.flush().await.is_err() { return; }
                                }
                            });
                        }
                    }
                }
            });
        });
        Self {
            path,
            requests,
            thread: Some(Background::new("media fake daemon", thread, move || {
                let _ = release.send(());
            })),
        }
    }
    fn source(&self) -> ColliculusSource {
        let mut source = ColliculusSource::new();
        source.fixture_connection = Some(self.path.clone());
        source
    }
    fn calls(&self, method: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|value| value["method"] == method)
            .cloned()
            .collect()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        drop(self.thread.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

fn opened() -> (FrameType, Value) {
    (
        FrameType::Response,
        json!({"op":"route.open","route_channel":7,"route_epoch":1}),
    )
}
fn result(value: Value) -> (FrameType, Value) {
    (FrameType::Response, json!({"result":value}))
}
fn page(entries: Vec<Value>) -> Value {
    if let Some(last) = entries.last() {
        json!({"last_seq":last["seq"],"entries":entries})
    } else {
        json!({"entries":[]})
    }
}
fn vendor_entry(seq: u64) -> Value {
    json!({"seq":seq,"record":{"record_id":format!("r-{seq}"),"principal":"person-a","job_id":"job-1","provider":"example","model":"speech","charge_basis":"vendor","auth_method":"api_key","started_at_ms":1,"finished_at_ms":2,"status":"done","selectors":{},"measurements":[{"role":"input_audio","unit":"second","quantity":"90","measure":"billed","source":"vendor_reply"}]}})
}
fn local_entry(seq: u64) -> Value {
    let mut local = vendor_entry(seq);
    local["record"]["charge_basis"] = json!("local");
    local["record"]
        .as_object_mut()
        .unwrap()
        .remove("auth_method");
    local["record"]["measurements"] = json!([]);
    local
}
fn tick(state: &SharedState, source: &ColliculusSource) {
    let _hold = crate::refresher::hold_pricing_pass_for_test(state);
    crate::refresher::run_iteration_with_harness(state, &[], source, &AtomicBool::new(false));
}
fn count(state: &SharedState, table: &str) -> i64 {
    state
        .with_conn(|conn| {
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())
        })
        .unwrap()
}
fn cursor(state: &SharedState) -> Option<String> {
    state
        .with_conn(|conn| {
            conn.query_row("SELECT through_seq FROM media_feed_cursor", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|error| error.to_string())
        })
        .unwrap()
}
fn coverage(state: &SharedState) -> astrocyte_core::report::LedgerCoverage {
    state
        .with_conn(|conn| {
            astrocyte_core::report::ledger_coverage(
                conn,
                &astrocyte_core::report::LedgerCoverageRequest {},
                100,
            )
            .map_err(|error| format!("{error:?}"))
        })
        .unwrap()
}
fn fixed_page(entries: Vec<Value>) -> Reply {
    Arc::new(move |value| {
        if value["op"] == "route.open" {
            return opened();
        }
        if value["method"] == "usage.ack" {
            return result(json!({"acked_through_seq":value["params"]["through_seq"]}));
        }
        assert_eq!(value["method"], "usage.read");
        assert_eq!(value["params"]["limit"], 1_000);
        let after = value["params"]["after_seq"].as_u64().unwrap();
        result(page(
            entries
                .iter()
                .filter(|entry| entry["seq"].as_u64().unwrap() > after)
                .cloned()
                .collect(),
        ))
    })
}

#[test]
fn media_route_absence_classifier_uses_only_daemon_codes() {
    for code in [error_codes::UNKNOWN_MODULE, error_codes::MODULE_REMOVED] {
        assert_eq!(refusal(code, &Value::Null), FeedError::Absent);
    }
}
#[test]
fn media_route_transient_classifier_preserves_structured_codes() {
    for code in [
        error_codes::MODULE_WARMING,
        error_codes::MODULE_RELOADING,
        error_codes::UNKNOWN_CHANNEL,
        error_codes::STALE_ROUTE_EPOCH,
    ] {
        assert_eq!(
            refusal(code, &Value::Null),
            FeedError::Transient(code.into())
        );
    }
}
#[test]
fn media_route_other_refusals_are_failures_not_absence() {
    for code in [
        "feed_not_admitted",
        error_codes::TARGET_UNAVAILABLE,
        "arbitrary_error",
    ] {
        assert!(
            matches!(refusal(code, &Value::Null), FeedError::Failed(reason) if reason.contains(code))
        );
    }
}

#[test]
fn media_feed_pages_commit_before_ack_and_retry_failed_ack() {
    let state = Arc::new(test_state());
    let observer = state.clone();
    let fail = AtomicBool::new(true);
    let served = Mutex::new(0_u32);
    let peer = Peer::start(Arc::new(move |value| {
        if value["op"] == "route.open" {
            assert_eq!(
                value["target"],
                json!({"kind":"management_surface","module_id":"colliculus"})
            );
            return opened();
        }
        match value["method"].as_str().unwrap() {
            "usage.read" => {
                let mut served = served.lock().unwrap();
                let response = match *served {
                    0 => {
                        assert_eq!(value["params"]["after_seq"], 0);
                        page(vec![vendor_entry(1)])
                    }
                    1 => {
                        assert_eq!(value["params"]["after_seq"], 1);
                        page(vec![vendor_entry(1), vendor_entry(2)])
                    }
                    _ => {
                        assert_eq!(value["params"]["after_seq"], 2);
                        page(vec![])
                    }
                };
                *served += 1;
                result(response)
            }
            "usage.ack" => {
                let through = value["params"]["through_seq"].as_u64().unwrap();
                assert_eq!(
                    cursor(&observer).as_deref(),
                    Some(through.to_string().as_str())
                );
                assert_eq!(
                    count(&observer, "usage_event"),
                    i64::try_from(through).unwrap()
                );
                if fail.swap(false, Ordering::Relaxed) {
                    return (FrameType::Error, json!({"code":"temporary_vendor_error"}));
                }
                result(json!({"acked_through_seq":through}))
            }
            other => panic!("unexpected method {other}"),
        }
    }));
    let source = peer.source();
    tick(&state, &source);
    assert_eq!(cursor(&state).as_deref(), Some("1"));
    assert!(state.producer_degraded_reason(SOURCE_KEY).is_some());
    tick(&state, &source);
    assert_eq!(cursor(&state).as_deref(), Some("2"));
    assert_eq!(count(&state, "usage_event"), 2);
    assert_eq!(count(&state, "rejected_delivery"), 0);
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
    assert_eq!(peer.calls("usage.ack").len(), 3);
}

#[test]
fn media_failed_commit_acks_nothing_and_retries_the_page() {
    let state = test_state();
    state.with_conn(|conn| {
        conn.execute_batch("CREATE TABLE fail_at_commit(id INTEGER PRIMARY KEY, event_key INTEGER REFERENCES usage_event(event_key) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER reject_media_commit AFTER INSERT ON media_feed_cursor BEGIN INSERT INTO fail_at_commit VALUES(1,-1); END;").map_err(|error| error.to_string())
    }).unwrap();
    let peer = Peer::start(fixed_page(vec![
        vendor_entry(1),
        local_entry(2),
        json!({"seq":3,"raw":{"bytes_b64":"/w==","reason":"undecodable"}}),
    ]));
    let source = peer.source();
    tick(&state, &source);
    assert!(peer.calls("usage.ack").is_empty());
    assert_eq!(cursor(&state), None);
    assert_eq!(count(&state, "usage_event"), 0);
    assert_eq!(count(&state, "measurement"), 0);
    assert_eq!(count(&state, "set_aside_record"), 0);
    assert!(coverage(&state).skipped_local_by_source.is_empty());
    assert!(state
        .producer_degraded_reason(SOURCE_KEY)
        .unwrap()
        .contains("FOREIGN KEY"));
    state
        .with_conn(|conn| {
            conn.execute_batch("DROP TRIGGER reject_media_commit; DROP TABLE fail_at_commit;")
                .map_err(|error| error.to_string())
        })
        .unwrap();
    tick(&state, &source);
    assert_eq!(cursor(&state).as_deref(), Some("3"));
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(count(&state, "set_aside_record"), 1);
    assert_eq!(
        coverage(&state).skipped_local_by_source.get(SOURCE_KEY),
        Some(&1)
    );
    assert_eq!(peer.calls("usage.ack").len(), 1);
}

#[test]
fn media_local_work_is_skipped_and_counted_once_under_redelivery() {
    let state = test_state();
    let local = local_entry(1);
    let reads = Mutex::new(0_u32);
    let peer = Peer::start(Arc::new(move |value| {
        if value["op"] == "route.open" {
            return opened();
        }
        if value["method"] == "usage.ack" {
            return result(json!({"acked_through_seq":1}));
        }
        let mut reads = reads.lock().unwrap();
        let entries = if *reads == 0 || *reads == 2 {
            vec![local.clone()]
        } else {
            vec![]
        };
        *reads += 1;
        result(page(entries))
    }));
    let source = peer.source();
    tick(&state, &source);
    tick(&state, &source);
    assert_eq!(count(&state, "usage_event"), 0);
    assert_eq!(count(&state, "measurement"), 0);
    assert_eq!(
        coverage(&state).skipped_local_by_source.get(SOURCE_KEY),
        Some(&1)
    );
    assert!(coverage(&state).set_aside_by_source[SOURCE_KEY].records == 0);
    assert_eq!(peer.calls("usage.ack").len(), 3);
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
}

#[test]
fn media_raw_feed_vectors_are_set_aside_and_acknowledged() {
    let state = test_state();
    let vectors: Value =
        serde_json::from_str(cortexkit_role_media::vectors::file("usage-feed.json").unwrap())
            .unwrap();
    let entries = vectors["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["valid"] == true)
        .map(|case| case["entry"].clone())
        .collect();
    let peer = Peer::start(fixed_page(entries));
    tick(&state, &peer.source());
    assert_eq!(cursor(&state).as_deref(), Some("3"));
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(coverage(&state).set_aside_by_source[SOURCE_KEY].records, 2);
    let reasons = state
        .with_conn(|conn| {
            let mut statement = conn
                .prepare("SELECT reason FROM set_aside_record ORDER BY locator")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| error.to_string())?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())
        })
        .unwrap();
    assert!(reasons[0].contains("undecodable"));
    assert!(reasons[0].contains("/w=="));
    assert!(reasons[1].contains("oversize"));
    assert!(reasons[1].contains("262145"));
    assert!(reasons[1].contains("ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"));
    assert_eq!(peer.calls("usage.ack")[0]["params"]["through_seq"], 3);
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
}

#[test]
fn media_usage_vectors_drain_with_unrepresentable_evidence() {
    let state = test_state();
    let vectors: Value =
        serde_json::from_str(cortexkit_role_media::vectors::file("usage.json").unwrap()).unwrap();
    let entries = vectors["records"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, record)| json!({"seq":index+1,"record":record}))
        .collect();
    let peer = Peer::start(fixed_page(entries));
    tick(&state, &peer.source());
    assert_eq!(cursor(&state).as_deref(), Some("5"));
    assert_eq!(count(&state, "usage_event"), 2);
    assert_eq!(count(&state, "measurement"), 1);
    assert_eq!(
        coverage(&state).skipped_local_by_source.get(SOURCE_KEY),
        Some(&1)
    );
    assert_eq!(coverage(&state).set_aside_by_source[SOURCE_KEY].records, 2);
    let evidence = state
        .with_conn(|conn| {
            let mut statement = conn
                .prepare("SELECT locator,reason FROM set_aside_record ORDER BY locator")
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| error.to_string())?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())
        })
        .unwrap();
    assert_eq!(evidence[0].0, "usage.read/seq/1");
    assert_eq!(evidence[1].0, "usage.read/seq/4");
    assert!(evidence
        .iter()
        .all(|(_, reason)| reason.contains("not representable")));
    assert_eq!(peer.calls("usage.ack")[0]["params"]["through_seq"], 5);
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
}

#[test]
fn media_source_absent_leaves_health_and_runtime_untouched() {
    for code in [error_codes::UNKNOWN_MODULE, error_codes::MODULE_REMOVED] {
        let state = test_state();
        harness_drain::remove_runtime_row(&state, "broca").unwrap();
        let before = state
            .public_projection()
            .health(crate::refresher::current_time_ms());
        let peer = Peer::start(Arc::new(move |_| (FrameType::Error, json!({"code":code}))));
        tick(&state, &peer.source());
        assert_eq!(count(&state, "source_runtime"), 0);
        assert_eq!(count(&state, "media_feed_cursor"), 0);
        assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
        assert_eq!(
            state
                .public_projection()
                .health(crate::refresher::current_time_ms())
                .status,
            before.status
        );
        assert!(peer.calls("usage.read").is_empty());
    }
}

#[test]
fn media_feed_not_admitted_degrades_immediately() {
    let state = test_state();
    let peer = Peer::start(Arc::new(move |value| {
        if value["op"] == "route.open" {
            opened()
        } else {
            (FrameType::Error, json!({"code":"feed_not_admitted"}))
        }
    }));
    tick(&state, &peer.source());
    assert!(state
        .producer_degraded_reason(SOURCE_KEY)
        .unwrap()
        .contains("feed_not_admitted"));
    assert!(coverage(&state)
        .set_aside_by_source
        .contains_key(SOURCE_KEY));
    assert_eq!(cursor(&state), None);
    assert!(peer.calls("usage.ack").is_empty());
}

#[test]
fn media_transient_preserves_cursor_and_existing_health_until_recovery() {
    let state = test_state();
    state
        .with_conn(|conn| {
            conn.execute(
                "INSERT INTO media_feed_cursor VALUES('colliculus','9',0)",
                [],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
        .unwrap();
    state.mark_producer_degraded(SOURCE_KEY, "prior failure".into());
    let warming = Arc::new(AtomicBool::new(true));
    let server_warming = warming.clone();
    let peer = Peer::start(Arc::new(move |value| {
        if server_warming.load(Ordering::Relaxed) {
            return (
                FrameType::Error,
                json!({"code":error_codes::MODULE_WARMING}),
            );
        }
        if value["op"] == "route.open" {
            return opened();
        }
        if value["method"] == "usage.ack" {
            return result(json!({"acked_through_seq":9}));
        }
        result(page(vec![]))
    }));
    let source = peer.source();
    tick(&state, &source);
    tick(&state, &source);
    assert_eq!(
        state.producer_degraded_reason(SOURCE_KEY).as_deref(),
        Some("prior failure")
    );
    assert_eq!(cursor(&state).as_deref(), Some("9"));
    assert!(peer.calls("usage.ack").is_empty());
    assert!(source.transient_logged.load(Ordering::Relaxed));
    warming.store(false, Ordering::Relaxed);
    tick(&state, &source);
    assert!(!source.transient_logged.load(Ordering::Relaxed));
    assert_eq!(state.producer_degraded_reason(SOURCE_KEY), None);
    assert_eq!(peer.calls("usage.ack").len(), 1);
}

#[test]
fn media_other_refusals_keep_cursor_and_mark_the_drain_failed() {
    let state = test_state();
    state
        .with_conn(|conn| {
            conn.execute(
                "INSERT INTO media_feed_cursor VALUES('colliculus','7',0)",
                [],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
        .unwrap();
    let peer = Peer::start(Arc::new(move |_| {
        (
            FrameType::Error,
            json!({"code":error_codes::TARGET_UNAVAILABLE}),
        )
    }));
    tick(&state, &peer.source());
    assert_eq!(cursor(&state).as_deref(), Some("7"));
    assert!(state
        .producer_degraded_reason(SOURCE_KEY)
        .unwrap()
        .contains(error_codes::TARGET_UNAVAILABLE));
    assert!(peer.calls("usage.ack").is_empty());
}

#[test]
fn media_exclusion_key_is_known_and_default_on() {
    use crate::config::HarnessIngest;
    assert!(crate::config::INGEST_SOURCES.contains(&SOURCE_KEY));
    assert!(HarnessIngest::all_on().ingests(SOURCE_KEY));
    let excluded = HarnessIngest {
        enabled: true,
        exclude: vec![SOURCE_KEY.into()],
    };
    assert!(!excluded.ingests(SOURCE_KEY));
    let state = test_state();
    let peer = Peer::start(fixed_page(vec![vendor_entry(1)]));
    assert!(!crate::refresher::ingesting(&state, &excluded, SOURCE_KEY));
    assert!(peer.requests.lock().unwrap().is_empty());
    assert_eq!(count(&state, "usage_event"), 0);
    assert_eq!(peer.source().source_key(), SOURCE_KEY);
}

#[test]
fn media_attestation_requires_launch_identity_without_connection_file_fallback() {
    let state = test_state();
    let peer = Peer::start(fixed_page(vec![vendor_entry(1)]));
    state.install_quota_bus_access(
        peer.path.clone(),
        "astrocyte".into(),
        "fixture-handoff".into(),
    );
    let source = ColliculusSource::new();
    tick(&state, &source);
    state.acknowledge_module_hello();
    tick(&state, &source);
    assert!(
        peer.requests.lock().unwrap().is_empty(),
        "a readable connection file must not permit an unattested media route"
    );
    assert_eq!(count(&state, "usage_event"), 0);
    assert_eq!(cursor(&state), None);
}

#[test]
fn media_attestation_refusal_never_includes_the_launch_nonce() {
    let error = refusal(
        "bad_consumer_identity",
        &json!({"code":"bad_consumer_identity","message":"fixture-sensitive-nonce"}),
    );
    assert_eq!(
        error,
        FeedError::Failed("colliculus identity refused".into())
    );
}

#[test]
fn media_sdk_uses_attested_launch_identity() {
    // The nonce cache is process-wide. A child gives the real SDK a supervised
    // launch without changing the environment of concurrently running tests.
    if std::env::var("ASTROCYTE_MEDIA_NONCE_TEST").as_deref() != Ok("child") {
        subc_os::launch_nonce().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "refresher::colliculus_source::tests::media_sdk_uses_attested_launch_identity",
                "--nocapture",
            ])
            .env("ASTROCYTE_MEDIA_NONCE_TEST", "child")
            .env(SUBC_MODULE_ID_ENV, "astrocyte")
            .env("SUBC_LAUNCH_NONCE", "fixture-handoff")
            .env_remove("SUBC_LAUNCH_NONCE_FD");
        let output = crate::test_support::output_within(&mut command, "attested media consumer");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    subc_os::launch_nonce().unwrap();
    let peer = Peer::start(fixed_page(vec![vendor_entry(1)]));
    let state = test_state();
    state.install_quota_bus_access(
        peer.path.clone(),
        "astrocyte".into(),
        "fixture-handoff".into(),
    );
    let source = ColliculusSource::new();
    tick(&state, &source);
    assert!(
        peer.requests.lock().unwrap().is_empty(),
        "route opened before HELLO_ACK"
    );
    state.acknowledge_module_hello();
    tick(&state, &source);
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(cursor(&state).as_deref(), Some("1"));
    let requests = peer.requests.lock().unwrap();
    let opens: Vec<_> = requests
        .iter()
        .filter(|value| value["op"] == "route.open")
        .collect();
    assert_eq!(opens.len(), 1);
    assert_eq!(
        opens[0]["target"],
        json!({"kind":"management_surface","module_id":"colliculus"})
    );
    assert_eq!(opens[0]["consumer_identity"]["module_id"], "astrocyte");
    assert!(opens[0]["consumer_identity"]["launch_nonce"] == "fixture-handoff");
    assert_eq!(
        requests
            .iter()
            .filter(|value| value["method"] == "usage.ack")
            .count(),
        1
    );
}

#[test]
fn media_sdk_warming_deadline_keeps_typed_refusal_deferred() {
    let peer = Peer::start(Arc::new(|_| {
        (
            FrameType::Error,
            json!({"code":error_codes::MODULE_WARMING,"message":"still starting"}),
        )
    }));
    let state = test_state();
    let prior_runtime_rows = count(&state, "source_runtime");
    let source = peer.source();
    let outcome = harness_drain::HarnessSource::drain_for_health(
        &source,
        &state,
        100,
        &AtomicBool::new(false),
    );
    assert!(matches!(outcome, Err(SourceDrainError::Deferred)));
    assert_eq!(count(&state, "source_runtime"), prior_runtime_rows);
    assert_eq!(count(&state, "media_feed_cursor"), 0);
    assert!(state.producer_degraded_reason(SOURCE_KEY).is_none());
    assert!(
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|value| value["op"] == "route.open")
            .count()
            > 1
    );
    assert!(peer.calls("usage.read").is_empty());
    assert!(peer.calls("usage.ack").is_empty());
}

#[test]
fn media_sdk_error_retains_only_safe_structural_codes() {
    for code in ["bad_consumer_identity", "fixture-sensitive-nonce"] {
        let body = serde_json::from_value(json!({"code":code,"message":"fixture-sensitive-nonce","detail":{"launch_nonce":"fixture-sensitive-nonce"}})).unwrap();
        let error = sdk_error(CallError::Module(body));
        assert!(
            matches!(error, FeedError::Failed(ref reason) if !reason.contains("fixture-sensitive-nonce"))
        );
    }
}

#[test]
fn media_unsigned_feed_sequences_survive_the_signed_store_boundary() {
    let state = test_state();
    let peer = Peer::start(fixed_page(vec![vendor_entry(u64::MAX)]));
    tick(&state, &peer.source());
    assert_eq!(cursor(&state).as_deref(), Some("18446744073709551615"));
    assert_eq!(
        peer.calls("usage.ack")[0]["params"]["through_seq"],
        json!(u64::MAX)
    );
    assert_eq!(
        peer.calls("usage.read")[1]["params"]["after_seq"],
        json!(u64::MAX)
    );
    assert_eq!(count(&state, "usage_event"), 1);
}

#[test]
fn media_malformed_feed_page_keeps_cursor_and_acks_nothing() {
    let state = test_state();
    let peer = Peer::start(Arc::new(move |value| {
        if value["op"] == "route.open" {
            return opened();
        }
        result(json!({"entries":[vendor_entry(1)],"last_seq":2}))
    }));
    tick(&state, &peer.source());
    assert!(state
        .producer_degraded_reason(SOURCE_KEY)
        .unwrap()
        .contains("invalid media page"));
    assert_eq!(cursor(&state), None);
    assert_eq!(count(&state, "usage_event"), 0);
    assert!(peer.calls("usage.ack").is_empty());
}
