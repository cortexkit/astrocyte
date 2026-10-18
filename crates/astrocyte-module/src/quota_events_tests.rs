use super::*;
use crate::refresher::{CapacitySource, PullError};
use crate::test_support::{wait_until, Background};
use serde_json::{json, Value};
use std::sync::atomic::AtomicUsize;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn entry(account: &str, used: f64) -> Value {
    json!({"provider":"codex", "apiProvider":"openai", "account":account,
        "source":"oauth", "fetchedAt":"2026-10-18T10:00:00Z",
        "usage":{"primary":{"usedPercent":used}}})
}

struct Capacity(AtomicUsize);
impl CapacitySource for Capacity {
    fn poll(&self) -> Result<CapacitySnapshot, PullError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let entries: Vec<ProviderUsage> =
            serde_json::from_value(json!([entry("a", 20.0), entry("b", 70.0)])).unwrap();
        Ok(crate::capacity_map::map_usage_array(&entries, Some(["codex".into()].into())).unwrap())
    }
}

fn tick(state: &SharedState, capacity: Option<&dyn CapacitySource>) {
    crate::refresher::run_iteration_with_capacity(state, &[], capacity, &AtomicBool::new(false));
}

fn seeded() -> (SharedState, Capacity) {
    let state = crate::state::test_state();
    state
        .configure_capacity_runtime(true, 30_000, now_ms())
        .unwrap();
    let capacity = Capacity(AtomicUsize::new(0));
    tick(&state, Some(&capacity));
    (state, capacity)
}

fn rows(state: &SharedState) -> Vec<(String, f64, i64)> {
    state.with_conn(|conn| {
        let mut statement = conn.prepare("SELECT account_ref,used_percent,observed_at_ms FROM capacity_window ORDER BY account_ref").unwrap();
        let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap().collect::<Result<_,_>>().unwrap();
        Ok(rows)
    }).unwrap()
}

fn sample_count(state: &SharedState) -> i64 {
    state
        .with_conn(|conn| {
            Ok(conn
                .query_row("SELECT count(*) FROM window_sample", [], |row| row.get(0))
                .unwrap())
        })
        .unwrap()
}

fn notice(seq: u64, epoch: &str) -> Notice {
    Notice {
        provider: "codex".into(),
        account: Some("a".into()),
        epoch: epoch.into(),
        seq,
        key: "a".repeat(64),
        digest: String::new(),
    }
}

fn body(notice: &mut Notice) -> Vec<u8> {
    // Intentionally non-alphabetic field order: hashing a parsed/reordered Value must fail.
    let account = notice
        .account
        .as_ref()
        .map(|account| format!(",\"account\":{}", serde_json::to_string(account).unwrap()))
        .unwrap_or_default();
    let body = format!(
        r#"{{"provider":"codex"{account},"epoch":"{}","seq":{},"source":"oauth","fetchedAt":"2026-10-18T10:01:00Z","usage":{{"primary":{{"usedPercent":42.0}}}}}}"#,
        notice.epoch, notice.seq
    );
    notice.digest = format!("{:x}", Sha256::digest(body.as_bytes()));
    format!(r#"{{"result":{{"body":{body},"digest":"{}","event_key":"{}","event_name":"quota_sampled","published_at":{}}}}}"#,
        notice.digest, notice.key, now_ms() + 60_000).into_bytes()
}

fn cursors() -> Cursors {
    Cursors([(("codex".into(), Some("a".into())), ("run1".into(), 1))].into())
}

#[test]
fn quota_in_sequence_fetches_exact_digest_persists_only_its_account_before_completion() {
    let (state, _) = seeded();
    state
        .quota_bus()
        .pulled(chrono::Utc::now().timestamp() + 900, now_ms());
    state
        .ordered_write(|tx| {
            tx.execute(
                "UPDATE capacity_runtime SET last_success_ms=?1 WHERE id=1",
                [now_ms() - 90_000],
            )
            .unwrap();
            Ok(((), true))
        })
        .unwrap();
    let before = rows(&state);
    let mut notice = notice(2, "run1");
    let bytes = body(&mut notice);
    let fetched = AtomicUsize::new(0);
    let mut cursors = cursors();
    runtime().block_on(async {
        let mut admitted = Box::pin(cursors.admit(state.quota_bus(), notice, async {
            fetched.fetch_add(1, Ordering::SeqCst);
            Ok(bytes)
        }));
        assert!(
            futures_util::poll!(admitted.as_mut()).is_pending(),
            "delivery completed before the account write committed"
        );
        assert_eq!(fetched.load(Ordering::SeqCst), 1);
        assert_eq!(rows(&state), before);
        tick(&state, None);
        admitted.await.unwrap();
    });
    let after = rows(&state);
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].1.to_bits(), 42_f64.to_bits());
    assert_eq!(
        after[1], before[1],
        "another account was retired or touched"
    );
    assert_eq!(sample_count(&state), 3);
    assert!(
        state.capacity_poll_ticket(now_ms()).is_none(),
        "event commit did not reset the quiet-reading timer"
    );
}

#[test]
fn quota_gap_epoch_missing_body_or_digest_mismatch_owes_snapshot_before_completion() {
    for case in [
        "first",
        "gap",
        "epoch",
        "missing",
        "digest",
        "reply-digest",
        "body-identity",
    ] {
        let (state, capacity) = seeded();
        let before = rows(&state);
        let mut cursors = if case == "first" {
            Cursors::default()
        } else {
            cursors()
        };
        let mut notice = notice(
            if case == "gap" { 3 } else { 2 },
            if case == "epoch" { "run2" } else { "run1" },
        );
        let mut bytes = body(&mut notice);
        if case == "digest" {
            notice.digest = "0".repeat(64);
        }
        if case == "reply-digest" {
            bytes = String::from_utf8(bytes)
                .unwrap()
                .replace(
                    &format!("\"digest\":\"{}\"", notice.digest),
                    &format!("\"digest\":\"{}\"", "0".repeat(64)),
                )
                .into_bytes();
        }
        if case == "body-identity" {
            let mut other = notice.clone();
            other.seq = 99;
            bytes = body(&mut other);
            notice.digest = other.digest;
        }
        let fetched = AtomicUsize::new(0);
        runtime().block_on(async {
            let mut admitted = Box::pin(cursors.admit(state.quota_bus(), notice, async {
                fetched.fetch_add(1, Ordering::SeqCst);
                if case == "missing" {
                    Err(PullError::Unavailable("event_not_found".into()))
                } else {
                    Ok(bytes)
                }
            }));
            assert!(
                futures_util::poll!(admitted.as_mut()).is_ready(),
                "{case}: tried a partial write instead of recording a snapshot"
            );
        });
        let ticket = state
            .capacity_poll_ticket(now_ms())
            .expect("snapshot was not owed before completion");
        state.quota_bus().finish_poll(ticket, false);
        assert_eq!(rows(&state), before, "{case}: wrote partial state");
        assert_eq!(sample_count(&state), 2);
        assert_eq!(
            fetched.load(Ordering::SeqCst),
            usize::from(!matches!(case, "first" | "gap" | "epoch"))
        );
        tick(&state, Some(&capacity));
        assert_eq!(
            capacity.0.load(Ordering::SeqCst),
            2,
            "{case}: refresher did not take the owed snapshot"
        );
    }
}

#[test]
fn quota_duplicate_and_redelivery_do_not_fetch_write_or_owe_another_snapshot() {
    let (state, _) = seeded();
    state
        .quota_bus()
        .pulled(chrono::Utc::now().timestamp() + 900, now_ms());
    let before = rows(&state);
    let mut cursors = cursors();
    runtime().block_on(async {
        for seq in [1, 1] {
            cursors
                .admit(state.quota_bus(), notice(seq, "run1"), async {
                    panic!("duplicate fetched a second body");
                })
                .await
                .unwrap();
        }
    });
    assert!(
        state.capacity_poll_ticket(now_ms()).is_none(),
        "duplicate incurred a new snapshot"
    );
    tick(&state, None);
    assert_eq!(rows(&state), before);
    assert_eq!(sample_count(&state), 2);
}

#[test]
fn quota_unlabeled_event_updates_provider_readings_without_touching_labeled_accounts() {
    let (state, _) = seeded();
    let before = rows(&state);
    let mut notice = notice(2, "run1");
    notice.account = None;
    let bytes = body(&mut notice);
    let mut cursors = Cursors([(("codex".into(), None), ("run1".into(), 1))].into());
    runtime().block_on(async {
        let mut admitted = Box::pin(cursors.admit(state.quota_bus(), notice, async { Ok(bytes) }));
        assert!(futures_util::poll!(admitted.as_mut()).is_pending());
        tick(&state, None);
        admitted.await.unwrap();
    });
    assert_eq!(rows(&state), before);
    assert_eq!(sample_count(&state), 2);
    let used: f64 = state
        .with_conn(|conn| {
            Ok(conn
                .query_row(
                    "SELECT used_percent FROM capacity_provider_reading WHERE provider='codex'",
                    [],
                    |row| row.get(0),
                )
                .unwrap())
        })
        .unwrap();
    assert_eq!(used.to_bits(), 42_f64.to_bits());
}

#[test]
fn quota_healthy_reader_bounds_backstop_by_freshness_and_dead_reader_polls_every_tick() {
    let (state, capacity) = seeded();
    let now = now_ms();
    let exp = chrono::Utc::now().timestamp() + 3600;
    state.quota_bus().pulled(exp, now);
    tick(&state, Some(&capacity));
    tick(&state, Some(&capacity));
    assert_eq!(capacity.0.load(Ordering::SeqCst), 1);
    let freshness = state
        .with_conn(|conn| Ok(astrocyte_core::capacity::freshness(conn).unwrap()))
        .unwrap();
    let started = freshness.last_success_ms.unwrap();
    let quiet_interval = freshness.horizon_ms() - freshness.effective_cadence_ms;
    assert_eq!(quiet_interval, 90_000);
    state.quota_bus().pulled(exp, started + quiet_interval - 1);
    assert!(state
        .capacity_poll_ticket(started + quiet_interval - 1)
        .is_none());
    state.quota_bus().pulled(exp, started + quiet_interval);
    let ticket = state
        .capacity_poll_ticket(started + quiet_interval)
        .expect("quiet capacity would become stale before the backstop");
    state.quota_bus().finish_poll(ticket, true);
    state.quota_bus().unavailable("no credential");
    tick(&state, Some(&capacity));
    tick(&state, Some(&capacity));
    assert_eq!(capacity.0.load(Ordering::SeqCst), 3);
    state.quota_bus().pulled(exp, now_ms() - PULL_FRESH_MS);
    tick(&state, Some(&capacity));
    assert_eq!(
        capacity.0.load(Ordering::SeqCst),
        4,
        "stalled pull suppressed fallback"
    );

    state
        .configure_capacity_runtime(true, 300_000, now_ms())
        .unwrap();
    state.quota_bus().unavailable("bus down");
    tick(&state, Some(&capacity));
    let started = state
        .quota_bus()
        .0
        .lock()
        .unwrap()
        .last_snapshot_ms
        .unwrap();
    state.quota_bus().pulled(exp, started + BACKSTOP_MS - 1);
    assert!(state
        .capacity_poll_ticket(started + BACKSTOP_MS - 1)
        .is_none());
    state.quota_bus().pulled(exp, started + BACKSTOP_MS);
    assert!(
        state.capacity_poll_ticket(started + BACKSTOP_MS).is_some(),
        "backstop exceeded ten minutes"
    );
}

type Reply = Arc<dyn Fn(&Value) -> Option<(subc_protocol::FrameType, Vec<u8>)> + Send + Sync>;

struct Peer {
    path: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    opens: Arc<Mutex<Vec<Value>>>,
    thread: Option<Background<()>>,
}

impl Peer {
    fn start(reply: Reply) -> Self {
        let (listener, path, connection) = crate::test_support::fake_daemon("quota-events", 31, 32);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let opens = Arc::new(Mutex::new(Vec::new()));
        let (server_requests, server_opens) = (requests.clone(), opens.clone());
        let (release, mut released) = oneshot::channel();
        let thread = std::thread::spawn(move || {
            runtime().block_on(async {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            loop {
                tokio::select! {
                    _ = &mut released => break,
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let (reply, requests, opens, connection) = (reply.clone(), server_requests.clone(), server_opens.clone(), connection.clone());
                        tokio::spawn(async move {
                            use subc_transport::{read_frame, write_frame};
                            use tokio::io::AsyncWriteExt;
                            if subc_transport::auth::authenticate_server(&mut stream, &connection.key, &connection.daemon_id, &connection.daemon_ver, Duration::from_secs(2)).await.is_err() { return; }
                            while let Ok(Some(request)) = read_frame(&mut stream).await {
                                let value: Value = serde_json::from_slice(&request.body).unwrap();
                                let (kind, bytes) = if value["op"] == "route.open" {
                                    opens.lock().unwrap().push(value);
                                    (subc_protocol::FrameType::Response, serde_json::to_vec(&json!({"op":"route.open","route_channel":7,"route_epoch":1})).unwrap())
                                } else {
                                    requests.lock().unwrap().push(value.clone());
                                    match reply(&value) {
                                        Some(reply) => reply,
                                        None => { std::future::pending::<()>().await; unreachable!() }
                                    }
                                };
                                let frame = subc_protocol::Frame::build(kind, subc_protocol::Flags::new(false, subc_protocol::Priority::Passive, false),
                                    request.header.channel, request.header.epoch, request.header.corr, bytes).unwrap();
                                if write_frame(&mut stream, &frame).await.is_err() || stream.flush().await.is_err() { return; }
                            }
                        });
                    }
                }
            }
        })
        });
        Self {
            path,
            requests,
            opens,
            thread: Some(Background::new("quota fake daemon", thread, move || {
                let _ = release.send(());
            })),
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        drop(self.thread.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
fn quota_reader_shutdown_cancels_mid_session_and_joins_within_the_drain_guard() {
    let peer = Peer::start(Arc::new(|_| None));
    let state = Arc::new(crate::state::test_state());
    state.install_quota_bus_access(
        peer.path.clone(),
        "astrocyte".into(),
        "fixture-handoff".into(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let reader = Reader::start(state.clone(), peer.path.clone(), stop.clone()).unwrap();
    let stopped = stop.clone();
    let (ended, joined) = std::sync::mpsc::channel();
    let joining = std::thread::spawn(move || {
        reader.join();
        let _ = ended.send(());
    });
    let joining = Background::new("quota reader shutdown", joining, move || {
        stopped.store(true, Ordering::Relaxed)
    });
    wait_until("ckbus credential request", || {
        !peer.requests.lock().unwrap().is_empty()
    });
    let opens = peer.opens.lock().unwrap();
    assert_eq!(opens.len(), 1);
    assert_eq!(opens[0]["target"]["module_id"], "ckbus");
    assert_eq!(opens[0]["consumer_identity"]["module_id"], "astrocyte");
    assert!(opens[0]["consumer_identity"]["launch_nonce"] == "fixture-handoff");
    drop(opens);
    stop.store(true, Ordering::Relaxed);
    joined
        .recv_timeout(Duration::from_secs(3))
        .expect("quota reader exceeded the three-second drain bound");
    joining.stop_and_join();
    assert!(!state.quota_bus().0.lock().unwrap().attached);
    assert!(state.capacity_poll_ticket(now_ms()).is_some());
}

#[test]
fn quota_refused_consumer_identity_keeps_polling_and_does_not_fail_the_module() {
    let peer = Peer::start(Arc::new(|_| {
        Some((
            subc_protocol::FrameType::Error,
            serde_json::to_vec(
                &json!({"code":"bad_consumer_identity", "message":"fixture-handoff"}),
            )
            .unwrap(),
        ))
    }));
    let access = Access {
        path: peer.path.clone(),
        module_id: "astrocyte".into(),
        nonce: "fixture-handoff".into(),
    };
    let client = quota_bus_client::Client::new(access);
    let result = runtime().block_on(quota_bus_client::credential(&client, None));
    assert!(matches!(result, Err("ckbus identity refused")));
    let (state, capacity) = seeded();
    state.quota_bus().unavailable("ckbus identity refused");
    tick(&state, Some(&capacity));
    assert_eq!(capacity.0.load(Ordering::SeqCst), 2);
    assert!(state.crash_cause().is_none());
}

struct NatsServer {
    child: std::process::Child,
    _directory: tempfile::TempDir,
    url: String,
}

impl NatsServer {
    fn start() -> Option<Self> {
        let version = match std::process::Command::new("nats-server")
            .arg("--version")
            .output()
        {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("SKIP real quota bus fixture: nats-server absent");
                return None;
            }
            Err(error) => panic!("cannot start nats-server: {error}"),
        };
        assert!(version.status.success());
        eprintln!("{}", String::from_utf8_lossy(&version.stdout).trim());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let directory = tempfile::tempdir().unwrap();
        let stderr = std::fs::File::create(directory.path().join("server.log")).unwrap();
        let child = std::process::Command::new("nats-server")
            .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
            .arg(directory.path())
            .stdout(std::process::Stdio::null())
            .stderr(stderr)
            .spawn()
            .unwrap();
        Some(Self {
            child,
            _directory: directory,
            url: format!("nats://127.0.0.1:{port}"),
        })
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn quota_real_reader_credentials_attach_and_shutdown_mid_pull() {
    let Some(server) = NatsServer::start() else {
        return;
    };
    let names = AccountNames::derive("box_quota_reader").unwrap();
    runtime().block_on(async {
        let client = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(client) = async_nats::connect(&server.url).await {
                    break client;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let stream = jetstream::new(client)
            .create_stream(jetstream::stream::Config {
                name: names.streams().event.clone(),
                subjects: vec![names.event_subject("insula", "quota_sampled", 1).unwrap()],
                ..Default::default()
            })
            .await
            .unwrap();
        let entry = EVENT_CONSUMERS
            .iter()
            .find(|entry| entry.module_id == "astrocyte")
            .unwrap();
        stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(entry.durable.to_owned()),
                ..Default::default()
            })
            .await
            .unwrap();
    });
    let url = server.url.clone();
    let peer = Peer::start(Arc::new(move |request| {
        let value = match request["method"].as_str().unwrap() {
            "ckbus.credential" => {
                json!({"result": {"jwt":"fixture", "exp":chrono::Utc::now().timestamp() + 900,
                "acct":"box_quota_reader", "inbox_prefix":"_INBOX.quota_fixture", "server_url":url,
                "credential_public":"quota_fixture", "spawn_generation":1, "credential_epoch":1}})
            }
            "ckbus.nonce_sign" => {
                json!({"result":{"signature_b64":base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0_u8;64])}})
            }
            _ => panic!("unexpected management operation"),
        };
        Some((
            subc_protocol::FrameType::Response,
            serde_json::to_vec(&value).unwrap(),
        ))
    }));
    let (state, _) = seeded();
    let state = Arc::new(state);
    state.install_quota_bus_access(
        peer.path.clone(),
        "astrocyte".into(),
        "fixture-handoff".into(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let reader = Reader::start(state.clone(), peer.path.clone(), stop.clone()).unwrap();
    let stopping = stop.clone();
    let (ended, joined) = std::sync::mpsc::channel();
    let joining = Background::new(
        "quota reader mid-pull shutdown",
        std::thread::spawn(move || {
            reader.join();
            let _ = ended.send(());
        }),
        move || stopping.store(true, Ordering::Relaxed),
    );
    wait_until("authenticated quota reader pull", || {
        state.quota_bus().0.lock().unwrap().attached
    });
    assert!(state.capacity_poll_ticket(now_ms()).is_none());
    stop.store(true, Ordering::Relaxed);
    joined
        .recv_timeout(Duration::from_secs(3))
        .expect("quota pull exceeded the three-second drain bound");
    joining.stop_and_join();
    assert!(state.capacity_poll_ticket(now_ms()).is_some());
    assert!(peer
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request["method"] == "ckbus.credential"));
}

#[test]
fn quota_real_durable_acknowledges_after_commit_and_never_creates_topology() {
    let Some(server) = NatsServer::start() else {
        return;
    };
    let (state, _) = seeded();
    let before = rows(&state);
    let mut notice = notice(2, "run1");
    let response = body(&mut notice);
    let peer = Peer::start(Arc::new(move |_| {
        Some((subc_protocol::FrameType::Response, response.clone()))
    }));
    let insula = crate::qta_client::QtaClient::new(&peer.path).unwrap();
    runtime().block_on(async {
        let admin = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(client) = async_nats::connect(&server.url).await { break client; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("nats-server did not become ready");
        let names = AccountNames::derive("box_quota_test").unwrap();
        let subject = names.event_subject("insula", "quota_sampled", 1).unwrap();
        let js = jetstream::new(admin.clone());
        let stream = js.create_stream(jetstream::stream::Config {
            name: names.streams().event.clone(), subjects: vec![subject.clone()], ..Default::default()
        }).await.unwrap();
        let empty = js.create_stream(jetstream::stream::Config {
            name: "quota_empty".into(), subjects: vec!["quota.empty.>".into()], ..Default::default()
        }).await.unwrap();
        let entry = EVENT_CONSUMERS.iter().find(|entry| entry.module_id == "astrocyte").unwrap();
        let mut observer = stream.create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(entry.durable.to_string()), filter_subject: subject.clone(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit, ack_wait: Duration::from_secs(30),
            ..Default::default()
        }).await.unwrap();
        let mut api = admin.subscribe("$JS.API.>").await.unwrap();
        admin.flush().await.unwrap();
        let connection = NatsConnection::connect(&server.url, async_nats::ConnectOptions::new(),
            cortexkit_bus_nats::ConnectConfig::new("quota_fixture").unwrap()).await.unwrap();
        assert!(bind(&connection, &empty.cached_info().config.name).await.is_err());
        let consumer = bind(&connection, &names.streams().event).await.unwrap();
        let mut headers = async_nats::HeaderMap::new();
        for (key, value) in [("kind", "quota_sampled"), ("provider", "codex"), ("account", "a"),
            ("epoch", "run1"), ("seq", "2"), ("Nats-Msg-Id", notice.key.as_str()), ("Ck-Content-Digest", notice.digest.as_str())] {
            headers.insert(key, value);
        }
        js.publish_with_headers(subject.clone(), headers, Vec::new().into()).await.unwrap().await.unwrap();
        let credential = Credential { jwt: "fixture".into(), exp: chrono::Utc::now().timestamp() + 900,
            acct: names.account().into(), inbox_prefix: "unused".into(), server_url: server.url.clone(),
            credential_public: "quota_fixture".into(), spawn_generation: 1, credential_epoch: 1 };
        let mut cursors = cursors();
        let session = run_session(state.quota_bus(), &insula, &credential, &consumer, &subject, &mut cursors);
        tokio::pin!(session);
        let mut committed = false;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut session => panic!("reader ended before acknowledgement: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if !state.quota_bus().0.lock().unwrap().pending.is_empty() {
                            assert_eq!(observer.info().await.unwrap().num_ack_pending, 1, "acknowledged before commit");
                            assert_eq!(rows(&state), before);
                            tick(&state, None);
                            committed = true;
                        }
                        if committed && observer.info().await.unwrap().num_ack_pending == 0 { break; }
                    }
                }
            }
        }).await.expect("account delivery was not committed and acknowledged");
        assert!(committed);
        assert_eq!(rows(&state)[1], before[1]);
        assert_eq!(rows(&state)[0].1.to_bits(), 42_f64.to_bits());
        assert_eq!(sample_count(&state), 3);
        assert_eq!(peer.requests.lock().unwrap()[0]["method"], "events_get");
        assert_eq!(peer.requests.lock().unwrap()[0]["params"]["event_key"], notice.key);
        admin.flush().await.unwrap();
        let mut infos = 0;
        while let Ok(Some(message)) = tokio::time::timeout(Duration::from_millis(50), api.next()).await {
            let subject = message.subject.as_str();
            assert!(!subject.contains("CREATE") && !subject.contains("DELETE") && !subject.contains("UPDATE"), "reader attempted topology mutation: {subject}");
            infos += usize::from(subject.contains(".CONSUMER.INFO."));
        }
        assert!(infos >= 2, "test did not observe the durable bindings");
    });
}
