use super::*;
use crate::{ops, refresher, state::test_state};
use astrocyte_core::catalog_client::CatalogCallError;
use fusiform_protocol::{PlanAmount, PlanPriceWire, PlanPricesResponse};

fn stage() -> Stage {
    let (ticks, _) = mpsc::unbounded_channel();
    Stage {
        reader: Reader {
            ticks,
            snapshots: Arc::default(),
            thread: None,
        },
        available: false,
        last_failure: None,
        last_reconcile_ms: None,
        last_mismatch_ms: None,
        diagnostics: vault_plan::Diagnostics::default(),
        price_available: false,
        invalid_plan_tier_since: Vec::new(),
        vault_tiers: BTreeMap::new(),
    }
}

fn listing(providers: &[&str], tier: Option<&str>, effective: Option<i64>) -> Value {
    json!({"result":{
        "credentials":[{"id":"oauth:login","categories":["llm-provider"],"type":"oauth",
            "serves":[],"provider_ids":providers,"state":"active","record_version":1,"operations":["list"],
            "account_id":"acct","plan_tier":tier,"plan_tier_source":"detected",
            "plan_tier_observed_at_ms":effective}],
        "grants":1,"grant_tuples":[{"selector_kind":"category","selector":"llm-provider","operation":"list"}],"view":"opaque"
    }})
}

fn snapshot(tier: &str, effective: i64) -> Snapshot {
    decode_listing(
        listing(&["openai"], Some(tier), Some(effective)),
        effective + 1,
    )
}

fn since_vector_listing() -> Value {
    let mut reply = listing(&["openai"], Some("pro_500"), None);
    let mut credentials: Vec<Value> = [
        include_str!("../tests/vectors/vault-plan-since/antigravity.json"),
        include_str!("../tests/vectors/vault-plan-since/chatgpt.json"),
    ]
    .into_iter()
    .map(|raw| serde_json::from_str(raw).unwrap())
    .collect();
    for (row, account) in credentials.iter_mut().zip(["google-acct", "acct"]) {
        row["account_id"] = json!(account);
        row["serves"] = json!([]);
        row["operations"] = json!(["list"]);
    }
    reply["result"]["credentials"] = json!(credentials);
    reply
}

#[test]
fn vault_since_vector_records_subscription_start_not_observation() {
    let state = test_state();
    let mut stage = stage();
    let plans = decode_listing(since_vector_listing(), 1_794_500_000_000).unwrap();
    stage.apply(&state, vec![Ok(plans)], None, 1_794_500_000_000);
    let recorded: Vec<(String, i64)> = state.with_conn(|conn| {
        conn.prepare("SELECT credential_id,effective_from_ms FROM vault_plan_history ORDER BY credential_id")
            .and_then(|mut stmt| stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect())
            .map_err(|error| error.to_string())
    }).unwrap();
    assert_eq!(
        recorded,
        vec![
            ("antigravity:google:ufuk".into(), 1_789_084_800_000),
            ("chatgpt:openai:gmail".into(), 1_794_096_000_000),
        ]
    );
}

fn history_starts(state: &SharedState) -> Vec<i64> {
    state
        .with_conn(|conn| {
            conn.prepare("SELECT effective_from_ms FROM vault_plan_history ORDER BY history_id")
                .and_then(|mut stmt| stmt.query_map([], |row| row.get(0))?.collect())
                .map_err(|error| error.to_string())
        })
        .unwrap()
}

#[test]
fn vault_since_date_only_changes_append_once_and_leave_same_key_ledger_untouched() {
    let state = test_state();
    ingest(&state, 100);
    let prices = Prices(vec![("pro_200", 20_000)]);
    let mut stage = stage();
    let mut reply = listing(&["openai"], Some("pro_200"), Some(200));
    stage.apply(
        &state,
        vec![decode_listing(reply.clone(), 300)],
        Some(&prices),
        300,
    );
    let ledger = rows(&state);
    assert_eq!(
        ledger,
        vec![(
            "openai/pro_200".into(),
            100,
            None,
            20_000,
            Some("vault".into())
        )]
    );
    for (since, now) in [(50, 301), (50, 302), (25, 303), (25, 304)] {
        reply["result"]["credentials"][0]["plan_tier_since_ms"] = json!(since);
        stage.apply(
            &state,
            vec![decode_listing(reply.clone(), now)],
            Some(&prices),
            now,
        );
        assert_eq!(
            rows(&state),
            ledger,
            "date-only changes never rewrite ledger plans"
        );
    }
    assert_eq!(history_starts(&state), vec![200, 50, 25]);
}

#[test]
fn vault_since_invalid_values_fall_back_and_are_reported_without_hiding_plans() {
    let state = test_state();
    let mut stage = stage();
    for (invalid, reason) in [
        (json!(201), "after_observed_at"),
        (json!(-1), "not_non_negative_integer"),
        (json!("50"), "not_non_negative_integer"),
        (json!(50.5), "not_non_negative_integer"),
        (Value::Null, "not_non_negative_integer"),
    ] {
        let mut reply = listing(&["openai"], Some("pro_200"), Some(200));
        reply["result"]["credentials"][0]["plan_tier_since_ms"] = invalid;
        stage.apply(&state, vec![decode_listing(reply, 300)], None, 300);
        assert_eq!(history_starts(&state), vec![200]);
        let value = report(&state);
        assert_eq!(value["vault_read_status"]["status"], "available");
        assert_eq!(
            value["invalid_plan_tier_since"],
            json!([
                {"credential_id":"oauth:login", "reason":reason}
            ])
        );
        assert_eq!(value["unpriced_plans"][0]["plan_key"], "openai/pro_200");
    }
    stage.apply(&state, vec![snapshot("pro_200", 200)], None, 301);
    assert!(
        report(&state).get("invalid_plan_tier_since").is_none(),
        "empty diagnostics are omitted"
    );
}

#[test]
fn vault_since_accepts_zero_equal_observed_and_missing_observed() {
    for (since, observed) in [(0, Some(200)), (200, Some(200)), (50, None)] {
        let mut reply = listing(&["openai"], Some("pro_200"), observed);
        reply["result"]["credentials"][0]["plan_tier_since_ms"] = json!(since);
        let listing = decode_listing(reply, 300).unwrap();
        assert_eq!(listing.plans[0].effective_from_ms, since);
        assert!(listing.invalid_plan_tier_since.is_empty());
    }
}

#[test]
fn vault_observed_timestamp_changes_do_not_append_without_since_date() {
    let state = test_state();
    let mut stage = stage();
    stage.apply(&state, vec![snapshot("pro_200", 100)], None, 101);
    stage.apply(&state, vec![snapshot("pro_200", 200)], None, 201);
    assert_eq!(history_starts(&state), vec![100]);
    let mut invalid = listing(&["openai"], Some("pro_200"), Some(300));
    invalid["result"]["credentials"][0]["plan_tier_since_ms"] = json!(-1);
    stage.apply(&state, vec![decode_listing(invalid, 301)], None, 301);
    assert_eq!(history_starts(&state), vec![100]);
    assert_eq!(
        report(&state)["invalid_plan_tier_since"][0]["reason"],
        "not_non_negative_integer"
    );
}

#[test]
fn vault_since_missing_timestamps_keep_first_observation_until_key_changes() {
    let state = test_state();
    let mut stage = stage();
    for (tier, now) in [
        ("pro_200", 100),
        ("pro_200", 200),
        ("plus", 300),
        ("plus", 400),
    ] {
        stage.apply(
            &state,
            vec![decode_listing(listing(&["openai"], Some(tier), None), now)],
            None,
            now,
        );
    }
    assert_eq!(history_starts(&state), vec![100, 300]);
    let mut invalid = listing(&["openai"], Some("plus"), None);
    invalid["result"]["credentials"][0]["plan_tier_since_ms"] = json!(-1);
    stage.apply(&state, vec![decode_listing(invalid, 500)], None, 500);
    assert_eq!(history_starts(&state), vec![100, 300]);
    assert_eq!(
        report(&state)["invalid_plan_tier_since"][0]["reason"],
        "not_non_negative_integer"
    );
}

struct Prices(Vec<(&'static str, i64)>);
impl PlanPriceSource for Prices {
    fn plan_prices(&self, _: PlanPricesRequest) -> Result<PlanPricesResponse, CatalogCallError> {
        Ok(PlanPricesResponse {
            prices: self
                .0
                .iter()
                .map(|(tier, minor)| PlanPriceWire {
                    provider_id: "openai".into(),
                    tier: (*tier).into(),
                    price: Some(PlanAmount {
                        minor_units: *minor,
                        exponent: 2,
                        currency: "USD".into(),
                        period: "month".into(),
                    }),
                    quota_value: None,
                    service_tiers: None,
                    refusal_reason: None,
                    boundary_at_ms: 0,
                    established_by: "fusiform".into(),
                    established_at_ms: 0,
                    review_by_ms: i64::MAX,
                    source_ref: "sourced".into(),
                })
                .collect(),
            resolved_at_ms: 0,
            tier_vocabulary: "vendor".into(),
            unit_policy: "list".into(),
        })
    }
}

fn ingest(state: &SharedState, at: i64) -> String {
    ops::dispatch(state,ops::OP_EVENT_INGEST,json!({
        "producer":"broca","producer_lineage":"lineage","producer_ref":format!("event-{at}"),
        "provider":"openai","account_ref":"acct","auth_selection":"oauth:login","occurred_at_ms":at,
        "charge_basis":"subscription_included","measurements":[{"role":"input","unit":"token","kind":"raw",
            "source":"usage","quantity_units":10,"quantity_exponent":0}]
    })).unwrap()["event_id"].as_str().unwrap().into()
}

type LedgerRow = (String, i64, Option<i64>, i64, Option<String>);

fn rows(state: &SharedState) -> Vec<LedgerRow> {
    state.with_conn(|conn| conn.prepare("SELECT plan_name,effective_from_ms,effective_to_ms,recurring_cost_minor,operator FROM account_subscription ORDER BY effective_from_ms")
        .and_then(|mut stmt| stmt.query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?.collect()).map_err(|e|e.to_string())).unwrap()
}

fn report(state: &SharedState) -> Value {
    ops::dispatch(state, ops::OP_SUBSCRIPTION_REPORT, json!({})).unwrap()
}

struct CountingPrices {
    calls: std::sync::atomic::AtomicUsize,
    priced: AtomicBool,
}

impl PlanPriceSource for CountingPrices {
    fn plan_prices(
        &self,
        request: PlanPricesRequest,
    ) -> Result<PlanPricesResponse, CatalogCallError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Prices(if self.priced.load(Ordering::Relaxed) {
            vec![("pro_200", 20_000)]
        } else {
            vec![]
        })
        .plan_prices(request)
    }
}

#[test]
fn vault_idle_full_ticks_do_not_call_prices_or_open_store_transactions() {
    let state = test_state();
    ingest(&state, 10);
    let source = CountingPrices {
        calls: 0.into(),
        priced: AtomicBool::new(true),
    };
    let mut stage = stage();
    stage.apply(&state, vec![snapshot("pro_200", 100)], Some(&source), 101);
    assert_eq!(source.calls.load(Ordering::Relaxed), 1);
    let before = crate::state::transaction_attempts_for_test();
    let expected = report(&state);
    stage.tick(&state, Some(&source), 30_101);
    assert_eq!(
        crate::state::transaction_attempts_for_test(),
        before,
        "an idle tick must not start a writer transaction"
    );
    assert_eq!(source.calls.load(Ordering::Relaxed), 1);
    assert_eq!(report(&state), expected);

    stage.apply(
        &state,
        vec![snapshot("pro_200", 100)],
        Some(&source),
        60_101,
    );
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        1,
        "unchanged snapshots do not reconcile"
    );
    assert_eq!(crate::state::transaction_attempts_for_test(), before);

    stage.tick(&state, Some(&source), 101 + 86_400_000);
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        2,
        "priced accounts have a daily backstop"
    );
    assert_eq!(
        crate::state::transaction_attempts_for_test(),
        before,
        "diagnostics alone do not start a writer transaction"
    );
    let mut restarted = self::stage();
    restarted.apply(
        &state,
        vec![snapshot("pro_200", 100)],
        Some(&source),
        101 + 86_400_001,
    );
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        3,
        "startup reconciles even when persisted history is unchanged"
    );
    assert_eq!(crate::state::transaction_attempts_for_test(), before);
}

#[test]
fn vault_unpriced_retry_waits_an_hour_then_applies_new_price() {
    let state = test_state();
    ingest(&state, 10);
    let source = CountingPrices {
        calls: 0.into(),
        priced: AtomicBool::new(false),
    };
    let mut stage = stage();
    stage.apply(&state, vec![snapshot("pro_200", 100)], Some(&source), 101);
    assert!(rows(&state).is_empty());
    assert_eq!(
        report(&state)["unpriced_plans"][0]["plan_key"],
        "openai/pro_200"
    );
    let before = crate::state::transaction_attempts_for_test();
    source.priced.store(true, Ordering::Relaxed);
    for now in [30_101, 101 + 3_600_000 - 1] {
        stage.tick(&state, Some(&source), now);
        assert_eq!(
            source.calls.load(Ordering::Relaxed),
            1,
            "unpriced plans retry at most hourly"
        );
        assert!(rows(&state).is_empty());
        assert_eq!(crate::state::transaction_attempts_for_test(), before);
    }
    stage.tick(&state, Some(&source), 101 + 3_600_000);
    assert_eq!(source.calls.load(Ordering::Relaxed), 2);
    assert_eq!(rows(&state)[0].0, "openai/pro_200");
    assert_eq!(rows(&state)[0].1, 10);
    assert_eq!(report(&state)["unpriced_plans"], json!([]));
    stage.tick(&state, Some(&source), 101 + 2 * 3_600_000);
    assert_eq!(
        source.calls.load(Ordering::Relaxed),
        2,
        "a priced key returns to the daily backstop"
    );
}

#[test]
fn vault_priced_plan_starts_at_first_call_and_waiting_calls_become_covered() {
    let state = test_state();
    let id = ingest(&state, 10);
    refresher::run_pricing_pass(&state, 500, &AtomicBool::new(false)).unwrap();
    let mut stage = stage();
    stage.apply(
        &state,
        vec![snapshot("pro_200", 100)],
        Some(&Prices(vec![("pro_200", 20_000)])),
        101,
    );
    assert_eq!(
        rows(&state),
        vec![(
            "openai/pro_200".into(),
            10,
            None,
            20_000,
            Some("vault".into())
        )]
    );
    refresher::run_pricing_pass(&state, 500, &AtomicBool::new(false)).unwrap();
    let (outcome,money): (String,Option<i64>) = state.with_conn(|conn| conn.query_row(
        "SELECT outcome,charge_nanos FROM priced_charge h WHERE event_key=(SELECT rowid FROM usage_event WHERE event_id=?1) AND NOT EXISTS(SELECT 1 FROM priced_charge n WHERE n.supersedes=h.charge_key)",
        [&id],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|e|e.to_string())).unwrap();
    assert_eq!((outcome, money), ("covered".into(), Some(0)));
}

#[test]
fn vault_tier_change_closes_and_opens_at_effective_time() {
    let state = test_state();
    ingest(&state, 10);
    let prices = Prices(vec![("pro_200", 20_000), ("plus", 2_000)]);
    let mut stage = stage();
    stage.apply(&state, vec![snapshot("pro_200", 100)], Some(&prices), 101);
    stage.apply(&state, vec![snapshot("plus", 200)], Some(&prices), 201);
    assert_eq!(
        rows(&state),
        vec![
            (
                "openai/pro_200".into(),
                10,
                Some(200),
                20_000,
                Some("vault".into())
            ),
            ("openai/plus".into(), 200, None, 2_000, Some("vault".into())),
        ]
    );
}

#[test]
fn vault_hand_plan_is_untouched_and_difference_is_reported() {
    let state = test_state();
    ingest(&state, 10);
    ops::dispatch(&state,ops::OP_SUBSCRIPTION_SET,json!({"provider":"openai","account_ref":"acct","plan_name":"hand",
        "recurring_cost_minor":123,"currency":"USD","amount_exponent":2,"period":"month","effective_from":5})).unwrap();
    let before = rows(&state);
    stage().apply(
        &state,
        vec![snapshot("pro_200", 100)],
        Some(&Prices(vec![("pro_200", 20_000)])),
        101,
    );
    assert_eq!(rows(&state), before);
    assert_eq!(
        report(&state)["hand_vault_differences"],
        json!([{"provider":"openai","account_id":"acct","ledger_plan":"hand","vault_plan":"openai/pro_200"}])
    );
}

#[test]
fn vault_unpriced_raw_tier_is_reported_and_later_price_records_plan() {
    let state = test_state();
    ingest(&state, 10);
    let mut stage = stage();
    stage.apply(
        &state,
        vec![snapshot("Odd-Tier", 100)],
        Some(&Prices(vec![])),
        101,
    );
    assert!(rows(&state).is_empty());
    assert_eq!(
        report(&state)["unpriced_plans"],
        json!([{"plan_key":"openai/Odd-Tier","provider":"openai","plan_tier":"Odd-Tier","accounts":["acct"]}])
    );
    stage.apply(
        &state,
        vec![],
        Some(&Prices(vec![("Odd-Tier", 25_000)])),
        101 + UNPRICED_RETRY_MS,
    );
    assert_eq!(rows(&state)[0].0, "openai/Odd-Tier");
    assert_eq!(rows(&state)[0].1, 10);
    assert_eq!(report(&state)["unpriced_plans"], json!([]));
}

#[test]
fn vault_multiple_providers_or_absent_tier_are_unknown_and_optional_time_falls_back() {
    let state = test_state();
    ingest(&state, 10);
    let mut stage = stage();
    for (providers, tier) in [
        (vec!["openai", "anthropic"], Some("pro_200")),
        (vec!["openai"], None),
    ] {
        let plans = decode_listing(listing(&providers, tier, None), 123).unwrap();
        assert_eq!(plans.plans[0].plan_key, None);
        assert_eq!(plans.plans[0].effective_from_ms, 123);
        stage.apply(
            &state,
            vec![Ok(plans)],
            Some(&Prices(vec![("pro_200", 20_000)])),
            124,
        );
        assert!(rows(&state).is_empty());
    }
    let plans = decode_listing(listing(&["openai"], Some("pro_200"), Some(7)), 123).unwrap();
    assert_eq!(plans.plans[0].effective_from_ms, 7);
}

#[test]
fn vault_absence_and_grant_refusal_preserve_recorded_data_and_report_status() {
    let state = test_state();
    ingest(&state, 10);
    let mut stage = stage();
    for code in ["unknown_module", "unauthorized"] {
        stage.apply(
            &state,
            vec![Err(code.into())],
            Some(&Prices(vec![("pro_200", 20_000)])),
            101,
        );
        assert!(rows(&state).is_empty());
        assert_eq!(
            report(&state)["vault_read_status"],
            json!({"status":"unavailable","reason":code})
        );
        assert_eq!(
            state
                .with_conn(|conn| conn
                    .query_row("SELECT count(*) FROM vault_plan_history", [], |r| r
                        .get::<_, i64>(0))
                    .map_err(|e| e.to_string()))
                .unwrap(),
            0
        );
    }
    let mut missing_grant = listing(&["openai"], Some("pro_200"), None);
    missing_grant["result"]["grant_tuples"] = json!([]);
    assert_eq!(
        decode_listing(missing_grant, 123).unwrap_err(),
        "list_grant_missing"
    );
}

struct Peer {
    _directory: tempfile::TempDir,
    access: Access,
    value: Arc<Mutex<Value>>,
    requests: Arc<Mutex<Vec<Value>>>,
    watchers: Arc<Mutex<Vec<mpsc::UnboundedSender<Value>>>>,
    _server: crate::test_support::Background<()>,
}

impl Peer {
    fn start(unsupported: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let path = directory.path().join("connection.json");
        let info = connection_file::ConnectionInfo {
            schema: connection_file::SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![connection_file::Endpoint {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
            }],
            key: vec![17; subc_transport::KEY_LEN],
            daemon_id: [9; 16],
            pid: std::process::id(),
            daemon_ver: "fixture".into(),
        };
        connection_file::write_atomic(&path, &info).unwrap();
        let value = Arc::new(Mutex::new(listing(&["openai"], Some("pro_200"), Some(100))));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let watchers = Arc::new(Mutex::new(Vec::<mpsc::UnboundedSender<Value>>::new()));
        let (v, r, w) = (value.clone(), requests.clone(), watchers.clone());
        let (release, mut released) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                loop { tokio::select! {
                    _ = &mut released => break,
                    accepted = listener.accept() => {
                        let (mut stream,_) = accepted.unwrap();
                        let (v,r,w,info) = (v.clone(),r.clone(),w.clone(),info.clone());
                        tokio::spawn(async move {
                            if subc_transport::auth::authenticate_server(&mut stream,&info.key,&info.daemon_id,&info.daemon_ver,BUDGET).await.is_err() { return; }
                            while let Ok(Some(frame)) = read_frame(&mut stream).await {
                                let request: Value = serde_json::from_slice(&frame.body).unwrap();
                                r.lock().unwrap().push(request.clone());
                                let (kind,answer) = if request["op"] == "route.open" {
                                    assert_eq!(request["consumer_identity"],json!({"module_id":"astrocyte","launch_nonce":"attested-fixture"}));
                                    (FrameType::Response,json!({"route_channel":7,"route_epoch":1}))
                                } else if request["method"] == "credential.list_scoped" {
                                    assert_eq!(request["params"],json!({}));
                                    (FrameType::Response,v.lock().unwrap().clone())
                                } else {
                                    assert_eq!(request["method"],"auth.watch_scoped");
                                    if unsupported { (FrameType::Error,json!({"code":"unknown_method","message":"old vault"})) }
                                    else {
                                        let (sender,mut notices) = mpsc::unbounded_channel();
                                        w.lock().unwrap().push(sender);
                                        let mut seq = 1;
                                        let mut next = json!({"kind":"changed","epoch":"fixture-epoch","seq":seq,"view":"view-1","keys":"keys-1"});
                                        loop {
                                            let notice = Frame::build(FrameType::StreamData,Flags::new(false,Priority::Passive,false),7,1,frame.header.corr,serde_json::to_vec(&next).unwrap()).unwrap();
                                            if write_frame(&mut stream,&notice).await.is_err() { return; }
                                            let Some(_) = notices.recv().await else { return; };
                                            seq += 1;
                                            next = json!({"kind":"changed","epoch":"fixture-epoch","seq":seq,"view":format!("view-{seq}"),"keys":format!("keys-{seq}")});
                                        }
                                    }
                                };
                                let response = Frame::build(kind,frame.header.flags,frame.header.channel,frame.header.epoch,frame.header.corr,serde_json::to_vec(&answer).unwrap()).unwrap();
                                if write_frame(&mut stream,&response).await.is_err() { return; }
                            }
                        });
                    }
                } }
            });
        });
        Self {
            _directory: directory,
            access: Access {
                path,
                module_id: "astrocyte".into(),
                nonce: "attested-fixture".into(),
            },
            value,
            requests,
            watchers,
            _server: crate::test_support::Background::new("vault fixture", thread, move || {
                let _ = release.send(());
            }),
        }
    }

    fn count(&self, method: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == method)
            .count()
    }
}

#[test]
fn vault_watch_notice_is_applied_on_next_full_tick_without_restart() {
    let peer = Peer::start(false);
    let state = test_state();
    ingest(&state, 10);
    state.install_quota_bus_access(
        peer.access.path.clone(),
        peer.access.module_id.clone(),
        peer.access.nonce.clone(),
    );
    state.acknowledge_module_hello();
    let mut stage = Stage::new(Arc::new(AtomicBool::new(false)));
    let prices = Prices(vec![("pro_200", 20_000), ("plus", 2_000)]);
    stage.tick(&state, Some(&prices), 101);
    crate::test_support::wait_until("initial vault list", || {
        !stage.reader.snapshots.lock().unwrap().is_empty()
    });
    stage.tick(&state, Some(&prices), 101);
    assert_eq!(rows(&state)[0].0, "openai/pro_200");
    crate::test_support::wait_until("vault watch", || !peer.watchers.lock().unwrap().is_empty());
    *peer.value.lock().unwrap() = listing(&["openai"], Some("plus"), Some(200));
    for watcher in peer.watchers.lock().unwrap().iter() {
        watcher.send(Value::Null).unwrap();
    }
    crate::test_support::wait_until("changed vault list", || {
        stage
            .reader
            .snapshots
            .lock()
            .unwrap()
            .iter()
            .any(|snapshot| {
                snapshot.as_ref().is_ok_and(|plans| {
                    plans
                        .plans
                        .iter()
                        .any(|plan| plan.plan_key.as_deref() == Some("openai/plus"))
                })
            })
    });
    assert_eq!(rows(&state).len(), 1, "reader never records ledger rows");
    stage.tick(&state, Some(&prices), 201);
    assert_eq!(rows(&state)[0].2, Some(200));
    assert_eq!(rows(&state)[1].0, "openai/plus");
    assert_eq!(peer.count("credential.get_scoped"), 0);
    stage.join();
}

#[test]
fn vault_unknown_watch_method_polls_only_on_full_ticks() {
    let peer = Peer::start(true);
    let state = test_state();
    state.install_quota_bus_access(
        peer.access.path.clone(),
        peer.access.module_id.clone(),
        peer.access.nonce.clone(),
    );
    state.acknowledge_module_hello();
    let reader = Reader::start(Arc::new(AtomicBool::new(false)));
    reader.tick(&state);
    crate::test_support::wait_until("watch refusal", || peer.count("auth.watch_scoped") == 1);
    crate::test_support::wait_until("initial list completed", || {
        !reader.snapshots.lock().unwrap().is_empty()
    });
    let count = peer.count("credential.list_scoped");
    assert_eq!(count, 1);
    reader.tick(&state);
    crate::test_support::wait_until("full-tick poll", || {
        peer.count("credential.list_scoped") > count
    });
    assert_eq!(peer.count("auth.watch_scoped"), 1);
    assert_eq!(peer.count("credential.get_scoped"), 0);
    reader.join();
}

#[test]
fn vault_shared_account_uses_latest_record_without_tick_order_churn() {
    let state = test_state();
    ingest(&state, 10);
    let prices = Prices(vec![("pro_200", 20_000), ("plus", 2_000)]);
    let mut older = snapshot("pro_200", 100).unwrap().plans.remove(0);
    older.credential_id = "old-login".into();
    let current = snapshot("plus", 200).unwrap().plans.remove(0);
    let mut stage = stage();
    stage.apply(
        &state,
        vec![Ok(Listing {
            plans: vec![older.clone(), current.clone()],
            invalid_plan_tier_since: Vec::new(),
            start_date_unspecified: Vec::new(),
        })],
        Some(&prices),
        201,
    );
    let expected = rows(&state);
    for plans in [vec![current.clone(), older.clone()], vec![older, current]] {
        stage.apply(
            &state,
            vec![Ok(Listing {
                plans,
                invalid_plan_tier_since: Vec::new(),
                start_date_unspecified: Vec::new(),
            })],
            Some(&prices),
            202,
        );
        assert_eq!(rows(&state), expected);
    }
    assert_eq!(expected[0].0, "openai/plus");
    assert_eq!(
        report(&state)["vault_record_differences"][0]["winning_plan"],
        "openai/plus"
    );
    assert_eq!(
        state
            .with_conn(|conn| conn
                .query_row("SELECT count(*) FROM vault_plan_history", [], |row| row
                    .get::<_, i64>(0))
                .map_err(|error| error.to_string()))
            .unwrap(),
        2
    );
}

#[test]
fn vault_chatgpt_mismatch_report_joins_the_exact_credential() {
    let state = test_state();
    ingest(&state, 10);
    for (id, credential, account) in [
        ("wrong", "oauth:login", "other-account"),
        ("unrelated", "other-login", "unrelated-account"),
    ] {
        ops::dispatch(&state,ops::OP_EVENT_INGEST,json!({"producer":"broca","producer_lineage":"lineage","producer_ref":id,
            "provider":"openai","account_ref":account,"auth_selection":credential,"occurred_at_ms":20,
            "charge_basis":"subscription_included","measurements":[]})).unwrap();
    }
    stage().apply(
        &state,
        vec![snapshot("pro_200", 100)],
        Some(&Prices(vec![("pro_200", 20_000)])),
        101,
    );
    assert_eq!(
        report(&state)["chatgpt_account_mismatches"],
        json!([{"credential_id":"oauth:login","account_id":"acct","account_refs":["other-account"]}])
    );
}

#[test]
fn vault_missing_catalog_keeps_raw_unpriced_key_and_available_vault_status() {
    let state = test_state();
    ingest(&state, 10);
    stage().apply(&state, vec![snapshot("pro_200", 100)], None, 101);
    assert!(rows(&state).is_empty());
    assert_eq!(
        report(&state)["unpriced_plans"][0]["plan_key"],
        "openai/pro_200"
    );
    assert_eq!(report(&state)["vault_read_status"]["status"], "available");
    assert_eq!(report(&state)["plan_price_status"], "unavailable");
}

#[test]
fn vault_annual_non_usd_and_zero_sourced_prices_use_existing_ledger_rules() {
    struct SourcedAmount {
        minor: i64,
        exponent: i32,
        currency: &'static str,
        period: &'static str,
    }
    impl PlanPriceSource for SourcedAmount {
        fn plan_prices(
            &self,
            request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            let mut response = Prices(vec![("pro_200", self.minor)]).plan_prices(request)?;
            response.prices[0].price = Some(PlanAmount {
                minor_units: self.minor,
                exponent: self.exponent,
                currency: self.currency.into(),
                period: self.period.into(),
            });
            Ok(response)
        }
    }
    for (amount, minor, exponent) in [
        (
            SourcedAmount {
                minor: 12_345,
                exponent: 2,
                currency: "EUR",
                period: "year",
            },
            12_345,
            2,
        ),
        (
            SourcedAmount {
                minor: 0,
                exponent: 2,
                currency: "USD",
                period: "month",
            },
            0,
            0,
        ),
        (
            SourcedAmount {
                minor: 2,
                exponent: -2,
                currency: "JPY",
                period: "month",
            },
            200,
            0,
        ),
        (
            SourcedAmount {
                minor: 12_300,
                exponent: 11,
                currency: "USD",
                period: "month",
            },
            123,
            9,
        ),
    ] {
        let state = test_state();
        ingest(&state, 10);
        stage().apply(&state, vec![snapshot("pro_200", 100)], Some(&amount), 101);
        let stored: (i64,u32,String,String) = state.with_conn(|conn|conn.query_row(
            "SELECT recurring_cost_minor,amount_exponent,currency,period FROM account_subscription",[],
            |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).map_err(|error|error.to_string())).unwrap();
        assert_eq!(
            stored,
            (
                minor,
                exponent,
                amount.currency.into(),
                amount.period.into()
            )
        );
        assert_eq!(report(&state)["unpriced_plans"], json!([]));
    }
}

/// A listing of key-only credentials (no account id) on `openai`, each with an
/// operator-set tier observed at `observed` and no start date.
fn key_only_listing(credentials: &[(&str, &str)], observed: i64) -> Value {
    let rows: Vec<Value> = credentials
        .iter()
        .map(|(id, tier)| {
            json!({"id":id,"categories":["llm-provider"],"type":"oauth","serves":[],
                "provider_ids":["openai"],"state":"active","record_version":1,"operations":["list"],
                "account_id":null,"plan_tier":tier,"plan_tier_source":"operator",
                "plan_tier_observed_at_ms":observed})
        })
        .collect();
    json!({"result":{"credentials":rows,"grants":1,
        "grant_tuples":[{"selector_kind":"category","selector":"llm-provider","operation":"list"}],
        "view":"opaque"}})
}

/// Records an account-less subscription call with the stated credential facts,
/// the way the BROCA store reader does: `resolved_credential` is not part of the
/// pushed ingest operation's schema.
fn ingest_key(
    state: &SharedState,
    reference: &str,
    at: i64,
    auth_selection: Option<&str>,
    resolved_credential: Option<&str>,
) -> String {
    let mut request: astrocyte_core::ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"broca","producer_lineage":"lineage","producer_ref":reference,
        "provider":"openai","auth_selection":auth_selection,
        "occurred_at_ms":at,"charge_basis":"subscription_included","measurements":[{"role":"input",
            "unit":"token","kind":"raw","source":"usage","quantity_units":10,"quantity_exponent":0}]
    }))
    .unwrap();
    request.resolved_credential = resolved_credential.map(str::to_string);
    state
        .ordered_tail_write(|tx| {
            let outcome = astrocyte_core::ingest::record_event_in_tx(
                tx,
                &request,
                at,
                astrocyte_core::ingest::DeliveryPath::Pushed,
            )
            .map_err(|error| format!("{error:?}"))?;
            Ok((outcome.event_id, true))
        })
        .unwrap()
}

type CredentialRow = (String, String, i64, Option<i64>, i64, Option<String>);

fn credential_plans(state: &SharedState) -> Vec<CredentialRow> {
    state.with_conn(|conn| conn.prepare("SELECT credential_ref,plan_name,effective_from_ms,effective_to_ms,recurring_cost_minor,operator FROM credential_subscription ORDER BY credential_ref,effective_from_ms")
        .and_then(|mut stmt| stmt.query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)))?.collect()).map_err(|e|e.to_string())).unwrap()
}

/// The outcome and unpriced reason of the event's current charge head.
fn head(state: &SharedState, event_id: &str) -> (String, Option<String>) {
    state.with_conn(|conn| conn.query_row(
        "SELECT outcome,unpriced_reason FROM priced_charge h WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) AND NOT EXISTS(SELECT 1 FROM priced_charge n WHERE n.supersedes=h.charge_key)",
        [event_id],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|e|e.to_string())).unwrap()
}

type ChargeRow = (i64, String, String, Option<String>, Option<i64>, Option<i64>);

/// Every charge row, in insertion order: key, event, outcome, reason, money, superseded key.
fn charge_rows(state: &SharedState) -> Vec<ChargeRow> {
    state.with_conn(|conn| conn.prepare("SELECT p.charge_key,e.event_id,p.outcome,p.unpriced_reason,p.charge_nanos,p.supersedes FROM priced_charge p JOIN usage_event e ON e.event_key=p.event_key ORDER BY p.charge_key")
        .and_then(|mut stmt| stmt.query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)))?.collect()).map_err(|e|e.to_string())).unwrap()
}

#[test]
fn vault_key_only_plan_covers_past_calls_by_paying_credential() {
    let state = test_state();
    let direct = ingest_key(&state, "direct", 10, Some("apikey:ollama"), None);
    let resolved = ingest_key(
        &state,
        "resolved",
        20,
        Some("apikey:routing"),
        Some("apikey:ollama"),
    );
    let differs = ingest_key(
        &state,
        "differs",
        30,
        Some("apikey:ollama"),
        Some("apikey:other"),
    );
    let anonymous = ingest_key(&state, "anonymous", 40, None, None);
    refresher::run_pricing_pass(&state, 500, &AtomicBool::new(false)).unwrap();
    let uncovered = (
        "unpriced".to_string(),
        Some("no_subscription_in_force".to_string()),
    );
    for id in [&direct, &resolved, &differs, &anonymous] {
        assert_eq!(head(&state, id), uncovered, "{id} before the plan");
    }
    let before = charge_rows(&state);

    let mut stage = stage();
    stage.apply(
        &state,
        vec![decode_listing(
            key_only_listing(&[("apikey:ollama", "go")], 100),
            101,
        )],
        Some(&Prices(vec![("go", 1_000)])),
        101,
    );
    refresher::run_pricing_pass(&state, 500, &AtomicBool::new(false)).unwrap();

    let covered = ("covered".to_string(), None);
    assert_eq!(head(&state, &direct), covered, "auth_selection names the plan");
    assert_eq!(
        head(&state, &resolved),
        covered,
        "resolved_credential names the plan"
    );
    assert_eq!(
        head(&state, &differs),
        uncovered,
        "resolved_credential wins over auth_selection"
    );
    assert_eq!(head(&state, &anonymous), uncovered, "no credential, no plan");
    // The plan starts at the credential's first call and stays out of the
    // account ledger.
    assert_eq!(
        credential_plans(&state),
        vec![(
            "apikey:ollama".into(),
            "openai/go".into(),
            10,
            None,
            1_000,
            Some("vault".into())
        )]
    );
    assert!(rows(&state).is_empty());
    // The backfill appended one new head per covered call; every earlier row
    // is unchanged, and the new heads charge nothing.
    let after = charge_rows(&state);
    assert_eq!(&after[..before.len()], &before[..]);
    let appended = &after[before.len()..];
    assert_eq!(appended.len(), 2, "{appended:?}");
    for row in appended {
        assert!(row.1 == direct || row.1 == resolved, "{row:?}");
        assert_eq!(row.2, "covered");
        assert_eq!(row.4, Some(0));
        let superseded = before.iter().find(|old| Some(old.0) == row.5).unwrap();
        assert_eq!(superseded.1, row.1);
    }
    let money = |rows: &[ChargeRow]| rows.iter().filter_map(|row| row.4).sum::<i64>();
    assert_eq!(money(&after), money(&before));

    let report = report(&state);
    assert_eq!(report["rows"], json!([]));
    assert_eq!(report["credential_rows"].as_array().unwrap().len(), 1);
    let row = &report["credential_rows"][0];
    assert_eq!(row["provider"], "openai");
    assert_eq!(row["credential_ref"], "apikey:ollama");
    assert_eq!(row["covered_events"], 2);
    assert_eq!(report["coverage"]["events_covered_by_credential"], 2);
    assert_eq!(report["coverage"]["events_unpriced"], 2);
    assert_eq!(report["coverage"]["events_without_account"], 4);
}

#[test]
fn vault_key_only_tier_change_closes_and_opens_at_effective_time() {
    let state = test_state();
    ingest_key(&state, "call", 10, Some("apikey:ollama"), None);
    let prices = Prices(vec![("go", 1_000), ("pro", 3_000)]);
    let mut stage = stage();
    for (tier, observed) in [("go", 100), ("pro", 200)] {
        stage.apply(
            &state,
            vec![decode_listing(
                key_only_listing(&[("apikey:ollama", tier)], observed),
                observed + 1,
            )],
            Some(&prices),
            observed + 1,
        );
    }
    assert_eq!(
        credential_plans(&state),
        vec![
            (
                "apikey:ollama".into(),
                "openai/go".into(),
                10,
                Some(200),
                1_000,
                Some("vault".into())
            ),
            (
                "apikey:ollama".into(),
                "openai/pro".into(),
                200,
                None,
                3_000,
                Some("vault".into())
            ),
        ]
    );
}

#[test]
fn vault_config_seed_counts_only_when_the_vault_states_the_same_tier() {
    let state = test_state();
    state
        .with_conn(|conn| {
            let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
            for credential in ["apikey:kimi", "apikey:opencode"] {
                vault_plan::append_in_tx(
                    &tx,
                    &VaultPlan {
                        credential_id: credential.into(),
                        provider: Some("openai".into()),
                        account_id: None,
                        plan_tier: Some("go".into()),
                        plan_key: Some("openai/go".into()),
                        source: "config".into(),
                        effective_from_ms: 0,
                        observed_at_ms: 1,
                    },
                )
                .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())
        })
        .unwrap();
    let kimi = ingest_key(
        &state,
        "kimi",
        10,
        Some("apikey:kimi-code-plan-global"),
        Some("apikey:kimi"),
    );
    let opencode = ingest_key(&state, "opencode", 15, Some("apikey:opencode"), None);
    refresher::run_pricing_pass(&state, 500, &AtomicBool::new(false)).unwrap();

    // The vault states kimi's seeded tier with no start date; it does not
    // list opencode, so nothing confirms that seed.
    let mut stage = stage();
    stage.apply(
        &state,
        vec![decode_listing(
            key_only_listing(&[("apikey:kimi", "go")], 100),
            101,
        )],
        Some(&Prices(vec![("go", 1_000)])),
        101,
    );
    let history: Vec<String> = state
        .with_conn(|conn| {
            conn.prepare("SELECT source FROM vault_plan_history ORDER BY history_id")
                .and_then(|mut stmt| stmt.query_map([], |row| row.get(0))?.collect())
                .map_err(|e| e.to_string())
        })
        .unwrap();
    assert_eq!(
        history,
        vec!["config", "config"],
        "an unchanged tier with no start date appends no history row"
    );
    assert_eq!(
        credential_plans(&state),
        vec![(
            "apikey:kimi".into(),
            "openai/go".into(),
            10,
            None,
            1_000,
            Some("vault".into())
        )]
    );
    refresher::run_pricing_pass(&state, 500, &AtomicBool::new(false)).unwrap();
    assert_eq!(head(&state, &kimi), ("covered".into(), None));
    assert_eq!(
        head(&state, &opencode),
        (
            "unpriced".into(),
            Some("no_subscription_in_force".into())
        )
    );
}
