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
        assert_eq!(plans[0].plan_key, None);
        assert_eq!(plans[0].effective_from_ms, 123);
        stage.apply(
            &state,
            vec![Ok(plans)],
            Some(&Prices(vec![("pro_200", 20_000)])),
            124,
        );
        assert!(rows(&state).is_empty());
    }
    let plans = decode_listing(listing(&["openai"], Some("pro_200"), Some(7)), 123).unwrap();
    assert_eq!(plans[0].effective_from_ms, 7);
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
    let mut older = snapshot("pro_200", 100).unwrap().remove(0);
    older.credential_id = "old-login".into();
    let current = snapshot("plus", 200).unwrap().remove(0);
    let mut stage = stage();
    stage.apply(
        &state,
        vec![Ok(vec![older.clone(), current.clone()])],
        Some(&prices),
        201,
    );
    let expected = rows(&state);
    for plans in [vec![current.clone(), older.clone()], vec![older, current]] {
        stage.apply(&state, vec![Ok(plans)], Some(&prices), 202);
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
