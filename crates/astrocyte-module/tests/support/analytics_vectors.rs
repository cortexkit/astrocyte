use crate::{
    ops,
    state::{test_state, SharedState},
};
use astrocyte_core::{
    multiplier::MultiplierConfig,
    rate_era::{fetched, publish},
    report::{valuation::ValuationMaintenance, SummaryMaintenance},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-vectors/analytics-reads-v3"
);

fn fixture() -> SharedState {
    let state = test_state();
    state.with_conn(|conn| {
        for (id,producer,model,class,time) in [("first","broca","alpha",None,1),("second","opencode","beta",Some("review"),3_600_001),("third","broca","alpha",None,86_400_001)] {
            conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,model,work_class,occurred_at_ms,recorded_at_ms,charge_basis,runner) VALUES(?1,?2,?1,?3,?4,?5,1,'metered_api',?6)",rusqlite::params![id,producer,model,class,time,if producer == "broca" { "broca" } else { "thalamus" }]).unwrap();
            for (role,unit,quantity) in [("input","token",Some(100)),("output","token",Some(30)),("reasoning","token",Some(12)),("cache_write","token_cache_5m",Some(3)),("cache_write","token_cache_1h",Some(5)),("duration","second",Some(2))] {
                conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,?3,'billed','synthetic',?4,0)",rusqlite::params![id,role,unit,quantity]).unwrap();
            }
        }
        conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,charge_basis) VALUES('unknown','pi','unknown',1,1,'metered_api')",[]).unwrap();
        conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source) VALUES((SELECT event_key FROM usage_event WHERE event_id='unknown'),'input','token','billed','synthetic')",[]).unwrap();
        Ok(())
    }).unwrap();
    state
}

fn plan_fixture(pending: bool) -> SharedState {
    let state = test_state();
    state.with_conn(|conn| {
        conn.execute("INSERT INTO account_subscription(account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms,effective_to_ms) VALUES('account','provider','plan',2000,'USD',2,'month',0,86400000)", []).unwrap();
        for (id, model, time, rate_units) in [
            ("included-alpha", "alpha", 1, 1_000_000_000_i64),
            ("included-beta", "beta", 3_600_001, 2_000_000_000),
        ] {
            conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,provider,model,account_ref,occurred_at_ms,recorded_at_ms,charge_basis) VALUES(?1,'opencode',?1,'provider',?2,'account',?3,1,'subscription_included')",rusqlite::params![id,model,time]).unwrap();
            for (role, quantity) in [("input", 100), ("output", 50)] {
                conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token','billed','synthetic',?3,0)",rusqlite::params![id,role,quantity]).unwrap();
            }
            conn.execute("UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id=?1",[id]).unwrap();
            for role in ["input", "output"] {
                conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token',1,'covered','subscription_included',1,'USD',0,'pricing_pass',1)",rusqlite::params![id,role]).unwrap();
            }
            // An unfetched beta timeline leaves exactly its hour pending even
            // after both maintenance queues have drained.
            if pending && model == "beta" {
                continue;
            }
            let history = serde_json::from_value(json!({
                "source":"synthetic", "provider_id":"provider", "model_id":model,
                "fact_prefix":"rate.", "eras":[], "served_by_key":{
                    "rate.input":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":rate_units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}],
                    "rate.output":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":rate_units*2,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}]
                }
            })).unwrap();
            publish(conn, &fetched(history, "provider", model, None).unwrap(), 1).unwrap();
        }
        build_hours(conn);
        let (built, waiting): (i64, i64) = conn.query_row(
            "SELECT SUM(state='built'),SUM(state='not_yet_valued') FROM valuation_hour WHERE kind=0", [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!((built, waiting), if pending { (1, 1) } else { (2, 0) });
        Ok(())
    }).unwrap();
    state
}

fn build_hours(conn: &rusqlite::Connection) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut summaries = SummaryMaintenance::default();
    while conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM summary_dirty_hour)",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "summary did not drain"
        );
        if summaries.read_chunk(conn, 10_000).unwrap() {
            let tx = conn.unchecked_transaction().unwrap();
            summaries.publish(&tx, 10_000).unwrap();
            tx.commit().unwrap();
        }
    }
    // Match test_state's empty multiplier config: without configured scope
    // rules, only an explicit covered account or credential can join a pool.
    let config = MultiplierConfig {
        api_twins: Default::default(),
        plan_price_overrides: Default::default(),
        quota_providers: Default::default(),
    };
    let tx = conn.unchecked_transaction().unwrap();
    ValuationMaintenance::prepare(&tx, &config, &[]).unwrap();
    tx.commit().unwrap();
    let mut valuation = ValuationMaintenance::default();
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "valuation did not drain"
        );
        let tx = conn.unchecked_transaction().unwrap();
        let ready = valuation.read_chunk(&tx, &config, &[]).unwrap();
        tx.commit().unwrap();
        if ready {
            let tx = conn.unchecked_transaction().unwrap();
            assert!(valuation.publish(&tx).unwrap());
            tx.commit().unwrap();
        } else if !valuation.has_pending() {
            break;
        }
    }
}

fn served(state: &SharedState, extra: Value) -> Value {
    let mut request = json!({"grouping":[],"grain":"day","timezone":"UTC"});
    request
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    match ops::dispatch(state, ops::OP_REPORT, request) {
        Ok(mut result) => {
            // The clock is not an input to spend.report. Normalize only its
            // as-of instant; covered charges have no resolved-rate timestamp.
            result["rate_staleness"]["as_of_ms"] = json!(0);
            json!({"result":result})
        }
        Err((code, message)) => json!({"error":{"code":code,"message":message}}),
    }
}

fn vectors() -> Vec<(String, Value)> {
    let state = fixture();
    let mut cases=vec![
        ("one-grouping",json!({"grouping":["producer"]})),
        ("two-groupings",json!({"grouping":["producer","model"]})),
        ("runner-harness",json!({"grouping":["runner","harness"]})),
        ("hour",json!({"grain":"hour","from_ms":0,"to_ms":7_200_000})),
        ("where-null",json!({"grouping":["producer"],"where":{"work_class":[null],"producer":["broca","pi"]}})),
        ("range",json!({"from_ms":1,"to_ms":2})),
        ("invalid-grouping-where",json!({"where":{"not_a_dimension":["x"]}})),
        ("invalid-filter",json!({"where":{"producer":[]}})),
        ("invalid-range",json!({"from_ms":1,"to_ms":1})),
        ("invalid-range-missing-end",json!({"from_ms":1})),
        ("range-too-large",json!({"from_ms":0,"to_ms":401_i64*86_400_000})),
        ("hour-no-range",json!({"grain":"hour"})),
        ("hour-range-too-large",json!({"grain":"hour","from_ms":1,"to_ms":31_i64*86_400_000+1})),
        ("hour-timezone-unsupported",json!({"grain":"hour","timezone":"Asia/Kolkata","from_ms":0,"to_ms":1})),
        ("malformed",json!({"unexpected":true})),
    ].into_iter().map(|(name,q)|(name.to_string(),q)).collect::<Vec<_>>();
    for dimension in ["alfonso", "workload", "workspace", "parent_alfonso"] {
        cases.push((
            format!("unavailable-grouping-{dimension}"),
            json!({"grouping":[dimension]}),
        ));
        cases.push((
            format!("unavailable-where-{dimension}"),
            json!({"where":{dimension:[null]}}),
        ));
    }
    let mut vectors: Vec<_> = cases
        .into_iter()
        .map(|(name, q)| (format!("{name}.json"), served(&state, q)))
        .collect();
    let valued = plan_fixture(false);
    for (name, query) in [
        (
            "plan",
            json!({"grouping":["plan"],"from_ms":0,"to_ms":86_400_000}),
        ),
        (
            "plan-account",
            json!({"grouping":["plan","plan_account"],"from_ms":0,"to_ms":86_400_000}),
        ),
        (
            "where-plan",
            json!({"grouping":["model"],"where":{"plan":["provider"]},"from_ms":0,"to_ms":86_400_000}),
        ),
    ] {
        vectors.push((format!("{name}.json"), served(&valued, query)));
    }
    vectors.push((
        "pending.json".into(),
        served(
            &plan_fixture(true),
            json!({"grouping":["plan","plan_account"],"from_ms":0,"to_ms":86_400_000}),
        ),
    ));
    vectors.push(("no-usd-fees.json".into(), served(&state, json!({"grouping":["producer"],"where":{"producer":["pi"]},"from_ms":0,"to_ms":86_400_000}))));
    vectors
}

#[test]
fn analytics_vectors_match_real_dispatch_and_sha256sums() {
    let export = std::env::var_os("ASTROCYTE_EXPORT_ANALYTICS_VECTORS").is_some();
    let mut hashes = String::new();
    for (name, value) in vectors() {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        hashes.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
        if export {
            println!(
                "VECTOR {}",
                json!({"name":name,"content":String::from_utf8(bytes).unwrap()})
            );
        } else {
            assert_eq!(
                std::fs::read(format!("{DIR}/{name}")).unwrap(),
                bytes,
                "{name}"
            );
        }
    }
    hashes.push_str(&format!(
        "{:x}  README.md\n",
        Sha256::digest(std::fs::read(format!("{DIR}/README.md")).unwrap())
    ));
    if export {
        println!("VECTOR {}", json!({"name":"SHA256SUMS","content":hashes}));
    } else {
        assert_eq!(
            std::fs::read_to_string(format!("{DIR}/SHA256SUMS")).unwrap(),
            hashes
        );
    }
}

#[test]
fn analytics_vectors_pin_inclusive_tokens_and_unknown_quantities() {
    let vectors: std::collections::BTreeMap<_, _> = vectors().into_iter().collect();
    let report = &vectors["two-groupings.json"]["result"];
    assert_eq!(report["totals"][0]["tokens"]["output"], 126);
    assert_eq!(
        report["totals"][0]["unknown_quantities"],
        json!({"input":1})
    );
    assert_eq!(
        vectors["hour-timezone-unsupported.json"]["error"]["code"],
        "hour_timezone_unsupported"
    );
}

#[test]
fn analytics_vectors_pin_list_value_deduced_and_pending() {
    let vectors: std::collections::BTreeMap<_, _> = vectors().into_iter().collect();
    for name in ["plan.json", "plan-account.json"] {
        let report = &vectors[name]["result"];
        assert_eq!(report["rows"][0]["plan"], "provider");
        assert_eq!(report["rows"][0]["list_value"]["nanos"], 600_000);
        assert_eq!(report["rows"][0]["deduced"]["nanos"], 645_161_290);
        assert_eq!(report["totals"][0]["total_nanos"], 0);
        assert_eq!(report["totals"][0]["list_value"]["not_yet_valued_hours"], 0);
        assert_eq!(report["totals"][0]["deduced"]["pending_days"], 0);
        assert_eq!(report["totals"][0]["deduced_unspread_nanos"], 0);
    }
    assert_eq!(
        vectors["plan-account.json"]["result"]["rows"][0]["plan_account"],
        "account"
    );
    let filtered = &vectors["where-plan.json"]["result"];
    assert_eq!(filtered["rows"].as_array().unwrap().len(), 2);
    assert_eq!(filtered["rows"][0]["list_value"]["nanos"], 200_000);
    assert_eq!(filtered["rows"][0]["deduced"]["nanos"], 215_053_763);
    assert_eq!(filtered["rows"][1]["list_value"]["nanos"], 400_000);
    assert_eq!(filtered["rows"][1]["deduced"]["nanos"], 430_107_527);
    let pending = &vectors["pending.json"]["result"];
    let pending_row = pending["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["plan"] == "pending")
        .unwrap();
    assert!(pending_row["plan_account"].is_null());
    assert_eq!(pending_row["events_total"], 1);
    assert_eq!(pending_row["list_value"]["not_yet_valued_hours"], 1);
    assert_eq!(pending["totals"][0]["list_value"]["nanos"], 200_000);
    assert_eq!(
        pending["totals"][0]["list_value"]["not_yet_valued_hours"],
        1
    );
    assert_eq!(pending["totals"][0]["deduced"]["pending_days"], 1);
    let no_fees = &vectors["no-usd-fees.json"]["result"];
    assert_eq!(no_fees["rows"].as_array().unwrap().len(), 1);
    assert!(no_fees["rows"][0]["currency"].is_null());
    assert_eq!(no_fees["totals"].as_array().unwrap().len(), 1);
    assert!(no_fees["totals"][0]["currency"].is_null());
    assert!(no_fees["totals"][0].get("deduced").is_none());
}
