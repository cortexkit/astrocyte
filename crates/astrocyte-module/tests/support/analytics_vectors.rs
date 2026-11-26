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
    "/test-vectors/analytics-reads-v6"
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

fn plan_fixture(pending: bool, recorded_currency: Option<&str>) -> SharedState {
    let state = test_state();
    state.with_conn(|conn| {
        conn.execute("INSERT INTO account_subscription(account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms,effective_to_ms) VALUES('account','provider','plan',2000,'USD',2,'month',0,86400000)", []).unwrap();
        for (id, model, time, rate_units) in [
            ("included-alpha", "alpha", 1, 1_000_000_000_i64),
            ("included-beta", "beta", 3_600_001, 2_000_000_000),
        ] {
            conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,provider,model,account_ref,occurred_at_ms,recorded_at_ms,charge_basis,runner,harness,session) VALUES(?1,'opencode',?1,'provider',?2,'account',?3,1,'subscription_included','thalamus','opencode',?1)",rusqlite::params![id,model,time]).unwrap();
            for (role, quantity) in [("input", 100), ("output", 50)] {
                conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token','billed','synthetic',?3,0)",rusqlite::params![id,role,quantity]).unwrap();
            }
            if let Some(currency) = recorded_currency {
                conn.execute("UPDATE usage_event SET pricing_generation=1,currency=?2 WHERE event_id=?1",rusqlite::params![id,currency]).unwrap();
                for role in ["input", "output"] {
                    conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token',1,'covered','subscription_included',1,?3,0,'pricing_pass',1)",rusqlite::params![id,role,currency]).unwrap();
                }
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

fn attribution_fixture() -> SharedState {
    use astrocyte_core::attribution::{commit_page, Page, Publisher};
    let state = plan_fixture(false, Some("USD"));
    state.with_conn(|conn| {
        let facts:Page=serde_json::from_value(json!({"facts":[
            {"fact_id":"fixture-a","supersedes":null,"subject":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"included-alpha"},"alfonso":"agent_fixture","parent":null,"workload":"mason","task":null,"project_id":"project_fixture","effective_from_ms":0,"recorded_at_ms":1},
            {"fact_id":"fixture-b","supersedes":null,"subject":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"included-beta"},"alfonso":null,"parent":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"included-alpha"},"workload":"summary","task":"magic-context.summary","project_id":"project_fixture","effective_from_ms":0,"recorded_at_ms":1}
        ],"next":null,"through":2})).unwrap();
        let placement:Page=serde_json::from_value(json!({"facts":[{"fact_id":"fixture-placement","project_id":"project_fixture","workspace_id":"workspace_fixture","effective_from_ms":0,"recorded_at_ms":1,"supersedes":null}],"next":null,"through":1})).unwrap();
        let tx=conn.unchecked_transaction().unwrap();commit_page(&tx,Publisher::Prefrontal,None,&facts,1).unwrap();commit_page(&tx,Publisher::Entorhinal,None,&placement,1).unwrap();tx.commit().unwrap();
        let mut summaries=SummaryMaintenance::default();let mut valuation=ValuationMaintenance::default();let config=MultiplierConfig{api_twins:Default::default(),plan_price_overrides:Default::default(),quota_providers:Default::default()};
        let deadline=std::time::Instant::now()+std::time::Duration::from_secs(30);
        loop {
            assert!(std::time::Instant::now()<deadline,"attribution projections did not drain");
            if summaries.read_chunk(conn,10_000).unwrap() {let tx=conn.unchecked_transaction().unwrap();summaries.publish(&tx,10_000).unwrap();tx.commit().unwrap();}
            let tx=conn.unchecked_transaction().unwrap();let ready=valuation.read_chunk(&tx,&config,&[]).unwrap();tx.commit().unwrap();
            if ready {let tx=conn.unchecked_transaction().unwrap();valuation.publish(&tx).unwrap();tx.commit().unwrap();} else if !valuation.has_pending() {break;}
        }
        Ok(())
    }).unwrap();
    state
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
    cases.push((
        "unavailable-grouping-task".into(),
        json!({"grouping":["task"]}),
    ));
    cases.push((
        "unavailable-where-task".into(),
        json!({"where":{"task":[null]}}),
    ));
    let mut vectors: Vec<_> = cases
        .into_iter()
        .map(|(name, q)| (format!("{name}.json"), served(&state, q)))
        .collect();
    let valued = plan_fixture(false, Some("USD"));
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
            &plan_fixture(true, Some("USD")),
            json!({"grouping":["plan","plan_account"],"from_ms":0,"to_ms":86_400_000}),
        ),
    ));
    vectors.push(("no-usd-fees.json".into(), served(&state, json!({"grouping":["producer"],"where":{"producer":["pi"]},"from_ms":0,"to_ms":86_400_000}))));
    let attribution = attribution_fixture();
    for (name, query) in [
        ("attribution-alfonso", json!({"grouping":["alfonso"]})),
        ("attribution-parent", json!({"grouping":["parent_alfonso"]})),
        ("attribution-workload", json!({"grouping":["workload"]})),
        ("attribution-project-id", json!({"grouping":["project_id"]})),
        ("attribution-workspace", json!({"grouping":["workspace"]})),
        (
            "attribution-alfonso-model",
            json!({"grouping":["alfonso","model"]}),
        ),
        (
            "attribution-workload-parent",
            json!({"grouping":["workload","parent_alfonso"]}),
        ),
        (
            "attribution-filter",
            json!({"grouping":["workload"],"where":{"alfonso":["agent_fixture"],"model":["alpha"]}}),
        ),
        (
            "attribution-null",
            json!({"grouping":["alfonso"],"where":{"alfonso":[null]}}),
        ),
        (
            "attribution-combination-legacy-project",
            json!({"grouping":["alfonso","project"]}),
        ),
        (
            "attribution-combination-provider-model",
            json!({"grouping":["alfonso","model"],"where":{"provider":["provider"]}}),
        ),
    ] {
        vectors.push((format!("{name}.json"), served(&attribution, query)));
    }
    vectors.push((
        "attribution-unattributed.json".into(),
        served(
            &state,
            json!({"grouping":["alfonso"],"from_ms":0,"to_ms":86_400_000}),
        ),
    ));
    vectors
}

#[test]
fn analytics_vectors_match_real_dispatch_and_sha256sums() {
    let export = std::env::var_os("ASTROCYTE_EXPORT_ANALYTICS_VECTORS").is_some();
    let mut hashes = String::new();
    for (name, value) in vectors().into_iter().chain(fixed_clock_vectors()) {
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

fn fixed_clock_vectors() -> Vec<(String, Value)> {
    use crate::route_reply;
    use subc_client_rs::HandlerOutcome;
    use subc_protocol::ErrorBody;

    let ordinary = fixture();
    let attribution = attribution_fixture();
    let unpriced_subscription = plan_fixture(false, None);
    let today = 86_400_000;
    let cases = [
        (
            "range-attribution-where.json",
            &attribution,
            false,
            json!({
                "grain":"range", "timezone":"UTC", "grouping":["workload","parent_alfonso"],
                "where":{"model":["beta"]}, "from_ms":1, "to_ms":2*today
            }),
        ),
        (
            "range-ends-today.json",
            &ordinary,
            false,
            json!({
                "grain":"range", "timezone":"UTC", "grouping":[], "from_ms":1, "to_ms":today+1
            }),
        ),
        (
            "reply-too-large.json",
            &ordinary,
            true,
            json!({
                "grain":"day", "timezone":"UTC", "grouping":["model"], "from_ms":0, "to_ms":2*today
            }),
        ),
        (
            "null-currency-deduced.json",
            &unpriced_subscription,
            false,
            json!({
                "grain":"day", "timezone":"UTC", "grouping":[], "from_ms":0, "to_ms":today
            }),
        ),
    ];
    let mut vectors = Vec::with_capacity(cases.len());
    for (name, state, force_limit, params) in cases {
        let method = ops::OP_REPORT;
        let dispatched = super::dispatch_with_report_clock(state, method, params, || today + 3600);
        let reply = route_reply::from_dispatch(method, dispatched);
        let value = if force_limit {
            let HandlerOutcome::Response(body) = &reply else {
                panic!("expected a report")
            };
            let limit = body.len() - 1;
            let HandlerOutcome::ErrorWithDetail {
                code,
                message,
                detail,
            } = route_reply::finish_with_limit(method, reply, limit)
            else {
                panic!("expected a refused read")
            };
            assert_eq!(code, "reply_too_large");
            json!({"error": ErrorBody::new(code, message).with_detail(detail)})
        } else {
            let HandlerOutcome::Response(body) = route_reply::finish(method, reply) else {
                panic!("expected a spend report")
            };
            let value: Value = serde_json::from_slice(&body).unwrap();
            let row = &value["result"]["rows"][0];
            if name == "null-currency-deduced.json" {
                assert_eq!(row["bucket_end_ms"], today);
                assert!(row["currency"].is_null());
                assert_eq!(row["total_nanos"], 0);
                assert_eq!(
                    row["deduced"],
                    json!({"currency":"USD", "nanos":645_161_290, "pending_days":0})
                );
            } else {
                assert_eq!(row["bucket_end_ms"], 2 * today);
                if name == "range-ends-today.json" {
                    assert_eq!(row["events_total"], 4);
                }
            }
            value
        };
        vectors.push((name.into(), value));
    }
    vectors
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

#[test]
#[ignore = "timing comparison over 90 days of synthetic attributed usage"]
fn spend_report_range_90_day_timing() {
    use crate::route_reply;
    use astrocyte_core::attribution::{commit_page, Page, Publisher};
    use std::time::Instant;
    use subc_client_rs::HandlerOutcome;

    const DAYS: i64 = 90;
    const DAY_MS: i64 = 86_400_000;
    const AGENTS: i64 = 24;
    const MODELS: i64 = 6;
    const DAILY_CALLS_PER_GROUP: i64 = 3;
    let state = test_state();
    state.with_conn(|conn| {
        let tx = conn.unchecked_transaction().unwrap();
        tx.execute("INSERT INTO account_subscription(account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms) VALUES('account','provider','plan',2000,'USD',2,'month',0)", []).unwrap();
        tx.execute(
            "WITH RECURSIVE day(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM day WHERE n+1<?1),
             agent(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM agent WHERE n+1<?2),
             model(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM model WHERE n+1<?3),
             call(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM call WHERE n+1<?4),
             seed AS (SELECT printf('call-%d-%d-%d-%d',day.n,agent.n,model.n,call.n) AS id,
                 printf('agent-%d',agent.n) AS agent,printf('model-%d',model.n) AS model,
                 day.n*86400000+(9+model.n)*3600000+call.n AS at
                 FROM day CROSS JOIN agent CROSS JOIN model CROSS JOIN call)
             INSERT INTO usage_event(event_id,producer,producer_ref,provider,model,account_ref,
                 occurred_at_ms,recorded_at_ms,charge_basis,runner,harness,session)
             SELECT id,'opencode',id,'provider',model,'account',at,at,'subscription_included',
                 'thalamus','opencode',agent FROM seed",
            rusqlite::params![DAYS, AGENTS, MODELS, DAILY_CALLS_PER_GROUP],
        ).unwrap();
        tx.execute("UPDATE usage_event SET pricing_generation=1,currency='USD'", []).unwrap();
        for (role, units) in [("input", 1300), ("output", 123)] {
            tx.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                SELECT event_key,?1,'token','billed','synthetic',?2,0 FROM usage_event", rusqlite::params![role, units]).unwrap();
            tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms)
                SELECT event_key,?1,'token',1,'covered','subscription_included',1,'USD',0,'pricing_pass',1 FROM usage_event", [role]).unwrap();
        }
        let facts = (0..AGENTS).map(|n| {
            let agent = format!("agent-{n}");
            json!({"fact_id":agent,"supersedes":null,"subject":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":agent},"alfonso":agent,"parent":null,"workload":"mason","task":null,"project_id":null,"effective_from_ms":0,"recorded_at_ms":1})
        }).collect::<Vec<_>>();
        let page: Page = serde_json::from_value(json!({"facts": facts,"next":null,"through":AGENTS})).unwrap();
        commit_page(&tx, Publisher::Prefrontal, None, &page, 1).unwrap();
        for n in 0..MODELS {
            let model = format!("model-{n}");
            let history = serde_json::from_value(json!({
                "source":"synthetic", "provider_id":"provider", "model_id":model,
                "fact_prefix":"rate.", "eras":[], "served_by_key":{
                    "rate.input":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":947_000_001,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}],
                    "rate.output":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":1_203_000_001,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}]
                }
            })).unwrap();
            publish(&tx, &fetched(history, "provider", &model, None).unwrap(), 1).unwrap();
        }
        tx.commit().unwrap();
        build_hours(conn);
        Ok(())
    }).unwrap();

    let run = |grain| {
        let query = json!({"grain":grain,"grouping":["alfonso","model"],"timezone":"UTC","from_ms":0,"to_ms":DAYS*DAY_MS});
        let method = ops::OP_REPORT;
        let start = Instant::now();
        let outcome =
            super::dispatch_with_report_clock(&state, method, query, || (DAYS + 1) * DAY_MS);
        let reply = route_reply::from_dispatch(method, outcome);
        // The pre-limit day path handed these exact bytes to the serve helper.
        // Include the production read check in the range timing.
        let reply = if grain == "range" {
            route_reply::finish(method, reply)
        } else {
            reply
        };
        let elapsed = start.elapsed();
        let HandlerOutcome::Response(body) = reply else {
            panic!("expected a report")
        };
        (elapsed, body)
    };

    let (_, day_body) = run("day");
    let (_, range_body) = run("range");
    let day_bytes = day_body.len();
    let range_bytes = range_body.len();
    let day: Value = serde_json::from_slice(&day_body).unwrap();
    let range: Value = serde_json::from_slice(&range_body).unwrap();
    assert_eq!(
        day["result"]["rows"].as_array().unwrap().len(),
        usize::try_from(DAYS * AGENTS * MODELS).unwrap()
    );
    assert_eq!(
        range["result"]["rows"].as_array().unwrap().len(),
        usize::try_from(AGENTS * MODELS).unwrap()
    );
    assert_eq!(day["result"]["totals"], range["result"]["totals"]);
    let HandlerOutcome::ErrorWithDetail { code, detail, .. } =
        route_reply::finish(ops::OP_REPORT, HandlerOutcome::Response(day_body))
    else {
        panic!("the day report must overflow the real frame limit")
    };
    assert_eq!(code, "reply_too_large");
    assert_eq!(detail["rows"], DAYS * AGENTS * MODELS);
    assert_eq!(detail["reply_bytes"], day_bytes);

    let mut day = Vec::new();
    let mut range = Vec::new();
    for trial in 0..7 {
        if trial % 2 == 0 {
            day.push(run("day").0);
            range.push(run("range").0);
        } else {
            range.push(run("range").0);
            day.push(run("day").0);
        }
    }
    day.sort();
    range.sort();
    println!("90 days × {AGENTS} agents × {MODELS} models, {} calls: day {:?} median, {day_bytes} bytes; range {:?} median, {range_bytes} bytes (7 pairs)", DAYS*AGENTS*MODELS*DAILY_CALLS_PER_GROUP, day[3], range[3]);
}
