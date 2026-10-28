//! Synthetic Runs wire vectors, regenerated through the real read dispatch.
use crate::{
    ops,
    state::{test_state, SharedState},
    step_feed,
};
use astrocyte_core::ingest;
use rusqlite::params;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test-vectors/usage-runs-v1");
const SESSION: &str = "ses_SyntheticRuns7pUwcjKtrYmEu0ILcUmY92";
const RUN: &str = "run-sid-7pUwcjKtrYmEu0ILcUmY92";

fn state() -> SharedState {
    let state = test_state();
    state.with_conn(|c|{
        let events=[
            ("opencode",Some(SESSION),None,None,"alpha",10,"priced",vec![("input",Some(100)),("output",Some(20)),("reasoning",Some(5))]),
            ("opencode",Some(SESSION),None,None,"beta",20,"covered",vec![("input",Some(200)),("cache_read",Some(30))]),
            ("opencode",Some(SESSION),None,None,"alpha",30,"unpriced",vec![("output",Some(0))]),
            ("opencode",Some(SESSION),None,None,"beta",40,"pending",vec![("cache_write",None)]),
            ("broca",None,Some(RUN),None,"alpha",6,"priced",vec![("input",Some(5))]),
            ("broca",None,None,Some("flow_synthetic_batch"),"beta",7,"unpriced",vec![("input",Some(7))]),
            ("goose",Some("ses_SyntheticGooseTotal"),None,None,"alpha",8,"covered",vec![("input",Some(500))]),
        ];
        for (i,(producer,session,run,flow,model,time,outcome,measurements)) in events.into_iter().enumerate(){
            let mut request:ingest::EventRecordRequest=serde_json::from_value(json!({"producer":producer,"producer_ref":format!("synthetic-fact-{i}"),"session":session,"run_ref":run,"provider":"synthetic-provider","model":model,"occurred_at_ms":time,"work_class":if flow.is_some(){Some("batch-synthetic")}else{None},"charge_basis":if outcome=="covered"{"subscription_included"}else{"metered_api"},"measurements":measurements.iter().map(|(role,q)|json!({"role":role,"unit":"token","kind":"billed","source":"synthetic","quantity_units":q,"quantity_exponent":q.map(|_|0)})).collect::<Vec<_>>() })).unwrap();
            request.flow_ref=flow.map(str::to_owned);
            let tx=c.unchecked_transaction().unwrap();
            let id=ingest::record_event_in_tx(&tx,&request,100,ingest::DeliveryPath::Pushed).unwrap().event_id;
            tx.commit().unwrap();
            if outcome=="pending"{continue;}
            let tx=c.unchecked_transaction().unwrap();let priced=outcome=="priced";let covered=outcome=="covered";
            let currency=if priced||covered{Some("USD")}else{None};
            tx.execute("UPDATE usage_event SET pricing_generation=1,currency=?2 WHERE event_id=?1",params![id,currency]).unwrap();
            for (role,_) in measurements {
                let amount=match role{"input"=>31,"output"=>17,_=>7};
                tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token',1,?3,?4,?5,?6,?7,?8,?9,?10,?14,?11,?12,?13,'pricing_pass',100)",params![id,role,outcome,if covered{"subscription_included"}else{"metered_api"},i64::from(covered),currency,if priced{Some(amount)}else if covered{Some(0)}else{None},if outcome=="unpriced"{Some("no_rate")}else{None},if priced{Some(1)}else{None},if priced{Some(9)}else{None},if priced{Some("synthetic")}else{None},if priced{Some(100)}else{None},if priced{Some("{}")}else{None},if priced{Some("none")}else{None}]).unwrap();
            }
            tx.commit().unwrap();
        }
        Ok::<_,String>(())
    }).unwrap();
    state
}
fn served(state: &SharedState, method: &str, params: Value) -> Value {
    match ops::dispatch(state, method, params) {
        Ok(result) => json!({"result":result}),
        Err((code, message)) => json!({"error":{"code":code,"message":message}}),
    }
}
fn permission(method: &str) -> Value {
    let allowed = step_feed::may_read_feed(
        Some(&subc_protocol::Principal::Reserved {
            module_id: "callosum".into(),
        }),
        "fed-sim:synthetic",
    );
    let (code, message) = step_feed::require_read_permission(method, allowed).unwrap_err();
    json!({"error":{"code":code,"message":message}})
}
fn vectors() -> Vec<(&'static str, Value)> {
    let state = state();
    let list = ops::OP_RUNS_LIST;
    let detail = ops::OP_RUNS_DETAIL;
    let first = served(&state, list, json!({"from_ms":0,"to_ms":100,"limit":1}));
    let second = served(
        &state,
        list,
        json!({"from_ms":0,"to_ms":100,"limit":1,"after":first["result"]["next"]}),
    );
    let key = json!({"kind":"session","harness":"opencode","session":SESSION});
    vec![
        ("list-page.json", first),
        ("list-next-page.json", second),
        (
            "list-all.json",
            served(&state, list, json!({"from_ms":0,"to_ms":100})),
        ),
        (
            "detail-states.json",
            served(&state, detail, json!({"key":key})),
        ),
        (
            "detail-run.json",
            served(
                &state,
                detail,
                json!({"key":{"kind":"run","harness":"broca","run_id":RUN}}),
            ),
        ),
        (
            "detail-goose.json",
            served(
                &state,
                detail,
                json!({"key":{"kind":"session","harness":"goose","session":"ses_SyntheticGooseTotal"}}),
            ),
        ),
        ("list-malformed.json", served(&state, list, Value::Null)),
        (
            "list-invalid-range.json",
            served(&state, list, json!({"from_ms":1,"to_ms":1})),
        ),
        (
            "list-range-too-large.json",
            served(&state, list, json!({"from_ms":0,"to_ms":32_i64*86400000})),
        ),
        (
            "list-invalid-request.json",
            served(&state, list, json!({"from_ms":0,"to_ms":1,"limit":0})),
        ),
        ("list-not-permitted.json", permission(list)),
        ("detail-malformed.json", served(&state, detail, Value::Null)),
        (
            "detail-invalid-request.json",
            served(&state, detail, json!({"key":key,"limit":0})),
        ),
        (
            "detail-not-found.json",
            served(
                &state,
                detail,
                json!({"key":{"kind":"run","harness":"broca","run_id":"run-sid-MissingSynthetic"}}),
            ),
        ),
        ("detail-not-permitted.json", permission(detail)),
    ]
}
#[test]
fn runs_vectors_match_real_dispatch_and_sha256sums() {
    let mut hashes = String::new();
    for (name, value) in vectors() {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        hashes.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
        if std::env::var_os("ASTROCYTE_EXPORT_RUNS_VECTORS").is_some() {
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
    let readme = std::fs::read(format!("{DIR}/README.md")).unwrap();
    hashes.push_str(&format!("{:x}  README.md\n", Sha256::digest(readme)));
    if std::env::var_os("ASTROCYTE_EXPORT_RUNS_VECTORS").is_some() {
        println!("VECTOR {}", json!({"name":"SHA256SUMS","content":hashes}));
    } else {
        assert_eq!(
            std::fs::read_to_string(format!("{DIR}/SHA256SUMS")).unwrap(),
            hashes
        );
    }
}
#[test]
fn runs_vectors_pin_models_states_goose_and_unlabelled_runs() {
    let vectors: std::collections::BTreeMap<_, _> = vectors().into_iter().collect();
    let first = &vectors["list-page.json"]["result"];
    assert!(first["next"].is_string());
    assert_eq!(first["rows"][0]["models"].as_array().unwrap().len(), 2);
    assert_eq!(first["totals"]["rows"], 4);
    let all = vectors["list-all.json"]["result"]["rows"]
        .as_array()
        .unwrap();
    let run = all.iter().find(|r| r["key"]["kind"] == "run").unwrap();
    assert!(run.get("session").is_none());
    assert_eq!(run["pricing_unit"], "run_segment");
    let goose = &vectors["detail-goose.json"]["result"];
    assert_eq!(goose["pricing_unit"], "session_total");
    assert_eq!(goose["items"].as_array().unwrap().len(), 1);
    assert_eq!(goose["items"][0]["step_ref"], "synthetic-fact-6");
    let mut states = std::collections::BTreeSet::new();
    for item in vectors["detail-states.json"]["result"]["items"]
        .as_array()
        .unwrap()
    {
        for m in item["measurements"].as_array().unwrap() {
            let state = m["state"].as_str().unwrap();
            states.insert(state);
            assert_eq!(m.get("currency").is_some(), state == "priced");
            assert_eq!(m.get("amount_nanos").is_some(), state == "priced");
            assert_eq!(m.get("reason").is_some(), state == "unpriced");
        }
    }
    assert_eq!(
        states,
        std::collections::BTreeSet::from(["priced", "covered", "unpriced", "pending"])
    );
}
