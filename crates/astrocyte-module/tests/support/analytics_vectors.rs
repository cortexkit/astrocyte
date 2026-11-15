use crate::{
    ops,
    state::{test_state, SharedState},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-vectors/analytics-reads-v2"
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

fn served(state: &SharedState, extra: Value) -> Value {
    let mut request = json!({"grouping":[],"grain":"day","timezone":"UTC"});
    request
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    match ops::dispatch(state, ops::OP_REPORT, request) {
        Ok(mut result) => {
            // The clock is not an input to spend.report. Normalize only its
            // as-of instant so these invented pending-usage samples are stable.
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
    cases
        .into_iter()
        .map(|(name, q)| (format!("{name}.json"), served(&state, q)))
        .collect()
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
