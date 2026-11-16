use cortexkit_role_llm_runner::{session_ref, SessionIdentity};

#[test]
fn attribution_session_ref_matches_real_claude_roots_pair() {
    let case: serde_json::Value = serde_json::from_str(include_str!(
        "../../astrocyte-core/test-vectors/attribution-joins-v1/claude-roots.json"
    ))
    .unwrap();
    let roots = &case["entry"];
    let subject = serde_json::json!({
        "kind": "session",
        "runner": case["runner"],
        "harness": case["harness"],
        "session": roots["owner_session"]
    });
    let reference = session_ref(&SessionIdentity {
        runner: subject["runner"].as_str().unwrap(),
        harness: subject["harness"].as_str().unwrap(),
        session: subject["session"].as_str().unwrap(),
    });
    assert_eq!(reference, roots["session_ref"].as_str().unwrap());
}
