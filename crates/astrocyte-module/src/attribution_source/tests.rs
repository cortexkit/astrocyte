use super::*;
use crate::state::{test_state, transaction_attempts_for_test};
use std::collections::VecDeque;

struct Peer {
    replies: VecDeque<Result<Vec<u8>, ()>>,
    calls: Vec<Value>,
}
impl Peer {
    fn new(replies: Vec<Value>) -> Self {
        Self {
            replies: replies
                .into_iter()
                .map(|reply| Ok(serde_json::to_vec(&reply).unwrap()))
                .collect(),
            calls: Vec::new(),
        }
    }
}
impl ReadTransport for Peer {
    async fn read(&mut self, params: Value) -> Result<Vec<u8>, ()> {
        self.calls.push(params);
        self.replies.pop_front().expect("unexpected publisher read")
    }
}
fn run(state: &SharedState, publisher: Publisher, peer: &mut Peer) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(drain_pages(
            state,
            publisher,
            100,
            &AtomicBool::new(false),
            peer,
        ))
        .unwrap();
}
fn basal_pages() -> Vec<Value> {
    let first: Value = serde_json::from_str(include_str!(
        "../../../astrocyte-core/test-vectors/attribution-facts-v1/basal/read-page.json"
    ))
    .unwrap();
    let session: Value = serde_json::from_str(include_str!(
        "../../../astrocyte-core/test-vectors/attribution-facts-v1/basal/flow-session.json"
    ))
    .unwrap();
    let program: Value = serde_json::from_str(include_str!(
        "../../../astrocyte-core/test-vectors/attribution-facts-v1/basal/program-session.json"
    ))
    .unwrap();
    vec![
        json!({"result":first}),
        json!({"result":{"facts":[session,program],"next":null,"through":4}}),
    ]
}
#[test]
fn attribution_reader_pages_from_durable_cursor_and_known_suffix_opens_no_transaction() {
    let state = test_state();
    let pages = basal_pages();
    let mut peer = Peer::new(pages.clone());
    run(&state, Publisher::Basal, &mut peer);
    assert_eq!(peer.calls.len(), 2);
    assert!(peer.calls[0].get("after").is_none());
    assert_eq!(peer.calls[1]["after"], 2);
    state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT count(*) FROM attribution_fact", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                4
            );
            assert_eq!(
                conn.query_row(
                    "SELECT cursor FROM attribution_cursor WHERE publisher='basal'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
                "2"
            );
            Ok(())
        })
        .unwrap();
    let before = transaction_attempts_for_test();
    let mut replay = Peer::new(vec![pages[1].clone()]);
    run(&state, Publisher::Basal, &mut replay);
    assert_eq!(replay.calls[0]["after"], 2);
    assert_eq!(transaction_attempts_for_test(), before);
}
#[test]
fn attribution_reader_refusals_route_errors_and_empty_pages_preserve_cursor_and_health() {
    for code in [
        "not_permitted",
        "unknown_method",
        "unknown_module",
        "module_removed",
        "stale_route_epoch",
        "caller_not_admitted",
    ] {
        let state = test_state();
        state.mark_producer_degraded("attribution/basal", "existing".into());
        let before = transaction_attempts_for_test();
        let mut peer = Peer::new(vec![json!({"error":{"code":code}})]);
        run(&state, Publisher::Basal, &mut peer);
        assert_eq!(peer.calls.len(), 1);
        assert_eq!(transaction_attempts_for_test(), before);
        assert_eq!(
            state
                .producer_degraded_reason("attribution/basal")
                .as_deref(),
            Some("existing")
        );
    }
    let state = test_state();
    let before = transaction_attempts_for_test();
    let mut empty = Peer::new(vec![json!({"result":{"facts":[],"next":null,"through":0}})]);
    run(&state, Publisher::Entorhinal, &mut empty);
    assert_eq!(empty.calls.len(), 1);
    assert_eq!(transaction_attempts_for_test(), before);
    let mut route = Peer {
        replies: VecDeque::from([Err(())]),
        calls: Vec::new(),
    };
    run(&state, Publisher::MagicContext, &mut route);
    assert_eq!(transaction_attempts_for_test(), before);
}

#[test]
fn attribution_reader_pinned_magic_context_and_placement_pages_use_their_own_cursors() {
    let mc: Value = serde_json::from_str(include_str!(
        "../../../astrocyte-core/test-vectors/attribution-facts-v1/magic-context/read-page.json"
    ))
    .unwrap();
    let placement: Value = serde_json::from_str(include_str!(
        "../../../astrocyte-core/test-vectors/attribution-facts-v1/entorhinal/records.json"
    ))
    .unwrap();
    for (publisher, responses, table, rows) in [
        (
            Publisher::MagicContext,
            vec![mc["response"].clone()],
            "attribution_fact",
            2,
        ),
        (
            Publisher::Entorhinal,
            vec![placement[1]["reply"].clone(), placement[2]["reply"].clone()],
            "placement_fact",
            2,
        ),
    ] {
        let state = test_state();
        let mut peer = Peer::new(responses);
        run(&state, publisher, &mut peer);
        if publisher == Publisher::Entorhinal {
            assert_eq!(
                peer.calls[1]["after"],
                placement[1]["reply"]["result"]["next"]
            );
        }
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get::<_, i64>(0))
                        .unwrap(),
                    rows
                );
                assert_eq!(
                    conn.query_row(
                        "SELECT read_name FROM attribution_cursor WHERE publisher=?1",
                        [publisher.module()],
                        |r| r.get::<_, String>(0)
                    )
                    .unwrap(),
                    publisher.read_name()
                );
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn attribution_reader_page_failure_rolls_back_facts_and_cursor_together() {
    let state = test_state();
    let pages = basal_pages();
    let mut first = Peer::new(vec![
        pages[0].clone(),
        json!({"result":{"facts":[],"next":null,"through":4}}),
    ]);
    run(&state, Publisher::Basal, &mut first);
    state.with_conn(|conn| {conn.execute_batch("CREATE TEMP TRIGGER refuse_program BEFORE INSERT ON attribution_fact WHEN NEW.workload='codemode' BEGIN SELECT RAISE(ABORT,'test page rollback'); END;").unwrap();Ok(())}).unwrap();
    let mut second = Peer::new(vec![pages[1].clone()]);
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(drain_pages(
            &state,
            Publisher::Basal,
            200,
            &AtomicBool::new(false),
            &mut second,
        ));
    assert!(result.is_err());
    state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT count(*) FROM attribution_fact", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                2
            );
            assert_eq!(
                conn.query_row(
                    "SELECT cursor FROM attribution_cursor WHERE publisher='basal'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
                "2"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn attribution_reader_changed_fact_and_malformed_envelope_are_counted_without_replacement() {
    let state = test_state();
    let mut pages = basal_pages();
    pages[0]["result"]["next"] = Value::Null;
    let original = pages[0].clone();
    let mut peer = Peer::new(vec![original.clone()]);
    run(&state, Publisher::Basal, &mut peer);
    let mut changed = original.clone();
    changed["result"]["facts"][0]["workload"] = json!("different");
    let mut peer = Peer::new(vec![changed]);
    run(&state, Publisher::Basal, &mut peer);
    let mut malformed = Peer::new(vec![
        json!({"result":{"facts":"not-an-array","next":null,"through":2}}),
    ]);
    run(&state, Publisher::Basal, &mut malformed);
    state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT count(*) FROM attribution_fact", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                2
            );
            let coverage = astrocyte_core::report::ledger_coverage(
                conn,
                &astrocyte_core::report::LedgerCoverageRequest {},
                200,
            )
            .unwrap();
            assert_eq!(coverage.set_aside_by_source["attribution/basal"].records, 2);
            let reasons = conn
                .prepare("SELECT reason FROM set_aside_record ORDER BY reason")
                .unwrap()
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(reasons, ["fact_body_changed", "malformed_page"]);
            Ok(())
        })
        .unwrap();
}
