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
fn attribution_reader_absent_publisher_backs_off_over_an_hour() {
    let state = test_state();
    let stop = AtomicBool::new(false);
    let source = crate::event_wakes::Source::Attribution(Publisher::Entorhinal.key());
    state.event_wakes().attached(&[source], true);
    let before = transaction_attempts_for_test();
    let mut absent_opens = Vec::new();
    let mut other_failures = 0;
    for now in (0..3_600_000).step_by(250) {
        let full_tick = now % 1_000 == 0;
        drain_selected(
            &state,
            &stop,
            full_tick,
            || now,
            |publisher| match publisher {
                Publisher::Entorhinal => {
                    absent_opens.push(now / 1_000);
                    Err(DrainError::Absent)
                }
                Publisher::Basal => {
                    other_failures += 1;
                    Ok(false)
                }
                _ => Ok(true),
            },
        );
    }
    assert_eq!(
        absent_opens.len(),
        9,
        "absent publisher route opens in one hour"
    );
    assert_eq!(absent_opens, [0, 60, 180, 420, 900, 1500, 2100, 2700, 3300]);
    assert_eq!(
        other_failures, 3_600,
        "other failures still retry each full tick"
    );
    assert_eq!(transaction_attempts_for_test(), before);
}

#[test]
fn attribution_reader_wake_and_open_reset_absence_backoff() {
    let state = test_state();
    let stop = AtomicBool::new(false);
    let source = crate::event_wakes::Source::Attribution(Publisher::Entorhinal.key());
    state.event_wakes().attached(&[source], true);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let step = |now, full_tick, should_open, served_refusal| {
        let mut opens = 0;
        drain_selected(
            &state,
            &stop,
            full_tick,
            || now,
            |publisher| {
                if publisher != Publisher::Entorhinal {
                    return Ok(true);
                }
                opens += 1;
                if served_refusal {
                    let mut peer = Peer::new(vec![json!({"error":{"code":"not_permitted"}})]);
                    runtime
                        .block_on(drain_pages(&state, publisher, now, &stop, &mut peer))
                        .map_err(DrainError::Page)
                } else {
                    Err(DrainError::Absent)
                }
            },
        );
        assert_eq!(opens, usize::from(should_open), "route opens at {now} ms");
    };

    step(0, true, true, false);
    step(60_000, true, true, false);
    step(61_000, true, false, false);
    let mut wake = state.event_wakes().wake(source);
    step(62_000, false, true, false);
    assert_eq!(
        wake.try_recv(),
        Ok(false),
        "absence must not acknowledge the wake"
    );
    step(121_999, true, false, false);
    step(122_000, true, true, false);
    // Opening successfully resets the schedule even if the page is refused.
    step(242_000, true, true, true);
    step(243_000, true, true, false);
    step(302_999, true, false, false);
    step(303_000, true, true, false);
}

#[tokio::test]
async fn attribution_reader_prefrontal_absence_requires_every_module_id() {
    let refusal = |code| {
        CallError::Module(
            serde_json::from_value(json!({"code": code, "message": "fixture refusal"})).unwrap(),
        )
    };
    for (second, expected) in [
        (Err(error_codes::TARGET_UNAVAILABLE), Err(OpenError::Absent)),
        (Err(error_codes::MODULE_RELOADING), Err(OpenError::Other)),
        (Ok("legacy route"), Ok("legacy route")),
    ] {
        let mut attempted = Vec::new();
        let route = open_publisher(Publisher::Prefrontal, |module| {
            attempted.push(module);
            std::future::ready(match module {
                "prefrontal" => Err(refusal(error_codes::UNKNOWN_MODULE)),
                _ => second.map_err(refusal),
            })
        })
        .await;
        assert_eq!(attempted, ["prefrontal", "prefrontal-core"]);
        assert_eq!(route, expected);
    }
}

#[tokio::test]
async fn attribution_reader_sdk_preserves_target_unavailable() {
    use crate::test_support::{fake_daemon, read_peer_request, HANG_GUARD};
    use subc_protocol::{Frame, FrameType};
    use subc_transport::{auth::authenticate_server, write_frame};

    let (listener, path, connection) = fake_daemon("attribution-unavailable", 41, 42);
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let daemon = async {
        let serve = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            authenticate_server(
                &mut stream,
                &connection.key,
                &connection.daemon_id,
                &connection.daemon_ver,
                HANG_GUARD,
            )
            .await
            .unwrap();
            while let Some(request) = read_peer_request(&mut stream).await {
                let open: Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(open["op"], "route.open");
                assert_eq!(open["target"]["module_id"], "entorhinal");
                let body =
                    json!({"code": error_codes::TARGET_UNAVAILABLE, "message": "fixture refusal"});
                let response = Frame::build(
                    FrameType::Error,
                    request.header.flags,
                    0,
                    0,
                    request.header.corr,
                    serde_json::to_vec(&body).unwrap(),
                )
                .unwrap();
                if write_frame(&mut stream, &response).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! { () = serve => {}, _ = released => {} }
    };
    let client = async {
        let consumer = crate::peer_exchange::PeerAccess::Fixture(path.clone())
            .connect(HANG_GUARD)
            .await
            .unwrap();
        let result = open_publisher(Publisher::Entorhinal, |module| {
            open_route(
                &consumer,
                RouteTarget::ManagementSurface {
                    module_id: module.into(),
                },
                "attribution/entorhinal",
                CallOptions {
                    route_retry_deadline: Duration::from_millis(200),
                    ..options()
                },
            )
        })
        .await;
        let _ = release.send(());
        result
    };
    let (_, result) = tokio::time::timeout(HANG_GUARD, async { tokio::join!(daemon, client) })
        .await
        .expect("attribution fake daemon did not finish");
    std::fs::remove_file(path).unwrap();
    assert!(matches!(result, Err(OpenError::Absent)));
}

#[test]
fn event_wakes_attribution_idle_opens_no_routes_and_subject_selects_one_publisher() {
    let state = test_state();
    let stop = AtomicBool::new(false);
    let sources =
        Publisher::ALL.map(|publisher| crate::event_wakes::Source::Attribution(publisher.key()));
    state.event_wakes().attached(&sources, true);
    let now = crate::state::now_ms();
    for source in sources {
        state
            .event_wakes()
            .begin(source, true, now)
            .unwrap()
            .finish(true);
    }
    let mut opens = 0;
    for _ in 0..10 {
        drain_selected(
            &state,
            &stop,
            true,
            || now,
            |_| {
                opens += 1;
                Ok(true)
            },
        );
    }
    assert_eq!(
        opens, 0,
        "an idle attached attribution durable must open no publisher routes"
    );
    let source = crate::event_wakes::Source::Attribution("basal");
    let mut first = state.event_wakes().wake(source);
    let mut second = state.event_wakes().wake(source);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    drain_selected(
        &state,
        &stop,
        false,
        || now,
        |publisher| {
            opens += 1;
            assert_eq!(publisher, Publisher::Basal);
            let mut peer = Peer::new(basal_pages());
            runtime
                .block_on(drain_pages(&state, publisher, now, &stop, &mut peer))
                .map_err(DrainError::Page)
        },
    );
    assert_eq!(opens, 1);
    assert_eq!(first.try_recv(), Ok(true));
    assert_eq!(second.try_recv(), Ok(true));
    let facts: i64 = state
        .with_conn(|conn| {
            conn.query_row("SELECT count(*) FROM attribution_fact", [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())
        })
        .unwrap();
    assert_eq!(facts, 4);
    let mut failed = state.event_wakes().wake(source);
    drain_selected(
        &state,
        &stop,
        false,
        || now,
        |publisher| {
            let mut peer = Peer::new(vec![json!({"error":{"code":"not_permitted"}})]);
            runtime
                .block_on(drain_pages(&state, publisher, now, &stop, &mut peer))
                .map_err(DrainError::Page)
        },
    );
    assert_eq!(
        failed.try_recv(),
        Ok(false),
        "a read refusal is not an empty committed page"
    );
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
                        [publisher.key()],
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
