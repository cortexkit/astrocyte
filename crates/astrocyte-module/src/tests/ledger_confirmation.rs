use super::*;

struct ConfirmationRig {
    state: Arc<SharedState>,
    stream: TcpStream,
    serving: tokio::task::JoinHandle<()>,
    _daemon: FakeDaemon,
}

impl ConfirmationRig {
    async fn start(required: bool, advertise_confirmation: bool) -> Self {
        let state = Arc::new(test_state());
        let daemon = FakeDaemon::start().await;
        let handler = AstrocyteHandler {
            state: Arc::clone(&state),
            routes: RouteBinds::default(),
            connection_end: Arc::new(Mutex::new(None)),
            subscriptions_stopping: tokio::sync::watch::channel(false).0,
            confirmation_required: required,
        };
        let path = daemon.config.connection_file_path.clone();
        let declaration = manifest(DEFAULT_MODULE_ID, &state.desired_config());
        let serving = tokio::spawn(async move {
            ServeOptions::default()
                .serve_with(&path, declaration, handler)
                .await
                .unwrap();
        });
        let (mut stream, _) = daemon.accept().await;
        let mut ack = hello_ack();
        let mut body: Value = serde_json::from_slice(&ack.body).unwrap();
        if advertise_confirmation {
            body["subc_ops"] = json!(["operator.confirm"]);
        }
        ack.body = serde_json::to_vec(&body).unwrap();
        // Rebuild after changing the body so the wire header has the right length.
        send(
            &mut stream,
            route_frame(FrameType::HelloAck, 0, 0, 1, ack.body),
        )
        .await;
        bind_as(&mut stream, 5, 1, Principal::Direct, "h").await;
        Self {
            state,
            stream,
            serving,
            _daemon: daemon,
        }
    }

    async fn finish(self) {
        drop(self.stream);
        tokio::time::timeout(HANG_GUARD, self.serving)
            .await
            .expect("confirmation test serving did not end at EOF")
            .unwrap();
    }
}

fn subscription_params(account: &str) -> Value {
    json!({"account_ref": account, "provider": "provider", "plan_name": "plan",
        "recurring_cost_minor": 100, "currency": "USD", "amount_exponent": 2,
        "period": "month", "effective_from": 1})
}

fn subscription_rows(state: &SharedState) -> Vec<(String, i64)> {
    state
        .with_conn(|conn| {
            let mut query = conn
                .prepare("SELECT account_ref, recurring_cost_minor FROM account_subscription ORDER BY account_ref")
                .map_err(|e| e.to_string())?;
            let rows = query
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| e.to_string())?;
            rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
        })
        .unwrap()
}

// Compare every stored value, not just row counts: refusals must not change money
// or existing rows, nor append audit or redelivery rows.
fn store_snapshot(state: &SharedState) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
    state
        .with_conn(|conn| {
            let tables = conn
                .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
                .map_err(|e| e.to_string())?
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            tables
                .into_iter()
                .map(|table| {
                    let mut query = conn
                        .prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))
                        .map_err(|e| e.to_string())?;
                    let columns = query.column_count();
                    let values = query
                        .query_map([], |row| {
                            (0..columns).map(|column| row.get(column)).collect()
                        })
                        .map_err(|e| e.to_string())?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| e.to_string())?;
                    Ok((table, values))
                })
                .collect()
        })
        .unwrap()
}

fn assert_route_response(frame: &Frame, channel: u16, corr: u64) {
    assert_eq!(
        frame.header.channel, channel,
        "unexpected prompt: {frame:?}"
    );
    assert_eq!(frame.header.corr, corr);
    assert_eq!(frame.header.ty, FrameType::Response, "{frame:?}");
}

async fn expect_confirmation(stream: &mut TcpStream, method: &str, target: &str) -> Frame {
    let frame = next_frame(stream).await;
    assert_eq!(
        frame.header.channel, 0,
        "write dispatched without confirmation"
    );
    assert_eq!(frame.header.ty, FrameType::Request);
    let body: Value = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(body["op"], "operator.confirm");
    assert_eq!(body["kind"], "operation");
    assert_eq!(body["operation"], method);
    assert_eq!(body["route_channel"], 5);
    assert_eq!(body["route_epoch"], 1);
    let summary = body["summary"].as_str().unwrap();
    assert!(summary.contains(method), "{summary}");
    assert!(summary.contains(target), "{summary}");
    assert!(summary.chars().count() <= 200, "{summary}");
    frame
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_off_direct_write_is_unprompted() {
    let mut rig = ConfirmationRig::start(false, true).await;
    send(
        &mut rig.stream,
        request_frame(
            5,
            10,
            ops::OP_SUBSCRIPTION_SET,
            subscription_params("off-account"),
        ),
    )
    .await;
    assert_route_response(&next_frame(&mut rig.stream).await, 5, 10);
    assert_eq!(
        subscription_rows(&rig.state),
        vec![("off-account".into(), 100)]
    );
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_approved_dispatches_each_write_without_holding_writer() {
    let mut rig = ConfirmationRig::start(true, true).await;
    bind_as(
        &mut rig.stream,
        6,
        2,
        Principal::Reserved {
            module_id: "peer".into(),
        },
        "peer",
    )
    .await;
    for (corr, account) in [(10, "confirmed-a"), (11, "confirmed-b")] {
        send(
            &mut rig.stream,
            request_frame(
                5,
                corr,
                ops::OP_SUBSCRIPTION_SET,
                subscription_params(account),
            ),
        )
        .await;
        let prompt = expect_confirmation(&mut rig.stream, ops::OP_SUBSCRIPTION_SET, account).await;
        assert!(!subscription_rows(&rig.state)
            .iter()
            .any(|(key, _)| key == account));
        // A module write completes while the person's direct write is waiting.
        // It goes through the real lock that serializes ledger writes and the
        // real store transaction, so it shows the waiting write holds neither.
        let peer_account = format!("peer-{corr}");
        send(
            &mut rig.stream,
            request_frame(
                6,
                corr,
                ops::OP_SUBSCRIPTION_SET,
                subscription_params(&peer_account),
            ),
        )
        .await;
        assert_route_response(&next_frame(&mut rig.stream).await, 6, corr);
        assert!(subscription_rows(&rig.state).contains(&(peer_account, 100)));
        send(
            &mut rig.stream,
            route_frame(
                FrameType::Response,
                0,
                0,
                prompt.header.corr,
                serde_json::to_vec(&json!({"op": "operator.confirm", "outcome": "confirmed"}))
                    .unwrap(),
            ),
        )
        .await;
        assert_route_response(&next_frame(&mut rig.stream).await, 5, corr);
        assert!(subscription_rows(&rig.state).contains(&(account.into(), 100)));
    }
    assert_eq!(subscription_rows(&rig.state).len(), 4);
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_refusal_or_failure_leaves_store_unchanged() {
    let mut rig = ConfirmationRig::start(true, true).await;
    let before = store_snapshot(&rig.state);
    for (index, (code, detail)) in [
        ("operator_declined", Value::Null),
        (
            "operator_declined",
            json!({"reason": "backoff", "retry_after_ms": 5_000}),
        ),
        ("operator_presence_unavailable", Value::Null),
        (
            "operator_presence_unavailable",
            json!({"reason": "unsupported_platform"}),
        ),
        ("operator_summary_invalid", Value::Null),
        ("operator_request_not_permitted", Value::Null),
        ("unsupported_control_frame", Value::Null),
        ("unknown_failure", Value::Null),
        ("malformed_reply", Value::Null),
    ]
    .into_iter()
    .enumerate()
    {
        let corr = 10 + u64::try_from(index).unwrap();
        send(
            &mut rig.stream,
            request_frame(
                5,
                corr,
                ops::OP_SUBSCRIPTION_SET,
                subscription_params("refused-account"),
            ),
        )
        .await;
        let prompt =
            expect_confirmation(&mut rig.stream, ops::OP_SUBSCRIPTION_SET, "refused-account").await;
        let (ty, body) = if code == "malformed_reply" {
            (FrameType::Response, json!({"confirmed": "not a boolean"}))
        } else {
            (
                FrameType::Error,
                json!({"code": code, "message": "SECRET_DAEMON_DETAIL", "detail": detail}),
            )
        };
        send(
            &mut rig.stream,
            route_frame(
                ty,
                0,
                0,
                prompt.header.corr,
                serde_json::to_vec(&body).unwrap(),
            ),
        )
        .await;
        let reply = next_frame(&mut rig.stream).await;
        assert_eq!(reply.header.channel, 5);
        assert_eq!(reply.header.corr, corr);
        assert_eq!(refusal_code_of(&reply), "operator_confirmation_refused");
        let error: ErrorBody = serde_json::from_slice(&reply.body).unwrap();
        assert!(!error.message.contains("SECRET_DAEMON_DETAIL"));
        let reason = match code {
            "operator_declined" if detail.is_null() => "operator_declined",
            "operator_declined" => "backoff",
            "operator_presence_unavailable" if !detail.is_null() => "unsupported_platform",
            "operator_summary_invalid" => "operator_summary_invalid",
            "operator_request_not_permitted" => "not permitted",
            "unsupported_control_frame" => "does not support",
            _ => "operator_presence_unavailable",
        };
        assert!(error.message.contains(reason), "{error:?}");
        assert_eq!(store_snapshot(&rig.state), before, "{code}");
    }
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_module_write_never_prompts() {
    let mut rig = ConfirmationRig::start(true, true).await;
    bind_as(
        &mut rig.stream,
        6,
        2,
        Principal::Reserved {
            module_id: "peer".into(),
        },
        "peer",
    )
    .await;
    send(
        &mut rig.stream,
        request_frame(
            6,
            10,
            ops::OP_SUBSCRIPTION_SET,
            subscription_params("module-account"),
        ),
    )
    .await;
    assert_route_response(&next_frame(&mut rig.stream).await, 6, 10);

    assert_eq!(
        subscription_rows(&rig.state),
        vec![("module-account".into(), 100)]
    );
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_direct_read_never_prompts() {
    let mut rig = ConfirmationRig::start(true, true).await;
    let before = store_snapshot(&rig.state);
    send(
        &mut rig.stream,
        request_frame(5, 11, ops::OP_VERDICT, json!({})),
    )
    .await;
    let read = next_frame(&mut rig.stream).await;
    assert_route_response(&read, 5, 11);
    assert!(answer_of(&read).is_object());
    assert_eq!(store_snapshot(&rig.state), before);
    rig.finish().await;
}

#[test]
fn ledger_confirmation_manifest_declares_exactly_the_nine_writes() {
    let declaration = manifest(DEFAULT_MODULE_ID, &config::intentional_empty().config);
    let names = declaration
        .confirm_operations
        .expect("confirm_operations must be declared");
    assert_eq!(names.len(), 9);
    assert_eq!(
        names.into_iter().collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(
            [
                "spend.event.ingest",
                "spend.correction.ingest",
                "spend.chargeability.settle",
                "spend.statement.import",
                "spend.subscription.set",
                "spend.subscription.close",
                "spend.event.reprice",
                "spend.basis.correct",
                "spend.multiplier.recompute",
            ]
            .map(str::to_string)
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_all_nine_writes_name_their_targets_without_secrets() {
    let mut rig = ConfirmationRig::start(true, true).await;
    let before = store_snapshot(&rig.state);
    let cases = [
        (
            "spend.event.ingest",
            json!({"producer": "p", "producer_ref": "r", "charge_basis": "unknown", "auth_selection": "SECRET"}),
            "producer p, event r",
        ),
        (
            "spend.correction.ingest",
            json!({"correction_id": "c", "event_id": "e", "currency": "USD", "amount_nanos": 100, "reason": "refund", "occurred_at_ms": 1, "note": "SECRET"}),
            "event e",
        ),
        (
            "spend.chargeability.settle",
            json!({"event_id": "e", "to_state": "written_off", "reason": "SECRET"}),
            "event e",
        ),
        (
            "spend.statement.import",
            json!({"path": "SECRET", "provider": "provider", "account_ref": "account", "period_start": 1, "period_end": 2, "amount_exponent": 2, "currency": "USD"}),
            "provider provider, account account",
        ),
        (
            "spend.subscription.set",
            subscription_params("account"),
            "provider provider, account account",
        ),
        (
            "spend.subscription.close",
            json!({"provider": "provider", "account_ref": "account", "effective_to": 2}),
            "provider provider, account account",
        ),
        ("spend.event.reprice", json!({"event_id": "e"}), "event e"),
        (
            "spend.basis.correct",
            json!({"event_id": "e", "corrected_basis": "unknown", "reason": "SECRET"}),
            "event e",
        ),
        (
            "spend.multiplier.recompute",
            json!({"observation_day": "2026-07-10", "quota_provider": "provider"}),
            "day 2026-07-10, provider provider",
        ),
    ];
    for (index, (method, params, target)) in cases.into_iter().enumerate() {
        let corr = 10 + u64::try_from(index).unwrap();
        send(&mut rig.stream, request_frame(5, corr, method, params)).await;
        let prompt = expect_confirmation(&mut rig.stream, method, target).await;
        let body: Value = serde_json::from_slice(&prompt.body).unwrap();
        assert_eq!(body["summary"], format!("{method}: {target}"));
        assert!(!body["summary"].as_str().unwrap().contains("SECRET"));
        send(
            &mut rig.stream,
            route_frame(
                FrameType::Error,
                0,
                0,
                prompt.header.corr,
                serde_json::to_vec(&json!({"code": "operator_declined", "message": "declined"}))
                    .unwrap(),
            ),
        )
        .await;
        let reply = next_frame(&mut rig.stream).await;
        assert_eq!(reply.header.channel, 5);
        assert_eq!(reply.header.corr, corr);
        assert_eq!(refusal_code_of(&reply), "operator_confirmation_refused");
        assert_eq!(store_snapshot(&rig.state), before, "{method}");
    }
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_unavailable_daemon_refuses_without_a_write() {
    let mut rig = ConfirmationRig::start(true, false).await;
    let before = store_snapshot(&rig.state);
    send(
        &mut rig.stream,
        request_frame(
            5,
            10,
            ops::OP_SUBSCRIPTION_SET,
            subscription_params("unavailable"),
        ),
    )
    .await;
    let reply = next_frame(&mut rig.stream).await;
    assert_eq!(reply.header.channel, 5);
    assert_eq!(reply.header.corr, 10);
    assert_eq!(refusal_code_of(&reply), "operator_confirmation_refused");
    let error: ErrorBody = serde_json::from_slice(&reply.body).unwrap();
    assert!(error.message.contains("does not support"));
    assert_eq!(store_snapshot(&rig.state), before);
    rig.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ledger_confirmation_enabled_flow_write_keeps_existing_refusal() {
    let mut rig = ConfirmationRig::start(true, true).await;
    send(
        &mut rig.stream,
        control_request(2, bind_body(6, 1, Some("flow-a"))),
    )
    .await;
    assert_route_response(&next_frame(&mut rig.stream).await, 0, 2);
    let before = store_snapshot(&rig.state);
    send(
        &mut rig.stream,
        request_frame(
            6,
            10,
            ops::OP_SUBSCRIPTION_SET,
            subscription_params("flow-account"),
        ),
    )
    .await;
    let reply = next_frame(&mut rig.stream).await;
    assert_eq!(reply.header.channel, 6);
    assert_eq!(refusal_code_of(&reply), "flow_scopes_unsupported");
    assert_eq!(store_snapshot(&rig.state), before);
    rig.finish().await;
}

#[test]
fn ledger_confirmation_summary_bounds_unicode_and_preserves_both_target_parts() {
    let params =
        json!({"provider": format!("p\n{}", "供".repeat(200)), "account_ref": "賬".repeat(200)});
    let summary = ledger_write_summary(&RouteRequest {
        method: ops::OP_SUBSCRIPTION_SET.into(),
        params,
    });
    assert!(summary.starts_with("spend.subscription.set: provider p "));
    assert!(summary.contains(", account 賬"));
    assert!(summary.ends_with('…'));
    assert!(!summary.contains('\n'));
    assert!(summary.chars().count() <= 200);
    let all = ledger_write_summary(&RouteRequest {
        method: ops::OP_MULTIPLIER_RECOMPUTE.into(),
        params: json!({"observation_day": "2026-07-10"}),
    });
    assert_eq!(
        all,
        "spend.multiplier.recompute: day 2026-07-10, provider all"
    );
}

// This deliberate release tripwire pins production policy, not gate behaviour.
// Whoever enables confirmation must update this assertion in the same release
// commit; all serving fixtures inject their own policy independently.
#[test]
fn ledger_confirmation_release_tripwire_requires_gate_off_until_approved_release() {
    assert!(!ledger_write_confirmation_required());
}
