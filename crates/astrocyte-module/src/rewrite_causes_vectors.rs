//! Synthetic rewrite explanations served by the real feed and cache handlers.

use cortexkit_bus_naming::AccountNames;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::state::{test_state, SharedState};
use crate::step_feed::{record_steps_for_test, TestStep};

const AT: i64 = 4_000_000_000_000;
const SESSION: &str = "synthetic-rewrite-causes";
const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-vectors/usage-cache-causes-v1"
);

fn event(
    state: &SharedState,
    runner: &str,
    step: &str,
    at: i64,
    read: u64,
    edit: impl FnOnce(&mut Value),
) {
    let names = AccountNames::derive("box_feed").unwrap();
    let (subject, body) = crate::runner_steps::test_event(&names, "minimal", runner, |body| {
        body["harness_step_ref"] = json!(step);
        body["session"] = json!(SESSION);
        body["started_at_ms"] = json!(at);
        body["finished_at_ms"] = json!(at + 10);
        body["input_tokens"] = json!(10);
        body["cached_input_tokens"] = json!(read);
        body["provider"] = json!("anthropic");
        body["model"] = json!("model");
        body["account_ref"] = json!("a".repeat(64));
        body["tools_digest"] = json!("tools-v1");
        body["system_digest"] = json!("system-v1");
        body["view_version"] = json!(1);
        body["signed_floor"] = json!("floor-1");
        body["warm_up"] = json!(true);
        body["cache"] = json!({"ttl_ms": 1000});
        edit(body);
    });
    crate::runner_steps::admit(state, &subject, &body);
}

fn record(state: &SharedState, step: &str, at: i64, cache_read: u64) {
    record_steps_for_test(
        state,
        "pi",
        &[TestStep {
            session: SESSION,
            step_ref: step,
            occurred_at_ms: at,
            cache_read,
            output: None,
            reasoning: None,
            message_id: None,
        }],
        false,
    )
    .unwrap();
}

fn feed(state: &SharedState) -> Vec<Value> {
    crate::step_feed::read(state, json!({"harness":"pi", "session":SESSION}))
        .unwrap_or_else(|_| panic!("feed read refused"))
        .rows
}

fn row(state: &SharedState, step: &str) -> Value {
    feed(state)
        .into_iter()
        .find(|row| row["step_ref"] == step)
        .unwrap()
}

#[test]
fn rewrite_recorded_replacement_keeps_evidence_and_cause() {
    let state = test_state();
    event(&state, "broca", "before", AT, 100, |_| {});
    event(&state, "broca", "changed", AT + 100, 0, |body| {
        body["tools_digest"] = json!("tools-v2")
    });
    let before = row(&state, "changed");
    assert_eq!(before["cause"], "tools_or_system_changed");
    record(&state, "before", AT, 100);
    record(&state, "changed", AT + 100, 0);
    let after = row(&state, "changed");
    assert_eq!(after["source"], "file");
    for member in [
        "started_at_ms",
        "finished_at_ms",
        "account_ref",
        "warm_up",
        "view_version",
        "tools_digest",
        "system_digest",
        "signed_floor",
        "cache_ttl_ms",
        "cause",
    ] {
        assert_eq!(after.get(member), before.get(member), "{member}");
        assert!(after.get(member).is_some(), "{member}");
    }
    let cache = crate::ops::dispatch(
        &state,
        crate::ops::OP_CACHE_STEPS,
        json!({"harness":"pi", "session":SESSION}),
    )
    .unwrap();
    assert_eq!(cache["steps"][1]["cause"], "tools_or_system_changed");
    state
        .with_conn(|conn| {
            let digests: i64 = conn
                .query_row("SELECT COUNT(*) FROM step_digest", [], |row| row.get(0))
                .unwrap();
            assert_eq!(digests, 3);
            let copies: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM step_feed WHERE row_json LIKE '%tools-v%'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                copies, 0,
                "feed emissions must not duplicate interned digests"
            );
            let causes: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM step_cause WHERE step_ref='changed'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(causes, 1, "an unchanged explanation is not appended twice");
            Ok(())
        })
        .unwrap();
}

#[test]
fn rewrite_cause_survives_evidence_and_feed_pruning() {
    let state = test_state();
    event(&state, "broca", "before", AT, 100, |_| {});
    event(&state, "broca", "changed", AT + 100, 0, |body| {
        body["view_version"] = json!(2)
    });
    record(&state, "before", AT, 100);
    record(&state, "changed", AT + 100, 0);
    state
        .with_conn(|conn| {
            let latest: i64 = conn
                .query_row("SELECT MAX(recorded_at_ms) FROM step_evidence", [], |row| {
                    row.get(0)
                })
                .unwrap();
            astrocyte_core::step_feed::append_changes(
                conn,
                latest + astrocyte_core::rewrite_evidence::EVIDENCE_WINDOW_MS + 1,
                &[],
            )
            .unwrap();
            for table in ["step_evidence", "step_digest", "step_feed"] {
                let count: i64 = conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, 0, "{table}");
            }
            assert_eq!(
                astrocyte_core::rewrite_evidence::cause(conn, "pi", "changed")
                    .unwrap()
                    .unwrap()
                    .as_str(),
                "view_changed"
            );
            Ok(())
        })
        .unwrap();
    let cache = crate::ops::dispatch(
        &state,
        crate::ops::OP_CACHE_STEPS,
        json!({"harness":"pi", "session":SESSION}),
    )
    .unwrap();
    assert_eq!(cache["steps"][1]["cause"], "view_changed");
    state
        .with_conn(|conn| {
            let reply = astrocyte_core::cache_diagnostics::steps_from_store(
                conn,
                &astrocyte_core::cache_diagnostics::StepsQuery {
                    producer: "pi".into(),
                    session: SESSION.into(),
                    limit: 200,
                    after_cursor: None,
                },
            )
            .unwrap();
            assert_eq!(
                reply.steps[1].cause.as_ref().unwrap().as_str(),
                "view_changed"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn rewrite_previous_step_is_scoped_to_runner_even_with_shared_host_session() {
    let state = test_state();
    event(&state, "broca", "other-runner", AT, 100, |body| {
        body["tools_digest"] = json!("other-tools")
    });
    event(&state, "thalamus", "no-prev", AT + 100, 1, |_| {});
    assert_eq!(row(&state, "no-prev")["cause"], "unexplained");
    event(&state, "broca", "same-runner", AT + 200, 0, |_| {});
    assert_eq!(
        row(&state, "same-runner")["cause"],
        "tools_or_system_changed"
    );
}

#[test]
fn rewrite_late_step_recomputes_its_successor() {
    let state = test_state();
    event(&state, "broca", "first", AT, 100, |_| {});
    event(&state, "broca", "last", AT + 200, 0, |body| {
        body["view_version"] = json!(2)
    });
    assert_eq!(row(&state, "last")["cause"], "view_changed");
    let cursor = row(&state, "last")["cursor"].clone();
    event(&state, "broca", "middle", AT + 100, 100, |body| {
        body["view_version"] = json!(2)
    });
    let after = row(&state, "last");
    assert_eq!(after["cause"], "unexplained");
    assert_ne!(
        after["cursor"], cursor,
        "cause corrections notify feed readers"
    );
}

#[test]
fn rewrite_event_after_durable_read_still_supplies_evidence() {
    let state = test_state();
    record(&state, "before", AT, 100);
    record(&state, "changed", AT + 100, 0);
    event(&state, "broca", "before", AT, 100, |_| {});
    event(&state, "broca", "changed", AT + 100, 0, |body| {
        body["system_digest"] = json!("system-v2")
    });
    let after = row(&state, "changed");
    assert_eq!(after["source"], "file");
    assert_eq!(after["cause"], "tools_or_system_changed");
    assert_eq!(after["system_digest"], "system-v2");
}

#[test]
fn rewrite_cause_corrections_append_and_can_clear_the_explanation() {
    let state = test_state();
    event(&state, "broca", "before", AT, 100, |_| {});
    event(&state, "broca", "changed", AT + 100, 0, |body| {
        body["view_version"] = json!(2);
    });
    assert_eq!(row(&state, "changed")["cause"], "view_changed");
    event(&state, "broca", "changed", AT + 100, 200, |body| {
        body["view_version"] = json!(2);
    });
    assert!(row(&state, "changed").get("cause").is_none());
    state
        .with_conn(|conn| {
            let history = conn
                .prepare("SELECT cause FROM step_cause WHERE step_ref='changed' ORDER BY cause_key")
                .unwrap()
                .query_map([], |row| row.get::<_, Option<String>>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(history, [Some("view_changed".into()), None]);
            Ok(())
        })
        .unwrap();
}

#[test]
fn rewrite_file_steps_do_not_compare_to_a_known_runner() {
    let state = test_state();
    record(&state, "unknown-before", AT - 100, 100);
    event(&state, "broca", "known", AT, 100, |body| {
        body["model"] = json!("other-model")
    });
    record(&state, "file-only", AT + 100, 1);
    assert_eq!(row(&state, "file-only")["cause"], "unexplained");
}

#[test]
fn rewrite_late_runner_identity_corrects_the_unknown_chain_it_left() {
    let state = test_state();
    record(&state, "unknown-before", AT, 100);
    let request: astrocyte_core::ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"pi", "producer_ref":"moving", "session":SESSION,
        "occurred_at_ms":AT+100, "provider":"anthropic", "model":"other-model",
        "charge_basis":"unknown", "measurements":[
            {"role":"input", "unit":"token", "kind":"billed", "source":"usage.input", "quantity_units":10,"quantity_exponent":0},
            {"role":"cache_read", "unit":"token", "kind":"billed", "source":"usage.cache_read", "quantity_units":100,"quantity_exponent":0}
        ]
    })).unwrap();
    state
        .ordered_write(|tx| {
            astrocyte_core::ingest::record_event_in_tx(
                tx,
                &request,
                1,
                astrocyte_core::ingest::DeliveryPath::Read,
            )
            .map_err(|error| format!("{error:?}"))?;
            Ok(((), true))
        })
        .unwrap();
    record(&state, "unknown-after", AT + 200, 1);
    assert_eq!(
        row(&state, "unknown-after")["cause"],
        "account_or_model_changed"
    );
    let before = row(&state, "unknown-after")["cursor"].clone();
    event(&state, "broca", "moving", AT + 100, 100, |body| {
        body["model"] = json!("other-model")
    });
    let after = row(&state, "unknown-after");
    assert_eq!(after["cause"], "unexplained");
    assert_ne!(after["cursor"], before);
}

#[test]
fn rewrite_causes_published_vectors_match_real_handlers() {
    let state = test_state();
    let causes = [
        "account_or_model_changed",
        "cache_expired",
        "tools_or_system_changed",
        "view_changed",
        "signed_floor_moved",
        "unexplained",
        "none",
    ];
    for (index, expected) in causes.iter().enumerate() {
        let at = AT + i64::try_from(index).unwrap() * 10_000;
        let before = format!("baseline-{index}");
        let step = format!("cause-{expected}");
        event(&state, "broca", &before, at, 100, |_| {});
        let at = at
            + if *expected == "cache_expired" {
                2_000
            } else {
                100
            };
        let read = if *expected == "none" { 200 } else { 0 };
        event(&state, "broca", &step, at, read, |body| match *expected {
            "account_or_model_changed" => body["account_ref"] = json!("b".repeat(64)),
            "tools_or_system_changed" => body["tools_digest"] = json!("tools-v2"),
            "view_changed" => body["view_version"] = json!(2),
            "signed_floor_moved" => body["signed_floor"] = json!("floor-2"),
            _ => {}
        });
        record(
            &state,
            &before,
            AT + i64::try_from(index).unwrap() * 10_000,
            100,
        );
        record(&state, &step, at, read);
        let served = row(&state, &step);
        if *expected == "none" {
            assert!(served.get("cause").is_none());
        } else {
            assert_eq!(served["cause"], *expected);
        }
    }
    let params = json!({"harness":"pi", "session":SESSION});
    let cache = crate::ops::dispatch(&state, crate::ops::OP_CACHE_STEPS, params.clone()).unwrap();
    for expected in &causes {
        let step_ref = format!("cause-{expected}");
        let served = cache["steps"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["step_ref"] == step_ref)
            .unwrap();
        if *expected == "none" {
            assert!(served.get("cause").is_none());
        } else {
            assert_eq!(served["cause"], *expected);
        }
    }
    let feed =
        crate::step_feed::read(&state, params).unwrap_or_else(|_| panic!("feed read refused"));
    for row in &feed.rows {
        cortexkit_usage_steps::StepRow::check_closed(row).unwrap();
        let wire: cortexkit_usage_steps::StepRow = serde_json::from_value(row.clone()).unwrap();
        wire.check_counts().unwrap();
    }
    let frames = [
        ("reply-cache-steps.json", json!({"result":cache})),
        ("reply-feed-read.json", json!({"result":feed})),
    ];
    let mut sums = String::new();
    let mut table = String::new();
    let regenerate = std::env::var_os("ASTROCYTE_REGENERATE_CACHE_CAUSES").is_some();
    if regenerate {
        std::fs::create_dir_all(DIR).unwrap();
    }
    for (file, frame) in frames {
        let mut bytes = serde_json::to_vec_pretty(&frame).unwrap();
        bytes.push(b'\n');
        let hash = format!("{:x}", Sha256::digest(&bytes));
        sums.push_str(&format!("{hash}  {file}\n"));
        table.push_str(&format!("| `{file}` | `{hash}` |\n"));
        let path = std::path::Path::new(DIR).join(file);
        if regenerate {
            std::fs::write(path, bytes).unwrap();
        } else {
            assert_eq!(std::fs::read(path).unwrap(), bytes, "{file}");
        }
    }
    let readme = format!(
        "# Usage cache causes v1\n\n\
Synthetic, content-free samples from astrocyte's real `usage.cache.steps` and\n\
`usage.steps.read` handlers using llm-runner 0.8.55 and usage-steps 0.2.8.\n\
The set is tagged `usage-cache-causes-v1`; published bytes are immutable.\n\
Each reply covers account/model change, cache expiry, tools/system change,\n\
applied view change, signed-floor movement, unexplained, and no cause.\n\
Warm-ups are included; file rows have replaced their runner hints.\n\
All identities, timestamps, digests and counts are invented. No store or user\n\
data is included. Feed cursors and cache cursors are separate opaque sequences.\n\n\
Regenerate an unpublished set with `ASTROCYTE_REGENERATE_CACHE_CAUSES=1 cargo test\n\
--workspace --locked rewrite_causes_published_vectors_match_real_handlers`.\n\
The ordinary test byte-compares both responses, SHA256SUMS and this table.\n\n\
| File | SHA-256 |\n| --- | --- |\n{table}"
    );
    for (file, bytes) in [("SHA256SUMS", sums), ("README.md", readme)] {
        let path = std::path::Path::new(DIR).join(file);
        if regenerate {
            std::fs::write(path, bytes).unwrap();
        } else {
            assert_eq!(std::fs::read_to_string(path).unwrap(), bytes, "{file}");
        }
    }
}
