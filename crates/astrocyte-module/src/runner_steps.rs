//! Runner step events: the `step.finished.v1` hints agent runners publish on
//! the bus's `live` root once a model step is durably recorded.
//!
//! Each event becomes a `source: "runner"` row in the live step feed (see
//! `astrocyte_core::step_feed::append_runner_step`), so the phone and the
//! dashboard see a step before astrocyte's own read of a durable store does;
//! that read later replaces the row. The events are display data only and
//! never reach the ledger. An event that does not decode is counted and
//! dropped. While the bus is unavailable no new runner rows arrive; runner
//! rows already appended stay served until a feed append prunes them.
//!
//! The subscriptions share the quota reader's NATS connection and credential
//! (see `quota_events::Reader`), because a fresh ckbus credential revokes the
//! module's previous one.

use std::time::Duration;

use astrocyte_core::cache_diagnostics::StepRow;
use astrocyte_core::step_feed::RunnerStep;
use cortexkit_bus_naming::{AccountNames, EVENT_CONSUMERS};
use cortexkit_bus_nats::NatsConnection;
use cortexkit_bus_trait::BusError;
use cortexkit_role_llm_runner::step_events::{decode_step_finished, RefusedAttempt, StepFinished};
use futures_util::StreamExt;

use crate::state::SharedState;
use crate::step_feed::RunnerFailure;

/// The live event name and version runners publish finished steps under.
const STEP_EVENT: &str = "step.finished";
const STEP_EVENT_VERSION: u32 = 1;
/// The longest wait between attempts to subscribe after a refusal.
const MAX_RETRY: Duration = Duration::from_secs(30);

/// The live subjects astrocyte follows, one per runner. The runners are the
/// ones the bus naming table lets astrocyte read `step.finished.v1` from
/// (`EVENT_CONSUMERS`, filters `event.<runner>.step.finished.v1`); the bus
/// grants astrocyte exactly those runners' live subjects and refuses a
/// wildcard subscription.
pub(crate) fn subjects(names: &AccountNames) -> Result<Vec<String>, &'static str> {
    let entry = EVENT_CONSUMERS
        .iter()
        .find(|entry| entry.module_id == "astrocyte")
        .ok_or("astrocyte is not in the bus naming table")?;
    let suffix = format!(".{STEP_EVENT}.v{STEP_EVENT_VERSION}");
    let subjects = entry
        .filter_subjects
        .iter()
        .filter_map(|filter| filter.strip_prefix("event.")?.strip_suffix(suffix.as_str()))
        .map(|runner| {
            names
                .live_subject(runner, STEP_EVENT, STEP_EVENT_VERSION)
                .map_err(|_| "invalid runner live subject")
        })
        .collect::<Result<Vec<_>, _>>()?;
    if subjects.is_empty() {
        return Err("the bus naming table names no runner step events for astrocyte");
    }
    Ok(subjects)
}

/// The feed step for a decoded event, or `None` when it cannot be served:
/// no row identity (the runner contract refuses to publish such an event)
/// or a time outside the store's range.
pub(crate) fn feed_step(event: StepFinished) -> Option<RunnerStep> {
    let (harness, step_ref) = cortexkit_usage_steps::live_row_identity(
        &event.runner,
        &event.harness,
        event.harness_step_ref.as_deref(),
        event.attempt_id.as_deref(),
    )?;
    // A step's time is when it started, as for a recorded step (OpenCode's
    // message creation time, for example). The file row that later replaces
    // this one then sorts at the same place in the feed.
    let occurred_at_ms = i64::try_from(event.started_at_ms).ok()?;
    let refused_attempts = event
        .fallback
        .as_ref()
        .and_then(|fallback| fallback.refused_attempts.as_deref())
        .unwrap_or_default();
    let answering = |total: Option<u64>, count: fn(&RefusedAttempt) -> Option<u64>| {
        let total = total?;
        // An omitted attempt counter makes the answering count unknown. Sum
        // in u128 so even inconsistent u64 counters cannot overflow the sum.
        let refused = refused_attempts.iter().try_fold(0_u128, |sum, attempt| {
            Some(sum + u128::from(count(attempt)?))
        })?;
        // If the refused usage alone exceeds the reported step total, the
        // producer's counters are inconsistent. Keep the original total.
        Some(
            u64::try_from(refused)
                .ok()
                .and_then(|refused| total.checked_sub(refused))
                .unwrap_or(total),
        )
    };
    let output = answering(event.output_tokens, |attempt| attempt.output_tokens);
    let reasoning = answering(event.reasoning_tokens, |attempt| attempt.reasoning_tokens);
    // The ledger stores output without reasoning and the served row adds
    // reasoning back (`served_output`). An event that does not say reasoning
    // is outside output is read as including it, the feed's own convention;
    // a reasoning count above such an output cannot be a breakdown of it and
    // is left out rather than served as one.
    let (output, reasoning) = match (output, reasoning) {
        (Some(output), Some(reasoning)) if event.reasoning_included_in_output != Some(false) => {
            match output.checked_sub(reasoning) {
                Some(rest) => (Some(rest), Some(reasoning)),
                None => (Some(output), None),
            }
        }
        counts => counts,
    };
    Some(RunnerStep {
        runner: event.runner,
        evidence: astrocyte_core::rewrite_evidence::Evidence {
            started_at_ms: Some(event.started_at_ms),
            finished_at_ms: Some(event.finished_at_ms),
            account_ref: event.account_ref,
            warm_up: event.warm_up,
            view_version: event.view_version,
            tools_digest: event.tools_digest,
            system_digest: event.system_digest,
            signed_floor: event.signed_floor,
            cache_ttl_ms: event.cache.as_ref().and_then(|cache| cache.ttl_ms),
            cache_implicit: event.cache.as_ref().and_then(|cache| cache.implicit),
        },
        harness,
        session: event.session,
        run_id: event.run_id,
        step: StepRow {
            // Unused: a runner step has no cache cursor.
            cursor: i64::MAX,
            step_ref,
            occurred_at_ms,
            input: answering(event.input_tokens, |attempt| attempt.input_tokens),
            cache_read: answering(event.cached_input_tokens, |attempt| {
                attempt.cached_input_tokens
            }),
            cache_write: answering(event.cache_write_tokens, |attempt| {
                attempt.cache_write_tokens
            }),
            output,
            reasoning,
            provider: Some(event.provider),
            model: Some(event.model),
            // As for a recorded step: the finish reason, else the status.
            finish: event.finish_reason.or(Some(event.terminal_status)),
            context_limit: event
                .context_limit
                .and_then(|limit| i64::try_from(limit).ok()),
            request_body_len: event.request_body_bytes,
        },
        cache_write_5m: answering(event.cache_write_5m_tokens, |attempt| {
            attempt.cache_write_5m_tokens
        }),
        cache_write_1h: answering(event.cache_write_1h_tokens, |attempt| {
            attempt.cache_write_1h_tokens
        }),
        // Served only as the runner sent it: a step id is not a message id.
        message_id: event.message_id,
    })
}

/// Decode one received event and add it to the feed, counting the outcome
/// in `FeedHub::runner_counts`. Never fails: a bad event is dropped.
pub(crate) fn admit(state: &SharedState, subject: &str, body: &[u8]) {
    let outcome = match decode_step_finished(subject, body).ok().and_then(feed_step) {
        None => Err(RunnerFailure::Dropped),
        Some(step) => state.publish_runner_step(&step).map_err(|error| {
            tracing::warn!(target: "astrocyte", %error, "runner step not added to the live feed");
            RunnerFailure::Failed
        }),
    };
    state.step_feed().count_runner(outcome);
}

/// Follow runners' step events on `connection` until it closes. A refused or
/// failed subscription is logged once per distinct reason and retried with
/// backoff on the same connection, so it never interrupts the quota reader
/// sharing that connection.
pub(crate) async fn follow(state: &SharedState, connection: &NatsConnection, names: &AccountNames) {
    let mut delay = Duration::from_secs(1);
    let mut logged: Option<String> = None;
    loop {
        match subscribe(connection, names).await {
            Ok(subscribers) => {
                tracing::info!(target: "astrocyte", "following runner step events");
                logged = None;
                delay = Duration::from_secs(1);
                let mut messages = futures_util::stream::select_all(subscribers);
                while let Some(message) = messages.next().await {
                    admit(state, message.subject.as_str(), &message.payload);
                }
            }
            Err(reason) => {
                if logged.as_deref() != Some(reason.as_str()) {
                    tracing::warn!(target: "astrocyte", %reason,
                        "runner step events unavailable; the live feed carries file rows only");
                    logged = Some(reason);
                }
            }
        }
        if connection.client().connection_state() == async_nats::connection::State::Disconnected {
            // The session that owns the connection reconnects.
            return;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_RETRY);
    }
}

async fn subscribe(
    connection: &NatsConnection,
    names: &AccountNames,
) -> Result<Vec<async_nats::Subscriber>, String> {
    let mut subscribers = Vec::new();
    for subject in subjects(names)? {
        subscribers.push(
            connection
                .subscribe(&subject)
                .await
                .map_err(|error| match error {
                    BusError::Denied { subject, .. } => {
                        format!("subscription to {subject} refused")
                    }
                    other => format!("subscription to {subject} failed: {other:?}"),
                })?,
        );
    }
    Ok(subscribers)
}

/// A `step-events.json` fixture from the runner crate, made into a
/// just-finished event on `runner`'s live subject, with `edit` applied to its
/// body. Nothing is published.
#[cfg(test)]
pub(crate) fn test_event(
    names: &AccountNames,
    vector: &str,
    runner: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) -> (String, Vec<u8>) {
    let vectors: serde_json::Value =
        serde_json::from_str(cortexkit_role_llm_runner::vectors::file("step-events.json").unwrap())
            .unwrap();
    let mut body = vectors[vector].clone();
    let now = u64::try_from(crate::state::now_ms()).unwrap();
    body["runner"] = serde_json::json!(runner);
    body["harness"] = serde_json::json!("pi");
    body["started_at_ms"] = serde_json::json!(now - 1_000);
    body["finished_at_ms"] = serde_json::json!(now);
    edit(&mut body);
    let subject = names
        .live_subject(runner, STEP_EVENT, STEP_EVENT_VERSION)
        .unwrap();
    (subject, serde_json::to_vec(&body).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{now_ms, test_state};
    use crate::step_feed::{read, record_steps_for_test, RunnerCounts, TestStep};
    use cortexkit_usage_steps::{StepRow as WireRow, StepSource};
    use serde_json::{json, Value};

    const RUNNERS: [&str; 3] = ["broca", "thalamus-harness", "thalamus"];

    #[test]
    fn refusal_fallback_row_uses_answering_attempt_counters() {
        let (subject, body) = event("refusal_fallback", "broca", |body| {
            // Keep only the attempt with complete counters in this case.
            body["fallback"]["refused_attempts"]
                .as_array_mut()
                .unwrap()
                .truncate(1);
        });
        let fallback = decode_step_finished(&subject, &body).expect("decode refusal fallback step");
        let state = test_state();
        admit_event(
            &state,
            &event("minimal", "broca", |previous| {
                previous["started_at_ms"] = json!(fallback.started_at_ms - 1_000);
                previous["finished_at_ms"] = json!(fallback.started_at_ms);
                // A baseline of 50 would be stable against the combined
                // counters, but the answer alone retains only 45 tokens.
                previous["cached_input_tokens"] = json!(49);
                previous["cache_write_tokens"] = json!(1);
            }),
        );
        admit(&state, &subject, &body);
        let served = rows(&state, json!({}));
        assert_eq!(served.len(), 2);
        let row = &served[1];
        assert_eq!(row["source"], "runner");
        assert_eq!(row["model"], "model_b");
        assert_eq!(row["input"], 88);
        assert_eq!(row["cache_read"], 45);
        assert_eq!(row["cache_write"], 27);
        assert_eq!(row["cache_write_5m"], 9);
        assert_eq!(row["cache_write_1h"], 18);
        assert_eq!(row["output"], 18);
        assert_eq!(row["reasoning"], 7);
        assert_eq!(row["prompt"], 160);
        assert_eq!(row["verdict"], "warning");
        assert_eq!(row["cause"], "account_or_model_changed");
    }

    #[test]
    fn refusal_fallback_without_category_is_admitted() {
        // Since llm-runner 0.9.9 a provider may give no refusal category; the
        // record then omits it, and the step must still reach the feed.
        let (subject, body) = event("refusal_fallback", "broca", |body| {
            body["fallback"].as_object_mut().unwrap().remove("category");
        });
        decode_step_finished(&subject, &body).expect("decode fallback without category");
        let state = test_state();
        admit(&state, &subject, &body);
        let served = rows(&state, json!({}));
        assert_eq!(served.len(), 1);
        assert_eq!(served[0]["source"], "runner");
    }

    #[test]
    fn refusal_fallback_row_omits_unknown_counters() {
        let state = test_state();
        admit_event(
            &state,
            &event("refusal_fallback", "broca", |body| {
                let second = &mut body["fallback"]["refused_attempts"][1];
                second["cached_input_tokens"] = json!(10);
                second["cache_write_1h_tokens"] = json!(0);
                second["output_tokens"] = json!(1);
                body.as_object_mut()
                    .unwrap()
                    .remove("cache_write_1h_tokens");
            }),
        );
        let served = rows(&state, json!({}));
        let row = &served[0];
        for unknown in ["input", "cache_write", "cache_write_1h", "reasoning"] {
            assert_eq!(row.get(unknown), None, "{unknown}");
        }
        assert_eq!(row["cache_read"], 35);
        assert_eq!(row["output"], 17);
    }

    #[test]
    fn refusal_fallback_row_keeps_inconsistent_step_totals() {
        let state = test_state();
        admit_event(
            &state,
            &event("refusal_fallback", "broca", |body| {
                let second = &mut body["fallback"]["refused_attempts"][1];
                second["input_tokens"] = json!(89);
                second["cached_input_tokens"] = json!(u64::MAX);
            }),
        );
        let served = rows(&state, json!({}));
        assert_eq!(served[0]["input"], 100);
        assert_eq!(served[0]["cache_read"], 50);
    }

    #[test]
    fn refusal_fallback_round_row_keeps_step_counters() {
        let state = test_state();
        admit_event(
            &state,
            &event("refusal_fallback", "broca", |body| {
                let fallback = body["fallback"].as_object_mut().unwrap();
                fallback.insert("cause".into(), json!("round"));
                fallback.remove("refused_attempts");
            }),
        );
        let served = rows(&state, json!({}));
        let row = &served[0];
        assert_eq!(row["input"], 100);
        assert_eq!(row["cache_read"], 50);
        assert_eq!(row["cache_write"], 30);
        assert_eq!(row["cache_write_5m"], 10);
        assert_eq!(row["cache_write_1h"], 20);
        assert_eq!(row["output"], 20);
        assert_eq!(row["reasoning"], 7);
    }

    #[test]
    fn rewrite_facts_0855_decode_with_every_new_member_and_without_them() {
        for with_facts in [false, true] {
            let (subject, body) = event("minimal", "broca", |body| {
                if with_facts {
                    body["account_ref"] = json!("a".repeat(64));
                    body["warm_up"] = json!(true);
                    body["view_version"] = json!(0);
                    body["tools_digest"] = json!("tools-v1");
                    body["system_digest"] = json!("system-v1");
                    body["signed_floor"] = json!("entry-1");
                    body["cache"] = json!({"implicit":true});
                }
            });
            let state = test_state();
            admit(&state, &subject, &body);
            assert_eq!(
                state.step_feed().runner_counts().appended,
                1,
                "with_facts={with_facts}"
            );
            let served = rows(&state, json!({}));
            assert_eq!(served.len(), 1);
            let sent: Value = serde_json::from_slice(&body).unwrap();
            for member in [
                "account_ref",
                "warm_up",
                "view_version",
                "tools_digest",
                "system_digest",
                "signed_floor",
            ] {
                assert_eq!(served[0].get(member), sent.get(member), "{member}");
            }
            assert_eq!(served[0].get("started_at_ms"), sent.get("started_at_ms"));
            assert_eq!(served[0].get("finished_at_ms"), sent.get("finished_at_ms"));
            assert_eq!(
                served[0].get("cache_implicit"),
                if with_facts {
                    Some(&Value::Bool(true))
                } else {
                    None
                }
            );
            assert!(served[0].get("cache_ttl_ms").is_none());
        }
    }

    fn names() -> AccountNames {
        AccountNames::derive("box_feed").unwrap()
    }

    fn event(vector: &str, runner: &str, edit: impl FnOnce(&mut Value)) -> (String, Vec<u8>) {
        test_event(&names(), vector, runner, edit)
    }

    fn admit_event(state: &SharedState, (subject, body): &(String, Vec<u8>)) {
        admit(state, subject, body);
    }

    /// The rows `usage.steps.read` serves, each checked against the
    /// published wire contract.
    fn rows(state: &SharedState, params: Value) -> Vec<Value> {
        let Ok(page) = read(state, params) else {
            panic!("steps read refused");
        };
        for row in &page.rows {
            WireRow::check_closed(row).unwrap();
            let wire: WireRow = serde_json::from_value(row.clone()).unwrap();
            wire.check_counts().unwrap();
        }
        page.rows
    }

    fn feed_rows(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM step_feed", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// The subjects come from the naming table's runner filters, one plain
    /// subject per runner and no wildcard, and each is a subject the runner
    /// contract accepts for that runner's events.
    #[test]
    fn astrocyte_follows_each_granted_runners_live_subject() {
        let subjects = subjects(&names()).unwrap();
        assert_eq!(
            subjects,
            [
                "ck.box_feed.live.broca.step.finished.v1",
                "ck.box_feed.live.thalamus-harness.step.finished.v1",
                "ck.box_feed.live.thalamus.step.finished.v1",
            ]
        );
        for (subject, runner) in subjects.iter().zip(RUNNERS) {
            let (_, body) = event("minimal", runner, |_| {});
            assert_eq!(decode_step_finished(subject, &body).unwrap().runner, runner);
        }
    }

    /// A decoded event from each runner is served as a `runner` row under the
    /// contract's identity: the runner and its attempt id, or the host
    /// harness and its record id.
    #[test]
    fn each_runners_event_is_served_as_a_runner_row() {
        let state = test_state();
        for runner in RUNNERS {
            admit_event(&state, &event("minimal", runner, |_| {}));
        }
        let full_event = event("full", "broca", |_| {});
        admit_event(&state, &full_event);
        let rows = rows(&state, json!({}));
        let identities: Vec<_> = rows
            .iter()
            .map(|row| {
                (
                    row["harness"].as_str().unwrap(),
                    row["step_ref"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            identities,
            [
                ("broca", "attempt_invented_b"),
                ("thalamus-harness", "attempt_invented_b"),
                ("thalamus", "attempt_invented_b"),
                ("pi", "msg_record_a"),
            ]
        );
        for row in &rows {
            let wire: WireRow = serde_json::from_value(row.clone()).unwrap();
            assert_eq!(wire.source, StepSource::Runner);
            assert_eq!(wire.session, "session_a");
            assert_eq!(wire.run_id.as_deref(), Some("run_a"));
            wire.check_counts().unwrap();
            assert_eq!(wire.message_id, None);
            assert!(
                row.get("message_id").is_none(),
                "runner ids are not host message ids"
            );
        }
        let mut full = rows[3].clone();
        full.as_object_mut().unwrap().remove("cursor");
        let occurred = full
            .as_object_mut()
            .unwrap()
            .remove("occurred_at_ms")
            .unwrap();
        let sent: Value = serde_json::from_slice(&full_event.1).unwrap();
        // The row's time is the event's start, not its finish a second later.
        assert_eq!(occurred, sent["started_at_ms"]);
        assert_ne!(occurred, sent["finished_at_ms"]);
        assert_eq!(
            full,
            json!({
                "harness": "pi", "source": "runner", "step_ref": "msg_record_a",
                "session": "session_a", "run_id": "run_a", "provider": "provider_a",
                "model": "model_a", "input": 100, "cache_read": 50, "cache_write": 30,
                "cache_write_5m": 10, "cache_write_1h": 20, "output": 20, "reasoning": 7,
                "finish": "future_finish_reason", "context_limit": 200_000,
                "request_body_len": 4096, "prompt": 180, "verdict": "cold_start",
                "prompt_dropped": false,
                "started_at_ms": sent["started_at_ms"], "finished_at_ms": sent["finished_at_ms"]
            })
        );
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                appended: 4,
                ..RunnerCounts::default()
            }
        );
    }

    /// The durable read of the same step (matching `harness_step_ref`)
    /// replaces the runner row: the feed serves only the file row.
    #[test]
    fn a_file_row_for_the_step_supersedes_the_runner_row() {
        let state = test_state();
        admit_event(
            &state,
            &event("full", "thalamus-harness", |body| {
                body["harness"] = json!("pi");
                body["session"] = json!("s1");
                body["harness_step_ref"] = json!("msg_x");
            }),
        );
        let runner = rows(&state, json!({"harness": "pi", "session": "s1"}));
        assert_eq!(runner.len(), 1);
        assert_eq!(runner[0]["source"], "runner");
        record_steps_for_test(&state, "pi", &[TestStep::now("s1", "msg_x")], false).unwrap();
        let served = rows(&state, json!({"harness": "pi", "session": "s1"}));
        assert_eq!(served.len(), 1, "{served:?}");
        assert_eq!(served[0]["source"], "file");
        assert_eq!(served[0]["step_ref"], "msg_x");
        assert_eq!(feed_rows(&state), 2);
    }

    /// A late runner event enriches the durable row with event-only evidence,
    /// without replacing its authoritative counts. Redeliveries append nothing.
    #[test]
    fn a_runner_event_after_the_file_row_leaves_the_file_row_newest() {
        let state = test_state();
        record_steps_for_test(&state, "pi", &[TestStep::now("s1", "msg_x")], false).unwrap();
        let late = event("full", "thalamus-harness", |body| {
            body["harness"] = json!("pi");
            body["session"] = json!("s1");
            body["harness_step_ref"] = json!("msg_x");
        });
        admit_event(&state, &late);
        let served = rows(&state, json!({}));
        assert_eq!(served.len(), 1);
        assert_eq!(served[0]["source"], "file");
        assert_eq!(feed_rows(&state), 2);
        assert!(served[0].get("finished_at_ms").is_some());
        admit_event(&state, &late);
        assert_eq!(feed_rows(&state), 2);
        let repeated = event("minimal", "broca", |_| {});
        admit_event(&state, &repeated);
        admit_event(&state, &repeated);
        assert_eq!(feed_rows(&state), 3);
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                appended: 2,
                unchanged: 1,
                superseded: 1,
                ..RunnerCounts::default()
            }
        );
    }

    #[test]
    fn audit18_file_first_positive_revises_an_earlier_runner_row() {
        let state = test_state();
        admit_event(
            &state,
            &event("minimal", "thalamus-harness", |body| {
                body["harness"] = json!("opencode");
                body["session"] = json!("s1");
                body["harness_step_ref"] = json!("msg_runner");
                body["input_tokens"] = json!(10);
                body["cached_input_tokens"] = json!(0);
            }),
        );
        let read = || rows(&state, json!({"harness": "opencode", "session": "s1"}));
        let initial = read();
        assert_eq!(initial[0]["verdict"], "unknown");
        let at = initial[0]["occurred_at_ms"].as_i64().unwrap();
        record_steps_for_test(
            &state,
            "opencode",
            &[TestStep {
                occurred_at_ms: at + 1,
                ..TestStep::now("s1", "msg_file")
            }],
            false,
        )
        .unwrap();
        let served = read();
        assert_eq!(served.len(), 2);
        assert!(served
            .iter()
            .any(|row| row["source"] == "file" && row["cache_read"] == 100));
        let runner = served
            .iter()
            .find(|row| row["step_ref"] == "msg_runner")
            .unwrap();
        assert_eq!(runner["source"], "runner");
        assert_eq!(runner["cache_read"], 0);
        assert_ne!(runner["cursor"], initial[0]["cursor"]);
        assert_eq!(runner["verdict"], "cold_start");
    }

    #[test]
    fn opencode2_runner_identity_joins_the_recorded_opencode_row() {
        let state = test_state();
        let announced = event("full", "thalamus", |body| {
            body["harness"] = json!("opencode2");
            body["session"] = json!("s1");
            body["harness_step_ref"] = json!("msg_x");
        });
        admit_event(&state, &announced);
        let runner = rows(&state, json!({}));
        assert_eq!(runner.len(), 1);
        assert_eq!(runner[0]["source"], "runner");

        record_steps_for_test(&state, "opencode", &[TestStep::now("s1", "msg_x")], false).unwrap();
        let served = rows(&state, json!({}));
        assert_eq!(served.len(), 1, "{served:?}");
        assert_eq!(served[0]["harness"], "opencode");
        assert_eq!(served[0]["step_ref"], "msg_x");
        assert_eq!(served[0]["source"], "file");
        assert_eq!(feed_rows(&state), 2);

        admit_event(&state, &announced);
        let newest = rows(&state, json!({}));
        assert_eq!(newest, served);
        assert_eq!(feed_rows(&state), 2);
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                appended: 1,
                superseded: 1,
                ..RunnerCounts::default()
            }
        );
    }

    #[test]
    fn unknown_host_runner_identity_is_counted_and_dropped() {
        let state = test_state();
        admit_event(
            &state,
            &event("full", "thalamus", |body| {
                body["harness"] = json!("unknown-host");
                body["harness_step_ref"] = json!("msg_x");
            }),
        );
        assert!(rows(&state, json!({})).is_empty());
        assert_eq!(feed_rows(&state), 0);
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                dropped: 1,
                ..RunnerCounts::default()
            }
        );
    }

    /// An event that does not decode, names another publisher than its
    /// subject, or arrives on a non-live subject is counted and dropped, and
    /// the feed is untouched.
    #[test]
    fn an_undecodable_event_is_counted_and_dropped() {
        let state = test_state();
        let (subject, body) = event("minimal", "broca", |_| {});
        let (_, unknown_member) = event("minimal", "broca", |body| body["prompt"] = json!("x"));
        let foreign = names().live_subject("thalamus", STEP_EVENT, 1).unwrap();
        let stored = names().event_subject("broca", STEP_EVENT, 1).unwrap();
        admit(&state, &subject, b"not json");
        admit(&state, &subject, &unknown_member);
        admit(&state, &foreign, &body);
        admit(&state, &stored, &body);
        assert_eq!(feed_rows(&state), 0);
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                dropped: 4,
                ..RunnerCounts::default()
            }
        );
        admit(&state, &subject, &body);
        assert_eq!(feed_rows(&state), 1);
    }

    /// The step event body is closed, so a decoder older than runner contract
    /// 0.8.7 refuses an event carrying that version's optional timing and
    /// routing members. They must decode, and the step must reach the feed.
    #[test]
    fn an_event_with_contract_0_8_7_members_is_admitted() {
        let state = test_state();
        let (subject, body) = event("minimal", "thalamus-harness", |body| {
            body["sent_to_last_content_ms"] = json!(1_200);
            body["first_to_last_content_ms"] = json!(900);
            body["route_decision_id"] = json!("rd_0123456789abcdef");
        });
        admit(&state, &subject, &body);
        assert_eq!(feed_rows(&state), 1);
        assert_eq!(state.step_feed().runner_counts().dropped, 0);
    }

    /// Runner contract 0.8.19 adds an optional cache report (the lifetime used
    /// and the warming state) to the closed step event, so an older decoder
    /// refuses an event carrying it. The step must still reach the feed.
    #[test]
    fn an_event_with_a_contract_0_8_19_cache_report_is_admitted() {
        let state = test_state();
        let (subject, body) = event("minimal", "thalamus-harness", |body| {
            body["cache"] = json!({"ttl_ms": 3_600_000});
        });
        admit(&state, &subject, &body);
        assert_eq!(feed_rows(&state), 1);
        assert_eq!(state.step_feed().runner_counts().dropped, 0);
    }

    /// Runner contract 0.8.37 adds an optional `message_id` to the closed step
    /// event: the runner's id for the assistant message the reply was placed
    /// in. An older decoder refuses an event carrying it. The event must
    /// decode, and its runner row must serve that id as `message_id`, apart
    /// from the step identity the row is keyed by.
    #[test]
    fn an_event_with_a_contract_0_8_37_message_id_serves_it_on_the_runner_row() {
        let state = test_state();
        admit_event(
            &state,
            &event("full", "thalamus-harness", |body| {
                body["session"] = json!("s1");
                body["harness_step_ref"] = json!("msg_x");
                body["message_id"] = json!("runner-message-1");
            }),
        );
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                appended: 1,
                ..RunnerCounts::default()
            }
        );
        let served = rows(&state, json!({"harness": "pi", "session": "s1"}));
        assert_eq!(served.len(), 1, "{served:?}");
        assert_eq!(served[0]["source"], "runner");
        assert_eq!(served[0]["step_ref"], "msg_x");
        assert_eq!(served[0]["message_id"], "runner-message-1");
    }

    /// A file row that replaces a runner row serves the host's message id
    /// for the step, or none, never the runner's.
    #[test]
    fn a_file_row_replacing_a_runner_row_keeps_its_own_message_id() {
        let state = test_state();
        for step_ref in ["msg_x", "msg_y"] {
            admit_event(
                &state,
                &event("full", "thalamus-harness", |body| {
                    body["session"] = json!("s1");
                    body["harness_step_ref"] = json!(step_ref);
                    body["message_id"] = json!(format!("runner-{step_ref}"));
                }),
            );
        }
        record_steps_for_test(
            &state,
            "pi",
            &[
                TestStep {
                    message_id: Some("host-msg_x"),
                    ..TestStep::now("s1", "msg_x")
                },
                TestStep::now("s1", "msg_y"),
            ],
            false,
        )
        .unwrap();
        let served = rows(&state, json!({"harness": "pi", "session": "s1"}));
        assert_eq!(served.len(), 2, "{served:?}");
        let row = |step_ref: &str| {
            served
                .iter()
                .find(|row| row["step_ref"] == step_ref)
                .unwrap()
        };
        assert_eq!(row("msg_x")["source"], "file");
        assert_eq!(row("msg_x")["message_id"], "host-msg_x");
        assert_eq!(row("msg_y")["source"], "file");
        assert!(row("msg_y").get("message_id").is_none(), "{served:?}");
    }

    /// A runner row's cache fields come from the same rules as a recorded
    /// step's, over the session's recorded steps: the file row that later
    /// replaces it, with the same counts, serves the same values.
    #[test]
    fn a_runner_rows_verdict_matches_the_file_row_that_replaces_it() {
        let state = test_state();
        let at = now_ms();
        record_steps_for_test(
            &state,
            "pi",
            &[TestStep {
                occurred_at_ms: at - 2_000,
                ..TestStep::now("s1", "msg_1")
            }],
            false,
        )
        .unwrap();
        admit_event(
            &state,
            &event("minimal", "thalamus-harness", |body| {
                body["harness"] = json!("pi");
                body["session"] = json!("s1");
                body["harness_step_ref"] = json!("msg_2");
                body["input_tokens"] = json!(10);
                body["cached_input_tokens"] = json!(50);
                body["output_tokens"] = json!(3);
            }),
        );
        let runner = rows(&state, json!({"harness": "pi", "session": "s1"}));
        assert_eq!(runner.len(), 2);
        assert_eq!(runner[1]["source"], "runner");
        assert_eq!(runner[1]["verdict"], "bust");
        record_steps_for_test(
            &state,
            "pi",
            &[TestStep {
                occurred_at_ms: at,
                cache_read: 50,
                ..TestStep::now("s1", "msg_2")
            }],
            false,
        )
        .unwrap();
        let file = rows(&state, json!({"harness": "pi", "session": "s1"}));
        let file = file.iter().find(|row| row["step_ref"] == "msg_2").unwrap();
        assert_eq!(file["source"], "file");
        for field in ["prompt", "retention", "verdict", "prompt_dropped"] {
            assert_eq!(runner[1][field], file[field], "{field}");
        }
    }

    /// BROCA's ledger events are run segments that sum several steps, so they
    /// are no baseline for a step. Here the segment sums two reads of about
    /// 400k; if the next announced step were judged against it, a steady
    /// 400k read would look like half the cache lost. The step is judged
    /// against the session's previous announced step instead.
    #[test]
    fn a_broca_run_segment_is_not_a_baseline_for_the_next_step() {
        let state = test_state();
        let at = now_ms();
        record_steps_for_test(
            &state,
            "broca",
            &[TestStep {
                // BROCA stamps a segment when it ends, so it sorts after the
                // steps it sums and is the session's latest recorded event.
                occurred_at_ms: at - 1_500,
                cache_read: 800_000,
                ..TestStep::now("sb", "segment")
            }],
            false,
        )
        .unwrap();
        for (attempt, offset, read) in [
            ("attempt-1", 2_000, 399_990_u64),
            ("attempt-2", 1_000, 400_000),
        ] {
            let start = u64::try_from(at - offset).unwrap();
            admit_event(
                &state,
                &event("minimal", "broca", |body| {
                    body["harness"] = json!("broca");
                    body["session"] = json!("sb");
                    body["attempt_id"] = json!(attempt);
                    body["started_at_ms"] = json!(start);
                    body["finished_at_ms"] = json!(start + 500);
                    body["input_tokens"] = json!(10);
                    body["cached_input_tokens"] = json!(read);
                }),
            );
        }
        let served = rows(&state, json!({"harness": "broca", "session": "sb"}));
        let second = served
            .iter()
            .find(|row| row["step_ref"] == "attempt-2")
            .unwrap();
        assert_eq!(second["verdict"], "stable", "{second}");
        assert_eq!(second["retention"], json!(1.0), "{second}");
        assert_eq!(second["prompt_dropped"], json!(false), "{second}");
    }

    fn insert_feed_row(state: &SharedState, committed_at_ms: i64, body: &Value) {
        state
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO step_feed(harness,step_ref,source,session,committed_at_ms,row_json)
                     VALUES (?1,?2,?3,?4,?5,?6)",
                    rusqlite::params![
                        body["harness"].as_str().unwrap(),
                        body["step_ref"].as_str().unwrap(),
                        body["source"].as_str().unwrap(),
                        body["session"].as_str().unwrap(),
                        committed_at_ms,
                        body.to_string(),
                    ],
                )
                .map_err(|error| error.to_string())?;
                Ok(())
            })
            .unwrap();
    }

    fn rejudge(state: &crate::state::SharedState, at: i64) {
        state
            .with_conn(|conn| {
                state
                    .step_feed()
                    .rejudge_runner_steps(conn, at)
                    .map_err(|error| error.to_string())
            })
            .unwrap();
    }

    #[test]
    fn rejudge_corrects_stored_runner_rows_once_without_touching_protected_rows() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let at = now_ms();
        record_steps_for_test(
            &state,
            "broca",
            &[TestStep {
                occurred_at_ms: at - 1_500,
                cache_read: 800_000,
                ..TestStep::now("sb", "segment")
            }],
            false,
        )
        .unwrap();
        admit_event(
            &state,
            &event("minimal", "broca", |body| {
                body["harness"] = json!("broca");
                body["session"] = json!("sb");
                body["attempt_id"] = json!("attempt-1");
                body["started_at_ms"] = json!(at - 2_000);
                body["finished_at_ms"] = json!(at - 1_500);
                body["input_tokens"] = json!(10);
                body["cached_input_tokens"] = json!(399_990);
            }),
        );
        // Insert a verdict computed against the aggregate segment directly, so
        // today's runner verdict calculation cannot correct the fixture early.
        let wrong = json!({
            "harness": "broca", "source": "runner", "session": "sb",
            "step_ref": "attempt-2", "run_id": "run-2", "occurred_at_ms": at - 1_000,
            "input": 10, "cache_read": 400_000, "cache_write": 30,
            "cache_write_5m": 10, "cache_write_1h": 20, "output": 20, "reasoning": 7,
            "provider": "anthropic", "model": "model", "finish": "completed",
            "context_limit": 1_000_000, "request_body_len": 4096, "prompt": 400_040,
            "verdict": "bust", "retention": 0.5, "prompt_dropped": true
        });
        insert_feed_row(&state, at, &wrong);
        let mut protected = wrong.clone();
        protected["step_ref"] = json!("replaced-by-file");
        protected["session"] = json!("other-session");
        insert_feed_row(&state, at, &protected);
        protected["source"] = json!("file");
        protected["message_id"] = json!("host-message");
        insert_feed_row(&state, at, &protected);
        let mut cached_harness = wrong.clone();
        cached_harness["harness"] = json!("pi");
        insert_feed_row(&state, at, &cached_harness);
        let before = feed_rows(&state);
        drop(state);

        // Startup no longer re-judges: reopening the store leaves the feed
        // exactly as it was.
        let state = crate::state::leased_test_state(&directory);
        assert_eq!(
            feed_rows(&state),
            before,
            "startup must not re-judge the feed"
        );
        rejudge(&state, at);
        assert_eq!(feed_rows(&state), before + 1, "one correction");
        let served = rows(&state, json!({}));
        let corrected = served.last().unwrap();
        let mut expected = wrong.clone();
        expected["verdict"] = json!("stable");
        expected["retention"] = json!(1.0);
        expected["prompt_dropped"] = json!(false);
        let mut corrected_body = corrected.clone();
        corrected_body.as_object_mut().unwrap().remove("cursor");
        assert_eq!(corrected_body, expected);
        assert!(corrected.get("message_id").is_none());
        // Earlier emissions remain byte-for-byte intact, including the wrong
        // runner row and the identity already replaced by a file row.
        state
            .with_conn(|conn| {
                let stored: Vec<String> = conn
                    .prepare("SELECT row_json FROM step_feed ORDER BY feed_cursor")
                    .unwrap()
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                assert_eq!(stored[1], wrong.to_string());
                assert_eq!(stored[3], protected.to_string());
                assert_eq!(stored[4], cached_harness.to_string());
                Ok(())
            })
            .unwrap();
        rejudge(&state, at);
        assert_eq!(
            feed_rows(&state),
            before + 1,
            "a second re-judge is a no-op"
        );
    }

    /// Re-judging a runner row changes only its cache diagnostics: the
    /// `message_id` the runner sent stays on the new row.
    #[test]
    fn rejudge_keeps_a_runner_rows_message_id() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let at = now_ms();
        record_steps_for_test(
            &state,
            "broca",
            &[TestStep {
                occurred_at_ms: at - 1_500,
                cache_read: 800_000,
                ..TestStep::now("sb", "segment")
            }],
            false,
        )
        .unwrap();
        admit_event(
            &state,
            &event("minimal", "broca", |body| {
                body["harness"] = json!("broca");
                body["session"] = json!("sb");
                body["attempt_id"] = json!("attempt-1");
                body["started_at_ms"] = json!(at - 2_000);
                body["finished_at_ms"] = json!(at - 1_500);
                body["input_tokens"] = json!(10);
                body["cached_input_tokens"] = json!(399_990);
            }),
        );
        // A verdict judged against the aggregate segment, inserted directly
        // so that startup has a row to correct.
        let wrong = json!({
            "harness": "broca", "source": "runner", "session": "sb",
            "step_ref": "attempt-2", "run_id": "run-2", "occurred_at_ms": at - 1_000,
            "message_id": "runner-message-2",
            "input": 10, "cache_read": 400_000, "prompt": 400_010,
            "verdict": "bust", "retention": 0.5, "prompt_dropped": true
        });
        insert_feed_row(&state, at, &wrong);
        let before = feed_rows(&state);
        rejudge(&state, at);
        assert_eq!(feed_rows(&state), before + 1, "one correction");
        let served = rows(&state, json!({}));
        let corrected = served.last().unwrap();
        assert_eq!(corrected["step_ref"], "attempt-2");
        assert_eq!(corrected["verdict"], "stable", "{corrected}");
        assert_eq!(corrected["message_id"], "runner-message-2");
    }

    #[test]
    fn startup_runner_rejudge_failure_rolls_back_and_still_serves() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let at = now_ms();
        for (step_ref, offset) in [("first", 2_000), ("second", 1_000)] {
            insert_feed_row(
                &state,
                at,
                &json!({
                    "harness": "goose", "source": "runner", "session": "s",
                    "step_ref": step_ref, "run_id": "r", "occurred_at_ms": at - offset,
                    "input": 10, "cache_read": 100, "prompt": 110,
                    "verdict": "bust", "retention": 0.5, "prompt_dropped": true
                }),
            );
        }
        state
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER refuse_rejudge BEFORE INSERT ON step_feed
                 WHEN NEW.step_ref='second'
                 BEGIN SELECT RAISE(ABORT, 'repair unavailable'); END;",
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        drop(state);

        let state = crate::state::leased_test_state(&directory);
        assert_eq!(
            feed_rows(&state),
            2,
            "the first correction must roll back too"
        );
        let served = rows(&state, json!({"harness": "goose", "session": "s"}));
        assert_eq!(served.len(), 2);
        assert!(served.iter().all(|row| row["verdict"] == "bust"));
    }

    /// Bodies broca's publisher wrote in its own tests, pinned byte for byte
    /// (`tests/vectors/broca-step-finished/SHA256SUMS`): completed,
    /// interrupted and keep-warm steps from both of broca's session storages,
    /// plain and encrypted.
    const BROCA_STEP_VECTORS: [(&str, &[u8]); 12] = [
        (
            "completed-store-encrypted",
            include_bytes!("../tests/vectors/broca-step-finished/completed-store-encrypted.json"),
        ),
        (
            "completed-store-plain",
            include_bytes!("../tests/vectors/broca-step-finished/completed-store-plain.json"),
        ),
        (
            "completed-today-encrypted",
            include_bytes!("../tests/vectors/broca-step-finished/completed-today-encrypted.json"),
        ),
        (
            "completed-today-plain",
            include_bytes!("../tests/vectors/broca-step-finished/completed-today-plain.json"),
        ),
        (
            "interrupted-store-encrypted",
            include_bytes!("../tests/vectors/broca-step-finished/interrupted-store-encrypted.json"),
        ),
        (
            "interrupted-store-plain",
            include_bytes!("../tests/vectors/broca-step-finished/interrupted-store-plain.json"),
        ),
        (
            "interrupted-today-encrypted",
            include_bytes!("../tests/vectors/broca-step-finished/interrupted-today-encrypted.json"),
        ),
        (
            "interrupted-today-plain",
            include_bytes!("../tests/vectors/broca-step-finished/interrupted-today-plain.json"),
        ),
        (
            "keep-warm-store-encrypted",
            include_bytes!("../tests/vectors/broca-step-finished/keep-warm-store-encrypted.json"),
        ),
        (
            "keep-warm-store-plain",
            include_bytes!("../tests/vectors/broca-step-finished/keep-warm-store-plain.json"),
        ),
        (
            "keep-warm-today-encrypted",
            include_bytes!("../tests/vectors/broca-step-finished/keep-warm-today-encrypted.json"),
        ),
        (
            "keep-warm-today-plain",
            include_bytes!("../tests/vectors/broca-step-finished/keep-warm-today-plain.json"),
        ),
    ];

    /// A fork attempt's step event as broca publishes it
    /// (`tests/vectors/broca-fork-spend`, 2026-11-12). A fork is a side
    /// question asked of a head: its event is an ordinary step of the fork's
    /// own session and run, so it is served under that session. The head's
    /// session gets no row from it, so a fork's prompt never counts as the
    /// head's next step when the head's cache reuse is judged.
    #[test]
    fn a_broca_fork_step_event_is_served_under_the_fork_session() {
        let bytes: &[u8] =
            include_bytes!("../tests/vectors/broca-fork-spend/step-finished.fork.before.json");
        let subject = names().live_subject("broca", STEP_EVENT, 1).unwrap();
        let mut sent: Value = serde_json::from_slice(bytes).unwrap();
        decode_step_finished(&subject, bytes).expect("the fork step event decodes");
        let start = u64::try_from(now_ms()).unwrap() - 60_000;
        sent["started_at_ms"] = json!(start);
        sent["finished_at_ms"] = json!(start + 1_500);
        let state = test_state();
        admit(&state, &subject, &serde_json::to_vec(&sent).unwrap());
        let fork_session = "head:fork:fork-72f6bdfa7eb7fb4cdf456d38c4d99623";
        let served = rows(&state, json!({"harness": "broca", "session": fork_session}));
        assert_eq!(served.len(), 1);
        assert_eq!(served[0]["step_ref"], sent["attempt_id"]);
        assert_eq!(served[0]["input"], 1200);
        assert!(rows(&state, json!({"harness": "broca", "session": "head"})).is_empty());
    }

    /// Thalamus's step bodies with the rewrite-cause facts, as its
    /// publisher wrote them for each host (`tests/vectors/thalamus-step-cause`,
    /// 16 Nov 2026). Each fact the body carries reaches the cause evidence
    /// unchanged, and a fact the body leaves out stays unknown. Claude Code
    /// sends only the account for now.
    #[test]
    fn agent_runner_step_cause_facts_decode_into_evidence() {
        const VECTORS: [(&str, &[u8]); 3] = [
            (
                "opencode2",
                include_bytes!("../tests/vectors/thalamus-step-cause/opencode2.json"),
            ),
            (
                "pi",
                include_bytes!("../tests/vectors/thalamus-step-cause/pi.json"),
            ),
            (
                "claude-code",
                include_bytes!("../tests/vectors/thalamus-step-cause/claude-code.json"),
            ),
        ];
        let subject = names()
            .live_subject("thalamus-harness", STEP_EVENT, 1)
            .unwrap();
        for (name, bytes) in VECTORS {
            let sent: Value = serde_json::from_slice(bytes).unwrap();
            let event = decode_step_finished(&subject, bytes)
                .unwrap_or_else(|error| panic!("{name} does not decode: {error:?}"));
            let step = feed_step(event).unwrap_or_else(|| panic!("{name} has no row identity"));
            let evidence = &step.evidence;
            assert_eq!(
                evidence.account_ref.as_deref(),
                sent["account_ref"].as_str(),
                "{name}"
            );
            assert!(evidence.account_ref.is_some(), "{name} carries its account");
            assert_eq!(
                evidence.view_version,
                sent["view_version"].as_u64(),
                "{name}"
            );
            assert_eq!(
                evidence.tools_digest.as_deref(),
                sent["tools_digest"].as_str(),
                "{name}"
            );
            assert_eq!(
                evidence.system_digest.as_deref(),
                sent["system_digest"].as_str(),
                "{name}"
            );
            assert_eq!(
                evidence.signed_floor.as_deref(),
                sent["signed_floor"].as_str(),
                "{name}"
            );
            if name == "claude-code" {
                assert!(evidence.tools_digest.is_none() && evidence.view_version.is_none());
            } else {
                assert!(evidence.tools_digest.is_some() && evidence.system_digest.is_some());
            }
        }
    }

    /// Thalamus's Claude Code body once its gateway records each request's
    /// cause facts (`tests/vectors/thalamus-step-cause-claude-code-v1`, 23 Nov
    /// 2026). All five facts reach the evidence as sent, as on OpenCode 2 and
    /// Pi, so a Claude Code cache bust can be named a tool or system change.
    #[test]
    fn agent_runner_claude_code_step_carries_every_cause_fact() {
        let bytes: &[u8] =
            include_bytes!("../tests/vectors/thalamus-step-cause-claude-code-v1/claude-code.json");
        let subject = names()
            .live_subject("thalamus-harness", STEP_EVENT, 1)
            .unwrap();
        let sent: Value = serde_json::from_slice(bytes).unwrap();
        let event = decode_step_finished(&subject, bytes).expect("the Claude Code body decodes");
        let step = feed_step(event).expect("the Claude Code body has a row identity");
        assert_eq!(step.harness, "claude-code");
        let evidence = &step.evidence;
        assert_eq!(
            evidence.account_ref.as_deref(),
            sent["account_ref"].as_str()
        );
        assert_eq!(evidence.view_version, Some(7));
        assert_eq!(
            evidence.tools_digest.as_deref(),
            sent["tools_digest"].as_str()
        );
        assert_eq!(
            evidence.system_digest.as_deref(),
            sent["system_digest"].as_str()
        );
        assert_eq!(evidence.signed_floor.as_deref(), Some("previous-reply"));
        assert!(evidence.tools_digest.is_some() && evidence.system_digest.is_some());
    }

    /// Thalamus's Pi step body once Pi keeps stated zero counts
    /// (`tests/vectors/thalamus-host-parity-wave1-v1`, 23 Nov 2026). A zero
    /// cache write and a zero reasoning count arrive as stated zeros, never as
    /// unknown, and the output stays whole because reasoning is zero.
    #[test]
    fn agent_runner_pi_step_keeps_stated_zero_counts() {
        let bytes: &[u8] = include_bytes!(
            "../tests/vectors/thalamus-host-parity-wave1-v1/pi-step-finished-zero.json"
        );
        let subject = names()
            .live_subject("thalamus-harness", STEP_EVENT, 1)
            .unwrap();
        let event = decode_step_finished(&subject, bytes).expect("the Pi body decodes");
        let step = feed_step(event).expect("the Pi body has a row identity");
        assert_eq!(step.harness, "pi");
        let row = &step.step;
        assert_eq!(
            (
                row.input,
                row.cache_read,
                row.cache_write,
                row.output,
                row.reasoning
            ),
            (Some(11), Some(900), Some(0), Some(7), Some(0))
        );
        assert_eq!(step.evidence.view_version, Some(0));
    }

    /// Thalamus's bodies for unasked steps
    /// (`tests/vectors/thalamus-step-cause-unasked`, 16 Nov 2026). A step that
    /// went out without asking the module carries the facts in force for its
    /// session, so its cause can be named; all four facts reach the evidence as
    /// sent. These samples carry no account, which stays unknown.
    #[test]
    fn agent_runner_unasked_step_facts_decode_into_evidence() {
        const VECTORS: [(&str, &[u8]); 2] = [
            (
                "opencode2",
                include_bytes!("../tests/vectors/thalamus-step-cause-unasked/opencode2.json"),
            ),
            (
                "pi",
                include_bytes!("../tests/vectors/thalamus-step-cause-unasked/pi.json"),
            ),
        ];
        let subject = names()
            .live_subject("thalamus-harness", STEP_EVENT, 1)
            .unwrap();
        for (name, bytes) in VECTORS {
            let event = decode_step_finished(&subject, bytes)
                .unwrap_or_else(|error| panic!("{name} does not decode: {error:?}"));
            let step = feed_step(event).unwrap_or_else(|| panic!("{name} has no row identity"));
            let evidence = &step.evidence;
            assert_eq!(evidence.view_version, Some(7), "{name}");
            assert_eq!(
                evidence.signed_floor.as_deref(),
                Some("previous-reply"),
                "{name}"
            );
            assert_eq!(
                evidence.tools_digest.as_deref(),
                Some("cac09688e76222fae6219bfe2453f0026ba99f49084974addd97c5c165fd0c8a"),
                "{name}"
            );
            assert_eq!(
                evidence.system_digest.as_deref(),
                Some("98d9872ac1f35aee39e46a1d9dce1333ba1233084aa640d8c6667203ea0050e3"),
                "{name}"
            );
            assert!(evidence.account_ref.is_none(), "{name}");
        }
    }

    /// Broca's step bodies with the rewrite-cause facts
    /// (`tests/vectors/broca-step-rewrite-facts/SHA256SUMS`, broca's publisher on
    /// llm-runner 0.8.55, 22 Nov 2026). Each fact the body carries reaches the
    /// cause evidence unchanged, a fact it leaves out stays unknown, and a
    /// stated `view_version` of 0 stays 0. Steps 7 and 8 are one run whose
    /// account moved between them, so both are served under that session.
    #[test]
    fn brocas_rewrite_cause_facts_decode_into_evidence() {
        const VECTORS: [(&str, &[u8]); 8] = [
            (
                "completed-store-encrypted",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/completed-store-encrypted.json"
                ),
            ),
            (
                "completed-store-plain",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/completed-store-plain.json"
                ),
            ),
            (
                "interrupted-store-encrypted",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/interrupted-store-encrypted.json"
                ),
            ),
            (
                "interrupted-store-plain",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/interrupted-store-plain.json"
                ),
            ),
            (
                "keep-warm-store-encrypted",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/keep-warm-store-encrypted.json"
                ),
            ),
            (
                "keep-warm-store-plain",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/keep-warm-store-plain.json"
                ),
            ),
            (
                "rewrite-facts-step-7",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/rewrite-facts-step-7.json"
                ),
            ),
            (
                "rewrite-facts-step-8",
                include_bytes!(
                    "../tests/vectors/broca-step-rewrite-facts/rewrite-facts-step-8.json"
                ),
            ),
        ];
        let subject = names().live_subject("broca", STEP_EVENT, 1).unwrap();
        for (name, bytes) in VECTORS {
            let sent: Value = serde_json::from_slice(bytes).unwrap();
            let event = decode_step_finished(&subject, bytes)
                .unwrap_or_else(|error| panic!("{name} does not decode: {error:?}"));
            let step = feed_step(event).unwrap_or_else(|| panic!("{name} has no row identity"));
            let evidence = &step.evidence;
            assert_eq!(
                evidence.account_ref.as_deref(),
                sent["account_ref"].as_str(),
                "{name}"
            );
            assert_eq!(evidence.warm_up, sent["warm_up"].as_bool(), "{name}");
            assert!(evidence.warm_up.is_some(), "{name} states warm_up");
            assert_eq!(
                evidence.view_version,
                sent["view_version"].as_u64(),
                "{name}"
            );
            assert_eq!(
                evidence.tools_digest.as_deref(),
                sent["tools_digest"].as_str(),
                "{name}"
            );
            assert_eq!(
                evidence.system_digest.as_deref(),
                sent["system_digest"].as_str(),
                "{name}"
            );
            assert_eq!(
                evidence.signed_floor.as_deref(),
                sent["signed_floor"].as_str(),
                "{name}"
            );
        }

        let state = test_state();
        let start = u64::try_from(now_ms()).unwrap() - 60_000;
        for (offset, (name, bytes)) in VECTORS[6..].iter().enumerate() {
            let mut recent: Value = serde_json::from_slice(bytes).unwrap();
            let length = recent["finished_at_ms"].as_u64().unwrap()
                - recent["started_at_ms"].as_u64().unwrap();
            let at = start + u64::try_from(offset).unwrap() * 10_000;
            recent["started_at_ms"] = json!(at);
            recent["finished_at_ms"] = json!(at + length);
            admit(&state, &subject, &serde_json::to_vec(&recent).unwrap());
            assert!(
                state.step_feed().runner_counts().appended > offset as u64,
                "{name}"
            );
        }
        let served = rows(&state, json!({"harness": "broca", "session": "session_a"}));
        assert_eq!(served.len(), 2);
        assert_ne!(served[0]["account_ref"], served[1]["account_ref"]);
        assert_eq!(served[0]["view_version"], 3);
        assert_eq!(served[1]["view_version"], 0);
        assert_eq!(served[0]["tools_digest"], served[1]["tools_digest"]);
    }

    /// Every pinned broca body decodes on broca's live subject exactly as
    /// sent, keyed by runner and attempt id since broca sends no host record.
    /// Moved into the feed's window, each is served as its own runner row:
    /// absent counts stay absent, and interrupted steps carry none.
    #[test]
    fn brocas_published_step_events_decode_and_are_served() {
        let subject = names().live_subject("broca", STEP_EVENT, 1).unwrap();
        let state = test_state();
        let now = u64::try_from(now_ms()).unwrap();
        for (index, (name, bytes)) in BROCA_STEP_VECTORS.iter().enumerate() {
            let event = decode_step_finished(&subject, bytes)
                .unwrap_or_else(|error| panic!("{name} does not decode: {error:?}"));
            let sent: Value = serde_json::from_slice(bytes).unwrap();
            let step = feed_step(event).unwrap_or_else(|| panic!("{name} has no row identity"));
            assert_eq!(step.harness, "broca", "{name}");
            assert_eq!(
                step.step.step_ref,
                sent["attempt_id"].as_str().unwrap(),
                "{name}"
            );
            let mut recent = sent.clone();
            let start = now - 60_000 + u64::try_from(index).unwrap() * 1_000;
            let length =
                sent["finished_at_ms"].as_u64().unwrap() - sent["started_at_ms"].as_u64().unwrap();
            recent["started_at_ms"] = json!(start);
            recent["finished_at_ms"] = json!(start + length);
            admit(&state, &subject, &serde_json::to_vec(&recent).unwrap());
        }
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                appended: 12,
                ..RunnerCounts::default()
            }
        );
        let served = rows(&state, json!({"harness": "broca", "session": "s1"}));
        assert_eq!(served.len(), 12);
        for (row, (name, bytes)) in served.iter().zip(BROCA_STEP_VECTORS) {
            let sent: Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(row["source"], "runner", "{name}");
            assert_eq!(row["step_ref"], sent["attempt_id"], "{name}");
            for (event_member, row_member) in [
                ("input_tokens", "input"),
                ("cached_input_tokens", "cache_read"),
                ("output_tokens", "output"),
            ] {
                assert_eq!(
                    row.get(row_member),
                    sent.get(event_member),
                    "{name}: {row_member}"
                );
            }
        }
    }

    /// Broca's step events before and after it fills the optional `cache`
    /// report (`tests/vectors/broca-step-cache/SHA256SUMS`, serialized by
    /// broca with llm-runner 0.8.36): each decodes on broca's subject and is
    /// served with its cache write and its 5-minute and 1-hour split.
    #[test]
    fn brocas_step_events_with_a_cache_report_decode_and_are_served() {
        let subject = names().live_subject("broca", STEP_EVENT, 1).unwrap();
        let directory =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/broca-step-cache");
        let mut decoded = 0;
        for phase in ["before", "after"] {
            let mut files: Vec<_> = std::fs::read_dir(directory.join(phase))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect();
            files.sort();
            for path in files {
                let name = path.display().to_string();
                // The samples come from broca's serializer on a synthetic
                // session, whose runner is the contract's placeholder
                // `runner_a`. Broca's live events name `broca`, which its
                // subject requires, so only that member and the times are set.
                let mut recent: Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                let now = u64::try_from(now_ms()).unwrap();
                recent["runner"] = json!("broca");
                recent["started_at_ms"] = json!(now - 2_000);
                recent["finished_at_ms"] = json!(now - 1_000);
                let bytes = serde_json::to_vec(&recent).unwrap();
                let event = decode_step_finished(&subject, &bytes)
                    .unwrap_or_else(|error| panic!("{name} does not decode: {error:?}"));
                assert!(feed_step(event).is_some(), "{name} has no row identity");
                let state = test_state();
                admit(&state, &subject, &bytes);
                assert_eq!(state.step_feed().runner_counts().appended, 1, "{name}");
                let served = rows(
                    &state,
                    json!({"harness": "broca", "session": recent["session"]}),
                );
                assert_eq!(served.len(), 1, "{name}");
                for member in ["cache_write", "cache_write_5m", "cache_write_1h"] {
                    let sent = format!("{member}_tokens");
                    assert_eq!(
                        served[0].get(member),
                        recent.get(sent.as_str()),
                        "{name}: {member}"
                    );
                }
                decoded += 1;
            }
        }
        assert_eq!(decoded, 10);
    }

    #[test]
    fn a_step_older_than_the_feed_window_is_not_added() {
        let state = test_state();
        let old = u64::try_from(now_ms() - 25 * 60 * 60 * 1_000).unwrap();
        admit_event(
            &state,
            &event("minimal", "broca", |body| {
                body["started_at_ms"] = json!(old);
                body["finished_at_ms"] = json!(old + 1_000);
            }),
        );
        assert_eq!(feed_rows(&state), 0);
        assert_eq!(state.step_feed().runner_counts().outside_window, 1);
    }
}
