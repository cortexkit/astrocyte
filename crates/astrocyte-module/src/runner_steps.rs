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
use cortexkit_role_llm_runner::step_events::{decode_step_finished, StepFinished};
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
    let occurred_at_ms = i64::try_from(event.finished_at_ms).ok()?;
    // The ledger stores output without reasoning and the served row adds
    // reasoning back (`served_output`). An event that does not say reasoning
    // is outside output is read as including it, the feed's own convention;
    // a reasoning count above such an output cannot be a breakdown of it and
    // is left out rather than served as one.
    let (output, reasoning) = match (event.output_tokens, event.reasoning_tokens) {
        (Some(output), Some(reasoning)) if event.reasoning_included_in_output != Some(false) => {
            match output.checked_sub(reasoning) {
                Some(rest) => (Some(rest), Some(reasoning)),
                None => (Some(output), None),
            }
        }
        counts => counts,
    };
    Some(RunnerStep {
        harness,
        session: event.session,
        run_id: event.run_id,
        step: StepRow {
            // Unused: a runner step has no cache cursor.
            cursor: i64::MAX,
            step_ref,
            occurred_at_ms,
            input: event.input_tokens,
            cache_read: event.cached_input_tokens,
            cache_write: event.cache_write_tokens,
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
        cache_write_5m: event.cache_write_5m_tokens,
        cache_write_1h: event.cache_write_1h_tokens,
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
        admit_event(&state, &event("full", "broca", |_| {}));
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
                ("host_a", "msg_record_a"),
            ]
        );
        for row in &rows {
            let wire: WireRow = serde_json::from_value(row.clone()).unwrap();
            assert_eq!(wire.source, StepSource::Runner);
            assert_eq!(wire.session, "session_a");
            assert_eq!(wire.run_id.as_deref(), Some("run_a"));
        }
        let mut full = rows[3].clone();
        full.as_object_mut().unwrap().remove("cursor");
        full.as_object_mut().unwrap().remove("occurred_at_ms");
        assert_eq!(
            full,
            json!({
                "harness": "host_a", "source": "runner", "step_ref": "msg_record_a",
                "session": "session_a", "run_id": "run_a", "provider": "provider_a",
                "model": "model_a", "input": 100, "cache_read": 50, "cache_write": 30,
                "cache_write_5m": 10, "cache_write_1h": 20, "output": 20, "reasoning": 7,
                "finish": "future_finish_reason", "context_limit": 200_000,
                "request_body_len": 4096, "prompt": 180, "verdict": "cold_start",
                "prompt_dropped": false
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

    /// A runner event for a step the feed already serves from a durable read
    /// is counted as superseded and leaves the file row newest; the same
    /// runner event twice appends once.
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
        assert_eq!(feed_rows(&state), 1);
        let repeated = event("minimal", "broca", |_| {});
        admit_event(&state, &repeated);
        admit_event(&state, &repeated);
        assert_eq!(feed_rows(&state), 2);
        assert_eq!(
            state.step_feed().runner_counts(),
            RunnerCounts {
                appended: 1,
                unchanged: 1,
                superseded: 1,
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

    #[test]
    fn a_step_older_than_the_feed_window_is_not_added() {
        let state = test_state();
        let old = u64::try_from(now_ms() - 25 * 60 * 60 * 1_000).unwrap();
        admit_event(
            &state,
            &event("minimal", "broca", |body| {
                body["finished_at_ms"] = json!(old)
            }),
        );
        assert_eq!(feed_rows(&state), 0);
        assert_eq!(state.step_feed().runner_counts().outside_window, 1);
    }
}
