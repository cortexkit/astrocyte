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
    // A step's time is when it started, as for a recorded step (OpenCode's
    // message creation time, for example). The file row that later replaces
    // this one then sorts at the same place in the feed.
    let occurred_at_ms = i64::try_from(event.started_at_ms).ok()?;
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

    #[test]
    fn startup_rejudges_stored_runner_rows_once_without_touching_protected_rows() {
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

        let state = crate::state::leased_test_state(&directory);
        assert_eq!(feed_rows(&state), before + 1, "one correction at startup");
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
        drop(state);

        let state = crate::state::leased_test_state(&directory);
        assert_eq!(feed_rows(&state), before + 1, "a second startup is a no-op");
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
