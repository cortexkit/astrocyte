//! Serving the live step feed: `usage.steps.subscribe` and `usage.steps.read`.
//!
//! The feed itself (its table, change detection and paged read) lives in
//! `astrocyte_core::step_feed`. This module appends to it after each commit
//! (`FeedHub::publish`, called from the writer's post-commit point), tells
//! open subscriptions, serves the read, and decides who may call.
//!
//! A subscription carries only `{"through": <newest cursor>}` notices. Each
//! subscriber has a bounded queue of the cursors published since it last
//! sent a notice; it sends the newest one at most four times a second. A
//! subscriber whose queue fills, because its sends are held up by the serve
//! helper's bounded outbound queue, ends with the error `lagged`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astrocyte_core::step_feed::{self as feed, FeedError, FeedFilter, FeedPage, FeedReadRequest};
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{async_trait, HandlerOutcome, RequestCtx};
use subc_protocol::Principal;
use tokio::sync::{mpsc, watch, Semaphore};

use crate::state::SharedState;

/// Subscriptions open at once. Each holds a request slot of the serve
/// helper for as long as it is open; one more is refused
/// `subscription_capacity`.
pub const MAX_FEED_SUBSCRIPTIONS: usize = 16;

/// Cursors a subscriber may have waiting while it sends a notice. A notice
/// carries only the newest cursor, so a subscriber that keeps up never holds
/// more than one; 64 means its sends have been held up for 64 commits (at
/// the fast lane's 2-second cadence, about two minutes), and it ends `lagged`.
pub const SUBSCRIBER_QUEUE: usize = 64;

/// The shortest gap between two notices to one subscriber: at most four a
/// second, as the contract says.
const NOTICE_INTERVAL: Duration = Duration::from_millis(250);

/// The route principals that may read the feed: local apps (`Direct`) and
/// Callosum's phone routes, whose bind names the phone in the harness field
/// (`fed-phone:<fingerprint>`). Simulators (`fed-sim:`), other computers
/// (`fed-computer:`) and every other principal are refused `not_permitted`.
pub fn may_read_feed(principal: Option<&Principal>, harness: &str) -> bool {
    match principal {
        Some(Principal::Direct) => true,
        Some(Principal::Reserved { module_id }) => {
            module_id == "callosum" && harness.starts_with("fed-phone:")
        }
        _ => false,
    }
}

/// `usage.steps.subscribe` params. `harness` and `session` select one session
/// and must be supplied together.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedSubscribeRequest {
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
}

struct Subscriber {
    queue: mpsc::Sender<i64>,
    lagged: AtomicBool,
    filter: Option<FeedFilter>,
}

#[derive(Default)]
struct HubState {
    /// The newest feed cursor published, or `None` before the first
    /// publication at startup.
    through: Option<i64>,
    subscribers: Vec<Arc<Subscriber>>,
}

/// The open subscriptions and the newest cursor told to them.
pub struct FeedHub {
    state: Mutex<HubState>,
    slots: Arc<Semaphore>,
}

impl Default for FeedHub {
    fn default() -> Self {
        Self {
            state: Mutex::new(HubState::default()),
            slots: Arc::new(Semaphore::new(MAX_FEED_SUBSCRIPTIONS)),
        }
    }
}

/// Where a subscription's notices go. The production sink is the request's
/// stream; tests supply their own to hold a subscriber back.
#[async_trait]
pub trait NoticeSink: Send + Sync {
    /// Send one notice. An error means the stream is gone.
    async fn emit(&self, notice: Vec<u8>) -> Result<(), String>;
    /// Completes when the subscriber cancels or its route closes.
    async fn ended(&self);
}

#[async_trait]
impl NoticeSink for RequestCtx {
    async fn emit(&self, notice: Vec<u8>) -> Result<(), String> {
        RequestCtx::emit(self, notice)
            .await
            .map_err(|error| error.to_string())
    }

    async fn ended(&self) {
        self.cancelled().await;
    }
}

/// How a subscription's stream ended on its own account.
enum StreamEnd {
    /// The sink is gone.
    Gone,
    /// The subscriber's queue filled; `through` is the last cursor it was
    /// sent.
    Lagged { through: i64 },
}

/// Removes a subscriber from the hub when its subscription ends.
struct Registration<'a> {
    hub: &'a FeedHub,
    subscriber: Arc<Subscriber>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.hub
            .lock()
            .subscribers
            .retain(|other| !Arc::ptr_eq(other, &self.subscriber));
    }
}

impl FeedHub {
    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Bring the feed up to date and tell subscribers. Called only after a
    /// commit, on the writer's connection under the writer fence (see
    /// `SharedState::publish_step_feed`). The append runs in a savepoint of
    /// its own, and subscribers are told only once it is released.
    pub(crate) fn publish(
        &self,
        conn: &Connection,
        now_ms: i64,
        late: &[i64],
    ) -> rusqlite::Result<()> {
        conn.execute_batch("SAVEPOINT step_feed")?;
        let appended = (|| {
            let before = feed::through(conn)?;
            feed::append_changes(conn, now_ms, late)?;
            let through = feed::through(conn)?;
            // Read only the appended cursor range, not the retained feed, to
            // decide which session subscriptions need a notice.
            let changed = conn
                .prepare_cached("SELECT harness,session FROM step_feed WHERE feed_cursor>?1")?
                .query_map([before], |row| {
                    Ok(FeedFilter {
                        harness: row.get(0)?,
                        session: row.get(1)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<_, rusqlite::Error>((through, changed))
        })();
        let (through, changed) = match appended {
            Ok(appended) => {
                conn.execute_batch("RELEASE step_feed")?;
                appended
            }
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK TO step_feed; RELEASE step_feed");
                return Err(error);
            }
        };
        self.notify(through, &changed);
        Ok(())
    }

    /// Queue a newer `through` for unfiltered subscribers and for subscribers
    /// whose exact `(harness, session)` pair gained a row. A full queue marks
    /// its subscriber lagged and
    /// drops it from the hub.
    fn notify(&self, through: i64, changed: &[FeedFilter]) {
        let mut hub = self.lock();
        if hub.through.is_some_and(|told| told >= through) {
            return;
        }
        hub.through = Some(through);
        hub.subscribers.retain(|subscriber| {
            if let Some(filter) = &subscriber.filter {
                if !changed.contains(filter) {
                    return true;
                }
            }
            match subscriber.queue.try_send(through) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    subscriber.lagged.store(true, Ordering::Release);
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }

    /// The newest feed cursor told to subscribers.
    pub fn through(&self) -> Option<i64> {
        self.lock().through
    }

    /// Subscriptions open now.
    pub fn open_subscriptions(&self) -> usize {
        MAX_FEED_SUBSCRIPTIONS - self.slots.available_permits()
    }

    async fn stream(
        &self,
        sink: &dyn NoticeSink,
        subscriber: &Subscriber,
        mut queue: mpsc::Receiver<i64>,
        opened_at: i64,
    ) -> StreamEnd {
        let mut last_told = opened_at;
        let mut last_sent: Option<tokio::time::Instant> = None;
        loop {
            let Some(mut through) = queue.recv().await else {
                // The hub drops its end only when this subscriber lagged.
                return StreamEnd::Lagged { through: last_told };
            };
            if let Some(sent) = last_sent {
                tokio::time::sleep_until(sent + NOTICE_INTERVAL).await;
            }
            while let Ok(newer) = queue.try_recv() {
                through = newer;
            }
            let notice = json!({ "through": feed::encode_cursor(through) });
            if sink.emit(notice.to_string().into_bytes()).await.is_err() {
                return StreamEnd::Gone;
            }
            last_told = through;
            last_sent = Some(tokio::time::Instant::now());
            if subscriber.lagged.load(Ordering::Acquire) {
                return StreamEnd::Lagged { through: last_told };
            }
        }
    }
}

fn refusal(code: &str, message: impl Into<String>) -> HandlerOutcome {
    HandlerOutcome::Error {
        code: code.into(),
        message: message.into(),
    }
}

/// Serve one `usage.steps.subscribe` request to the end of its stream. The
/// caller has already checked the route's principal with `may_read_feed`.
///
/// A refusal before the first notice is an error reply. The stream then
/// ends in one of four ways: the subscriber cancels (`cancelled`), its
/// route closes (no reply reaches anyone, the route is gone), astrocyte
/// stops or drains (a clean stream end), or the subscriber falls behind
/// its queue (`lagged`, with the last cursor it was told in
/// `detail.through`).
pub async fn subscribe(
    state: &SharedState,
    params: Value,
    sink: &dyn NoticeSink,
    mut stopping: watch::Receiver<bool>,
) -> HandlerOutcome {
    let request = match serde_json::from_value::<FeedSubscribeRequest>(params) {
        Ok(request) => request,
        Err(error) => return refusal("malformed", format!("steps subscribe params: {error}")),
    };
    let filter = match FeedFilter::from_pair(request.harness, request.session) {
        Ok(filter) => filter,
        Err(message) => return refusal("invalid_request", message),
    };
    if *stopping.borrow() {
        return refusal(
            "draining",
            "astrocyte is stopping; subscribe again once it is back",
        );
    }
    let hub = state.step_feed();
    let Ok(_slot) = Arc::clone(&hub.slots).try_acquire_owned() else {
        return refusal(
            "subscription_capacity",
            format!("at most {MAX_FEED_SUBSCRIPTIONS} step feed subscriptions may be open"),
        );
    };
    let (queue_sender, queue) = mpsc::channel(SUBSCRIBER_QUEUE);
    let subscriber = Arc::new(Subscriber {
        queue: queue_sender,
        lagged: AtomicBool::new(false),
        filter,
    });
    let opened_at = {
        let mut hub_state = hub.lock();
        hub_state.subscribers.push(Arc::clone(&subscriber));
        hub_state.through.unwrap_or(0)
    };
    let _registration = Registration {
        hub,
        subscriber: Arc::clone(&subscriber),
    };
    let ended = tokio::select! {
        biased;
        () = sink.ended() => return refusal("cancelled", "subscription cancelled"),
        _ = stopping.wait_for(|stopping| *stopping) => return HandlerOutcome::Streamed,
        ended = hub.stream(sink, &subscriber, queue, opened_at) => ended,
    };
    match ended {
        StreamEnd::Gone => HandlerOutcome::Streamed,
        StreamEnd::Lagged { through } => HandlerOutcome::ErrorWithDetail {
            code: "lagged".into(),
            message: "the subscriber fell behind its queue; subscribe again and read from \
                      the last cursor it holds"
                .into(),
            detail: json!({ "through": feed::encode_cursor(through) }),
        },
    }
}

/// A refused `usage.steps.read`, with the detail `cursor_expired` carries.
pub struct ReadRefusal {
    pub code: String,
    pub message: String,
    pub detail: Option<Value>,
}

/// Serve one `usage.steps.read` from the read-only connection, in one
/// snapshot. The caller has already checked the route's principal.
pub fn read(state: &SharedState, params: Value) -> Result<FeedPage, ReadRefusal> {
    let request: FeedReadRequest = serde_json::from_value(params).map_err(|error| ReadRefusal {
        code: "malformed".into(),
        message: format!("steps read params: {error}"),
        detail: None,
    })?;
    let page = state
        .with_read_op(|conn| Ok(feed::read(conn, request)))
        .map_err(|(code, message)| ReadRefusal {
            code,
            message,
            detail: None,
        })?;
    page.map_err(|error| match error {
        FeedError::Refused(refused) => ReadRefusal {
            code: refused.code.into(),
            message: refused.message,
            detail: refused.detail,
        },
        FeedError::Store(error) => ReadRefusal {
            code: "internal".into(),
            message: format!("steps read: {error}"),
            detail: None,
        },
    })
}

/// [`read`] as the serve helper's outcome: `{"result": <page>}`, or the
/// refusal with its detail.
pub fn read_outcome(state: &SharedState, params: Value) -> HandlerOutcome {
    match read(state, params) {
        Ok(page) => match serde_json::to_vec(&json!({ "result": page })) {
            Ok(body) => HandlerOutcome::Response(body),
            Err(error) => refusal("internal", format!("steps read reply: {error}")),
        },
        Err(ReadRefusal {
            code,
            message,
            detail: Some(detail),
        }) => HandlerOutcome::ErrorWithDetail {
            code,
            message,
            detail,
        },
        Err(ReadRefusal { code, message, .. }) => HandlerOutcome::Error { code, message },
    }
}

/// Record steps `(session, step_ref, occurred_at_ms, cache_read, output,
/// reasoning)` for `producer` in one transaction through the writer, as a
/// reader would, so the post-commit feed path runs. With `fail` the
/// transaction records them and then fails, so it rolls back.
#[cfg(any(test, feature = "test-support"))]
pub fn record_steps_for_test(
    state: &SharedState,
    producer: &str,
    steps: &[TestStep<'_>],
    fail: bool,
) -> Result<(), String> {
    use astrocyte_core::fact::ChargeBasis;
    use astrocyte_core::ingest::{
        record_event_in_tx, DeliveryPath, EventRecordRequest, MeasurementInput, StepDetail,
    };
    let token = |role: &str, units: u64| MeasurementInput {
        role: role.into(),
        unit: "token".into(),
        kind: "billed".into(),
        source: format!("usage.{role}"),
        quantity_units: Some(i64::try_from(units).unwrap()),
        quantity_exponent: Some(0),
    };
    let request = |step: &TestStep<'_>| {
        let mut measurements = vec![token("input", 10), token("cache_read", step.cache_read)];
        if let Some(output) = step.output {
            measurements.push(token("output", output));
        }
        if let Some(reasoning) = step.reasoning {
            measurements.push(token("reasoning", reasoning));
        }
        EventRecordRequest {
            producer: producer.into(),
            producer_lineage: None,
            producer_ref: step.step_ref.into(),
            prior_event_ref: None,
            harness: None,
            project_root: None,
            session: Some(step.session.into()),
            run_ref: None,
            flow_ref: None,
            work_class: None,
            occurred_at_ms: Some(step.occurred_at_ms),
            emitted_at_ms: None,
            terminal_status: None,
            attempt_ordinal: None,
            vendor_request_id: None,
            provider: Some("anthropic".into()),
            model: Some("model".into()),
            account_ref: None,
            auth_selection: None,
            charge_basis: ChargeBasis::Unknown,
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements,
            step_detail: StepDetail {
                finish: None,
                context_limit: None,
            },
        }
    };
    state.ordered_write(|tx| {
        for step in steps {
            record_event_in_tx(tx, &request(step), 1, DeliveryPath::Read)
                .map_err(|error| format!("{error:?}"))?;
        }
        if fail {
            return Err("the test failed this transaction after recording".into());
        }
        Ok(((), true))
    })
}

/// One step for `record_steps_for_test`.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy)]
pub struct TestStep<'a> {
    pub session: &'a str,
    pub step_ref: &'a str,
    pub occurred_at_ms: i64,
    pub cache_read: u64,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
}

#[cfg(any(test, feature = "test-support"))]
impl<'a> TestStep<'a> {
    /// A step that happened just now, reading 100 cached tokens.
    pub fn now(session: &'a str, step_ref: &'a str) -> Self {
        Self {
            session,
            step_ref,
            occurred_at_ms: crate::state::now_ms(),
            cache_read: 100,
            output: Some(3),
            reasoning: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{leased_test_state, now_ms, test_state};
    use crate::test_support::HANG_GUARD;
    use cortexkit_usage_steps::{ReadReply, ReadRequest, SubscribeRequest};

    // Use the pinned package's bytes without depending on a machine's Cargo cache.
    macro_rules! wire_vector {
        ($path:literal) => {
            include_bytes!(concat!("../test-vectors/usage-steps-0.2.1/", $path))
        };
    }

    /// A sink that hands each notice to the test, waits for a permit before
    /// sending one, and never ends on its own.
    struct TestSink {
        sent: mpsc::UnboundedSender<Value>,
        entered: mpsc::UnboundedSender<()>,
        permits: Arc<Semaphore>,
    }

    #[async_trait]
    impl NoticeSink for TestSink {
        async fn emit(&self, notice: Vec<u8>) -> Result<(), String> {
            let _ = self.entered.send(());
            self.permits
                .acquire()
                .await
                .map_err(|error| error.to_string())?
                .forget();
            self.sent
                .send(serde_json::from_slice(&notice).unwrap())
                .map_err(|error| error.to_string())
        }

        async fn ended(&self) {
            std::future::pending::<()>().await;
        }
    }

    fn read_page(state: &SharedState, params: Value) -> Value {
        match read(state, params) {
            Ok(page) => json!(page),
            Err(refusal) => panic!("read refused {}: {}", refusal.code, refusal.message),
        }
    }

    fn wire_params(request: &impl serde::Serialize) -> Value {
        let bytes = serde_json::to_vec(request).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn check_wire_reply(body: &[u8]) {
        #[derive(Deserialize)]
        struct Reply {
            result: ReadReply,
        }
        let reply: Reply = serde_json::from_slice(body).unwrap();
        for row in &reply.result.rows {
            row.check_counts().unwrap();
        }
    }

    fn read_wire_page(state: &SharedState, request: ReadRequest) -> Value {
        read_wire_bytes(state, &serde_json::to_vec(&request).unwrap())
    }

    fn read_wire_bytes(state: &SharedState, bytes: &[u8]) -> Value {
        let request: ReadRequest = serde_json::from_slice(bytes).unwrap();
        request.check_pairing().unwrap();
        let outcome = read_outcome(state, serde_json::from_slice(bytes).unwrap());
        let HandlerOutcome::Response(body) = outcome else {
            panic!("shared read request was refused: {outcome:?}");
        };
        check_wire_reply(&body);
        let reply: Value = serde_json::from_slice(&body).unwrap();
        reply["result"].clone()
    }

    fn replay_shared_read_vectors() {
        let state = test_state();
        state
            .with_conn(|conn| {
                for (cursor, bytes) in [
                    (109, wire_vector!("read/row-every-member.json").as_slice()),
                    (110, wire_vector!("read/row-required-only.json").as_slice()),
                ] {
                    let mut row: Value = serde_json::from_slice(bytes).unwrap();
                    row.as_object_mut().unwrap().remove("cursor");
                    conn.execute(
                        "INSERT INTO step_feed(feed_cursor,harness,step_ref,source,session,committed_at_ms,row_json)
                         VALUES (?1,?2,?3,?4,?5,?6,?7)",
                        rusqlite::params![
                            cursor,
                            row["harness"].as_str().unwrap(),
                            row["step_ref"].as_str().unwrap(),
                            row["source"].as_str().unwrap(),
                            row["session"].as_str().unwrap(),
                            now_ms(),
                            row.to_string(),
                        ],
                    )
                    .map_err(|error| error.to_string())?;
                }
                Ok(())
            })
            .unwrap();
        let mut expected: Value =
            serde_json::from_slice(wire_vector!("read/reply-last-page.json")).unwrap();
        // Cursors are opaque: these seeded rows use the provider's spelling,
        // not the zero-padded spelling used by the client's examples.
        expected["rows"][0]["cursor"] = json!("c109");
        expected["rows"][1]["cursor"] = json!("c110");
        expected["through"] = json!("c110");
        for bytes in [
            wire_vector!("read/request-from-oldest.json").as_slice(),
            wire_vector!("read/request-after.json").as_slice(),
            wire_vector!("tolerant/read-request-null-members.json").as_slice(),
        ] {
            assert_eq!(read_wire_bytes(&state, bytes), expected);
        }
        let mut empty: Value =
            serde_json::from_slice(wire_vector!("read/reply-session-empty.json")).unwrap();
        empty["through"] = json!("c110");
        assert_eq!(
            read_wire_bytes(&state, wire_vector!("read/request-session.json")),
            empty
        );
        let selected = read_wire_page(
            &state,
            ReadRequest::new().for_session("example-runner", "ses_ex_f"),
        );
        assert_eq!(selected["rows"], json!([expected["rows"][0]]));
        assert_eq!(selected["next"], Value::Null);
        assert_eq!(selected["through"], "c110");
    }

    fn feed_rows(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM step_feed", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn step<'a>(session: &'a str, step_ref: &'a str, at: i64, cache_read: u64) -> TestStep<'a> {
        TestStep {
            occurred_at_ms: at,
            cache_read,
            ..TestStep::now(session, step_ref)
        }
    }

    /// A transaction that recorded steps and then rolled back appends
    /// nothing to the feed and tells no subscriber; a committed one appends
    /// its rows and tells subscribers the cursor a fresh reader sees.
    #[test]
    fn a_notice_is_sent_only_after_its_steps_commit() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);
        let told = state.step_feed().through();
        record_steps_for_test(&state, "pi", &[TestStep::now("rolled-back", "rb:1")], true)
            .unwrap_err();
        assert_eq!(
            state.step_feed().through(),
            told,
            "subscribers were told of a transaction that rolled back"
        );
        assert_eq!(feed_rows(&state), 0);
        record_steps_for_test(
            &state,
            "pi",
            &[TestStep::now("kept", "k:1"), TestStep::now("other", "o:1")],
            false,
        )
        .unwrap();
        let reader = Connection::open(directory.path().join("ledger.db")).unwrap();
        let committed = feed::through(&reader).unwrap();
        assert_eq!(state.step_feed().through(), Some(committed));
        assert_eq!(feed_rows(&state), 2);
    }

    /// Pages follow `next` in commit order until `next` is null, and every
    /// page states the same `through`.
    #[test]
    fn reads_page_through_the_feed_in_commit_order() {
        let state = test_state();
        let at = now_ms();
        for (index, step_ref) in ["a:1", "a:2", "a:3", "a:4", "a:5"].iter().enumerate() {
            let offset = i64::try_from(index).unwrap();
            record_steps_for_test(
                &state,
                "pi",
                &[step("a", step_ref, at + offset, 100)],
                false,
            )
            .unwrap();
        }
        let mut seen = Vec::new();
        let mut params = json!({"limit": 2});
        loop {
            let page = read_page(&state, params.clone());
            assert_eq!(page["through"], json!(feed::encode_cursor(5)));
            for row in page["rows"].as_array().unwrap() {
                seen.push(row["step_ref"].as_str().unwrap().to_string());
                assert_eq!(row["harness"], "pi");
                assert_eq!(row["session"], "a");
                assert_eq!(row["source"], "file");
            }
            match page["next"].as_str() {
                Some(next) => params = json!({"after": next, "limit": 2}),
                None => break,
            }
        }
        assert_eq!(seen, ["a:1", "a:2", "a:3", "a:4", "a:5"]);
        let refused = read(&state, json!({"after": "c99"})).err().unwrap();
        assert_eq!(refused.code, "invalid_request");
    }

    #[test]
    fn unfiltered_read_keeps_its_reply_bytes() {
        let state = test_state();
        let step = TestStep::now("s", "s:1");
        record_steps_for_test(&state, "pi", &[step], false).unwrap();
        let request = ReadRequest::new();
        request.check_pairing().unwrap();
        let HandlerOutcome::Response(body) = read_outcome(&state, wire_params(&request)) else {
            panic!("unfiltered read did not return a response");
        };
        check_wire_reply(&body);
        let body = String::from_utf8(body).unwrap();
        let time = format!("\"occurred_at_ms\":{}", step.occurred_at_ms);
        assert_eq!(body.matches(&time).count(), 1);
        assert_eq!(
            body.replace(&time, "\"occurred_at_ms\":0").as_bytes(),
            br#"{"result":{"next":null,"rows":[{"cache_read":100,"cursor":"c1","harness":"pi","input":10,"model":"model","occurred_at_ms":0,"output":3,"prompt":110,"prompt_dropped":false,"provider":"anthropic","session":"s","source":"file","step_ref":"s:1","verdict":"cold_start"}],"through":"c1"}}"#
        );
    }

    #[test]
    fn filtered_reads_page_only_the_sessions_latest_rows() {
        let state = test_state();
        let at = now_ms();
        for (harness, session, reference, time) in [
            ("pi", "chosen", "later", at),
            ("pi", "other", "other", at),
            ("opencode", "chosen", "other-harness", at),
            ("pi", "chosen", "earlier", at - 1_000),
            ("pi", "other", "tail", at + 1_000),
        ] {
            record_steps_for_test(
                &state,
                harness,
                &[step(session, reference, time, 100)],
                false,
            )
            .unwrap();
        }
        let first = read_wire_page(
            &state,
            ReadRequest::new().for_session("pi", "chosen").with_limit(1),
        );
        assert_eq!(first["through"], "c6", "a changed step was not sent again");
        assert_eq!(
            first["next"], "c4",
            "filtered pagination counted an unrelated row"
        );
        assert_eq!(first["rows"].as_array().unwrap().len(), 1);
        assert_eq!(first["rows"][0]["step_ref"], "earlier");
        let last = read_wire_page(
            &state,
            ReadRequest::new()
                .for_session("pi", "chosen")
                .with_limit(1)
                .after(first["next"].as_str().unwrap()),
        );
        assert_eq!(last["through"], "c6");
        assert_eq!(last["next"], Value::Null);
        assert_eq!(last["rows"].as_array().unwrap().len(), 1);
        assert_eq!(last["rows"][0]["step_ref"], "later");
        assert_eq!(last["rows"][0]["cursor"], "c5");
        assert_ne!(last["rows"][0]["verdict"], "cold_start");
        for page in [&first, &last] {
            assert_eq!(page["rows"][0]["harness"], "pi");
            assert_eq!(page["rows"][0]["session"], "chosen");
        }
        for request in [
            ReadRequest::new().for_session("pi", "chosen").after("c6"),
            ReadRequest::new().for_session("PI", "chosen"),
            ReadRequest::new().for_session("pi", "Chosen"),
        ] {
            let empty = read_wire_page(&state, request);
            assert_eq!(empty, json!({"rows": [], "next": null, "through": "c6"}));
        }
        replay_shared_read_vectors();
    }

    #[tokio::test]
    async fn feed_filters_require_a_pair_and_keep_unknown_members_malformed() {
        let state = test_state();
        let (sent, _notices) = mpsc::unbounded_channel();
        let (entered, _entered) = mpsc::unbounded_channel();
        let sink = TestSink {
            sent,
            entered,
            permits: Arc::new(Semaphore::new(0)),
        };
        let (_stop, stopping) = watch::channel(false);
        for (params, expected) in [
            (json!({"harness": "pi"}), "invalid_request"),
            (json!({"session": "s"}), "invalid_request"),
            (json!({"harness": "pi", "session": null}), "invalid_request"),
            (json!({"harness": null, "session": "s"}), "invalid_request"),
            (json!({"unknown": "s"}), "malformed"),
            (json!({"harness": 1, "session": "s"}), "malformed"),
            (
                serde_json::from_slice(wire_vector!("rejected/read-request-harness-alone.json"))
                    .unwrap(),
                "invalid_request",
            ),
            (
                serde_json::from_slice(wire_vector!(
                    "rejected/subscribe-request-session-alone.json"
                ))
                .unwrap(),
                "invalid_request",
            ),
        ] {
            let (read_params, subscribe_params) = if expected == "invalid_request" {
                let read: ReadRequest = serde_json::from_value(params.clone()).unwrap();
                let subscription: SubscribeRequest = serde_json::from_value(params).unwrap();
                assert!(read.check_pairing().is_err());
                assert!(subscription.check_pairing().is_err());
                assert!(cortexkit_usage_steps::codes::SUBSCRIBE_REFUSALS.contains(&expected));
                (wire_params(&read), wire_params(&subscription))
            } else {
                (params.clone(), params)
            };
            let refused = read(&state, read_params).err().unwrap();
            assert_eq!(refused.code, expected);
            let outcome = tokio::time::timeout(
                HANG_GUARD,
                subscribe(&state, subscribe_params, &sink, stopping.clone()),
            )
            .await
            .expect("an invalid filter opened a subscription instead of being refused");
            let HandlerOutcome::Error { code, .. } = outcome else {
                panic!("invalid filter was not refused: {outcome:?}");
            };
            assert_eq!(code, expected);
            assert_eq!(state.step_feed().open_subscriptions(), 0);
        }
        assert_eq!(
            read_page(&state, json!({"harness": null, "session": null})),
            read_page(&state, json!({}))
        );
        assert_eq!(
            read_page(&state, json!({"harness": "", "session": ""})),
            json!({"rows": [], "next": null, "through": "c0"})
        );
    }

    /// A step that arrives late, earlier in its session than a step already
    /// in the feed, changes that later step's verdict: the later step's whole
    /// row is sent again at a new cursor, and a read returns only its newest
    /// row.
    #[test]
    fn a_verdict_changed_by_a_late_earlier_step_is_sent_again() {
        let state = test_state();
        let at = now_ms();
        record_steps_for_test(&state, "pi", &[step("s", "later", at, 100)], false).unwrap();
        let first = read_page(&state, json!({}));
        assert_eq!(first["rows"][0]["verdict"], "cold_start");
        let first_cursor = first["rows"][0]["cursor"].as_str().unwrap().to_string();

        record_steps_for_test(
            &state,
            "pi",
            &[step("s", "earlier", at - 1_000, 100)],
            false,
        )
        .unwrap();
        let page = read_page(&state, json!({ "after": first_cursor }));
        let rows = page["rows"].as_array().unwrap();
        let by_ref = |step_ref: &str| {
            rows.iter()
                .find(|row| row["step_ref"] == step_ref)
                .unwrap_or_else(|| panic!("{step_ref} was not sent again: {page}"))
                .clone()
        };
        assert_eq!(by_ref("earlier")["verdict"], "cold_start");
        let later = by_ref("later");
        assert_ne!(later["verdict"], "cold_start", "{later}");
        assert_ne!(later["cursor"], json!(first_cursor));
        let whole = read_page(&state, json!({}));
        let laters: Vec<_> = whole["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["step_ref"] == "later")
            .collect();
        assert_eq!(
            laters.len(),
            1,
            "the newest row for an identity wins: {whole}"
        );
        assert_eq!(laters[0]["cursor"], later["cursor"]);
    }

    /// Once retention has deleted rows after a cursor, a read from that
    /// cursor is refused `cursor_expired` with the newest cursor in the
    /// detail.
    #[test]
    fn a_read_from_beyond_the_window_is_refused_cursor_expired() {
        let state = test_state();
        record_steps_for_test(&state, "pi", &[TestStep::now("s", "s:1")], false).unwrap();
        let held = read_page(&state, json!({"after": "c0"}));
        assert_eq!(held["rows"].as_array().unwrap().len(), 1);
        let a_day_later = now_ms() + feed::FEED_WINDOW_MS + 60_000;
        state
            .with_conn(|conn| {
                feed::append_changes(conn, a_day_later, &[]).map_err(|error| error.to_string())
            })
            .unwrap();
        let refused = read(&state, json!({"after": "c0"})).err().unwrap();
        assert_eq!(refused.code, "cursor_expired");
        assert_eq!(refused.detail, Some(json!({"through": held["through"]})));
        let fresh = read_page(&state, json!({}));
        assert_eq!(fresh["rows"], json!([]));
        assert_eq!(fresh["through"], held["through"]);
        let refused = read(
            &state,
            json!({"after": "c0", "harness": "pi", "session": "s"}),
        )
        .err()
        .unwrap();
        assert_eq!(refused.code, "cursor_expired");
        assert_eq!(refused.detail, Some(json!({"through": held["through"]})));
    }

    struct ByteSink(mpsc::UnboundedSender<Vec<u8>>);

    #[async_trait]
    impl NoticeSink for ByteSink {
        async fn emit(&self, notice: Vec<u8>) -> Result<(), String> {
            self.0.send(notice).map_err(|error| error.to_string())
        }

        async fn ended(&self) {
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test]
    async fn filtered_subscriptions_only_queue_their_sessions_appends() {
        let state = test_state();
        let (stop, stopping) = watch::channel(false);
        let (filtered_tx, mut filtered) = mpsc::unbounded_channel();
        let (all_tx, mut all) = mpsc::unbounded_channel();
        let (vector_tx, mut vector_filtered) = mpsc::unbounded_channel();
        let (vector_all_tx, mut vector_all) = mpsc::unbounded_channel();
        let mut serving = Vec::new();
        for (bytes, sender) in [
            (
                serde_json::to_vec(&SubscribeRequest::new().for_session("pi", "chosen")).unwrap(),
                filtered_tx,
            ),
            (
                serde_json::to_vec(&SubscribeRequest::new()).unwrap(),
                all_tx,
            ),
            (
                wire_vector!("subscribe/request-session.json").to_vec(),
                vector_tx,
            ),
            (
                wire_vector!("subscribe/request.json").to_vec(),
                vector_all_tx,
            ),
        ] {
            let request: SubscribeRequest = serde_json::from_slice(&bytes).unwrap();
            request.check_pairing().unwrap();
            let params = serde_json::from_slice(&bytes).unwrap();
            let state = state.clone();
            let stopping = stopping.clone();
            serving.push(tokio::spawn(async move {
                subscribe(&state, params, &ByteSink(sender), stopping).await
            }));
        }
        tokio::time::timeout(HANG_GUARD, async {
            while state.step_feed().lock().subscribers.len() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscriptions never registered");
        for (harness, session, reference) in [
            ("pi", "other", "other"),
            ("opencode", "chosen", "other-harness"),
        ] {
            record_steps_for_test(&state, harness, &[TestStep::now(session, reference)], false)
                .unwrap();
            // Publication is synchronous; an unrelated append must not even
            // occupy a filtered queue slot or cause that subscriber to lag.
            let hub = state.step_feed().lock();
            for subscriber in hub.subscribers.iter().filter(|s| s.filter.is_some()) {
                assert_eq!(
                    subscriber.queue.capacity(),
                    SUBSCRIBER_QUEUE,
                    "an unrelated append occupied the filtered queue"
                );
            }
        }
        let unfiltered = tokio::time::timeout(HANG_GUARD, all.recv())
            .await
            .expect("unfiltered subscription was not notified")
            .unwrap();
        assert_eq!(unfiltered, br#"{"through":"c2"}"#);
        let unfiltered_vector = tokio::time::timeout(HANG_GUARD, vector_all.recv())
            .await
            .expect("unfiltered vector subscription was not notified")
            .unwrap();
        assert_eq!(unfiltered_vector, br#"{"through":"c2"}"#);
        assert!(filtered.try_recv().is_err());
        assert!(vector_filtered.try_recv().is_err());
        record_steps_for_test(&state, "pi", &[TestStep::now("chosen", "own")], false).unwrap();
        let own = tokio::time::timeout(HANG_GUARD, filtered.recv())
            .await
            .expect("filtered subscription was not notified")
            .unwrap();
        assert_eq!(own, br#"{"through":"c3"}"#);
        assert!(vector_filtered.try_recv().is_err());
        record_steps_for_test(
            &state,
            "opencode",
            &[TestStep::now("ses_demo01", "vector-own")],
            false,
        )
        .unwrap();
        let vector_notice = tokio::time::timeout(HANG_GUARD, vector_filtered.recv())
            .await
            .expect("filtered vector subscription was not notified")
            .unwrap();
        assert_eq!(vector_notice, br#"{"through":"c4"}"#);
        stop.send(true).unwrap();
        for task in serving {
            let outcome = tokio::time::timeout(HANG_GUARD, task)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(outcome, HandlerOutcome::Streamed));
        }
        assert_eq!(state.step_feed().open_subscriptions(), 0);
    }

    /// Served rows report `output` with reasoning inside it, on the feed and
    /// on `usage.cache.steps` alike, while the ledger keeps them disjoint. A
    /// step that stored only reasoning reports no output.
    #[test]
    fn served_output_includes_reasoning_on_both_reads() {
        let state = test_state();
        let at = now_ms();
        let with = TestStep {
            output: Some(30),
            reasoning: Some(12),
            ..step("s", "with", at, 100)
        };
        let alone = TestStep {
            output: None,
            reasoning: Some(5),
            ..step("s", "alone", at + 1, 100)
        };
        record_steps_for_test(&state, "pi", &[with, alone], false).unwrap();
        let stored: i64 = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT quantity_units FROM measurement WHERE role='output'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            stored, 30,
            "the ledger keeps output disjoint from reasoning"
        );
        let feed_page = read_page(&state, json!({}));
        let history = crate::ops::dispatch(
            &state,
            crate::ops::OP_CACHE_STEPS,
            json!({"harness": "pi", "session": "s"}),
        )
        .unwrap();
        for (read, rows) in [("feed", &feed_page["rows"]), ("history", &history["steps"])] {
            let rows = rows.as_array().unwrap();
            assert_eq!(
                rows[0]["output"],
                json!(42),
                "{read} served disjoint output"
            );
            assert_eq!(rows[0]["reasoning"], json!(12), "{read}");
            assert!(rows[1].get("output").is_none(), "{read}: {}", rows[1]);
            assert_eq!(rows[1]["reasoning"], json!(5), "{read}");
        }
    }

    /// A subscriber whose sends are held up while more commits arrive than
    /// its queue holds ends `lagged`, naming the last cursor it was told.
    #[tokio::test]
    async fn a_subscriber_that_falls_behind_its_queue_ends_lagged() {
        let state = test_state();
        let (sent_tx, mut sent) = mpsc::unbounded_channel();
        let (entered_tx, mut entered) = mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(0));
        let sink = TestSink {
            sent: sent_tx,
            entered: entered_tx,
            permits: Arc::clone(&permits),
        };
        let (_stop, stopping) = watch::channel(false);
        let subscribing = state.clone();
        let serving =
            tokio::spawn(async move { subscribe(&subscribing, json!({}), &sink, stopping).await });
        tokio::time::timeout(HANG_GUARD, async {
            while state.step_feed().open_subscriptions() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the subscription never opened");
        record_steps_for_test(&state, "pi", &[TestStep::now("s", "s:0")], false).unwrap();
        // The subscriber is now sending its first notice and is held there
        // while more commits arrive than its queue holds.
        tokio::time::timeout(HANG_GUARD, entered.recv())
            .await
            .expect("the subscriber never tried to send")
            .unwrap();
        let first = state.step_feed().through().unwrap();
        for index in 1..=SUBSCRIBER_QUEUE + 1 {
            let step_ref = format!("s:{index}");
            record_steps_for_test(&state, "pi", &[TestStep::now("s", &step_ref)], false).unwrap();
        }
        permits.add_permits(1_000);
        let told = tokio::time::timeout(HANG_GUARD, sent.recv())
            .await
            .expect("the held notice was never sent")
            .unwrap();
        assert_eq!(told, json!({"through": feed::encode_cursor(first)}));
        let ended = tokio::time::timeout(HANG_GUARD, serving)
            .await
            .expect("the subscription did not end")
            .unwrap();
        let HandlerOutcome::ErrorWithDetail { code, detail, .. } = ended else {
            panic!("a lagging subscriber did not end lagged: {ended:?}");
        };
        assert_eq!(code, "lagged");
        assert_eq!(detail, json!({"through": feed::encode_cursor(first)}));
        assert_eq!(state.step_feed().open_subscriptions(), 0);
    }

    #[test]
    fn only_local_apps_and_callosum_phones_may_read_the_feed() {
        let callosum = Principal::Reserved {
            module_id: "callosum".into(),
        };
        assert!(may_read_feed(Some(&Principal::Direct), "anything"));
        assert!(may_read_feed(Some(&callosum), "fed-phone:abc"));
        assert!(!may_read_feed(Some(&callosum), "fed-sim:abc"));
        assert!(!may_read_feed(Some(&callosum), "fed-computer:abc"));
        assert!(!may_read_feed(
            Some(&Principal::Unverified),
            "fed-phone:abc"
        ));
        assert!(!may_read_feed(None, "fed-phone:abc"));
    }
}
