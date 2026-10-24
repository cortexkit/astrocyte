//! `usage.cache.subscribe`: live notices that a session gained cache steps.
//!
//! After every committed write that adds steps (harness events or BROCA
//! steps), the writer publishes one notice into a bounded in-memory log:
//! `{"cursor": <n>, "sessions": [{"harness": <h>, "session": <s>}]}`, where
//! `cursor` is the shared step cursor after the commit and `sessions` the
//! sessions whose steps that commit added. A notice never carries step
//! bodies; the app reads them with `usage.cache.steps` and `after_cursor`.
//!
//! A subscription keeps only its position in the log, so the module never
//! buffers notices per subscriber; the helper's bounded egress queue is the
//! only queue, and its back-pressure is what slows a subscriber down. When
//! the log drops a notice a subscriber has not yet sent, the subscriber has
//! fallen behind, and its stream ends with the typed error `lagged`. The
//! client then subscribes again with the last cursor it saw.
//!
//! The log lives in memory only. After a restart it starts empty at the
//! store's current cursor, so a client resuming from an older cursor gets one
//! `{"resync": true, "cursor": <n>}` notice, re-reads the sessions it shows,
//! and continues from `cursor`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subc_client_rs::{async_trait, HandlerOutcome, RequestCtx};
use tokio::sync::{watch, Semaphore};

use crate::state::SharedState;

/// Notices kept for resuming subscribers. Astrocyte commits at most one
/// step-adding transaction per refresher pass, and the fast lane runs every
/// 2 seconds, so 1,024 notices cover more than half an hour of continuous
/// activity: long enough for a phone that lost its connection to resume
/// without a resync. A notice holds at most `MAX_NOTICE_SESSIONS` session
/// names, so the log stays within a few megabytes at its fullest, and is
/// usually a few hundred kilobytes.
pub const NOTICE_LOG_CAPACITY: usize = 1_024;

/// Sessions named in one notice. A commit that adds steps to more sessions
/// than this (a first full pass after a long stop) publishes a resync notice
/// instead, telling clients to re-read their sessions.
pub const MAX_NOTICE_SESSIONS: usize = 64;

/// Subscriptions open at once. Each holds a request slot of the serve
/// helper for as long as it is open; one more is refused
/// `subscription_capacity`.
pub const MAX_CACHE_SUBSCRIPTIONS: usize = 16;

/// Notices sent per visit to the log, so a long replay does not hold the
/// log's lock while waiting on a slow subscriber.
const REPLAY_BATCH: usize = 64;

/// `usage.cache.subscribe` params: the cursor to resume after, if any.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheSubscribeRequest {
    #[serde(default)]
    pub after_cursor: Option<i64>,
}

/// One session in a notice: the harness producer name (`broca` for BROCA)
/// and the session identity, as `usage.cache.steps` takes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NoticeSession {
    pub harness: String,
    pub session: String,
}

#[derive(Serialize)]
struct NoticeBody<'a> {
    cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    sessions: Option<&'a [NoticeSession]>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    resync: bool,
}

fn encode(cursor: i64, sessions: Option<&[NoticeSession]>) -> Arc<[u8]> {
    let body = NoticeBody {
        cursor,
        resync: sessions.is_none(),
        sessions,
    };
    serde_json::to_vec(&body)
        .expect("a notice of integers and strings always encodes")
        .into()
}

#[derive(Clone)]
struct Notice {
    cursor: i64,
    body: Arc<[u8]>,
}

#[derive(Default)]
struct LogState {
    /// The shared step cursor after the last commit published, or `None`
    /// before the first publication at startup.
    high_water: Option<i64>,
    /// Every notice after this cursor is still in `notices`. It is the
    /// cursor of the last notice dropped, or the high water at startup.
    floor: i64,
    notices: VecDeque<Notice>,
}

/// The bounded notice log and the wake-up signal for open subscriptions.
pub struct NoticeLog {
    state: Mutex<LogState>,
    capacity: std::sync::atomic::AtomicUsize,
    published: watch::Sender<u64>,
    slots: Arc<Semaphore>,
}

impl Default for NoticeLog {
    fn default() -> Self {
        Self {
            state: Mutex::new(LogState::default()),
            capacity: std::sync::atomic::AtomicUsize::new(NOTICE_LOG_CAPACITY),
            published: watch::channel(0).0,
            slots: Arc::new(Semaphore::new(MAX_CACHE_SUBSCRIPTIONS)),
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
    /// The log dropped notices this subscriber had not sent; `cursor` is
    /// the last one it was sent.
    Lagged { cursor: i64 },
}

/// Where a new subscription starts, decided under the log's lock.
enum Start {
    /// Send notices after this cursor.
    At(i64),
    /// Send one resync notice at this cursor, then continue from it.
    Resync(i64),
}

impl NoticeLog {
    fn lock(&self) -> std::sync::MutexGuard<'_, LogState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Publish what the commits since the last call added. Called only after
    /// a commit, on the writer's connection under the writer fence (see
    /// `SharedState::publish_cache_notices`), so the steps read here are
    /// committed and nothing commits meanwhile. The first call only records
    /// the store's cursor.
    pub(crate) fn publish(&self, conn: &Connection) -> rusqlite::Result<()> {
        let mut log = self.lock();
        let high_water = astrocyte_core::cache_diagnostics::last_event_key(conn)?;
        let Some(previous) = log.high_water else {
            log.high_water = Some(high_water);
            log.floor = high_water;
            return Ok(());
        };
        if high_water <= previous {
            return Ok(());
        }
        let sessions = sessions_recorded_between(conn, previous, high_water)?;
        log.high_water = Some(high_water);
        if sessions.is_empty() {
            return Ok(());
        }
        let body = if sessions.len() > MAX_NOTICE_SESSIONS {
            encode(high_water, None)
        } else {
            encode(high_water, Some(&sessions))
        };
        log.notices.push_back(Notice {
            cursor: high_water,
            body,
        });
        let capacity = self.capacity.load(std::sync::atomic::Ordering::Relaxed);
        while log.notices.len() > capacity {
            if let Some(dropped) = log.notices.pop_front() {
                log.floor = dropped.cursor;
            }
        }
        drop(log);
        self.published
            .send_modify(|count| *count = count.wrapping_add(1));
        Ok(())
    }

    /// Where a subscription resuming after `after_cursor` starts, or the
    /// refusal for a cursor this module has never issued.
    fn start(&self, after_cursor: Option<i64>) -> Result<Start, (String, String)> {
        let log = self.lock();
        let Some(high_water) = log.high_water else {
            return Err((
                "internal".into(),
                "the cache notice log has not been initialized".into(),
            ));
        };
        match after_cursor {
            None => Ok(Start::At(high_water)),
            Some(cursor) if cursor > high_water => Err((
                "invalid_request".into(),
                format!(
                    "after_cursor {cursor} is newer than the newest cursor recorded ({high_water})"
                ),
            )),
            Some(cursor) if cursor < log.floor => Ok(Start::Resync(high_water)),
            Some(cursor) => Ok(Start::At(cursor)),
        }
    }

    /// The notices after `position`, at most `REPLAY_BATCH` of them, or
    /// `None` when the log has dropped some of them.
    fn after(&self, position: i64) -> Option<Vec<Notice>> {
        let log = self.lock();
        if position < log.floor {
            return None;
        }
        Some(
            log.notices
                .iter()
                .filter(|notice| notice.cursor > position)
                .take(REPLAY_BATCH)
                .cloned()
                .collect(),
        )
    }

    /// The bodies of every notice in the log, oldest first.
    #[cfg(any(test, feature = "test-support"))]
    pub fn notices_for_test(&self) -> Vec<Value> {
        self.lock()
            .notices
            .iter()
            .map(|notice| serde_json::from_slice(&notice.body).unwrap())
            .collect()
    }

    /// Keep at most `capacity` notices from now on.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_capacity_for_test(&self, capacity: usize) {
        self.capacity
            .store(capacity, std::sync::atomic::Ordering::Relaxed);
    }

    /// Subscriptions open now.
    pub fn open_subscriptions(&self) -> usize {
        MAX_CACHE_SUBSCRIPTIONS - self.slots.available_permits()
    }

    async fn stream(
        &self,
        start: Start,
        sink: &dyn NoticeSink,
    ) -> Result<StreamEnd, (String, String)> {
        // Subscribe before the first read, so a notice published between
        // that read and the first wait still wakes this subscription.
        let mut published = self.published.subscribe();
        published.borrow_and_update();
        let mut position = match start {
            Start::At(cursor) => cursor,
            Start::Resync(cursor) => {
                if sink.emit(encode(cursor, None).to_vec()).await.is_err() {
                    return Ok(StreamEnd::Gone);
                }
                cursor
            }
        };
        loop {
            let Some(batch) = self.after(position) else {
                return Ok(StreamEnd::Lagged { cursor: position });
            };
            let full = batch.len() == REPLAY_BATCH;
            for notice in batch {
                if sink.emit(notice.body.to_vec()).await.is_err() {
                    return Ok(StreamEnd::Gone);
                }
                position = notice.cursor;
            }
            if full {
                continue;
            }
            if published.changed().await.is_err() {
                return Ok(StreamEnd::Gone);
            }
            published.borrow_and_update();
        }
    }
}

/// Serve one `usage.cache.subscribe` request to the end of its stream.
///
/// A refusal before the first notice is an error reply. The stream then
/// ends in one of four ways: the subscriber cancels (`cancelled`), its
/// route closes (no reply, the route is gone), astrocyte stops or drains
/// (a clean stream end), or the subscriber falls behind the log (`lagged`,
/// with the last cursor it was sent in `detail.cursor`).
pub async fn subscribe(
    state: &SharedState,
    params: Value,
    sink: &dyn NoticeSink,
    mut stopping: watch::Receiver<bool>,
) -> HandlerOutcome {
    let request: CacheSubscribeRequest = match serde_json::from_value(params) {
        Ok(request) => request,
        Err(error) => {
            return HandlerOutcome::Error {
                code: "malformed".into(),
                message: format!("cache subscribe params: {error}"),
            }
        }
    };
    if *stopping.borrow() {
        return HandlerOutcome::Error {
            code: "draining".into(),
            message: "astrocyte is stopping; subscribe again once it is back".into(),
        };
    }
    let log = state.cache_notices();
    let Ok(_slot) = Arc::clone(&log.slots).try_acquire_owned() else {
        return HandlerOutcome::Error {
            code: "subscription_capacity".into(),
            message: format!("at most {MAX_CACHE_SUBSCRIPTIONS} cache subscriptions may be open"),
        };
    };
    let start = match log.start(request.after_cursor) {
        Ok(start) => start,
        Err((code, message)) => return HandlerOutcome::Error { code, message },
    };
    let ended = tokio::select! {
        biased;
        () = sink.ended() => {
            return HandlerOutcome::Error {
                code: "cancelled".into(),
                message: "subscription cancelled".into(),
            };
        }
        _ = stopping.wait_for(|stopping| *stopping) => return HandlerOutcome::Streamed,
        ended = log.stream(start, sink) => ended,
    };
    match ended {
        Ok(StreamEnd::Gone) => HandlerOutcome::Streamed,
        Ok(StreamEnd::Lagged { cursor }) => HandlerOutcome::ErrorWithDetail {
            code: "lagged".into(),
            message: "the subscriber fell behind the notice log; subscribe again with its \
                      last cursor"
                .into(),
            detail: serde_json::json!({ "cursor": cursor }),
        },
        Err((code, message)) => HandlerOutcome::Error { code, message },
    }
}

/// The sessions with a step whose cursor is in `(after, through]`, harness
/// sessions first, each once, in name order.
fn sessions_recorded_between(
    conn: &Connection,
    after: i64,
    through: i64,
) -> rusqlite::Result<Vec<NoticeSession>> {
    let harnesses = astrocyte_core::cache_diagnostics::CACHE_HARNESSES
        .iter()
        .filter(|harness| **harness != "broca")
        .map(|harness| format!("'{harness}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT e.producer,e.session FROM cache_step_sequence s
           JOIN usage_event e ON e.event_key=s.event_key
          WHERE s.cursor>?1 AND s.cursor<=?2 AND e.session IS NOT NULL
            AND e.producer IN ({harnesses})
         UNION
         SELECT 'broca',b.session FROM cache_step_sequence s
           JOIN broca_step b ON b.step_key=s.broca_step_key
          WHERE s.cursor>?1 AND s.cursor<=?2 AND b.session IS NOT NULL
         ORDER BY 1,2"
    );
    let mut statement = conn.prepare_cached(&sql)?;
    let rows = statement.query_map([after, through], |row| {
        Ok(NoticeSession {
            harness: row.get(0)?,
            session: row.get(1)?,
        })
    })?;
    rows.collect()
}

/// Record one harness step for `session` through the writer, as a reader
/// would, so the post-commit notice path runs. For serving tests.
#[cfg(any(test, feature = "test-support"))]
pub fn record_step_for_test(state: &SharedState, producer: &str, session: &str, step_ref: &str) {
    record_steps_for_test(state, producer, &[(session, step_ref)], false).unwrap();
}

/// Record steps `(session, step_ref)` in one transaction through the
/// writer. With `fail` the transaction records them and then fails, so it
/// rolls back.
#[cfg(any(test, feature = "test-support"))]
pub fn record_steps_for_test(
    state: &SharedState,
    producer: &str,
    steps: &[(&str, &str)],
    fail: bool,
) -> Result<(), String> {
    use astrocyte_core::fact::ChargeBasis;
    use astrocyte_core::ingest::{
        record_event_in_tx, DeliveryPath, EventRecordRequest, MeasurementInput, StepDetail,
    };
    let request = |session: &str, step_ref: &str| EventRecordRequest {
        producer: producer.into(),
        producer_lineage: None,
        producer_ref: step_ref.into(),
        prior_event_ref: None,
        harness: None,
        project_root: None,
        session: Some(session.into()),
        run_ref: None,
        flow_ref: None,
        work_class: None,
        occurred_at_ms: Some(1_000),
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
        measurements: vec![MeasurementInput {
            role: "input".into(),
            unit: "token".into(),
            kind: "billed".into(),
            source: "usage.input".into(),
            quantity_units: Some(10),
            quantity_exponent: Some(0),
        }],
        step_detail: StepDetail {
            finish: None,
            context_limit: None,
        },
    };
    state.ordered_write(|tx| {
        for (session, step_ref) in steps {
            record_event_in_tx(tx, &request(session, step_ref), 1, DeliveryPath::Read)
                .map_err(|error| format!("{error:?}"))?;
        }
        if fail {
            return Err("the test failed this transaction after recording".into());
        }
        Ok(((), true))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{leased_test_state, test_state};
    use crate::test_support::HANG_GUARD;
    use serde_json::json;

    /// A sink that hands each notice to the test, waits for a permit before
    /// sending one, and ends when the test says so.
    struct TestSink {
        sent: tokio::sync::mpsc::UnboundedSender<Value>,
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        permits: Arc<Semaphore>,
        ended: watch::Receiver<bool>,
    }

    struct SinkControl {
        sent: tokio::sync::mpsc::UnboundedReceiver<Value>,
        entered: tokio::sync::mpsc::UnboundedReceiver<()>,
        permits: Arc<Semaphore>,
        end: watch::Sender<bool>,
    }

    fn sink(permits: usize) -> (TestSink, SinkControl) {
        let (sent_tx, sent) = tokio::sync::mpsc::unbounded_channel();
        let (entered_tx, entered) = tokio::sync::mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(permits));
        let (end, ended) = watch::channel(false);
        (
            TestSink {
                sent: sent_tx,
                entered: entered_tx,
                permits: Arc::clone(&permits),
                ended,
            },
            SinkControl {
                sent,
                entered,
                permits,
                end,
            },
        )
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
            let mut ended = self.ended.clone();
            let _ = ended.wait_for(|ended| *ended).await;
        }
    }

    impl SinkControl {
        async fn next(&mut self) -> Value {
            tokio::time::timeout(HANG_GUARD, self.sent.recv())
                .await
                .expect("no notice reached the subscriber")
                .expect("the subscription ended")
        }
    }

    fn open(
        state: &SharedState,
        params: Value,
        sink: TestSink,
    ) -> (tokio::task::JoinHandle<HandlerOutcome>, watch::Sender<bool>) {
        let (stop, stopping) = watch::channel(false);
        let state = state.clone();
        let serving = tokio::spawn(async move { subscribe(&state, params, &sink, stopping).await });
        (serving, stop)
    }

    async fn outcome(serving: tokio::task::JoinHandle<HandlerOutcome>) -> HandlerOutcome {
        tokio::time::timeout(HANG_GUARD, serving)
            .await
            .expect("the subscription did not end")
            .unwrap()
    }

    fn session(notice: &Value) -> Vec<(String, String)> {
        notice["sessions"]
            .as_array()
            .unwrap_or_else(|| panic!("no sessions in {notice}"))
            .iter()
            .map(|session| {
                (
                    session["harness"].as_str().unwrap().to_string(),
                    session["session"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn pi(sessions: &[&str]) -> Vec<(String, String)> {
        sessions
            .iter()
            .map(|session| ("pi".to_string(), (*session).to_string()))
            .collect()
    }

    /// A transaction that recorded steps and then rolled back publishes
    /// nothing; a committed one publishes one notice naming each session it
    /// added steps to, once, at the cursor a fresh reader sees after the
    /// commit. A committed event from a producer that records no steps
    /// publishes nothing either.
    #[test]
    fn a_notice_is_published_only_after_its_steps_commit() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);
        record_steps_for_test(&state, "pi", &[("rolled-back", "rb:1")], true).unwrap_err();
        assert_eq!(
            state.cache_notices().notices_for_test(),
            Vec::<Value>::new(),
            "a rolled-back transaction published a notice"
        );
        record_steps_for_test(
            &state,
            "pi",
            &[("kept", "k:1"), ("kept", "k:2"), ("other", "o:1")],
            false,
        )
        .unwrap();
        record_steps_for_test(&state, "goose", &[("kept", "g:1")], false).unwrap();
        let notices = state.cache_notices().notices_for_test();
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert_eq!(session(&notices[0]), pi(&["kept", "other"]));
        let reader = Connection::open(directory.path().join("ledger.db")).unwrap();
        let goose_cursor = astrocyte_core::cache_diagnostics::last_event_key(&reader).unwrap();
        assert_eq!(notices[0]["cursor"], json!(goose_cursor - 1));
        assert!(notices[0].get("resync").is_none(), "{}", notices[0]);
    }

    /// A cursor inside the log replays the notices after it, in order, and
    /// the stream then carries new notices as they are published.
    #[tokio::test]
    async fn a_cursor_inside_the_log_replays_the_newer_notices() {
        let state = test_state();
        for name in ["a", "b", "c"] {
            record_step_for_test(&state, "pi", name, &format!("{name}:1"));
        }
        let first = state.cache_notices().notices_for_test()[0]["cursor"].clone();
        let (sink, mut control) = sink(1_000);
        let (serving, stop) = open(&state, json!({"after_cursor": first}), sink);
        assert_eq!(session(&control.next().await), pi(&["b"]));
        assert_eq!(session(&control.next().await), pi(&["c"]));
        record_step_for_test(&state, "pi", "d", "d:1");
        assert_eq!(session(&control.next().await), pi(&["d"]));
        stop.send_replace(true);
        assert_eq!(outcome(serving).await, HandlerOutcome::Streamed);
    }

    /// A cursor older than the log gets exactly one resync notice at the
    /// newest cursor, then only notices published after it.
    #[tokio::test]
    async fn a_cursor_older_than_the_log_gets_one_resync_then_live_notices() {
        let state = test_state();
        state.cache_notices().set_capacity_for_test(2);
        for name in ["a", "b", "c", "d"] {
            record_step_for_test(&state, "pi", name, &format!("{name}:1"));
        }
        let kept = state.cache_notices().notices_for_test();
        assert_eq!(kept.len(), 2);
        let newest = kept[1]["cursor"].as_i64().unwrap();
        let (sink, mut control) = sink(1_000);
        let (serving, stop) = open(&state, json!({"after_cursor": 0}), sink);
        assert_eq!(
            control.next().await,
            json!({"cursor": newest, "resync": true})
        );
        record_step_for_test(&state, "pi", "e", "e:1");
        let live = control.next().await;
        assert_eq!(session(&live), pi(&["e"]));
        assert!(live["cursor"].as_i64().unwrap() > newest);
        stop.send_replace(true);
        assert_eq!(outcome(serving).await, HandlerOutcome::Streamed);
    }

    /// A cursor this module never issued is refused before anything is sent.
    #[tokio::test]
    async fn a_cursor_newer_than_the_newest_is_refused() {
        let state = test_state();
        record_step_for_test(&state, "pi", "a", "a:1");
        let newest = state.cache_notices().notices_for_test()[0]["cursor"]
            .as_i64()
            .unwrap();
        let (sink, mut control) = sink(1_000);
        let (serving, _stop) = open(&state, json!({"after_cursor": newest + 1}), sink);
        let HandlerOutcome::Error { code, .. } = outcome(serving).await else {
            panic!("a future cursor was accepted");
        };
        assert_eq!(code, "invalid_request");
        assert!(control.sent.try_recv().is_err(), "a notice was sent");
    }

    /// A subscriber held back while the log drops notices it has not sent
    /// ends `lagged`, naming the last cursor it was sent.
    #[tokio::test]
    async fn a_subscriber_that_falls_behind_the_log_ends_lagged() {
        let state = test_state();
        state.cache_notices().set_capacity_for_test(2);
        let (sink, mut control) = sink(0);
        let (serving, _stop) = open(&state, json!({}), sink);
        tokio::time::timeout(HANG_GUARD, async {
            while state.cache_notices().open_subscriptions() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the subscription never opened");
        record_step_for_test(&state, "pi", "a", "a:1");
        // The subscriber is now sending the first notice and is held there
        // while three more commits push it out of the two-notice log.
        tokio::time::timeout(HANG_GUARD, control.entered.recv())
            .await
            .expect("the subscriber never tried to send")
            .unwrap();
        for name in ["b", "c", "d"] {
            record_step_for_test(&state, "pi", name, &format!("{name}:1"));
        }
        control.permits.add_permits(1_000);
        let sent = control.next().await;
        assert_eq!(session(&sent), pi(&["a"]));
        let ended = outcome(serving).await;
        assert_eq!(
            ended,
            HandlerOutcome::ErrorWithDetail {
                code: "lagged".into(),
                message: "the subscriber fell behind the notice log; subscribe again with its \
                          last cursor"
                    .into(),
                detail: json!({"cursor": sent["cursor"]}),
            }
        );
        assert!(
            control.sent.try_recv().is_err(),
            "a notice followed the lag"
        );
    }

    /// A subscriber that cancels ends the subscription with `cancelled`
    /// and frees its slot.
    #[tokio::test]
    async fn a_cancelled_subscription_ends() {
        let state = test_state();
        // Resume after the current cursor, so the notice below is sent
        // whether or not the subscription has started by then.
        let newest = state
            .with_conn(|conn| {
                astrocyte_core::cache_diagnostics::last_event_key(conn)
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        let (sink, mut control) = sink(1_000);
        let (serving, _stop) = open(&state, json!({ "after_cursor": newest }), sink);
        record_step_for_test(&state, "pi", "a", "a:1");
        assert_eq!(session(&control.next().await), pi(&["a"]));
        control.end.send_replace(true);
        let HandlerOutcome::Error { code, .. } = outcome(serving).await else {
            panic!("a cancelled subscription did not end cancelled");
        };
        assert_eq!(code, "cancelled");
        assert_eq!(state.cache_notices().open_subscriptions(), 0);
    }
}
