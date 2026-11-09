//! Acknowledged media usage, drained only on full refresher ticks.

use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use astrocyte_core::ingest::{record_media_event_in_tx, RecordDisposition};
use cortexkit_role_media::usage::{
    self, UsageAckAnswer, UsageAckRequest, UsageReadAnswer, UsageReadRequest,
};
use rusqlite::{params, OptionalExtension};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use subc_client_rs::{
    CallError, CallOptions, ConsumerOptions, NotSentCause, RouteHandle, SubcConsumer,
};
use subc_protocol::{error_codes, RouteTarget, SUBC_MODULE_ID_ENV};

use crate::refresher::{harness_drain, SetAsideRecord, SourceDrainError};
use crate::state::SharedState;

pub(crate) const SOURCE_KEY: &str = "colliculus";
const BUDGET: Duration = Duration::from_secs(10);
const LIMIT: u64 = 1_000;

#[derive(Debug, PartialEq, Eq)]
enum FeedError {
    Absent,
    Transient(String),
    Failed(String),
    Stopped,
}

impl From<String> for FeedError {
    fn from(reason: String) -> Self {
        Self::Failed(reason)
    }
}

fn refusal(code: &str, detail: &Value) -> FeedError {
    match code {
        error_codes::UNKNOWN_MODULE | error_codes::MODULE_REMOVED => FeedError::Absent,
        error_codes::MODULE_WARMING
        | error_codes::MODULE_RELOADING
        | error_codes::UNKNOWN_CHANNEL
        | error_codes::STALE_ROUTE_EPOCH => FeedError::Transient(code.into()),
        "bad_consumer_identity" => FeedError::Failed("colliculus identity refused".into()),
        _ => FeedError::Failed(format!("colliculus refusal {code}: {detail}")),
    }
}

fn call_options() -> CallOptions {
    // The SDK normally waits through long module restarts. Keep its backoff and
    // implicit launch identity, but bound retries to this reader's tick budget.
    CallOptions {
        timeout: BUDGET,
        route_retry_deadline: BUDGET,
        ..CallOptions::default()
    }
}

fn sdk_error(error: CallError) -> FeedError {
    let cause = error.not_sent_cause();
    let code = error
        .route_open_refusal()
        .map(|body| body.code.as_str())
        .or_else(|| error.code())
        .or(match &cause {
            Some(
                NotSentCause::RouteOpenRefused { code }
                | NotSentCause::DaemonRefusedBeforeForward { code },
            ) => Some(code.as_str()),
            _ => None,
        });
    // A daemon refusal can echo the route's launch credential. Retain only the
    // known structural codes, never its message, detail or debug rendering.
    match code {
        Some(
            code @ (error_codes::UNKNOWN_MODULE
            | error_codes::MODULE_REMOVED
            | error_codes::MODULE_WARMING
            | error_codes::MODULE_RELOADING
            | error_codes::UNKNOWN_CHANNEL
            | error_codes::STALE_ROUTE_EPOCH
            | error_codes::TARGET_UNAVAILABLE
            | "feed_not_admitted"
            | "bad_consumer_identity"),
        ) => refusal(code, &Value::Null),
        _ => FeedError::Failed("colliculus operation unavailable".into()),
    }
}

fn check_page_structure(page: &UsageReadAnswer) -> Result<(), FeedError> {
    // The contract's answer check also validates every record. Check only the
    // envelope here so one unreadable entry cannot stall the whole feed.
    if page.entries.len() > usize::try_from(LIMIT).expect("small page limit") {
        return Err(FeedError::Failed(
            "media page exceeds requested limit".into(),
        ));
    }
    let mut previous = 0;
    for entry in &page.entries {
        if entry.seq <= previous {
            return Err(FeedError::Failed(
                "invalid media page: sequence numbers must strictly increase from zero".into(),
            ));
        }
        previous = entry.seq;
    }
    if page.last_seq != page.entries.last().map(|entry| entry.seq)
        || serde_json::to_vec(page)
            .map_err(|error| format!("invalid media page: {error}"))?
            .len()
            > cortexkit_role_media::MESSAGE_MAX
    {
        return Err(FeedError::Failed(
            "invalid media page: last_seq mismatch or message too large".into(),
        ));
    }
    Ok(())
}

struct Route {
    consumer: SubcConsumer,
    handle: RouteHandle,
}

impl Route {
    async fn open(path: &Path) -> Result<Self, FeedError> {
        let consumer = tokio::time::timeout(
            BUDGET,
            SubcConsumer::connect(path, ConsumerOptions::default()),
        )
        .await
        .map_err(|_| "colliculus connection timeout".to_string())?
        .map_err(|_| "colliculus daemon connection unavailable".to_string())?;
        let identity =
            serde_json::from_value(json!({"harness":"ck-astrocyte","session":"media-usage"}))
                .map_err(|_| "colliculus route identity invalid".to_string())?;
        let handle = consumer
            .open_route(
                RouteTarget::ManagementSurface {
                    module_id: SOURCE_KEY.into(),
                },
                identity,
                call_options(),
            )
            .await
            .map_err(sdk_error)?;
        Ok(Self { consumer, handle })
    }

    async fn exchange(&mut self, body: Value) -> Result<Value, FeedError> {
        let bytes = serde_json::to_vec(&body).map_err(|error| error.to_string())?;
        let response = self
            .consumer
            .request(&self.handle, bytes, call_options())
            .await
            .map_err(sdk_error)?;
        serde_json::from_slice(&response).map_err(|error| error.to_string().into())
    }

    async fn request<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<T, FeedError> {
        let value = self
            .exchange(json!({"method":method,"params":params}))
            .await?;
        serde_json::from_value(
            value
                .get("result")
                .cloned()
                .ok_or("colliculus result missing".to_string())?,
        )
        .map_err(|error| FeedError::Failed(format!("colliculus {method} reply: {error}")))
    }

    async fn ack(&mut self, through_seq: u64) -> Result<(), FeedError> {
        let answer: UsageAckAnswer = self
            .request("usage.ack", json!(UsageAckRequest::new(through_seq)))
            .await?;
        if answer.acked_through_seq < through_seq {
            return Err(FeedError::Failed(
                "colliculus acknowledgement regressed".into(),
            ));
        }
        Ok(())
    }
}

/// How long to wait before asking for colliculus again after the daemon said
/// it isn't installed. Each refusal counts at the daemon, so a machine without
/// colliculus would otherwise log one every tick forever. The wait doubles up
/// to `ABSENT_RETRY_MAX_MS`; colliculus keeps records until they are
/// acknowledged, so a late first read loses nothing.
const ABSENT_RETRY_FIRST_MS: i64 = 60_000;
const ABSENT_RETRY_MAX_MS: i64 = 600_000;

#[derive(Debug, Default, PartialEq, Eq)]
struct AbsentBackoff {
    /// When colliculus was last found absent, and how long to wait from then.
    waiting: Option<(i64, i64)>,
}

impl AbsentBackoff {
    fn may_try(&self, now: i64) -> bool {
        self.waiting
            .is_none_or(|(since, wait)| now.saturating_sub(since) >= wait)
    }

    fn absent(&mut self, now: i64) {
        let wait = self.waiting.map_or(ABSENT_RETRY_FIRST_MS, |(_, wait)| {
            wait.saturating_mul(2).min(ABSENT_RETRY_MAX_MS)
        });
        self.waiting = Some((now, wait));
    }

    fn present(&mut self) {
        self.waiting = None;
    }
}

pub(crate) struct ColliculusSource {
    #[cfg(test)]
    fixture_connection: Option<PathBuf>,
    transient_logged: AtomicBool,
    absent: std::sync::Mutex<AbsentBackoff>,
}

impl ColliculusSource {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(test)]
            fixture_connection: None,
            transient_logged: AtomicBool::new(false),
            absent: std::sync::Mutex::new(AbsentBackoff::default()),
        }
    }

    async fn open(&self, state: &SharedState) -> Result<Route, FeedError> {
        #[cfg(test)]
        if let Some(path) = &self.fixture_connection {
            return Route::open(path).await;
        }
        let access = state
            .quota_bus()
            .access()
            .ok_or("colliculus attested module identity unavailable".to_string())?;
        let nonce = subc_os::launch_nonce()
            .map_err(|_| "colliculus launch identity unavailable".to_string())?;
        // The SDK derives implicit identity from the same process cache as HELLO.
        // Refuse before connecting if it cannot attest the identity we staged.
        if std::env::var(SUBC_MODULE_ID_ENV).ok().as_deref() != Some(access.module_id.as_str())
            || nonce.as_ref().map(|value| value.value()) != Some(access.nonce.as_str())
        {
            return Err(FeedError::Failed(
                "colliculus attested module identity unavailable".into(),
            ));
        }
        Route::open(&access.path).await
    }

    async fn drain(
        &self,
        state: &SharedState,
        now: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, FeedError> {
        let mut cursor = state.with_conn(|conn| {
            let text: Option<String> = conn
                .query_row(
                    "SELECT through_seq FROM media_feed_cursor WHERE source_key=?1",
                    [SOURCE_KEY],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| error.to_string())?;
            text.map_or(Ok(0), |text| {
                text.parse::<u64>()
                    .map_err(|error| format!("invalid media cursor: {error}"))
            })
        })?;
        if stop.load(Ordering::Relaxed) {
            return Err(FeedError::Stopped);
        }
        let mut route = self.open(state).await?;
        // A prior acknowledgement can be lost after the page committed. Retrying
        // the durable watermark also releases retained records on a quiet feed.
        if cursor > 0 {
            route.ack(cursor).await?;
        }
        let mut unhealthy = false;
        let mut replayed = false;
        loop {
            if stop.load(Ordering::Relaxed) {
                return Err(FeedError::Stopped);
            }
            let page: UsageReadAnswer = route
                .request(
                    "usage.read",
                    json!(UsageReadRequest::new(LIMIT).with_after_seq(cursor)),
                )
                .await?;
            check_page_structure(&page)?;
            let next = page.last_seq.unwrap_or(cursor).max(cursor);
            if !page.entries.is_empty() && next == cursor {
                if replayed {
                    return Err(FeedError::Failed("media feed made no progress".into()));
                }
                replayed = true;
            } else {
                replayed = false;
            }
            let mut stopped = false;
            let committed = state.ordered_write(|tx| {
                let mut local = 0_i64;
                let mut evidence = Vec::new();
                let mut quarantined = false;
                for entry in &page.entries {
                    if stop.load(Ordering::Relaxed) {
                        stopped = true;
                        return Err("media ingest stopped".into());
                    }
                    let locator = format!("usage.read/seq/{}", entry.seq);
                    if let Err(error) = usage::check_usage_entry(entry) {
                        evidence.push(SetAsideRecord { locator, reason: format!("invalid media entry: {error}") });
                        continue;
                    }
                    if let Some(record) = &entry.record {
                        match astrocyte_core::media::map_record(record) {
                            Ok(Some((event, facts))) => {
                                let outcome = record_media_event_in_tx(tx, &event, &facts, now).map_err(|error| format!("record media event: {error:?}"))?;
                                quarantined |= outcome.disposition == RecordDisposition::QuarantinedRedelivery;
                            }
                            Ok(None) => { if entry.seq > cursor { local += 1; } }
                            Err(reason) => evidence.push(SetAsideRecord { locator, reason: format!("record {}: {reason}", record.record_id) }),
                        }
                    } else if let Some(raw) = &entry.raw {
                        evidence.push(SetAsideRecord { locator, reason: format!("raw media entry: {}", serde_json::to_string(raw).map_err(|error| error.to_string())?) });
                    }
                }
                let new = harness_drain::record_set_aside_in_tx(tx, SOURCE_KEY, &evidence, now)?;
                tx.execute("INSERT INTO media_feed_cursor(source_key,through_seq,skipped_local) VALUES(?1,?2,?3) ON CONFLICT(source_key) DO UPDATE SET through_seq=excluded.through_seq,skipped_local=media_feed_cursor.skipped_local+excluded.skipped_local", params![SOURCE_KEY,next.to_string(),local]).map_err(|error| error.to_string())?;
                harness_drain::ensure_runtime_row_in_tx(tx, SOURCE_KEY).map_err(|error| error.to_string())?;
                harness_drain::mark_current_in_tx(tx, SOURCE_KEY, SOURCE_KEY, now)?;
                tx.execute("UPDATE source_runtime SET pending_unknown=?2 WHERE source_key=?1", params![SOURCE_KEY,!page.entries.is_empty()]).map_err(|error| error.to_string())?;
                Ok(((new, quarantined), true))
            });
            let (new, anomalous) = match committed {
                Err(_) if stopped => return Err(FeedError::Stopped),
                other => other?,
            };
            harness_drain::warn_set_aside(SOURCE_KEY, &new);
            unhealthy |= anomalous;
            cursor = next;
            if page.entries.is_empty() {
                break;
            }
            // ordered_write returns only after SQLite commits events, evidence,
            // coverage and the cursor. No acknowledgement can precede that act.
            route.ack(cursor).await?;
        }
        Ok(unhealthy.then(|| "colliculus deliveries were quarantined".into()))
    }
}

impl harness_drain::HarnessSource for ColliculusSource {
    fn source_key(&self) -> &'static str {
        SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        now: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        #[cfg(not(test))]
        let wait_for_hello = true;
        #[cfg(test)]
        let wait_for_hello = self.fixture_connection.is_none();
        if wait_for_hello && !state.quota_bus().hello_acked() {
            return Err(SourceDrainError::Deferred);
        }
        let mut absent = self
            .absent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !absent.may_try(now) {
            // Still waiting after colliculus was found absent: treat it as
            // absent again, which leaves health and runtime rows untouched.
            return Ok(None);
        }
        // The blocking refresher owns this runtime. Consumer tasks and their
        // connections are dropped with it after the drain ends.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let outcome = runtime.block_on(self.drain(state, now, stop));
        match &outcome {
            Err(FeedError::Absent) => absent.absent(now),
            Err(FeedError::Stopped) => {}
            // Any other answer, a warming refusal included, means colliculus
            // is installed, so the next tick asks again.
            _ => absent.present(),
        }
        drop(absent);
        if let Err(FeedError::Transient(reason)) = &outcome {
            if !self.transient_logged.swap(true, Ordering::Relaxed) {
                tracing::info!(source = SOURCE_KEY, %reason, "media route temporarily unavailable; retrying next tick");
            }
            return Err(SourceDrainError::Deferred);
        }
        self.transient_logged.store(false, Ordering::Relaxed);
        match outcome {
            Ok(reason) => Ok(reason),
            Err(FeedError::Absent) => {
                harness_drain::remove_runtime_row(state, SOURCE_KEY)?;
                Ok(None)
            }
            Err(FeedError::Stopped) => Err(SourceDrainError::Stopped),
            Err(FeedError::Failed(reason)) => {
                harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
                Err(SourceDrainError::Failed(reason))
            }
            Err(FeedError::Transient(_)) => unreachable!("handled above"),
        }
    }
}

#[cfg(test)]
#[path = "colliculus_source_tests.rs"]
mod tests;
