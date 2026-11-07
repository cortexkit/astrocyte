//! Acknowledged media usage, drained only on full refresher ticks.

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
use subc_protocol::{error_codes, FrameType};
use subc_transport::{authenticate_client, connection_file};
use tokio::net::TcpStream;

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
        _ => FeedError::Failed(format!("colliculus refusal {code}: {detail}")),
    }
}

struct Route {
    stream: TcpStream,
    channel: u16,
    epoch: u32,
    correlation: u64,
}

impl Route {
    async fn exchange(&mut self, body: Value) -> Result<Value, FeedError> {
        let bytes = serde_json::to_vec(&body).map_err(|error| error.to_string())?;
        let response = crate::peer_exchange::exchange(
            &mut self.stream,
            self.channel,
            self.epoch,
            self.correlation,
            bytes,
            crate::peer_exchange::ExchangeMode::Peer,
        )
        .await?;
        self.correlation += 1;
        let value: Value =
            serde_json::from_slice(&response.body).map_err(|error| error.to_string())?;
        if response.header.ty == FrameType::Error {
            return Err(refusal(
                value.get("code").and_then(Value::as_str).unwrap_or(""),
                &value,
            ));
        }
        if response.header.ty != FrameType::Response {
            return Err(FeedError::Failed("unexpected colliculus frame type".into()));
        }
        Ok(value)
    }

    async fn request<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<T, FeedError> {
        let value = tokio::time::timeout(
            BUDGET,
            self.exchange(json!({"method":method,"params":params})),
        )
        .await
        .map_err(|_| "colliculus response timeout".to_string())??;
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

pub(crate) struct ColliculusSource {
    connection_file: PathBuf,
    transient_logged: AtomicBool,
}

impl ColliculusSource {
    pub(crate) fn new(connection_file: PathBuf) -> Self {
        Self {
            connection_file,
            transient_logged: AtomicBool::new(false),
        }
    }

    async fn open(&self) -> Result<Route, FeedError> {
        let metadata = connection_file::read(&self.connection_file)
            .map_err(|error| format!("connection file: {error:?}"))?;
        let endpoint = metadata
            .endpoints
            .first()
            .ok_or("no daemon endpoint".to_string())?;
        let mut stream = crate::peer_exchange::connect_endpoint(&endpoint.host, endpoint.port)
            .await
            .map_err(|error| format!("daemon connect: {error}"))?;
        authenticate_client(&mut stream, &metadata, BUDGET)
            .await
            .map_err(|error| format!("daemon authentication: {error:?}"))?;
        let mut route = Route {
            stream,
            channel: 0,
            epoch: 0,
            correlation: 1,
        };
        let opened = route
            .exchange(json!({
                "op":"route.open", "target":{"kind":"management_surface","module_id":SOURCE_KEY},
                "identity":{"harness":"ck-astrocyte","session":"media-usage"}
            }))
            .await?;
        route.channel = opened
            .get("route_channel")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .filter(|value| *value != 0)
            .ok_or("invalid route channel".to_string())?;
        route.epoch = opened
            .get("route_epoch")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or("invalid route epoch".to_string())?;
        Ok(route)
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
        let mut route = tokio::time::timeout(BUDGET, self.open())
            .await
            .map_err(|_| "colliculus route timeout".to_string())??;
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
            usage::check_usage_answer(&page)
                .map_err(|error| format!("invalid media page: {error}"))?;
            if page.entries.len() > usize::try_from(LIMIT).expect("small page limit") {
                return Err(FeedError::Failed(
                    "media page exceeds requested limit".into(),
                ));
            }
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
        // The transport runtime is owned by the blocking refresher thread, and
        // route admission is attempted only during a tick, never during boot.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let outcome = runtime.block_on(self.drain(state, now, stop));
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
