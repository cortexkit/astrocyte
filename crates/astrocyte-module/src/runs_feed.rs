//! A held change notice for Runs, sharing the step watch's admission and slots.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::HandlerOutcome;
use tokio::sync::watch;

use crate::{
    state::SharedState,
    step_feed::{NoticeSink, MAX_FEED_SUBSCRIPTIONS},
};

const INTERVAL: Duration = Duration::from_secs(2);
// Both primary keys are append-only. SQLite answers each MAX with an indexed
// seek, not a scan of the ledger, even when a commit inserted no billable call.
const WATERMARK: &str = "SELECT COALESCE((SELECT MAX(event_key) FROM usage_event),0), COALESCE((SELECT MAX(charge_key) FROM priced_charge),0)";

pub(crate) struct Hub {
    summary: AtomicU64,
    notices: watch::Sender<String>,
    last: Mutex<Option<(i64, i64, u64)>>,
}

impl Default for Hub {
    fn default() -> Self {
        Self {
            summary: AtomicU64::new(0),
            notices: watch::channel(String::new()).0,
            last: Mutex::new(None),
        }
    }
}

impl Hub {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn subscribers(&self) -> usize {
        self.notices.receiver_count()
    }
    fn snapshot(&self, conn: &Connection) -> Result<(), String> {
        let (event, charge): (i64, i64) = conn
            .query_row(WATERMARK, [], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|error| error.to_string())?;
        let summary = self.summary.load(Ordering::Relaxed);
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *last != Some((event, charge, summary)) {
            *last = Some((event, charge, summary));
            // This is a process-held watch, not a durable resume position. The
            // summary counter restarts with the process and can repeat a cursor;
            // subscribers reconnect and re-read rather than resume across restarts.
            self.notices
                .send_replace(format!("runs1:{event:016x}:{charge:016x}:{summary:016x}"));
        }
        Ok(())
    }

    pub(crate) fn committed(&self, conn: &Connection) {
        if self.notices.receiver_count() == 0 {
            return;
        }
        if let Err(error) = self.snapshot(conn) {
            tracing::warn!(%error, "runs change watermark unavailable");
        }
    }

    pub(crate) fn summary_committed(&self, conn: &Connection) {
        self.summary.fetch_add(1, Ordering::Relaxed);
        self.committed(conn);
    }

    pub(crate) fn open(&self, conn: &Connection) -> Result<watch::Receiver<String>, String> {
        self.snapshot(conn)?;
        Ok(self.notices.subscribe())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {}

fn refusal(code: &str, message: impl Into<String>) -> HandlerOutcome {
    HandlerOutcome::Error {
        code: code.into(),
        message: message.into(),
    }
}

pub async fn subscribe(
    state: &SharedState,
    params: Value,
    sink: &dyn NoticeSink,
    mut stopping: watch::Receiver<bool>,
) -> HandlerOutcome {
    if let Err(error) = serde_json::from_value::<Request>(params) {
        return refusal("malformed", format!("runs subscribe params: {error}"));
    }
    if *stopping.borrow() {
        return refusal(
            "draining",
            "astrocyte is stopping; subscribe again once it is back",
        );
    }
    let Ok(_slot) = Arc::clone(&state.step_feed().slots).try_acquire_owned() else {
        return refusal(
            "subscription_capacity",
            format!("at most {MAX_FEED_SUBSCRIPTIONS} feed subscriptions may be open"),
        );
    };
    let mut notices = match state.open_runs_watch() {
        Ok(notices) => notices,
        Err(error) => return refusal("internal", error),
    };
    let stream = async {
        let mut next = tokio::time::Instant::now();
        loop {
            if notices.changed().await.is_err() {
                return;
            }
            tokio::time::sleep_until(next).await;
            let cursor = notices.borrow_and_update().clone();
            if sink
                .emit(serde_json::to_vec(&json!({"cursor":cursor})).expect("JSON notice"))
                .await
                .is_err()
            {
                return;
            }
            next = tokio::time::Instant::now() + INTERVAL;
        }
    };
    tokio::select! {
        biased;
        () = sink.ended() => refusal("cancelled", "subscription cancelled"),
        _ = stopping.wait_for(|stopping| *stopping) => HandlerOutcome::Streamed,
        () = stream => HandlerOutcome::Streamed,
    }
}

#[cfg(test)]
#[path = "runs_feed_tests.rs"]
mod tests;
