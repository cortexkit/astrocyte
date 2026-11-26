//! Read attribution on scheduled refreshes, not the shorter pricing-only cycles.
use crate::peer_exchange::open_route;
use crate::state::SharedState;
use astrocyte_core::attribution::{self, Page, Publisher};
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use subc_client_rs::{CallError, CallOptions, NotSentCause, RouteHandle, SubcConsumer};
use subc_protocol::{error_codes, RouteTarget};

const LIMIT: usize = 128;
const BUDGET: Duration = Duration::from_secs(10);
fn options() -> CallOptions {
    CallOptions {
        timeout: BUDGET,
        route_retry_deadline: BUDGET,
        ..Default::default()
    }
}
trait ReadTransport {
    async fn read(&mut self, params: Value) -> Result<Vec<u8>, ()>;
}
struct Route {
    consumer: SubcConsumer,
    handle: RouteHandle,
    publisher: Publisher,
}

#[derive(Debug, PartialEq, Eq)]
enum OpenError {
    Absent,
    Other,
}

impl From<CallError> for OpenError {
    fn from(error: CallError) -> Self {
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
        // A route refusal can echo launch credentials. Classify by code without
        // inspecting or logging the message or details.
        match code {
            Some(
                error_codes::TARGET_UNAVAILABLE
                | error_codes::UNKNOWN_MODULE
                | error_codes::MODULE_REMOVED,
            ) => Self::Absent,
            _ => Self::Other,
        }
    }
}

async fn open_publisher<T, F: Future<Output = Result<T, CallError>>>(
    publisher: Publisher,
    mut open: impl FnMut(&'static str) -> F,
) -> Result<T, OpenError> {
    // Prefrontal answers under its new id after its rename and its old id
    // before it. Try both before concluding that the publisher is absent.
    let mut failure = OpenError::Absent;
    for &module in publisher.modules() {
        match open(module).await.map_err(OpenError::from) {
            Ok(handle) => return Ok(handle),
            Err(OpenError::Other) => failure = OpenError::Other,
            Err(OpenError::Absent) => {}
        }
    }
    Err(failure)
}

impl Route {
    async fn open(state: &SharedState, publisher: Publisher) -> Result<Self, OpenError> {
        let access = state.peer_access().ok_or(OpenError::Other)?;
        let consumer = access.connect(BUDGET).await.map_err(|_| OpenError::Other)?;
        let session = publisher.source();
        let handle = open_publisher(publisher, |module| {
            open_route(
                &consumer,
                RouteTarget::ManagementSurface {
                    module_id: module.into(),
                },
                &session,
                options(),
            )
        })
        .await?;
        Ok(Self {
            consumer,
            handle,
            publisher,
        })
    }
}
impl ReadTransport for Route {
    async fn read(&mut self, params: Value) -> Result<Vec<u8>, ()> {
        let request =
            serde_json::to_vec(&json!({"method":self.publisher.read_name(),"params":params}))
                .map_err(|_| ())?;
        self.consumer
            .request(&self.handle, request, options())
            .await
            .map(|reply| reply.to_vec())
            .map_err(|_| ())
    }
}

fn malformed_page(
    state: &SharedState,
    publisher: Publisher,
    bytes: &[u8],
    reason: &str,
    now: i64,
) -> Result<(), String> {
    let digest = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let locator = format!("{}/page/{digest}", publisher.read_name());
    let new = state.with_conn(|conn| {
        conn.query_row(
            "SELECT 1 FROM set_aside_record WHERE source_key=?1 AND locator=?2",
            params![publisher.source(), locator],
            |_| Ok(()),
        )
        .optional()
        .map(|known| known.is_none())
        .map_err(|e| e.to_string())
    })?;
    if new {
        state.ordered_write(|tx| {
        let changed=tx.execute("INSERT OR IGNORE INTO set_aside_record(source_key,locator,reason,first_seen_ms,last_seen_ms) VALUES(?1,?2,?3,?4,?4)",params![publisher.source(),locator,reason,now]).map_err(|e|e.to_string())?!=0;
        Ok(((),changed))
    })?;
    }
    Ok(())
}

async fn drain_pages<T: ReadTransport>(
    state: &SharedState,
    publisher: Publisher,
    now: i64,
    stop: &AtomicBool,
    route: &mut T,
) -> Result<bool, String> {
    let stored = state.with_conn(|conn| {
        conn.query_row(
            "SELECT cursor FROM attribution_cursor WHERE publisher=?1",
            [publisher.key()],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .map(Option::flatten)
        .map_err(|e| e.to_string())
    })?;
    let mut cursor = stored
        .map(|cursor| serde_json::from_str::<Value>(&cursor).map_err(|e| e.to_string()))
        .transpose()?;
    let mut seen = Vec::new();
    if let Some(cursor) = &cursor {
        seen.push(cursor.clone());
    }
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let mut params = json!({"limit":LIMIT});
        if let Some(cursor) = &cursor {
            params["after"] = cursor.clone();
        }
        // SDK failures and served refusals say nothing about ledger health.
        // They are retried on the next scheduled refresh without a durable write.
        let Ok(bytes) = route.read(params).await else {
            return Ok(false);
        };
        if stop.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let reply = serde_json::from_slice::<Value>(&bytes);
        if reply
            .as_ref()
            .ok()
            .and_then(|reply| reply.get("error"))
            .is_some_and(|error| !error.is_null())
        {
            return Ok(false);
        }
        let parsed = reply
            .ok()
            .and_then(|reply| reply.get("result").cloned())
            .and_then(|result| serde_json::from_value::<Page>(result).ok());
        let Some(page) = parsed.filter(|page| page.facts.len() <= LIMIT) else {
            malformed_page(state, publisher, &bytes, "malformed_page", now)?;
            return Ok(false);
        };
        if page.facts.is_empty() {
            return Ok(true);
        }
        if page.next.as_ref().is_some_and(|next| seen.contains(next)) {
            malformed_page(state, publisher, &bytes, "cursor_not_advanced", now)?;
            return Ok(false);
        }
        let needs_commit =
            state.with_conn(|conn| attribution::page_needs_commit(conn, publisher, &page))?;
        if needs_commit {
            if stop.load(Ordering::Relaxed) {
                return Ok(false);
            }
            state.ordered_write(|tx| {
                let changed = attribution::commit_page(tx, publisher, cursor.as_ref(), &page, now)?;
                if stop.load(Ordering::Relaxed) {
                    return Err("attribution drain stopped".into());
                }
                Ok(((), changed))
            })?;
        }
        let Some(next) = page.next else {
            return Ok(true);
        };
        seen.push(next.clone());
        cursor = Some(next);
    }
}

enum DrainError {
    Absent,
    Page(String),
}

pub(super) fn drain_all(state: &SharedState, stop: &AtomicBool, full_tick: bool) {
    if !state.quota_bus().hello_acked() {
        return;
    }
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    drain_selected(state, stop, full_tick, crate::state::now_ms, |publisher| {
        runtime.block_on(async {
            // Absence and routing refusals are normal while publishers roll out.
            // Do not create source_runtime rows or change an existing health result.
            let mut route = match Route::open(state, publisher).await {
                Ok(route) => route,
                Err(OpenError::Absent) => return Err(DrainError::Absent),
                Err(OpenError::Other) => return Ok(false),
            };
            drain_pages(state, publisher, crate::state::now_ms(), stop, &mut route)
                .await
                .map_err(DrainError::Page)
        })
    });
}

fn drain_selected(
    state: &SharedState,
    stop: &AtomicBool,
    full_tick: bool,
    mut now_ms: impl FnMut() -> i64,
    mut read: impl FnMut(Publisher) -> Result<bool, DrainError>,
) {
    for publisher in Publisher::ALL {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now = now_ms();
        let Some(ticket) = state.event_wakes().begin(
            crate::event_wakes::Source::Attribution(publisher.key()),
            full_tick,
            now,
        ) else {
            continue;
        };
        match read(publisher) {
            Err(DrainError::Absent) => ticket.finish_absent(now),
            Ok(committed) => ticket.finish(committed),
            Err(DrainError::Page(error)) => {
                ticket.finish(false);
                if !stop.load(Ordering::Relaxed) {
                    tracing::warn!(
                        publisher = publisher.key(),
                        %error,
                        "attribution page was not committed"
                    );
                }
            }
        }
    }
}

pub(super) fn append_roots(
    state: &SharedState,
    entries: &[attribution::SessionAliases],
    stop: &AtomicBool,
) -> Result<(), crate::refresher::SourceDrainError> {
    use crate::refresher::SourceDrainError;
    if stop.load(Ordering::Relaxed) {
        return Err(SourceDrainError::Stopped);
    }
    if !state.with_conn(|conn| attribution::aliases_need_commit(conn, entries))? {
        return Ok(());
    }
    state
        .ordered_write(|tx| {
            let changed = attribution::append_alias_batch(tx, entries)?;
            if stop.load(Ordering::Relaxed) {
                return Err("session alias drain stopped".into());
            }
            Ok(((), changed))
        })
        .map_err(|reason| {
            if stop.load(Ordering::Relaxed) {
                SourceDrainError::Stopped
            } else {
                reason.into()
            }
        })
}

#[cfg(test)]
#[path = "attribution_source/tests.rs"]
mod tests;
