//! List-only vault access. Network work belongs to a separate reader, never
//! startup or the ledger writer; the full refresher tick applies its snapshots.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astrocyte_core::multiplier::PlanPriceSource;
use astrocyte_core::vault_plan::{self, VaultPlan};
use cortexkit_claustrum_client::{decode_list_scoped_reply, WatchNotice};
use fusiform_protocol::PlanPricesRequest;
use rusqlite::OptionalExtension;
use serde::Serialize;
use serde_json::{json, Value};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::quota_bus_client::Access;
use crate::state::SharedState;

const BUDGET: Duration = Duration::from_secs(10);
const DAY_MS: i64 = 86_400_000;
const UNPRICED_RETRY_MS: i64 = 3_600_000;
type Snapshot = Result<Listing, String>;

#[derive(Debug)]
pub(crate) struct Listing {
    plans: Vec<VaultPlan>,
    invalid_plan_tier_since: Vec<InvalidPlanTierSince>,
    // With neither vault timestamp, repeated reads retain the first observation
    // of the same plan instead of inventing a new start on every poll.
    effective_time_unspecified: Vec<String>,
}

#[derive(Debug, Serialize)]
struct InvalidPlanTierSince {
    credential_id: String,
    reason: &'static str,
}

fn decode_listing(value: Value, now: i64) -> Snapshot {
    let listing =
        decode_list_scoped_reply(value.clone()).map_err(|error| error.code().to_string())?;
    if !listing.undecodable_credentials.is_empty() || !listing.undecodable_grants.is_empty() {
        return Err("invalid_listing".into());
    }
    if !listing.grant_tuples.iter().any(|grant| {
        grant.selector_kind == "category"
            && grant.selector == "llm-provider"
            && grant.operation == "list"
    }) {
        return Err("list_grant_missing".into());
    }
    let raw = value["result"]["credentials"]
        .as_array()
        .ok_or("invalid_listing")?;
    let mut invalid_plan_tier_since = Vec::new();
    let mut effective_time_unspecified = Vec::new();
    let plans = listing
        .credentials
        .into_iter()
        .filter(|record| record.categories.iter().any(|v| v == "llm-provider"))
        .map(|record| {
            let row = raw
                .iter()
                .find(|row| row["id"].as_str() == Some(&record.id));
            let observed = row
                .and_then(|row| row.get("plan_tier_observed_at_ms"))
                .and_then(Value::as_i64);
            let since = row
                .and_then(|row| row.get("plan_tier_since_ms"))
                .and_then(|value| {
                    let result = match value.as_i64() {
                        Some(since) if since >= 0 => {
                            if observed.is_some_and(|observed| since > observed) {
                                Err("after_observed_at")
                            } else {
                                Ok(since)
                            }
                        }
                        _ => Err("not_non_negative_integer"),
                    };
                    match result {
                        Ok(since) => Some(since),
                        Err(reason) => {
                            invalid_plan_tier_since.push(InvalidPlanTierSince {
                                credential_id: record.id.clone(),
                                reason,
                            });
                            None
                        }
                    }
                });
            let effective = since.or(observed).unwrap_or_else(|| {
                effective_time_unspecified.push(record.id.clone());
                now
            });
            let provider = record.provider_ids.first().cloned();
            let plan_key = if record.provider_ids.len() == 1 {
                record
                    .plan_tier
                    .as_ref()
                    .map(|tier| format!("{}/{tier}", record.provider_ids[0]))
            } else {
                None
            };
            VaultPlan {
                credential_id: record.id,
                provider,
                account_id: record.account_id,
                plan_tier: record.plan_tier,
                plan_key,
                source: record.plan_tier_source.unwrap_or_else(|| "vault".into()),
                effective_from_ms: effective,
                observed_at_ms: now,
            }
        })
        .collect();
    Ok(Listing {
        plans,
        invalid_plan_tier_since,
        effective_time_unspecified,
    })
}

struct Route {
    stream: TcpStream,
    channel: u16,
    epoch: u32,
}

impl Route {
    async fn open(access: &Access) -> Result<Self, String> {
        let metadata = connection_file::read(&access.path).map_err(|_| "vault_unavailable")?;
        let endpoint = metadata.endpoints.first().ok_or("vault_unavailable")?;
        let mut stream = crate::peer_exchange::connect_endpoint(&endpoint.host, endpoint.port)
            .await
            .map_err(|_| "vault_unavailable")?;
        authenticate_client(&mut stream, &metadata, BUDGET)
            .await
            .map_err(|_| "vault_unavailable")?;
        let reply = crate::peer_exchange::exchange(&mut stream, 0, 0, 1, serde_json::to_vec(&json!({
            "op":"route.open", "target":{"kind":"management_surface","module_id":"claustrum"},
            "identity":{"harness":"ck-astrocyte","session":"subscription-plans"},
            "consumer_identity":{"module_id":access.module_id,"launch_nonce":access.nonce}, "config":[]
        })).map_err(|_| "encode")?, crate::peer_exchange::ExchangeMode::Peer).await
            .map_err(|_| "vault_unavailable")?;
        let value: Value = serde_json::from_slice(&reply.body).map_err(|_| "invalid_response")?;
        if reply.header.ty == FrameType::Error {
            return Err(safe_refusal(&value));
        }
        Ok(Self {
            stream,
            channel: value["route_channel"]
                .as_u64()
                .and_then(|v| u16::try_from(v).ok())
                .ok_or("invalid_route")?,
            epoch: value["route_epoch"]
                .as_u64()
                .and_then(|v| u32::try_from(v).ok())
                .ok_or("invalid_route")?,
        })
    }

    async fn list(access: &Access) -> Snapshot {
        tokio::time::timeout(BUDGET, async {
            let mut route = Self::open(access).await?;
            let reply = crate::peer_exchange::exchange(
                &mut route.stream,
                route.channel,
                route.epoch,
                2,
                br#"{"method":"credential.list_scoped","params":{}}"#.to_vec(),
                crate::peer_exchange::ExchangeMode::Peer,
            )
            .await
            .map_err(|_| "vault_unavailable".to_string())?;
            let value: Value =
                serde_json::from_slice(&reply.body).map_err(|_| "invalid_response".to_string())?;
            if reply.header.ty == FrameType::Error {
                return Err(safe_refusal(&value));
            }
            decode_listing(value, crate::state::now_ms())
        })
        .await
        .unwrap_or_else(|_| Err("vault_timeout".into()))
    }

    async fn watch(access: Access, notices: mpsc::UnboundedSender<Result<(), String>>) {
        let opened = tokio::time::timeout(BUDGET, async {
            let mut route = Self::open(&access).await?;
            let request = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Passive, false),
                route.channel,
                route.epoch,
                2,
                br#"{"method":"auth.watch_scoped","params":{}}"#.to_vec(),
            )
            .map_err(|_| "encode".to_string())?;
            write_frame(&mut route.stream, &request)
                .await
                .map_err(|_| "vault_unavailable".to_string())?;
            Ok::<_, String>(route)
        })
        .await;
        let mut route = match opened {
            Ok(Ok(route)) => route,
            _ => {
                let _ = notices.send(Err("vault_watch_unavailable".into()));
                return;
            }
        };
        loop {
            let frame = match read_frame(&mut route.stream).await {
                Ok(Some(frame)) => frame,
                _ => {
                    let _ = notices.send(Err("vault_watch_lost".into()));
                    return;
                }
            };
            if frame.header.ty == FrameType::Ping {
                if let Ok(pong) = Frame::build_with_version(
                    frame.header.ver,
                    FrameType::Pong,
                    frame.header.flags,
                    0,
                    0,
                    frame.header.corr,
                    vec![],
                ) {
                    if write_frame(&mut route.stream, &pong).await.is_err() {
                        return;
                    }
                }
                continue;
            }
            if frame.header.channel != route.channel
                || frame.header.epoch != route.epoch
                || frame.header.corr != 2
            {
                continue;
            }
            let result = match frame.header.ty {
                FrameType::StreamData if WatchNotice::decode(&frame.body).is_ok() => Ok(()),
                FrameType::Error => {
                    let value = serde_json::from_slice(&frame.body).unwrap_or(Value::Null);
                    Err(safe_refusal(&value))
                }
                _ => Err("vault_watch_lost".into()),
            };
            let ended = result.is_err();
            if notices.send(result).is_err() || ended {
                return;
            }
        }
    }
}

// Peer errors can echo launch credentials. Keep only known structural codes.
fn safe_refusal(value: &Value) -> String {
    match value["code"].as_str() {
        Some(
            code @ ("unknown_method"
            | "unknown_module"
            | "module_removed"
            | "unauthorized"
            | "forbidden"
            | "bad_consumer_identity"),
        ) => code.into(),
        _ => "vault_refused".into(),
    }
}

pub(crate) struct Reader {
    ticks: mpsc::UnboundedSender<Access>,
    snapshots: Arc<Mutex<Vec<Snapshot>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Reader {
    pub(crate) fn start(stop: Arc<AtomicBool>) -> Self {
        let (ticks, mut received) = mpsc::unbounded_channel::<Access>();
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&snapshots);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            let Ok(runtime) = runtime else {
                return;
            };
            runtime.block_on(async {
                let mut access = None;
                let mut watch: Option<tokio::task::JoinHandle<()>> = None;
                let (notices, mut changed) = mpsc::unbounded_channel();
                let mut polling = false;
                let mut last_list = None;
                while !stop.load(Ordering::Relaxed) {
                    let mut reread = false;
                    tokio::select! {
                        tick = received.recv() => {
                            let Some(tick) = tick else { break; };
                            access = Some(tick);
                            reread = polling || last_list.is_none_or(|at| crate::state::now_ms() - at >= DAY_MS);
                            if watch.as_ref().is_none_or(|task| task.is_finished()) && !polling {
                                watch = Some(tokio::spawn(Route::watch(access.as_ref().unwrap().clone(), notices.clone())));
                            }
                        }
                        notice = changed.recv() => {
                            match notice {
                                Some(Ok(())) => reread = true,
                                Some(Err(code)) if code == "unknown_method" => { polling = true; }
                                Some(Err(code)) => {
                                    output.lock().unwrap().push(Err(code));
                                    last_list = None;
                                }
                                None => break,
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                    }
                    if reread {
                        if let Some(access) = &access {
                            let snapshot = Route::list(access).await;
                            last_list = Some(crate::state::now_ms());
                            output.lock().unwrap().push(snapshot);
                        }
                    }
                }
                if let Some(watch) = watch { watch.abort(); }
            });
        });
        Self {
            ticks,
            snapshots,
            thread: Some(thread),
        }
    }

    pub(crate) fn tick(&self, state: &SharedState) -> Vec<Snapshot> {
        if state.quota_bus().hello_acked() {
            if let Some(access) = state.quota_bus().access() {
                let _ = self.ticks.send(access);
            }
        }
        std::mem::take(&mut *self.snapshots.lock().unwrap())
    }

    pub(crate) fn join(mut self) {
        drop(self.ticks);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) struct Stage {
    reader: Reader,
    available: bool,
    last_failure: Option<String>,
    last_reconcile_ms: Option<i64>,
    last_mismatch_ms: Option<i64>,
    diagnostics: vault_plan::Diagnostics,
    price_available: bool,
    invalid_plan_tier_since: Vec<InvalidPlanTierSince>,
}

impl Stage {
    pub(crate) fn new(stop: Arc<AtomicBool>) -> Self {
        Self {
            reader: Reader::start(stop),
            available: false,
            last_failure: None,
            last_reconcile_ms: None,
            last_mismatch_ms: None,
            diagnostics: vault_plan::Diagnostics::default(),
            price_available: false,
            invalid_plan_tier_since: Vec::new(),
        }
    }

    pub(crate) fn tick(
        &mut self,
        state: &SharedState,
        source: Option<&dyn PlanPriceSource>,
        now: i64,
    ) {
        let snapshots = self.reader.tick(state);
        self.apply(state, snapshots, source, now);
    }

    fn apply(
        &mut self,
        state: &SharedState,
        snapshots: Vec<Snapshot>,
        source: Option<&dyn PlanPriceSource>,
        now: i64,
    ) {
        for snapshot in snapshots {
            match snapshot {
                Err(code) => {
                    self.available = false;
                    self.failure(state, code);
                }
                Ok(listing) => {
                    let result = self.append_snapshot(state, &listing).and_then(|changed| {
                        self.invalid_plan_tier_since = listing.invalid_plan_tier_since;
                        self.available = true;
                        self.process(state, source, now, changed)?;
                        self.publish(state);
                        Ok(())
                    });
                    if let Err(error) = result {
                        self.failure(state, error);
                        return;
                    }
                }
            }
        }
        if self.available {
            match self.process(state, source, now, false) {
                Ok(true) => self.publish(state),
                Ok(false) => {}
                Err(error) => self.failure(state, error),
            }
        }
    }

    fn append_snapshot(&self, state: &SharedState, listing: &Listing) -> Result<bool, String> {
        let mut plans = listing.plans.clone();
        let changed = state.with_conn(|conn| {
            let mut changed = false;
            for plan in &mut plans {
                if listing.effective_time_unspecified.contains(&plan.credential_id) {
                    let previous: Option<(Option<String>, i64)> = conn.query_row(
                        "SELECT plan_key,effective_from_ms FROM vault_plan_history WHERE credential_id=?1 ORDER BY history_id DESC LIMIT 1",
                        [&plan.credential_id], |row| Ok((row.get(0)?, row.get(1)?)),
                    ).optional().map_err(|error| error.to_string())?;
                    if let Some((key, effective)) = previous {
                        if key == plan.plan_key {
                            plan.effective_from_ms = effective;
                        }
                    }
                }
                changed |= vault_plan::differs_from_latest(conn, plan).map_err(|error| error.to_string())?;
            }
            Ok(changed)
        })?;
        if !changed {
            return Ok(false);
        }
        state.ordered_tail_write(|tx| {
            let mut changed = false;
            for plan in &plans {
                changed |= vault_plan::append_in_tx(tx, plan).map_err(|error| error.to_string())?;
            }
            Ok((changed, changed))
        })
    }

    fn process(
        &mut self,
        state: &SharedState,
        source: Option<&dyn PlanPriceSource>,
        now: i64,
        history_changed: bool,
    ) -> Result<bool, String> {
        let interval = if self.diagnostics.unpriced_plans.is_empty() {
            DAY_MS
        } else {
            UNPRICED_RETRY_MS
        };
        let reconcile = history_changed
            || self
                .last_reconcile_ms
                .is_none_or(|at| now.saturating_sub(at) >= interval);
        let mismatches = history_changed
            || self
                .last_mismatch_ms
                .is_none_or(|at| now.saturating_sub(at) >= DAY_MS);
        if !reconcile && !mismatches {
            return Ok(false);
        }
        let plans = state.with_conn(|conn| {
            vault_plan::at(conn, now.saturating_add(1)).map_err(|error| error.to_string())
        })?;
        if mismatches {
            self.last_mismatch_ms = Some(now);
            self.diagnostics.chatgpt_account_mismatches =
                state.with_conn(|conn| vault_plan::chatgpt_account_mismatches(conn, &plans))?;
        }
        if reconcile {
            self.reconcile(state, &plans, source, now)?;
        }
        Ok(true)
    }

    fn reconcile(
        &mut self,
        state: &SharedState,
        plans: &[VaultPlan],
        source: Option<&dyn PlanPriceSource>,
        now: i64,
    ) -> Result<(), String> {
        self.last_reconcile_ms = Some(now);
        // Only a scheduled reconciliation asks the plan-price source
        // (fusiform's `plan.prices`), and it does so before any writer
        // transaction starts. Plans it can't price stay listed as unpriced
        // and are retried hourly.
        let result = source
            .ok_or_else(|| "plan_prices_unavailable".to_string())
            .and_then(|source| {
                source
                    .plan_prices(PlanPricesRequest { provider_id: None })
                    .map_err(|_| "plan_prices_unavailable".into())
            });
        self.price_available = result.is_ok();
        let prices = result.unwrap_or_else(|_| fusiform_protocol::PlanPricesResponse {
            prices: Vec::new(),
            resolved_at_ms: now,
            tier_vocabulary: String::new(),
            unit_policy: String::new(),
        });
        let prepared = state
            .with_conn(|conn| vault_plan::prepare_reconciliation(conn, plans, &prices, now))?;
        let mut report = if prepared.has_changes() {
            // A hand-set plan can win after the preview. Recheck ownership in the
            // write transaction; if no changes remain, roll back rather than commit.
            state.ordered_tail_write(|tx| {
                let prepared = vault_plan::prepare_reconciliation(tx, plans, &prices, now)?;
                let changed = prepared.has_changes();
                Ok((prepared.apply_in_tx(tx)?, changed))
            })?
        } else {
            prepared.diagnostics
        };
        report.chatgpt_account_mismatches = self.diagnostics.chatgpt_account_mismatches.clone();
        self.diagnostics = report;
        Ok(())
    }

    fn publish(&mut self, state: &SharedState) {
        let report = &self.diagnostics;
        let mut value = json!({"vault_read_status":{"status":"available"},
            "plan_price_status":if self.price_available { "available" } else { "unavailable" },
            "unknown_plans":report.unknown_plans,"unpriced_plans":report.unpriced_plans,
            "hand_vault_differences":report.hand_vault_differences,"vault_record_differences":report.vault_record_differences,
            "chatgpt_account_mismatches":report.chatgpt_account_mismatches});
        if !self.invalid_plan_tier_since.is_empty() {
            value["invalid_plan_tier_since"] = json!(self.invalid_plan_tier_since);
        }
        state.set_vault_report(value);
        self.last_failure = None;
    }

    fn failure(&mut self, state: &SharedState, code: String) {
        if self.last_failure.as_ref() != Some(&code) {
            tracing::warn!(target: "astrocyte", reason=%code, "subscription vault read unavailable");
            self.last_failure = Some(code.clone());
        }
        let mut report = state.vault_report();
        report["vault_read_status"] = json!({"status":"unavailable","reason":code});
        state.set_vault_report(report);
    }

    pub(crate) fn join(self) {
        self.reader.join();
    }
}

#[cfg(test)]
#[path = "vault_source_tests.rs"]
mod tests;
