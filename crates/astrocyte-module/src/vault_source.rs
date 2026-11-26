//! List-only vault access. Network work belongs to a separate reader, never
//! startup or the ledger writer; the full refresher tick applies its snapshots.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astrocyte_core::multiplier::PlanPriceSource;
use astrocyte_core::vault_plan::{self, VaultPlan};
use cortexkit_claustrum_client::{decode_list_scoped_reply, WatchNotice};
use fusiform_protocol::PlanPricesRequest;
use serde::Serialize;
use serde_json::{json, Value};
use subc_client_rs::{CallOptions, ConsumerOptions, RouteHandle, SubcConsumer, SubscribeOptions};
use subc_protocol::{Priority, RouteTarget};
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
    // Without an explicit start date, a new observation timestamp is not
    // evidence that an unchanged plan started at a different time.
    start_date_unspecified: Vec<String>,
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
    if !listing
        .grant_tuples
        .iter()
        .any(|grant| grant.operation == "list")
    {
        return Err("list_grant_missing".into());
    }
    let raw = value["result"]["credentials"]
        .as_array()
        .ok_or("invalid_listing")?;
    let mut invalid_plan_tier_since = Vec::new();
    let mut start_date_unspecified = Vec::new();
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
            if since.is_none() {
                start_date_unspecified.push(record.id.clone());
            }
            let effective = since.or(observed).unwrap_or(now);
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
        start_date_unspecified,
    })
}

struct Route {
    consumer: SubcConsumer,
    handle: RouteHandle,
}

fn call_options() -> CallOptions {
    CallOptions {
        timeout: BUDGET,
        route_retry_deadline: BUDGET,
        ..CallOptions::default()
    }
}

fn sdk_refusal(error: subc_client_rs::CallError) -> String {
    let code = error
        .route_open_refusal()
        .map(|body| body.code.as_str())
        .or_else(|| error.code());
    safe_refusal(&json!({"code": code}))
}

impl Route {
    async fn open(access: &Access) -> Result<Self, String> {
        // Supervised connections get a self claim from the SDK's serving registration owner.
        let consumer = SubcConsumer::connect(&access.path, ConsumerOptions::default())
            .await
            .map_err(|_| "vault_unavailable")?;
        let identity = serde_json::from_value(
            json!({"harness":"ck-astrocyte","session":"subscription-plans"}),
        )
        .map_err(|_| "encode")?;
        let handle = consumer
            .open_route(
                RouteTarget::ManagementSurface {
                    module_id: "claustrum".into(),
                },
                identity,
                call_options(),
            )
            .await
            .map_err(sdk_refusal)?;
        Ok(Self { consumer, handle })
    }

    async fn list(access: &Access) -> Snapshot {
        tokio::time::timeout(BUDGET, async {
            let route = Self::open(access).await?;
            let reply = route
                .consumer
                .request(
                    &route.handle,
                    br#"{"method":"credential.list_scoped","params":{}}"#.to_vec(),
                    call_options(),
                )
                .await
                .map_err(sdk_refusal)?;
            let value = serde_json::from_slice(&reply).map_err(|_| "invalid_response")?;
            decode_listing(value, crate::state::now_ms())
        })
        .await
        .unwrap_or_else(|_| Err("vault_timeout".into()))
    }

    async fn watch(access: Access, notices: mpsc::UnboundedSender<Result<(), String>>) {
        let opened = tokio::time::timeout(BUDGET, async {
            let route = Self::open(&access).await?;
            let subscription = route
                .consumer
                .subscribe_route(
                    &route.handle,
                    br#"{"method":"auth.watch_scoped","params":{}}"#.to_vec(),
                    SubscribeOptions {
                        priority: Priority::Passive,
                        route_open_timeout: BUDGET,
                        ..SubscribeOptions::default()
                    },
                )
                .await
                .map_err(sdk_refusal)?;
            Ok::<_, String>((route, subscription))
        })
        .await;
        let (_route, mut subscription) = match opened {
            Ok(Ok(opened)) => opened,
            _ => {
                let _ = notices.send(Err("vault_watch_unavailable".into()));
                return;
            }
        };
        while let Some(body) = subscription.events().recv().await {
            let result = WatchNotice::decode(&body)
                .map(|_| ())
                .map_err(|_| "vault_watch_lost".into());
            let ended = result.is_err();
            if notices.send(result).is_err() || ended {
                return;
            }
        }
        let reason = subscription
            .closed()
            .await
            .err()
            .map(sdk_refusal)
            .unwrap_or_else(|| "vault_watch_lost".into());
        let _ = notices.send(Err(reason));
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
                            last_list = snapshot.as_ref().ok().map(|_| crate::state::now_ms());
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

#[derive(Default)]
struct SnapshotChanges {
    history: bool,
    seed_confirmed: bool,
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
    /// Credential id to the plan key the vault's latest listing states for it,
    /// for records whose tier the vault itself sourced (not config seeds).
    /// Reconciliation reads plan history, which keeps a config seed as the
    /// newest row when the vault later states the same tier with no start
    /// date; this map is how it sees that the vault confirms the seed.
    vault_tiers: BTreeMap<String, String>,
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
            vault_tiers: BTreeMap::new(),
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
                    let result = self.append_snapshot(state, &listing).and_then(|changes| {
                        self.invalid_plan_tier_since = listing.invalid_plan_tier_since;
                        self.vault_tiers = listing
                            .plans
                            .iter()
                            .filter(|plan| plan.source != "config")
                            .filter_map(|plan| {
                                Some((plan.credential_id.clone(), plan.plan_key.clone()?))
                            })
                            .collect();
                        self.available = true;
                        self.process(state, source, now, changes)?;
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
            match self.process(state, source, now, SnapshotChanges::default()) {
                Ok(true) => self.publish(state),
                Ok(false) => {}
                Err(error) => self.failure(state, error),
            }
        }
    }

    fn append_snapshot(
        &self,
        state: &SharedState,
        listing: &Listing,
    ) -> Result<SnapshotChanges, String> {
        let mut plans = listing.plans.clone();
        let mut changes = state.with_conn(|conn| {
            let mut changes = SnapshotChanges::default();
            for plan in &mut plans {
                // With no start date from the vault, an unchanged plan keeps
                // its recorded start, so a later listing does not restamp it;
                // that includes a change only to who stated the tier. A changed
                // provider, account, tier or key starts at this listing's
                // observation time.
                if listing.start_date_unspecified.contains(&plan.credential_id) {
                    let previous = vault_plan::latest(conn, &plan.credential_id)
                        .map_err(|error| error.to_string())?;
                    if let Some(previous) = previous.filter(|previous| previous.same_facts(plan)) {
                        plan.effective_from_ms = previous.effective_from_ms;
                        // An unchanged credential-only seed stays in history,
                        // but its first Vault confirmation can open coverage.
                        changes.seed_confirmed |= previous.source == "config"
                            && plan.source != "config"
                            && plan.account_id.is_none()
                            && plan.plan_key.is_some()
                            && self.vault_tiers.get(&plan.credential_id) != plan.plan_key.as_ref();
                    }
                }
                changes.history |= vault_plan::differs_from_latest(conn, plan)
                    .map_err(|error| error.to_string())?;
            }
            Ok(changes)
        })?;
        if changes.history {
            changes.history = state.ordered_tail_write(|tx| {
                let mut changed = false;
                for plan in &plans {
                    changed |=
                        vault_plan::append_in_tx(tx, plan).map_err(|error| error.to_string())?;
                }
                Ok((changed, changed))
            })?;
        }
        Ok(changes)
    }

    fn process(
        &mut self,
        state: &SharedState,
        source: Option<&dyn PlanPriceSource>,
        now: i64,
        changes: SnapshotChanges,
    ) -> Result<bool, String> {
        let interval = if self.diagnostics.unpriced_plans.is_empty() {
            DAY_MS
        } else {
            UNPRICED_RETRY_MS
        };
        let reconcile = changes.history
            || changes.seed_confirmed
            || self
                .last_reconcile_ms
                .is_none_or(|at| now.saturating_sub(at) >= interval);
        let mismatches = changes.history
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
        // Only reconciliation asks the plan-price source (Fusiform's
        // `plan.prices`), and it does so before any writer
        // transaction starts. Plans it can't price stay listed as unpriced
        // and are retried hourly.
        let result = source
            .ok_or_else(|| "plan_prices_unavailable".to_string())
            .and_then(|source| {
                source
                    .plan_prices(PlanPricesRequest {
                        provider_id: None,
                        at_ms: None,
                    })
                    .map_err(|_| "plan_prices_unavailable".into())
            });
        self.price_available = result.is_ok();
        let prices = result.unwrap_or_else(|_| fusiform_protocol::PlanPricesResponse {
            prices: Vec::new(),
            resolved_at_ms: now,
            tier_vocabulary: String::new(),
            unit_policy: String::new(),
            quota_pools: Vec::new(),
        });
        let vault_tiers = &self.vault_tiers;
        let prepared = state.with_conn(|conn| {
            vault_plan::prepare_reconciliation(conn, plans, vault_tiers, &prices, now)
        })?;
        let mut report = if prepared.has_changes() {
            // A hand-set plan can win after the preview. Recheck ownership in the
            // write transaction; if no changes remain, roll back rather than commit.
            state.ordered_tail_write(|tx| {
                let prepared =
                    vault_plan::prepare_reconciliation(tx, plans, vault_tiers, &prices, now)?;
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
        if !report.reconciliation_refusals.is_empty() {
            value["reconciliation_refusals"] = json!(report.reconciliation_refusals);
        }
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
