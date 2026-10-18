//! Insula event delivery, account-isolated admission, and polling failover.
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astrocyte_core::capacity::{apply_account_in_tx, CapacitySnapshot};
use async_nats::jetstream::{self, consumer::PullConsumer};
use cortexkit_bus_naming::{AccountNames, EVENT_CONSUMERS};
use cortexkit_bus_nats::NatsConnection;
use cortexkit_provider_usage::ProviderUsage;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::quota_bus_client::{self, Access, Credential};
use crate::state::{now_ms, SharedState};

const BACKSTOP_MS: i64 = 600_000;
const PULL_FRESH_MS: i64 = 5_000;
type Account = (String, Option<String>);

struct Pending {
    snapshot: CapacitySnapshot,
    account: Account,
    done: oneshot::Sender<Result<(), String>>,
}

struct Inner {
    access: Option<Access>,
    attached: bool,
    credential_expires_ms: i64,
    last_pull_ms: Option<i64>,
    reason: &'static str,
    event_driven: bool,
    owed: u64,
    fulfilled: u64,
    last_snapshot_ms: Option<i64>,
    snapshot_boundary_ms: i64,
    snapshot_in_flight: bool,
    pending: VecDeque<Pending>,
}

/// Process-local cursors restart with a snapshot owed, even if a notice was acked before a crash.
pub(crate) struct Control(Mutex<Inner>);

impl Default for Control {
    fn default() -> Self {
        Self(Mutex::new(Inner {
            access: None,
            attached: false,
            credential_expires_ms: 0,
            last_pull_ms: None,
            reason: "no credential",
            event_driven: false,
            owed: 1,
            fulfilled: 0,
            last_snapshot_ms: None,
            snapshot_boundary_ms: i64::MIN,
            snapshot_in_flight: false,
            pending: VecDeque::new(),
        }))
    }
}

pub(crate) struct PollTicket {
    owed: u64,
    started_ms: i64,
}

impl Control {
    pub fn install_access(&self, path: PathBuf, module_id: String, nonce: String) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).access = Some(Access {
            path,
            module_id,
            nonce,
        });
    }

    fn access(&self) -> Option<Access> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .access
            .clone()
    }

    fn pulled(&self, exp: i64, now: i64) {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        inner.attached = true;
        inner.credential_expires_ms = exp.saturating_sub(60).saturating_mul(1000);
        inner.last_pull_ms = Some(now);
        inner.reason = "durable attached and pulling";
        mode(&mut inner, now);
    }

    fn unavailable(&self, reason: &'static str) {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        inner.attached = false;
        inner.reason = reason;
        mode(&mut inner, now_ms());
    }

    fn owe_snapshot(&self) {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        inner.owed = inner.owed.saturating_add(1);
    }

    pub fn poll_ticket(
        &self,
        now: i64,
        freshness: Option<&astrocyte_core::capacity::CapacityFreshness>,
    ) -> Option<PollTicket> {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let healthy = mode(&mut inner, now);
        // Empty pulls prove broker health, not producer liveness. Refresh a quiet reading
        // one cadence before its unchanged freshness horizon, and never later than ten minutes.
        let freshness_due = freshness.is_none_or(|freshness| {
            let interval = freshness
                .horizon_ms()
                .saturating_sub(freshness.effective_cadence_ms)
                .clamp(0, BACKSTOP_MS);
            freshness
                .last_success_ms
                .is_none_or(|last| now.saturating_sub(last) >= interval)
        });
        if !healthy
            || freshness_due
            || inner.owed != inner.fulfilled
            || inner
                .last_snapshot_ms
                .is_none_or(|last| now.saturating_sub(last) >= BACKSTOP_MS)
        {
            inner.snapshot_in_flight = true;
            Some(PollTicket {
                owed: inner.owed,
                started_ms: now,
            })
        } else {
            None
        }
    }

    pub fn finish_poll(&self, ticket: PollTicket, success: bool) {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        inner.snapshot_in_flight = false;
        if success {
            inner.fulfilled = ticket.owed;
            inner.last_snapshot_ms = Some(ticket.started_ms);
            inner.snapshot_boundary_ms = ticket.started_ms;
        }
    }

    fn submit(
        &self,
        notice: &Notice,
        snapshot: CapacitySnapshot,
        published_ms: i64,
    ) -> Option<oneshot::Receiver<Result<(), String>>> {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if inner.snapshot_in_flight || inner.owed != inner.fulfilled || inner.pending.len() >= 32 {
            inner.owed = inner.owed.saturating_add(1);
            return None;
        }
        // A full read started after publication already covers this historical notice.
        // Do not replace its newer account windows while draining deliver-all history.
        if published_ms <= inner.snapshot_boundary_ms {
            return None;
        }
        let (done, completed) = oneshot::channel();
        inner.pending.push_back(Pending {
            snapshot,
            account: notice.account(),
            done,
        });
        Some(completed)
    }
}

fn mode(inner: &mut Inner, now: i64) -> bool {
    let recent_pull = inner
        .last_pull_ms
        .is_some_and(|last| now.saturating_sub(last) < PULL_FRESH_MS);
    let healthy = inner.attached && now < inner.credential_expires_ms && recent_pull;
    if healthy != inner.event_driven {
        let reason = if inner.attached && !recent_pull {
            "successful pull is stale"
        } else if inner.attached && now >= inner.credential_expires_ms {
            "credential needs renewal"
        } else {
            inner.reason
        };
        tracing::info!(target: "capacity", mode = if healthy { "event-driven" } else { "polling" }, reason,
            "quota input mode changed");
        inner.event_driven = healthy;
    }
    healthy
}

/// The refresher owns writes, so joining the bus reader cannot leave a SQLite worker behind.
pub(crate) fn drain(state: &SharedState, stop: &AtomicBool) {
    let pending = {
        let mut inner = state
            .quota_bus()
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut inner.pending)
    };
    for reading in pending {
        let result = if stop.load(Ordering::Relaxed) {
            Err("quota reader stopping".into())
        } else {
            state.ordered_write(|tx| {
                apply_account_in_tx(
                    tx,
                    &reading.snapshot,
                    &reading.account.0,
                    reading.account.1.as_deref(),
                    now_ms(),
                )
                .map_err(|error| format!("apply quota account reading: {error}"))?;
                Ok(((), true))
            })
        };
        let _ = reading.done.send(result);
    }
}

#[derive(Clone)]
struct Notice {
    provider: String,
    account: Option<String>,
    epoch: String,
    seq: u64,
    key: String,
    digest: String,
}

impl Notice {
    fn account(&self) -> Account {
        (self.provider.clone(), self.account.clone())
    }

    fn decode(message: &jetstream::Message) -> Result<Self, &'static str> {
        if !message.payload.is_empty() {
            return Err("quota notice has a body");
        }
        let headers = message
            .headers
            .as_ref()
            .ok_or("quota notice headers absent")?;
        let get = |name: &str| {
            headers
                .get_last(name)
                .map(|value| value.as_str().to_owned())
        };
        if get("kind").as_deref() != Some("quota_sampled") {
            return Err("quota notice kind invalid");
        }
        let notice = Self {
            provider: get("provider").ok_or("quota notice provider absent")?,
            account: get("account"),
            epoch: get("epoch").ok_or("quota notice epoch absent")?,
            seq: get("seq")
                .and_then(|value| value.parse().ok())
                .ok_or("quota notice seq invalid")?,
            key: get("Nats-Msg-Id").ok_or("quota notice key absent")?,
            digest: get("Ck-Content-Digest").ok_or("quota notice digest absent")?,
        };
        if notice.provider.is_empty()
            || notice.epoch.is_empty()
            || notice.account.as_deref() == Some("")
            || notice.seq == 0
            || !hex_digest(&notice.key)
            || digest_hex(&notice.digest).is_none()
        {
            return Err("quota notice identity invalid");
        }
        Ok(notice)
    }
}

fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn digest_hex(value: &str) -> Option<&str> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    hex_digest(value).then_some(value)
}

#[derive(Deserialize)]
struct Fetched<'a> {
    #[serde(borrow)]
    body: &'a RawValue,
    digest: String,
    event_key: String,
    event_name: String,
    published_at: i64,
}

fn reading(notice: &Notice, bytes: &[u8]) -> Result<(CapacitySnapshot, i64), &'static str> {
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        result: Fetched<'a>,
    }
    let fetched: Envelope<'_> =
        serde_json::from_slice(bytes).map_err(|_| "quota body reply invalid")?;
    let fetched = fetched.result;
    let digest = format!("{:x}", Sha256::digest(fetched.body.get().as_bytes()));
    if fetched.event_key != notice.key
        || fetched.event_name != "quota_sampled"
        || fetched.digest != digest
        || digest_hex(&notice.digest) != Some(digest.as_str())
    {
        return Err("quota body digest or identity mismatch");
    }
    #[derive(Deserialize)]
    struct Identity {
        provider: String,
        account: Option<String>,
        epoch: String,
        seq: u64,
    }
    let identity: Identity =
        serde_json::from_str(fetched.body.get()).map_err(|_| "quota body identity invalid")?;
    if identity.provider != notice.provider
        || identity.account != notice.account
        || identity.epoch != notice.epoch
        || identity.seq != notice.seq
    {
        return Err("quota body does not match notice");
    }
    let entry: ProviderUsage =
        serde_json::from_str(fetched.body.get()).map_err(|_| "quota reading invalid")?;
    let snapshot = crate::capacity_map::map_usage_array(&[entry], None)
        .map_err(|_| "quota reading mapping failed")?;
    Ok((snapshot, fetched.published_at))
}

#[derive(Default)]
struct Cursors(HashMap<Account, (String, u64)>);

impl Cursors {
    async fn admit(
        &mut self,
        control: &Control,
        notice: Notice,
        fetch: impl std::future::Future<Output = Result<Vec<u8>, crate::refresher::PullError>>,
    ) -> Result<(), &'static str> {
        let key = notice.account();
        let last = self.0.get(&key);
        if last.is_some_and(|(epoch, seq)| epoch == &notice.epoch && notice.seq <= *seq) {
            return Ok(());
        }
        let in_sequence = last.is_some_and(|(epoch, seq)| {
            epoch == &notice.epoch && seq.checked_add(1) == Some(notice.seq)
        });
        if !in_sequence {
            control.owe_snapshot();
        } else {
            match fetch
                .await
                .ok()
                .and_then(|body| reading(&notice, &body).ok())
            {
                Some((snapshot, published_ms)) => {
                    if let Some(completed) = control.submit(&notice, snapshot, published_ms) {
                        completed
                            .await
                            .map_err(|_| "quota write cancelled")?
                            .map_err(|_| "quota write failed")?;
                    }
                }
                None => control.owe_snapshot(),
            }
        }
        self.0.insert(key, (notice.epoch, notice.seq));
        Ok(())
    }
}

pub(crate) struct Reader {
    thread: Option<std::thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl Reader {
    pub fn start(
        state: Arc<SharedState>,
        insula_path: PathBuf,
        stop: Arc<AtomicBool>,
    ) -> Option<Self> {
        let thread_stop = stop.clone();
        let thread = std::thread::Builder::new()
            .name("astrocyte-quota-bus".into())
            .spawn(move || {
                struct Stopped(Arc<SharedState>);
                impl Drop for Stopped {
                    fn drop(&mut self) {
                        self.0.quota_bus().unavailable("reader stopped");
                    }
                }
                let _stopped = Stopped(state.clone());
                // QtaClient's polling runtime is never entered here; event requests run on this reader.
                // Both runtimes are dropped on the OS thread, outside an async context.
                let Ok(insula) = crate::qta_client::QtaClient::new(insula_path) else {
                    return;
                };
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(async {
                    tokio::select! {
                        () = stopped(&thread_stop) => {},
                        () = reader_loop(&state, &insula) => {},
                    }
                });
            });
        match thread {
            Ok(thread) => Some(Self {
                thread: Some(thread),
                stop,
            }),
            Err(_) => {
                tracing::warn!(target: "capacity", "quota bus thread unavailable; retaining polling");
                None
            }
        }
    }

    pub fn join(mut self) {
        if self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_err())
        {
            tracing::warn!(target: "capacity", "quota bus reader panicked; retaining polling");
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_err())
        {
            tracing::warn!(target: "capacity", "quota bus reader panicked; retaining polling");
        }
    }
}

async fn stopped(stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn reader_loop(state: &SharedState, insula: &crate::qta_client::QtaClient) {
    let control = state.quota_bus();
    let access = loop {
        if let Some(access) = control.access() {
            break access;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let client = quota_bus_client::Client::new(access);
    let mut prior = None;
    let mut cursors = Cursors::default();
    let mut delay = Duration::from_millis(250);
    loop {
        let credential = match quota_bus_client::credential(&client, prior.as_ref()).await {
            Ok(credential) => credential,
            Err(reason) => {
                control.unavailable(reason);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        let session = async {
            let connection = quota_bus_client::connect(client.clone(), &credential).await?;
            let names =
                AccountNames::derive(&credential.acct).map_err(|_| "invalid bus account")?;
            let consumer = bind(&connection, &names.streams().event).await?;
            let subject = names
                .event_subject("insula", "quota_sampled", 1)
                .map_err(|_| "invalid quota subject")?;
            delay = Duration::from_millis(250);
            let remaining = credential
                .exp
                .saturating_sub(60)
                .saturating_sub(chrono::Utc::now().timestamp());
            let remaining = Duration::from_secs(u64::try_from(remaining).unwrap_or(0));
            tokio::time::timeout(
                remaining,
                run_session(
                    control,
                    insula,
                    &credential,
                    &consumer,
                    &subject,
                    &mut cursors,
                ),
            )
            .await
            .unwrap_or(Ok(()))
        }
        .await;
        control.unavailable(session.err().unwrap_or("credential renewal"));
        prior = Some(credential);
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

async fn bind(connection: &NatsConnection, stream: &str) -> Result<PullConsumer, &'static str> {
    let entry = EVENT_CONSUMERS
        .iter()
        .find(|entry| entry.module_id == "astrocyte")
        .ok_or("astrocyte consumer not in naming table")?;
    // Bootstrap owns topology. Even a missing durable must only trigger polling failover.
    jetstream::new(connection.client())
        .get_consumer_from_stream(entry.durable, stream)
        .await
        .map_err(|_| "quota durable unavailable")
}

async fn pull(consumer: &PullConsumer) -> Result<Vec<jetstream::Message>, &'static str> {
    let mut stream = consumer
        .fetch()
        .max_messages(32)
        .expires(Duration::from_secs(1))
        .messages()
        .await
        .map_err(|_| "quota pull failed")?;
    let mut batch = Vec::new();
    while let Some(message) = stream.next().await {
        batch.push(message.map_err(|_| "quota pull failed")?);
    }
    Ok(batch)
}

async fn run_session(
    control: &Control,
    insula: &crate::qta_client::QtaClient,
    credential: &Credential,
    consumer: &PullConsumer,
    subject: &str,
    cursors: &mut Cursors,
) -> Result<(), &'static str> {
    while chrono::Utc::now().timestamp() < credential.exp.saturating_sub(60) {
        let batch = pull(consumer).await?;
        control.pulled(credential.exp, now_ms());
        let commit = async {
            for message in &batch {
                if message.subject.as_str() == subject {
                    match Notice::decode(message) {
                        Ok(notice) => {
                            let key = notice.key.clone();
                            cursors
                                .admit(control, notice, insula.event_body(&key))
                                .await?;
                        }
                        Err(_) => control.owe_snapshot(),
                    }
                }
                message
                    .double_ack()
                    .await
                    .map_err(|_| "quota acknowledgement failed")?;
            }
            Ok::<(), &'static str>(())
        };
        tokio::pin!(commit);
        let mut progress = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(5),
            Duration::from_secs(5),
        );
        loop {
            tokio::select! {
                result = &mut commit => { result?; break; }
                _ = progress.tick() => {
                    for message in &batch {
                        message.ack_with(jetstream::AckKind::Progress).await
                            .map_err(|_| "quota progress acknowledgement failed")?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "quota_events_tests.rs"]
mod tests;
