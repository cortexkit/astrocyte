use super::*;
use crate::quota_events::tests::NatsServer;
use crate::state::test_state;
use crate::step_feed::{record_steps_for_test, TestStep};
use futures_util::FutureExt;
use std::sync::atomic::Ordering;

pub(crate) struct TestBus {
    pub(crate) server: NatsServer,
    pub(crate) names: AccountNames,
    pub(crate) connection: NatsConnection,
    pub(crate) multiplier: async_nats::jetstream::kv::Store,
    pub(crate) read_marks: async_nats::jetstream::kv::Store,
    control: Control,
    task: tokio::task::JoinHandle<()>,
}

impl TestBus {
    pub(crate) async fn start(state: &SharedState) -> Option<Self> {
        let server = NatsServer::start()?;
        let names = AccountNames::derive("astrocyte_test").unwrap();
        let connection = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(connection) = NatsConnection::connect(
                    &server.url,
                    async_nats::ConnectOptions::new(),
                    cortexkit_bus_nats::ConnectConfig::new("astrocyte-test").unwrap(),
                )
                .await
                {
                    break connection;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("nats-server did not become ready");
        let js = async_nats::jetstream::new(connection.client());
        let mut stores = Vec::new();
        for bucket in ["multiplier", "read-marks"] {
            let row = cortexkit_bus_naming::MODULE_BUCKETS
                .iter()
                .find(|row| row.owner == "astrocyte" && row.name == bucket)
                .unwrap();
            stores.push(
                js.create_key_value(async_nats::jetstream::kv::Config {
                    bucket: names.module_bucket_name("astrocyte", bucket).unwrap(),
                    history: i64::from(row.history),
                    max_value_size: i32::try_from(row.max_value_bytes).unwrap(),
                    max_bytes: i64::try_from(row.max_bytes).unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap(),
            );
        }
        let read_marks = stores.pop().unwrap();
        let multiplier = stores.pop().unwrap();
        let (control, writer, account) = (
            state.bus_publications().clone(),
            connection.clone(),
            names.clone(),
        );
        let publisher = control.clone();
        let task = tokio::spawn(async move {
            publisher.follow(&writer, &account).await;
        });
        let bus = Self {
            server,
            names,
            connection,
            multiplier,
            read_marks,
            control,
            task,
        };
        bus.after(0).await;
        Some(bus)
    }

    pub(crate) fn attempts(&self) -> u64 {
        self.control.attempts.load(Ordering::Acquire)
    }

    pub(crate) async fn after(&self, before: u64) {
        let generation = self.control.desired.lock().unwrap().generation;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let attempted = self.control.attempted.notified();
                if self.attempts() > before
                    && self.control.processed_generation.load(Ordering::Acquire) >= generation
                {
                    return;
                }
                attempted.await;
            }
        })
        .await
        .expect("publisher did not process its post-commit snapshot");
    }

    pub(crate) async fn finish(self) {
        self.task.abort();
        assert!(self.task.await.unwrap_err().is_cancelled());
        let _ = self.connection.close(Duration::from_secs(1)).await;
    }
}

#[test]
fn bus_publications_key_components_are_total_and_unambiguous() {
    assert_eq!(key_component("a/_.é"), "x612f5f2ec3a9");
    assert_eq!(pair_key("a.b", "c"), "x612e62.x63");
    assert_ne!(pair_key("a.b", "c"), pair_key("a", "b.c"));
    assert_eq!(key_component(""), "x");
}

#[tokio::test]
async fn bus_publications_steps_follow_commit_and_coalesce_to_newest_cursor() {
    let state = test_state();
    let Some(bus) = TestBus::start(&state).await else {
        return;
    };
    let subject = bus
        .names
        .live_subject("astrocyte", "steps_committed", 1)
        .unwrap();
    assert_eq!(
        subject,
        "ck.astrocyte_test.live.astrocyte.steps_committed.v1"
    );
    let mut hints = bus.connection.subscribe(&subject).await.unwrap();
    record_steps_for_test(&state, "pi", &[TestStep::now("rollback", "rb:1")], true).unwrap_err();
    assert!(state
        .bus_publications()
        .desired
        .lock()
        .unwrap()
        .steps
        .is_none());
    for step in ["s:1", "s:2", "s:3"] {
        record_steps_for_test(&state, "pi", &[TestStep::now("session", step)], false).unwrap();
    }
    let hint = tokio::time::timeout(Duration::from_secs(5), hints.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hint.subject.as_str(), subject);
    assert_eq!(hint.payload.as_ref(), br#"{"cursor":"c3"}"#);
    assert_eq!(state.step_feed().through(), Some(3));
    assert!(hints.next().now_or_never().is_none());
    bus.finish().await;
}

fn store_watermarks(state: &SharedState, at: i64, fail: bool) -> Result<(), String> {
    state.ordered_write(|tx| {
        for source in ["opencode-thalamus", "thalamus-payer"] {
            tx.execute("INSERT INTO read_watermark VALUES (?1,?2,?2) ON CONFLICT(source_key) DO UPDATE SET watermark_ms=excluded.watermark_ms,updated_at_ms=excluded.updated_at_ms",
                rusqlite::params![source, at]).map_err(|error| error.to_string())?;
        }
        if fail { return Err("rollback watermark".into()); }
        Ok(((), true))
    })
}

#[tokio::test]
async fn bus_publications_watermark_matches_served_value_only_after_commit() {
    let state = test_state();
    let Some(bus) = TestBus::start(&state).await else {
        return;
    };
    store_watermarks(&state, 120, true).unwrap_err();
    assert!(bus
        .read_marks
        .get("thalamus.watermark")
        .await
        .unwrap()
        .is_none());
    let before = bus.attempts();
    store_watermarks(&state, 123, false).unwrap();
    bus.after(before).await;
    let entry = bus
        .read_marks
        .entry("thalamus.watermark")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.value.as_ref(), br#"{"watermark_ms":123}"#);
    let principal = subc_protocol::Principal::Reserved {
        module_id: "thalamus-harness".into(),
    };
    let served = crate::ops::watermark_get(&state, Some(&principal), json!({})).unwrap();
    assert_eq!(entry.value.as_ref(), serde_json::to_vec(&served).unwrap());
    let before = bus.attempts();
    store_watermarks(&state, 123, false).unwrap();
    assert_eq!(bus.attempts(), before);
    assert_eq!(
        bus.read_marks
            .entry("thalamus.watermark")
            .await
            .unwrap()
            .unwrap()
            .revision,
        entry.revision
    );
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_outage_never_rolls_back_steps_or_watermarks_or_health() {
    let state = test_state();
    let Some(mut bus) = TestBus::start(&state).await else {
        return;
    };
    bus.server.stop();
    tokio::time::timeout(Duration::from_secs(5), async {
        while bus.connection.client().connection_state() == async_nats::connection::State::Connected
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = bus.attempts();
    record_steps_for_test(&state, "pi", &[TestStep::now("kept", "k:1")], false).unwrap();
    store_watermarks(&state, 456, false).unwrap();
    bus.after(before).await;
    assert_eq!(state.step_feed().through(), Some(1));
    let principal = subc_protocol::Principal::Reserved {
        module_id: "thalamus-harness".into(),
    };
    assert_eq!(
        crate::ops::watermark_get(&state, Some(&principal), json!({})).unwrap(),
        json!({"watermark_ms":456})
    );
    assert!(state.crash_cause().is_none());
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_multiplier_reconciles_null_plan_and_unchanged_values() {
    let state = test_state();
    let Some(bus) = TestBus::start(&state).await else {
        return;
    };
    let before = bus.attempts();
    state
        .bus_publications()
        .multiplier(&[json!({"quota_provider":"codex","plan":null,"multiplier":2})]);
    bus.after(before).await;
    assert_eq!(
        bus.multiplier
            .get("x636f646578.n")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        br#"{"multiplier":2,"plan":null,"quota_provider":"codex"}"#
    );
    let latest = json!({"quota_provider":"codex","plan":"openai/pro_20x","multiplier":3});
    let before = bus.attempts();
    state
        .bus_publications()
        .multiplier(std::slice::from_ref(&latest));
    bus.after(before).await;
    assert!(bus.multiplier.get("x636f646578.n").await.unwrap().is_none());
    let key = "x636f646578.x6f70656e61692f70726f5f323078";
    let entry = bus.multiplier.entry(key).await.unwrap().unwrap();
    assert_eq!(entry.value.as_ref(), serde_json::to_vec(&latest).unwrap());
    let before = bus.attempts();
    state
        .bus_publications()
        .multiplier(std::slice::from_ref(&latest));
    assert_eq!(bus.attempts(), before);
    assert_eq!(
        bus.multiplier.entry(key).await.unwrap().unwrap().revision,
        entry.revision
    );
    let before = bus.attempts();
    state.bus_publications().multiplier(&[
        json!({"quota_provider":"codex","plan":"openai/pro_20x","samples":vec![0;8192]}),
    ]);
    bus.after(before).await;
    assert!(bus.multiplier.get(key).await.unwrap().is_none());
    let before = bus.attempts();
    state.bus_publications().multiplier(&[latest]);
    bus.after(before).await;
    assert!(bus.multiplier.get(key).await.unwrap().is_some());
    let longest = pair_key("kimi-for-coding", "kimi-code-plan-global/allegro");
    bus.multiplier
        .put(&longest, b"{}".to_vec().into())
        .await
        .unwrap();
    assert_eq!(
        bus.multiplier
            .get(&longest)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"{}"
    );
    bus.finish().await;
}
