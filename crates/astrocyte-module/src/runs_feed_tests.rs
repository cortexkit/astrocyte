use super::*;
use crate::state::test_state;
use tokio::sync::{mpsc, Notify};

struct Sink {
    sent: mpsc::UnboundedSender<Value>,
    ended: Notify,
}
#[subc_client_rs::async_trait]
impl NoticeSink for Sink {
    async fn emit(&self, notice: Vec<u8>) -> Result<(), String> {
        self.sent
            .send(serde_json::from_slice(&notice).unwrap())
            .map_err(|error| error.to_string())
    }
    async fn ended(&self) {
        self.ended.notified().await;
    }
}

fn record(state: &SharedState, id: &str) {
    crate::step_feed::record_steps_for_test(
        state,
        "pi",
        &[crate::step_feed::TestStep::now("runs", id)],
        false,
    )
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn runs_subscribe_commit_coalesces_and_idle_is_quiet() {
    let state = test_state();
    let (sent, mut notices) = mpsc::unbounded_channel();
    let sink = Arc::new(Sink {
        sent,
        ended: Notify::new(),
    });
    let (stop, stopping) = watch::channel(false);
    let serving = {
        let state = state.clone();
        let sink = sink.clone();
        tokio::spawn(async move { subscribe(&state, json!({}), sink.as_ref(), stopping).await })
    };
    tokio::task::yield_now().await;
    assert!(
        notices.try_recv().is_err(),
        "opening a watch is not a change"
    );
    record(&state, "1");
    tokio::task::yield_now().await;
    let first = notices.try_recv().expect("committed call must notify");
    assert_eq!(first.as_object().unwrap().len(), 1);
    record(&state, "2");
    record(&state, "3");
    tokio::task::yield_now().await;
    assert!(notices.try_recv().is_err());
    tokio::time::advance(INTERVAL).await;
    tokio::task::yield_now().await;
    let second = notices.try_recv().expect("burst must emit one successor");
    assert!(second["cursor"].as_str().unwrap() > first["cursor"].as_str().unwrap());
    assert!(notices.try_recv().is_err());
    state.ordered_write(|_| Ok(((), true))).unwrap();
    tokio::time::advance(INTERVAL * 5).await;
    tokio::task::yield_now().await;
    assert!(
        notices.try_recv().is_err(),
        "idle projection commits must stay quiet"
    );
    sink.ended.notify_one();
    assert!(
        matches!(serving.await.unwrap(), HandlerOutcome::Error { code, .. } if code == "cancelled")
    );
    drop(stop);
}

#[test]
fn runs_subscribe_watermark_uses_fixed_indexed_steps() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE usage_event(event_key INTEGER PRIMARY KEY); CREATE TABLE priced_charge(charge_key INTEGER PRIMARY KEY); INSERT INTO usage_event VALUES(1); INSERT INTO priced_charge VALUES(1);").unwrap();
    let steps = || {
        let mut statement = conn.prepare(WATERMARK).unwrap();
        let keys: (i64, i64) = statement
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        (
            keys,
            statement.get_status(rusqlite::StatementStatus::VmStep),
        )
    };
    let small = steps();
    conn.execute_batch("WITH RECURSIVE n(x) AS (VALUES(2) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO usage_event SELECT x FROM n; INSERT INTO priced_charge SELECT event_key FROM usage_event WHERE event_key>1;").unwrap();
    let large = steps();
    assert_eq!(large.0, (10000, 10000));
    assert_eq!(small.1, large.1, "watermark must not scan the ledger");
    assert!(large.1 < 50);
}

#[test]
fn runs_subscribe_rollback_and_rebuild_markers() {
    let state = test_state();
    let notices = state.open_runs_watch().unwrap();
    let initial = notices.borrow().clone();
    let failed = crate::step_feed::record_steps_for_test(
        &state,
        "pi",
        &[crate::step_feed::TestStep::now("runs", "rolled-back")],
        true,
    );
    assert!(failed.is_err());
    assert_eq!(*notices.borrow(), initial);
    state.runs_summary_committed();
    assert_ne!(*notices.borrow(), initial);
}

#[test]
fn runs_subscribe_new_and_superseding_charge_heads_notify() {
    let state = test_state();
    record(&state, "priced");
    let notices = state.open_runs_watch().unwrap();
    let initial = notices.borrow().clone();
    for generation in [1, 2] {
        state.ordered_write(|tx| {
            tx.execute("UPDATE usage_event SET pricing_generation=?1", [generation]).map_err(|error| error.to_string())?;
            tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,unpriced_reason,supersedes,origin,recorded_at_ms) VALUES(1,'input','token',?1,'unpriced','unknown',0,'charge_basis_unknown',?2,'pricing_pass',100)", rusqlite::params![generation, if generation==1 {None} else {Some(1)}]).map_err(|error| error.to_string())?;
            Ok(((),true))
        }).unwrap();
        assert_ne!(*notices.borrow(), initial);
        assert!(notices.borrow().ends_with(":0000000000000000"));
        assert!(notices.borrow().contains(&format!(":{generation:016x}:")));
    }
    let (charges, heads): (i64,i64) = state.with_conn(|conn| conn.query_row("SELECT count(*),sum(NOT EXISTS(SELECT 1 FROM priced_charge newer WHERE newer.supersedes=c.charge_key)) FROM priced_charge c", [], |row| Ok((row.get(0)?,row.get(1)?))).map_err(|error| error.to_string())).unwrap();
    assert_eq!((charges, heads), (2, 1));
}

#[test]
fn runs_subscribe_vectors() {
    use sha2::{Digest, Sha256};
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test-vectors/usage-runs-subscribe-v1");
    for line in std::fs::read_to_string(directory.join("SHA256SUMS"))
        .unwrap()
        .lines()
    {
        let (hash, name) = line.split_once("  ").unwrap();
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(std::fs::read(directory.join(name)).unwrap())
            ),
            hash
        );
    }
    let load = |name| {
        serde_json::from_slice::<Value>(&std::fs::read(directory.join(name)).unwrap()).unwrap()
    };
    let request = load("request.json");
    assert_eq!(request["method"], crate::ops::OP_RUNS_SUBSCRIBE);
    serde_json::from_value::<Request>(request["params"].clone()).unwrap();
    let state = test_state();
    let notices = state.open_runs_watch().unwrap();
    record(&state, "vector");
    assert_eq!(
        load("notice.json"),
        json!({"cursor":notices.borrow().clone()})
    );
    let (code, message) =
        crate::step_feed::require_read_permission(crate::ops::OP_RUNS_SUBSCRIBE, false)
            .unwrap_err();
    assert_eq!(
        load("not-permitted.json"),
        json!({"error":{"code":code,"message":message}})
    );
}
