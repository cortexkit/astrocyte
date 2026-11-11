use std::cell::Cell;

use astrocyte_core::ingest::{record_event_in_tx, DeliveryPath};
use rusqlite::{params_from_iter, Connection};

use super::*;

thread_local! {
    pub(super) static LOOKUP_VM_STEPS: Cell<i32> = const { Cell::new(0) };
}

fn transcript_request(producer: &str, producer_ref: &str) -> EventRecordRequest {
    serde_json::from_value(serde_json::json!({
        "producer": producer,
        "producer_ref": producer_ref,
        "charge_basis": "unknown",
    }))
    .unwrap()
}

fn insert_main(store: &Connection, harness: &str, attempt: &str, step: &str) {
    store
        .execute(
            "INSERT OR IGNORE INTO payer_session VALUES (?1,'session-a','owner-a',1000)",
            [harness],
        )
        .unwrap();
    store.execute("INSERT INTO payer_record(attempt_id,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,auth_selection) VALUES (?1,?2,'session-a','main',?3,1000,'anthropic','model-a','login')", params![attempt,harness,step]).unwrap();
}

#[test]
fn thalamus_payer_late_main_lookup_does_not_walk_unrelated_producer_events() {
    const UNRELATED: usize = 4_096;
    let (_home, source, store, state) = super::tests::fixture();
    state
        .ordered_write(|tx| {
            for index in 0..UNRELATED {
                record_event_in_tx(
                    tx,
                    &transcript_request("opencode", &format!("unrelated-{index}")),
                    1000,
                    DeliveryPath::Read,
                )
                .unwrap();
            }
            Ok(((), true))
        })
        .unwrap();
    insert_main(&store, "opencode", "absent-attempt", "absent-call");
    LOOKUP_VM_STEPS.with(|steps| steps.set(0));
    source
        .drain_for_health(&state, 2000, &AtomicBool::new(false))
        .unwrap();

    state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| {
                    row.get::<_, usize>(0)
                })
                .unwrap(),
                UNRELATED
            );
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM thalamus_payer_record WHERE attempt_id='absent-attempt'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                1
            );
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind='late'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                0
            );

            let mut explain = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {LATE_MAIN_LOOKUP_SQL}"))
                .unwrap();
            let bindings = vec![""; explain.parameter_count()];
            let plan = explain
                .query_map(params_from_iter(bindings), |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let steps = LOOKUP_VM_STEPS.with(Cell::get);
            println!(
                "late-main absent lookup: unrelated={UNRELATED}, vm_steps={steps}, plan={plan:?}"
            );
            assert!(steps > 0, "the drain must execute the late-main lookup");
            assert!(
                steps <= 64,
                "late-main lookup visited unrelated producer events: vm_steps={steps}"
            );
            assert_eq!(
                plan.iter()
                    .filter(|detail| detail.contains("SEARCH usage_event"))
                    .count(),
                1,
                "{plan:?}"
            );
            assert!(
                plan.iter().any(|detail| detail.contains("(event_id=?)")),
                "{plan:?}"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn thalamus_payer_late_main_counts_existing_transcript_calls_for_each_harness() {
    for harness in ["claude-code", "opencode", "pi"] {
        let (_home, source, store, state) = super::tests::fixture();
        state
            .ordered_write(|tx| {
                record_event_in_tx(
                    tx,
                    &transcript_request(harness, "recorded-call"),
                    1000,
                    DeliveryPath::Read,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        insert_main(&store, harness, "late-attempt", "recorded-call");
        insert_main(&store, harness, "absent-attempt", "absent-call");
        for at in [2000, 3000] {
            source
                .drain_for_health(&state, at, &AtomicBool::new(false))
                .unwrap();
        }
        state
            .with_conn(|conn| {
                let late = conn
                    .prepare("SELECT identity FROM thalamus_payer_coverage WHERE kind='late'")
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(late, ["late-attempt"], "{harness}");
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM usage_event WHERE producer=?1 AND producer_lineage IS NULL AND producer_ref='recorded-call'",
                        [harness],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                    1,
                    "{harness}"
                );
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_record", [], |row| row.get::<_, i64>(0)).unwrap(),
                    2,
                    "{harness}"
                );
                Ok(())
            })
            .unwrap();
    }
}
