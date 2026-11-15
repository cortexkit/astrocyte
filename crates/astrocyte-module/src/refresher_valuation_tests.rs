use super::*;

#[test]
fn valuation_tick_has_its_own_budget_after_summary_maintenance() {
    let state = crate::state::test_state();
    crate::ops::dispatch(&state,"spend.event.ingest",serde_json::json!({
        "producer":"fixture","producer_ref":"valuation-budget","charge_basis":"metered_api","occurred_at_ms":1,
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":12,"quantity_exponent":0}]
    })).unwrap();
    let request = serde_json::json!({"grouping":["provider"],"grain":"day","timezone":"UTC","from_ms":0,"to_ms":3_600_000});
    let mut before = crate::ops::dispatch(&state, "spend.report", request.clone()).unwrap();
    let stop = AtomicBool::new(false);
    let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
    summary_tick(&state, &mut summaries, &stop);
    let query = |sql: &str| {
        state
            .with_conn(|conn| {
                conn.query_row(sql, [], |r| r.get::<_, i64>(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    };
    assert!(query("SELECT COUNT(*) FROM summary_cell") > 0);
    let mut valuations = astrocyte_core::report::valuation::ValuationMaintenance::default();
    valuation_tick(&state, &mut valuations, &stop, Duration::ZERO);
    assert_eq!(query("SELECT COUNT(*) FROM valuation_cell"), 0);
    assert_eq!(query("SELECT COUNT(*) FROM summary_dirty_hour"), 0);
    valuation_tick(&state, &mut valuations, &stop, Duration::from_secs(1));
    assert_eq!(
        query("SELECT COUNT(*) FROM valuation_hour WHERE state='built'"),
        1
    );
    assert_eq!(query("SELECT COUNT(*) FROM valuation_cell"), 1);
    let mut after = crate::ops::dispatch(&state, "spend.report", request).unwrap();
    // Served reads have different clock instants, but all report data is stable.
    for response in [&mut before, &mut after] {
        response["rate_staleness"]
            .as_object_mut()
            .unwrap()
            .remove("as_of_ms");
    }
    assert_eq!(after, before);
}
