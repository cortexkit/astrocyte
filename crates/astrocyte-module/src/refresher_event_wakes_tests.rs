use super::*;
use crate::event_wakes::Source;
use crate::state::test_state;
use serde_json::json;

#[test]
fn event_wakes_catalog_coalesces_and_retries_failed_discovery() {
    use crate::fusiform_client::fixtures::{CatalogPeer, StatusMode};
    let peer =
        CatalogPeer::start_with_status(100, 9, 66_666, StatusMode::Transient { failures: 2 });
    let state = test_state();
    let mut config = config::intentional_empty().config;
    config.fusiform_connection_file = Some(peer.path.to_string_lossy().into_owned());
    let (mut pending, _) = configure_catalog(&state, &config, &AtomicBool::new(false)).unwrap();
    assert!(pending.is_some());
    state.event_wakes().attached(&[Source::Catalog], true);
    let mut multiplier = MultiplierStage::new(None);
    let mut failed = state.event_wakes().wake(Source::Catalog);
    catalog_wake_tick(
        &state,
        &mut pending,
        &mut multiplier,
        &AtomicBool::new(false),
        false,
    );
    assert_eq!(failed.try_recv(), Ok(false));
    assert!(state.catalog_client().is_none());
    let mut first = state.event_wakes().wake(Source::Catalog);
    let mut second = state.event_wakes().wake(Source::Catalog);
    catalog_wake_tick(
        &state,
        &mut pending,
        &mut multiplier,
        &AtomicBool::new(false),
        false,
    );
    assert_eq!(first.try_recv(), Ok(true));
    assert_eq!(second.try_recv(), Ok(true));
    assert!(pending.is_none());
    assert_eq!(state.catalog_client().unwrap().history_start_ms(), 100);
    for _ in 0..10 {
        catalog_wake_tick(
            &state,
            &mut pending,
            &mut multiplier,
            &AtomicBool::new(false),
            true,
        );
    }
    let requests = peer.finish();
    assert_eq!(requests.iter().filter(|request| request["name"] == "catalog.status").count(), 4,
        "one failed startup, one failed wake, one coalesced discovery-and-version cycle, no idle polls");
}

#[test]
fn event_wakes_new_model_gets_rates_on_next_full_tick_without_status_rpc() {
    use crate::fusiform_client::fixtures::CatalogPeer;
    let peer = CatalogPeer::start(100, 9, 66_666);
    let state = test_state();
    let mut config = config::intentional_empty().config;
    config.fusiform_connection_file = Some(peer.path.to_string_lossy().into_owned());
    let (mut pending, _) = configure_catalog(&state, &config, &AtomicBool::new(false)).unwrap();
    state.event_wakes().attached(&[Source::Catalog], true);
    let mut multiplier = MultiplierStage::new(None);
    catalog_wake_tick(
        &state,
        &mut pending,
        &mut multiplier,
        &AtomicBool::new(false),
        true,
    );
    let request = serde_json::from_value::<astrocyte_core::ingest::EventRecordRequest>(json!({
        "producer":"pi", "producer_ref":"new-model", "session":"new-model-session",
        "provider":"anthropic", "model":"claude-opus-5-5", "occurred_at_ms":1_790_000_000_000_i64,
        "charge_basis":"unknown", "measurements":[{"role":"input", "unit":"token", "kind":"billed", "source":"usage.input", "quantity_units":1, "quantity_exponent":0}]
    })).unwrap();
    state
        .ordered_write(|tx| {
            astrocyte_core::ingest::record_event_in_tx(
                tx,
                &request,
                100,
                astrocyte_core::ingest::DeliveryPath::Read,
            )
            .map_err(|error| format!("{error:?}"))?;
            Ok(((), true))
        })
        .unwrap();
    let _hold = hold_pricing_pass_for_test(&state);
    run_tick(
        &state,
        &[],
        &FileSources::default(),
        None,
        &mut pending,
        &mut multiplier,
        &mut PricingWalk::new(PricingBudget::PER_TICK),
        &mut QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET),
        &AtomicBool::new(false),
    );
    multiplier.tick_days(&state, false, current_time_ms(), &AtomicBool::new(false));
    let (version, segments): (i64,i64) = state.with_conn(|conn| {
        conn.query_row("SELECT catalog_version,(SELECT count(*) FROM rate_era WHERE provider='anthropic' AND model='claude-opus-5-5') FROM rate_model WHERE provider='anthropic' AND model='claude-opus-5-5'", [], |row| Ok((row.get(0)?,row.get(1)?))).map_err(|error| error.to_string())
    }).unwrap();
    assert_eq!(version, 987);
    assert!(
        segments > 0,
        "newly ingested model must have its served rates"
    );
    let requests = peer.finish();
    assert_eq!(requests.iter().filter(|request| request["name"] == "catalog.status").count(), 2,
        "only startup discovery and the first authoritative version read; no status RPC on the next full tick");
    assert!(requests
        .iter()
        .any(|request| request["name"] == "catalog.history"
            && request["arguments"]["model_id"] == "claude-opus-5-5"));
}

#[test]
fn runs_subscribe_summary_publication_notifies_after_run_cells_rebuilt() {
    let state = test_state();
    crate::step_feed::record_steps_for_test(
        &state,
        "pi",
        &[crate::step_feed::TestStep::now("summary", "step")],
        false,
    )
    .unwrap();
    let notices = state.open_runs_watch().unwrap();
    let before = notices.borrow().clone();
    let mut maintenance = astrocyte_core::report::SummaryMaintenance::default();
    summary_tick_within(
        &state,
        &mut maintenance,
        &AtomicBool::new(false),
        Duration::from_secs(5),
    );
    let cells: i64 = state
        .with_conn(|conn| {
            conn.query_row("SELECT count(*) FROM summary_run_cell", [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())
        })
        .unwrap();
    assert!(cells > 0);
    assert_ne!(*notices.borrow(), before);
    let after = notices.borrow().clone();
    summary_tick_within(
        &state,
        &mut maintenance,
        &AtomicBool::new(false),
        Duration::from_secs(5),
    );
    assert_eq!(
        *notices.borrow(),
        after,
        "an idle summary audit is not a rebuilt hour"
    );
}
