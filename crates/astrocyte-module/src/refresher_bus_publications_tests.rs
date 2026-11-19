#[tokio::test]
async fn bus_publications_multiplier_daily_and_recompute_match_report_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    let key = "x636f646578.x6f70656e61692f70726f5f323078";
    let before = bus.attempts();
    MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
    bus.after(before).await;
    let report = crate::ops::dispatch(
        &state,
        crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms":0,"to_ms":1}),
    )
    .unwrap();
    assert_eq!(report["latest"].as_array().unwrap().len(), 1);
    assert_eq!(report["latest"][0]["plan"], "openai/pro_20x");
    assert!(
        serde_json::to_vec(&report["latest"][0]).unwrap().len() <= 8192,
        "report entry bytes: {}",
        serde_json::to_vec(&report["latest"][0]).unwrap().len()
    );
    assert_eq!(
        bus.multiplier.get(key).await.unwrap().unwrap().as_ref(),
        serde_json::to_vec(&report["latest"][0]).unwrap()
    );
    let original = bus.multiplier.entry(key).await.unwrap().unwrap();
    fusiform
        .input_rate_units
        .store(20_000_000_000, Ordering::SeqCst);
    let before = bus.attempts();
    recompute(&state, &fusiform, DAY, None, NOW + 1).unwrap();
    bus.after(before).await;
    let report = crate::ops::dispatch(
        &state,
        crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms":0,"to_ms":1}),
    )
    .unwrap();
    let corrected = bus.multiplier.entry(key).await.unwrap().unwrap();
    assert_eq!(
        corrected.value.as_ref(),
        serde_json::to_vec(&report["latest"][0]).unwrap()
    );
    assert!(corrected.revision > original.revision);
    assert_eq!(report["latest"][0]["api_nanos"], 20_000_000);
    // Removing config does not erase historical entries from the served report.
    let mut empty: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
    empty["subscription_multiplier"]["quota_providers"] = json!({});
    publish_document(&state, &directory, &empty.to_string());
    crate::bus_publications::publish_multiplier(&state);
    assert_eq!(
        bus.multiplier.entry(key).await.unwrap().unwrap().revision,
        corrected.revision
    );
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_multiplier_last_eligible_matches_report() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    state.ordered_write(|tx| {
        // Spread equal attributed usage over seven days so the daily stage
        // produces an eligible head rather than a stability diagnostic.
        for index in 0..40 {
            let id = format!("eligible-{index}");
            let at = DAY_START - (index % 7 + 1) * DAY_MS
                + ((index / 7) % 4 * 6 + 3) * 3_600_000;
            tx.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES (?1,'opencode','lineage',?1,?2,?2,'openai','m','unknown','acct')", params![id, at]).map_err(|error| error.to_string())?;
            tx.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','usage.input',1000,0)", [id]).map_err(|error| error.to_string())?;
        }
        Ok(((), true))
    }).unwrap();
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    let key = "x636f646578.x6f70656e61692f70726f5f323078";
    let before = bus.attempts();
    MultiplierStage::new(Some(fusiform)).tick(&state, false, NOW);
    bus.after(before).await;

    let report = crate::ops::dispatch(
        &state,
        crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms":0,"to_ms":1}),
    )
    .unwrap();
    let last_eligible = report["latest"][0]
        .get("last_eligible")
        .expect("an eligible latest row carries its own values");
    assert_eq!(
        last_eligible["day"],
        report["latest"][0]["observation_day"]
    );
    let published = bus.multiplier.get(key).await.unwrap().unwrap();
    let published: serde_json::Value = serde_json::from_slice(published.as_ref()).unwrap();
    assert_eq!(published["last_eligible"], *last_eligible);
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_multiplier_startup_restores_current_pairs() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    MultiplierStage::new(Some(fusiform)).tick(&state, false, NOW);
    state.bus_publications().clear_multiplier_for_test();
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    let key = "x636f646578.x6f70656e61692f70726f5f323078";
    let mut updates = bus.multiplier.watch(key).await.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let reader = crate::quota_events::Reader::start(state.clone(), None, stop.clone()).unwrap();
    let entry = tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::StreamExt::next(&mut updates),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(entry.key, key);
    assert!(bus.multiplier.get(key).await.unwrap().is_some());
    stop.store(true, Ordering::Relaxed);
    reader.join();
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_multiplier_outage_keeps_daily_and_recompute_commits() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    let Some(mut bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    bus.server.stop();
    let before = bus.attempts();
    MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
    bus.after(before).await;
    assert_eq!(current_day_observations(&state).len(), 1);
    let before = bus.attempts();
    recompute(&state, &fusiform, DAY, None, NOW + 1).unwrap();
    bus.after(before).await;
    assert_eq!(current_day_observations(&state).len(), 2);
    assert!(state.crash_cause().is_none());
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_watermark_config_exclusion_revokes_and_reinclude_restores_key() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    state
        .ordered_write(|tx| {
            for source in ["opencode-thalamus", "thalamus-payer"] {
                tx.execute("INSERT INTO read_watermark VALUES (?1,123,123)", [source])
                    .unwrap();
            }
            Ok(((), true))
        })
        .unwrap();
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    assert_eq!(
        bus.read_marks
            .get("thalamus.watermark")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        br#"{"watermark_ms":123}"#
    );
    for disable_all in [false, true] {
        let mut document: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
        if disable_all {
            document["harness_ingest"]["enabled"] = json!(false);
        } else {
            document["harness_ingest"]["exclude"] = json!(["opencode-thalamus"]);
        }
        let before = bus.attempts();
        publish_document(&state, &directory, &document.to_string());
        bus.after(before).await;
        assert!(bus
            .read_marks
            .get("thalamus.watermark")
            .await
            .unwrap()
            .is_none());
        let before = bus.attempts();
        publish_desired(&state, &directory);
        bus.after(before).await;
        assert_eq!(
            bus.read_marks
                .get("thalamus.watermark")
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            br#"{"watermark_ms":123}"#
        );
    }
    bus.finish().await;
}
