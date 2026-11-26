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
    let key = "x636f646578.x6f70656e61692f70726f5f323078.x7072696d617279";
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
    // Disabling ingestion does not erase historical entries from the served report.
    let mut empty: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
    empty["harness_ingest"]["enabled"] = json!(false);
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
            tx.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES (?1,'opencode','lineage',?1,?2,?2,'openai','m','subscription_included','acct')", params![id, at]).map_err(|error| error.to_string())?;
            tx.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','usage.input',1000,0)", [id]).map_err(|error| error.to_string())?;
        }
        Ok(((), true))
    }).unwrap();
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    let key = "x636f646578.x6f70656e61692f70726f5f323078.x7072696d617279";
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
async fn bus_publications_multiplier_startup_restores_current_pools() {
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
    let key = "x636f646578.x6f70656e61692f70726f5f323078.x7072696d617279";
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
async fn bus_publications_multiplier_startup_clears_legacy_keys_but_a_failed_read_does_not() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    let legacy_key = "x636f646578.x6f70656e61692f70726f5f323078";
    bus.multiplier.put(legacy_key, b"{}".to_vec().into()).await.unwrap();
    let before = bus.attempts();
    let stop = Arc::new(AtomicBool::new(false));
    let reader = crate::quota_events::Reader::start(state.clone(), None, stop.clone()).unwrap();
    bus.after(before).await;
    assert!(bus.multiplier.get(legacy_key).await.unwrap().is_none());
    stop.store(true, Ordering::Relaxed);
    reader.join();

    let key = "x636f646578.x6f70656e61692f70726f5f323078.x7072696d617279";
    let revision = bus.multiplier.put(key, b"{}".to_vec().into()).await.unwrap();
    state.with_conn(|conn| {
        conn.execute("DROP TABLE multiplier_provider_day", []).unwrap();
        Ok(())
    }).unwrap();
    assert!(crate::ops::dispatch(&state, crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms":0,"to_ms":1})).is_err());
    let before = bus.attempts();
    crate::bus_publications::publish_multiplier(&state);
    state.bus_publications().steps("wake after report read".into());
    bus.after(before).await;
    assert_eq!(bus.multiplier.entry(key).await.unwrap().unwrap().revision, revision);
    bus.finish().await;
}

#[tokio::test]
async fn bus_publications_multiplier_failed_provider_keeps_its_keys_while_another_advances() {
    use futures_util::TryStreamExt;
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    state.ordered_write(|tx| {
        tx.execute("INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider)
                    SELECT 'other',account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider
                    FROM window_sample WHERE provider='codex'", []).unwrap();
        Ok(((), true))
    }).unwrap();
    let Some(bus) = crate::bus_publications::tests::TestBus::start(&state).await else {
        return;
    };
    let codex = "x636f646578.x6f70656e61692f70726f5f323078.x7072696d617279";
    let other = "x6f74686572.x6f70656e61692f70726f5f323078.x7072696d617279";
    let mut stage = MultiplierStage::new(Some(fusiform));
    let before = bus.attempts();
    stage.tick(&state, false, NOW);
    bus.after(before).await;
    let old_codex = bus.multiplier.entry(codex).await.unwrap().unwrap();
    let old_other = bus.multiplier.entry(other).await.unwrap().unwrap();

    state.with_conn(|conn| {
        conn.execute_batch("CREATE TRIGGER fail_other_day BEFORE INSERT ON multiplier_provider_day
                            WHEN NEW.quota_provider='other' BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").unwrap();
        conn.execute("INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider)
                      VALUES ('codex','acct','primary',?1,50,10080,'2024-10-10T00:00:00Z',NULL,'openai')",
                     [(DAY_START + DAY_MS) * 1_000_000]).unwrap();
        Ok(())
    }).unwrap();
    let before = bus.attempts();
    stage.tick(&state, false, NOW + DAY_MS);
    bus.after(before).await;
    let next_codex = bus.multiplier.entry(codex).await.unwrap().unwrap();
    assert!(next_codex.revision > old_codex.revision);
    let next_row: serde_json::Value = serde_json::from_slice(&next_codex.value).unwrap();
    assert_eq!(next_row["interval_end_ms"], DAY_START + DAY_MS);
    assert_eq!(next_row["eligibility_reasons"], json!(["scope_unstated"]));
    assert_eq!(bus.multiplier.entry(other).await.unwrap().unwrap().revision, old_other.revision);
    assert!(!recorded_provider_days(&state).iter().any(|(provider, day, _)| {
        provider == "other" && *day == multiplier::utc_day(DAY_START + DAY_MS).unwrap()
    }));

    state.with_conn(|conn| {
        conn.execute_batch("CREATE TRIGGER fail_codex_day BEFORE INSERT ON multiplier_provider_day
                            WHEN NEW.quota_provider='codex' BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").unwrap();
        Ok(())
    }).unwrap();
    let days = recorded_provider_days(&state);
    let before = bus.attempts();
    stage.tick(&state, false, NOW + 2 * DAY_MS);
    state.bus_publications().steps("wake after failed provider days".into());
    bus.after(before).await;
    assert_eq!(recorded_provider_days(&state), days);
    assert_eq!(bus.multiplier.entry(codex).await.unwrap().unwrap().revision, next_codex.revision);
    assert_eq!(bus.multiplier.entry(other).await.unwrap().unwrap().revision, old_other.revision);
    state.with_conn(|conn| {
        conn.execute_batch("DROP TRIGGER fail_codex_day").unwrap();
        Ok(())
    }).unwrap();

    // Far enough past the readings, codex records empty days; the other provider
    // still cannot commit and retains its previous key.
    let before = bus.attempts();
    stage.tick(&state, false, NOW + 9 * DAY_MS);
    bus.after(before).await;
    let keys: Vec<String> = bus.multiplier.keys().await.unwrap().try_collect().await.unwrap();
    assert_eq!(keys, [other]);
    assert_eq!(bus.multiplier.entry(other).await.unwrap().unwrap().revision, old_other.revision);
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
