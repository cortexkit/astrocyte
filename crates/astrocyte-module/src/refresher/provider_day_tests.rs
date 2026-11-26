fn recorded_provider_days(state: &SharedState) -> Vec<(String, String, i64)> {
    state.with_conn(|conn| {
        let mut statement = conn.prepare("SELECT quota_provider, observation_day, recorded_at_ms FROM multiplier_provider_day ORDER BY 1,2").unwrap();
        Ok(statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap()
            .collect::<rusqlite::Result<_>>().unwrap())
    }).unwrap()
}

fn add_pool(state: &SharedState, key: &str, scope: Option<&str>, minutes: Option<i64>) {
    state.ordered_write(|tx| {
        tx.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider)
                    SELECT provider,account_ref,?1,fetched_at_instant,used_percent,?2,resets_at,?3,api_provider
                    FROM window_sample WHERE pool_key='primary'", params![key,minutes,scope]).unwrap();
        Ok(((), true))
    }).unwrap();
}

fn add_late_input(state: &SharedState) {
    state.ordered_write(|tx| {
        tx.execute("INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref)
                    VALUES ('later-input','another-reader','later-input',?1,?2,'openai','m','subscription_included','acct')", params![DAY_START - DAY_MS, NOW]).unwrap();
        tx.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                    SELECT event_key,'input','token','billed','usage.input',3000,0 FROM usage_event WHERE event_id='later-input'", []).unwrap();
        Ok(((), true))
    }).unwrap();
}

fn day_heads(state: &SharedState) -> Vec<serde_json::Value> {
    let report = crate::ops::dispatch(
        state,
        crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms":DAY_START-1,"to_ms":DAY_START+1}),
    )
    .unwrap();
    let mut heads = report["series"].as_array().unwrap().clone();
    heads.sort_by_key(|row| row["observation_id"].as_i64().unwrap());
    heads
}

#[test]
fn provider_day_snapshot_keeps_late_usage_out_of_both_pools_until_recompute() {
    use std::sync::{mpsc, Mutex};

    struct DelayedPlans {
        inner: Arc<FakeFusiform>,
        gate: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    }
    impl PlanPriceSource for DelayedPlans {
        fn plan_prices(
            &self,
            request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            let gate = if request.at_ms == Some(DAY_START) {
                self.gate.lock().unwrap().take()
            } else {
                None
            };
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            self.inner.plan_prices(request)
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    seed_usage(&state);
    add_pool(&state, "extra:5:Fable", Some("subset"), Some(10080));
    let fusiform = FakeFusiform::new(false);
    fusiform.future_plan_price.store(true, Ordering::SeqCst);
    fusiform.quota_pools.lock().unwrap().push(
        serde_json::from_value(json!({
            "provider_id":"openai", "pool_id":"Fable", "models":["openai/m"],
            "source_ref":"https://example.invalid/pools", "quote":"test coverage",
            "established_at_ms":0, "review_by_ms":i64::MAX
        }))
        .unwrap(),
    );
    install_catalog(&state, &fusiform);
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    let peer = Arc::new(DelayedPlans {
        inner: fusiform.clone(),
        gate: Mutex::new(Some((entered, resume))),
    });
    let worker = {
        let state = state.clone();
        std::thread::spawn(move || MultiplierStage::new(Some(peer)).tick(&state, false, NOW))
    };
    waiting.recv_timeout(Duration::from_secs(10)).unwrap();
    add_late_input(&state);
    assert!(
        day_heads(&state).is_empty(),
        "the reader is free before the peer returns, but the day is not committed"
    );
    release.send(()).unwrap();
    worker.join().unwrap();

    let check = |tokens, api_nanos| {
        let rows = day_heads(&state);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row["pool_scope"] == "all"));
        assert!(rows.iter().any(|row| row["pool_scope"] == "subset"));
        for row in &rows {
            assert_eq!(row["input_tokens"], tokens, "{}", row["window_pool_key"]);
            assert_eq!(row["api_nanos"], api_nanos);
            assert_eq!(row["plan_prices"][0]["monthly_nanos"], 200_000_000_000_i64);
            if row["pool_scope"] == "subset" {
                assert_eq!(row["pool_models_at_ms"], DAY_START);
            }
        }
        rows
    };
    let before = check(1000, 10_000_000);
    let outcome = recompute(&state, &fusiform, DAY, Some("codex"), NOW + DAY_MS).unwrap();
    assert_eq!(outcome["recomputed"].as_array().unwrap().len(), 2);
    let after = check(4000, 40_000_000);
    for row in &after {
        let parent = before
            .iter()
            .find(|old| old["window_pool_key"] == row["window_pool_key"])
            .unwrap();
        assert_eq!(row["supersedes"], parent["observation_id"]);
    }
    let instants = fusiform.plan_instants.lock().unwrap();
    assert_eq!(
        &instants[instants.len() - 2..],
        &[Some(DAY_START), Some(DAY_START)]
    );
    assert!(recorded_provider_days(&state).contains(&("codex".into(), DAY.into(), NOW)));
}

#[test]
fn provider_day_empty_set_is_recorded_once_and_then_only_recompute_can_fill_it() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    seed_usage_with_window(&state, true, None);
    MultiplierStage::new(None).tick(&state, false, NOW);
    assert!(all_rows(&state).is_empty());
    let before = recorded_provider_days(&state);
    assert_eq!(before.len(), 7);
    assert_eq!(before.last().unwrap(), &("codex".into(), DAY.into(), NOW));

    add_pool(&state, "secondary", Some("all"), Some(10080));
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + 1);
    assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0);
    assert!(all_rows(&state).is_empty());
    let outcome = recompute(&state, &fusiform, DAY, Some("codex"), NOW + 2).unwrap();
    assert!(outcome["recomputed"][0]["supersedes"].is_null());
    assert_eq!(day_heads(&state)[0]["window_pool_key"], "secondary");
    assert_eq!(
        recorded_provider_days(&state),
        before,
        "the watermark is append-only"
    );

    let later = NOW + 8 * DAY_MS;
    MultiplierStage::new(None).tick_days(&state, false, later, &AtomicBool::new(false));
    let end = later - later.rem_euclid(DAY_MS);
    assert!(recorded_provider_days(&state).contains(&(
        "codex".into(),
        multiplier::utc_day(end).unwrap(),
        later
    )));
    assert_eq!(
        all_rows(&state).len(),
        1,
        "a provider carries forward after its readings disappear"
    );
}

#[test]
fn provider_day_recompute_retires_a_shorter_head_when_late_readings_supply_the_longer_pool() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    seed_usage_with_window(&state, true, Some(300));
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    let original = recompute(&state, &fusiform, DAY, Some("codex"), NOW).unwrap();
    let old = original["recomputed"][0]["observation_id"]
        .as_i64()
        .unwrap();
    add_pool(&state, "secondary", Some("all"), Some(10080));
    let later = NOW + 1;
    let new = recompute(&state, &fusiform, DAY, Some("codex"), later).unwrap();
    let rows = new["recomputed"].as_array().unwrap();
    assert_eq!(
        rows.len(),
        2,
        "a new longer pool and a retirement must be written"
    );
    assert!(rows[0]["supersedes"].is_null());
    assert_eq!(rows[1]["supersedes"], old);
    assert_retirement(&state, old, later);
    let heads = day_heads(&state);
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0]["pool_id"], "secondary");
    assert_eq!(heads[0]["input_tokens"], 1000);
    let report = crate::ops::dispatch(
        &state, crate::ops::OP_MULTIPLIER_REPORT, json!({"from_ms":0,"to_ms":1})
    ).unwrap();
    assert_eq!(report["latest"].as_array().unwrap().len(), 1);
    assert_eq!(report["latest"][0]["pool_id"], "secondary");
}

fn assert_retirement(state: &SharedState, old: i64, now: i64) {
    state.with_conn(|conn| {
        let (reasons, snapshot, same_facts, placeholders): (String, i64, bool, bool) = conn.query_row(
            "SELECT r.eligibility_reasons,r.snapshot_at_ms,
                    r.quota_provider=p.quota_provider AND r.observation_day=p.observation_day
                      AND r.plan=p.plan AND r.window_pool_key=p.window_pool_key AND r.window_minutes=p.window_minutes
                      AND r.interval_start_ms=p.interval_start_ms AND r.interval_end_ms=p.interval_end_ms
                      AND r.token_interval_start_ms IS p.token_interval_start_ms AND r.token_interval_end_ms IS p.token_interval_end_ms
                      AND r.plan_prices=p.plan_prices AND r.catalog_resolved_at_ms=p.catalog_resolved_at_ms
                       AND r.pool_scope=p.pool_scope AND r.pool_models IS p.pool_models AND r.catalog_provider IS p.catalog_provider
                      AND r.pool_models_at_ms IS p.pool_models_at_ms AND r.pool_member_models IS p.pool_member_models,
                    r.input_tokens=0 AND r.cache_read_tokens=0 AND r.cache_write_tokens=0 AND r.output_tokens=0 AND r.reasoning_tokens=0
                      AND r.api_nanos=0 AND r.tokens_outside_quota_coverage=0 AND r.base_rate_tier_untested_tokens=0
                      AND r.zero_measurement_events=0 AND r.missing_quantity_events=0 AND r.delta_percent=0
                      AND r.subscription_nanos=0 AND r.fee_paid_nanos=0 AND r.accounts_included_count=0
                      AND r.clean_hours_share=0 AND r.saturated_share=0 AND r.gap_share=0 AND r.unpriced_share=0 AND r.unattributed_event_share=0
                      AND r.api_nanos_by_model='[]' AND r.unpriced_tokens='[]' AND r.accounts_included='[]'
                      AND r.accounts_excluded='[]' AND r.excluded_pairs='[]'
                      AND r.routing_eligible=0 AND r.multiplier IS NULL AND r.fee_multiplier IS NULL
               FROM multiplier_observation p JOIN multiplier_observation r ON r.supersedes=p.observation_id
              WHERE p.observation_id=?1", [old], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap();
        assert_eq!(reasons, r#"["pool_retired"]"#);
        assert_eq!(snapshot, now);
        assert!(same_facts, "retirement must carry the parent's dated facts and pool columns");
        assert!(placeholders, "retirement must clear every measured field");
        Ok(())
    }).unwrap();
}

#[test]
fn provider_day_vault_correction_writes_the_new_plan_and_retires_the_old_without_its_price() {
    struct CurrentPlanOnly(Arc<FakeFusiform>);
    impl PlanPriceSource for CurrentPlanOnly {
        fn plan_prices(
            &self,
            request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            let mut response = self.0.plan_prices(request)?;
            response.prices.retain(|row| row.tier == "pro_100");
            Ok(response)
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    seed_usage(&state);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    let original = recompute(&state, &fusiform, DAY, Some("codex"), NOW).unwrap();
    let old = original["recomputed"][0]["observation_id"]
        .as_i64()
        .unwrap();
    state
        .ordered_write(|tx| {
            astrocyte_core::vault_plan::append_in_tx(
                tx,
                &astrocyte_core::vault_plan::VaultPlan {
                    credential_id: "fixture:acct".into(),
                    provider: Some("openai".into()),
                    account_id: Some("acct".into()),
                    plan_tier: Some("pro_100".into()),
                    plan_key: Some("openai/pro_100".into()),
                    source: "operator".into(),
                    effective_from_ms: 0,
                    observed_at_ms: NOW + 1,
                },
            )
            .unwrap();
            Ok(((), true))
        })
        .unwrap();
    let later = NOW + 2;
    let reply = recompute_multiplier_day(
        &state,
        Some(&CurrentPlanOnly(fusiform)),
        &multiplier::MultiplierRecomputeRequest {
            quota_provider: Some("codex".into()),
            observation_day: DAY.into(),
        },
        later,
    )
    .unwrap();
    assert_eq!(reply["recomputed"].as_array().unwrap().len(), 2);
    assert_eq!(reply["recomputed"][0]["plan"], "openai/pro_100");
    assert!(reply["recomputed"][0]["supersedes"].is_null());
    assert_retirement(&state, old, later);
    let current = day_heads(&state)
        .into_iter()
        .find(|row| row["plan"] == "openai/pro_100")
        .unwrap();
    assert_eq!(current["api_nanos"], 10_000_000);
    assert_eq!(
        current["plan_prices"][0]["monthly_nanos"],
        100_000_000_000_i64
    );
}

#[test]
fn provider_day_mixed_scope_supersedes_only_its_previous_head() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    let original = recompute(&state, &fusiform, DAY, None, NOW).unwrap();
    state.ordered_write(|tx| {
        tx.execute("INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider)
                    VALUES ('codex','acct','primary',?1,8,10080,'2024-10-10T00:00:00Z','subset','openai')", [(DAY_START - DAY_MS + 60_000) * 1_000_000]).unwrap();
        Ok(((), true))
    }).unwrap();
    let reply = recompute(&state, &fusiform, DAY, None, NOW + DAY_MS).unwrap();
    let report = crate::ops::dispatch(
        &state,
        crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms":DAY_START-1,"to_ms":DAY_START+1}),
    )
    .unwrap();
    let heads = day_heads(&state);
    assert_eq!(heads.len(), 1);
    let head = &heads[0];
    assert_eq!(head["eligibility_reasons"], json!(["scope_changed"]));
    assert_eq!(
        head["supersedes"],
        original["recomputed"][0]["observation_id"]
    );
    assert_eq!(head["api_nanos"], 0);
    assert_eq!(head["pool_scope"], "unknown");
    assert_eq!(
        head["observation_id"],
        reply["recomputed"][0]["observation_id"]
    );
    assert_eq!(
        head["observation_id"],
        report["latest"][0]["observation_id"]
    );
    assert_eq!(
        *fusiform.plan_instants.lock().unwrap(),
        [Some(DAY_START), Some(DAY_START)]
    );
}

#[test]
fn provider_day_recompute_keeps_the_pre_cut_chain_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    let day = ProviderDay {
        provider: "codex",
        day: DAY,
        interval_start_ms: DAY_START - MULTIPLIER_INTERVAL_MS,
        interval_end_ms: DAY_START,
    };
    let mut legacy = compute_multiplier_day(&state, Some(fusiform.as_ref()), day, NOW, false)
        .unwrap_or_else(|error| panic!("{}: {}", error.code, error.message))
        .remove(0);
    legacy.pool_scope = None;
    legacy.catalog_provider = None;
    state
        .ordered_write(|tx| {
            legacy.window_pool_key = "secondary".into();
            multiplier::insert_observation_in_tx(tx, &legacy).unwrap();
            legacy.window_pool_key = "primary".into();
            multiplier::insert_superseding_observation_in_tx(tx, &legacy).unwrap();
            legacy.quota_provider = "legacy-only".into();
            multiplier::insert_observation_in_tx(tx, &legacy).unwrap();
            Ok(((), true))
        })
        .unwrap();
    let legacy = all_rows(&state);
    let next = recompute(&state, &fusiform, DAY, Some("codex"), NOW + 1).unwrap();
    assert_eq!(next["recomputed"].as_array().unwrap().len(), 1);
    assert!(next["recomputed"][0]["supersedes"].is_null());
    let legacy_only = recompute_multiplier_day(
        &state,
        None,
        &multiplier::MultiplierRecomputeRequest {
            quota_provider: Some("legacy-only".into()),
            observation_day: DAY.into(),
        },
        NOW + 2,
    )
    .unwrap();
    assert_eq!(legacy_only["recomputed"], json!([]));
    assert_eq!(&all_rows(&state)[..3], legacy.as_slice());
    assert!(recorded_provider_days(&state).contains(&("legacy-only".into(), DAY.into(), NOW + 2)));
}

#[test]
fn provider_day_write_rolls_back_the_complete_set_when_a_later_pool_fails() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    add_pool(&state, "secondary", Some("subset"), Some(10080));
    state.ordered_write(|tx| {
        tx.execute_batch("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider)
            SELECT 'second',account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider FROM window_sample WHERE pool_key='primary';
            CREATE TRIGGER test_refuse_secondary BEFORE INSERT ON multiplier_observation
            WHEN NEW.quota_provider='codex' AND NEW.window_pool_key='secondary'
            BEGIN SELECT RAISE(ABORT, 'refuse second pool'); END;").unwrap();
        Ok(((), true))
    }).unwrap();
    let mut stage = MultiplierStage::new(Some(fusiform));
    stage.tick(&state, false, NOW);
    assert_eq!(
        current_day_observations(&state),
        [("second".into(), DAY.into(), 10_000_000)]
    );
    let days = recorded_provider_days(&state);
    assert_eq!(days.len(), 7);
    assert!(days.iter().all(|(provider, _, _)| provider == "second"));
    state
        .ordered_write(|tx| {
            tx.execute("DROP TRIGGER test_refuse_secondary", [])
                .unwrap();
            Ok(((), true))
        })
        .unwrap();
    stage.tick(&state, false, NOW + 1);
    assert_eq!(recorded_provider_days(&state).len(), 14);
    assert_eq!(current_day_observations(&state).len(), 3);
}
