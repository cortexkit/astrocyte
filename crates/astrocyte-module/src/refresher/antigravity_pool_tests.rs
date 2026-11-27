const ANTIGRAVITY_LOGINS: [(&str, &str); 2] = [
    ("one@example.invalid", "antigravity:google:one"),
    ("two@example.invalid", "antigravity:google"),
];
const GEMINI: &str = "antigravity-gemini-4-hydrogen";
const SONNET: &str = "antigravity-claude-sonnet-5-5";
const WEEKLY_POOLS: [(&str, i64); 2] = [("gemini-weekly", 10080), ("3p-weekly", 10080)];

struct GooglePlans(Arc<FakeFusiform>);

impl PlanPriceSource for GooglePlans {
    fn plan_prices(
        &self,
        request: PlanPricesRequest,
    ) -> Result<PlanPricesResponse, CatalogCallError> {
        let mut response = self.0.plan_prices(request)?;
        response.prices.truncate(1);
        response.prices[0].provider_id = "google".into();
        response.prices[0].tier = "ultra_5x".into();
        Ok(response)
    }
}

fn google_pool_list(pool_id: &str, model: &str) -> QuotaPoolWire {
    serde_json::from_value(json!({
        "provider_id":"google", "pool_id":pool_id, "models":[format!("google/{model}")],
        "source_ref":"https://example.invalid/pools", "quote":"test coverage",
        "established_at_ms":0, "review_by_ms":i64::MAX
    }))
    .unwrap()
}

fn seed_antigravity_accounts(state: &SharedState) {
    state.ordered_write(|tx| {
        // There is no active log reader in this fixture.
        tx.execute("DELETE FROM source_runtime", []).unwrap();
        tx.execute(
            "UPDATE capacity_runtime SET runtime_variant='configured_unavailable',
             changed_at_ms=NULL,enabled_at_ms=0,detail='no usable poll completed' WHERE id=1",
            [],
        ).unwrap();
        for (account, credential) in ANTIGRAVITY_LOGINS {
            astrocyte_core::vault_plan::append_in_tx(tx, &astrocyte_core::vault_plan::VaultPlan {
                credential_id: credential.into(),
                provider: Some("google".into()),
                account_id: Some(account.into()),
                plan_tier: Some("ultra_5x".into()),
                plan_key: Some("google/ultra_5x".into()),
                source: "operator".into(),
                effective_from_ms: 0,
                observed_at_ms: 0,
            }).unwrap();
            for model in [GEMINI, SONNET] {
                let event = format!("{account}:{model}");
                let key: i64 = tx.query_row(
                    "INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,
                                             charge_basis,account_ref,resolved_credential,auth_selection)
                     VALUES (?1,'opencode',?1,?2,?2,'google',?3,'subscription_included',?4,?5,'another-selection')
                     RETURNING event_key",
                    params![event, DAY_START - DAY_MS, model, account, credential],
                    |row| row.get(0),
                ).unwrap();
                tx.execute(
                    "INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                     VALUES (?1,'input','token','billed','usage.input',1000,0)",
                    [key],
                ).unwrap();
            }
        }
        Ok(((), true))
    }).unwrap();
}

fn antigravity_snapshot(state: &SharedState, at_ms: i64, used: i64, pools: &[(&str, i64)]) {
    let fetched = chrono::DateTime::from_timestamp_millis(at_ms)
        .unwrap()
        .to_rfc3339();
    let windows: Vec<_> = pools
        .iter()
        .map(|(id, minutes)| {
            json!({
                "id":id, "window":{
                    "usedPercent":used, "modelScope":"subset", "windowMinutes":minutes,
                    "resetsAt":"2024-10-10T00:00:00Z"
                }
            })
        })
        .collect();
    let entries = ANTIGRAVITY_LOGINS.map(|(account, credential)| {
        serde_json::from_value(json!({
            "provider":"antigravity", "apiProvider":"google", "source":"oauth",
            "account":account, "credentialRef":credential, "fetchedAt":fetched,
            "usage":{"extraRateWindows":windows}
        }))
        .unwrap()
    });
    let snapshot = crate::capacity_map::map_usage_array(&entries, None).unwrap();
    state
        .ordered_write(|tx| {
            astrocyte_core::capacity::apply_snapshot_in_tx(tx, &snapshot, at_ms).unwrap();
            Ok(((), true))
        })
        .unwrap();
}

#[test]
fn antigravity_weekly_pools_value_exact_list_members_with_cached_rates() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    seed_antigravity_accounts(&state);
    let start = DAY_START - MULTIPLIER_INTERVAL_MS;
    for (at, used) in [(start, 10), (start + 300_000, 11), (DAY_START, 11)] {
        antigravity_snapshot(&state, at, used, &WEEKLY_POOLS);
    }
    let fusiform = FakeFusiform::new(false);
    *fusiform.quota_pools.lock().unwrap() = vec![
        google_pool_list("gemini-weekly", GEMINI),
        google_pool_list("3p-weekly", SONNET),
    ];
    install_catalog(&state, &fusiform);
    fusiform
        .dated_snapshot_uncovered
        .store(true, Ordering::SeqCst);
    let plans = Arc::new(GooglePlans(fusiform));
    MultiplierStage::new(Some(plans.clone())).tick(&state, false, NOW);

    let heads = day_heads(&state);
    assert_eq!(heads.len(), 2);
    for (id, model) in [("gemini-weekly", GEMINI), ("3p-weekly", SONNET)] {
        let row = heads.iter().find(|row| row["pool_id"] == id).unwrap();
        assert_eq!(row["quota_provider"], "antigravity");
        assert_eq!(row["catalog_provider"], "google");
        assert_eq!(row["plan"], "google/ultra_5x");
        assert_eq!(row["pool_scope"], "subset");
        assert_eq!(row["pool_models"], json!([format!("google/{model}")]));
        let accounts: Vec<_> = row["accounts_included"]
            .as_array()
            .unwrap()
            .iter()
            .map(|account| account["account_ref"].as_str().unwrap())
            .collect();
        assert_eq!(accounts, ["one@example.invalid", "two@example.invalid"]);
        assert_eq!(row["clean_hours_share"], json!(1.0));
        assert_eq!(
            row["api_nanos"], 20_000_000,
            "{id} must use its cached rates"
        );
        assert_eq!(
            row["input_tokens"], 2000,
            "{id} must count only its own two calls"
        );
        assert_eq!(row["api_nanos_by_model"].as_array().unwrap().len(), 1);
        assert_eq!(row["pool_member_models"], json!([]));
    }

    // Recompute has no dated rate for these models. Membership still reports
    // the unpriced usage; the served coverage list does not invent a rate.
    recompute_multiplier_day(
        &state,
        Some(plans.as_ref()),
        &multiplier::MultiplierRecomputeRequest {
            quota_provider: Some("antigravity".into()),
            observation_day: DAY.into(),
        },
        NOW + 1,
    )
    .unwrap();
    let heads = day_heads(&state);
    for (id, model) in [("gemini-weekly", GEMINI), ("3p-weekly", SONNET)] {
        let row = heads.iter().find(|row| row["pool_id"] == id).unwrap();
        assert_eq!(row["api_nanos"], 0);
        assert_eq!(
            row["unpriced_tokens"],
            json!([{
                "reason":"no_catalog_coverage", "model":format!("opencode/google/{model}"),
                "role":"input", "tokens":2000, "measurements":2
            }])
        );
    }
}

#[test]
fn antigravity_reorder_preserves_named_pools_and_equal_coverage_retires_shorter_heads() {
    let directory = tempfile::tempdir().unwrap();
    let state = state::leased_test_state(&directory);
    seed_antigravity_accounts(&state);
    let mut windows = [
        WEEKLY_POOLS[0],
        ("gemini-5h", 300),
        WEEKLY_POOLS[1],
        ("3p-5h", 300),
    ];
    let start = DAY_START - MULTIPLIER_INTERVAL_MS;
    for (at, used) in [(start, 10), (start + 300_000, 11), (DAY_START - 60_000, 11)] {
        antigravity_snapshot(&state, at, used, &windows);
    }
    let fusiform = FakeFusiform::new(false);
    *fusiform.quota_pools.lock().unwrap() = vec![
        google_pool_list("gemini-weekly", GEMINI),
        google_pool_list("3p-weekly", SONNET),
        google_pool_list("gemini-5h", "gemini-short-only"),
        google_pool_list("3p-5h", "3p-short-only"),
    ];
    install_catalog(&state, &fusiform);
    let plans = GooglePlans(fusiform.clone());
    let request = multiplier::MultiplierRecomputeRequest {
        quota_provider: Some("antigravity".into()),
        observation_day: DAY.into(),
    };
    let recompute = |now| recompute_multiplier_day(&state, Some(&plans), &request, now).unwrap();
    recompute(NOW);
    let before = day_heads(&state);
    assert_eq!(before.len(), 4);

    windows.reverse();
    antigravity_snapshot(&state, DAY_START, 11, &windows);
    assert_eq!(
        recompute(NOW + 1)["recomputed"].as_array().unwrap().len(),
        4
    );
    let after_reorder = day_heads(&state);
    assert_eq!(after_reorder.len(), 4);
    for (id, key) in [
        ("gemini-weekly", "extra:13:gemini-weekly"),
        ("3p-weekly", "extra:9:3p-weekly"),
        ("gemini-5h", "extra:9:gemini-5h"),
        ("3p-5h", "extra:5:3p-5h"),
    ] {
        let previous = before.iter().find(|row| row["pool_id"] == id).unwrap();
        let current = after_reorder
            .iter()
            .find(|row| row["pool_id"] == id)
            .unwrap();
        assert_eq!(current["window_pool_key"], key);
        assert_eq!(current["supersedes"], previous["observation_id"]);
    }

    // Only a coverage change makes the 5h windows redundant; their named
    // keys and readings are unchanged since the preceding recompute.
    let mut lists = fusiform.quota_pools.lock().unwrap();
    lists[2] = google_pool_list("gemini-5h", GEMINI);
    lists[3] = google_pool_list("3p-5h", SONNET);
    drop(lists);
    assert_eq!(
        recompute(NOW + 2)["recomputed"].as_array().unwrap().len(),
        4
    );
    let current = day_heads(&state);
    assert_eq!(current.len(), 2);
    for (weekly, short) in [("gemini-weekly", "gemini-5h"), ("3p-weekly", "3p-5h")] {
        assert!(current.iter().any(|row| row["pool_id"] == weekly));
        let retired = after_reorder
            .iter()
            .find(|row| row["pool_id"] == short)
            .unwrap();
        assert_eq!(retired["pool_scope"], "subset");
        assert_eq!(retired["catalog_provider"], "google");
        assert_retirement(&state, retired["observation_id"].as_i64().unwrap(), NOW + 2);
    }
}
