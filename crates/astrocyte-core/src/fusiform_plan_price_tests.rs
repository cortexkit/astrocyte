#[test]
fn plan_prices_decodes_pinned_synthetic_quota_pool() {
    use sha2::{Digest, Sha256};

    const PAYLOAD: &[u8] =
        include_bytes!("../test-vectors/fusiform-28bf597d/plan-prices-synthetic-quota-pool.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(PAYLOAD)),
        "869eaeb7b9f22a16a18c1b6b5b82973df9ae3103a2afbb14a1773e1c8bfc09a2"
    );
    let response: PlanPricesResponse = serde_json::from_slice(PAYLOAD).unwrap();
    assert_eq!(response.resolved_at_ms, -1);
    let [pool] = response.quota_pools.as_slice() else {
        panic!("expected one quota pool, got {:?}", response.quota_pools);
    };
    assert_eq!(pool.provider_id, "google");
    assert_eq!(pool.pool_id, "Weekly");
    assert_eq!(pool.models, ["google/antigravity-gemini-4-hydrogen"]);
    assert_eq!(pool.established_at_ms, 1000);
}

#[test]
fn fusiform_071_curated_plan_prices_keep_allegro_fee_without_credit_rate() {
    use sha2::{Digest, Sha256};

    const PAYLOAD: &[u8] =
        include_bytes!("../test-vectors/fusiform-protocol-0.71.0/plan-prices.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(PAYLOAD)),
        "01279f714bdb3c5cd934719c6bfc31fd40aeece0326923da43b945f524e32dbc"
    );

    struct CapturedPlanPrices;
    impl PlanPriceSource for CapturedPlanPrices {
        fn plan_prices(
            &self,
            request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            assert_eq!(request.provider_id, None);
            // Decode the entire reply as the transport does, so a service tier
            // without a purchased-credit rate cannot erase every plan fee.
            serde_json::from_slice(PAYLOAD).map_err(|error| CatalogCallError {
                code: "undecodable".into(),
                message: format!("fusiform plan.prices result: {error}"),
            })
        }
    }

    let raw: Value = serde_json::from_slice(PAYLOAD).unwrap();
    assert_eq!(raw["prices"].as_array().unwrap().len(), 13);
    let allegro = raw["prices"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["provider_id"] == "kimi-code-plan-global" && row["tier"] == "allegro")
        .unwrap();
    let fast = &allegro["service_tiers"]["tiers"][0];
    assert_eq!(fast["tier"], "fast");
    assert_eq!(
        fast["included_usage_rate"],
        json!({"units": 2, "exponent": 0})
    );
    assert!(fast.get("credit_rate").is_none());

    let conn = conn();
    fixture_vault_plan(&conn, "kimi-code-plan-global/allegro");
    fixture_pool_schema_at_genesis(&conn);
    let start = 1_795_000_000_000_i64.div_euclid(HOUR_MS) * HOUR_MS;
    let end = start + DAY;
    // A reading every ten minutes keeps the whole day covered: the multiplier
    // drops quota used across a long gap between readings, even when the plan
    // fee is known, and this test needs the day measured.
    for step in 0_i64..=144 {
        let at_ms = start + step * 600_000;
        let used = if at_ms == end { 10 } else { 0 };
        conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant,
                 used_percent, resets_at, window_minutes, source, model_scope)
             VALUES ('kimi-code-plan-global', 'acct', 'primary', ?1, ?2,
                 '2026-12-01T00:00:00Z', 10080, 'vault', 'all')",
            params![at_ms * NANOS_PER_MS, used],
        )
        .unwrap();
    }
    let tx = conn.unchecked_transaction().unwrap();
    let day = pools::capture_pool_day(&tx, "kimi-code-plan-global", start, end).unwrap();
    tx.commit().unwrap();
    let prices = CapturedPlanPrices
        .plan_prices(PlanPricesRequest {
            provider_id: None,
            at_ms: Some(end),
        })
        .expect("all curated plan prices decode even when a service tier has no credit rate");
    let planned = pools::plan_pools(&day.readings, &prices.quota_pools);
    let observation = pools::compute_pool_observations(
        &day,
        &planned,
        &ResolvedRates::default(),
        &Default::default(),
        &prices,
        end,
    )
    .unwrap()
    .remove(0);
    let [used] = observation.plan_prices.as_slice() else {
        panic!("expected one plan fee")
    };
    assert_eq!(used.source.as_deref(), Some("fusiform"));
    assert_eq!(used.monthly_nanos, Some(99_000_000_000));
    assert_eq!(used.unpriceable_reason, None);
    assert!(!used.plan_price_overdue);

    // The monthly $99 fee still funds the weekly quota and one day's paid
    // share. An unstated purchased-credit rate is neither zero nor a fee.
    assert_eq!(observation.subscription_nanos, 2_310_000_000);
    assert_eq!(observation.fee_paid_nanos, 3_300_000_000);
    assert_eq!(observation.accounts_included.len(), 1);
    assert!(observation.accounts_excluded.is_empty());
}
