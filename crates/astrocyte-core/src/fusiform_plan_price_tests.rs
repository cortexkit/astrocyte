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
    let mut config = config(vec![rule("pi", "kimi-code-plan-global", None)]);
    let mut provider = config.quota_providers.remove("codex").unwrap();
    provider.accounts.get_mut("acct").unwrap().plan = "kimi-code-plan-global/allegro".into();
    config
        .quota_providers
        .insert("kimi-code-plan-global".into(), provider);
    config.config = config.declarations();
    let start = 1_795_000_000_000;
    let end = start + DAY;
    // A reading every ten minutes keeps the whole day covered: the multiplier
    // drops quota used across a long gap between readings, even when the plan
    // fee is known, and this test needs the day measured.
    for step in 0_i64..=144 {
        let at_ms = start + step * 600_000;
        let used = if at_ms == end { 10 } else { 0 };
        conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant,
                 used_percent, resets_at, window_minutes, source)
             VALUES ('kimi-code-plan-global', 'acct', 'primary', ?1, ?2,
                 '2026-12-01T00:00:00Z', 10080, 'vault')",
            params![at_ms * NANOS_PER_MS, used],
        )
        .unwrap();
    }
    let plan = plan_observation(&conn, &config, "kimi-code-plan-global", start, end).unwrap();
    let prices = super::resolve_plans(Some(&CapturedPlanPrices), &config, &plan.provider, end)
        .expect("all curated plan prices decode even when a service tier has no credit rate");
    let used = &prices["acct"];
    assert_eq!(used.source.as_deref(), Some("fusiform"));
    assert_eq!(used.monthly_nanos, Some(99_000_000_000));
    assert_eq!(used.unpriceable_reason, None);
    assert!(!used.plan_price_overdue);

    let observation = super::compute_observation(
        &conn,
        &config,
        &plan,
        &ResolvedRates::default(),
        &prices,
        end,
        &[],
    )
    .unwrap();
    // The monthly $99 fee still funds the weekly quota and one day's paid
    // share. An unstated purchased-credit rate is neither zero nor a fee.
    assert_eq!(observation.subscription_nanos, 2_310_000_000);
    assert_eq!(observation.fee_paid_nanos, 3_300_000_000);
    assert_eq!(observation.accounts_included.len(), 1);
    assert!(observation.accounts_excluded.is_empty());
    assert_eq!(observation.plan_prices, vec![used.clone()]);
}
