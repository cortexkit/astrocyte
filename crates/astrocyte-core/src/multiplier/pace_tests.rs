fn pace_call(conn: &Connection, id: &str, producer: &str, model: &str, role: &str) {
    account_event(
        conn,
        id,
        producer,
        "openai",
        model,
        "subscription_included",
        100,
        &[(role, "usage", 10)],
    );
}

fn pace_request(to_ms: i64, plan: Option<&str>, pool: Option<&str>) -> MultiplierPaceRequest {
    MultiplierPaceRequest {
        quota_provider: "codex".into(),
        plan: plan.map(str::to_owned),
        pool: pool.map(str::to_owned),
        from_ms: 0,
        to_ms,
    }
}

#[test]
fn pace_pool_choice_uses_frozen_coverage_in_either_insertion_order() {
    for reverse in [false, true] {
        let mut conn = conn();
        let all = pace_head(&conn);
        let mut subset = all.clone();
        subset.window_pool_key = "secondary".into();
        subset.pool_scope = Some(PoolScope::Subset);
        subset.pool_models = Some(vec!["openai/large".into()]);
        subset.pool_models_at_ms = Some(subset.interval_end_ms);
        subset.pool_member_models = Some(vec!["openai/large-low".into()]);
        if reverse {
            insert_pace_observation(&mut conn, &subset);
            insert_pace_observation(&mut conn, &all);
        } else {
            insert_pace_observation(&mut conn, &all);
            insert_pace_observation(&mut conn, &subset);
        }
        for (id, model) in [
            ("listed", "large"),
            ("recorded-fallback", "large-low"),
            ("new-suffix", "large-high"),
            ("new-alias", "large-alias"),
        ] {
            pace_call(&conn, id, "opencode", model, "input");
        }
        pace_call(&conn, "outside", "broca", "small", "input");
        // Plan attribution and charge basis are still required for an all pool.
        account_event(
            &conn,
            "metered",
            "opencode",
            "openai",
            "large",
            "metered_api",
            100,
            &[("input", "usage", 10)],
        );
        event(
            &conn,
            "accountless",
            "opencode",
            "openai",
            "large",
            "subscription_included",
            100,
            &[("input", "usage", 10)],
        );

        assert_eq!(
            multiplier_pace(&conn, &pace_request(3 * HOUR_MS, None, None))
                .unwrap_err()
                .code(),
            "pool_required"
        );
        let all_reply =
            multiplier_pace(&conn, &pace_request(3 * HOUR_MS, None, Some("primary"))).unwrap();
        assert_eq!(all_reply["pool_id"], "primary");
        assert_eq!(all_reply["hours"][0]["producers"]["opencode"]["events"], 4);
        assert_eq!(all_reply["hours"][0]["producers"]["broca"]["events"], 1);
        assert_eq!(
            all_reply["hours"][0]["producers"]["opencode"]["list_nanos"],
            100_000
        );
        assert_eq!(
            all_reply["hours"][0]["producers"]["broca"]["list_nanos"],
            20_000
        );
        assert_eq!(
            all_reply["hours"][0]["producers"]["opencode"]["unpriced_tokens"],
            30
        );

        let subset_reply =
            multiplier_pace(&conn, &pace_request(3 * HOUR_MS, None, Some("secondary"))).unwrap();
        assert_eq!(subset_reply["pool_id"], "secondary");
        assert!(subset_reply["hours"][0]["producers"].get("broca").is_none());
        assert_eq!(
            subset_reply["hours"][0]["producers"]["opencode"]["events"],
            2
        );
        assert_eq!(
            subset_reply["hours"][0]["producers"]["opencode"]["list_nanos"],
            100_000
        );
        assert_eq!(
            subset_reply["hours"][0]["producers"]["opencode"]["unpriced_tokens"],
            10
        );
    }
}

#[test]
fn pace_selects_latest_bounded_head_before_judging_ambiguity() {
    for reverse in [false, true] {
        let mut conn = conn();
        let mut latest = pace_head(&conn);
        let mut older = latest.clone();
        older.observation_day = "1969-12-31".into();
        older.interval_start_ms -= DAY;
        older.interval_end_ms -= DAY;
        older.token_interval_start_ms = older.token_interval_start_ms.map(|at| at - DAY);
        older.token_interval_end_ms = older.token_interval_end_ms.map(|at| at - DAY);
        older.api_nanos = 50_000_000;
        older.pool_scope = Some(PoolScope::Subset);
        older.pool_models = Some(vec!["openai/small".into()]);
        older.pool_models_at_ms = Some(older.interval_end_ms);
        latest.pool_scope = Some(PoolScope::Subset);
        latest.pool_models = Some(vec!["openai/large".into()]);
        latest.pool_models_at_ms = Some(latest.interval_end_ms);
        if reverse {
            insert_pace_observation(&mut conn, &latest);
            insert_pace_observation(&mut conn, &older);
        } else {
            insert_pace_observation(&mut conn, &older);
            insert_pace_observation(&mut conn, &latest);
        }
        // These heads are after to_ms. They must create neither plan_required
        // nor pool_required, even though they differ in both dimensions.
        for (plan, pool) in [
            ("openai/other", "secondary"),
            ("openai/pro_20x", "tertiary"),
        ] {
            let mut future = latest.clone();
            future.plan = Some(plan.into());
            future.window_pool_key = pool.into();
            future.interval_end_ms += 1;
            insert_pace_observation(&mut conn, &future);
        }
        pace_call(&conn, "new-list", "opencode", "large", "input");
        pace_call(&conn, "old-list", "opencode", "small", "input");

        let reply = multiplier_pace(&conn, &pace_request(3 * HOUR_MS, None, None)).unwrap();
        assert_eq!(reply["pool_id"], "primary");
        assert_eq!(reply["observation_day"], "1970-01-01");
        assert_eq!(reply["per_point_nanos"], 10_000_000);
        assert_eq!(reply["hours"][0]["producers"]["opencode"]["events"], 1);
        assert_eq!(
            reply["hours"][0]["producers"]["opencode"]["list_nanos"],
            100_000
        );
    }
}

#[test]
fn pace_distinguishes_unknown_provider_missing_candidates_and_plan_ambiguity() {
    let mut conn = conn();
    let legacy = sample_observation(&conn);
    insert_pace_observation(&mut conn, &legacy);
    assert_eq!(
        multiplier_pace(&conn, &pace_request(DAY, None, None))
            .unwrap_err()
            .code(),
        "unknown_quota_provider"
    );

    let mut ineligible = pace_ready(legacy);
    ineligible.routing_eligible = false;
    ineligible.eligibility_reasons = vec!["gaps".into()];
    insert_pace_observation(&mut conn, &ineligible);
    assert_eq!(
        multiplier_pace(&conn, &pace_request(DAY, None, None))
            .unwrap_err()
            .code(),
        "no_multiplier_observation"
    );

    let mut ambiguous = crate::test_support::conn();
    let first = pace_head(&ambiguous);
    let mut second = first.clone();
    second.plan = Some("openai/other".into());
    insert_pace_observation(&mut ambiguous, &first);
    insert_pace_observation(&mut ambiguous, &second);
    assert_eq!(
        multiplier_pace(
            &ambiguous,
            &pace_request(3 * HOUR_MS, None, Some("primary")),
        )
        .unwrap_err()
        .code(),
        "plan_required"
    );
    assert_eq!(
        multiplier_pace(
            &ambiguous,
            &pace_request(3 * HOUR_MS, Some("openai/pro_20x"), Some("primary")),
        )
        .unwrap()["plan"],
        "openai/pro_20x"
    );
}

#[test]
fn pace_reasoning_follows_only_the_recorded_rate_rule() {
    let mut conn = conn();
    let mut head = pace_head(&conn);
    // A provider-wide reasoning_billed_as_output setting must not price reasoning
    // when the recorded rates omit its rule. An explicit recorded reasoning price applies.
    head.reasoning_billed_as_output = vec!["openai".into()];
    for line in &mut head.api_nanos_by_model {
        if line.model.as_deref() == Some("small") {
            if let Some(ModelResolution::Rates(rates)) = line.rates.as_mut() {
                rates.base.remove("reasoning");
            }
        }
    }
    let mut own = ModelLine {
        producer: "broca".into(),
        provider: "openai".into(),
        model: Some("own".into()),
        rates: rates_for(&[("openai", "own")], 2_000_000_000, None)
            .models
            .remove(&("openai".into(), "own".into())),
        ..ModelLine::default()
    };
    let ModelResolution::Rates(rates) = own.rates.as_mut().unwrap() else {
        panic!("own-price fixture must have rates");
    };
    rates.base.insert(
        "reasoning".into(),
        RateState::Priced {
            units_per_million: 3_000_000_000,
        },
    );
    head.api_nanos_by_model.push(own);
    insert_pace_observation(&mut conn, &head);
    for (id, producer, model) in [
        ("billed-output", "opencode", "large"),
        ("unpriced", "broca", "small"),
        ("own-price", "broca", "own"),
    ] {
        account_event_in_class(
            &conn,
            id,
            producer,
            "openai",
            model,
            "subscription_included",
            100,
            Some(model),
            &[("reasoning", "usage", 10)],
        );
    }

    let reply = multiplier_pace(&conn, &pace_request(3 * HOUR_MS, None, None)).unwrap();
    let classes = &reply["hours"][0]["producers"]["opencode"]["by_work_class"];
    assert_eq!(classes["large"]["list_nanos"], 100_000);
    let classes = &reply["hours"][0]["producers"]["broca"]["by_work_class"];
    assert_eq!(classes["small"]["list_nanos"], 0);
    assert_eq!(classes["own"]["list_nanos"], 30_000);
    assert_eq!(
        reply["hours"][0]["producers"]["broca"]["unpriced_tokens"],
        10
    );
}
