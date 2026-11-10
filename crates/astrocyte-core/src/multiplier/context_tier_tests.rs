fn context_tier_rates(entries: &[(&str, i64)]) -> ResolvedRates {
    let snapshot = CatalogSnapshot {
        alias_target: None,
        source: "models.dev".into(),
        resolved_at_ms: 42,
        model_key: "openai/m".into(),
        facts: entries
            .iter()
            .map(|(key, units)| {
                (
                    (*key).into(),
                    json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}),
                )
            })
            .collect(),
        withheld_facts: BTreeMap::new(),
        uncertain_facts: BTreeMap::new(),
        fact_overrides: BTreeMap::new(),
    };
    let resolution = rates_from_snapshot(&snapshot);
    let stored: ModelResolution =
        serde_json::from_value(serde_json::to_value(&resolution).unwrap()).unwrap();
    assert_eq!(stored, resolution);
    ResolvedRates {
        resolved_at_ms: 42,
        models: BTreeMap::from([(("openai".into(), "m".into()), stored)]),
    }
}

#[test]
fn context_tier_is_selected_per_role_above_and_between_thresholds() {
    let rates = context_tier_rates(&[
        ("rate.input", TEN_PER_M),
        ("rate.output", TEN_PER_M),
        ("rate.input.above_context.50000", 3 * TEN_PER_M / 2),
        ("rate.input.above_context.100000", 2 * TEN_PER_M),
        ("rate.output.above_context.200000", 3 * TEN_PER_M),
        ("rate.input.above_context.300000", 4 * TEN_PER_M),
    ]);
    for (input, expected_nanos) in [(250_000, 5_600_000_000), (150_000, 3_200_000_000)] {
        let conn = conn();
        let config = config(vec![rule("opencode", "openai", None)]);
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "e",
            "opencode",
            "openai",
            "m",
            "unknown",
            10,
            &[
                ("input", "usage.input", input),
                ("output", "usage.output", 20_000),
            ],
        );
        let observation = observe(&conn, &config, 0, DAY, &rates);
        // Input is $20/M in both calls; output is $30/M only above 200k.
        assert_eq!(observation.api_nanos, expected_nanos, "input={input}");
        assert_eq!(observation.tokens.input, input);
        assert_eq!(observation.tokens.output, 20_000);
        assert!(observation.unpriced_tokens.is_empty());
        let line = &observation.api_nanos_by_model[0];
        assert_eq!(line.api_nanos, expected_nanos);
        assert_eq!(line.tiered_tokens, input + 20_000);
        assert_eq!(line.base_rate_tier_untested_tokens, 0);
    }
}

#[test]
fn context_tier_shared_threshold_preserves_the_stored_day_observation() {
    let mut conn = conn();
    let config = config(vec![rule("opencode", "openai", None)]);
    reading(&conn, "acct", 0, 0.0, None);
    reading(&conn, "acct", 100, 10.0, None);
    event(
        &conn,
        "e",
        "opencode",
        "openai",
        "m",
        "unknown",
        10,
        &[
            ("input", "usage.input", 250_000),
            ("output", "usage.output", 20_000),
        ],
    );
    let rates = context_tier_rates(&[
        ("rate.input", TEN_PER_M),
        ("rate.output", TEN_PER_M),
        ("rate.input.above_context.100000", 2 * TEN_PER_M),
        ("rate.output.above_context.100000", 2 * TEN_PER_M),
    ]);
    let observation = observe(&conn, &config, 0, DAY, &rates);
    assert_eq!(observation.api_nanos, 5_400_000_000);
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &observation).unwrap();
    tx.commit().unwrap();
    let stored: (i64, i64, i64, String, String, i64) = conn
        .query_row(
            "SELECT api_nanos, input_tokens, output_tokens, api_nanos_by_model,
                    unpriced_tokens, base_rate_tier_untested_tokens
             FROM multiplier_observation",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        (stored.0, stored.1, stored.2),
        (5_400_000_000, 250_000, 20_000)
    );
    assert_eq!(stored.4, "[]");
    assert_eq!(stored.5, 0);
    assert_eq!(
        serde_json::from_str::<Value>(&stored.3).unwrap(),
        json!([{
            "producer": "opencode", "provider": "openai", "model": "m", "events": 1,
            "measured_tokens": {
                "input": 250000, "cache_read": 0, "cache_write": 0,
                "output": 20000, "reasoning": 0
            },
            "inclusive_output_adjustment": 0, "tiered_tokens": 270000,
            "base_rate_tier_untested_tokens": 0, "mode_unrecorded_tokens": 0,
            "api_nanos": 5400000000_i64,
            "rates": {
                "state": "rates",
                "base": {
                    "input": {"state": "priced", "units_per_million": 10000000000_i64},
                    "output": {"state": "priced", "units_per_million": 10000000000_i64}
                },
                "tiers": {"100000": {
                    "input": {"state": "priced", "units_per_million": 20000000000_i64},
                    "output": {"state": "priced", "units_per_million": 20000000000_i64}
                }}
            }
        }])
    );
}

#[test]
fn context_tier_per_role_keeps_complete_mode_and_ttl_matching() {
    let entries = [
        ("rate.input", TEN_PER_M),
        ("rate.input.above_context.50000", 2 * TEN_PER_M),
        ("rate.input.mode.fast", 3 * TEN_PER_M),
        (
            "rate.input.mode.fast.above_context.50000",
            5 * TEN_PER_M / 2,
        ),
        ("rate.cache_write", TEN_PER_M),
        ("rate.cache_write.ttl.1h", TEN_PER_M),
        (
            "rate.cache_write.ttl.1h.above_context.100000",
            4 * TEN_PER_M,
        ),
        ("rate.cache_write.ttl.1h.mode.fast", 3 * TEN_PER_M),
        (
            "rate.cache_write.ttl.1h.mode.fast.above_context.100000",
            5 * TEN_PER_M,
        ),
        ("rate.output", TEN_PER_M),
        ("rate.output.above_context.200000", 3 * TEN_PER_M),
        ("rate.output.mode.fast", 2 * TEN_PER_M),
        (
            "rate.output.mode.fast.above_context.200000",
            7 * TEN_PER_M / 2,
        ),
    ];
    for (mode, omitted, expected_nanos, refusal) in [
        (None, "", 5_800_000_000, None),
        (Some("fast"), "", 7_200_000_000, None),
        (
            Some("fast"),
            "rate.cache_write.ttl.1h.mode.fast.above_context.100000",
            6_700_000_000,
            Some(("no_rate", "cache_write", 10_000)),
        ),
        (
            Some("fast"),
            "rate.input.mode.fast.above_context.50000",
            1_200_000_000,
            Some(("mode_rate_above_tier", "input", 240_000)),
        ),
    ] {
        let conn = conn();
        let config = config(vec![rule("opencode", "openai", None)]);
        reading(&conn, "acct", 0, 0.0, None);
        reading(&conn, "acct", 100, 10.0, None);
        event(
            &conn,
            "e",
            "opencode",
            "openai",
            "m",
            "unknown",
            10,
            &[
                ("input", "usage.input", 240_000),
                ("output", "usage.output", 20_000),
            ],
        );
        conn.execute(
            "INSERT INTO measurement (event_key, role, unit, kind, source, quantity_units,
                 quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id='e'),
                 'cache_write', 'token_cache_1h', 'billed', 'usage.cache_write', 10000, 0)",
            [],
        )
        .unwrap();
        let entries: Vec<_> = entries
            .iter()
            .copied()
            .filter(|(key, _)| *key != omitted)
            .collect();
        let rates = context_tier_rates(&entries);
        let mut plan = plan_observation(&conn, &config, "codex", 0, DAY).unwrap();
        // Stored usage currently has no mode; exercise stated modes on the
        // captured event without bypassing observation valuation.
        for event in &mut plan.events {
            event.tokens.mode = mode.map(str::to_owned);
        }
        let observation =
            super::compute_observation(&conn, &config, &plan, &rates, &priced_plan(), DAY, &[])
                .unwrap();
        assert_eq!(
            observation.api_nanos, expected_nanos,
            "mode={mode:?}, omitted={omitted}"
        );
        assert_eq!(observation.tokens.input, 240_000);
        assert_eq!(observation.tokens.cache_write, 10_000);
        assert_eq!(observation.tokens.output, 20_000);
        assert_eq!(observation.api_nanos_by_model[0].tiered_tokens, 270_000);
        let unpriced: Vec<_> = observation
            .unpriced_tokens
            .iter()
            .map(|entry| (entry.reason.as_str(), entry.role.as_str(), entry.tokens))
            .collect();
        assert_eq!(unpriced, refusal.into_iter().collect::<Vec<_>>());
    }
}
