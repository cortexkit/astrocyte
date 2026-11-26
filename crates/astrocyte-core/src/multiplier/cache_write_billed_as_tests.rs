use crate::test_support::{cache_write_vector, XAI_MODEL, XAI_MODEL_KEY};

fn publish_cache_write_rates(conn: &Connection, mut reply: fusiform_protocol::CatalogGetResponse) {
    let segments: BTreeMap<_, _> = reply
        .models
        .remove(XAI_MODEL_KEY)
        .unwrap()
        .into_iter()
        .filter(|(key, _)| key.starts_with("rate."))
        .map(|(key, value)| (key, json!([{"from_ms":0,"state":"value","value":value}])))
        .collect();
    let history = serde_json::from_value(json!({
        "source":reply.source, "provider_id":"xai", "model_id":XAI_MODEL,
        "fact_prefix":"rate.", "eras":[], "served_by_key":segments,
    }))
    .unwrap();
    let fetched = crate::rate_era::fetched(history, "xai", XAI_MODEL, None).unwrap();
    crate::rate_era::publish(conn, &fetched, 1).unwrap();
}

fn cache_write_call(quantity: i64, unit: &str) -> EventTokens {
    EventTokens {
        producer: "opencode".into(),
        provider: "xai".into(),
        model: Some(XAI_MODEL.into()),
        mode: None,
        broca_attempt: false,
        roles: vec![(
            "cache_write".into(),
            unit.into(),
            "fixture".into(),
            Some(quantity),
            Some(0),
        )],
    }
}

#[test]
fn cache_write_billed_as_multiplier_values_each_tier_and_stores_the_rule() {
    let mut conn = conn();
    publish_cache_write_rates(&conn, cache_write_vector());
    let config = config(vec![rule("opencode", "xai", None)]);
    let rules = ValuationRules::from_config(&config, &[]);
    let resolution = crate::rate_era::resolve(&conn, "xai", XAI_MODEL, 10).unwrap();
    for (id, quantity, expected_nanos) in [
        ("base", 200_000, 250_000_000_i64),
        ("tier", 300_000, 750_000_000_i64),
    ] {
        let call = cache_write_call(quantity, "token");
        let value = crate::valuation::list_value_event(&rules, resolution.as_ref(), &call).unwrap();
        assert_eq!(
            value.numerator,
            i128::from(expected_nanos) * 1_000_000,
            "{id}"
        );
        assert_eq!(value.unvalued.tokens, 0);
        event(
            &conn,
            id,
            "opencode",
            "xai",
            XAI_MODEL,
            "unknown",
            10,
            &[("cache_write", "fixture", quantity)],
        );
    }
    reading(&conn, "acct", 0, 0.0, None);
    reading(&conn, "acct", 20, 10.0, None);
    let plan = plan_observation(&conn, &config, "codex", 0, 20).unwrap();
    let rates =
        crate::rate_era::resolved_rates(&conn, &catalog_models(&config, &plan), 20).unwrap();
    let observation =
        super::compute_observation(&conn, &config, &plan, &rates, &priced_plan(), 20, &[]).unwrap();
    assert_eq!(observation.api_nanos, 1_000_000_000);
    assert_eq!(observation.tokens.cache_write, 500_000);
    assert!(observation.unpriced_tokens.is_empty());
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &observation).unwrap();
    tx.commit().unwrap();
    let stored: String = conn
        .query_row(
            "SELECT api_nanos_by_model FROM multiplier_observation",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        stored[0]["rates"]["base"]["cache_write"],
        json!({"state":"billed_as", "key":"rate.input"})
    );
    assert_eq!(
        stored[0]["rates"]["tiers"]["200000"]["cache_write"],
        json!({"state":"billed_as", "key":"rate.input.above_context.200000"})
    );
    assert_eq!(
        serde_json::from_value::<ModelResolution>(stored[0]["rates"].clone()).unwrap(),
        resolution.unwrap()
    );
}

#[test]
fn cache_write_billed_as_valuation_honors_the_named_tier_and_explicit_verdicts() {
    let mut reply = cache_write_vector();
    let key = "rate.cache_write.above_context.200000";
    let facts = reply.models.get_mut(XAI_MODEL_KEY).unwrap();
    let tier_rule = facts[key].clone();
    // The explicit alias names the 200,000 tier even though a plain input
    // measurement for the same call would select a higher input tier.
    facts.insert(
        "rate.input.above_context.250000".into(),
        reference_rate(5_000_000_000),
    );
    for (variant, nanos_per_million, unpriced) in [
        (tier_rule, 2_500_000_000_i64, 0),
        (reference_rate(7_000_000_000), 7_000_000_000, 0),
        (
            json!({"state":"unpriced", "reason":"unknown_charge_basis"}),
            0,
            300_000,
        ),
        (
            json!({"state":"unpriced", "reason":"missing_rate"}),
            5_000_000_000,
            0,
        ),
    ] {
        reply
            .models
            .get_mut(XAI_MODEL_KEY)
            .unwrap()
            .insert(key.into(), variant);
        let conn = conn();
        publish_cache_write_rates(&conn, reply.clone());
        let resolution = crate::rate_era::resolve(&conn, "xai", XAI_MODEL, 10).unwrap();
        let config = config(vec![]);
        let call = cache_write_call(300_000, "token");
        let value = crate::valuation::list_value_event(
            &ValuationRules::from_config(&config, &[]),
            resolution.as_ref(),
            &call,
        )
        .unwrap();
        assert_eq!(value.numerator, i128::from(nanos_per_million) * 300_000);
        assert_eq!(value.unvalued.tokens, unpriced);
    }
}

#[test]
fn cache_write_billed_as_valuation_refuses_unserved_pairs_and_conditions() {
    let reply = cache_write_vector();
    for (fact, target) in [
        ("rate.cache_write", "rate.output"),
        ("rate.reasoning", "rate.input"),
        ("rate.cache_write.above_context.200000", "rate.input"),
        (
            "rate.cache_write.above_context.200000",
            "rate.input.above_context.300000",
        ),
        ("rate.cache_write.mode.fast", "rate.input.mode.fast"),
        ("rate.cache_write.ttl.5m", "rate.input"),
    ] {
        let conn = conn();
        let mut invalid = reply.clone();
        invalid
            .models
            .get_mut(XAI_MODEL_KEY)
            .unwrap()
            .insert(fact.into(), json!({"state":"billed_as", "key":target}));
        publish_cache_write_rates(&conn, invalid);
        assert_eq!(
            crate::rate_era::resolve(&conn, "xai", XAI_MODEL, 10).unwrap(),
            Some(ModelResolution::Unpriceable {
                reason: "rate_undecodable".into()
            }),
            "{fact} → {target}"
        );
    }
    let conn = conn();
    publish_cache_write_rates(&conn, reply);
    assert!(matches!(
        crate::rate_era::resolve(&conn, "xai", XAI_MODEL, 10).unwrap(),
        Some(ModelResolution::Rates(_))
    ));
}

#[test]
fn cache_write_billed_as_valuation_requires_the_recorded_lifetime() {
    let mut reply = cache_write_vector();
    reply.models.get_mut(XAI_MODEL_KEY).unwrap().insert(
        "rate.cache_write.ttl.1h".into(),
        reference_rate(7_000_000_000),
    );
    let conn = conn();
    publish_cache_write_rates(&conn, reply);
    let resolution = crate::rate_era::resolve(&conn, "xai", XAI_MODEL, 10).unwrap();
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    let hour = cache_write_call(100, "token_cache_1h");
    let value = crate::valuation::value_event(&rules, resolution.as_ref(), &hour).unwrap();
    assert_eq!(value.numerator, 700_000_000_000);
    let five_minutes = cache_write_call(100, "token_cache_5m");
    let value = crate::valuation::value_event(&rules, resolution.as_ref(), &five_minutes).unwrap();
    assert_eq!(value.numerator, 0);
    assert_eq!(value.unpriced.len(), 1);
    assert_eq!(value.unpriced[0].reason, "no_rate");
    assert_eq!(value.unpriced[0].quantity, 100);
}
