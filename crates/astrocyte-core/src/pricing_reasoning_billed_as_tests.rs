type ReasoningCharge = (Option<i64>, Option<String>, Option<String>);

fn reasoning_billed_as_ledger(
    response: CatalogGetResponse,
    mode: Option<&str>,
    prompt: i64,
) -> Result<ReasoningCharge, PricingError> {
    let (provider, model) = response
        .models
        .keys()
        .next()
        .unwrap()
        .split_once('/')
        .unwrap();
    let at_ms = response.resolved_at_ms;
    let mut conn = conn();
    let request: EventRecordRequest = serde_json::from_value(json!({
        "producer":"producer", "producer_lineage":"lineage", "producer_ref":"reasoning",
        "provider":provider, "model":model, "charge_basis":"metered_api", "occurred_at_ms":at_ms,
        "measurements":[
            {"role":"input", "unit":"token", "kind":"billed", "source":"test",
             "quantity_units":prompt, "quantity_exponent":0},
            {"role":"reasoning", "unit":"token", "kind":"billed", "source":"test",
             "quantity_units":1_000_000, "quantity_exponent":0}
        ]
    }))
    .unwrap();
    let id = record_event(&mut conn, &request, at_ms + 1)
        .unwrap()
        .event_id;
    let fake = FakeCatalog::new(BTreeMap::new());
    *fake.response.lock().unwrap() = response;
    let mut plan = read_reprice(&conn, &id).unwrap();
    plan.mode = mode.map(str::to_owned);
    let resolved = resolve_reprice(&plan, &client(fake.clone()))?;
    assert_eq!(fake.requests.lock().unwrap()[0].at_ms, Some(at_ms));
    let tx = conn.transaction().unwrap();
    commit_reprice_in_tx(&tx, &plan, &resolved, at_ms + 2, PricingOrigin::PricingPass).unwrap();
    tx.commit().unwrap();
    let (role, nanos, reason, key): (String, _, _, _) = conn
        .query_row(
            "SELECT role,charge_nanos,unpriced_reason,json_extract(catalog_entry,'$.fact_key')
         FROM priced_charge WHERE role!='input'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(role, "reasoning");
    Ok((nanos, reason, key))
}

fn assert_reasoning_billed_as_sample(name: &str, mode: Option<&str>, nanos: i64, key: &str) {
    let response = crate::test_support::reasoning_vector(name);
    assert_eq!(
        reasoning_billed_as_ledger(response, mode, 100).unwrap(),
        (Some(nanos), None, Some(key.into())),
    );
}

#[test]
fn reasoning_billed_as_openai_current_ledger() {
    assert_reasoning_billed_as_sample("openai-current.json", None, 10_000_000_000, "rate.output");
}

#[test]
fn reasoning_billed_as_openai_dated_ledger() {
    assert_reasoning_billed_as_sample("openai-dated.json", None, 10_000_000_000, "rate.output");
}

#[test]
fn reasoning_billed_as_anthropic_current_ledger() {
    assert_reasoning_billed_as_sample(
        "anthropic-current.json",
        None,
        20_000_000_000,
        "rate.output",
    );
}

#[test]
fn reasoning_billed_as_anthropic_dated_ledger() {
    assert_reasoning_billed_as_sample("anthropic-dated.json", None, 20_000_000_000, "rate.output");
}

#[test]
fn reasoning_billed_as_ledger_follows_fast_mode_but_still_refuses_crossed_tiers() {
    let response = crate::test_support::reasoning_vector("openai-dated.json");
    assert_eq!(
        reasoning_billed_as_ledger(response, Some("fast"), 272_001).unwrap(),
        (None, Some("rate_tiered".into()), None),
    );
    assert_reasoning_billed_as_sample(
        "openai-dated.json",
        Some("fast"),
        20_000_000_000,
        "rate.output.mode.fast",
    );
    assert_reasoning_billed_as_sample(
        "anthropic-dated.json",
        Some("fast"),
        40_000_000_000,
        "rate.output.mode.fast",
    );
}

#[test]
fn reasoning_billed_as_ledger_variant_wins_over_base_rule() {
    let mut response: CatalogGetResponse =
        crate::test_support::reasoning_vector("anthropic-dated.json");
    let facts = response
        .models
        .get_mut("anthropic/claude-opus-5-5")
        .unwrap();
    facts.insert("rate.reasoning.mode.fast".into(), rate(7_000_000_000));
    assert_eq!(
        reasoning_billed_as_ledger(response.clone(), Some("fast"), 100)
            .unwrap(),
        (
            Some(7_000_000_000),
            None,
            Some("rate.reasoning.mode.fast".into())
        ),
    );
    for (reason, expected) in [
        (
            "unknown_charge_basis",
            (
                None,
                Some("catalog_unknown_charge_basis".into()),
                Some("rate.reasoning.mode.fast".into()),
            ),
        ),
        (
            "missing_rate",
            (
                Some(40_000_000_000),
                None,
                Some("rate.output.mode.fast".into()),
            ),
        ),
    ] {
        response
            .models
            .get_mut("anthropic/claude-opus-5-5")
            .unwrap()
            .insert(
                "rate.reasoning.mode.fast".into(),
                json!({"state":"unpriced", "reason":reason}),
            );
        assert_eq!(
            reasoning_billed_as_ledger(response.clone(), Some("fast"), 100).unwrap(),
            expected
        );
    }
}

#[test]
fn reasoning_billed_as_ledger_keeps_other_billing_plane_refusals() {
    let response: CatalogGetResponse =
        crate::test_support::reasoning_vector("anthropic-dated.json");
    for (rate_key, target) in [
        ("rate.reasoning", "rate.input"),
        ("rate.output", "rate.output"),
    ] {
        let mut invalid = response.clone();
        invalid
            .models
            .get_mut("anthropic/claude-opus-5-5")
            .unwrap()
            .insert(rate_key.into(), json!({"state":"billed_as", "key": target}));
        assert!(matches!(
            reasoning_billed_as_ledger(invalid, None, 100),
            Err(PricingError::Catalog(
                CatalogClientError::InvalidRate { .. }
            )),
        ));
    }
    assert_eq!(
        reasoning_billed_as_ledger(response, None, 100).unwrap(),
        (Some(20_000_000_000), None, Some("rate.output".into())),
    );
}

#[test]
fn reasoning_billed_as_ledger_does_not_charge_the_kimi_quota_proxy() {
    let response = crate::test_support::reasoning_vector("kimi-code-plan-global.json");
    assert_eq!(
        reasoning_billed_as_ledger(response, Some("fast"), 100).unwrap(),
        (
            None,
            Some("rate_inherited".into()),
            Some("rate.output.mode.fast".into())
        ),
    );
}
