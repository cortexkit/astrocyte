#[test]
fn rate_era_multiplier_values_each_call_and_does_not_recompute_stored_observations() {
    let mut conn = conn();
    for (index, at) in [10, 110, 210, 310].into_iter().enumerate() {
        event(
            &conn,
            &format!("era-{index}"),
            "opencode",
            "openai",
            "m",
            "unknown",
            at,
            &[("input", "fixture", 100)],
        );
    }
    let history: fusiform_protocol::HistoryResponse = serde_json::from_value(json!({
        "source":"synthetic", "provider_id":"openai", "model_id":"m", "fact_prefix":"rate.", "eras":[],
        "served_by_key":{"rate.input":[
            {"from_ms":0,"until_ms":100,"state":"value","value":{"state":"priced","currency":"USD","units":1000000000,"exponent":9,"unit_provenance":{"kind":"stated"}}},
            {"from_ms":100,"until_ms":200,"state":"value","value":{"state":"priced","currency":"USD","units":2000000000,"exponent":9,"unit_provenance":{"kind":"stated"}}},
            {"from_ms":200,"until_ms":300,"state":"value","value":{"state":"priced","currency":"USD","units":1000000000,"exponent":9,"unit_provenance":{"kind":"stated"}}},
            {"from_ms":300,"until_ms":null,"state":"refused","reason":"correction"}
        ]}
    })).unwrap();
    let fetched = crate::rate_era::fetched(history, "openai", "m", None).unwrap();
    crate::rate_era::publish(&conn, &fetched, 1).unwrap();
    let rates = crate::rate_era::resolved_rates(&conn, &test_models(&conn, 0, 400), 400).unwrap();
    let observation = storeable_usage(&conn, (0, 400), &rates, 400);
    assert_eq!(observation.api_nanos, 400_000);
    assert!(observation
        .unpriced_tokens
        .iter()
        .any(|row| row.reason == "refused" && row.tokens == 100));
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
    let mut changed = fetched;
    changed.segments.clear();
    crate::rate_era::publish(&conn, &changed, 2).unwrap();
    assert!(
        observation_head(&conn, "codex", &observation.observation_day)
            .unwrap()
            .is_some()
    );
    let unchanged: String = conn
        .query_row(
            "SELECT api_nanos_by_model FROM multiplier_observation",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, unchanged);
    let amount: i64 = conn
        .query_row("SELECT api_nanos FROM multiplier_observation", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(amount, 400_000);
}

fn reasoning_billed_as_history() -> fusiform_protocol::HistoryResponse {
    let captured: fusiform_protocol::HistoryResponse =
        crate::test_support::reasoning_vector("openai-history.json");
    let mut reasoning = captured.served.unwrap();
    let from_ms = reasoning[0].from_ms;
    // Close the sample's open interval to test both boundaries. Output is
    // already priced before the rule and remains priced when the rule ends.
    reasoning[0].until_ms = Some(from_ms + 100);
    let output: fusiform_protocol::CatalogGetResponse =
        crate::test_support::reasoning_vector("openai-dated.json");
    serde_json::from_value(json!({
        "source": captured.source, "provider_id": captured.provider_id,
        "model_id": captured.model_id, "fact_prefix": "rate.", "eras": [],
        "served_by_key": {
            "rate.output": [{"from_ms": from_ms - 100, "state": "value",
                "value": output.models["openai/gpt-6.1-sol"]["rate.output"]}],
            "rate.reasoning": reasoning
        }
    }))
    .unwrap()
}

#[test]
fn reasoning_billed_as_history_controls_list_value_and_multiplier_at_call_time() {
    let conn = conn();
    let history = reasoning_billed_as_history();
    let from_ms = history.served_by_key.as_ref().unwrap()["rate.reasoning"][0].from_ms;
    let model = "gpt-6.1-sol";
    let fetched = crate::rate_era::fetched(history, "openai", model, None).unwrap();
    crate::rate_era::publish(&conn, &fetched, 1).unwrap();
    let call = EventTokens {
        producer: "opencode".into(),
        provider: "openai".into(),
        model: Some(model.into()),
        mode: None,
        broca_attempt: false,
        roles: vec![(
            "reasoning".into(),
            "token".into(),
            "fixture".into(),
            Some(1_000_000),
            Some(0),
        )],
    };
    for (i, (at, charge)) in [
        (from_ms - 1, 0),
        (from_ms, 10_000_000_000_i64),
        (from_ms + 99, 10_000_000_000),
        (from_ms + 100, 0),
    ]
    .into_iter()
    .enumerate()
    {
        let resolution = crate::rate_era::resolve(&conn, "openai", model, at).unwrap();
        let value = crate::valuation::list_value_event(resolution.as_ref(), &call).unwrap();
        assert_eq!(
            value.numerator,
            i128::from(charge) * 1_000_000,
            "call at {at}"
        );
        assert_eq!(
            value.unvalued.tokens,
            if charge == 0 { 1_000_000 } else { 0 }
        );
        event(
            &conn,
            &format!("reasoning-{i}"),
            "opencode",
            "openai",
            model,
            "unknown",
            at,
            &[("reasoning", "fixture", 1_000_000)],
        );
    }
    let end = from_ms + 150;
    let rates = crate::rate_era::resolved_rates(&conn, &test_models(&conn, from_ms - 50, end), end)
        .unwrap();
    let observation = storeable_usage(&conn, (from_ms - 50, end), &rates, end);
    assert_eq!(observation.api_nanos, 20_000_000_000);
    assert_eq!(observation.tokens.reasoning, 4_000_000);
    assert_eq!(observation.unpriced_tokens.len(), 1);
    assert_eq!(observation.unpriced_tokens[0].reason, "no_rate");
    assert_eq!(observation.unpriced_tokens[0].tokens, 2_000_000);
    let resolution = crate::rate_era::resolve(&conn, "openai", model, from_ms)
        .unwrap()
        .unwrap();
    let encoded = serde_json::to_value(&resolution).unwrap();
    assert_eq!(
        encoded["base"]["reasoning"],
        json!({"state":"billed_as", "key":"rate.output"})
    );
    assert_eq!(
        serde_json::from_value::<ModelResolution>(encoded).unwrap(),
        resolution
    );
}

fn reasoning_billed_as_valuation(
    facts: BTreeMap<String, Value>,
    mode: Option<&str>,
) -> crate::valuation::ListValue {
    let conn = conn();
    let segments: BTreeMap<_, _> = facts
        .into_iter()
        .map(|(key, value)| {
            (
                key,
                json!([
                    {"from_ms":0,"state":"value","value":value}
                ]),
            )
        })
        .collect();
    let history = serde_json::from_value(json!({
        "source":"fixture", "provider_id":"openai", "model_id":"gpt-6.1-sol",
        "fact_prefix":"rate.", "eras":[], "served_by_key":segments,
    }))
    .unwrap();
    let fetched = crate::rate_era::fetched(history, "openai", "gpt-6.1-sol", None).unwrap();
    crate::rate_era::publish(&conn, &fetched, 1).unwrap();
    let resolution = crate::rate_era::resolve(&conn, "openai", "gpt-6.1-sol", 10).unwrap();
    // The sample is filtered to output and reasoning, so the input is
    // unpriced; its quantity is still what selects the tier.
    let call = EventTokens {
        producer: "opencode".into(),
        provider: "openai".into(),
        model: Some("gpt-6.1-sol".into()),
        mode: mode.map(str::to_owned),
        broca_attempt: false,
        roles: vec![
            (
                "input".into(),
                "token".into(),
                "fixture".into(),
                Some(300_000),
                Some(0),
            ),
            (
                "reasoning".into(),
                "token".into(),
                "fixture".into(),
                Some(1_000_000),
                Some(0),
            ),
        ],
    };
    crate::valuation::list_value_event(resolution.as_ref(), &call).unwrap()
}

#[test]
fn reasoning_billed_as_valuation_follows_the_crossed_output_tier() {
    let mut response: fusiform_protocol::CatalogGetResponse =
        crate::test_support::reasoning_vector("openai-dated.json");
    let facts = response.models.remove("openai/gpt-6.1-sol").unwrap();
    assert_eq!(
        reasoning_billed_as_valuation(facts, None).numerator,
        15_000_000_000_000_000
    );
}

#[test]
fn reasoning_billed_as_valuation_prefers_the_published_tier() {
    let mut response: fusiform_protocol::CatalogGetResponse =
        crate::test_support::reasoning_vector("openai-dated.json");
    let mut facts = response.models.remove("openai/gpt-6.1-sol").unwrap();
    facts.insert(
        "rate.reasoning.above_context.272000".into(),
        reference_rate(7_000_000_000),
    );
    assert_eq!(
        reasoning_billed_as_valuation(facts.clone(), None).numerator,
        7_000_000_000_000_000
    );
    for (reason, expected) in [
        ("unknown_charge_basis", 0),
        ("missing_rate", 15_000_000_000_000_000),
    ] {
        facts.insert(
            "rate.reasoning.above_context.272000".into(),
            json!({"state":"unpriced", "reason":reason}),
        );
        assert_eq!(
            reasoning_billed_as_valuation(facts.clone(), None).numerator,
            expected
        );
    }
}

struct ReasoningQuotaCatalog;

impl crate::catalog_client::CatalogConnection for ReasoningQuotaCatalog {
    fn catalog_status(
        &self,
        _: fusiform_protocol::StatusRequest,
    ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
        unreachable!("the fixture supplies the history boundary")
    }

    fn catalog_get(
        &self,
        request: fusiform_protocol::CatalogGetRequest,
    ) -> Result<fusiform_protocol::CatalogGetResponse, CatalogCallError> {
        let provider = request.provider_id.as_deref().unwrap();
        let model = request.model_id.as_deref().unwrap();
        let key = format!("{provider}/{model}");
        let name = match key.as_str() {
            "kimi-code-plan-global/k3.1-pro" | "moonshotai/kimi-k3.1-pro" => {
                "kimi-code-plan-global.json"
            }
            "opencode-go/kimi-k3.1-pro" => "opencode-go.json",
            _ => panic!("unexpected catalog request: {request:?}"),
        };
        let mut response: fusiform_protocol::CatalogGetResponse =
            crate::test_support::reasoning_vector(name);
        if provider == "moonshotai" {
            let mut facts = response.models.pop_first().unwrap().1;
            // The donor publishes its own API prices, not inherited rates.
            for (key, value) in &mut facts {
                if key.starts_with("rate.output") {
                    value.as_object_mut().unwrap().remove("inherited_from");
                }
            }
            response.models.insert(key, facts);
        } else if request.auth_method.is_none() {
            // The ordinary plan card has no positive token prices. Discovery
            // must not count the served billing rule as a price.
            let facts = response.models.get_mut(&key).unwrap();
            facts.retain(|key, _| key != "rate.output.mode.fast");
            facts.insert("rate.output".into(), json!({"state":"stated_zero"}));
        }
        Ok(response)
    }

    fn catalog_history(
        &self,
        request: fusiform_protocol::HistoryRequest,
        _: std::time::Duration,
    ) -> Result<fusiform_protocol::HistoryResponse, CatalogCallError> {
        let provider = request.provider_id;
        let model = request.model_id;
        let response = self
            .catalog_get(
                serde_json::from_value(json!({
                    "provider_id": &provider, "model_id": &model,
                }))
                .unwrap(),
            )
            .unwrap();
        let key = format!("{provider}/{model}");
        let segments: BTreeMap<_, _> = response.models[&key]
            .iter()
            .filter(|(key, _)| key.starts_with("rate."))
            .map(|(key, value)| (key, json!([{"from_ms":0,"state":"value","value":value}])))
            .collect();
        Ok(serde_json::from_value(json!({
            "source":"fixture", "provider_id":provider, "model_id":model, "fact_prefix":"rate.",
            "served_by_key":segments, "eras":[],
        }))
        .unwrap())
    }
}

#[test]
fn reasoning_billed_as_kimi_uses_the_dated_quota_donor_but_opencode_does_not() {
    let conn = conn();
    let catalog = CatalogClient::new(std::sync::Arc::new(ReasoningQuotaCatalog), 0);
    let end = 100;
    for (provider, model) in [
        ("opencode-go", "kimi-k3.1-pro"),
        ("kimi-code-plan-global", "k3.1-pro"),
        ("moonshotai", "kimi-k3.1-pro"),
    ] {
        let fetched = crate::rate_era::fetch(&catalog, provider, model).unwrap();
        crate::rate_era::publish(&conn, &fetched, 1).unwrap();
    }
    for (provider, model, units) in [
        ("opencode-go", "kimi-k3.1-pro", 0),
        ("kimi-code-plan-global", "k3.1-pro", 10_000_000_000_i64),
    ] {
        let call = EventTokens {
            producer: "opencode".into(),
            provider: provider.into(),
            model: Some(model.into()),
            mode: None,
            broca_attempt: false,
            roles: vec![(
                "reasoning".into(),
                "token".into(),
                "fixture".into(),
                Some(1_000_000),
                Some(0),
            )],
        };
        let resolution = crate::rate_era::resolve(&conn, provider, model, 20).unwrap();
        let value = crate::valuation::list_value_event(resolution.as_ref(), &call).unwrap();
        assert_eq!(value.numerator, i128::from(units) * 1_000_000, "{provider}");
        event(
            &conn,
            provider,
            "opencode",
            provider,
            model,
            "unknown",
            20,
            &[("reasoning", "fixture", 1_000_000)],
        );
    }
    let models = test_models(&conn, 0, end);
    let cached = crate::rate_era::resolved_rates(&conn, &models, end).unwrap();
    let direct = resolve_rates(&catalog, &models, end).unwrap();
    for rates in [cached, direct] {
        let observation = storeable_usage(&conn, (0, end), &rates, end);
        assert_eq!(observation.api_nanos, 10_000_000_000);
        assert_eq!(observation.unpriced_tokens.len(), 1);
        assert_eq!(observation.unpriced_tokens[0].reason, "no_rate");
        assert_eq!(observation.unpriced_tokens[0].role, "reasoning");
        assert_eq!(observation.unpriced_tokens[0].tokens, 1_000_000);
    }
}
