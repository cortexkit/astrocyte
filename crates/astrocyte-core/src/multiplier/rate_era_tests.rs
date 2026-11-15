#[test]
fn rate_era_multiplier_values_each_call_and_does_not_recompute_stored_observations() {
    let mut conn = conn();
    let declarations = config(vec![rule("opencode", "openai", None)]);
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
    reading(&conn, "acct", 0, 0.0, None);
    reading(&conn, "acct", 400, 10.0, None);
    let plan = plan_observation(&conn, &declarations, "codex", 0, 400).unwrap();
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
    let rates =
        crate::rate_era::resolved_rates(&conn, &catalog_models(&declarations, &plan), 400).unwrap();
    let observation =
        compute_observation(&conn, &declarations, &plan, &rates, &priced_plan(), 400).unwrap();
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
    assert!(observation_exists(&conn, "codex", &observation.observation_day).unwrap());
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
