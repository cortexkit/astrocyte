use crate::test_support::{cache_write_vector, XAI_MODEL, XAI_MODEL_KEY};

type StoredCacheWriteCharge = (String, Option<i64>, Option<String>, Option<String>);

fn cache_write_billed_as_ledger(
    reply: CatalogGetResponse,
    prompt: i64,
    unit: &str,
    mode: Option<&str>,
) -> Result<Vec<StoredCacheWriteCharge>, PricingError> {
    let mut conn = conn();
    let request: EventRecordRequest = serde_json::from_value(json!({
        "producer":"producer", "producer_lineage":"lineage", "producer_ref":"cache-write",
        "provider":"xai", "model":XAI_MODEL, "charge_basis":"metered_api", "occurred_at_ms":1_000,
        "measurements":[
            {"role":"input", "unit":"token", "kind":"billed", "source":"test",
             "quantity_units":prompt, "quantity_exponent":0},
            {"role":"cache_write", "unit":unit, "kind":"billed", "source":"test",
             "quantity_units":10, "quantity_exponent":0}
        ]
    }))
    .unwrap();
    let id = record_event(&mut conn, &request, 1_001).unwrap().event_id;
    let fake = FakeCatalog::new(BTreeMap::new());
    *fake.response.lock().unwrap() = reply;
    let mut plan = read_reprice(&conn, &id).unwrap();
    plan.mode = mode.map(str::to_owned);
    let resolved = resolve_reprice(&plan, &client(fake))?;
    let tx = conn.transaction().unwrap();
    commit_reprice_in_tx(&tx, &plan, &resolved, 1_002, PricingOrigin::PricingPass).unwrap();
    tx.commit().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT role, charge_nanos, unpriced_reason, json_extract(catalog_entry,'$.fact_key')
         FROM priced_charge ORDER BY role",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    Ok(rows)
}

#[test]
fn cache_write_billed_as_ledger_uses_input_below_tier_but_refuses_the_whole_crossing_event() {
    let reply = cache_write_vector();
    assert_eq!(
        cache_write_billed_as_ledger(reply.clone(), 200_000, "token", None).unwrap(),
        ["cache_write", "input"].map(|role| (role.into(), None, Some("rate_tiered".into()), None)),
    );
    assert_eq!(
        cache_write_billed_as_ledger(reply, 100, "token", None).unwrap(),
        [
            (
                "cache_write".into(),
                Some(12_500),
                None,
                Some("rate.input".into())
            ),
            (
                "input".into(),
                Some(125_000),
                None,
                Some("rate.input".into())
            )
        ],
    );
}

#[test]
fn cache_write_billed_as_ledger_prefers_explicit_variants_and_follows_input_mode_if_missing() {
    let mut reply = cache_write_vector();
    let alias = "rate.cache_write.mode.fast";
    for (variant, nanos, reason, key) in [
        (rate(7_000_000_000), Some(70_000), None, alias),
        (
            json!({"state":"unpriced", "reason":"unknown_charge_basis"}),
            None,
            Some("catalog_unknown_charge_basis"),
            alias,
        ),
        (
            json!({"state":"unpriced", "reason":"missing_rate"}),
            Some(40_000),
            None,
            "rate.input.mode.fast",
        ),
    ] {
        let facts = reply.models.get_mut(XAI_MODEL_KEY).unwrap();
        facts.insert("rate.input.mode.fast".into(), rate(4_000_000_000));
        facts.insert(alias.into(), variant);
        let rows = cache_write_billed_as_ledger(reply.clone(), 100, "token", Some("fast")).unwrap();
        assert_eq!(
            rows[0],
            (
                "cache_write".into(),
                nanos,
                reason.map(str::to_owned),
                Some(key.into())
            )
        );
    }
}
