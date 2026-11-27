#[test]
fn catalog_regression_prices_adjusted_quantity_above_signed_range() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "large-adjusted-quantity",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![unit_billing_measurement("input_audio", "second", i64::MAX)],
    );
    let mut value = unit_billing_rate(
        Some(2),
        json!({"kind":"quantum","quantity":2}),
        json!({"kind":"none"}),
    );
    value["units"] = json!(1);
    let catalog = client(FakeCatalog::new(BTreeMap::from([(
        "rate.input_audio.per_second".into(),
        value,
    )])));

    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();

    let (charge, entry) = unit_billing_stored(&conn);
    assert_eq!(charge, Some(1_i64 << 62));
    assert_eq!(
        entry["raw_quantity"],
        json!({"units":i64::MAX,"exponent":0})
    );
    assert_eq!(
        entry["priced_quantity"],
        json!({"units":1_u64 << 63,"exponent":0})
    );
}

#[test]
fn catalog_regression_records_uncertainty_without_a_rate_value() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "uncertain-without-value",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![measurement("input", Some(1))],
    );
    let fake = FakeCatalog::new(BTreeMap::new());
    let uncertainty = UncertainFactWire {
        provider_id: "provider".into(),
        model_id: "model".into(),
        fact_key: "rate.input".into(),
        superseded_after_ms: 600,
        superseded_by_ms: 800,
    };
    let expected = json!({"fact_key":"rate.input", "uncertain":uncertainty});
    fake.response.lock().unwrap().uncertain = vec![uncertainty];

    reprice_event(
        &mut conn,
        &id,
        &client(fake),
        1_000,
        PricingOrigin::PricingPass,
    )
    .unwrap();

    let (reason, charge, entry): (String, Option<i64>, String) = conn
        .query_row(
            "SELECT unpriced_reason,charge_nanos,catalog_entry FROM priced_charge",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(reason, "rate_uncertain");
    assert_eq!(charge, None);
    assert_eq!(serde_json::from_str::<Value>(&entry).unwrap(), expected);
}

#[test]
fn catalog_regression_media_tokens_require_the_exact_selector_set() {
    let mut conn = conn();
    let catalog = client(FakeCatalog::new(BTreeMap::from([
        ("rate.output_image".into(), rate(1_000_000)),
        ("rate.output_image.quality.high".into(), rate(9_000_000)),
        (
            "rate.output_image.quality.high.style.photo".into(),
            rate(5_000_000),
        ),
    ])));
    for (id, quality, expected_charge, expected_reason) in [
        ("high", "high", Some(18), None),
        ("low", "low", None, Some("no_rate")),
    ] {
        let wire = json!({
            "record_id":id,"principal":"person-a","job_id":"job-1",
            "provider":"provider","model":"model","charge_basis":"vendor",
            "auth_method":"api_key","started_at_ms":700,"finished_at_ms":800,
            "status":"cancelled","selectors":{"quality":quality},
            "measurements":[{
                "role":"output_image","unit":"token","quantity":"2",
                "measure":"billed","source":"vendor_reply"
            }]
        });
        let usage = serde_json::from_value(wire).unwrap();
        let (request, facts) = crate::media::map_record(&usage).unwrap().unwrap();
        let tx = conn.transaction().unwrap();
        let event = crate::ingest::record_media_event_in_tx(&tx, &request, &facts, 900).unwrap();
        tx.commit().unwrap();

        reprice_event(
            &mut conn,
            &event.event_id,
            &catalog,
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();

        let (charge, reason, key): (Option<i64>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT charge_nanos,unpriced_reason,json_extract(catalog_entry,'$.fact_key')
                 FROM priced_charge JOIN usage_event USING (event_key) WHERE event_id=?1",
                [event.event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(charge, expected_charge, "quality {quality}");
        assert_eq!(reason.as_deref(), expected_reason);
        assert_eq!(key.unwrap(), format!("rate.output_image.quality.{quality}"));
    }
}

#[test]
fn catalog_regression_prices_newly_covered_history_after_status_poll() {
    let mut conn = conn();
    let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), rate(9_000_000))]));
    let catalog = CatalogClient::connect(fake.clone()).unwrap();
    assert_eq!(catalog.history_start_ms(), 500);

    let mut price = |name, at_ms| {
        let id = record(
            &mut conn,
            name,
            ChargeBasis::MeteredApi,
            Some(at_ms),
            vec![measurement("input", Some(2))],
        );
        reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    };
    price("before-refresh", 400);
    assert!(fake.requests.lock().unwrap().is_empty());

    fake.history_begins_at_ms.store(300, Ordering::SeqCst);
    catalog
        .with_budget(std::time::Duration::from_secs(1))
        .rates_version()
        .unwrap();
    price("after-refresh", 400);
    price("still-pre-history", 200);

    let dates: Vec<_> = fake
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.at_ms)
        .collect();
    assert_eq!(dates, [Some(400)]);
    let charges: Vec<(String, Option<i64>, Option<String>)> = conn
        .prepare(
            "SELECT producer_ref,charge_nanos,unpriced_reason
                  FROM priced_charge JOIN usage_event USING (event_key) ORDER BY charge_key",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        charges,
        [
            (
                "before-refresh".into(),
                None,
                Some("catalog_pre_history".into())
            ),
            ("after-refresh".into(), Some(18), None),
            (
                "still-pre-history".into(),
                None,
                Some("catalog_pre_history".into())
            ),
        ]
    );
}

#[test]
fn catalog_regression_token_rates_apply_stated_quantity_rules() {
    for (measured, increment, minimum, billed, nanos) in [
        (
            1,
            json!({"kind":"none"}),
            json!({"kind":"minimum","quantity":100}),
            100,
            450,
        ),
        (
            111,
            json!({"kind":"quantum","quantity":10}),
            json!({"kind":"none"}),
            120,
            540,
        ),
    ] {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "token-quantity-rule",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![measurement("input", Some(measured))],
        );
        let value = unit_billing_rate(None, increment, minimum);
        let catalog = client(FakeCatalog::new(BTreeMap::from([(
            "rate.input".into(),
            value,
        )])));

        reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();

        let (charge, entry) = unit_billing_stored(&conn);
        // The fixture rate is 4.5 million nanodollars per million tokens.
        assert_eq!(charge, Some(nanos));
        assert_eq!(
            entry["raw_quantity"],
            json!({"units":measured,"exponent":0})
        );
        assert_eq!(
            entry["priced_quantity"],
            json!({"units":billed,"exponent":0})
        );
        assert!(entry.get("per").is_none());
    }
}
