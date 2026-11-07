fn media_price_record(
    conn: &mut Connection,
    id: &str,
    selectors: Value,
    measure: &str,
    auth: Option<&str>,
) -> String {
    let wire = json!({"record_id":id,"principal":"person-a","job_id":"job-1","provider":"provider","model":"model","charge_basis":"vendor","auth_method":auth,"started_at_ms":700,"finished_at_ms":800,"status":"cancelled","selectors":selectors,"measurements":[{"role":"input_audio","unit":"second","quantity":"90","measure":measure,"source":"vendor_reply"}]});
    let record = serde_json::from_value(wire).unwrap();
    let (request, facts) = crate::media::map_record(&record).unwrap().unwrap();
    let tx = conn.transaction().unwrap();
    let outcome = crate::ingest::record_media_event_in_tx(&tx, &request, &facts, 900).unwrap();
    tx.commit().unwrap();
    outcome.event_id
}

#[test]
fn media_transcribe_prices_90_seconds_to_6750000_nanodollars_end_to_end() {
    let mut conn = conn();
    let id = media_price_record(
        &mut conn,
        "transcribe",
        json!({}),
        "billed",
        Some("api_key"),
    );
    let catalog = client(FakeCatalog::new(BTreeMap::from([(
        "rate.input_audio.per_second".into(),
        unit_billing_rate(Some(60), json!({"kind":"none"}), json!({"kind":"none"})),
    )])));
    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    assert_eq!(unit_billing_stored(&conn).0, Some(6_750_000));
    assert_eq!(
        unit_billing_stored(&conn).1["fact_key"],
        "rate.input_audio.per_second"
    );
    assert_eq!(
        conn.query_row("SELECT terminal_status FROM usage_event", [], |row| row
            .get::<_, String>(
            0
        ))
        .unwrap(),
        "cancelled"
    );
}

#[test]
fn media_observed_quantities_stay_stored_and_never_priced() {
    let mut conn = conn();
    let id = media_price_record(
        &mut conn,
        "observed",
        json!({}),
        "observed",
        Some("api_key"),
    );
    let catalog = client(FakeCatalog::new(BTreeMap::from([(
        "rate.input_audio.per_second".into(),
        unit_billing_rate(Some(60), json!({"kind":"none"}), json!({"kind":"none"})),
    )])));
    assert!(matches!(
        reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass),
        Err(PricingError::Refused(PricingRefusal::NoMeasurementsToPrice))
    ));
    assert_eq!(
        conn.query_row("SELECT count(*) FROM priced_charge", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        conn.query_row(
            "SELECT quantity_units FROM measurement WHERE kind='observed'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        90
    );
}

#[test]
fn media_stored_selectors_and_speed_mode_choose_only_exact_keyed_rates() {
    for (selectors, expected) in [
        (
            json!({"quality":"high","size":"large","speed_mode":"fast"}),
            Some(6_750_000),
        ),
        (
            json!({"quality":"unknown","size":"large","speed_mode":"fast"}),
            None,
        ),
        (json!({"speed_mode":"default"}), Some(13_500_000)),
    ] {
        let mut conn = conn();
        let id = media_price_record(
            &mut conn,
            "selected",
            selectors.clone(),
            "billed",
            Some("api_key"),
        );
        let exact = unit_billing_rate(Some(60), json!({"kind":"none"}), json!({"kind":"none"}));
        let mut base = exact.clone();
        base["units"] = json!(9_000_000);
        let key = "rate.input_audio.per_second.size.large.quality.high.mode.fast";
        let catalog = client(FakeCatalog::new(BTreeMap::from([
            (key.into(), exact),
            ("rate.input_audio.per_second".into(), base),
        ])));
        reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
        let (charge, entry) = unit_billing_stored(&conn);
        assert_eq!(charge, expected, "{selectors}");
        if expected == Some(6_750_000) {
            assert_eq!(entry["fact_key"], key);
        }
    }
}

#[test]
fn media_charge_basis_uses_only_vendor_auth_method() {
    for (auth, basis) in [
        (Some("api_key"), "metered_api"),
        (Some("oauth"), "subscription_included"),
        (None, "unknown"),
    ] {
        let mut conn = conn();
        media_price_record(&mut conn, "basis", json!({}), "billed", auth);
        assert_eq!(
            conn.query_row("SELECT charge_basis FROM usage_event", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            basis
        );
    }
}

#[test]
fn media_conflicting_mode_aliases_preserve_facts_and_price_neither_rate() {
    for (mode, speed_mode, conflict) in [
        ("slow", "fast", true),
        ("fast", "default", true),
        ("fast", "fast", false),
        ("default", "default", false),
    ] {
        let mut conn = conn();
        let selectors = json!({"mode":mode,"speed_mode":speed_mode});
        let record = serde_json::from_value(json!({
            "record_id":"mode-aliases", "principal":"person-a", "job_id":"job-1",
            "provider":"provider", "model":"model", "charge_basis":"vendor",
            "auth_method":"api_key", "started_at_ms":700,"finished_at_ms":800,
            "status":"done", "selectors":selectors,
            "measurements":[
                {"role":"input_audio","unit":"second","quantity":"90","measure":"billed","source":"vendor_reply"},
                {"role":"input","unit":"token","quantity":"90","measure":"billed","source":"vendor_reply"}
            ]
        })).unwrap();
        let (request, facts) = crate::media::map_record(&record).unwrap().unwrap();
        let tx = conn.transaction().unwrap();
        let id = crate::ingest::record_media_event_in_tx(&tx, &request, &facts, 900)
            .unwrap()
            .event_id;
        tx.commit().unwrap();
        let mut rates = BTreeMap::new();
        for suffix in ["", ".mode.fast", ".mode.slow"] {
            rates.insert(
                format!("rate.input_audio.per_second{suffix}"),
                unit_billing_rate(Some(60), json!({"kind":"none"}), json!({"kind":"none"})),
            );
            rates.insert(
                format!("rate.input{suffix}"),
                unit_billing_rate(None, json!({"kind":"none"}), json!({"kind":"none"})),
            );
        }
        reprice_event(
            &mut conn,
            &id,
            &client(FakeCatalog::new(rates)),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        let stored: String = conn
            .query_row("SELECT media_facts FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&stored).unwrap()["selectors"],
            selectors
        );
        let mut query = conn
            .prepare("SELECT outcome,unpriced_reason,charge_nanos,catalog_entry FROM priced_charge")
            .unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        if conflict {
            assert!(
                rows.iter().all(|row| row.0 == "unpriced"),
                "conflicting selectors priced a measurement: {rows:?}"
            );
        } else {
            let mut amounts = rows.iter().map(|row| row.2).collect::<Vec<_>>();
            amounts.sort_unstable();
            assert_eq!(amounts, [Some(405), Some(6_750_000)]);
        }
        for (outcome, reason, amount, entry) in rows {
            if conflict {
                assert_eq!(outcome, "unpriced", "{selectors}");
                assert_eq!(reason.as_deref(), Some("no_rate"));
                assert_eq!(amount, None);
                let entry: Value =
                    serde_json::from_str(&entry.expect("conflict is explained")).unwrap();
                assert_eq!(entry["state"], "selector_conflict");
                assert_eq!(entry["selectors"], selectors);
            } else {
                assert_eq!(outcome, "priced", "{selectors}");
                assert_eq!(reason, None);
                assert!(matches!(amount, Some(405 | 6_750_000)));
            }
        }
    }
}
