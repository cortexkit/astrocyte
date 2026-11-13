#[test]
fn media_token_roles_price_only_from_their_own_per_million_keys() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "media-tokens",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![
            measurement("input_image", Some(250_000)),
            measurement("output_image", Some(500_000)),
            measurement("output_video", Some(2_000_000)),
        ],
    );
    let catalog = client(FakeCatalog::new(BTreeMap::from([
        ("rate.input".into(), rate(1_000_000_000)),
        ("rate.output".into(), rate(2_000_000_000)),
        ("rate.input_image".into(), rate(7_000_000_000)),
        ("rate.output_image".into(), rate(8_000_000_000)),
        ("rate.output_video".into(), rate(9_000_000_000)),
    ])));
    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    let rows = conn
        .prepare(
            "SELECT role,unit,outcome,charge_nanos,json_extract(catalog_entry,'$.fact_key')
             FROM priced_charge ORDER BY role",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                "input_image".into(),
                "token".into(),
                "priced".into(),
                1_750_000_000,
                "rate.input_image".into(),
            ),
            (
                "output_image".into(),
                "token".into(),
                "priced".into(),
                4_000_000_000,
                "rate.output_image".into(),
            ),
            (
                "output_video".into(),
                "token".into(),
                "priced".into(),
                18_000_000_000,
                "rate.output_video".into(),
            ),
        ]
    );
}

#[test]
fn media_token_roles_without_own_keys_stay_no_rate_despite_text_rates() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "missing-media-token-rates",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![
            measurement("input_image", Some(1_000_000)),
            measurement("output_image", Some(1_000_000)),
            measurement("output_video", Some(1_000_000)),
        ],
    );
    let catalog = client(FakeCatalog::new(BTreeMap::from([
        ("rate.input".into(), rate(1_000_000_000)),
        ("rate.output".into(), rate(2_000_000_000)),
    ])));
    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    let rows = conn
        .prepare(
            "SELECT role,outcome,charge_nanos,unpriced_reason,catalog_entry
             FROM priced_charge ORDER BY role",
        )
        .unwrap()
        .query_map([], |row| {
            let entry: Option<String> = row.get(4)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, String>(3)?,
                entry.map(|entry| serde_json::from_str::<Value>(&entry).unwrap()),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                "input_image".into(),
                "unpriced".into(),
                None,
                "no_rate".into(),
                Some(json!({"fact_key":"rate.input_image", "state":"absent"})),
            ),
            (
                "output_image".into(),
                "unpriced".into(),
                None,
                "no_rate".into(),
                Some(json!({"fact_key":"rate.output_image", "state":"absent"})),
            ),
            (
                "output_video".into(),
                "unpriced".into(),
                None,
                "no_rate".into(),
                Some(json!({"fact_key":"rate.output_video", "state":"absent"})),
            ),
        ]
    );
}

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
fn observed_only_media_ingest_does_not_queue_pricing() {
    let mut conn = conn();
    let id = media_price_record(
        &mut conn,
        "observed-ingest",
        json!({}),
        "observed",
        Some("api_key"),
    );
    assert!(pending_event_ids(&conn, None, 10).unwrap().0.is_empty());
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM measurement WHERE kind='observed'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(enqueue_if_unpriced_in_tx(&conn, &id).unwrap(), 0);
}

#[test]
fn observed_only_media_reconciliation_does_not_queue_pricing() {
    let mut conn = conn();
    let id = media_price_record(
        &mut conn,
        "observed-reconcile",
        json!({}),
        "observed",
        Some("api_key"),
    );
    // Start with the queue empty to exercise reconciliation independently of ingest.
    conn.execute("DELETE FROM pricing_pending", []).unwrap();
    let tx = conn.transaction().unwrap();
    assert_eq!(
        reconcile_pending_range_in_tx(&tx, None, Some(&id)).unwrap(),
        0
    );
    tx.commit().unwrap();
    assert!(pending_event_ids(&conn, None, 10).unwrap().0.is_empty());
}

#[test]
fn observed_only_media_operator_reprice_still_refuses() {
    let mut conn = conn();
    let id = media_price_record(
        &mut conn,
        "observed-reprice",
        json!({}),
        "observed",
        Some("api_key"),
    );
    let catalog = client(FakeCatalog::new(BTreeMap::new()));
    assert!(matches!(
        reprice_event(
            &mut conn,
            &id,
            &catalog,
            1_000,
            PricingOrigin::OperatorReprice
        ),
        Err(PricingError::Refused(PricingRefusal::NoMeasurementsToPrice))
    ));
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
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

/// A text-to-speech job whose vendor bills input characters but reports no
/// billed count: the provider counts the text it sent and reports that count
/// as `request_derived` (media contract 0.1.6). It prices like any billed
/// quantity, while the vendor's own header, sent as an observed reading,
/// stays stored and unpriced.
#[test]
fn media_request_derived_characters_price_and_observed_credits_do_not() {
    let mut conn = conn();
    let wire = json!({"record_id":"speech","principal":"person-a","job_id":"job-1","provider":"provider","model":"model","charge_basis":"vendor","auth_method":"api_key","started_at_ms":700,"finished_at_ms":800,"status":"done","selectors":{},"measurements":[
        {"role":"input","unit":"character","quantity":"39","measure":"billed","source":"request_derived"},
        {"role":"input","unit":"credit","quantity":"20","measure":"observed","source":"vendor_reply"}
    ]});
    let record = serde_json::from_value(wire).unwrap();
    let (request, facts) = crate::media::map_record(&record).unwrap().unwrap();
    let tx = conn.transaction().unwrap();
    let id = crate::ingest::record_media_event_in_tx(&tx, &request, &facts, 900)
        .unwrap()
        .event_id;
    tx.commit().unwrap();
    // $0.04 per 1,000 characters.
    let mut per_character = rate(40_000_000);
    per_character["per"] = json!(1_000);
    per_character["increment"] = json!({"kind":"none"});
    per_character["minimum_quantity"] = json!({"kind":"none"});
    let catalog = client(FakeCatalog::new(BTreeMap::from([(
        "rate.input.per_character".into(),
        per_character,
    )])));
    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    let charges: Vec<(String, Option<i64>)> = conn
        .prepare("SELECT unit,charge_nanos FROM priced_charge ORDER BY unit")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(charges, vec![("character".into(), Some(1_560_000))]);
    let sources: Vec<(String, String, String)> = conn
        .prepare("SELECT unit,kind,source FROM measurement ORDER BY unit")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        sources,
        vec![
            ("character".into(), "billed".into(), "request_derived".into()),
            ("credit".into(), "observed".into(), "vendor_reply".into()),
        ]
    );
}

/// The contract bills a request-derived count only on a finished job, so a
/// failed job that claims one is refused as a record, never priced.
#[test]
fn media_request_derived_billing_on_a_failed_job_is_refused() {
    let wire = json!({"record_id":"speech","principal":"person-a","job_id":"job-1","provider":"provider","model":"model","charge_basis":"vendor","auth_method":"api_key","started_at_ms":700,"finished_at_ms":800,"status":"failed","failure":{"code":"engine_crashed","class":"transient","message":"stopped"},"selectors":{},"measurements":[
        {"role":"input","unit":"character","quantity":"39","measure":"billed","source":"request_derived"}
    ]});
    let record = serde_json::from_value(wire).unwrap();
    let error = crate::media::map_record(&record).unwrap_err();
    assert!(error.contains("status"), "{error}");
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
