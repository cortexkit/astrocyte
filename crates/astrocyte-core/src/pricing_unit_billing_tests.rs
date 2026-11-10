fn unit_billing_rate(per: Option<u64>, increment: Value, minimum: Value) -> Value {
    let mut value = rate(4_500_000);
    value["increment"] = increment;
    value["minimum_quantity"] = minimum;
    if let Some(per) = per {
        value["per"] = json!(per);
    }
    value
}

fn unit_billing_measurement(role: &str, unit: &str, quantity: i64) -> MeasurementInput {
    let mut result = measurement(role, Some(quantity));
    result.unit = unit.into();
    result
}

fn unit_billing_stored(conn: &Connection) -> (Option<i64>, Value) {
    let (charge, entry): (Option<i64>, String) = conn
        .query_row(
            "SELECT charge_nanos,catalog_entry FROM priced_charge ORDER BY charge_key DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    (charge, serde_json::from_str(&entry).unwrap())
}

#[test]
fn unit_billing_refuses_missing_or_zero_denominator() {
    for per in [None, Some(0)] {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "bad",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![unit_billing_measurement("input_audio", "second", 90)],
        );
        let catalog = client(FakeCatalog::new(BTreeMap::from([(
            "rate.input_audio.per_second".into(),
            unit_billing_rate(per, json!({"kind":"unknown"}), json!({"kind":"unknown"})),
        )])));
        assert!(matches!(
            reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass),
            Err(PricingError::Catalog(
                CatalogClientError::InvalidRate { .. }
            ))
        ));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM priced_charge", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn unit_billing_refuses_denominator_on_token_rate() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "bad",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![measurement("input", Some(90))],
    );
    let mut value = rate(4_500_000);
    value["per"] = json!(60);
    let catalog = client(FakeCatalog::new(BTreeMap::from([(
        "rate.input".into(),
        value,
    )])));
    assert!(matches!(
        reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass),
        Err(PricingError::Catalog(
            CatalogClientError::InvalidRate { .. }
        ))
    ));
    assert_eq!(
        conn.query_row("SELECT count(*) FROM priced_charge", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn unit_billing_rounding_case(
    increment: Value,
    minimum: Value,
    quantity: i64,
    exponent: i32,
    expected_quantity: Value,
    expected_nanos: i64,
) {
    let mut conn = conn();
    let mut measured = unit_billing_measurement("input_audio", "second", quantity);
    measured.quantity_exponent = Some(exponent);
    let id = record(
        &mut conn,
        "audio",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![measured],
    );
    let catalog = client(FakeCatalog::new(BTreeMap::from([(
        "rate.input_audio.per_second".into(),
        unit_billing_rate(Some(60), increment.clone(), minimum.clone()),
    )])));
    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    let (charge, entry) = unit_billing_stored(&conn);
    assert_eq!(charge, Some(expected_nanos));
    assert_eq!(entry["fact_key"], "rate.input_audio.per_second");
    assert_eq!(entry["per"], 60);
    assert_eq!(entry["increment"], increment);
    assert_eq!(entry["minimum_quantity"], minimum);
    assert_eq!(
        entry["raw_quantity"],
        json!({"units":quantity,"exponent":exponent})
    );
    assert_eq!(entry["priced_quantity"], expected_quantity);
}

#[test]
fn unit_billing_quantum_rounds_quantity_up() {
    unit_billing_rounding_case(
        json!({"kind":"quantum","quantity":60}),
        json!({"kind":"none"}),
        901,
        -1,
        json!({"units":120,"exponent":0}),
        9_000_000,
    );
}

#[test]
fn unit_billing_minimum_raises_quantity() {
    unit_billing_rounding_case(
        json!({"kind":"none"}),
        json!({"kind":"minimum","quantity":60}),
        1,
        0,
        json!({"units":60,"exponent":0}),
        4_500_000,
    );
    unit_billing_rounding_case(
        json!({"kind":"quantum","quantity":30}),
        json!({"kind":"minimum","quantity":120}),
        31,
        0,
        json!({"units":120,"exponent":0}),
        9_000_000,
    );
    unit_billing_rounding_case(
        json!({"kind":"quantum","quantity":60}),
        json!({"kind":"minimum","quantity":90}),
        1,
        0,
        json!({"units":90,"exponent":0}),
        6_750_000,
    );
}

#[test]
fn unit_billing_unknown_rounding_is_recorded_and_prices_raw_quantity() {
    unit_billing_rounding_case(
        json!({"kind":"unknown"}),
        json!({"kind":"unknown"}),
        90,
        0,
        json!({"units":90,"exponent":0}),
        6_750_000,
    );
    unit_billing_rounding_case(
        json!({"kind":"none"}),
        json!({"kind":"none"}),
        100,
        0,
        json!({"units":100,"exponent":0}),
        7_500_000,
    );
}

#[test]
fn unit_billing_selectors_match_exactly_without_base_fallback() {
    for quality in ["high", "unserved"] {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "image",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![unit_billing_measurement("output", "image", 2)],
        );
        let value = unit_billing_rate(Some(1), json!({"kind":"none"}), json!({"kind":"none"}));
        let catalog = client(FakeCatalog::new(BTreeMap::from([
            ("rate.output.per_image".into(), value.clone()),
            (
                "rate.output.per_image.quality.high.size.large".into(),
                value,
            ),
        ])));
        let mut plan = read_reprice(&conn, &id).unwrap();
        plan.targets[0].price_selectors = BTreeMap::from([
            ("quality".into(), quality.into()),
            ("size".into(), "large".into()),
        ]);
        let resolved = resolve_reprice(&plan, &catalog, &[]).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        let (charge, entry) = unit_billing_stored(&conn);
        if quality == "high" {
            assert_eq!(charge, Some(9_000_000));
            assert_eq!(
                entry["fact_key"],
                "rate.output.per_image.quality.high.size.large"
            );
        } else {
            assert_eq!(charge, None);
            assert_eq!(
                conn.query_row("SELECT unpriced_reason FROM priced_charge", [], |r| r
                    .get::<_, String>(0))
                    .unwrap(),
                "no_rate"
            );
        }
    }
}

#[test]
fn unit_billing_standard_mode_prices_at_the_base_unit_rate() {
    for (mode, expected_key) in [
        ("standard", "rate.input_audio.per_second"),
        ("fast", "rate.input_audio.per_second.mode.fast"),
    ] {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "audio",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![unit_billing_measurement("input_audio", "second", 60)],
        );
        let value = unit_billing_rate(Some(60), json!({"kind":"none"}), json!({"kind":"none"}));
        let catalog = client(FakeCatalog::new(BTreeMap::from([
            ("rate.input_audio.per_second".into(), value.clone()),
            ("rate.input_audio.per_second.mode.fast".into(), value),
        ])));
        let mut plan = read_reprice(&conn, &id).unwrap();
        plan.mode = Some(mode.into());
        let resolved = resolve_reprice(&plan, &catalog, &[]).unwrap();
        let tx = conn.transaction().unwrap();
        commit_reprice_in_tx(&tx, &plan, &resolved, 1_000, PricingOrigin::PricingPass).unwrap();
        tx.commit().unwrap();
        let (charge, entry) = unit_billing_stored(&conn);
        assert!(charge.is_some(), "{mode}");
        assert_eq!(entry["fact_key"], expected_key, "{mode}");
    }
}

#[test]
fn unit_billing_transcription_golden_decodes_and_prices_end_to_end() {
    use sha2::{Digest, Sha256};
    let bytes = include_bytes!("../test-vectors/fusiform-protocol-0.52.0/gpt-transcribe.json");
    assert_eq!(
        hex::encode(Sha256::digest(bytes)),
        "3dccd68c5ffead1e9182a553c2832847e3e904b789e8d499ecf3765efc29578c"
    );
    let fixture: Value = serde_json::from_slice(bytes).unwrap();
    let response: CatalogGetResponse = serde_json::from_value(fixture["response"].clone()).unwrap();
    let mut conn = conn();
    let request = serde_json::from_value::<EventRecordRequest>(json!({
        "producer":"producer", "producer_lineage":"lineage", "producer_ref":"audio",
        "provider":"openai", "model":"gpt-transcribe", "charge_basis":"metered_api",
        "occurred_at_ms":700,
        "measurements":[unit_billing_measurement("input_audio", "second", 90)],
    }))
    .unwrap();
    let id = record_event(&mut conn, &request, 900).unwrap().event_id;
    let fake = FakeCatalog::new(BTreeMap::new());
    *fake.response.lock().unwrap() = response;
    let catalog = client(fake.clone());
    reprice_event(&mut conn, &id, &catalog, 1_000, PricingOrigin::PricingPass).unwrap();
    let (charge, entry) = unit_billing_stored(&conn);
    assert_eq!(charge, Some(6_750_000));
    assert_eq!(entry["fact_key"], "rate.input_audio.per_second");
    assert_eq!(
        entry["value"],
        fixture["response"]["models"]["openai/gpt-transcribe"]["rate.input_audio.per_second"]
    );
    assert_eq!(entry["per"], 60);
    assert_eq!(entry["increment"], json!({"kind":"unknown"}));
    assert_eq!(entry["minimum_quantity"], json!({"kind":"unknown"}));
    assert_eq!(entry["raw_quantity"], json!({"units":90,"exponent":0}));
    assert_eq!(entry["priced_quantity"], entry["raw_quantity"]);
    let requests = fake.requests.lock().unwrap();
    assert!(requests.iter().all(|r| !r.paged && r.page_token.is_none()));
    assert!(requests
        .iter()
        .any(|r| r.provider_id.as_deref() == Some("openai")
            && r.model_id.as_deref() == Some("gpt-transcribe")));
}

#[test]
fn unit_billing_sourced_override_reaches_charge_provenance() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "override",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![measurement("input", Some(1_000_000))],
    );
    let value = rate(3_000_000_000);
    let override_entry = json!({
        "model":"provider/model", "provider_id":"provider", "model_id":"model",
        "fact_key":"rate.input", "value":value, "replaces":null,
        "reason":"Published price is missing upstream", "source_ref":"https://example.invalid/pricing",
        "quote":"Input costs three dollars per million tokens", "established_by":"catalog",
        "established_at_ms":600, "review_by_ms":900,
    });
    let fake = FakeCatalog::new(BTreeMap::from([("rate.input".into(), value)]));
    fake.response.lock().unwrap().fact_overrides =
        vec![serde_json::from_value(override_entry.clone()).unwrap()];
    reprice_event(
        &mut conn,
        &id,
        &client(fake),
        1_000,
        PricingOrigin::PricingPass,
    )
    .unwrap();
    let (charge, entry) = unit_billing_stored(&conn);
    assert_eq!(charge, Some(3_000_000_000));
    assert_eq!(entry["fact_override"], override_entry);
}

#[test]
fn unit_billing_matches_role_and_unit_and_keeps_token_amounts() {
    let mut conn = conn();
    let id = record(
        &mut conn,
        "mixed",
        ChargeBasis::MeteredApi,
        Some(700),
        vec![
            measurement("input_audio", Some(1_000_000)),
            measurement("output_audio", Some(1_000_000)),
            measurement("cached_input_audio", Some(1_000_000)),
            unit_billing_measurement("input_audio", "second", 90),
            unit_billing_measurement("input", "character", 2),
            unit_billing_measurement("output", "image", 2),
            unit_billing_measurement("request", "request", 2),
        ],
    );
    let mut facts = BTreeMap::from([
        ("rate.input_audio".into(), rate(7_000_000_000)),
        ("rate.output_audio".into(), rate(8_000_000_000)),
        ("rate.cached_input_audio".into(), rate(9_000_000_000)),
    ]);
    for key in [
        "rate.input_audio.per_second",
        "rate.input.per_character",
        "rate.output.per_image",
        "rate.request.per_request",
    ] {
        facts.insert(
            key.into(),
            unit_billing_rate(Some(60), json!({"kind":"none"}), json!({"kind":"none"})),
        );
    }
    facts.insert(
        "rate.input.per_character.above_context.0".into(),
        unit_billing_rate(Some(1), json!({"kind":"none"}), json!({"kind":"none"})),
    );
    reprice_event(
        &mut conn,
        &id,
        &client(FakeCatalog::new(facts)),
        1_000,
        PricingOrigin::PricingPass,
    )
    .unwrap();
    let rows = conn
        .prepare("SELECT role,unit,charge_nanos FROM priced_charge ORDER BY role,unit")
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("cached_input_audio".into(), "token".into(), 9_000_000_000),
            ("input".into(), "character".into(), 150_000),
            ("input_audio".into(), "second".into(), 6_750_000),
            ("input_audio".into(), "token".into(), 7_000_000_000),
            ("output".into(), "image".into(), 150_000),
            ("output_audio".into(), "token".into(), 8_000_000_000),
            ("request".into(), "request".into(), 150_000),
        ]
    );
    let token_entry: String = conn
        .query_row(
            "SELECT catalog_entry FROM priced_charge WHERE role='input_audio' AND unit='token'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        token_entry,
        serde_json::to_string(&json!({"fact_key":"rate.input_audio",
        "value":rate(7_000_000_000), "overridden":null}))
        .unwrap()
    );
}

#[test]
fn unit_billing_fractional_nanos_use_half_even_once() {
    for (per, quantity, expected) in [(2, 1, 0), (2, 3, 2), (2, 5, 2), (3, 1, 0), (3, 2, 1)] {
        let mut conn = conn();
        let id = record(
            &mut conn,
            "tiny",
            ChargeBasis::MeteredApi,
            Some(700),
            vec![unit_billing_measurement("request", "request", quantity)],
        );
        let mut value =
            unit_billing_rate(Some(per), json!({"kind":"none"}), json!({"kind":"none"}));
        value["units"] = json!(1);
        reprice_event(
            &mut conn,
            &id,
            &client(FakeCatalog::new(BTreeMap::from([(
                "rate.request.per_request".into(),
                value,
            )]))),
            1_000,
            PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(
            unit_billing_stored(&conn).0,
            Some(expected),
            "per={per} quantity={quantity}"
        );
    }
}

/// Fusiform's served reply for a model whose cache writes are priced per
/// lifetime, checked by digest so the pinned bytes can't drift.
fn cache_lifetime_vector(name: &str, bytes: &[u8], sha256: &str) -> CatalogGetResponse {
    use sha2::{Digest, Sha256};
    assert_eq!(hex::encode(Sha256::digest(bytes)), sha256, "{name}");
    let fixture: Value = serde_json::from_slice(bytes).unwrap();
    serde_json::from_value(fixture["response"].clone()).unwrap()
}

fn record_cache_writes(conn: &mut Connection, provider: &str, model: &str, units: &[&str]) -> String {
    let measurements: Vec<Value> = units
        .iter()
        .map(|unit| {
            json!({"role":"cache_write", "unit":unit, "kind":"billed", "source":"test",
                "quantity_units":1_000_000, "quantity_exponent":0})
        })
        .collect();
    let request = serde_json::from_value::<EventRecordRequest>(json!({
        "producer":"producer", "producer_lineage":"lineage", "producer_ref":model,
        "provider":provider, "model":model, "charge_basis":"metered_api",
        "occurred_at_ms":700, "measurements":measurements,
    }))
    .unwrap();
    record_event(conn, &request, 900).unwrap().event_id
}

fn charges_by_unit(conn: &Connection) -> Vec<(String, Option<i64>, Option<String>)> {
    conn.prepare("SELECT unit, charge_nanos, unpriced_reason FROM priced_charge ORDER BY unit")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// Claude Opus 5.5 as fusiform serves it: 5-minute writes at $5 and 1-hour
/// writes at $8 per million tokens. A write without a stated lifetime keeps
/// the plain rate, and each split write is priced at its own lifetime.
#[test]
fn cache_lifetime_golden_prices_each_anthropic_write_at_its_lifetime() {
    let response = cache_lifetime_vector(
        "claude-opus-5-5",
        include_bytes!("../test-vectors/fusiform-protocol-0.54.0/claude-opus-5-5-cache-lifetimes.json"),
        "1c6d423bc6821633b404e6cdd39f77a864064359a3b4829033c74b0abfb2e61b",
    );
    let mut conn = conn();
    let id = record_cache_writes(
        &mut conn,
        "anthropic",
        "claude-opus-5-5",
        &["token", "token_cache_5m", "token_cache_1h"],
    );
    let fake = FakeCatalog::new(BTreeMap::new());
    *fake.response.lock().unwrap() = response;
    reprice_event(&mut conn, &id, &client(fake), 1_000, PricingOrigin::PricingPass).unwrap();
    assert_eq!(
        charges_by_unit(&conn),
        vec![
            ("token".into(), Some(5_000_000_000), None),
            ("token_cache_1h".into(), Some(8_000_000_000), None),
            ("token_cache_5m".into(), Some(5_000_000_000), None),
        ]
    );
}

/// GPT-6.1 Sol as fusiform serves it carries lifetime prices on its
/// above-272k tier too (`rate.cache_write.above_context.272000.ttl.12h`).
/// The reply must decode, and the ledger still refuses a tiered model's
/// event whole rather than choosing a tier.
#[test]
fn cache_lifetime_golden_tiered_openai_reply_decodes_and_refuses_as_tiered() {
    let response = cache_lifetime_vector(
        "gpt-6.1-sol",
        include_bytes!("../test-vectors/fusiform-protocol-0.54.0/gpt-6.1-sol-cache-lifetimes.json"),
        "c302a523f188ac031c84abff027a119845393357d76c5add979564afc7c827a5",
    );
    let mut conn = conn();
    let id = record_cache_writes(
        &mut conn,
        "openai",
        "gpt-6.1-sol",
        &["token_cache_30m", "token_cache_12h"],
    );
    let fake = FakeCatalog::new(BTreeMap::new());
    *fake.response.lock().unwrap() = response;
    reprice_event(&mut conn, &id, &client(fake), 1_000, PricingOrigin::PricingPass).unwrap();
    assert_eq!(
        charges_by_unit(&conn),
        vec![
            ("token_cache_12h".into(), None, Some("rate_tiered".into())),
            ("token_cache_30m".into(), None, Some("rate_tiered".into())),
        ]
    );
}

/// Every cache-write lifetime is part of the prompt a context tier is tested
/// against. A request with 200k input tokens and a 100k-token 12-hour cache
/// write sends a 300k prompt, so it crosses a 272k tier.
#[test]
fn every_cache_write_lifetime_counts_toward_the_prompt() {
    for unit in ["token_cache_5m", "token_cache_30m", "token_cache_1h", "token_cache_12h", "token_cache_24h"] {
        let prompt = prompt_size([
            ("input", "token", Some(200_000), Some(0)),
            ("cache_write", unit, Some(100_000), Some(0)),
        ])
        .unwrap();
        assert!(prompt.exceeds(272_000), "{unit}");
    }
}

/// Records one call of `provider`/`model` with the given (role, unit,
/// quantity) measurements, prices it against `response`, and returns each
/// charge as (role, unit, charge in nanodollars, unpriced reason).
fn price_against_vector(
    response: CatalogGetResponse,
    provider: &str,
    model: &str,
    measurements: &[(&str, &str, i64)],
) -> Vec<(String, String, Option<i64>, Option<String>)> {
    let mut conn = conn();
    let measurements: Vec<Value> = measurements
        .iter()
        .map(|(role, unit, quantity)| {
            json!({"role":role, "unit":unit, "kind":"billed", "source":"test",
                "quantity_units":quantity, "quantity_exponent":0})
        })
        .collect();
    let request = serde_json::from_value::<EventRecordRequest>(json!({
        "producer":"producer", "producer_lineage":"lineage", "producer_ref":model,
        "provider":provider, "model":model, "charge_basis":"metered_api",
        "occurred_at_ms":700, "measurements":measurements,
    }))
    .unwrap();
    let id = record_event(&mut conn, &request, 900).unwrap().event_id;
    let fake = FakeCatalog::new(BTreeMap::new());
    *fake.response.lock().unwrap() = response;
    if let Err(error) = reprice_event(&mut conn, &id, &client(fake), 1_000, PricingOrigin::PricingPass) {
        panic!("pricing {provider}/{model}: {error:?}");
    }
    let mut statement = conn
        .prepare(
            "SELECT role, unit, charge_nanos, unpriced_reason FROM priced_charge ORDER BY role, unit",
        )
        .unwrap();
    let charges = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    charges
}

/// Fusiform's media rows as it serves them from 066f43e
/// (test-vectors/fusiform-066f43e). Each reply must decode, and each new rate
/// role must price a measurement of that role from its own key. Prices are per
/// million tokens unless the key carries a denominator.
#[test]
fn media_rows_from_fusiform_066f43e_decode_and_price_each_role() {
    let row = |role: &str, unit: &str, nanos: i64| {
        (role.to_string(), unit.to_string(), Some(nanos), None::<String>)
    };
    let vector = |name: &str, bytes: &[u8], sha256: &str| cache_lifetime_vector(name, bytes, sha256);

    // $0.04 per 1,000 characters.
    let eleven = vector(
        "eleven-flash-tts",
        include_bytes!("../test-vectors/fusiform-066f43e/eleven-flash-tts.json"),
        "d52ec0b31abdcca1d28ef07980d14d725af6d6ca8ef5a47e394e9571a16096a9",
    );
    assert_eq!(
        price_against_vector(eleven, "elevenlabs", "eleven_flash_v2_5", &[("input", "character", 1_000)]),
        [row("input", "character", 40_000_000)]
    );

    // Image input $8/M and image output $30/M, apart from text input.
    let flare = vector(
        "gpt-image-flare",
        include_bytes!("../test-vectors/fusiform-066f43e/gpt-image-flare.json"),
        "d6ec584db238bf8bed29617d3c40d8b6478c8e6acb39bac46e42580efca50d84",
    );
    assert_eq!(
        price_against_vector(
            flare,
            "openai",
            "gpt-image-2.6-flare",
            &[("input_image", "token", 1_000_000), ("output_image", "token", 1_000_000)]
        ),
        [row("input_image", "token", 8_000_000_000), row("output_image", "token", 30_000_000_000)]
    );

    // Text output $3/M and image output $60/M are separate roles.
    let flash_image = vector(
        "gemini-flash-image-split",
        include_bytes!("../test-vectors/fusiform-066f43e/gemini-flash-image-split.json"),
        "f680e3324ac32fa280466afbc02b96847f8d7c5be477d857b9a0534c4cc01642",
    );
    assert_eq!(
        price_against_vector(
            flash_image,
            "google",
            "gemini-3.1-flash-image",
            &[("output", "token", 1_000_000), ("output_image", "token", 1_000_000)]
        ),
        [row("output", "token", 3_000_000_000), row("output_image", "token", 60_000_000_000)]
    );

    // The Antigravity alias inherits those rates, which the ledger never
    // charges as its own, so each role stays unpriced as inherited.
    let alias = vector(
        "gemini-flash-image-alias-split",
        include_bytes!("../test-vectors/fusiform-066f43e/gemini-flash-image-alias-split.json"),
        "bc4a7259696c521296ee50803ef5615be5129625718ebcccd7e4cf6a33ab13cb",
    );
    let alias_charges = price_against_vector(
        alias,
        "google",
        "antigravity-gemini-3.1-flash-image",
        &[("output", "token", 1_000_000), ("output_image", "token", 1_000_000)],
    );
    assert!(
        alias_charges
            .iter()
            .all(|(_, _, nanos, reason)| nanos.is_none() && reason.as_deref() == Some("rate_inherited")),
        "{alias_charges:?}"
    );

    // Audio output $9/M.
    let tts = vector(
        "gemini-flash-tts",
        include_bytes!("../test-vectors/fusiform-066f43e/gemini-flash-tts.json"),
        "54c4642866c4790b134c87b49037b5cebeb622b7353c55b66a2d2b39301d585a",
    );
    assert_eq!(
        price_against_vector(tts, "google", "gemini-3.8-flash-tts", &[("output_audio", "token", 1_000_000)]),
        [row("output_audio", "token", 9_000_000_000)]
    );

    // Video output $12/M; the per-resolution limit facts don't affect pricing.
    let omni = vector(
        "gemini-omni-video",
        include_bytes!("../test-vectors/fusiform-066f43e/gemini-omni-video.json"),
        "4b3ea9c6ddf01056975496f61d097db506bb9111f431bae2e9cfa86cd1fc4df3",
    );
    assert_eq!(
        price_against_vector(omni, "google", "gemini-omni-1.2-flash", &[("output_video", "token", 1_000_000)]),
        [row("output_video", "token", 12_000_000_000)]
    );
}
