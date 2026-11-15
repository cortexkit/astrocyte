fn shared_event(measurements: &[(&str, &str, i64)]) -> EventTokens {
    EventTokens {
        producer: "codex".into(),
        provider: "openai".into(),
        model: Some("m".into()),
        mode: None,
        roles: measurements
            .iter()
            .map(|(role, unit, quantity)| {
                (
                    role.to_string(),
                    unit.to_string(),
                    "usage".into(),
                    Some(*quantity),
                    Some(0),
                )
            })
            .collect(),
    }
}

#[test]
fn shared_valuer_unvalued_tokens_count_only_token_units() {
    use crate::valuation::{list_value_event, UnvaluedReason};
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    let event = shared_event(&[
        ("input", "token", 7),
        ("cache_write", "token_cache_5m", 11),
        ("cache_write", "token_cache_1h", 13),
        ("input", "token_audio", 17),
        ("output", "second", 19),
        ("output", "character", 23),
        ("output", "image", 29),
        ("output", "request", 31),
        ("reasoning", "token", 0),
        ("output", "request", 0),
    ]);
    let value = list_value_event(&rules, None, &event).unwrap();
    assert_eq!(value.numerator, 0);
    assert_eq!(value.unvalued.measurements, 8);
    assert_eq!(value.unvalued.tokens, 48);
    assert_eq!(
        value.unvalued.by_reason,
        BTreeMap::from([(UnvaluedReason::NoRate, 8)])
    );
}

#[test]
fn shared_valuer_maps_legacy_reasons_only_in_list_adapter() {
    use crate::valuation::{list_value_event, UnvaluedReason};
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    let event = shared_event(&[
        ("input", "token", 7),
        ("cache_write", "token_cache_5m", 11),
        ("cache_write", "token_cache_1h", 13),
    ]);
    for (reason, expected) in [
        ("refused", UnvaluedReason::Refused),
        ("rate_withheld", UnvaluedReason::Refused),
        ("rate_uncertain", UnvaluedReason::Refused),
        ("non_usd", UnvaluedReason::NonUsd),
        ("missing_model", UnvaluedReason::MissingModel),
        ("valuation_error", UnvaluedReason::ValuationError),
        ("unit_not_priced", UnvaluedReason::NoRate),
        ("rate_not_resolved", UnvaluedReason::NoRate),
        ("rate_tiered", UnvaluedReason::NoRate),
        ("no_catalog_coverage", UnvaluedReason::NoRate),
    ] {
        let resolution = ModelResolution::Unpriceable {
            reason: reason.into(),
        };
        let detailed = crate::valuation::value_event(&rules, Some(&resolution), &event).unwrap();
        assert_eq!(detailed.unpriced.len(), 3);
        assert!(detailed
            .unpriced
            .iter()
            .all(|diagnostic| diagnostic.reason == reason));
        let value = list_value_event(&rules, Some(&resolution), &event).unwrap();
        assert_eq!(value.unvalued.measurements, 3, "{reason}");
        assert_eq!(value.unvalued.tokens, 31, "{reason}");
        assert_eq!(
            value.unvalued.by_reason,
            BTreeMap::from([(expected, 3)]),
            "{reason}"
        );
    }
    let mut missing_model = event;
    missing_model.model = None;
    let value = list_value_event(&rules, None, &missing_model).unwrap();
    assert_eq!(
        value.unvalued.by_reason,
        BTreeMap::from([(UnvaluedReason::MissingModel, 3)])
    );
}

#[test]
fn shared_valuer_counts_each_unpriced_measurement_and_keeps_priced_value() {
    use crate::valuation::{list_value_event, UnvaluedReason};
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    let resolution = ModelResolution::Rates(ModelRates {
        base: BTreeMap::from([
            ("input".into(), priced(3)),
            (
                "output".into(),
                RateState::Unpriced {
                    reason: "non_usd".into(),
                },
            ),
        ]),
        ..ModelRates::default()
    });
    let event = shared_event(&[
        ("input", "token", 7),
        ("output", "token", 11),
        ("cache_write", "token_cache_5m", 13),
        ("cache_write", "token_cache_1h", 17),
        ("reasoning", "token", 0),
    ]);
    let value = list_value_event(&rules, Some(&resolution), &event).unwrap();
    assert_eq!(value.numerator, 21);
    assert_eq!(value.unvalued.measurements, 3);
    assert_eq!(value.unvalued.tokens, 41);
    assert_eq!(
        value.unvalued.by_reason,
        BTreeMap::from([(UnvaluedReason::NoRate, 2), (UnvaluedReason::NonUsd, 1)])
    );
}

#[test]
fn shared_valuer_repeated_role_measurements_are_counted_once_each() {
    use crate::valuation::{list_value_event, UnvaluedReason};
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    let event = shared_event(&[
        ("output", "token", 7),
        ("output", "token", 11),
        ("output", "image", 13),
    ]);
    let value = list_value_event(&rules, None, &event).unwrap();
    assert_eq!(value.unvalued.measurements, 3);
    assert_eq!(value.unvalued.tokens, 18);
    assert_eq!(
        value.unvalued.by_reason,
        BTreeMap::from([(UnvaluedReason::NoRate, 3)])
    );
}

#[test]
fn shared_valuer_event_error_discards_value_and_counts_positive_measurements() {
    use crate::valuation::{list_value_event, UnvaluedReason};
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    // Inject an arithmetic error with negative lifetime-split write quantities.
    // The image still counts as a failed measurement, never as failed tokens.
    let event = shared_event(&[
        ("input", "token", 7),
        ("cache_write", "token_cache_5m", -i64::MAX),
        ("cache_write", "token_cache_1h", -2),
        ("output", "image", 13),
        ("reasoning", "token", 0),
    ]);
    let value = list_value_event(&rules, None, &event).unwrap();
    assert_eq!(value.numerator, 0);
    assert_eq!(value.base_rate_tier_untested_tokens, 0);
    assert_eq!(value.unvalued.measurements, 2);
    assert_eq!(value.unvalued.tokens, 7);
    assert_eq!(
        value.unvalued.by_reason,
        BTreeMap::from([(UnvaluedReason::ValuationError, 2)])
    );
}

#[test]
fn shared_valuer_bundles_keep_base_value_and_untested_counter() {
    use crate::valuation::list_value_event;
    let config = config(vec![]);
    let rules = ValuationRules::from_config(&config, &[]);
    let rates = rates_for(&[("openai", "m")], 3, Some((100, 9)));
    let resolution = rates.models.get(&("openai".into(), "m".into()));
    for producer in ["broca", "goose"] {
        let mut event = shared_event(&[("input", "token", 101), ("output", "token", 11)]);
        event.producer = producer.into();
        let value = list_value_event(&rules, resolution, &event).unwrap();
        assert_eq!(value.numerator, 336);
        assert_eq!(value.base_rate_tier_untested_tokens, 112);
        assert_eq!(value.unvalued.measurements, 0);
    }
}

#[test]
fn shared_valuer_randomized_stored_daily_numerator_matches_legacy() {
    // A fixed seed makes the randomized differential reproducible without
    // depending on an external RNG. Rates are constant throughout each day.
    let mut seed = 0x51a7_ed19_6014_771d_u64;
    let mut random = |bound: u64| {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        i64::try_from((seed >> 16) % bound).unwrap()
    };
    let mut config = config(vec![
        rule("pi", "openai", None),
        rule("codex", "openai", None),
        rule("broca", "openai", None),
        rule("goose", "openai", None),
        rule("opencode", "twin", None),
    ]);
    config.api_twins.insert("twin".into(), "openai".into());
    let day_ms = 24 * HOUR_MS;
    for day in 0..12 {
        let mut conn = conn();
        let start = day * day_ms;
        let end = start + day_ms;
        for minute in (0..=24 * 60).step_by(5) {
            reset_reading(
                &conn,
                "acct",
                start + minute * 60_000,
                f64::from(i32::try_from(minute).unwrap()) / 60.0,
                end + 7 * day_ms,
            );
        }
        let rate = 1 + random(1_000_000_000);
        let mut rates = rates_for(
            &[("openai", "m"), ("openai", "alias-high")],
            rate,
            Some((100, rate * 3)),
        );
        for resolution in rates.models.values_mut() {
            let ModelResolution::Rates(model) = resolution else {
                unreachable!()
            };
            model.base.insert("reasoning".into(), priced(rate * 2));
            model.reference_model = Some(CatalogModel {
                provider_id: "openai".into(),
                model_id: "public-card".into(),
            });
            model.modes.insert(
                "fast".into(),
                model
                    .base
                    .keys()
                    .map(|role| (role.clone(), priced(rate * 5)))
                    .collect(),
            );
            for (ttl, ttl_rate) in [("5m", rate * 2), ("1h", rate * 4)] {
                model
                    .conditional
                    .insert(format!("rate.cache_write.ttl.{ttl}"), priced(ttl_rate));
                model.conditional.insert(
                    format!("rate.cache_write.above_context.100.ttl.{ttl}"),
                    priced(ttl_rate * 3),
                );
                model.conditional.insert(
                    format!("rate.cache_write.mode.fast.ttl.{ttl}"),
                    priced(ttl_rate * 5),
                );
                model.conditional.insert(
                    format!("rate.cache_write.above_context.100.mode.fast.ttl.{ttl}"),
                    priced(ttl_rate * 7),
                );
            }
            for role in ["input", "cache_read", "output", "reasoning"] {
                model.conditional.insert(
                    format!("rate.{role}.above_context.100.mode.fast"),
                    priced(rate * 7),
                );
            }
        }
        for hour in 0..24 {
            let producer =
                ["pi", "codex", "broca", "goose", "opencode"][usize::try_from(random(5)).unwrap()];
            let provider = if producer == "opencode" {
                "twin"
            } else {
                "openai"
            };
            let model = if random(2) == 0 { "m" } else { "alias-high" };
            let id = format!("random-{day}-{hour}");
            let basis = ["metered_api", "subscription_included", "unknown"]
                [usize::try_from(random(3)).unwrap()];
            conn.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES (?1,?2,'lineage',?1,?3,?3,?4,?5,?6,'acct')", params![id, producer, start + hour * HOUR_MS + HOUR_MS / 2, provider, model, basis]).unwrap();
            for (role, quantity) in [
                ("input", 1 + random(200)),
                ("cache_read", random(40)),
                ("output", 100 + random(100)),
                ("reasoning", random(40)),
            ] {
                conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token','billed','usage',?3,0)", params![id, role, quantity]).unwrap();
            }
            for unit in ["token_cache_5m", "token_cache_1h"] {
                conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'cache_write',?2,'billed','usage',?3,0)", params![id, unit, random(40)]).unwrap();
            }
        }
        let mut plan = plan_observation(&conn, &config, "codex", start, end).unwrap();
        // Stored events currently have no mode. Exercise the same owned planning
        // snapshot with recorded modes before sending it through both valuers.
        for event in &mut plan.events {
            event.tokens.mode = match random(3) {
                0 => None,
                1 => Some("standard".into()),
                _ => Some("fast".into()),
            };
        }
        let lookup = ModelLookup::new(&rates);
        let reasoning = ["openai".into()];
        let rules = LegacyValuationRules {
            api_twins: &config.api_twins,
            reasoning_billed_as_output: &reasoning,
        };
        let mut legacy_lines = BTreeMap::new();
        let mut legacy_unpriced = BTreeMap::new();
        let mut legacy_tokens = RoleTokens::default();
        for scoped in &plan.events {
            let event = &scoped.tokens;
            let resolution = event
                .model
                .as_deref()
                .and_then(|model| lookup.get(rules.catalog_provider(&event.provider), model));
            legacy_value_event(
                &rules,
                resolution,
                event,
                &mut legacy_lines,
                &mut legacy_unpriced,
                &mut legacy_tokens,
            )
            .unwrap();
        }
        assert!(
            legacy_unpriced.is_empty(),
            "fixture must contain only fully rateable calls"
        );
        let expected_numerator: i128 = legacy_lines.values().map(|line| line.numerator).sum();
        let row = compute_plan_observations(
            &conn,
            &config,
            &plan,
            &rates,
            &priced_plan(),
            end,
            &reasoning,
        )
        .unwrap()
        .remove(0);
        assert!((row.clean_hours_share - 1.0).abs() < f64::EPSILON);
        assert_eq!(row.api_nanos, round_nano_num(expected_numerator).unwrap());
        assert_eq!(row.tokens, legacy_tokens);
        let tx = conn.transaction().unwrap();
        insert_observation_in_tx(&tx, &row).unwrap();
        let id = tx.last_insert_rowid();
        tx.commit().unwrap();
        let stored: String = conn
            .query_row(
                "SELECT accounts_included FROM multiplier_observation WHERE observation_id=?1",
                [id],
                |row| row.get(0),
            )
            .unwrap();
        let stored: Value = serde_json::from_str(&stored).unwrap();
        let stored_numerator: i128 = stored[0]["plan_calibration"]["bins"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bin| {
                bin["api_numerator"]
                    .as_str()
                    .unwrap()
                    .parse::<i128>()
                    .unwrap()
            })
            .sum();
        assert_eq!(stored_numerator, expected_numerator, "day {day}");
        assert_eq!(row.api_nanos_by_model.len(), legacy_lines.len());
        for (actual, expected) in row.api_nanos_by_model.iter().zip(legacy_lines.values()) {
            assert_eq!(actual.numerator, expected.numerator);
            assert_eq!(actual.measured_tokens, expected.measured_tokens);
            assert_eq!(
                actual.inclusive_output_adjustment,
                expected.inclusive_output_adjustment
            );
            assert_eq!(
                actual.base_rate_tier_untested_tokens,
                expected.base_rate_tier_untested_tokens
            );
            assert_eq!(actual.tiered_tokens, expected.tiered_tokens);
            assert_eq!(
                actual.mode_unrecorded_tokens,
                expected.mode_unrecorded_tokens
            );
        }
    }
}
