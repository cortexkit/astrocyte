fn shared_event(measurements: &[(&str, &str, i64)]) -> EventTokens {
    EventTokens {
        producer: "codex".into(),
        provider: "openai".into(),
        model: Some("m".into()),
        mode: None,
        broca_attempt: false,
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
    let value = list_value_event(None, &event).unwrap();
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
        let detailed = crate::valuation::value_event(Some(&resolution), &event).unwrap();
        assert_eq!(detailed.unpriced.len(), 3);
        assert!(detailed
            .unpriced
            .iter()
            .all(|diagnostic| diagnostic.reason == reason));
        let value = list_value_event(Some(&resolution), &event).unwrap();
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
    let value = list_value_event(None, &missing_model).unwrap();
    assert_eq!(
        value.unvalued.by_reason,
        BTreeMap::from([(UnvaluedReason::MissingModel, 3)])
    );
}

#[test]
fn shared_valuer_counts_each_unpriced_measurement_and_keeps_priced_value() {
    use crate::valuation::{list_value_event, UnvaluedReason};
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
    let value = list_value_event(Some(&resolution), &event).unwrap();
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
    let event = shared_event(&[
        ("output", "token", 7),
        ("output", "token", 11),
        ("output", "image", 13),
    ]);
    let value = list_value_event(None, &event).unwrap();
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
    // Inject an arithmetic error with negative lifetime-split write quantities.
    // The image still counts as a failed measurement, never as failed tokens.
    let event = shared_event(&[
        ("input", "token", 7),
        ("cache_write", "token_cache_5m", -i64::MAX),
        ("cache_write", "token_cache_1h", -2),
        ("output", "image", 13),
        ("reasoning", "token", 0),
    ]);
    let value = list_value_event(None, &event).unwrap();
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
    let rates = rates_for(&[("openai", "m")], 3, Some((100, 9)));
    let resolution = rates.models.get(&("openai".into(), "m".into()));
    for producer in ["broca", "goose"] {
        let mut event = shared_event(&[("input", "token", 101), ("output", "token", 11)]);
        event.producer = producer.into();
        let value = list_value_event(resolution, &event).unwrap();
        assert_eq!(value.numerator, 336);
        assert_eq!(value.base_rate_tier_untested_tokens, 112);
        assert_eq!(value.unvalued.measurements, 0);
    }
}
