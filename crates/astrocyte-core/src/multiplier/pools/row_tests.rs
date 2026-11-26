use fusiform_protocol::{PlanAmount, PlanPriceWire};

use crate::multiplier::{
    insert_observation_in_tx, multiplier_report, ModelRates, ModelResolution,
    MultiplierReportRequest, Observation, RateState, ResolvedRates, HOUR_MS,
};

use super::call_tests::{call, capture_day, INCLUDED, MATERIAL};
use super::*;

const SNAPSHOT: i64 = END + 3 * DAY;
const STEP: i64 = HOUR_MS / 6;
const TWO: (PlanLedger, &str) = (PlanLedger::Account, "two");

fn prices(quota_pools: Vec<QuotaPoolWire>) -> PlanPricesResponse {
    let price = |dollars, at| PlanPriceWire {
        provider_id: CATALOG_PROVIDER.into(),
        tier: "max".into(),
        price: Some(PlanAmount {
            minor_units: dollars,
            exponent: 0,
            currency: "USD".into(),
            period: "month".into(),
        }),
        quota_value: None,
        service_tiers: None,
        concurrency: None,
        refusal_reason: None,
        boundary_at_ms: at,
        established_by: "fusiform".into(),
        established_at_ms: 0,
        review_by_ms: SNAPSHOT,
        source_ref: "https://example.invalid/prices".into(),
    };
    PlanPricesResponse {
        prices: vec![price(30, 0), price(60, END + 1)],
        resolved_at_ms: END,
        tier_vocabulary: "vendor".into(),
        unit_policy: "list".into(),
        quota_pools,
    }
}

fn rates() -> ResolvedRates {
    let rate = ModelResolution::Rates(ModelRates {
        base: BTreeMap::from([(
            "input".into(),
            RateState::Priced {
                units_per_million: 2_000_000_000,
            },
        )]),
        ..ModelRates::default()
    });
    ResolvedRates {
        resolved_at_ms: END,
        models: BTreeMap::from([
            ((CATALOG_PROVIDER.into(), "fable-low".into()), rate.clone()),
            ((CATALOG_PROVIDER.into(), "other".into()), rate),
        ]),
        ..ResolvedRates::default()
    }
}

fn rows(
    day: &PoolDayCapture,
    prices: &PlanPricesResponse,
    identities: &PoolModelIdentities,
) -> Vec<Observation> {
    let pools = plan_pools(&day.readings, &prices.quota_pools);
    compute_pool_observations(day, &pools, &rates(), identities, prices, SNAPSHOT).unwrap()
}

fn counter_sample(
    conn: &Connection,
    (ledger, login): (PlanLedger, &str),
    pool: &str,
    (at, used): (i64, i64),
    (minutes, scope): (Option<i64>, Option<&str>),
) {
    let (table, field) = match ledger {
        PlanLedger::Account => ("window_sample", "account_ref"),
        PlanLedger::Credential => ("credential_window_sample", "credential_ref"),
    };
    conn.execute(
        &format!(
            "INSERT INTO {table}(provider,{field},pool_key,fetched_at_instant,
                used_percent,window_minutes,model_scope,api_provider,resets_at)
                VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'1970-02-01T00:00:00Z')"
        ),
        params![
            PROVIDER,
            login,
            pool,
            at * NANOS_PER_MS,
            used,
            minutes,
            scope,
            CATALOG_PROVIDER
        ],
    )
    .unwrap();
}

fn counter(
    conn: &Connection,
    login: (PlanLedger, &str),
    pool: &str,
    scope: Option<&str>,
    slope: i64,
) {
    for step in 0..=12 {
        counter_sample(
            conn,
            login,
            pool,
            (START + step * STEP, 10 + step * slope),
            (WEEK, scope),
        );
    }
}

fn event(conn: &Connection, id: &str, model: &str, login: Option<(PlanLedger, &str)>, at: i64) {
    let (account, credential, selected) = match login {
        Some((PlanLedger::Account, id)) => (Some(id), None, None),
        // Ensure a resolved credential takes precedence over the selected one.
        Some((PlanLedger::Credential, id)) => (None, Some(id), Some(CREDENTIAL.1)),
        None => (None, None, None),
    };
    call(
        conn,
        id,
        (Some(CATALOG_PROVIDER), Some(model)),
        (account, credential, selected),
        (at, INCLUDED),
    );
}

fn persist_and_report(conn: &Connection, rows: &[Observation]) -> Vec<serde_json::Value> {
    let tx = conn.unchecked_transaction().unwrap();
    for row in rows {
        insert_observation_in_tx(&tx, row).unwrap();
    }
    tx.commit().unwrap();
    multiplier_report(
        conn,
        &MultiplierReportRequest {
            from_ms: START,
            to_ms: SNAPSHOT,
        },
    )
    .unwrap()["series"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn nested_pool_rows_match_only_logins_with_their_own_pool_readings() {
    let conn = fixture();
    vault_plan(&conn, TWO, Some(PLAN), 0);
    counter(&conn, ACCOUNT, "secondary", ALL, 1);
    counter(&conn, TWO, "secondary", ALL, 2);
    // B's Fable counter has only one reading; it cannot supply a pair.
    counter(&conn, ACCOUNT, FABLE, SUBSET, 1);
    counter_sample(&conn, TWO, FABLE, (START, 10), (WEEK, SUBSET));
    event(&conn, "a-fable", "fable-low", Some(ACCOUNT), START + 1);
    event(&conn, "b-fable", "fable-low", Some(TWO), START + 1);
    event(&conn, "b-other", "other", Some(TWO), START + 1);
    event(
        &conn,
        "b-unpriced",
        "unpriced",
        Some(TWO),
        START + HOUR_MS + 1,
    );
    // A's second hour is bad only in the all-models pool: the subset does
    // not cover this call. B has a listed model but lacks subset readings.
    event(
        &conn,
        "a-unpriced",
        "outside-subset",
        Some(ACCOUNT),
        START + HOUR_MS + 1,
    );
    let served = prices(vec![list(
        CATALOG_PROVIDER,
        "Fable",
        &["anthropic/fable", "anthropic/unpriced"],
    )]);
    let mut identities = PoolModelIdentities::default();
    identities.record(
        CATALOG_PROVIDER,
        "fable-low",
        CATALOG_PROVIDER,
        "fable",
        true,
    );
    identities.record(
        CATALOG_PROVIDER,
        "unpriced",
        CATALOG_PROVIDER,
        "unpriced",
        false,
    );
    let observations = rows(&capture_day(&conn), &served, &identities);
    let report = persist_and_report(&conn, &observations);
    assert_eq!(report.len(), 2);
    let subset = report
        .iter()
        .find(|row| row["window_pool_key"] == FABLE)
        .unwrap();
    let all = report
        .iter()
        .find(|row| row["window_pool_key"] == "secondary")
        .unwrap();
    assert_eq!(subset["pool_scope"], "subset");
    assert_eq!(subset["quota_provider"], "claude");
    assert_eq!(subset["catalog_provider"], "anthropic");
    assert_eq!(subset["pool_id"], "Fable");
    assert_eq!(
        subset["pool_models"],
        json!(["anthropic/fable", "anthropic/unpriced"])
    );
    assert_eq!(subset["pool_member_models"], json!(["anthropic/fable-low"]));
    assert_eq!(subset["pool_models_at_ms"], END);
    assert_eq!(subset["accounts_included_count"], 1);
    assert_eq!(subset["api_nanos"], 46_000_000);
    assert_eq!(subset["input_tokens"], MATERIAL);
    assert_eq!(subset["unpriced_tokens"], json!([]));
    assert_eq!(subset["unpriced_share"], json!(0.0));
    assert_eq!(subset["delta_percent"], json!(12.0));
    assert_eq!(subset["subscription_nanos"], 840_000_000);
    assert_eq!(subset["fee_paid_nanos"], 1_000_000_000);
    assert_eq!(all["quota_provider"], PROVIDER);
    assert_eq!(all["plan"], PLAN);
    assert_eq!(all["pool_scope"], "all");
    assert_eq!(all["accounts_included_count"], 2);
    assert_eq!(all["api_nanos"], 138_000_000);
    assert_eq!(all["input_tokens"], 3 * MATERIAL);
    assert_eq!(all["delta_percent"], json!(18.0));
    assert_eq!(all["accounts_included"][0]["delta_percent"], json!(6.0));
    assert_eq!(all["accounts_included"][1]["delta_percent"], json!(12.0));
    assert_eq!(all["unpriced_tokens"][0]["tokens"], MATERIAL);
    assert_eq!(all["unpriced_tokens"][1]["tokens"], MATERIAL);
    assert_eq!(all["unpriced_share"], json!(2.0 / 48.0));
    assert_eq!(all["fee_paid_nanos"], 2_000_000_000);
}

#[test]
fn credential_bad_hour_vetoes_only_its_attributed_counter() {
    let conn = fixture();
    let other = (PlanLedger::Credential, "other-credential");
    for (login, slope) in [(CREDENTIAL, 1), (other, 2)] {
        vault_plan(&conn, login, Some(PLAN), 0);
        counter(&conn, login, "secondary", ALL, slope);
    }
    event(&conn, "c1-priced", "other", Some(CREDENTIAL), START + 1);
    event(&conn, "c2-priced", "other", Some(other), START + 1);
    let served = prices(Vec::new());
    let identities = PoolModelIdentities::default();
    let before = rows(&capture_day(&conn), &served, &identities);
    assert_eq!(before[0].api_nanos, 92_000_000);
    event(&conn, "unattributed", "unpriced", None, START + 1);
    assert_eq!(rows(&capture_day(&conn), &served, &identities), before);

    event(
        &conn,
        "c1-unpriced",
        "unpriced",
        Some(CREDENTIAL),
        START + 1,
    );
    let after = rows(&capture_day(&conn), &served, &identities);
    let report = persist_and_report(&conn, &after);
    let row = &report[0];
    assert_eq!(row["delta_percent"], json!(30.0));
    assert_eq!(row["api_nanos"], 46_000_000);
    assert_eq!(row["subscription_nanos"], 2_100_000_000);
    assert_eq!(row["fee_paid_nanos"], 2_000_000_000);
    assert_eq!(row["fee_multiplier"], json!(0.046));
    assert_eq!(row["accounts_included"][0]["credential_ref"], CREDENTIAL.1);
    assert_eq!(row["accounts_included"][0]["delta_percent"], json!(6.0));
    assert_eq!(row["accounts_included"][1]["credential_ref"], other.1);
    assert_eq!(row["accounts_included"][1]["delta_percent"], json!(24.0));
    assert_eq!(row["clean_hours_share"], json!(3.0 / 48.0));
    assert_eq!(row["unpriced_share"], json!(1.0 / 48.0));
    assert_eq!(row["unpriced_tokens"][0]["tokens"], MATERIAL);
}

#[test]
fn unmeasured_pools_store_placeholders_even_with_rising_counters_and_unpriced_calls() {
    let conn = fixture();
    counter(&conn, ACCOUNT, "primary", ALL, 1);
    counter(&conn, ACCOUNT, "extra:8:Unstated", None, 1);
    counter(&conn, ACCOUNT, "extra:7:Missing", SUBSET, 1);
    for step in 0..=12 {
        let reading = (START + step * STEP, 10 + step);
        counter_sample(
            &conn,
            ACCOUNT,
            "extra:13:Scope-changed",
            reading,
            (WEEK, if step == 0 { ALL } else { SUBSET }),
        );
        counter_sample(
            &conn,
            ACCOUNT,
            FABLE,
            reading,
            (if step == 0 { SHORT } else { WEEK }, SUBSET),
        );
    }
    event(
        &conn,
        "material-unpriced",
        "unpriced",
        Some(ACCOUNT),
        START + 1,
    );
    event(&conn, "valued", "other", Some(ACCOUNT), START + HOUR_MS + 1);
    let models = ["anthropic/unpriced", "anthropic/other"];
    let served = prices(vec![list(CATALOG_PROVIDER, "Fable", &models)]);
    let mut identities = PoolModelIdentities::default();
    identities.record(
        CATALOG_PROVIDER,
        "unpriced",
        CATALOG_PROVIDER,
        "unpriced",
        false,
    );
    identities.record(CATALOG_PROVIDER, "other", CATALOG_PROVIDER, "other", false);
    let report = persist_and_report(&conn, &rows(&capture_day(&conn), &served, &identities));
    assert_eq!(report.len(), 5);
    let measured = report
        .iter()
        .find(|row| row["window_pool_key"] == "primary")
        .unwrap();
    assert_eq!(measured["delta_percent"], json!(6.0));
    assert_eq!(measured["subscription_nanos"], 420_000_000);
    assert_eq!(measured["unpriced_tokens"][0]["tokens"], MATERIAL);
    for (key, id, code, scope, pool_models) in [
        (
            "extra:8:Unstated",
            "Unstated",
            "scope_unstated",
            "unknown",
            None,
        ),
        (
            "extra:13:Scope-changed",
            "Scope-changed",
            "scope_changed",
            "unknown",
            None,
        ),
        (FABLE, "Fable", "window_changed", "subset", Some(models)),
        (
            "extra:7:Missing",
            "Missing",
            "pool_list_missing",
            "subset",
            None,
        ),
    ] {
        let actual = report
            .iter()
            .find(|row| row["window_pool_key"] == key)
            .unwrap();
        let mut expected = measured.clone();
        expected["observation_id"] = actual["observation_id"].clone();
        expected["window_pool_key"] = json!(key);
        expected["pool_id"] = json!(id);
        expected["pool_scope"] = json!(scope);
        expected["pool_models"] = json!(pool_models);
        expected["pool_models_at_ms"] = json!((scope == "subset").then_some(END));
        expected["pool_member_models"] = json!(pool_models.map(|_| Vec::<String>::new()));
        expected["eligibility_reasons"] = json!([code]);
        expected["low_resolution"] = json!(true);
        expected["routing_eligible"] = json!(false);
        for field in ["multiplier", "fee_multiplier", "stability_diagnostics"] {
            expected[field] = serde_json::Value::Null;
        }
        for field in [
            "input_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "output_tokens",
            "reasoning_tokens",
            "api_nanos",
            "tokens_outside_quota_coverage",
            "base_rate_tier_untested_tokens",
            "zero_measurement_events",
            "missing_quantity_events",
            "subscription_nanos",
            "fee_paid_nanos",
            "accounts_included_count",
            "unknown_quantity_measurements",
        ] {
            expected[field] = json!(0);
        }
        for field in [
            "clean_hours_share",
            "saturated_share",
            "gap_share",
            "unpriced_share",
            "unattributed_event_share",
            "delta_percent",
        ] {
            expected[field] = json!(0.0);
        }
        for field in [
            "api_nanos_by_model",
            "unpriced_tokens",
            "accounts_included",
            "accounts_excluded",
            "excluded_pairs",
        ] {
            expected[field] = json!([]);
        }
        assert_eq!(actual, &expected, "{code}");
        assert_eq!(actual["scope_rules"], json!([]));
        assert_eq!(actual["api_twins"], json!({}));
        assert_eq!(actual["reasoning_billed_as_output"], json!([]));
    }
}

#[test]
fn tied_windows_preserve_every_measured_field_except_routing_and_ratios() {
    let conn = fixture();
    let end = START + 3 * DAY;
    let tie_counter = |key| {
        for day in 0..3 {
            for step in 0..=48 {
                counter_sample(
                    &conn,
                    ACCOUNT,
                    key,
                    (START + day * DAY + step * STEP, day * 8 + step / 6),
                    (WEEK, ALL),
                );
            }
        }
    };
    for day in 0..3 {
        event(
            &conn,
            &format!("valued-{day}"),
            "other",
            Some(ACCOUNT),
            START + day * DAY + 1,
        );
    }
    let capture = || {
        let tx = conn.unchecked_transaction().unwrap();
        capture_pool_day(&tx, PROVIDER, START, end).unwrap()
    };
    let served = prices(Vec::new());
    let identities = PoolModelIdentities::default();
    tie_counter("primary");
    let mut measured = rows(&capture(), &served, &identities).pop().unwrap();
    assert!(
        measured.routing_eligible,
        "{:?}",
        measured.eligibility_reasons
    );
    assert!(measured.multiplier.is_some());
    assert!(measured.fee_multiplier.is_some());

    tie_counter("secondary");
    measured.multiplier = None;
    measured.fee_multiplier = None;
    measured.routing_eligible = false;
    measured.eligibility_reasons = vec!["window_tie".into()];
    let tied = rows(&capture(), &served, &identities);
    assert_eq!(tied.len(), 2);
    assert_eq!(tied[0], measured);
    measured.window_pool_key = "secondary".into();
    assert_eq!(tied[1], measured);
}
