fn reset_reading(conn: &Connection, account: &str, at: i64, used: f64, reset_ms: i64) {
    reading(conn, account, at, used, None);
    let reset = chrono::DateTime::from_timestamp_millis(reset_ms)
        .unwrap()
        .to_rfc3339();
    conn.execute(
        "UPDATE window_sample SET resets_at=?1 WHERE account_ref=?2 AND fetched_at_instant=?3",
        params![reset, account, at * NANOS_PER_MS],
    )
    .unwrap();
}

fn v2_event(
    conn: &Connection,
    id: &str,
    account: Option<&str>,
    model: &str,
    at: i64,
    quantity: Option<i64>,
) {
    conn.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES (?1,'pi','lineage',?1,?2,?2,'openai',?3,'subscription_included',?4)", params![id, at, model, account]).unwrap();
    conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','input',?2,?3)", params![id, quantity, quantity.map(|_| 0)]).unwrap();
}

/// Decode the whole served reply before resolving every listed plan, so one
/// incompatible row cannot silently remove the other providers' plan prices.
#[test]
fn plan_prices_from_fusiform_90a0531_decode_and_price_every_plan() {
    use sha2::{Digest, Sha256};
    let bytes =
        include_bytes!("../../test-vectors/fusiform-90a0531/plan-prices-every-curated-row.json");
    assert_eq!(
        hex::encode(Sha256::digest(bytes)),
        "f18fc68554a0a98a4be2104938cdd15795081ecb1f463831f982b75d62b9dc5c"
    );
    let fixture: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let response: PlanPricesResponse = serde_json::from_value(fixture["response"].clone()).unwrap();
    assert!(response.quota_pools.is_empty());
    let kimi = response
        .prices
        .iter()
        .find(|row| row.provider_id == "kimi-code-plan-global" && row.tier == "allegro")
        .unwrap();
    assert!(kimi.service_tiers.is_none());

    // State the money independently of the fixture's numeric representation.
    // The free plan has no positive monthly fee; the ambiguous Pro key is
    // explicitly refused rather than guessed from the priced Pro tiers.
    let expected = [
        ("anthropic/max_20x", Some(200_000_000_000_i64), None),
        ("anthropic/max_5x", Some(100_000_000_000), None),
        ("anthropic/pro", Some(20_000_000_000), None),
        ("google/ultra_5x", Some(99_990_000_000), None),
        ("kimi-code-plan-global/allegro", Some(99_000_000_000), None),
        ("meta/power_usage", Some(50_000_000_000), None),
        ("ollama-cloud/free", None, Some("plan_price_out_of_range")),
        ("openai/plus", Some(20_000_000_000), None),
        ("openai/pro", None, Some("plan_price_refused")),
        ("openai/pro_100", Some(100_000_000_000), None),
        ("openai/pro_200", Some(200_000_000_000), None),
        ("openai/pro_500", Some(500_000_000_000), None),
        ("opencode-go/go", Some(10_000_000_000), None),
    ];
    assert_eq!(response.prices.len(), expected.len());
    for row in &response.prices {
        let key = format!("{}/{}", row.provider_id, row.tier);
        assert!(expected.iter().any(|(plan, _, _)| *plan == key), "{key}");
    }
    for (plan, nanos, reason) in expected {
        let mut used = test_plan_price("acct", plan, None);
        fusiform_price(&response, plan, 1_794_700_800_000, &mut used);
        assert_eq!(used.monthly_nanos, nanos, "{plan}");
        assert_eq!(used.unpriceable_reason.as_deref(), reason, "{plan}");
        assert_eq!(used.source.as_deref(), nanos.map(|_| "fusiform"), "{plan}");
        assert!(!used.plan_price_overdue, "{plan}");
        if plan == "openai/pro" {
            assert!(used
                .refusal_text
                .as_deref()
                .unwrap()
                .starts_with("\"pro\" is an API plan string, not a vendor tier name."));
        }
    }
}

#[test]
fn v2_reset_periods_ignore_rounding_flaps_and_jitter_but_count_new_first_reading() {
    let conn = conn();
    let reset = 7 * 24 * HOUR_MS;
    for (minute, used, horizon) in [
        (0, 60.0, reset),
        (5, 59.0, reset),
        (10, 61.0, reset),
        (15, 0.0, reset),
        (20, 61.0, reset),
        (25, 0.0, reset),
        (30, 61.0, reset + 700),
        (35, 80.0, reset + 7 * 24 * HOUR_MS),
    ] {
        reset_reading(&conn, "acct", minute * 60_000, used, horizon);
    }
    let measured = delta(&conn, 0, HOUR_MS);
    assert!((measured.delta_percent - 81.0).abs() < 1e-9);
    assert_eq!(measured.segments(), 2);
}

#[test]
fn v2_boundary_pairs_are_clipped_and_distant_tail_is_not_coverage() {
    let conn = conn();
    reset_reading(&conn, "acct", 0, 0.0, 100 * HOUR_MS);
    reset_reading(&conn, "acct", 10 * 60_000, 10.0, 100 * HOUR_MS);
    reset_reading(&conn, "acct", 2 * HOUR_MS, 90.0, 100 * HOUR_MS);
    let delta = delta(&conn, 5 * 60_000, 15 * 60_000);
    assert!((delta.delta_percent - 5.0).abs() < 1e-9);
    assert_eq!(delta.hours[&0].covered_ms, 5 * 60_000);
}

#[test]
fn v2_cached_previous_period_cannot_raise_current_high_water() {
    let conn = conn();
    let old_reset = 7 * 24 * HOUR_MS;
    let new_reset = old_reset + 7 * 24 * HOUR_MS;
    for (minute, used, horizon) in [
        (0, 90.0, old_reset),
        (5, 5.0, new_reset),
        (10, 95.0, old_reset),
        (15, 10.0, new_reset),
    ] {
        reset_reading(&conn, "acct", minute * 60_000, used, horizon);
    }
    let measured = delta(&conn, 0, HOUR_MS);
    assert!((measured.delta_percent - 10.0).abs() < 1e-9);
    assert_eq!(measured.stale_reset_readings, 1);
}

#[test]
fn v2_reset_jitter_is_not_rollover_or_stale_previous_period() {
    let conn = conn();
    let old = 14 * 24 * HOUR_MS;
    let new = old + 7 * 24 * HOUR_MS;
    for (minute, used, horizon) in [
        (0, 10.0, old),
        (1, 15.0, old + 2_000),
        (2, 16.0, old + 312_000),
        (3, 17.0, old + 1_000),
        (4, 20.0, old),
        (5, 45.0, new),
        (6, 50.0, new + 4_000),
        (7, 100.0, old),
        (8, 55.0, new),
    ] {
        reset_reading(&conn, "acct", minute * 60_000, used, horizon);
    }
    let measured = delta(&conn, 0, HOUR_MS);
    assert!((measured.delta_percent - 65.0).abs() < 1e-9);
    assert_eq!(measured.reset_boundaries, 1);
    assert_eq!(measured.stale_reset_readings, 1);
    assert_eq!(measured.readings, 8);
}

#[test]
fn v2_jitter_tolerance_does_not_merge_short_window_rollovers() {
    let conn = conn();
    reset_reading(&conn, "acct", 0, 80.0, 60_000);
    reset_reading(&conn, "acct", 60_000, 20.0, 120_000);
    conn.execute("UPDATE window_sample SET window_minutes=1", [])
        .unwrap();
    let measured = delta(&conn, 0, 120_000);
    assert!((measured.delta_percent - 20.0).abs() < 1e-9);
    assert_eq!(measured.reset_boundaries, 1);
}

fn stability_fixture(conn: &Connection, daily_scale: [i64; 3]) {
    for minute in (0..=3 * 24 * 60).step_by(5) {
        reset_reading(
            conn,
            "acct",
            minute * 60_000,
            minute as f64 / 60.0,
            14 * 24 * HOUR_MS,
        );
    }
    // Distinct UTC days and six-hour bins carry matched value and points,
    // rather than making a one-call arithmetic fixture certify stability.
    for hour in (0..72).step_by(6) {
        let day = usize::try_from(hour / 24).unwrap();
        v2_event(
            conn,
            &format!("stable-{hour}"),
            Some("acct"),
            "m",
            (hour + 3) * HOUR_MS,
            Some(250_000 * daily_scale[day]),
        );
    }
}

fn calibration_pool_row(conn: &Connection, end: i64, rates: &ResolvedRates) -> Observation {
    calibration_pool_row_between(conn, (0, end), rates)
}

fn calibration_pool_row_between(
    conn: &Connection,
    (start, end): (i64, i64),
    rates: &ResolvedRates,
) -> Observation {
    fixture_vault_plan(conn, "openai/pro_20x");
    fixture_pool_schema_at_genesis(conn);
    let tx = conn.unchecked_transaction().unwrap();
    let capture = pools::capture_pool_day(&tx, "codex", start, end).unwrap();
    tx.commit().unwrap();
    let served: PlanPricesResponse = serde_json::from_value(json!({
        "resolved_at_ms":end,"tier_vocabulary":"vendor","unit_policy":"list",
        "prices":[{"provider_id":"openai","tier":"pro_20x","price":{"minor_units":200,"exponent":0,"currency":"USD","period":"month"},
            "boundary_at_ms":0,"established_by":"test","established_at_ms":0,"review_by_ms":end,"source_ref":"https://example.invalid/plan"}]
    })).unwrap();
    let pools = pools::plan_pools(&capture.readings, &served.quota_pools);
    pools::compute_pool_observations(&capture, &pools, rates, &Default::default(), &served, end)
        .unwrap()
        .remove(0)
}

#[test]
fn v2_adequate_stable_sample_does_not_need_half_the_week() {
    let mut conn = conn();
    let day_ms = 24 * HOUR_MS;
    let horizon = 21 * day_ms;
    for day in 0..7 {
        for minute in (0..24 * 60).step_by(5) {
            if minute > 14 * 60 && minute < 19 * 60 {
                continue;
            }
            let (used, reset) = if minute < 14 * 60 {
                (
                    if minute < 9 * 60 {
                        minute as f64 / 60.0
                    } else {
                        100.0
                    },
                    horizon + 2 * day * day_ms,
                )
            } else {
                (
                    (minute - 14 * 60) as f64 / 60.0,
                    horizon + (2 * day + 1) * day_ms,
                )
            };
            reset_reading(&conn, "acct", day * day_ms + minute * 60_000, used, reset);
        }
        for hour in 0..8 {
            v2_event(
                &conn,
                &format!("valued-{day}-{hour}"),
                Some("acct"),
                "m",
                day * day_ms + hour * HOUR_MS + HOUR_MS / 2,
                Some(125_000),
            );
        }
        for hour in 19..24 {
            v2_event(
                &conn,
                &format!("unpriced-{day}-{hour}"),
                Some("acct"),
                "premium",
                day * day_ms + hour * HOUR_MS + HOUR_MS / 2,
                Some(1),
            );
        }
    }
    reset_reading(&conn, "acct", 7 * day_ms, 0.0, horizon + 14 * day_ms);
    let row = calibration_pool_row(&conn, 7 * day_ms, &rates_for(&[("openai", "m")], 100, None));
    assert!(row.routing_eligible, "{:?}", row.eligibility_reasons);
    assert!((row.clean_hours_share - 1.0 / 3.0).abs() < 1e-12);
    assert!(row.gap_share < 0.25);
    assert_eq!(row.api_nanos, 700);
    assert!((row.delta_percent - 56.0).abs() < 1e-9);
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &row).unwrap();
    tx.commit().unwrap();
    let report = multiplier_report(
        &conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 8 * day_ms,
        },
    )
    .unwrap();
    let stability = &report["series"][0]["stability_diagnostics"];
    assert_eq!(stability["active_days"], 7);
    assert!(stability["bootstrap_relative_width"].as_f64().unwrap() < 1e-9);
}

#[test]
fn v2_changed_gaps_do_not_veto_an_adequate_stable_sample() {
    let conn = conn();
    let day_ms = 24 * HOUR_MS;
    for day in 0..7 {
        for minute in (0..=4 * 60).step_by(5) {
            reset_reading(
                &conn,
                "acct",
                day * day_ms + minute * 60_000,
                day as f64 * 10.0 + minute as f64 / 60.0,
                21 * day_ms,
            );
        }
        v2_event(
            &conn,
            &format!("priced-{day}"),
            Some("acct"),
            "m",
            day * day_ms + 2 * HOUR_MS,
            Some(1_000_000),
        );
    }
    reset_reading(&conn, "acct", 7 * day_ms, 70.0, 21 * day_ms);
    let row = calibration_pool_row(&conn, 7 * day_ms, &rates_for(&[("openai", "m")], 100, None));
    assert!(row.routing_eligible, "{:?}", row.eligibility_reasons);
    assert!((row.gap_share - 5.0 / 6.0).abs() < 1e-12);
    assert!((row.clean_hours_share - 1.0 / 6.0).abs() < 1e-12);
    assert!((row.delta_percent - 28.0).abs() < 1e-9);
    assert_eq!(row.api_nanos, 700);
}

#[test]
fn v2_variable_day_calibration_is_not_routing_eligible() {
    let mut conn = conn();
    stability_fixture(&conn, [1, 10, 1]);
    let row = calibration_pool_row(
        &conn,
        72 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(!row.routing_eligible);
    assert_eq!(row.eligibility_reasons, vec!["unstable_estimate"]);
    assert_eq!(row.api_nanos, 1200);
    assert!((row.delta_percent - 72.0).abs() < 1e-9);
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &row).unwrap();
    tx.commit().unwrap();
    let report = multiplier_report(
        &conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 73 * HOUR_MS,
        },
    )
    .unwrap();
    let stability = &report["series"][0]["stability_diagnostics"];
    assert_eq!(stability["active_days"], 3);
    assert_eq!(stability["held_out_test_bins"], 12);
    assert!(stability["bootstrap_relative_width"].as_f64().unwrap() > 0.5);
    assert!((stability["bootstrap_95_low"].as_f64().unwrap() - 100.0e-9 / 24.0).abs() < 1e-20);
    assert!((stability["bootstrap_95_high"].as_f64().unwrap() - 1000.0e-9 / 24.0).abs() < 1e-20);
    // Held out: four high-value bins err by 54 points each; each of eight
    // low-value bins errs by 54/11. Their mean squared error is 119556/121.
    let expected_rmse = (119_556.0_f64 / 121.0).sqrt();
    assert!(
        (stability["held_out_ratio_rmse_points"].as_f64().unwrap() - expected_rmse).abs() < 1e-9
    );
}

#[test]
fn multiplier_regression_expired_reset_cannot_certify_a_clean_hour() {
    let conn = conn();
    for hour in [1, 2] {
        reset_reading(&conn, "acct", hour * HOUR_MS, 10.0, HOUR_MS / 2);
    }
    v2_event(
        &conn,
        "expired-period",
        Some("acct"),
        "m",
        HOUR_MS + 1,
        Some(1_000_000),
    );
    let row = calibration_pool_row_between(
        &conn,
        (HOUR_MS, 2 * HOUR_MS),
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert_eq!(row.api_nanos, 0, "expired quota admitted priced usage");
    assert!(row.clean_hours_share.abs() < f64::EPSILON);
}

#[test]
fn v2_day_blocks_keep_account_value_and_points_paired() {
    let conn = conn();
    let mut base = 0.0;
    for (day, scale) in [1_i64, 2, 4].into_iter().enumerate() {
        let day = i64::try_from(day).unwrap();
        for minute in (0..24 * 60).step_by(5) {
            reset_reading(
                &conn,
                "acct",
                day * 24 * HOUR_MS + minute * 60_000,
                base + minute as f64 / 240.0 * scale as f64,
                14 * 24 * HOUR_MS,
            );
        }
        for hour in (0..24).step_by(6) {
            v2_event(
                &conn,
                &format!("paired-{day}-{hour}"),
                Some("acct"),
                "m",
                (day * 24 + hour + 3) * HOUR_MS,
                Some(250_000 * scale),
            );
        }
        base += 6.0 * scale as f64;
    }
    reset_reading(&conn, "acct", 72 * HOUR_MS, base, 14 * 24 * HOUR_MS);
    let row = calibration_pool_row_between(
        &conn,
        (0, 72 * HOUR_MS),
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(row.routing_eligible, "{:?}", row.eligibility_reasons);
    assert_eq!(row.api_nanos, 700);
    assert!((row.delta_percent - 42.0).abs() < 1e-9);
    let summary = &row.accounts_included[0]["plan_calibration"]["summary"];
    assert!(summary["bootstrap_relative_width"].as_f64().unwrap() < 1e-9);
    assert!(summary["held_out_ratio_rmse_points"].as_f64().unwrap() < 1e-9);
    assert!((summary["held_out_ratio_r2"].as_f64().unwrap() - 1.0).abs() < 1e-9);
}

#[test]
fn v2_unpriced_share_does_not_veto_an_adequate_stable_sample() {
    let mut conn = conn();
    stability_fixture(&conn, [1, 1, 1]);
    for hour in (0..72).filter(|hour| hour % 6 < 3) {
        v2_event(
            &conn,
            &format!("unpriced-{hour}"),
            Some("acct"),
            "premium",
            hour * HOUR_MS,
            Some(1),
        );
    }
    let row = calibration_pool_row_between(
        &conn,
        (0, 72 * HOUR_MS),
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(row.routing_eligible, "{:?}", row.eligibility_reasons);
    assert!((row.unpriced_share - 0.5).abs() < 1e-12);
    assert!((row.clean_hours_share - 0.5).abs() < 1e-12);
    assert!((row.delta_percent - 36.0).abs() < 1e-9);
    assert_eq!(row.api_nanos, 300);
    assert_eq!(row.tokens.input, 3_000_000);
    assert!(row
        .api_nanos_by_model
        .iter()
        .all(|model| model.model.as_deref() == Some("m")));
    assert!(row
        .unpriced_tokens
        .iter()
        .any(|item| item.model.ends_with("/premium") && item.tokens == 36));
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &row).unwrap();
    tx.commit().unwrap();
    let report = multiplier_report(
        &conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 73 * HOUR_MS,
        },
    )
    .unwrap();
    let served = &report["series"][0];
    assert_eq!(served["routing_eligible"], true);
    assert!((served["unpriced_share"].as_f64().unwrap() - 0.5).abs() < 1e-12);
    assert_eq!(served["stability_diagnostics"]["active_days"], 3);
    assert!(
        served["stability_diagnostics"]["bootstrap_relative_width"]
            .as_f64()
            .unwrap()
            < 1e-9
    );
    assert!(
        served["stability_diagnostics"]["held_out_ratio_rmse_points"]
            .as_f64()
            .unwrap()
            < 1e-9
    );
}

#[test]
fn v2_two_days_do_not_establish_day_block_stability() {
    let conn = conn();
    stability_fixture(&conn, [1, 1, 1]);
    let row = calibration_pool_row(
        &conn,
        48 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(!row.routing_eligible);
    assert_eq!(row.eligibility_reasons, vec!["insufficient_stability_days"]);
    assert_eq!(row.api_nanos, 200);
    assert!((row.delta_percent - 48.0).abs() < 1e-9);
}

#[test]
fn v2_total_money_is_rounded_once_across_models() {
    let conn = conn();
    v2_event(&conn, "first", Some("acct"), "m", 1, Some(1));
    v2_event(&conn, "second", Some("acct"), "n", 1, Some(1));
    let rates = rates_for(&[("openai", "m"), ("openai", "n")], 500_000, None);
    let row = measured_usage(&test_calls(&conn, 0, HOUR_MS), (0, HOUR_MS), &rates).unwrap();
    assert_eq!(row.api_nanos, 1);
}

#[test]
fn v2_migration_preserves_legacy_values_and_marks_them_ineligible() {
    let conn = Connection::open_in_memory().unwrap();
    for migration in crate::store::MIGRATIONS
        .iter()
        .filter(|migration| migration.version < 35)
    {
        conn.execute_batch(migration.statements).unwrap();
    }
    conn.execute_batch("INSERT INTO multiplier_observation(quota_provider,observation_day,snapshot_at_ms,interval_start_ms,interval_end_ms,window_pool_key,window_minutes,input_tokens,cache_read_tokens,cache_write_tokens,output_tokens,reasoning_tokens,api_nanos,api_nanos_by_model,unpriced_tokens,tokens_outside_quota_coverage,base_rate_tier_untested_tokens,subscription_nanos,fee_paid_nanos,delta_percent,accounts_included_count,accounts_included,accounts_excluded,excluded_pairs,low_resolution,scope_rules,plan_prices,catalog_resolved_at_ms,api_twins,reasoning_billed_as_output,error_deflating,error_inflating) VALUES ('codex','1970-01-02',86400000,0,86400000,'primary',10080,0,0,0,0,0,17,'[]','[]',0,0,0,0,0,0,'[]','[]','{}',1,'[]','[]',86400000,'{}','[]','','');").unwrap();
    conn.execute_batch(
        crate::store::MIGRATIONS
            .iter()
            .find(|migration| migration.version == 35)
            .unwrap()
            .statements,
    )
    .unwrap();
    let (plan, eligible, reasons, nanos): (Option<String>, i64, String, i64) = conn.query_row("SELECT plan,routing_eligible,eligibility_reasons,api_nanos FROM multiplier_observation", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
    assert!(plan.is_none());
    assert_eq!(eligible, 0);
    assert_eq!(reasons, "[\"legacy_estimator\"]");
    assert_eq!(nanos, 17);
}

#[test]
fn v2_quiet_gaps_are_covered_but_changed_gaps_are_unknown() {
    let horizon = 7 * 24 * HOUR_MS;
    let mut measured = BTreeMap::new();
    for (account, before, after, reset) in [
        ("quiet", 10.0, 10.0, horizon + 700),
        ("changed", 10.0, 20.0, horizon),
        ("reset", 10.0, 10.0, horizon + 7 * 24 * HOUR_MS),
        ("capped", 100.0, 100.0, horizon),
    ] {
        let iso = |at| {
            chrono::DateTime::from_timestamp_millis(at)
                .unwrap()
                .to_rfc3339()
        };
        let (old, new) = (iso(horizon), iso(reset));
        measured.insert(
            account,
            clean::measure_run(
                account,
                [
                    (0, before, Some(old.as_str())),
                    (4 * HOUR_MS * NANOS_PER_MS, after, Some(new.as_str())),
                ],
                clean::reset_tolerance_ms(10080),
                (HOUR_MS, 3 * HOUR_MS),
            ),
        );
    }
    let quiet = &measured["quiet"];
    assert_eq!(quiet.coverage_ms, Some((HOUR_MS, 3 * HOUR_MS)));
    assert!((quiet.delta_percent).abs() < 1e-12);
    assert_eq!(quiet.hours[&HOUR_MS].covered_ms, HOUR_MS);
    assert_eq!(quiet.hours[&(2 * HOUR_MS)].covered_ms, HOUR_MS);
    for account in ["changed", "reset"] {
        let unknown = &measured[account];
        assert!(unknown.coverage_ms.is_none());
        assert!(unknown.hours.values().all(|hour| hour.covered_ms == 0));
        assert!(unknown.delta_percent.abs() < 1e-12);
    }
    let capped = &measured["capped"];
    assert_eq!(capped.hours[&HOUR_MS].covered_ms, HOUR_MS);
    assert!(capped.hours[&HOUR_MS].saturated);
}

#[test]
fn v2_antigravity_drops_only_the_frozen_wrong_vault_history() {
    let conn = conn();
    fixture_vault_plan(&conn, "google/plan");
    fixture_pool_schema_at_genesis(&conn);
    let cutoff = 1_792_523_280_000_i64;
    for (at, used, source) in [
        (cutoff - 600_000, 10.0, "oauth"),
        (cutoff - 300_000, 80.0, "vault"),
        (cutoff, 20.0, "vault"),
        (cutoff + 300_000, 25.0, "oauth"),
    ] {
        conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,resets_at,window_minutes,source,model_scope) VALUES ('antigravity','acct','primary',?1,?2,'2026-11-01T00:00:00Z',10080,?3,'all')", params![at * NANOS_PER_MS, used, source]).unwrap();
    }
    let (start, end) = (cutoff - 600_000, cutoff + 600_000);
    let capture = pools::capture_pool_readings(&conn, "antigravity", start, end).unwrap();
    let planned = pools::plan_pools(&capture, &[]);
    let run = &planned[0].reading_runs[0];
    let measured = clean::measure_run(
        "acct",
        run.readings.iter().map(|r| {
            (
                r.fetched_at_instant_ns,
                r.used_percent,
                r.resets_at.as_deref(),
            )
        }),
        clean::reset_tolerance_ms(planned[0].window_minutes),
        (start, end),
    );
    assert!((measured.delta_percent - 15.0).abs() < 1e-9);
    assert_eq!(measured.readings, 3);
}

#[test]
fn v2_fee_ratio_counts_calls_in_every_hour_but_quota_ratio_only_clean_ones() {
    let conn = conn();
    for minute in (0..=48 * 60).step_by(5) {
        reset_reading(
            &conn,
            "acct",
            minute * 60_000,
            f64::from(i32::try_from(minute).unwrap()) / 60.0,
            7 * DAY,
        );
    }
    v2_event(
        &conn,
        "priced",
        Some("acct"),
        "m",
        10 * HOUR_MS,
        Some(1_000_000),
    );
    // Hours 20 to 22 lose their readings while usage rises, so they are not
    // clean. A call in hour 21 still contributes to the fee ratio.
    conn.execute(
        "DELETE FROM window_sample WHERE fetched_at_instant > ?1 AND fetched_at_instant < ?2",
        params![20 * HOUR_MS * NANOS_PER_MS, 23 * HOUR_MS * NANOS_PER_MS],
    )
    .unwrap();
    v2_event(
        &conn,
        "unclean",
        Some("acct"),
        "m",
        21 * HOUR_MS,
        Some(1_000_000),
    );
    let row = calibration_pool_row(
        &conn,
        48 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert_eq!(
        row.api_nanos, 100,
        "the quota side counts only the clean call"
    );
    let fee = row.fee_multiplier.expect("fee ratio");
    assert!(
        (fee - 200.0 / row.fee_paid_nanos as f64).abs() < 1e-12,
        "fee ratio {fee} must count both calls against {} nanos",
        row.fee_paid_nanos
    );
}

#[test]
fn v2_clean_hours_exclude_saturation_gaps_and_unpriced_usage() {
    let conn = conn();
    for minute in (0..=48 * 60).step_by(5) {
        reset_reading(
            &conn,
            "acct",
            minute * 60_000,
            f64::from(i32::try_from(minute).unwrap()) / 60.0,
            7 * DAY,
        );
    }
    v2_event(
        &conn,
        "priced",
        Some("acct"),
        "m",
        10 * HOUR_MS,
        Some(1_000_000),
    );
    // One saturated hour, one hole, and one premium-model hour are different
    // exclusions; none of their value may leak into the clean numerator.
    conn.execute("UPDATE window_sample SET raw_used_percent=100 WHERE fetched_at_instant>=?1 AND fetched_at_instant<=?2", params![HOUR_MS * NANOS_PER_MS, 2 * HOUR_MS * NANOS_PER_MS]).unwrap();
    conn.execute(
        "DELETE FROM window_sample WHERE fetched_at_instant>?1 AND fetched_at_instant<?2",
        params![3 * HOUR_MS * NANOS_PER_MS, 4 * HOUR_MS * NANOS_PER_MS],
    )
    .unwrap();
    v2_event(
        &conn,
        "premium",
        Some("acct"),
        "unpriced",
        5 * HOUR_MS,
        Some(1),
    );
    let row = calibration_pool_row(
        &conn,
        48 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(row.saturated_share > 0.0 && row.gap_share > 0.0 && row.unpriced_share > 0.0);
    assert!(row.clean_hours_share < 1.0);
    assert_eq!(row.tokens.input, 1_000_000);
    assert!(row.delta_percent < 48.0);
}

#[test]
fn v2_a_day_without_clean_hours_has_no_quota_ratio() {
    let conn = conn();
    reset_reading(&conn, "acct", 0, 10.0, 7 * DAY);
    reset_reading(&conn, "acct", 2 * HOUR_MS, 20.0, 7 * DAY);
    v2_event(&conn, "used", Some("acct"), "m", HOUR_MS, Some(1_000_000));
    let row = calibration_pool_row(
        &conn,
        2 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert_eq!(row.clean_hours_share, 0.0);
    assert!(row.multiplier.is_none(), "multiplier {:?}", row.multiplier);
    assert!(row
        .eligibility_reasons
        .iter()
        .any(|reason| reason == "insufficient_clean_hours"));
}

#[test]
fn v2_zero_measurement_events_are_unknown_not_free_usage() {
    let mut conn = conn();
    stability_fixture(&conn, [1, 1, 1]);
    conn.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES ('empty','pi','fixture','empty',?1,?1,'openai','m','subscription_included','acct')", [HOUR_MS]).unwrap();
    let row = calibration_pool_row(
        &conn,
        72 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(row.routing_eligible);
    assert!((row.clean_hours_share - 71.0 / 72.0).abs() < 1e-12);
    assert!((row.unpriced_share - 1.0 / 72.0).abs() < 1e-12);
    assert!((row.delta_percent - 71.0).abs() < 1e-9);
    assert_eq!(row.api_nanos, 300);
    assert_eq!(row.zero_measurement_events, 1);
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &row).unwrap();
    tx.commit().unwrap();
    let report = multiplier_report(
        &conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 73 * HOUR_MS,
        },
    )
    .unwrap();
    assert_eq!(report["series"][0]["zero_measurement_events"], 1);
    assert_eq!(report["series"][0]["unknown_quantity_measurements"], 0);
    assert_eq!(report["series"][0]["routing_eligible"], true);
}

#[test]
fn v2_missing_quantity_is_unknown_not_zero() {
    let mut conn = conn();
    stability_fixture(&conn, [1, 1, 1]);
    v2_event(&conn, "unknown", Some("acct"), "m", HOUR_MS, None);
    conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id='unknown'),'output','token','billed','output',NULL,NULL)", []).unwrap();
    let row = calibration_pool_row(
        &conn,
        72 * HOUR_MS,
        &rates_for(&[("openai", "m")], 100, None),
    );
    assert!(row.routing_eligible);
    assert!(row.multiplier.is_some_and(|value| value > 0.0));
    assert!(row.fee_multiplier.is_some_and(|value| value > 0.0));
    assert!((row.clean_hours_share - 71.0 / 72.0).abs() < 1e-12);
    assert!((row.unpriced_share - 1.0 / 72.0).abs() < 1e-12);
    assert!((row.delta_percent - 71.0).abs() < 1e-9);
    assert_eq!(row.api_nanos, 300);
    assert_eq!(row.missing_quantity_events, 1);
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &row).unwrap();
    tx.commit().unwrap();
    let report = multiplier_report(
        &conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 73 * HOUR_MS,
        },
    )
    .unwrap();
    assert_eq!(report["series"][0]["unknown_quantity_measurements"], 2);
    assert_eq!(report["series"][0]["routing_eligible"], true);
}

fn observation_days_later(observation: &Observation, days: i64) -> Observation {
    let mut shifted = observation.clone();
    let offset = days * DAY;
    shifted.interval_start_ms += offset;
    shifted.interval_end_ms += offset;
    shifted.token_interval_start_ms = shifted.token_interval_start_ms.map(|at| at + offset);
    shifted.token_interval_end_ms = shifted.token_interval_end_ms.map(|at| at + offset);
    shifted.snapshot_at_ms += offset;
    shifted.observation_day = utc_day(shifted.interval_end_ms).unwrap();
    shifted
}

fn eligible_plan_observation(conn: &Connection) -> Observation {
    let mut observation = sample_observation(conn);
    observation.plan = Some("openai/pro_200".into());
    observation.routing_eligible = true;
    observation.eligibility_reasons.clear();
    observation
}

fn latest_report(conn: &Connection) -> Value {
    multiplier_report(
        conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 10 * DAY,
        },
    )
    .unwrap()
}

fn test_provider_day(observation: &Observation) -> provider_day::ProviderDay<'_> {
    provider_day::ProviderDay {
        provider: &observation.quota_provider,
        day: &observation.observation_day,
        interval_start_ms: observation.interval_start_ms,
        interval_end_ms: observation.interval_end_ms,
    }
}

fn record_one(tx: &Transaction<'_>, row: &Observation) {
    assert!(test_provider_day(row)
        .record(tx, row.snapshot_at_ms, std::slice::from_ref(row))
        .unwrap());
}

#[test]
fn v2_latest_last_eligible_uses_newest_eligible_plan_head() {
    let mut conn = conn();
    let mut first = eligible_plan_observation(&conn);
    first.pool_scope = Some(PoolScope::All);
    first.multiplier = Some(1.25);
    first.fee_multiplier = None;
    let mut second = observation_days_later(&first, 1);
    second.multiplier = Some(2.5);
    second.fee_multiplier = Some(0.75);
    let mut newest = observation_days_later(&second, 1);
    newest.routing_eligible = false;
    newest.eligibility_reasons = vec!["insufficient_points".into()];
    newest.multiplier = Some(9.0);
    newest.fee_multiplier = Some(0.25);
    let tx = conn.transaction().unwrap();
    for observation in [&first, &second, &newest] {
        record_one(&tx, observation);
    }
    tx.commit().unwrap();

    let report = latest_report(&conn);
    let latest = &report["latest"][0];
    assert_eq!(latest["observation_day"], "1970-01-04");
    assert_eq!(latest["routing_eligible"], false);
    assert_eq!(
        latest["last_eligible"],
        json!({
            "day": "1970-01-03",
            "interval_end_ms": 2 * DAY,
            "multiplier": 2.5,
            "fee_multiplier": 0.75,
        })
    );
    println!(
        "latest with last_eligible: {}",
        serde_json::to_string(latest).unwrap()
    );
    assert!(report["series"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row.get("last_eligible").is_none()));
}

#[test]
fn v2_latest_last_eligible_describes_eligible_latest_row() {
    let mut conn = conn();
    let mut eligible = eligible_plan_observation(&conn);
    eligible.pool_scope = Some(PoolScope::All);
    eligible.multiplier = None;
    eligible.fee_multiplier = None;
    let tx = conn.transaction().unwrap();
    record_one(&tx, &eligible);
    tx.commit().unwrap();

    let report = latest_report(&conn);
    let latest = &report["latest"][0];
    assert_eq!(latest["routing_eligible"], true);
    assert_eq!(
        latest["last_eligible"],
        json!({
            "day": latest["observation_day"],
            "interval_end_ms": latest["interval_end_ms"],
            "multiplier": null,
            "fee_multiplier": null,
        })
    );
}

#[test]
fn v2_latest_last_eligible_is_absent_without_an_eligible_plan_head() {
    let mut conn = conn();
    let mut never_eligible = eligible_plan_observation(&conn);
    never_eligible.routing_eligible = false;
    never_eligible.eligibility_reasons = vec!["insufficient_points".into()];
    let mut legacy = never_eligible.clone();
    legacy.quota_provider = "legacy".into();
    legacy.plan = None;
    legacy.eligibility_reasons = vec!["legacy_estimator".into()];
    never_eligible.pool_scope = Some(PoolScope::All);
    let mut eligible_control = never_eligible.clone();
    eligible_control.plan = Some("openai/pro_100".into());
    eligible_control.routing_eligible = true;
    eligible_control.eligibility_reasons.clear();
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &legacy).unwrap();
    test_provider_day(&never_eligible)
        .record(&tx, DAY, &[never_eligible.clone(), eligible_control])
        .unwrap();
    tx.commit().unwrap();

    let report = latest_report(&conn);
    let latest = report["latest"].as_array().unwrap();
    assert_eq!(latest.len(), 2);
    assert!(latest
        .iter()
        .find(|row| row["plan"] == "openai/pro_100")
        .unwrap()
        .get("last_eligible")
        .is_some());
    assert!(latest
        .iter()
        .filter(|row| row["plan"] != "openai/pro_100")
        .all(|row| row.get("last_eligible").is_none()));
}

#[test]
fn v2_latest_last_eligible_ignores_superseded_eligible_heads() {
    let mut conn = conn();
    let mut older = eligible_plan_observation(&conn);
    older.pool_scope = Some(PoolScope::All);
    older.multiplier = Some(1.25);
    older.fee_multiplier = Some(0.5);
    let mut superseded = observation_days_later(&older, 1);
    superseded.multiplier = Some(2.5);
    let mut correction = superseded.clone();
    correction.routing_eligible = false;
    correction.eligibility_reasons = vec!["insufficient_points".into()];
    correction.multiplier = Some(7.0);
    let mut newest = observation_days_later(&correction, 1);
    newest.multiplier = Some(9.0);
    let tx = conn.transaction().unwrap();
    for observation in [&older, &superseded, &newest] {
        record_one(&tx, observation);
    }
    insert_superseding_observation_in_tx(&tx, &correction).unwrap();
    tx.commit().unwrap();

    let report = latest_report(&conn);
    assert_eq!(report["latest"][0]["observation_day"], "1970-01-04");
    assert_eq!(
        report["latest"][0]["last_eligible"],
        json!({
            "day": "1970-01-02",
            "interval_end_ms": DAY,
            "multiplier": 1.25,
            "fee_multiplier": 0.5,
        })
    );
}

#[test]
fn v2_latest_uses_ordered_scoped_plan_heads_and_keeps_legacy_in_series() {
    let mut conn = conn();
    let mut legacy = sample_observation(&conn);
    legacy.plan = None;
    legacy.routing_eligible = false;
    legacy.eligibility_reasons = vec!["legacy_estimator".into()];
    let request = MultiplierReportRequest {
        from_ms: 0,
        to_ms: legacy.interval_end_ms + 1,
    };
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &legacy).unwrap();
    tx.commit().unwrap();
    let fallback = multiplier_report(&conn, &request).unwrap();
    assert_eq!(fallback["latest"], json!([]));
    assert!(fallback["series"][0]["plan"].is_null());
    assert!(fallback["series"][0]["pool_scope"].is_null());
    let tx = conn.transaction().unwrap();
    let scoped = ["openai/pro_200", "openai/pro_100"].map(|plan| {
        let mut row = legacy.clone();
        row.plan = Some(plan.into());
        row.pool_scope = Some(PoolScope::All);
        row.eligibility_reasons = vec!["insufficient_stability_days".into()];
        row
    });
    test_provider_day(&legacy)
        .record(&tx, DAY, &scoped)
        .unwrap();
    tx.commit().unwrap();
    let report = multiplier_report(&conn, &request).unwrap();
    let latest = report["latest"].as_array().unwrap();
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0]["plan"], "openai/pro_100");
    assert_eq!(latest[1]["plan"], "openai/pro_200");
    let series = report["series"].as_array().unwrap();
    assert_eq!(series.len(), 3);
    assert_eq!(series.iter().filter(|row| row["plan"].is_null()).count(), 1);
}

#[test]
fn v2_latest_uses_provider_days_and_matches_kimi_legacy_by_pool_identity() {
    let mut conn = conn();
    let mut legacy = eligible_plan_observation(&conn);
    legacy.quota_provider = "kimi".into();
    legacy.window_pool_key = "extra:7:primary".into();
    legacy.multiplier = Some(2.5);
    legacy.fee_multiplier = Some(0.75);

    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &legacy).unwrap();
    let before = latest_report(&tx);
    assert_eq!(before["latest"], json!([]));
    assert_eq!(before["series"][0]["pool_id"], "primary");
    assert!(before["series"][0]["pool_scope"].is_null());

    let mut older_other_pool = observation_days_later(&legacy, 1);
    older_other_pool.window_pool_key = "tertiary".into();
    older_other_pool.pool_scope = Some(PoolScope::All);
    record_one(&tx, &older_other_pool);
    let mut other_provider = older_other_pool.clone();
    other_provider.quota_provider = "claude".into();
    record_one(&tx, &other_provider);

    let mut unmeasured = observation_days_later(&older_other_pool, 1);
    unmeasured.window_pool_key = "primary".into();
    unmeasured.pool_scope = Some(PoolScope::Unknown);
    unmeasured.routing_eligible = false;
    unmeasured.eligibility_reasons = vec!["scope_unstated".into()];
    unmeasured.multiplier = None;
    unmeasured.fee_multiplier = None;
    let mut sibling = unmeasured.clone();
    sibling.window_pool_key = "extra:5:Fable".into();
    test_provider_day(&unmeasured)
        .record(&tx, 3 * DAY, &[unmeasured.clone(), sibling])
        .unwrap();

    let report = latest_report(&tx);
    let latest = report["latest"].as_array().unwrap();
    assert_eq!(latest.len(), 3);
    assert_eq!(latest[0]["quota_provider"], "claude");
    assert_eq!(latest[0]["interval_end_ms"], 2 * DAY);
    assert_eq!(latest[1]["pool_id"], "Fable");
    assert!(latest[1].get("last_eligible").is_none());
    assert_eq!(latest[2]["pool_id"], "primary");
    assert_eq!(latest[2]["eligibility_reasons"], json!(["scope_unstated"]));
    assert_eq!(
        latest[2]["last_eligible"],
        json!({
            "day": legacy.observation_day, "interval_end_ms": DAY,
            "multiplier": 2.5, "fee_multiplier": 0.75,
        })
    );
    assert!(report["series"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| !row["pool_scope"].is_null()));

    let empty = observation_days_later(&unmeasured, 1);
    test_provider_day(&empty).record(&tx, 4 * DAY, &[]).unwrap();
    let latest = latest_report(&tx)["latest"].as_array().unwrap().clone();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0]["quota_provider"], "claude");
}
