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

fn fixture(conn: &Connection) -> MultiplierConfig {
    fixture_quantity(conn, Some(1_000_000))
}

fn fixture_quantity(conn: &Connection, quantity: Option<i64>) -> MultiplierConfig {
    let config = config(vec![rule("pi", "openai", None)]);
    for minute in (0..=48 * 60).step_by(5) {
        reset_reading(
            conn,
            "acct",
            minute * 60_000,
            minute as f64 / 60.0,
            7 * 24 * HOUR_MS,
        );
    }
    v2_event(conn, "priced", None, "m", 10 * HOUR_MS, quantity);
    config
}

fn stable_fixture(conn: &Connection) -> MultiplierConfig {
    stability_fixture(conn, [1, 1, 1])
}

fn stability_fixture(conn: &Connection, daily_scale: [i64; 3]) -> MultiplierConfig {
    let config = config(vec![rule("pi", "openai", None)]);
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
            None,
            "m",
            (hour + 3) * HOUR_MS,
            Some(250_000 * daily_scale[day]),
        );
    }
    config
}

fn rows(conn: &Connection, config: &MultiplierConfig) -> Vec<Observation> {
    let plan = plan_observation(conn, config, "codex", 0, 48 * HOUR_MS).unwrap();
    compute_plan_observations(
        conn,
        config,
        &plan,
        &rates_for(&[("openai", "m")], 100, None),
        &priced_plan(),
        48 * HOUR_MS,
        &["openai".into()],
    )
    .unwrap()
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
    let measured = account_delta(&conn, "codex", "acct", &window(), 0, HOUR_MS).unwrap();
    assert!((measured.delta_percent - 81.0).abs() < 1e-9);
    assert_eq!(measured.segments(), 2);
}

#[test]
fn v2_boundary_pairs_are_clipped_and_distant_tail_is_not_coverage() {
    let conn = conn();
    reset_reading(&conn, "acct", 0, 0.0, 100 * HOUR_MS);
    reset_reading(&conn, "acct", 10 * 60_000, 10.0, 100 * HOUR_MS);
    reset_reading(&conn, "acct", 2 * HOUR_MS, 90.0, 100 * HOUR_MS);
    let delta = account_delta(&conn, "codex", "acct", &window(), 5 * 60_000, 15 * 60_000).unwrap();
    assert!((delta.delta_percent - 5.0).abs() < 1e-9);
    assert_eq!(delta.hours[&0].covered_ms, 5 * 60_000);
}

#[test]
fn v2_clean_hours_exclude_saturation_gaps_and_unpriced_usage() {
    let conn = conn();
    let config = fixture(&conn);
    // One saturated hour, one hole, and one premium-model hour are different
    // exclusions; none of their value may leak into the clean numerator.
    conn.execute("UPDATE window_sample SET raw_used_percent=100 WHERE fetched_at_instant>=?1 AND fetched_at_instant<=?2", params![HOUR_MS * NANOS_PER_MS, 2 * HOUR_MS * NANOS_PER_MS]).unwrap();
    conn.execute(
        "DELETE FROM window_sample WHERE fetched_at_instant>?1 AND fetched_at_instant<?2",
        params![3 * HOUR_MS * NANOS_PER_MS, 4 * HOUR_MS * NANOS_PER_MS],
    )
    .unwrap();
    event(
        &conn,
        "premium",
        "pi",
        "openai",
        "unpriced",
        "subscription_included",
        5 * HOUR_MS,
        &[("input", "input", 1)],
    );
    let row = rows(&conn, &config).remove(0);
    assert!(row.saturated_share > 0.0 && row.gap_share > 0.0 && row.unpriced_share > 0.0);
    assert!(row.clean_hours_share < 1.0);
    assert_eq!(row.tokens.input, 1_000_000);
    assert!(row.delta_percent < 48.0);
}

#[test]
fn v2_unconfigured_accounts_make_the_row_unmeasurable() {
    let conn = conn();
    let mut config = fixture(&conn);
    reset_reading(&conn, "new-account", HOUR_MS, 10.0, 7 * 24 * HOUR_MS);
    let row = rows(&conn, &config).remove(0);
    assert!(row.multiplier.is_none());
    assert!(!row.routing_eligible);
    assert_eq!(row.unconfigured_accounts, vec!["new-account"]);
    assert!(row
        .eligibility_reasons
        .iter()
        .any(|reason| reason == "unconfigured_accounts"));
    let account = config.quota_providers["codex"].accounts["acct"].clone();
    config
        .quota_providers
        .get_mut("codex")
        .unwrap()
        .accounts
        .insert("new-account".into(), account);
    assert!(rows(&conn, &config)[0].unconfigured_accounts.is_empty());
}

#[test]
fn v2_snapshot_does_not_value_late_ingest() {
    let conn = conn();
    let config = fixture(&conn);
    let plan = plan_observation(&conn, &config, "codex", 0, 48 * HOUR_MS).unwrap();
    let rates = rates_for(&[("openai", "m")], 100, None);
    let before = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates,
        &priced_plan(),
        48 * HOUR_MS,
        &[],
    )
    .unwrap();
    event(
        &conn,
        "late",
        "pi",
        "openai",
        "m",
        "subscription_included",
        HOUR_MS / 2,
        &[("input", "input", 999)],
    );
    let after = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates,
        &priced_plan(),
        48 * HOUR_MS,
        &[],
    )
    .unwrap();
    assert_eq!(before, after);
    assert_eq!(after[0].tokens.input, 1_000_000);
}

#[test]
fn v2_missing_quantity_is_unknown_not_zero() {
    let mut conn = conn();
    let mut config = stable_fixture(&conn);
    let account = config.quota_providers["codex"].accounts["acct"].clone();
    config
        .quota_providers
        .get_mut("codex")
        .unwrap()
        .accounts
        .insert("second".into(), account);
    conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at) SELECT provider,'second',pool_key,fetched_at_instant,used_percent,window_minutes,resets_at FROM window_sample WHERE account_ref='acct'", []).unwrap();
    let mut prices = priced_plan();
    let mut second_price = prices["acct"].clone();
    second_price.account_ref = "second".into();
    prices.insert("second".into(), second_price);
    v2_event(&conn, "unknown", Some("acct"), "m", HOUR_MS, None);
    conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id='unknown'),'output','token','billed','output',NULL,NULL)", []).unwrap();
    let calculate = |conn: &Connection| {
        let plan = plan_observation(conn, &config, "codex", 0, 72 * HOUR_MS).unwrap();
        compute_plan_observations(
            conn,
            &config,
            &plan,
            &rates_for(&[("openai", "m")], 100, None),
            &prices,
            72 * HOUR_MS,
            &["openai".into()],
        )
        .unwrap()
        .remove(0)
    };
    let row = calculate(&conn);
    assert!(row.routing_eligible);
    assert!(row.multiplier.is_some_and(|value| value > 0.0));
    assert!(row.fee_multiplier.is_some_and(|value| value > 0.0));
    assert!((row.clean_hours_share - 143.0 / 144.0).abs() < 1e-12);
    assert!((row.unpriced_share - 1.0 / 144.0).abs() < 1e-12);
    assert!((row.delta_percent - 143.0).abs() < 1e-9);
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
    for hour in (0..3).flat_map(|day| {
        (0..24)
            .filter(|hour| hour % 6 != 3)
            .take(13)
            .map(move |hour| day * 24 + hour)
    }) {
        v2_event(
            &conn,
            &format!("unknown-hour-{hour}"),
            Some("acct"),
            "m",
            hour * HOUR_MS,
            None,
        );
    }
    let high_unknown_share = calculate(&conn);
    assert!((high_unknown_share.unpriced_share - 39.0 / 144.0).abs() < 1e-12);
    assert!(
        high_unknown_share.routing_eligible,
        "{:?}",
        high_unknown_share.eligibility_reasons
    );
    assert!(high_unknown_share.eligibility_reasons.is_empty());
    assert!(high_unknown_share
        .multiplier
        .is_some_and(|value| value > 0.0));
    assert_eq!(high_unknown_share.api_nanos, 300);
    assert!((high_unknown_share.delta_percent - 105.0).abs() < 1e-9);
}

#[test]
fn v2_total_money_is_rounded_once_across_models() {
    let conn = conn();
    let config = fixture_quantity(&conn, Some(1));

    event(
        &conn,
        "second",
        "pi",
        "openai",
        "n",
        "subscription_included",
        HOUR_MS / 2,
        &[("input", "input", 1)],
    );
    let plan = plan_observation(&conn, &config, "codex", 0, 48 * HOUR_MS).unwrap();
    let rates = rates_for(&[("openai", "m"), ("openai", "n")], 500_000, None);
    let row = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates,
        &priced_plan(),
        48 * HOUR_MS,
        &[],
    )
    .unwrap()
    .remove(0);
    assert_eq!(row.api_nanos, 1);
}

#[test]
fn v2_antigravity_drops_only_the_frozen_wrong_vault_history() {
    let conn = conn();
    let cutoff = 1_792_523_280_000_i64;
    for (at, used, source) in [
        (cutoff - 600_000, 10.0, "oauth"),
        (cutoff - 300_000, 80.0, "vault"),
        (cutoff, 20.0, "vault"),
        (cutoff + 300_000, 25.0, "oauth"),
    ] {
        conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,resets_at,window_minutes,source) VALUES ('antigravity','acct','primary',?1,?2,'2026-11-01T00:00:00Z',10080,?3)", params![at * NANOS_PER_MS, used, source]).unwrap();
    }
    let measured = account_delta(
        &conn,
        "antigravity",
        "acct",
        &window(),
        cutoff - 600_000,
        cutoff + 600_000,
    )
    .unwrap();
    assert!((measured.delta_percent - 15.0).abs() < 1e-9);
    assert_eq!(measured.readings, 3);
}

#[test]
fn v2_attribution_uses_own_clean_hours_and_never_pools_plans() {
    let conn = conn();
    let mut config = fixture(&conn);
    let mut second = config.quota_providers["codex"].accounts["acct"].clone();
    config
        .quota_providers
        .get_mut("codex")
        .unwrap()
        .accounts
        .insert("second".into(), second.clone());
    for minute in (0..=48 * 60).step_by(5) {
        reset_reading(
            &conn,
            "second",
            minute * 60_000,
            if minute <= 60 { 100.0 } else { 10.0 },
            7 * 24 * HOUR_MS,
        );
    }
    v2_event(&conn, "matched", Some("acct"), "m", HOUR_MS / 2, Some(7));
    v2_event(&conn, "unmatched", None, "m", HOUR_MS / 2, Some(999));
    v2_event(&conn, "other", Some("second"), "m", 10 * HOUR_MS, Some(11));
    let same_plan = rows(&conn, &config).remove(0);
    assert_eq!(same_plan.tokens.input, 1_000_018);
    second.plan = "openai/pro_100".into();
    config
        .quota_providers
        .get_mut("codex")
        .unwrap()
        .accounts
        .insert("second".into(), second);
    let separate = rows(&conn, &config);
    assert_eq!(separate.len(), 2);
    let original = separate
        .iter()
        .find(|row| row.plan.as_deref() == Some("openai/pro_20x"))
        .unwrap();
    assert_eq!(original.tokens.input, 7);
    assert_eq!(original.accounts_included.len(), 1);
    assert!(original
        .eligibility_reasons
        .iter()
        .any(|reason| reason == "unattributed_plan_usage"));
}

#[test]
fn v2_eligibility_explains_every_quality_exclusion() {
    let conn = conn();
    let config = stable_fixture(&conn);
    let rates = rates_for(&[("openai", "m")], 100, None);
    let plan = plan_observation(&conn, &config, "codex", 0, 72 * HOUR_MS).unwrap();
    let calculate = |plan: &ObservationPlan, prices: &BTreeMap<String, PlanPriceUsed>| {
        compute_plan_observations(&conn, &config, plan, &rates, prices, 72 * HOUR_MS, &[])
            .unwrap()
            .remove(0)
    };
    assert!(calculate(&plan, &priced_plan()).routing_eligible);
    for (reason, change) in [
        ("saturation", 0),
        ("insufficient_clean_hours", 1),
        ("unknown_reset", 2),
        ("insufficient_points", 3),
    ] {
        let mut changed = plan.clone();
        for hour in changed.accounts[0].hours.values_mut() {
            match change {
                0 => hour.saturated = true,
                1 => hour.covered_ms = 0,
                2 => hour.reset_unknown = true,
                _ => hour.points = 0.0,
            }
        }
        let row = calculate(&changed, &priced_plan());
        assert!(!row.routing_eligible);
        assert!(
            row.eligibility_reasons
                .iter()
                .any(|actual| actual == reason),
            "{reason}: {:?}",
            row.eligibility_reasons
        );
        if change == 3 {
            assert!(row
                .eligibility_reasons
                .iter()
                .any(|reason| reason == "nonpositive_ratio"));
        }
        if change < 3 {
            assert!(row
                .eligibility_reasons
                .iter()
                .any(|reason| reason == "insufficient_clean_hours"));
        }
    }
    assert!(calculate(&plan, &BTreeMap::new())
        .eligibility_reasons
        .iter()
        .any(|reason| reason == "unpriced_plan"));
    let mut unknown = plan.clone();
    unknown.events[0].tokens.model = Some("premium".into());
    for hour in 0..48 {
        let mut event = unknown.events[0].clone();
        event.occurred_at_ms = hour * HOUR_MS;
        unknown.events.push(event);
    }
    let sample = calculate(&unknown, &priced_plan());
    assert!(sample.unpriced_share > 0.25);
    assert!(!sample.routing_eligible);
    assert_eq!(
        sample.eligibility_reasons,
        vec!["insufficient_stability_days"]
    );
}

#[test]
fn v2_pace_refuses_an_ineligible_head_and_plan_rows_keep_independent_chains() {
    let mut conn = conn();
    let config = fixture(&conn);
    let mut row = rows(&conn, &config).remove(0);
    row.routing_eligible = false;
    row.eligibility_reasons = vec!["gaps".into()];
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &row).unwrap();
    let mut second = row.clone();
    second.plan = Some("openai/pro_100".into());
    insert_observation_in_tx(&tx, &second).unwrap();
    let first_head = observation_plan_head(&tx, "codex", &row.observation_day, row.plan.as_deref())
        .unwrap()
        .unwrap();
    let superseded = insert_superseding_observation_in_tx(&tx, &second).unwrap();
    assert_ne!(superseded.supersedes, first_head);
    tx.commit().unwrap();
    let request = MultiplierPaceRequest {
        quota_provider: "codex".into(),
        plan: row.plan.clone(),
        from_ms: 0,
        to_ms: HOUR_MS,
    };
    assert_eq!(
        multiplier_pace(&conn, Some(&config), &request)
            .unwrap_err()
            .code(),
        "no_multiplier_observation"
    );
    let report = multiplier_report(
        &conn,
        &MultiplierReportRequest {
            from_ms: 0,
            to_ms: 49 * HOUR_MS,
        },
    )
    .unwrap();
    assert_eq!(report["series"].as_array().unwrap().len(), 2);
    assert_eq!(report["series"][0]["routing_eligible"], false);
}

/// Manual measurement only: callers supply a read-only clone and scope file.
/// No private input is embedded in the test or needed by CI.
#[test]
#[ignore = "manual local-only measurement on a private store clone"]
fn v2_measure_store_clone() {
    let path = std::env::var("ASTROCYTE_MULTIPLIER_STORE_CLONE").expect("store clone path");
    let config_path =
        std::env::var("ASTROCYTE_MULTIPLIER_MEASURE_CONFIG").expect("scope file path");
    let config: MultiplierConfig =
        serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    conn.execute_batch("PRAGMA query_only=ON; BEGIN;").unwrap();
    let mut rates = ResolvedRates {
        resolved_at_ms: utc_day_start_ms("2026-10-28").unwrap(),
        models: BTreeMap::new(),
    };
    let mut statement = conn.prepare("SELECT api_nanos_by_model FROM multiplier_observation ORDER BY snapshot_at_ms,observation_id").unwrap();
    for json in statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
    {
        for line in serde_json::from_str::<Vec<PaceRateLine>>(&json.unwrap()).unwrap() {
            if let (Some(model), Some(resolution)) = (line.model, line.rates) {
                rates.models.insert(
                    (catalog_provider(&config, &line.provider).to_string(), model),
                    resolution,
                );
            }
        }
    }
    for day in 22..=28 {
        let day = format!("2026-10-{day}");
        let end = utc_day_start_ms(&day).unwrap();
        for (provider, declaration) in &config.quota_providers {
            let plan = plan_observation(&conn, &config, provider, end - 7 * DAY, end).unwrap();
            let prices = resolve_plans(None, &config, declaration, rates.resolved_at_ms).unwrap();
            let rows = compute_plan_observations(
                &conn,
                &config,
                &plan,
                &rates,
                &prices,
                rates.resolved_at_ms,
                &[
                    "openai".into(),
                    "anthropic".into(),
                    "meta".into(),
                    "google".into(),
                ],
            )
            .unwrap();
            for mut row in rows {
                let mut sources = conn.prepare("SELECT source_key FROM source_runtime WHERE last_success_ms IS NULL OR last_success_ms<?1 OR pending_known>0 OR pending_unknown!=0").unwrap();
                row.source_backlog = sources
                    .query_map([end], |row| row.get::<_, String>(0))
                    .unwrap()
                    .map(Result::unwrap)
                    .filter(|source| source_in_scope(declaration, source))
                    .collect();
                if !row.source_backlog.is_empty() {
                    row.routing_eligible = false;
                    row.eligibility_reasons.push("source_backlog".into());
                }
                println!(
                    "MEASURE {}",
                    json!({"day": day, "provider": provider, "plan": row.plan, "multiplier": row.multiplier, "dollars_per_point": (row.delta_percent>0.0).then(|| row.api_nanos as f64 / 1e9 / row.delta_percent), "points": row.delta_percent, "list_dollars": row.api_nanos as f64 / 1e9, "routing_eligible": row.routing_eligible, "reasons": row.eligibility_reasons, "clean_share": row.clean_hours_share, "saturated_share": row.saturated_share, "gap_share": row.gap_share, "unpriced_share": row.unpriced_share, "unconfigured_count": row.unconfigured_accounts.len(), "source_backlog": row.source_backlog, "zero_measurement_events": row.zero_measurement_events, "missing_quantity_events": row.missing_quantity_events, "unattributed_event_share": row.unattributed_event_share, "unpriced_tokens": row.unpriced_tokens, "plan_calibration": row.accounts_included.first().and_then(|account| account.get("plan_calibration")), "diagnostic_multiplier": (row.subscription_nanos > 0).then(|| row.api_nanos as f64 / row.subscription_nanos as f64)})
                );
            }
        }
    }
    conn.execute_batch("ROLLBACK;").unwrap();
}

#[test]
fn v2_zero_measurement_events_are_unknown_not_free_usage() {
    let mut conn = conn();
    let mut config = stable_fixture(&conn);
    let account = config.quota_providers["codex"].accounts["acct"].clone();
    config
        .quota_providers
        .get_mut("codex")
        .unwrap()
        .accounts
        .insert("second".into(), account);
    conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at) SELECT provider,'second',pool_key,fetched_at_instant,used_percent,window_minutes,resets_at FROM window_sample WHERE account_ref='acct'", []).unwrap();
    let mut prices = priced_plan();
    let mut second_price = prices["acct"].clone();
    second_price.account_ref = "second".into();
    prices.insert("second".into(), second_price);
    conn.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis) VALUES ('empty','pi','lineage','empty',?1,?1,'openai','m','unknown')", [HOUR_MS]).unwrap();
    let calculate = |conn: &Connection| {
        let plan = plan_observation(conn, &config, "codex", 0, 72 * HOUR_MS).unwrap();
        compute_plan_observations(
            conn,
            &config,
            &plan,
            &rates_for(&[("openai", "m")], 100, None),
            &prices,
            72 * HOUR_MS,
            &["openai".into()],
        )
        .unwrap()
        .remove(0)
    };
    let row = calculate(&conn);
    assert!(row.routing_eligible);
    assert!(row.multiplier.is_some_and(|value| value > 0.0));
    assert!((row.clean_hours_share - 71.0 / 72.0).abs() < 1e-12);
    assert!((row.unpriced_share - 1.0 / 72.0).abs() < 1e-12);
    assert!((row.delta_percent - 142.0).abs() < 1e-9);
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
    for hour in (0..3).flat_map(|day| {
        (0..24)
            .filter(|hour| hour % 6 != 3)
            .take(7)
            .map(move |hour| day * 24 + hour)
    }) {
        conn.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis) VALUES (?1,'pi','lineage',?1,?2,?2,'openai','m','unknown')", params![format!("empty-hour-{hour}"), hour * HOUR_MS]).unwrap();
    }
    let high_unknown_share = calculate(&conn);
    assert!((high_unknown_share.unpriced_share - 21.0 / 72.0).abs() < 1e-12);
    assert!(
        high_unknown_share.routing_eligible,
        "{:?}",
        high_unknown_share.eligibility_reasons
    );
    assert!(high_unknown_share.eligibility_reasons.is_empty());
    assert!(high_unknown_share
        .multiplier
        .is_some_and(|value| value > 0.0));
    assert_eq!(high_unknown_share.api_nanos, 300);
    assert!((high_unknown_share.delta_percent - 102.0).abs() < 1e-9);
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
    let measured = account_delta(&conn, "codex", "acct", &window(), 0, HOUR_MS).unwrap();
    assert!((measured.delta_percent - 10.0).abs() < 1e-9);
    assert_eq!(measured.stale_reset_readings, 1);
}

#[test]
fn v2_unresolved_plan_price_is_not_a_partial_multiplier() {
    let conn = conn();
    let mut config = fixture(&conn);
    let account = config.quota_providers["codex"].accounts["acct"].clone();
    config
        .quota_providers
        .get_mut("codex")
        .unwrap()
        .accounts
        .insert("unpriced-account".into(), account);
    conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at) SELECT provider,'unpriced-account',pool_key,fetched_at_instant,used_percent,window_minutes,resets_at FROM window_sample", []).unwrap();
    let row = rows(&conn, &config).remove(0);
    assert!(row.multiplier.is_none() && row.fee_multiplier.is_none());
    assert!(row
        .eligibility_reasons
        .iter()
        .any(|reason| reason == "unpriced_plan"));
    assert_eq!(row.api_nanos, 100);
    assert!(row.subscription_nanos > 0);
}

#[test]
fn v2_quiet_gaps_are_covered_but_changed_gaps_are_unknown() {
    let conn = conn();
    let horizon = 7 * 24 * HOUR_MS;
    for (account, before, after, reset) in [
        ("quiet", 10.0, 10.0, horizon + 700),
        ("changed", 10.0, 20.0, horizon),
        ("reset", 10.0, 10.0, horizon + 7 * 24 * HOUR_MS),
        ("capped", 100.0, 100.0, horizon),
    ] {
        reset_reading(&conn, account, 0, before, horizon);
        reset_reading(&conn, account, 4 * HOUR_MS, after, reset);
    }
    let quiet = account_delta(&conn, "codex", "quiet", &window(), HOUR_MS, 3 * HOUR_MS).unwrap();
    assert_eq!(quiet.coverage_ms, Some((HOUR_MS, 3 * HOUR_MS)));
    assert!((quiet.delta_percent).abs() < 1e-12);
    assert_eq!(quiet.hours[&HOUR_MS].covered_ms, HOUR_MS);
    assert_eq!(quiet.hours[&(2 * HOUR_MS)].covered_ms, HOUR_MS);
    for account in ["changed", "reset"] {
        let unknown =
            account_delta(&conn, "codex", account, &window(), HOUR_MS, 3 * HOUR_MS).unwrap();
        assert!(unknown.coverage_ms.is_none());
        assert!(unknown.hours.values().all(|hour| hour.covered_ms == 0));
        assert!(unknown.delta_percent.abs() < 1e-12);
    }
    let capped = account_delta(&conn, "codex", "capped", &window(), HOUR_MS, 3 * HOUR_MS).unwrap();
    assert_eq!(capped.hours[&HOUR_MS].covered_ms, HOUR_MS);
    assert!(capped.hours[&HOUR_MS].saturated);
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
    let measured = account_delta(&conn, "codex", "acct", &window(), 0, HOUR_MS).unwrap();
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
    let window = WindowSelector {
        pool_key: "primary".into(),
        window_minutes: 1,
    };
    let measured = account_delta(&conn, "codex", "acct", &window, 0, 120_000).unwrap();
    assert!((measured.delta_percent - 20.0).abs() < 1e-9);
    assert_eq!(measured.reset_boundaries, 1);
}

#[test]
fn v2_adequate_stable_sample_does_not_need_half_the_week() {
    let mut conn = conn();
    let config = config(vec![rule("pi", "openai", None)]);
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
    let plan = plan_observation(&conn, &config, "codex", 0, 7 * day_ms).unwrap();
    let row = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates_for(&[("openai", "m")], 100, None),
        &priced_plan(),
        7 * day_ms,
        &[],
    )
    .unwrap()
    .remove(0);
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
    let config = config(vec![rule("pi", "openai", None)]);
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
    let plan = plan_observation(&conn, &config, "codex", 0, 7 * day_ms).unwrap();
    let row = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates_for(&[("openai", "m")], 100, None),
        &priced_plan(),
        7 * day_ms,
        &[],
    )
    .unwrap()
    .remove(0);
    assert!(row.routing_eligible, "{:?}", row.eligibility_reasons);
    assert!((row.gap_share - 5.0 / 6.0).abs() < 1e-12);
    assert!((row.clean_hours_share - 1.0 / 6.0).abs() < 1e-12);
    assert!((row.delta_percent - 28.0).abs() < 1e-9);
    assert_eq!(row.api_nanos, 700);
}

#[test]
fn v2_variable_day_calibration_is_not_routing_eligible() {
    let mut conn = conn();
    let config = stability_fixture(&conn, [1, 10, 1]);
    let plan = plan_observation(&conn, &config, "codex", 0, 72 * HOUR_MS).unwrap();
    let row = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates_for(&[("openai", "m")], 100, None),
        &priced_plan(),
        72 * HOUR_MS,
        &[],
    )
    .unwrap()
    .remove(0);
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
fn v2_two_days_do_not_establish_day_block_stability() {
    let conn = conn();
    let config = stable_fixture(&conn);
    let row = rows(&conn, &config).remove(0);
    assert!(!row.routing_eligible);
    assert_eq!(row.eligibility_reasons, vec!["insufficient_stability_days"]);
    assert_eq!(row.api_nanos, 200);
    assert!((row.delta_percent - 48.0).abs() < 1e-9);
}

#[test]
fn v2_day_blocks_keep_account_value_and_points_paired() {
    let conn = conn();
    let config = config(vec![rule("pi", "openai", None)]);
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
    let plan = plan_observation(&conn, &config, "codex", 0, 72 * HOUR_MS).unwrap();
    let row = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates_for(&[("openai", "m")], 100, None),
        &priced_plan(),
        72 * HOUR_MS,
        &[],
    )
    .unwrap()
    .remove(0);
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
    let config = stable_fixture(&conn);
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
    let plan = plan_observation(&conn, &config, "codex", 0, 72 * HOUR_MS).unwrap();
    let row = compute_plan_observations(
        &conn,
        &config,
        &plan,
        &rates_for(&[("openai", "m")], 100, None),
        &priced_plan(),
        72 * HOUR_MS,
        &[],
    )
    .unwrap()
    .remove(0);
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
