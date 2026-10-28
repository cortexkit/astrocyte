
fn reset_reading(conn: &Connection, account: &str, at: i64, used: f64, reset_ms: i64) {
    reading(conn, account, at, used, None);
    let reset = chrono::DateTime::from_timestamp_millis(reset_ms).unwrap().to_rfc3339();
    conn.execute("UPDATE window_sample SET resets_at=?1 WHERE account_ref=?2 AND fetched_at_instant=?3", params![reset, account, at * NANOS_PER_MS]).unwrap();
}

fn v2_event(conn: &Connection, id: &str, account: Option<&str>, model: &str, at: i64, quantity: Option<i64>) {
    conn.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES (?1,'pi','lineage',?1,?2,?2,'openai',?3,'subscription_included',?4)", params![id, at, model, account]).unwrap();
    conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','input',?2,0)", params![id, quantity]).unwrap();
}

fn fixture(conn: &Connection) -> MultiplierConfig { fixture_quantity(conn, Some(1_000_000)) }

fn fixture_quantity(conn: &Connection, quantity: Option<i64>) -> MultiplierConfig {
    let config = config(vec![rule("pi", "openai", None)]);
    for minute in (0..=48 * 60).step_by(5) {
        reset_reading(conn, "acct", minute * 60_000, minute as f64 / 60.0, 7 * 24 * HOUR_MS);
    }
    v2_event(conn, "priced", None, "m", 10 * HOUR_MS, quantity);
    config
}

fn rows(conn: &Connection, config: &MultiplierConfig) -> Vec<Observation> {
    let plan = plan_observation(conn, config, "codex", 0, 48 * HOUR_MS).unwrap();
    compute_plan_observations(conn, config, &plan, &rates_for(&[("openai", "m")], 100, None), &priced_plan(), 48 * HOUR_MS, &["openai".into()]).unwrap()
}

#[test]
fn v2_reset_periods_ignore_rounding_flaps_and_jitter_but_count_new_first_reading() {
    let conn = conn();
    let reset = 7 * 24 * HOUR_MS;
    for (minute, used, horizon) in [(0, 60.0, reset), (5, 59.0, reset), (10, 61.0, reset), (15, 0.0, reset), (20, 61.0, reset), (25, 0.0, reset), (30, 61.0, reset + 700), (35, 80.0, reset + 7 * 24 * HOUR_MS)] {
        reset_reading(&conn, "acct", minute * 60_000, used, horizon);
    }
    let measured = account_delta(&conn, "codex", "acct", &window(), 0, HOUR_MS).unwrap();
    assert!((measured.delta_percent - 81.0).abs() < 1e-9);
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
    conn.execute("DELETE FROM window_sample WHERE fetched_at_instant>?1 AND fetched_at_instant<?2", params![3 * HOUR_MS * NANOS_PER_MS, 4 * HOUR_MS * NANOS_PER_MS]).unwrap();
    event(&conn, "premium", "pi", "openai", "unpriced", "subscription_included", 5 * HOUR_MS, &[("input", "input", 1)]);
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
    assert!(row.eligibility_reasons.iter().any(|reason| reason == "unconfigured_accounts"));
    let account = config.quota_providers["codex"].accounts["acct"].clone();
    config.quota_providers.get_mut("codex").unwrap().accounts.insert("new-account".into(), account);
    assert!(rows(&conn, &config)[0].unconfigured_accounts.is_empty());
}

#[test]
fn v2_snapshot_does_not_value_late_ingest() {
    let conn = conn();
    let config = fixture(&conn);
    let plan = plan_observation(&conn, &config, "codex", 0, 48 * HOUR_MS).unwrap();
    let rates = rates_for(&[("openai", "m")], 100, None);
    let before = compute_plan_observations(&conn, &config, &plan, &rates, &priced_plan(), 48 * HOUR_MS, &[]).unwrap();
    event(&conn, "late", "pi", "openai", "m", "subscription_included", HOUR_MS / 2, &[("input", "input", 999)]);
    let after = compute_plan_observations(&conn, &config, &plan, &rates, &priced_plan(), 48 * HOUR_MS, &[]).unwrap();
    assert_eq!(before, after);
    assert_eq!(after[0].tokens.input, 1_000_000);
}

#[test]
fn v2_missing_quantity_is_unknown_not_zero() {
    let conn = conn();
    let config = fixture_quantity(&conn, None);
    let row = rows(&conn, &config).remove(0);
    assert!(row.multiplier.is_none() && row.fee_multiplier.is_none());
    assert!(!row.routing_eligible);
    assert!(row.eligibility_reasons.iter().any(|reason| reason == "missing_quantity"));
}

#[test]
fn v2_total_money_is_rounded_once_across_models() {
    let conn = conn();
    let config = fixture_quantity(&conn, Some(1));
    
    event(&conn, "second", "pi", "openai", "n", "subscription_included", HOUR_MS / 2, &[("input", "input", 1)]);
    let plan = plan_observation(&conn, &config, "codex", 0, 48 * HOUR_MS).unwrap();
    let rates = rates_for(&[("openai", "m"), ("openai", "n")], 500_000, None);
    let row = compute_plan_observations(&conn, &config, &plan, &rates, &priced_plan(), 48 * HOUR_MS, &[]).unwrap().remove(0);
    assert_eq!(row.api_nanos, 1);
}
