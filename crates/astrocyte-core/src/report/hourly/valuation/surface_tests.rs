use super::*;
use fees::{FeeDays, DAY_MS};

fn request(grouping: &[&str], from: i64, to: i64) -> ReportRequest {
    ReportRequest {
        grouping: grouping.iter().map(|s| (*s).into()).collect(),
        grain: "day".into(),
        timezone: "UTC".into(),
        from_ms: Some(from),
        to_ms: Some(to),
        ..ReportRequest::default()
    }
}
fn report(conn: &Connection, request: &ReportRequest) -> serde_json::Value {
    serde_json::to_value(spend_report(conn, request, 10 * DAY_MS).unwrap()).unwrap()
}
fn old_members(mut value: serde_json::Value) -> serde_json::Value {
    for key in ["rows", "totals"] {
        for row in value[key].as_array_mut().unwrap() {
            for member in [
                "list_value",
                "deduced",
                "deduced_unspread_nanos",
                "deduced_non_usd_plans",
                "deduced_conflict_plans",
            ] {
                row.as_object_mut().unwrap().remove(member);
            }
        }
    }
    value
}
fn ready(conn: &mut Connection) {
    summaries(conn);
    drain(conn, &config());
}

#[test]
fn ledger_regression_usd_fee_survives_null_recorded_currency_in_both_readers() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    event(
        &mut conn,
        "unknown-basis",
        1,
        "A",
        Some("account"),
        None,
        "unknown",
        100,
    );
    ready(&mut conn);

    for grouping in [&[][..], &["alfonso"][..]] {
        let value = report(&conn, &request(grouping, 0, DAY_MS));
        assert_eq!(value["rows"][0]["list_value"]["nanos"], 200_000);
        assert!(value["rows"][0]["currency"].is_null());
        assert_eq!(
            value["rows"][0]["deduced"],
            json!({"currency": "USD", "nanos": 645_161_290, "pending_days": 0})
        );
        let usd = value["totals"]
            .as_array()
            .unwrap()
            .iter()
            .find(|total| total["currency"] == "USD")
            .unwrap();
        assert_eq!(usd["total_nanos"], 0);
        assert_eq!(usd["list_value"]["nanos"], 0);
        assert_eq!(usd["deduced"]["nanos"], 645_161_290);
        assert_eq!(usd["deduced_unspread_nanos"], 0);
    }
}

#[test]
fn report_surface_non_usd_row_fee_share_is_only_in_usd_total() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    let key = event(
        &mut conn,
        "covered-eur",
        1,
        "A",
        Some("account"),
        None,
        "subscription_included",
        100,
    );
    // Recorded currency does not decide whether the call matches a USD plan.
    conn.execute(
        "UPDATE usage_event SET currency='EUR' WHERE event_key=?1",
        [key],
    )
    .unwrap();
    ready(&mut conn);

    for grouping in [&[][..], &["alfonso"][..]] {
        let value = report(&conn, &request(grouping, 0, DAY_MS));
        let rows = value["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["currency"], "EUR");
        assert!(rows[0].get("deduced").is_none());
        let usd = value["totals"]
            .as_array()
            .unwrap()
            .iter()
            .find(|total| total["currency"] == "USD")
            .unwrap();
        assert_eq!(usd["total_nanos"], 0);
        assert_eq!(usd["deduced"]["nanos"], 645_161_290);
        assert_eq!(usd["deduced_unspread_nanos"], 0);
    }
}

#[test]
fn report_surface_plan_grouping_filters_and_unfiltered_fee_weights() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    rate(&conn, "B", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    // Only one quarter of this record's daily weight matches the filtered row.
    for (id, at, model, account, quantity) in [
        ("a", 1, "A", Some("account"), 100),
        ("b", 12 * HOUR_MS, "B", Some("account"), 300),
        ("rule", 2, "A", None, 100),
    ] {
        let key = event(
            &mut conn,
            id,
            at,
            model,
            account,
            None,
            "subscription_included",
            quantity,
        );
        recorded_price(&conn, key, 0);
    }
    ready(&mut conn);
    let mut req = request(&["plan", "plan_account", "model"], 0, DAY_MS);
    req.filters.insert("model".into(), vec![Some("A".into())]);
    req.filters
        .insert("plan_account".into(), vec![Some("account".into())]);
    let value = report(&conn, &req);
    assert_eq!(value["rows"].as_array().unwrap().len(), 1);
    let row = &value["rows"][0];
    assert_eq!(row["plan"], "pool");
    assert_eq!(row["plan_account"], "account");
    assert_eq!(row["list_value"]["nanos"], 200_000);
    assert_eq!(row["deduced"]["nanos"], 161_290_323);
    assert!(row["deduced"].get("by_rule_share").is_none());
    assert!(value["totals"][0].get("deduced_unspread_nanos").is_none());
    req.filters.clear();
    req.filters.insert("plan".into(), vec![Some("pool".into())]);
    let all = report(&conn, &req);
    assert_eq!(all["totals"][0]["deduced"]["nanos"], 645_161_290);
    assert_eq!(all["totals"][0]["deduced_unspread_nanos"], 0);
    // A record with no own calls routes its fee to the rule-matched calls.
    plan(&conn, PlanLedger::Account, "provider", "unused", 0, None);
    ready(&mut conn);
    req.filters
        .insert("plan_account".into(), vec![Some("account_unknown".into())]);
    let by_rule = report(&conn, &req);
    assert_eq!(by_rule["rows"][0]["deduced"]["nanos"], 645_161_290);
    assert_eq!(by_rule["rows"][0]["deduced"]["by_rule_share"], 1.0);
}

#[test]
fn audit17_report_plan_filters_also_filter_dated_coverage() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    let key = event(&mut conn, "api", 1, "A", None, None, "metered_api", 100);
    recorded_price(&conn, key, 123);
    ready(&mut conn);

    let mut req = request(&["plan"], 0, DAY_MS);
    let all = report(&conn, &req);
    assert_eq!(all["undated"]["events_report_total"], 1);
    assert_eq!(all["rate_staleness"]["measurements_total"], 2);
    req.filters.insert("plan".into(), vec![Some("pool".into())]);
    let excluded = report(&conn, &req);
    assert_eq!(excluded["rows"], serde_json::json!([]));
    assert_eq!(excluded["totals"], serde_json::json!([]));
    assert_eq!(excluded["undated"]["events_report_total"], 0);
    assert_eq!(excluded["rate_staleness"]["measurements_total"], 0);
    assert_eq!(excluded["rate_staleness"]["priced_measurements"], 0);
    assert!(excluded["rate_staleness"]["oldest_resolved_at_ms"].is_null());
}

#[test]
fn report_surface_undated_coverage_is_excluded_only_by_explicit_plan_filters() {
    let mut conn = conn();
    let request = serde_json::from_value(serde_json::json!({
        "producer":"fixture","producer_ref":"undated","charge_basis":"metered_api",
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture",
            "quantity_units":100,"quantity_exponent":0}]
    }))
    .unwrap();
    crate::ingest::record_event(&mut conn, &request, 1).unwrap();
    ready(&mut conn);
    let mut req = ReportRequest {
        grouping: vec!["plan".into()],
        grain: "day".into(),
        timezone: "UTC".into(),
        ..ReportRequest::default()
    };
    let grouped = report(&conn, &req);
    assert_eq!(grouped["undated"]["events_undated"], 1);
    assert_eq!(grouped["rate_staleness"]["measurements_total"], 1);
    req.filters.insert("plan_account".into(), vec![None]);
    let filtered = report(&conn, &req);
    assert_eq!(filtered["undated"]["events_undated"], 0);
    assert_eq!(filtered["undated"]["events_report_total"], 0);
    assert_eq!(filtered["undated"]["reasons"], serde_json::json!({}));
    assert_eq!(filtered["rate_staleness"]["measurements_total"], 0);
}

#[test]
fn audit17_report_unspread_fee_uses_only_the_active_part_of_the_day() {
    let conn = conn();
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        DAY_MS / 2,
        None,
    );
    let unspread = |from, to| {
        let mut req = request(&[], from, to);
        req.grain = "hour".into();
        report(&conn, &req)["totals"][0]["deduced_unspread_nanos"].clone()
    };
    assert_eq!(unspread(0, DAY_MS / 2), 0);
    let full_day = unspread(0, DAY_MS);
    assert!(full_day.as_i64().unwrap() > 0);
    assert_eq!(unspread(DAY_MS / 2, DAY_MS), full_day);
}

#[test]
fn report_surface_pending_hours_preserve_recorded_spend_and_distinct_counts() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    for (id, at) in [("a", 1), ("b", 2), ("c", HOUR_MS + 1)] {
        let key = event(&mut conn, id, at, "A", None, None, "metered_api", 100);
        recorded_price(&conn, key, 123);
    }
    summaries(&mut conn);
    prepare(&mut conn, &config());
    let mut req = request(&["plan", "plan_account"], 0, DAY_MS);
    let pending = report(&conn, &req);
    assert_eq!(pending["rows"][0]["plan"], "pending");
    assert!(pending["rows"][0]["plan_account"].is_null());
    assert_eq!(pending["rows"][0]["total_nanos"], 369);
    assert_eq!(pending["rows"][0]["list_value"]["not_yet_valued_hours"], 2);
    assert_eq!(
        pending["totals"][0]["list_value"]["not_yet_valued_hours"],
        2
    );
    req.filters.insert("plan".into(), vec![Some("api".into())]);
    assert!(report(&conn, &req)["rows"].as_array().unwrap().is_empty());
    req = request(&["model"], 0, DAY_MS);
    let ordinary = report(&conn, &req);
    assert_eq!(ordinary["rows"][0]["total_nanos"], 369);
    assert_eq!(ordinary["rows"][0]["list_value"]["nanos"], 0);
    assert_eq!(ordinary["rows"][0]["list_value"]["not_yet_valued_hours"], 2);
    drain(&mut conn, &config());
    let built = report(&conn, &req);
    assert_eq!(built["rows"][0]["list_value"]["nanos"], 600_000);
    assert_eq!(built["rows"][0]["list_value"]["not_yet_valued_hours"], 0);
}

#[test]
fn report_surface_dirty_fallback_matches_built_across_five_timezones() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    let mut seed = 8192_u64;
    for index in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let at = i64::try_from(seed % u64::try_from(3 * DAY_MS).unwrap()).unwrap();
        let key = event(
            &mut conn,
            &format!("call-{index}"),
            at,
            "A",
            Some("account"),
            None,
            "metered_api",
            i64::try_from(seed % 100 + 2).unwrap(),
        );
        recorded_price(&conn, key, 7);
    }
    ready(&mut conn);
    let zones = [
        "UTC",
        "America/New_York",
        "Asia/Kolkata",
        "Asia/Kathmandu",
        "Australia/Lord_Howe",
    ];
    let mut expected = Vec::new();
    for zone in zones {
        let mut req = request(&["plan", "plan_account", "model"], 0, 3 * DAY_MS);
        req.timezone = zone.into();
        expected.push((req.clone(), report(&conn, &req)));
    }
    conn.execute("UPDATE summary_hour SET revision=revision+1", [])
        .unwrap();
    for (req, built) in expected {
        assert_eq!(report(&conn, &req), built, "{}", req.timezone);
    }
}

#[test]
fn report_surface_existing_members_are_byte_identical() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    for (id, at, basis) in [
        ("metered", 1, "metered_api"),
        ("covered", 5 * HOUR_MS, "subscription_included"),
    ] {
        let key = event(&mut conn, id, at, "A", Some("account"), None, basis, 20);
        recorded_price(&conn, key, 71);
    }
    ready(&mut conn);
    for zone in ["UTC", "Asia/Kolkata"] {
        let mut req = request(&["provider", "model", "charge_basis"], 0, DAY_MS);
        req.timezone = zone.into();
        let before =
            serde_json::to_value(recorded_spend_report(&conn, &req, 10 * DAY_MS).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_vec(&old_members(report(&conn, &req))).unwrap(),
            serde_json::to_vec(&old_members(before)).unwrap()
        );
    }
}

#[test]
fn report_surface_uses_stored_config_and_defaults_missing_bundle_counter() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    let key = event(
        &mut conn,
        "a",
        1,
        "A",
        None,
        None,
        "subscription_included",
        10,
    );
    recorded_price(&conn, key, 0);
    ready(&mut conn);
    let req = request(&["plan"], 0, DAY_MS);
    assert_eq!(report(&conn, &req)["rows"][0]["plan"], "pool");
    let mut freshly_loaded = config();
    freshly_loaded.quota_providers.clear();
    assert_ne!(
        surface::stored_config(&conn).unwrap().0.quota_providers,
        freshly_loaded.quota_providers
    );
    conn.execute("UPDATE summary_hour SET revision=revision+1", [])
        .unwrap();
    assert_eq!(
        report(&conn, &req)["rows"][0]["plan"],
        "pool",
        "dirty fallback follows the stored builder snapshot"
    );
    ready(&mut conn);
    conn.execute("UPDATE valuation_cell SET recorded=json_remove(recorded,'$.base_rate_tier_untested_tokens')", []).unwrap();
    let value = report(&conn, &req);
    assert!(value["rows"][0]["list_value"]
        .get("base_rate_tier_untested_tokens")
        .is_none());
}

#[test]
fn report_surface_range_free_fees_cover_empty_ledger_and_days_after_last_call() {
    for first_call in [None, Some(2 * DAY_MS + 1)] {
        let mut conn = conn();
        plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
        if let Some(at) = first_call {
            rate(&conn, "A", 1_000_000_000);
            let key = event(&mut conn, "api", at, "A", None, None, "metered_api", 100);
            recorded_price(&conn, key, 25);
        }
        ready(&mut conn);
        let req = ReportRequest {
            grouping: vec!["model".into()],
            grain: "day".into(),
            timezone: "UTC".into(),
            ..ReportRequest::default()
        };
        let value = report(&conn, &req);
        let total = &value["totals"][0];
        assert_eq!(total["currency"], "USD");
        // now is day 10. That day is pending; days before the ledger's first
        // bucket are excluded, while completed days after its last call remain.
        assert_eq!(
            total["deduced_unspread_nanos"],
            if first_call.is_some() {
                5_161_290_323_i64
            } else {
                0
            }
        );
        assert_eq!(
            total["total_nanos"],
            if first_call.is_some() { 25 } else { 0 }
        );
        if first_call.is_none() {
            assert!(value["rows"].as_array().unwrap().is_empty());
        }
    }
    let mut conn = conn();
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    ready(&mut conn);
    let value = report(&conn, &request(&[], 0, DAY_MS));
    assert_eq!(value["totals"][0]["deduced_unspread_nanos"], 645_161_290);
}

#[test]
fn report_surface_currency_totals_expose_fees_once_and_do_not_add_empty_usd() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    let mut req: ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"opencode", "producer_ref":"eur", "provider":"provider", "model":"A", "occurred_at_ms":1, "charge_basis":"metered_api",
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":100,"quantity_exponent":0}]
    })).unwrap();
    let result = ingest::record_event(&mut conn, &req, 1).unwrap();
    conn.execute(
        "UPDATE usage_event SET currency='EUR' WHERE event_id=?1",
        [result.event_id],
    )
    .unwrap();
    ready(&mut conn);
    let query = request(&[], 0, DAY_MS);
    let no_fee = report(&conn, &query);
    assert_eq!(no_fee["totals"].as_array().unwrap().len(), 1);
    assert_eq!(no_fee["totals"][0]["currency"], "EUR");
    assert!(no_fee["totals"][0].get("deduced").is_none());
    assert!(no_fee["totals"][0].get("deduced_non_usd_plans").is_none());
    assert!(no_fee["totals"][0]["list_value"].is_object());
    plan(&conn, PlanLedger::Account, "provider", "unused", 0, None);
    ready(&mut conn);
    let fee_only = report(&conn, &query);
    assert_eq!(fee_only["totals"].as_array().unwrap().len(), 2);
    assert_eq!(fee_only["totals"][1]["currency"], "USD");
    assert_eq!(fee_only["totals"][1]["deduced_unspread_nanos"], 645_161_290);
    req.producer_ref = "null".into();
    ingest::record_event(&mut conn, &req, 1).unwrap();
    let key = event(&mut conn, "usd", 2, "A", None, None, "metered_api", 100);
    recorded_price(&conn, key, 123);
    ready(&mut conn);
    let mixed = report(&conn, &query);
    assert_eq!(mixed["totals"].as_array().unwrap().len(), 3);
    assert_eq!(
        mixed["totals"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t.get("deduced_unspread_nanos").is_some())
            .count(),
        1
    );
    assert_eq!(mixed["totals"][2]["total_nanos"], 123);
}

#[test]
fn report_surface_bundle_counter_is_served_in_rows_and_totals() {
    let mut conn = conn();
    let history = serde_json::from_value(json!({
        "source":"synthetic", "provider_id":"provider", "model_id":"A", "fact_prefix":"rate.", "eras":[],
        "served_by_key":{
            "rate.input":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":1000000000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}],
            "rate.input.above_context.100":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":3000000000_i64,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}]
        }
    })).unwrap();
    publish(&conn, &fetched(history, "provider", "A", None).unwrap(), 1).unwrap();
    let req = serde_json::from_value(json!({
        "producer":"broca", "producer_ref":"bundle", "provider":"provider", "model":"A", "occurred_at_ms":1, "charge_basis":"metered_api",
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":101,"quantity_exponent":0}]
    })).unwrap();
    let result = ingest::record_event(&mut conn, &req, 1).unwrap();
    conn.execute(
        "UPDATE usage_event SET currency='USD' WHERE event_id=?1",
        [result.event_id],
    )
    .unwrap();
    ready(&mut conn);
    let query = request(&[], 0, DAY_MS);
    let value = report(&conn, &query);
    for key in ["rows", "totals"] {
        assert_eq!(value[key][0]["list_value"]["nanos"], 101_000);
        assert_eq!(
            value[key][0]["list_value"]["base_rate_tier_untested_tokens"],
            101
        );
        assert_eq!(value[key][0]["list_value"]["unvalued"]["measurements"], 0);
    }
    conn.execute("UPDATE summary_hour SET revision=revision+1", [])
        .unwrap();
    assert_eq!(report(&conn, &query), value);
}

#[test]
fn report_surface_dirty_pool_day_defers_fees_even_when_live_rows_are_valued() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    for (id, at) in [("a", 1), ("b", 12 * HOUR_MS)] {
        let key = event(
            &mut conn,
            id,
            at,
            "A",
            Some("account"),
            None,
            "subscription_included",
            100,
        );
        recorded_price(&conn, key, 0);
    }
    ready(&mut conn);
    let query = request(&["plan"], 0, DAY_MS);
    let built = report(&conn, &query);
    assert_eq!(built["rows"][0]["deduced"]["nanos"], 645_161_290);
    conn.execute(
        "UPDATE summary_hour SET revision=revision+1 WHERE hour_number=12",
        [],
    )
    .unwrap();
    let dirty = report(&conn, &query);
    assert_eq!(
        dirty["rows"][0]["list_value"],
        built["rows"][0]["list_value"]
    );
    assert_eq!(dirty["rows"][0]["deduced"]["pending_days"], 1);
    assert_eq!(dirty["rows"][0]["deduced"]["nanos"], 0);
    assert_eq!(dirty["totals"][0]["deduced"]["pending_days"], 1);
    assert_eq!(dirty["totals"][0]["deduced_unspread_nanos"], 0);
    summaries(&mut conn);
    conn.execute(
        "INSERT OR IGNORE INTO summary_dirty_hour(kind,hour_number) VALUES(0,12)",
        [],
    )
    .unwrap();
    summaries(&mut conn);
    let pending = report(&conn, &query);
    assert!(pending["rows"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["plan"] == "pending"));
    assert_eq!(pending["totals"][0]["deduced"]["pending_days"], 1);
    drain(&mut conn, &config());
    assert_eq!(report(&conn, &query), built);
}

#[test]
fn report_surface_plan_diagnostics_and_unfetched_hours() {
    let mut conn = conn();
    let mut config = config();
    let mut second = config.quota_providers["pool"].clone();
    second.scope[0].producer = "pi".into();
    config.quota_providers.insert("second".into(), second);
    plan(&conn, PlanLedger::Account, "provider", "conflict", 0, None);
    conn.execute("INSERT INTO account_subscription(account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms) VALUES('euro','other','plan',100,'EUR',2,'month',0)", []).unwrap();
    let key = event(
        &mut conn,
        "unfetched",
        1,
        "A",
        None,
        None,
        "metered_api",
        100,
    );
    recorded_price(&conn, key, 25);
    summaries(&mut conn);
    drain(&mut conn, &config);
    let query = request(&["plan"], 0, DAY_MS);
    let value = report(&conn, &query);
    assert_eq!(value["rows"][0]["plan"], "pending");
    assert_eq!(value["rows"][0]["list_value"]["not_yet_valued_hours"], 1);
    assert_eq!(
        value["rows"][0]["list_value"]["unvalued"]["measurements"],
        0
    );
    assert_eq!(value["totals"][0]["deduced_conflict_plans"], 1);
    assert_eq!(value["totals"][0]["deduced_non_usd_plans"], 1);
    assert_eq!(value["totals"][0]["deduced_unspread_nanos"], 645_161_290);
    rate(&conn, "A", 1_000_000_000);
    drain(&mut conn, &config);
    let fetched = report(&conn, &query);
    assert_eq!(fetched["rows"][0]["plan"], "api");
    assert_eq!(fetched["rows"][0]["list_value"]["nanos"], 200_000);
}

#[test]
fn report_surface_future_first_call_keeps_recorded_and_list_value_without_fees() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    let key = event(
        &mut conn,
        "future",
        1_700_000_000_000,
        "A",
        Some("account"),
        None,
        "metered_api",
        100,
    );
    recorded_price(&conn, key, 70);
    ready(&mut conn);
    let query = ReportRequest {
        grouping: Vec::new(),
        grain: "day".into(),
        timezone: "UTC".into(),
        ..ReportRequest::default()
    };
    let value = serde_json::to_value(spend_report(&conn, &query, 1_000).unwrap()).unwrap();
    assert_eq!(value["rows"][0]["total_nanos"], 70);
    assert_eq!(value["rows"][0]["list_value"]["nanos"], 200_000);
    assert_eq!(value["rows"][0]["deduced"]["nanos"], 70);
    assert_eq!(value["totals"][0]["deduced_unspread_nanos"], 0);
    assert!(FeeDays::load(&conn, &config(), 1, 1)
        .unwrap()
        .days
        .is_empty());
    let mut reversed = query;
    reversed.from_ms = Some(DAY_MS);
    reversed.to_ms = Some(0);
    assert!(matches!(
        spend_report(&conn, &reversed, 1_000),
        Err(ReportError::Refused(ReportRefusal::InvalidRange))
    ));
}

#[test]
fn report_surface_unknown_basis_credential_pool_waits_for_valuation() {
    let mut conn = conn();
    let mut config = config();
    config.quota_providers.get_mut("pool").unwrap().scope[0].producer = "pi".into();
    rate(&conn, "A", 1_000_000_000);
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "resolved",
        0,
        None,
    );
    let key = event(
        &mut conn,
        "unknown",
        1,
        "A",
        None,
        Some("resolved"),
        "unknown",
        100,
    );
    conn.execute(
        "UPDATE usage_event SET currency='USD' WHERE event_key=?1",
        [key],
    )
    .unwrap();
    summaries(&mut conn);
    prepare(&mut conn, &config);
    let query = request(&["plan", "plan_account"], 0, DAY_MS);
    let pending = report(&conn, &query);
    assert_eq!(pending["rows"][0]["plan"], "pending");
    assert_eq!(pending["rows"][0]["deduced"]["pending_days"], 1);
    assert_eq!(pending["totals"][0]["deduced_unspread_nanos"], 0);
    drain(&mut conn, &config);
    let built = report(&conn, &query);
    assert_eq!(built["rows"][0]["plan"], "pool");
    assert_eq!(built["rows"][0]["plan_account"], "resolved");
    assert_eq!(built["rows"][0]["deduced"]["pending_days"], 0);
    assert_eq!(built["rows"][0]["deduced"]["nanos"], 645_161_290);
}
