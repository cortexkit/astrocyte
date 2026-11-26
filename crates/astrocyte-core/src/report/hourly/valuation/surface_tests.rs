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
    drain(conn);
}

#[test]
fn report_surface_ledger_plan_labels_ignore_multiplier_scopes() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "manual", 0, None);
    for (id, account, basis) in [
        ("manual", Some("manual"), "subscription_included"),
        ("unmatched", None, "subscription_included"),
        ("api", Some("manual"), "metered_api"),
    ] {
        let key = event(&mut conn, id, 1, "A", account, None, basis, 100);
        recorded_price(&conn, key, 0);
    }
    ready(&mut conn);
    let value = report(&conn, &request(&["plan", "plan_account"], 0, DAY_MS));
    let rows = value["rows"].as_array().unwrap();
    let actual: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row["plan"].as_str().unwrap(),
                row["plan_account"].as_str(),
                row["events_total"].as_u64().unwrap(),
                row["deduced"]["nanos"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        actual,
        [
            ("api", None, 1, 0),
            ("none", None, 1, 0),
            ("provider", Some("manual"), 1, 645_161_290),
        ]
    );
    assert_eq!(value["totals"][0]["deduced"]["nanos"], 645_161_290);
    assert_eq!(value["totals"][0]["deduced_unspread_nanos"], 0);
}

#[test]
fn report_surface_two_measured_pools_share_one_fee_and_ignore_manual_traffic() {
    use crate::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
    use crate::multiplier::insert_observation_in_tx;
    use crate::multiplier::pools::{
        capture_pool_day, compute_pool_observations, plan_pools, resolve_pool_models,
    };
    use crate::vault_plan::{append_in_tx, VaultPlan};
    use fusiform_protocol::{
        CatalogGetRequest, CatalogGetResponse, PlanPricesResponse, StatusRequest, StatusResponse,
    };
    use std::sync::Arc;

    const START: i64 = DAY_MS;
    const END: i64 = START + 3 * DAY_MS;
    struct Catalog;
    impl CatalogConnection for Catalog {
        fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
            unreachable!("the fixture supplies the history boundary")
        }

        fn catalog_get(
            &self,
            req: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            assert_eq!(req.provider_id.as_deref(), Some("provider"));
            assert_eq!(req.model_id.as_deref(), Some("A"));
            let rate = |units| {
                json!({"state":"priced","units":units,"exponent":9,
                "currency":"USD","unit_provenance":{"kind":"stated"}})
            };
            Ok(
                serde_json::from_value(json!({"source":"fixture","resolved_at_ms":END,
                "catalog_version":1,"models":{"provider/A":{
                    "rate.input":rate(1_000_000_000_i64),"rate.output":rate(2_000_000_000_i64)
                }}}))
                .unwrap(),
            )
        }
    }

    let mut conn = conn();
    conn.execute_batch("CREATE TABLE cortexkit_schema_version(namespace TEXT, version INTEGER, applied_at_unix INTEGER);
        INSERT INTO cortexkit_schema_version VALUES ('astrocyte',68,0)").unwrap();
    let tx = conn.transaction().unwrap();
    append_in_tx(
        &tx,
        &VaultPlan {
            credential_id: "oauth:member".into(),
            provider: Some("provider".into()),
            account_id: Some("member".into()),
            plan_tier: Some("plan".into()),
            plan_key: Some("provider/plan".into()),
            source: "operator".into(),
            effective_from_ms: 0,
            observed_at_ms: 0,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    for login in ["member", "manual"] {
        plan(
            &conn,
            PlanLedger::Account,
            "provider",
            login,
            START,
            Some(END),
        );
        for day in 0..3 {
            for step in 0..=48 {
                for (pool, scope) in [("primary", "all"), ("secondary", "subset")] {
                    let at = START + day * DAY_MS + step * (HOUR_MS / 6);
                    conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,
                        used_percent,window_minutes,model_scope,api_provider,resets_at)
                        VALUES ('provider',?1,?2,?3,?4,10080,?5,'provider','1970-02-01T00:00:00Z')",
                        params![login, pool, at * 1_000_000, day * 8 + step / 6, scope]).unwrap();
                }
            }
            let key = event(
                &mut conn,
                &format!("{login}-{day}"),
                START + day * DAY_MS + 1,
                "A",
                Some(login),
                None,
                "subscription_included",
                24_000,
            );
            recorded_price(&conn, key, 0);
        }
    }
    let tx = conn.transaction().unwrap();
    let capture = capture_pool_day(&tx, "provider", START, END).unwrap();
    tx.commit().unwrap();
    let served: PlanPricesResponse = serde_json::from_value(json!({
        "prices":[{"provider_id":"provider","tier":"plan","price":{"minor_units":20,"exponent":0,
            "currency":"USD","period":"month"},"boundary_at_ms":0,"established_by":"fixture",
            "established_at_ms":0,"review_by_ms":END,"source_ref":"https://example.invalid/prices"}],
        "resolved_at_ms":END,"tier_vocabulary":"vendor","unit_policy":"list",
        "quota_pools":[{"provider_id":"provider","pool_id":"secondary","models":["provider/A"],
            "established_at_ms":START,"review_by_ms":END,"quote":"fixture subset",
            "source_ref":"https://example.invalid/quota"}]
    })).unwrap();
    let pools = plan_pools(&capture.readings, &served.quota_pools);
    let catalog = CatalogClient::new(Arc::new(Catalog), START);
    let (identities, rates) =
        resolve_pool_models(&catalog, &capture.models_for_pools(&pools), END).unwrap();
    let rows =
        compute_pool_observations(&capture, &pools, &rates, &identities, &served, END).unwrap();
    assert_eq!(rows.len(), 2);
    let tx = conn.transaction().unwrap();
    for row in &rows {
        assert_eq!(row.plan.as_deref(), Some("provider/plan"));
        assert!(row.routing_eligible, "{:?}", row.eligibility_reasons);
        assert_eq!(row.tokens.input, 72_000, "only the vault member counts");
        insert_observation_in_tx(&tx, row).unwrap();
    }
    tx.commit().unwrap();

    rate(&conn, "A", 1_000_000_000);
    ready(&mut conn);
    let value = report(
        &conn,
        &request(&["plan", "plan_account"], END - DAY_MS, END),
    );
    let rows = value["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    for (row, login) in rows.iter().zip(["manual", "member"]) {
        assert_eq!(row["plan"], "provider");
        assert_eq!(row["plan_account"], login);
        assert_eq!(row["events_total"], 1);
        assert_eq!(row["deduced"]["nanos"], 645_161_290);
    }
    assert_eq!(value["totals"][0]["deduced"]["nanos"], 1_290_322_581);
    assert_eq!(value["totals"][0]["deduced_unspread_nanos"], 0);
    assert_eq!(value["totals"][0]["deduced_conflict_plans"], 0);
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
        ("unmatched", 2, "A", None, 100),
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
    assert_eq!(row["plan"], "provider");
    assert_eq!(row["plan_account"], "account");
    assert_eq!(row["list_value"]["nanos"], 200_000);
    assert_eq!(row["deduced"]["nanos"], 161_290_323);
    assert!(row["deduced"].get("by_rule_share").is_none());
    assert!(value["totals"][0].get("deduced_unspread_nanos").is_none());
    req.filters.clear();
    req.filters
        .insert("plan".into(), vec![Some("provider".into())]);
    let all = report(&conn, &req);
    assert_eq!(all["totals"][0]["deduced"]["nanos"], 645_161_290);
    assert_eq!(all["totals"][0]["deduced_unspread_nanos"], 0);
    // A record with no own calls stays unspread, not assigned to another call.
    plan(&conn, PlanLedger::Account, "provider", "unused", 0, None);
    ready(&mut conn);
    let all = report(&conn, &req);
    assert_eq!(all["totals"][0]["deduced"]["nanos"], 645_161_290);
    assert_eq!(all["totals"][0]["deduced_unspread_nanos"], 645_161_290);
    req.filters.insert("plan".into(), vec![Some("none".into())]);
    let unmatched = report(&conn, &req);
    assert_eq!(unmatched["rows"].as_array().unwrap().len(), 1);
    assert!(unmatched["rows"][0]["plan_account"].is_null());
    assert_eq!(unmatched["rows"][0]["deduced"]["nanos"], 0);
    assert!(unmatched["rows"][0]["deduced"]
        .get("by_rule_share")
        .is_none());
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
    req.filters
        .insert("plan".into(), vec![Some("provider".into())]);
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
    prepare(&mut conn);
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
    drain(&mut conn);
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
fn report_surface_legacy_config_rebuilds_stale_labels_and_defaults_missing_bundle_counter() {
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
    assert_eq!(report(&conn, &req)["rows"][0]["plan"], "none");
    let legacy_key = r#"[{"twin":"provider"},{},["provider"]]"#;
    conn.execute(
        "UPDATE valuation_state SET config=?1 WHERE id=1",
        [legacy_key],
    )
    .unwrap();
    conn.execute(
        "UPDATE valuation_cell SET plan='pool',plan_account='account_unknown'",
        [],
    )
    .unwrap();
    assert_eq!(report(&conn, &req)["rows"][0]["plan"], "pending");
    let attributed = report(&conn, &request(&["alfonso"], 0, DAY_MS));
    assert_eq!(
        attributed["rows"][0]["list_value"]["not_yet_valued_hours"],
        1
    );
    assert!(ValuationMaintenance::needs_prepare(&conn).unwrap());
    assert_eq!(drain(&mut conn), vec![0]);
    assert_eq!(report(&conn, &req)["rows"][0]["plan"], "none");

    conn.execute("UPDATE summary_hour SET revision=revision+1", [])
        .unwrap();
    assert_eq!(
        report(&conn, &req)["rows"][0]["plan"],
        "none",
        "dirty fallback also uses ledger membership"
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
    drain(&mut conn);
    assert_eq!(report(&conn, &query), built);
}

#[test]
fn report_surface_plan_diagnostics_and_unfetched_hours() {
    let mut conn = conn();
    plan(&conn, PlanLedger::Account, "provider", "unused", 0, None);
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
    drain(&mut conn);
    let query = request(&["plan"], 0, DAY_MS);
    let value = report(&conn, &query);
    assert_eq!(value["rows"][0]["plan"], "pending");
    assert_eq!(value["rows"][0]["list_value"]["not_yet_valued_hours"], 1);
    assert_eq!(
        value["rows"][0]["list_value"]["unvalued"]["measurements"],
        0
    );
    assert_eq!(value["totals"][0]["deduced_conflict_plans"], 0);
    assert_eq!(value["totals"][0]["deduced_non_usd_plans"], 1);
    assert_eq!(value["totals"][0]["deduced_unspread_nanos"], 645_161_290);
    rate(&conn, "A", 1_000_000_000);
    drain(&mut conn);
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
    assert!(FeeDays::load(&conn, 1, 1).unwrap().days.is_empty());
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
    prepare(&mut conn);
    let query = request(&["plan", "plan_account"], 0, DAY_MS);
    let pending = report(&conn, &query);
    assert_eq!(pending["rows"][0]["plan"], "pending");
    assert_eq!(pending["rows"][0]["deduced"]["pending_days"], 1);
    assert_eq!(pending["totals"][0]["deduced_unspread_nanos"], 0);
    drain(&mut conn);
    let built = report(&conn, &query);
    assert_eq!(built["rows"][0]["plan"], "provider");
    assert_eq!(built["rows"][0]["plan_account"], "resolved");
    assert_eq!(built["rows"][0]["deduced"]["pending_days"], 0);
    assert_eq!(built["rows"][0]["deduced"]["nanos"], 645_161_290);
}
