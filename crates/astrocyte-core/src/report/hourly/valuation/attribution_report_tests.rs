use super::super::fees::DAY_MS;
use super::*;

fn call(
    conn: &mut Connection,
    id: &str,
    at: i64,
    session: &str,
    model: &str,
    basis: &str,
    quantity: i64,
) -> i64 {
    let mut request:ingest::EventRecordRequest=serde_json::from_value(json!({
        "producer":"opencode","producer_ref":id,"provider":"provider","model":model,"account_ref":"account",
        "harness":"opencode","session":session,"project_root":"/recorded/folder","occurred_at_ms":at,"charge_basis":basis,
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":quantity,"quantity_exponent":0}]
    })).unwrap();
    request.runner = Some("thalamus".into());
    let result = ingest::record_event(conn, &request, 1).unwrap();
    let key: i64 = conn
        .query_row(
            "SELECT event_key FROM usage_event WHERE event_id=?1",
            [result.event_id],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_key=?1",
        [key],
    )
    .unwrap();
    let nanos = if basis == "metered_api" {
        quantity * 3
    } else {
        0
    };
    let covered = basis == "subscription_included";
    if covered {
        conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms) VALUES(?1,'input','token',1,'covered',?2,1,'USD',0,'pricing_pass',1)",params![key,basis]).unwrap();
    } else {
        conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms) VALUES(?1,'input','token',1,'priced',?2,0,'USD',?3,1,9,'unknown','fusiform',100,'{}','pricing_pass',100)",params![key,basis,nanos]).unwrap();
    }
    key
}
fn facts(conn: &mut Connection) {
    let page:crate::attribution::Page=serde_json::from_value(json!({"facts":[
        {"fact_id":"a","supersedes":null,"subject":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"session-a"},"alfonso":"agent-a","parent":null,"workload":"mason","task":null,"project_id":"project-a","effective_from_ms":0,"recorded_at_ms":1},
        {"fact_id":"b","supersedes":null,"subject":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"session-b"},"alfonso":"agent-b","parent":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"session-a"},"workload":"summary","task":"magic-context.summary","project_id":null,"effective_from_ms":0,"recorded_at_ms":1}
    ],"next":null,"through":2})).unwrap();
    let tx = conn.transaction().unwrap();
    crate::attribution::commit_page(
        &tx,
        crate::attribution::Publisher::Prefrontal,
        None,
        &page,
        1,
    )
    .unwrap();
    tx.commit().unwrap();
}
fn request(extra: serde_json::Value) -> ReportRequest {
    let mut value =
        json!({"grouping":["alfonso"],"grain":"day","timezone":"UTC","from_ms":0,"to_ms":DAY_MS});
    value
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    serde_json::from_value(value).unwrap()
}

#[test]
fn attribution_report_real_split_rows_keep_unattributed_and_all_metric_totals() {
    let mut conn = conn();
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        0,
        Some(DAY_MS),
    );
    let api = call(&mut conn, "api", 1, "session-a", "a", "metered_api", 100);
    call(
        &mut conn,
        "included",
        HOUR_MS + 1,
        "session-b",
        "b",
        "subscription_included",
        200,
    );
    call(
        &mut conn,
        "unattributed",
        2 * HOUR_MS + 1,
        "unknown",
        "a",
        "metered_api",
        300,
    );
    conn.execute("INSERT INTO adjustment(adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin) VALUES('refund',?1,'USD',-30,'refund',1,2,'correction_ingest')",[api]).unwrap();
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    rate(&conn, "b", 2_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let ordinary = spend_report(&conn, &request(json!({"grouping":[]})), DAY_MS).unwrap();
    let split = spend_report(&conn, &request(json!({})), DAY_MS).unwrap();
    assert_eq!(
        serde_json::to_value(&split.totals).unwrap(),
        serde_json::to_value(&ordinary.totals).unwrap()
    );
    assert_eq!(split.rows.len(), 3);
    assert_eq!(
        split.rows.iter().map(|row| row.events_total).sum::<u64>(),
        3
    );
    assert_eq!(
        split.rows.iter().map(|row| row.metered_nanos).sum::<i64>(),
        ordinary.totals[0].metered_nanos
    );
    assert!(split
        .rows
        .iter()
        .any(|row| row.grouping["alfonso"].is_none()));
    let wire = serde_json::to_value(&split).unwrap();
    assert_eq!(
        wire["attribution_coverage"],
        json!({"attributed_calls":2,"unattributed_calls":1})
    );
    assert!(serde_json::to_value(&ordinary)
        .unwrap()
        .get("attribution_coverage")
        .is_none());
}

#[test]
fn attribution_report_filter_coverage_ignores_attribution_but_keeps_model_filter() {
    let mut conn = conn();
    call(&mut conn, "a", 1, "session-a", "a", "metered_api", 100);
    call(&mut conn, "b", 1, "session-b", "b", "metered_api", 100);
    call(&mut conn, "unknown", 1, "unknown", "a", "metered_api", 100);
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    rate(&conn, "b", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let split = spend_report(
        &conn,
        &request(json!({"where":{"alfonso":["agent-a"],"model":["a"]}})),
        DAY_MS,
    )
    .unwrap();
    assert_eq!(split.rows.len(), 1);
    assert_eq!(split.rows[0].events_total, 1);
    assert_eq!(
        serde_json::to_value(split).unwrap()["attribution_coverage"],
        json!({"attributed_calls":1,"unattributed_calls":1})
    );
}

#[test]
fn attribution_report_unbounded_keeps_future_calls_like_the_ordinary_report() {
    let mut conn = conn();
    call(
        &mut conn,
        "future",
        2 * DAY_MS + 1,
        "session-a",
        "a",
        "metered_api",
        100,
    );
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let ordinary = spend_report(
        &conn,
        &request(json!({"grouping":[],"from_ms":null,"to_ms":null})),
        DAY_MS,
    )
    .unwrap();
    let split = spend_report(
        &conn,
        &request(json!({"from_ms":null,"to_ms":null})),
        DAY_MS,
    )
    .unwrap();
    assert_eq!(
        split.rows.iter().map(|row| row.events_total).sum::<u64>(),
        1
    );
    assert_eq!(
        serde_json::to_value(split.totals).unwrap(),
        serde_json::to_value(ordinary.totals).unwrap()
    );
}

#[test]
fn attribution_report_randomized_groups_sum_to_ordinary_totals_with_identical_model_filters() {
    let mut conn = conn();
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        0,
        Some(7 * DAY_MS),
    );
    let mut seed = 0x5a17_u64;
    for n in 0..96 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let session = ["session-a", "session-b", "unknown"][usize::try_from(seed % 3).unwrap()];
        let model = if seed & 8 == 0 { "a" } else { "b" };
        let basis = if seed & 16 == 0 {
            "metered_api"
        } else {
            "subscription_included"
        };
        let at = i64::try_from(seed % u64::try_from(7 * DAY_MS).unwrap()).unwrap();
        let quantity = i64::try_from(seed % 120).unwrap() + 1;
        call(
            &mut conn,
            &format!("call-{n}"),
            at,
            session,
            model,
            basis,
            quantity,
        );
    }
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    rate(&conn, "b", 2_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    for grouping in [
        vec!["alfonso"],
        vec!["alfonso", "model"],
        vec!["workload", "parent_alfonso"],
        vec!["project_id", "workspace"],
    ] {
        for models in [json!({}), json!({"model":["a"]}), json!({"model":["b"]})] {
            let base = json!({"grouping":[],"to_ms":7*DAY_MS,"where":models});
            let ordinary = spend_report(&conn, &request(base.clone()), 7 * DAY_MS).unwrap();
            let mut split_request = base;
            split_request["grouping"] = json!(grouping);
            let split = spend_report(&conn, &request(split_request.clone()), 7 * DAY_MS).unwrap();
            assert_eq!(
                serde_json::to_value(&split.totals).unwrap(),
                serde_json::to_value(&ordinary.totals).unwrap(),
                "{split_request}"
            );
            let total = &ordinary.totals[0];
            assert_eq!(
                split.rows.iter().map(|r| r.events_total).sum::<u64>(),
                total.events_total
            );
            assert_eq!(
                split.rows.iter().map(|r| r.total_nanos).sum::<i64>(),
                total.total_nanos
            );
            assert_eq!(
                split.rows.iter().map(|r| r.metered_nanos).sum::<i64>(),
                total.metered_nanos
            );
            assert_eq!(
                split
                    .rows
                    .iter()
                    .map(|r| r.valuation.list_value.nanos)
                    .sum::<i64>(),
                total.valuation.row.list_value.nanos
            );
            for report in [&ordinary, &split] {
                let row_sum = report
                    .rows
                    .iter()
                    .filter_map(|row| row.valuation.deduced.as_ref())
                    .map(|value| i128::from(value.nanos))
                    .sum::<i128>();
                let total = i128::from(
                    report.totals[0]
                        .valuation
                        .row
                        .deduced
                        .as_ref()
                        .unwrap()
                        .nanos,
                );
                assert!(
                    (row_sum - total).abs() * 2 <= i128::try_from(report.rows.len()).unwrap(),
                    "{split_request}: rows={row_sum}, total={total}"
                );
            }
            let coverage = split.attribution_coverage.unwrap();
            assert_eq!(
                coverage.attributed_calls + coverage.unattributed_calls,
                total.events_total
            );
        }
    }
}

#[test]
fn attribution_report_query_plans_seek_split_hour_cells_for_each_required_shape() {
    use super::super::surface::attribution_report::{CELLS_SQL, HOURS_SQL};
    let conn = conn();
    for (name, days) in [
        ("alfonso-7d", 7),
        ("alfonso-model-30d", 30),
        ("day-workload-90d", 90),
    ] {
        let cells = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {CELLS_SQL}"))
            .unwrap()
            .query_map(params![0, days * 24], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            cells,
            ["SEARCH attribution_valuation_cell USING INDEX sqlite_autoindex_attribution_valuation_cell_1 (kind=? AND hour_number=?)"],
            "{name}"
        );
        let hours = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {HOURS_SQL}"))
            .unwrap()
            .query_map(params![0, days * 24], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            hours.iter().any(|step| step
                == "SEARCH s USING PRIMARY KEY (kind=? AND hour_number>? AND hour_number<?)"),
            "{name}: {hours:?}"
        );
        assert!(
            !hours.iter().any(|step| step.contains("usage_event")),
            "{name}: {hours:?}"
        );
    }
}

#[test]
fn attribution_report_split_combination_matrix_preserves_legacy_project_and_task_refusals() {
    let conn = conn();
    let before = conn.total_changes();
    for attribution in [
        "alfonso",
        "parent_alfonso",
        "workload",
        "project_id",
        "workspace",
    ] {
        for other in [
            "account",
            "auth_selection",
            "charge_basis",
            "work_class",
            "project",
            "producer",
            "runner",
            "harness",
            "plan",
            "plan_account",
        ] {
            for query in [
                json!({"grouping":[attribution,other]}),
                json!({"grouping":[attribution],"where":{other:[null]}}),
                json!({"grouping":[other],"where":{attribution:[null]}}),
            ] {
                let error = spend_report(&conn, &request(query.clone()), DAY_MS).unwrap_err();
                let ReportError::Refused(reason) = error else {
                    panic!("unexpected store failure")
                };
                assert_eq!(reason.code(), "combination_unavailable", "{query}");
                assert!(
                    reason.message().contains(attribution) && reason.message().contains(other),
                    "{query}"
                );
            }
        }
        for other in ["model", "provider"] {
            assert!(spend_report(
                &conn,
                &request(json!({"grouping":[attribution,other]})),
                DAY_MS
            )
            .is_ok());
        }
    }
    for query in [
        json!({"grouping":["alfonso","model"],"where":{"provider":[null]}}),
        json!({"grouping":["alfonso","workload","workspace"]}),
    ] {
        assert!(matches!(
            spend_report(&conn, &request(query), DAY_MS),
            Err(ReportError::Refused(ReportRefusal::CombinationUnavailable(
                _
            )))
        ));
    }
    for query in [
        json!({"grouping":["task"]}),
        json!({"where":{"task":[null]}}),
    ] {
        assert!(
            matches!(spend_report(&conn,&request(query),DAY_MS),Err(ReportError::Refused(ReportRefusal::DimensionUnavailable(name))) if name=="task")
        );
    }
    assert_eq!(conn.total_changes(), before);
}

#[test]
fn attribution_report_stale_summary_valuation_and_fact_windows_match_ordinary_metrics() {
    let mut conn = conn();
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        0,
        Some(DAY_MS),
    );
    call(
        &mut conn,
        "a",
        1,
        "session-a",
        "a",
        "subscription_included",
        100,
    );
    call(
        &mut conn,
        "b",
        HOUR_MS + 1,
        "session-b",
        "a",
        "subscription_included",
        200,
    );
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    let assert_parity = |conn: &Connection, label: &str| {
        let ordinary = spend_report(conn, &request(json!({"grouping":[]})), DAY_MS).unwrap();
        let split = spend_report(conn, &request(json!({})), DAY_MS).unwrap();
        assert_eq!(
            serde_json::to_value(split.totals).unwrap(),
            serde_json::to_value(ordinary.totals).unwrap(),
            "{label}"
        );
    };
    assert_parity(&conn, "summary dirty");
    summaries(&mut conn);
    prepare(&mut conn, &config());
    assert_parity(&conn, "valuation pending");
    drain(&mut conn, &config());
    assert_parity(&conn, "cached");
    let page:crate::attribution::Page=serde_json::from_value(json!({"facts":[{"fact_id":"corrected-a","supersedes":"a","subject":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"session-a"},"alfonso":"new-agent","parent":null,"workload":"head","task":null,"project_id":"project-a","effective_from_ms":0,"recorded_at_ms":2}],"next":null,"through":3})).unwrap();
    let tx = conn.transaction().unwrap();
    crate::attribution::commit_page(
        &tx,
        crate::attribution::Publisher::Prefrontal,
        None,
        &page,
        2,
    )
    .unwrap();
    tx.commit().unwrap();
    assert_parity(&conn, "only attribution stale");
    let split = spend_report(&conn, &request(json!({})), DAY_MS).unwrap();
    assert!(split
        .rows
        .iter()
        .any(|row| row.grouping["alfonso"].as_deref() == Some("new-agent")));
    rate(&conn, "a", 2_000_000_000);
    assert_parity(&conn, "new rate generation");
    call(
        &mut conn,
        "late",
        HOUR_MS + 2,
        "unknown",
        "a",
        "metered_api",
        25,
    );
    assert_parity(&conn, "late call");
}

#[test]
fn attribution_report_no_range_filters_undated_separately_from_dated_coverage() {
    let mut conn = conn();
    call(
        &mut conn,
        "dated-a",
        1,
        "session-a",
        "a",
        "metered_api",
        100,
    );
    for (id, session) in [("undated-a", "session-a"), ("undated-unknown", "unknown")] {
        let mut request:ingest::EventRecordRequest=serde_json::from_value(json!({"producer":"opencode","producer_ref":id,"harness":"opencode","session":session,"charge_basis":"metered_api","measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":100,"quantity_exponent":0}]})).unwrap();
        request.runner = Some("thalamus".into());
        ingest::record_event(&mut conn, &request, 1).unwrap();
    }
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let split = spend_report(
        &conn,
        &request(json!({"from_ms":null,"to_ms":null,"where":{"alfonso":["agent-a"]}})),
        DAY_MS,
    )
    .unwrap();
    assert_eq!(split.undated.events_undated, 1);
    assert_eq!(split.undated.events_report_total, 2);
    assert_eq!(split.undated.reasons["missing_occurred_at"], 1);
    assert_eq!(
        split.attribution_coverage.unwrap(),
        AttributionCoverage {
            attributed_calls: 1,
            unattributed_calls: 0
        }
    );
}
