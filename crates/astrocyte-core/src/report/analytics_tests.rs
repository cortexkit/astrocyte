use serde_json::{json, Value};

fn read(conn: &Connection, extra: Value) -> Value {
    let mut request = json!({"grouping":[],"grain":"day","timezone":"UTC"});
    request
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let request = serde_json::from_value(request).unwrap();
    serde_json::to_value(spend_report(conn, &request, 100).unwrap()).unwrap()
}

#[test]
fn analytics_range_widens_outward_and_scopes_disclosures() {
    let conn = conn();
    for (id, time) in [
        ("before", Some(-1)),
        ("first", Some(0)),
        ("last", Some(86_399_999)),
        ("after", Some(86_400_000)),
        ("undated", None),
    ] {
        event(&conn, id, None, time, "metered_api");
    }
    let report = read(&conn, json!({"from_ms":1,"to_ms":2}));
    assert_eq!(report["rows"].as_array().unwrap().len(), 1);
    assert_eq!(report["rows"][0]["events_total"], 2);
    assert_eq!(report["rows"][0]["bucket_start_ms"], 0);
    assert_eq!(report["rows"][0]["bucket_end_ms"], 86_400_000);
    assert_eq!(report["undated"]["events_undated"], 0);
    assert_eq!(report["undated"]["events_report_total"], 2);
}

#[test]
fn analytics_hour_uses_utc_hours_across_dst() {
    let conn = conn();
    let from = 1_730_610_000_000_i64;
    event(&conn, "a", None, Some(from), "metered_api");
    event(&conn, "b", None, Some(from + 3_600_000), "metered_api");
    let report = read(
        &conn,
        json!({"grain":"hour","timezone":"America/New_York","from_ms":from,"to_ms":from+7_200_000}),
    );
    assert_eq!(report["rows"].as_array().unwrap().len(), 2);
    for row in report["rows"].as_array().unwrap() {
        assert_eq!(row["bucket_start_ms"].as_i64().unwrap() % 3_600_000, 0);
        assert_eq!(
            row["bucket_end_ms"].as_i64().unwrap() - row["bucket_start_ms"].as_i64().unwrap(),
            3_600_000
        );
    }
}

#[test]
fn analytics_producer_and_where_use_or_and_null_and_bound_strings() {
    let conn = conn();
    event(&conn, "null", None, Some(1), "metered_api");
    event(
        &conn,
        "quoted",
        Some("x' OR 1=1 --"),
        Some(2),
        "metered_api",
    );
    event(&conn, "excluded", Some("other"), Some(3), "metered_api");
    let report = read(
        &conn,
        json!({"grouping":["producer","model"],"where":{"producer":["producer"],"work_class":[null,"x' OR 1=1 --"]}}),
    );
    assert_eq!(report["rows"].as_array().unwrap().len(), 1);
    assert_eq!(report["rows"][0]["producer"], "producer");
    assert_eq!(report["rows"][0]["model"], "model");
    assert_eq!(report["totals"][0]["events_total"], 2);
    assert_eq!(
        read(
            &conn,
            json!({"where":{"producer":["missing"],"work_class":[null]}})
        )["rows"],
        json!([])
    );
}

#[test]
fn analytics_tokens_and_exact_media_units_are_additive() {
    let mut conn = conn();
    event(&conn, "a", None, Some(1), "metered_api");
    for (role, unit, q) in [
        ("input", "token", 0),
        ("output", "token", 30),
        ("reasoning", "token", 12),
        ("cache_read", "token", 7),
        ("cache_write", "token_cache_5m", 3),
        ("cache_write", "token_cache_1h", 5),
        ("cache_write", "token_cache_1d", 11),
        ("duration", "second", 2),
    ] {
        measurement(&conn, "a", role, unit, q);
    }
    event(&conn, "b", None, Some(86_400_000), "metered_api");
    measurement(&conn, "b", "reasoning", "token", 5);
    let report = read(&conn, json!({}));
    let tokens = json!({"input":0,"output":42,"reasoning":12,"cache_read":7,"cache_write":19,"cache_write_5m":3,"cache_write_1h":5});
    assert_eq!(report["rows"][0]["tokens"], tokens);
    assert_eq!(report["rows"][1]["tokens"], json!({"reasoning":5}));
    assert_eq!(report["totals"][0]["tokens"]["output"], 47);
    assert_eq!(
        report["rows"][0]["units"],
        json!([{"role":"duration","unit":"second","quantity":{"units":2,"exponent":0}}])
    );
    assert_eq!(report["totals"][0]["units"], report["rows"][0]["units"]);
    fill(&mut conn);
    assert_eq!(read(&conn, json!({})), report);
}

fn fill(conn: &mut Connection) {
    let mut maintenance = SummaryMaintenance::default();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(std::time::Instant::now() < deadline);
        let tx = conn.transaction().unwrap();
        if maintenance.read_chunk(&tx, 100).unwrap() {
            maintenance.publish(&tx, 100).unwrap();
        }
        tx.commit().unwrap();
        if conn
            .query_row("SELECT COUNT(*) FROM summary_dirty_hour", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
            == 0
        {
            break;
        }
    }
}

#[test]
fn analytics_decimal_quantities_sum_exactly_across_lifetimes() {
    let mut conn = conn();
    event(&conn, "decimal", None, Some(1), "metered_api");
    for (role, unit, units) in [
        ("input", "token", 10),
        ("output", "token", 1),
        ("reasoning", "token", 20),
        ("cache_write", "token_cache_5m", 5),
        ("cache_write", "token_cache_1h", 5),
        ("duration", "second", 15),
    ] {
        conn.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES((SELECT event_key FROM usage_event WHERE event_id='decimal'),?1,?2,'billed','fixture',?3,-1)",params![role,unit,units]).unwrap();
    }
    event(&conn, "whole", None, Some(2), "metered_api");
    measurement(&conn, "whole", "duration", "second", 2);
    let expected = read(&conn, json!({}));
    assert_eq!(
        expected["rows"][0]["tokens"],
        json!({"input":1,"reasoning":2,"cache_write":1})
    );
    assert_eq!(
        expected["rows"][0]["units"],
        json!([{"role":"duration","unit":"second","quantity":{"units":35,"exponent":-1}}])
    );
    fill(&mut conn);
    assert_eq!(read(&conn, json!({})), expected);
}

#[test]
fn analytics_refusals_are_specific_and_read_only() {
    let conn = conn();
    event(&conn, "a", None, Some(1), "metered_api");
    let mut cases = vec![
        (json!({"from_ms":1}), "invalid_range"),
        (json!({"to_ms":2}), "invalid_range"),
        (json!({"from_ms":2,"to_ms":2}), "invalid_range"),
        (
            json!({"from_ms":0,"to_ms":401_i64*86_400_000}),
            "range_too_large",
        ),
        (json!({"grain":"hour"}), "range_too_large"),
        (
            json!({"grain":"hour","from_ms":0,"to_ms":32_i64*86_400_000}),
            "range_too_large",
        ),
        (
            json!({"where":{"no_such_dimension":["x"]}}),
            "invalid_grouping",
        ),
        (json!({"where":{"model":[]}}), "invalid_filter"),
        (
            json!({"grain":"hour","timezone":"Asia/Kolkata","from_ms":0,"to_ms":1}),
            "hour_timezone_unsupported",
        ),
        // Caracas moved from -4:00 to -4:30 at 07:00Z on 9 Dec 2007. A range
        // whose only fractional-offset instant is its last hour is refused.
        (
            json!({"grain":"hour","timezone":"America/Caracas","from_ms":1_197_072_000_000_i64,"to_ms":1_197_183_600_001_i64}),
            "hour_timezone_unsupported",
        ),
    ];
    for dimension in ["alfonso", "workload", "workspace", "parent_alfonso"] {
        cases.push((json!({"grouping":[dimension]}), "dimension_unavailable"));
        cases.push((json!({"where":{dimension:[null]}}), "dimension_unavailable"));
    }
    for (extra, expected) in cases {
        let mut request = json!({"grouping":[],"grain":"day","timezone":"UTC"});
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let request = serde_json::from_value(request).unwrap();
        match spend_report(&conn, &request, 100) {
            Err(ReportError::Refused(reason)) => assert_eq!(reason.code(), expected, "{extra}"),
            other => panic!("expected {expected} for {extra}: {other:?}"),
        }
    }
    // The same zone before the change has a whole-hour offset, so hourly
    // buckets are local hours there.
    let before: ReportRequest = serde_json::from_value(json!({"grouping":[],"grain":"hour","timezone":"America/Caracas","from_ms":1_197_072_000_000_i64,"to_ms":1_197_183_600_000_i64})).unwrap();
    assert!(spend_report(&conn, &before, 100).is_ok());
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM usage_event", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn analytics_unknown_quantities_are_not_measured_zero_on_either_path() {
    let mut conn = conn();
    event(&conn, "unknown", None, Some(1), "metered_api");
    measurement_without_quantity(&conn, "unknown", "input", "token");
    measurement_without_quantity(&conn, "unknown", "duration", "second");
    event(&conn, "zero", None, Some(3_600_001), "metered_api");
    measurement(&conn, "zero", "input", "token", 0);
    measurement(&conn, "zero", "duration", "second", 2);
    event(&conn, "undated", None, None, "metered_api");
    let mut requests = Vec::new();
    for grain in ["hour", "day", "week", "month"] {
        for timezone in ["UTC", "Europe/Istanbul", "America/New_York"] {
            requests.push(json!({"grain":grain,"timezone":timezone,"from_ms":0,"to_ms":7_200_000,"grouping":["producer","model"]}));
        }
    }
    for dimension in [
        "producer",
        "provider",
        "model",
        "account",
        "auth_selection",
        "charge_basis",
        "work_class",
        "project",
    ] {
        for values in [
            json!([null]),
            json!(["missing"]),
            json!([
                null,
                "producer",
                "provider",
                "model",
                "account",
                "metered_api",
                "/raw/project"
            ]),
        ] {
            requests.push(json!({"where":{dimension:values}}));
        }
    }
    let expected: Vec<_> = requests.iter().map(|q| read(&conn, q.clone())).collect();
    fill(&mut conn);
    let q: ReportRequest =
        serde_json::from_value(json!({"grouping":[],"grain":"day","timezone":"UTC"})).unwrap();
    assert!(
        hourly::spend(&conn, &q, 100).unwrap().is_some(),
        "must exercise the summary path"
    );
    for (q, want) in requests.iter().zip(&expected) {
        assert_eq!(&read(&conn, q.clone()), want, "{q}");
    }
    let report = read(&conn, json!({"grain":"hour","from_ms":0,"to_ms":7_200_000}));
    assert_eq!(report["rows"][0]["tokens"], json!({}));
    assert_eq!(report["rows"][0]["unknown_quantities"], json!({"input":1}));
    assert_eq!(
        report["rows"][0]["units"],
        json!([{"role":"duration","unit":"second","unknown_quantities":1}])
    );
    assert_eq!(report["rows"][1]["tokens"], json!({"input":0}));
    assert_eq!(report["totals"][0]["tokens"], json!({"input":0}));
    assert_eq!(
        report["totals"][0]["unknown_quantities"],
        json!({"input":1})
    );
    assert_eq!(
        report["totals"][0]["units"],
        json!([{"role":"duration","unit":"second","quantity":{"units":2,"exponent":0},"unknown_quantities":1}])
    );
    // A dirty hour must merge the same unknown counts and lower bounds.
    event(&conn, "dirty", None, Some(2), "metered_api");
    measurement(&conn, "dirty", "input", "token", 7);
    let q:ReportRequest=serde_json::from_value(json!({"grouping":["producer"],"grain":"day","timezone":"UTC","where":{"work_class":[null]}})).unwrap();
    assert_eq!(
        serde_json::to_value(recorded_spend_report(&conn, &q, 100).unwrap()).unwrap(),
        serde_json::to_value(spend_report_base(&conn, &q, 100, None).unwrap()).unwrap()
    );
}

#[test]
fn analytics_widened_range_caps_and_calendar_edges() {
    let conn = conn();
    for (grain, from, to, accepted) in [
        ("hour", 0, 31_i64 * 86_400_000, true),
        ("hour", 1, 31 * 86_400_000 + 1, false),
        ("day", 0, 400 * 86_400_000, true),
        ("day", 1, 400 * 86_400_000 + 1, false),
        ("week", 1, 400 * 86_400_000 + 1, true),
        ("month", 1, 400 * 86_400_000 + 1, true),
        ("week", 0, 400 * 86_400_000 + 1, false),
        ("month", 0, 400 * 86_400_000 + 1, false),
    ] {
        let q: ReportRequest = serde_json::from_value(
            json!({"grouping":[],"grain":grain,"timezone":"UTC","from_ms":from,"to_ms":to}),
        )
        .unwrap();
        let answer = spend_report(&conn, &q, 100);
        if accepted {
            answer.unwrap();
        } else {
            assert!(matches!(
                answer,
                Err(ReportError::Refused(ReportRefusal::RangeTooLarge))
            ));
        }
    }
    for grain in ["week", "month"] {
        event(&conn, grain, None, Some(1), "metered_api");
        let r = read(&conn, json!({"grain":grain,"from_ms":1,"to_ms":2}));
        let row = &r["rows"][0];
        assert!(row["bucket_start_ms"].as_i64().unwrap() <= 0);
        assert!(row["bucket_end_ms"].as_i64().unwrap() > 2);
    }
}
