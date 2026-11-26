use super::super::fees::DAY_MS;
use super::*;
use crate::attribution::Publisher;
use crate::broca::{record_scope_in_tx, CallKind, ScopeStamp};

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

fn broca_call(conn: &mut Connection, id: &str, at: i64, run: &str) {
    let mut call: ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"broca", "producer_ref":id, "run_ref":run,
        "provider":"provider", "model":"a", "occurred_at_ms":at, "charge_basis":"metered_api",
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture",
                         "quantity_units":17,"quantity_exponent":0}]
    }))
    .unwrap();
    call.runner = Some("broca".into());
    ingest::record_event(conn, &call, 1).unwrap();
}

fn broca_stamp(conn: &mut Connection, id: &str, kind: CallKind, stamp: ScopeStamp) {
    let tx = conn.transaction().unwrap();
    assert_eq!(record_scope_in_tx(&tx, id, kind, &stamp).unwrap(), 1);
    tx.commit().unwrap();
}

fn publish_facts(conn: &mut Connection, publisher: Publisher, facts: serde_json::Value) {
    let page = serde_json::from_value(json!({"facts":facts,"next":null,"through":1})).unwrap();
    let tx = conn.transaction().unwrap();
    crate::attribution::commit_page(&tx, publisher, None, &page, 1).unwrap();
    tx.commit().unwrap();
}

#[test]
fn attribution_report_broca_scope_agent_uses_stamped_run_parent() {
    let mut conn = conn();
    broca_call(&mut conn, "session-call", 10, "broca-run");
    broca_stamp(
        &mut conn,
        "session-call",
        CallKind::Segment,
        ScopeStamp {
            agent_id: Some("stamped-agent".into()),
            agent_run_id: Some("launch-run".into()),
            flow_id: None,
        },
    );
    broca_call(&mut conn, "batch-call", 20, "batch-run");
    broca_stamp(
        &mut conn,
        "batch-call",
        CallKind::SystemOne,
        ScopeStamp {
            agent_id: Some("batch-agent".into()),
            agent_run_id: Some("batch-run".into()),
            flow_id: None,
        },
    );
    rate(&conn, "a", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let workload = request(json!({"grouping":["alfonso","workload"]}));
    let result = spend_report(&conn, &workload, DAY_MS).unwrap();
    assert!(result
        .rows
        .iter()
        .any(
            |row| row.grouping["alfonso"].as_deref() == Some("stamped-agent")
                && row.grouping["workload"].as_deref() == Some("broca_session")
                && row.events_total == 1
        ));
    assert!(result
        .rows
        .iter()
        .any(
            |row| row.grouping["alfonso"].as_deref() == Some("batch-agent")
                && row.grouping["workload"].as_deref() == Some("system_one")
                && row.events_total == 1
        ));

    publish_facts(
        &mut conn,
        Publisher::Prefrontal,
        json!([{
            "fact_id":"launch","subject":{"kind":"run","harness":"broca","run_id":"launch-run"},
            "alfonso":"launcher","workload":"launcher-kind","project_id":"launch-project",
            "effective_from_ms":0,"recorded_at_ms":0
        }]),
    );
    publish_facts(
        &mut conn,
        Publisher::Entorhinal,
        json!([{
            "fact_id":"place","project_id":"launch-project","workspace_id":"launch-space",
            "effective_from_ms":5,"recorded_at_ms":5
        }]),
    );
    for (dimension, expected) in [
        ("parent_alfonso", "launcher"),
        ("project_id", "launch-project"),
        ("workspace", "launch-space"),
    ] {
        let query = request(json!({"grouping":[dimension], "where":{"alfonso":["stamped-agent"]}}));
        let report = spend_report(&conn, &query, DAY_MS).unwrap();
        assert_eq!(report.rows.len(), 1);
        assert_eq!(
            report.rows[0].grouping[dimension].as_deref(),
            Some(expected)
        );
    }
}

#[test]
fn attribution_report_broca_scope_flow_joins_publisher_flow() {
    let mut conn = conn();
    broca_call(&mut conn, "flow-call", 1, "broca-run");
    broca_stamp(
        &mut conn,
        "flow-call",
        CallKind::Segment,
        ScopeStamp {
            flow_id: Some("stamped-flow".into()),
            ..ScopeStamp::default()
        },
    );
    rate(&conn, "a", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let query = request(json!({"grouping":["alfonso","workload"]}));
    let before = spend_report(&conn, &query, DAY_MS).unwrap();
    assert_eq!(before.attribution_coverage.unwrap().unattributed_calls, 1);
    publish_facts(
        &mut conn,
        Publisher::Basal,
        json!([{
            "fact_id":"flow","subject":{"kind":"flow","flow_id":"stamped-flow"},
            "alfonso":"flow-owner","workload":"flow-workload","project_id":"flow-project",
            "effective_from_ms":0,"recorded_at_ms":0
        }]),
    );
    let report = spend_report(&conn, &query, DAY_MS).unwrap();
    assert_eq!(report.rows.len(), 1);
    assert_eq!(
        report.rows[0].grouping["alfonso"].as_deref(),
        Some("flow-owner")
    );
    assert_eq!(
        report.rows[0].grouping["workload"].as_deref(),
        Some("flow-workload")
    );
    assert_eq!(report.attribution_coverage.unwrap().attributed_calls, 1);
}

#[test]
fn attribution_report_broca_scope_publisher_wins_and_counts_disagreement() {
    let mut conn = conn();
    for (fact, stamp_agent) in [("different", "stamped-agent"), ("same", "publisher-agent")] {
        broca_call(&mut conn, fact, 1, "native-run");
        broca_stamp(
            &mut conn,
            fact,
            CallKind::SystemOne,
            ScopeStamp {
                agent_id: Some(stamp_agent.into()),
                agent_run_id: Some("native-run".into()),
                flow_id: None,
            },
        );
    }
    publish_facts(
        &mut conn,
        Publisher::Prefrontal,
        json!([{
            "fact_id":"owner","subject":{"kind":"run","harness":"broca","run_id":"native-run"},
            "alfonso":"publisher-agent","workload":"publisher-workload",
            "effective_from_ms":0,"recorded_at_ms":0
        }]),
    );
    rate(&conn, "a", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let result = spend_report(
        &conn,
        &request(json!({"grouping":["alfonso","workload"]})),
        DAY_MS,
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].grouping["alfonso"].as_deref(),
        Some("publisher-agent")
    );
    assert_eq!(
        result.rows[0].grouping["workload"].as_deref(),
        Some("publisher-workload")
    );
    assert_eq!(result.rows[0].events_total, 2);
    let query = request(json!({"where":{"alfonso":["stamped-agent"],"model":["a"]}}));
    let filtered = spend_report(&conn, &query, DAY_MS).unwrap();
    assert!(filtered.rows.is_empty());
    assert_eq!(
        serde_json::to_value(filtered.attribution_coverage.unwrap()).unwrap(),
        json!({
            "attributed_calls":2,"unattributed_calls":0,"stamp_disagreement_calls":1
        })
    );
}

#[test]
fn attribution_report_broca_scope_join_seeks_stamps_by_call_key() {
    use crate::report::hourly::attribution::stamps_sql;
    let conn = conn();
    let plan = conn
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {}",
            stamps_sql("occurred_at_ms>=0 AND occurred_at_ms<3600000")
        ))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(
        plan.iter()
            .any(|step| step.starts_with("SEARCH usage_event")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|step| step == "SEARCH s USING INTEGER PRIMARY KEY (rowid=?)"),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|step| step.starts_with("SCAN")),
        "{plan:?}"
    );
}

fn basis_parity_store(stored: bool) -> (Connection, String, String) {
    let mut db = conn();
    plan(&db, PlanLedger::Account, "provider", "account", 0, None);
    let record = |db: &mut Connection, id: &str, at: i64, input: i64| {
        let mut request: ingest::EventRecordRequest = serde_json::from_value(json!({
            "producer":"opencode", "producer_ref":id, "provider":"provider", "model":"a",
            "account_ref":"account", "harness":"opencode", "session":"session-a",
            "occurred_at_ms":at, "charge_basis":"unknown",
            "measurements":[
                {"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":input,"quantity_exponent":0},
                {"role":"output","unit":"token","kind":"billed","source":"fixture","quantity_units":0,"quantity_exponent":0},
                {"role":"duration","unit":"second","kind":"observed","source":"fixture","quantity_units":1,"quantity_exponent":0}]
        })).unwrap();
        request.runner = Some("thalamus".into());
        ingest::record_event(db, &request, 1).unwrap().event_id
    };
    let unknown = record(&mut db, "unknown", 1, 12);
    let zero = record(&mut db, "zero", 2, 0);
    let corrected = record(&mut db, "corrected", HOUR_MS + 1, 42);
    for id in [&unknown, &zero] {
        // Removing a redundant head does not rewrite the event's generation.
        db.execute(
            "UPDATE usage_event SET pricing_generation=1 WHERE event_id=?1",
            [id],
        )
        .unwrap();
        if stored {
            db.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,unpriced_reason,origin,recorded_at_ms)
                        SELECT m.event_key,m.role,m.unit,1,'unpriced','unknown',0,'charge_basis_unknown','pricing_pass',1
                        FROM measurement m JOIN usage_event e USING(event_key) WHERE e.event_id=?1 AND m.kind='billed'", [id]).unwrap();
        }
    }
    let plan = crate::subscription::read_basis_correction(
        &db,
        &ingest::BasisCorrectRequest {
            event_id: corrected.clone(),
            corrected_basis: ChargeBasis::SubscriptionIncluded,
            reason: "payer evidence".into(),
        },
    )
    .unwrap();
    let resolved = crate::subscription::resolve_reprice_without_catalog(&plan).unwrap();
    let tx = db.transaction().unwrap();
    ingest::correct_basis_in_tx(&tx, &plan, &resolved, 2).unwrap();
    tx.commit().unwrap();

    facts(&mut db);
    rate(&db, "a", 1_000_000_000);
    (db, unknown, corrected)
}

fn same_bytes<T: Serialize>(left: T, right: T) {
    assert_eq!(
        serde_json::to_string(&left).unwrap(),
        serde_json::to_string(&right).unwrap()
    );
}

#[test]
fn unknown_basis_derived_and_stored_heads_have_identical_reader_replies() {
    use crate::{multiplier, runs, verdict};
    let (mut legacy, unknown, corrected) = basis_parity_store(true);
    let (mut headless, _, _) = basis_parity_store(false);
    let config = config();
    let read = |old: &Connection, new: &Connection, rebuilt: bool| {
        for (group, zone) in [
            (json!([]), "UTC"),
            (
                json!(["provider", "model", "charge_basis"]),
                "Asia/Kathmandu",
            ),
            (json!(["harness", "work_class"]), "America/New_York"),
        ] {
            let request = request(json!({"grouping":group, "timezone":zone}));
            same_bytes(
                spend_report(old, &request, DAY_MS).unwrap(),
                spend_report(new, &request, DAY_MS).unwrap(),
            );
            same_bytes(
                spend_report_base(old, &request, DAY_MS, None).unwrap(),
                spend_report_base(new, &request, DAY_MS, None).unwrap(),
            );
        }
        same_bytes(
            ledger_coverage(old, &LedgerCoverageRequest {}, DAY_MS).unwrap(),
            ledger_coverage(new, &LedgerCoverageRequest {}, DAY_MS).unwrap(),
        );
        same_bytes(
            work_class_report(old, &WorkClassReportRequest {}).unwrap(),
            work_class_report(new, &WorkClassReportRequest {}).unwrap(),
        );
        same_bytes(
            subscription_report(old, &SubscriptionReportRequest {}).unwrap(),
            subscription_report(new, &SubscriptionReportRequest {}).unwrap(),
        );
        let verdict = serde_json::from_value(json!({})).unwrap();
        same_bytes(
            verdict::evaluate(old, &verdict).unwrap(),
            verdict::evaluate(new, &verdict).unwrap(),
        );
        same_bytes(
            verdict::evaluate_base(old, &verdict).unwrap(),
            verdict::evaluate_base(new, &verdict).unwrap(),
        );
        let list = runs::ListRequest {
            from_ms: 0,
            to_ms: DAY_MS,
            harness: None,
            limit: None,
            after: None,
        };
        let detail = runs::DetailRequest {
            key: runs::RunKey::Session {
                harness: "opencode".into(),
                session: "session-a".into(),
            },
            limit: None,
            after: None,
        };
        let listed = runs::list(new, &list).unwrap();
        assert!(!listed.rows.is_empty());
        same_bytes(runs::list(old, &list).unwrap(), listed);
        same_bytes(
            runs::detail(old, &detail).unwrap(),
            runs::detail(new, &detail).unwrap(),
        );
        if rebuilt {
            for grouping in [json!(["alfonso"]), json!(["plan"])] {
                let request = request(json!({"grouping":grouping}));
                let derived = spend_report(new, &request, DAY_MS).unwrap();
                assert!(!derived.rows.is_empty());
                same_bytes(spend_report(old, &request, DAY_MS).unwrap(), derived);
            }
        }
    };
    read(&legacy, &headless, false);
    let explain = |db, id: &str| {
        explain_charge(
            db,
            &ChargeExplainRequest {
                event_id: id.into(),
            },
        )
        .unwrap()
    };
    let mut old = explain(&legacy, &unknown);
    let mut new = explain(&headless, &unknown);
    for (recorded, derived) in old.measurements.iter_mut().zip(&mut new.measurements) {
        let wire = serde_json::to_value(&*derived).unwrap();
        if recorded.measurement.kind == "observed" {
            assert!(wire.get("derived_charge").is_none());
        } else {
            assert_eq!(recorded.charges.len(), 1);
            assert!(derived.charges.is_empty());
            assert_eq!(
                wire["derived_charge"],
                json!({"outcome":"unpriced", "unpriced_reason":"charge_basis_unknown", "source":"the call's recorded charge basis"})
            );
        }
        recorded.charges.clear();
        derived.derived_charge = None;
    }
    same_bytes(old, new);
    let corrected_reply = explain(&headless, &corrected);
    assert_eq!(
        corrected_reply.event.charge_basis,
        ChargeBasis::SubscriptionIncluded
    );
    assert!(corrected_reply
        .measurements
        .iter()
        .all(|row| row.derived_charge.is_none()));
    assert_eq!(
        corrected_reply
            .measurements
            .iter()
            .flat_map(|row| &row.charges)
            .map(|charge| (charge.outcome.as_str(), charge.origin.as_str()))
            .collect::<Vec<_>>(),
        [
            ("covered", "basis_correction"),
            ("covered", "basis_correction")
        ]
    );
    same_bytes(explain(&legacy, &corrected), corrected_reply);

    for db in [&mut legacy, &mut headless] {
        summaries(db);
        assert!(!drain(db, &config).is_empty());
    }
    read(&legacy, &headless, true);
    for table in [
        "summary_cell",
        "summary_quantity",
        "summary_money",
        "summary_run_cell",
        "valuation_cell",
        "attribution_cell",
        "attribution_valuation_cell",
    ] {
        let cells = |db: &Connection| {
            let mut stmt = db
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1,2,3"))
                .unwrap();
            let width = stmt.column_count();
            stmt.query_map([], |row| {
                (0..width)
                    .map(|column| row.get::<_, rusqlite::types::Value>(column))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };
        let derived = cells(&headless);
        assert!(!derived.is_empty(), "{table}");
        assert_eq!(cells(&legacy), derived, "{table}");
    }
    let inputs = |db| multiplier::plan_observation(db, &config, "pool", 0, DAY_MS).unwrap();
    let derived = inputs(&headless);
    assert!(derived
        .models
        .contains(&("provider".into(), Some("a".into()))));
    assert_eq!(inputs(&legacy), derived);
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
fn attribution_summary_publication_plan_seeks_rowid_logical_key() {
    use crate::report::hourly::attribution::DELETE_CELLS_SQL;
    let conn = conn();
    assert_eq!(
        conn.query_row(
            "SELECT wr FROM pragma_table_list WHERE name='attribution_cell'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    let columns=conn.prepare("SELECT name FROM pragma_index_info('sqlite_autoindex_attribution_cell_1') ORDER BY seqno").unwrap().query_map([],|row|row.get::<_,String>(0)).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
    assert_eq!(
        columns,
        [
            "kind",
            "hour_number",
            "currency",
            "attribution",
            "provider",
            "model"
        ]
    );
    assert!(conn.query_row("SELECT \"unique\" FROM pragma_index_list('attribution_cell') WHERE name='sqlite_autoindex_attribution_cell_1'",[],|row|row.get::<_,bool>(0)).unwrap());
    let plan = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {DELETE_CELLS_SQL}"))
        .unwrap()
        .query_map(params![0, 1], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(plan.iter().any(|step|step=="SEARCH attribution_cell USING COVERING INDEX sqlite_autoindex_attribution_cell_1 (kind=? AND hour_number=?)"),"{plan:?}");
    assert!(
        !plan
            .iter()
            .any(|step| step.contains("SCAN attribution_cell") || step.contains("usage_event")),
        "{plan:?}"
    );
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

    broca_call(&mut conn, "historical-broca", 2, "broca-run");
    summaries(&mut conn);
    drain(&mut conn, &config());
    let revision = |conn: &Connection| {
        conn.query_row(
        "SELECT a.revision,s.revision FROM attribution_hour a JOIN summary_hour s USING(kind,hour_number)
         WHERE a.kind=0 AND a.hour_number=0", [], |row| Ok((row.get::<_,i64>(0)?, row.get::<_,i64>(1)?))
    ).unwrap()
    };
    let (attribution_revision, summary_revision) = revision(&conn);
    broca_stamp(
        &mut conn,
        "historical-broca",
        CallKind::Segment,
        ScopeStamp {
            agent_id: Some("backfilled-owner".into()),
            ..ScopeStamp::default()
        },
    );
    assert_eq!(
        revision(&conn),
        (attribution_revision + 1, summary_revision)
    );
    let query = request(json!({}));
    let live = spend_report(&conn, &query, DAY_MS).unwrap();
    assert!(live
        .rows
        .iter()
        .any(|row| row.grouping["alfonso"].as_deref() == Some("backfilled-owner")));
    assert_parity(&conn, "stamped attribution stale");
    summaries(&mut conn);
    drain(&mut conn, &config());
    same_bytes(live, spend_report(&conn, &query, DAY_MS).unwrap());
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
            unattributed_calls: 0,
            stamp_disagreement_calls: 0,
        }
    );
}

#[test]
fn attribution_report_hour_and_fractional_offset_day_seams_match_ordinary_totals() {
    let mut conn = conn();
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        0,
        Some(3 * DAY_MS),
    );
    for (n, at) in [
        1,
        HOUR_MS - 1,
        HOUR_MS + 1,
        66_599_999,
        66_600_000,
        DAY_MS - 1,
        DAY_MS + 1,
    ]
    .into_iter()
    .enumerate()
    {
        call(
            &mut conn,
            &format!("seam-{n}"),
            at,
            if n % 2 == 0 { "session-a" } else { "session-b" },
            "a",
            "subscription_included",
            100,
        );
    }
    facts(&mut conn);
    rate(&conn, "a", 1_000_000_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    for (grain, zone, to) in [
        ("hour", "UTC", 2 * HOUR_MS),
        ("day", "Asia/Kolkata", DAY_MS),
        ("day", "America/New_York", DAY_MS),
    ] {
        let ordinary = spend_report(
            &conn,
            &request(json!({"grouping":["model"],"grain":grain,"timezone":zone,"to_ms":to})),
            3 * DAY_MS,
        )
        .unwrap();
        let split = spend_report(
            &conn,
            &request(
                json!({"grouping":["alfonso","model"],"grain":grain,"timezone":zone,"to_ms":to}),
            ),
            3 * DAY_MS,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&split.totals).unwrap(),
            serde_json::to_value(&ordinary.totals).unwrap(),
            "{grain}/{zone}"
        );
        assert_eq!(
            split.rate_staleness, ordinary.rate_staleness,
            "{grain}/{zone}"
        );
        for expected in &ordinary.rows {
            let rows = split
                .rows
                .iter()
                .filter(|row| {
                    row.bucket_start_ms == expected.bucket_start_ms
                        && row.bucket_end_ms == expected.bucket_end_ms
                        && row.currency == expected.currency
                        && row.grouping["model"] == expected.grouping["model"]
                })
                .collect::<Vec<_>>();
            assert_eq!(
                rows.iter().map(|row| row.events_total).sum::<u64>(),
                expected.events_total,
                "{grain}/{zone}"
            );
            assert_eq!(
                rows.iter()
                    .map(|row| row.valuation.list_value.nanos)
                    .sum::<i64>(),
                expected.valuation.list_value.nanos,
                "{grain}/{zone}"
            );
            let summed = rows
                .iter()
                .filter_map(|row| row.valuation.deduced.as_ref())
                .map(|value| i128::from(value.nanos))
                .sum::<i128>();
            let target = i128::from(expected.valuation.deduced.as_ref().unwrap().nanos);
            assert!(
                (summed - target).abs() * 2 <= i128::try_from(rows.len()).unwrap(),
                "{grain}/{zone}"
            );
        }
        if zone == "Asia/Kolkata" {
            assert_eq!(
                split
                    .rows
                    .iter()
                    .filter(|row| row.bucket_start_ms == -19_800_000)
                    .map(|row| row.events_total)
                    .sum::<u64>(),
                4
            );
            assert_eq!(
                split
                    .rows
                    .iter()
                    .filter(|row| row.bucket_start_ms == 66_600_000)
                    .map(|row| row.events_total)
                    .sum::<u64>(),
                3
            );
        }
    }
}

#[test]
fn spend_report_range_matches_daily_values_across_dst() {
    let mut conn = conn();
    let at = |day, hour| {
        Utc.with_ymd_and_hms(2025, 3, day, hour, 0, 0)
            .unwrap()
            .timestamp_millis()
    };
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    for (n, day, hour, session, model, quantity) in [
        (0, 7, 12, "session-a", "a", 1),
        (1, 8, 12, "session-a", "a", 1),
        (2, 9, 6, "session-a", "a", 1),
        (3, 9, 8, "session-b", "a", 7),
        (4, 10, 1, "session-a", "a", 1),
        (5, 10, 6, "session-b", "a", 3),
        (6, 10, 7, "session-a", "excluded", 9),
        (7, 13, 7, "session-b", "a", 2),
    ] {
        call(
            &mut conn,
            &format!("range-{n}"),
            at(day, hour),
            session,
            model,
            "subscription_included",
            quantity,
        );
    }
    facts(&mut conn);
    // One input token is worth 0.4 list nanos, so rounding after summing
    // the period would give a different result from summing the day rows.
    rate(&conn, "a", 400_000);
    rate(&conn, "excluded", 400_000);
    summaries(&mut conn);
    drain(&mut conn, &config());
    let now = at(12, 12);
    for grouping in [vec!["alfonso", "workload"], vec!["model", "plan"]] {
        for explicit in [true, false] {
            let mut query = request(json!({
                "grouping": grouping, "timezone": "America/New_York",
                "from_ms": explicit.then(|| at(7, 1)), "to_ms": explicit.then(|| at(11, 1)),
                "where": {"model": ["a"]}
            }));
            let day = spend_report(&conn, &query, now).unwrap();
            assert!(day
                .rows
                .iter()
                .any(|row| row.bucket_end_ms - row.bucket_start_ms == 23 * HOUR_MS));
            assert!(day
                .rows
                .iter()
                .filter_map(|r| r.valuation.deduced.as_ref())
                .any(|v| v.nanos > 0));
            query.grain = "range".into();
            let range = spend_report(&conn, &query, now).unwrap();
            assert_eq!(range.totals, day.totals);
            assert_eq!(range.attribution_coverage, day.attribution_coverage);
            let (start, end) = if explicit {
                (at(6, 5), at(11, 4))
            } else {
                (at(7, 5), at(14, 4))
            };
            let groups: BTreeSet<_> = day
                .rows
                .iter()
                .map(|r| (&r.grouping, &r.currency))
                .collect();
            assert_eq!(range.rows.len(), groups.len());
            for row in &range.rows {
                assert_eq!((row.bucket_start_ms, row.bucket_end_ms), (start, end));
                let matching: Vec<_> = day
                    .rows
                    .iter()
                    .filter(|r| r.grouping == row.grouping && r.currency == row.currency)
                    .collect();
                assert!(!matching.is_empty());
                let money = |r: &SpendRow| {
                    [
                        r.total_nanos,
                        r.metered_nanos,
                        r.valuation.list_value.nanos,
                        r.valuation.list_value.unvalued.tokens,
                        r.valuation.list_value.base_rate_tier_untested_tokens,
                        r.valuation.deduced.as_ref().unwrap().nanos,
                    ]
                };
                let counts = |r: &SpendRow| {
                    [
                        r.events_total,
                        r.events_priced,
                        r.events_unpriced,
                        r.cost_sample_n,
                        r.zero_measurement_events,
                        r.measurements_total,
                        r.valuation.list_value.unvalued.measurements,
                        r.usage.tokens.counts.input.unwrap(),
                    ]
                };
                for (i, v) in money(row).into_iter().enumerate() {
                    assert_eq!(
                        v,
                        matching.iter().map(|r| money(r)[i]).sum::<i64>(),
                        "money slot {i}"
                    );
                }
                for (i, v) in counts(row).into_iter().enumerate() {
                    assert_eq!(
                        v,
                        matching.iter().map(|r| counts(r)[i]).sum::<u64>(),
                        "count slot {i}"
                    );
                }
                assert_eq!(
                    row.valuation.list_value.not_yet_valued_hours,
                    matching
                        .iter()
                        .map(|r| r.valuation.list_value.not_yet_valued_hours)
                        .sum::<usize>()
                );
                assert_eq!(
                    row.valuation.deduced.as_ref().unwrap().pending_days,
                    matching
                        .iter()
                        .map(|r| r.valuation.deduced.as_ref().unwrap().pending_days)
                        .sum::<usize>()
                );
            }
        }
    }
}
