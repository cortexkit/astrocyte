use super::*;
use crate::ingest;
use crate::money::round_nano_num;
use crate::rate_era::{fetched, publish};
use crate::subscription::PlanLedger;
use crate::test_support::conn;
use serde_json::json;

#[path = "attribution_report_tests.rs"]
mod attribution_report_tests;

fn record_id(ledger: PlanLedger, identity: &str, effective_from_ms: i64) -> PlanRecordId {
    PlanRecordId {
        ledger,
        provider: "provider".into(),
        identity: identity.into(),
        effective_from_ms,
    }
}

fn rate(conn: &Connection, model: &str, units: i64) {
    let history = serde_json::from_value(json!({
        "source":"synthetic", "provider_id":"provider", "model_id":model,
        "fact_prefix":"rate.", "eras":[], "served_by_key":{
            "rate.input":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}],
            "rate.output":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":units*2,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}]
        }
    })).unwrap();
    publish(conn, &fetched(history, "provider", model, None).unwrap(), 1).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn event(
    conn: &mut Connection,
    id: &str,
    at: i64,
    model: &str,
    account: Option<&str>,
    credential: Option<&str>,
    basis: &str,
    quantity: i64,
) -> i64 {
    let mut request: ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"opencode","producer_ref":id,"provider":"provider","model":model,
        "account_ref":account,"auth_selection":"selected",
        "occurred_at_ms":at,"charge_basis":basis,
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":quantity,"quantity_exponent":0},
            {"role":"output","unit":"token","kind":"billed","source":"fixture","quantity_units":quantity/2,"quantity_exponent":0}]
    })).unwrap();
    request.resolved_credential = credential.map(str::to_string);
    let result = ingest::record_event(conn, &request, 1).unwrap();
    conn.query_row(
        "SELECT event_key FROM usage_event WHERE event_id=?1",
        [result.event_id],
        |r| r.get(0),
    )
    .unwrap()
}

fn plan(
    conn: &Connection,
    ledger: PlanLedger,
    provider: &str,
    identity: &str,
    from: i64,
    to: Option<i64>,
) {
    conn.execute(&format!("INSERT INTO {}({},provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms,effective_to_ms) VALUES(?1,?2,'plan',2000,'USD',2,'month',?3,?4)",ledger.table(),ledger.key_column()),params![identity,provider,from,to]).unwrap();
}

fn prepare(conn: &mut Connection) {
    let tx = conn.transaction().unwrap();
    ValuationMaintenance::prepare(&tx).unwrap();
    tx.commit().unwrap();
}

fn drain(conn: &mut Connection) -> Vec<i64> {
    prepare(conn);
    let mut maintenance = ValuationMaintenance::default();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut hours = Vec::new();
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "valuation did not drain"
        );
        let tx = conn.unchecked_transaction().unwrap();
        let ready = maintenance.read_chunk(&tx).unwrap();
        tx.commit().unwrap();
        if ready {
            let hour = maintenance.pending.as_ref().unwrap().hour.number;
            let tx = conn.transaction().unwrap();
            if maintenance.publish(&tx).unwrap() {
                hours.push(hour);
            }
            tx.commit().unwrap();
        } else if !maintenance.has_pending() {
            break;
        }
    }
    hours
}

#[test]
fn valuation_migration_requeues_existing_media_and_unavailable_rate_hours_only() {
    let mut conn = Connection::open_in_memory().unwrap();
    crate::store::initialize_connection(&conn).unwrap();
    let (previous, migration) = crate::store::migration_parts(63);
    for step in previous {
        conn.execute_batch(step.statements).unwrap();
    }
    crate::test_support::add_scope_schema_for_historical_summary(&conn);
    let history = serde_json::from_value(json!({"source":"test","provider_id":"provider","model_id":"media","fact_prefix":"rate.","eras":[],"served_by_key":{
        "rate.input_audio.per_second":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":7,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"},"per":1}}]
    }})).unwrap();
    publish(
        &conn,
        &fetched(history, "provider", "media", None).unwrap(),
        1,
    )
    .unwrap();
    let media = serde_json::from_value(json!({"producer":"colliculus","producer_ref":"media","provider":"provider","model":"media","occurred_at_ms":1,"charge_basis":"metered_api","measurements":[
        {"role":"input_audio","unit":"second","kind":"billed","source":"fixture","quantity_units":2,"quantity_exponent":0}
    ]})).unwrap();
    ingest::record_event(&mut conn, &media, 1).unwrap();
    for (hour, model) in [(1, "ordinary"), (2, "failed"), (3, "alias")] {
        rate(&conn, model, 1_000_000_000);
        event(
            &mut conn,
            model,
            hour * HOUR_MS,
            model,
            None,
            None,
            "unknown",
            100,
        );
    }
    conn.execute("UPDATE rate_model SET reference_provider='provider',reference_model='failed' WHERE model='alias'", []).unwrap();
    summaries(&mut conn);
    assert_eq!(drain(&mut conn), [0, 1, 2, 3]);

    // Simulate a cached media hour with no list value and a rate model whose
    // failed refresh cleared its catalog version.
    conn.execute(
        "UPDATE valuation_cell SET list_numerator='0' WHERE hour_number=0",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE rate_model SET catalog_version=NULL WHERE model='failed'",
        [],
    )
    .unwrap();
    let original = stored_cells(&conn, 1);
    conn.execute_batch(migration.statements).unwrap();
    let hours = |sql| {
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert!(hours("SELECT hour_number FROM summary_dirty_hour").is_empty());
    assert_eq!(
        hours("SELECT hour_number FROM valuation_hour WHERE state='dirty' ORDER BY hour_number"),
        [0, 2, 3]
    );
    assert_eq!(hours("SELECT hour_number FROM attribution_hour WHERE valuation_revision IS NULL ORDER BY hour_number"),[0,2,3]);
    assert_eq!(drain(&mut conn), [0, 2, 3]);
    assert_eq!(stored_cells(&conn, 1), original);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|cell| cell.numerator)
            .sum::<i128>(),
        14_000_000
    );
    assert_eq!(state(&conn, 2), "not_yet_valued");
    assert_eq!(state(&conn, 3), "not_yet_valued");
}

#[test]
fn ledger_regression_media_list_value_uses_unit_pricing_and_dated_rates() {
    let mut conn = conn();
    let rate = |units| json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"},"per":2,"increment":{"kind":"quantum","quantity":2},"minimum_quantity":{"kind":"minimum","quantity":3}});
    let history = serde_json::from_value(json!({"source":"test","provider_id":"provider","model_id":"media","fact_prefix":"rate.","eras":[],"served_by_key":{
        "rate.input_audio.per_second":[{"from_ms":0,"until_ms":2,"state":"value","value":rate(7)}, {"from_ms":2,"until_ms":null,"state":"value","value":rate(11)}]
    }})).unwrap();
    publish(
        &conn,
        &fetched(history, "provider", "media", None).unwrap(),
        1,
    )
    .unwrap();
    for at in [1, 2] {
        let event = serde_json::from_value(json!({"producer":"colliculus","producer_ref":format!("media-{at}"),"provider":"provider","model":"media","occurred_at_ms":at,"charge_basis":"metered_api","measurements":[
            {"role":"input_audio","unit":"second","kind":"billed","source":"fixture","quantity_units":9,"quantity_exponent":-1}
        ]})).unwrap();
        ingest::record_event(&mut conn, &event, 1).unwrap();
    }
    let request = ReportRequest {
        from_ms: Some(0),
        to_ms: Some(fees::DAY_MS),
        grain: "day".into(),
        timezone: "UTC".into(),
        ..ReportRequest::default()
    };
    let list = |conn: &Connection| {
        spend_report(conn, &request, 2 * fees::DAY_MS)
            .unwrap()
            .rows
            .remove(0)
            .valuation
            .list_value
    };
    for built in [false, true] {
        if built {
            summaries(&mut conn);
            drain(&mut conn);
        }
        let value = list(&conn);
        // 0.9 seconds rounds up to 2 and then to the minimum of 3.
        // The two charges round 10.5 and 16.5 to 10 and 16 respectively.
        assert_eq!(value.nanos, 26);
        assert_eq!(value.unvalued.measurements, 0);
    }
}

#[test]
fn broca_attempt_migration_dirties_only_its_valuation_and_attribution_hours() {
    let conn = Connection::open_in_memory().unwrap();
    crate::store::initialize_connection(&conn).unwrap();
    let (previous, migration) = crate::store::migration_parts(63);
    for migration in previous {
        conn.execute_batch(migration.statements).unwrap();
    }
    conn.execute_batch(&format!("INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,charge_basis,pricing_generation)
        VALUES ('legacy','broca','legacy',0,1,'unknown',0),('attempt','broca','attempt',{HOUR_MS},1,'unknown',0);
        INSERT INTO broca_attempt_detail(event_key,fact_id,share_index,attempt_id,step_id,attempt,outcome,content_received,response_bytes,recorded_at_ms,requested_service_tier)
        SELECT event_key,'fact',0,'attempt',1,0,'complete',1,1,1,'standard' FROM usage_event WHERE event_id='attempt';
        UPDATE summary_hour SET built_revision=revision,format_version={FORMAT};
        DELETE FROM summary_dirty_hour;
        UPDATE valuation_hour SET state='built';
        UPDATE attribution_hour SET valuation_revision=1;")).unwrap();
    conn.execute_batch(migration.statements).unwrap();
    let hours = conn.prepare("SELECT v.hour_number,v.state,a.valuation_revision,s.built_revision=s.revision FROM valuation_hour v JOIN attribution_hour a USING(kind,hour_number) JOIN summary_hour s USING(kind,hour_number) ORDER BY v.hour_number").unwrap()
        .query_map([], |r| Ok((r.get::<_, i64>(0)?,r.get::<_, String>(1)?,r.get::<_, Option<i64>>(2)?,r.get::<_, bool>(3)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
    assert_eq!(
        hours,
        [
            (0, "built".into(), Some(1), true),
            (1, "dirty".into(), None, true)
        ]
    );
}

#[test]
fn ledger_regression_broca_attempt_values_fast_mode_and_own_prompt_tier() {
    let mut conn = conn();
    let segment = |units: i64| json!([{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}]);
    for (model, rates) in [
        (
            "fast-rate",
            json!({"rate.input":segment(1_000_000_000),"rate.input.mode.fast":segment(4_000_000_000)}),
        ),
        ("missing-fast", json!({"rate.input":segment(1_000_000_000)})),
        (
            "tier",
            json!({"rate.input":segment(1_000_000_000),"rate.input.above_context.100":segment(5_000_000_000)}),
        ),
    ] {
        let history = serde_json::from_value(json!({"source":"test","provider_id":"provider","model_id":model,"fact_prefix":"rate.","eras":[],"served_by_key":rates})).unwrap();
        publish(
            &conn,
            &fetched(history, "provider", model, None).unwrap(),
            1,
        )
        .unwrap();
        let (quantity, mode) = if model == "tier" {
            (101, "standard")
        } else {
            (1_000, "fast")
        };
        let request = serde_json::from_value(json!({"producer":"broca","producer_ref":model,"provider":"provider","model":model,"occurred_at_ms":1,"charge_basis":"metered_api","measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":quantity,"quantity_exponent":0}]})).unwrap();
        let outcome = ingest::record_event(&mut conn, &request, 1).unwrap();
        conn.execute("INSERT INTO broca_attempt_detail(event_key,fact_id,share_index,attempt_id,step_id,attempt,outcome,content_received,response_bytes,recorded_at_ms,requested_service_tier,mode) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,0,?2,1,0,'complete',1,1,1,'standard',?3)",params![outcome.event_id,model,mode]).unwrap();
    }
    let mut cells = Cells::new();
    assert!(reduce(&conn, "occurred_at_ms=1", &mut cells).unwrap().1);
    let value = |model: &str| {
        cells
            .iter()
            .find(|((key, _), _)| key.0[Grouping::Model as usize].as_deref() == Some(model))
            .unwrap()
            .1
    };
    assert_eq!(
        round_nano_num(value("fast-rate").numerator).unwrap(),
        4_000_000
    );
    assert_eq!(round_nano_num(value("tier").numerator).unwrap(), 505_000);
    assert_eq!(value("tier").recorded.base_rate_tier_untested_tokens, 0);
    assert_eq!(value("missing-fast").numerator, 0);
    assert_eq!(value("missing-fast").unvalued.measurements, 1);
}

#[test]
fn ledger_regression_failed_rate_reload_keeps_clean_and_dirty_list_value_equal() {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    event(&mut conn, "original", 1, "A", None, None, "unknown", 100);
    summaries(&mut conn);
    drain(&mut conn);
    let list_value = |conn: &Connection| {
        let request = ReportRequest {
            from_ms: Some(0),
            to_ms: Some(fees::DAY_MS),
            grain: "day".into(),
            timezone: "UTC".into(),
            ..ReportRequest::default()
        };
        spend_report(conn, &request, 2 * fees::DAY_MS)
            .unwrap()
            .rows
            .remove(0)
            .valuation
            .list_value
    };
    let before = list_value(&conn);
    assert_eq!(before.nanos, 200_000);
    crate::rate_era::prepare(&conn, 2).unwrap();
    crate::rate_era::record_failure(
        &conn,
        "provider",
        "A",
        &crate::multiplier::MultiplierError::RateFactUnrecognized("unknown rate key".into()),
    )
    .unwrap();
    assert_eq!(
        crate::rate_era::pending(&conn, 1).unwrap(),
        vec![("provider".into(), "A".into())]
    );
    assert_eq!(list_value(&conn), before);
    event(&mut conn, "zero", 2, "A", None, None, "unknown", 0);
    assert_eq!(list_value(&conn), before);
}

fn summaries(conn: &mut Connection) {
    let mut maintenance = SummaryMaintenance::default();
    for _ in 0..1000 {
        let dirty: bool = conn
            .query_row("SELECT EXISTS(SELECT 1 FROM summary_dirty_hour)", [], |r| {
                r.get(0)
            })
            .unwrap();
        if !dirty {
            return;
        }
        if maintenance.read_chunk(conn, 10_000).unwrap() {
            let tx = conn.transaction().unwrap();
            maintenance.publish(&tx, 10_000).unwrap();
            tx.commit().unwrap();
        }
    }
    panic!("summary did not drain");
}

fn stored_cells(conn: &Connection, hour: i64) -> Cells {
    let mut stmt = conn.prepare("SELECT summary_key,plan,plan_account,recorded,list_numerator,unvalued,plan_record FROM valuation_cell WHERE kind=0 AND hour_number=?1").unwrap();
    stmt.query_map([hour], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, Option<String>>(6)?,
        ))
    })
    .unwrap()
    .map(|row| {
        let (key, plan, plan_account, recorded, numerator, unvalued, record) = row.unwrap();
        (
            (
                serde_json::from_str(&key).unwrap(),
                PlanMember {
                    plan,
                    plan_account,
                    record: record.map(|record| serde_json::from_str(&record).unwrap()),
                },
            ),
            ValuedCell {
                recorded: serde_json::from_str(&recorded).unwrap(),
                numerator: numerator.parse().unwrap(),
                unvalued: serde_json::from_str(&unvalued).unwrap(),
            },
        )
    })
    .collect()
}

fn state(conn: &Connection, hour: i64) -> String {
    conn.query_row(
        "SELECT state FROM valuation_hour WHERE kind=0 AND hour_number=?1",
        [hour],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn attribution_projection_combines_ordinary_dimensions_without_losing_quantities() {
    let mut conn = conn();
    event(
        &mut conn,
        "a",
        1,
        "A",
        Some("account-a"),
        None,
        "metered_api",
        4,
    );
    event(
        &mut conn,
        "b",
        2,
        "A",
        Some("account-b"),
        None,
        "metered_api",
        6,
    );
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    assert_eq!(
        conn.query_row("SELECT count(*) FROM summary_cell", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM attribution_cell", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let payload: String = conn
        .query_row("SELECT payload FROM attribution_cell", [], |r| r.get(0))
        .unwrap();
    let recorded: Recorded = serde_json::from_str(&payload).unwrap();
    assert_eq!(recorded.cell.events, 2);
    assert_eq!(recorded.cell.measurements, 4);
    assert_eq!(recorded.cell.priced, 0);
    assert_eq!(
        recorded
            .quantities
            .iter()
            .map(|(_, q)| q.observations)
            .sum::<u64>(),
        4
    );
    assert_eq!(
        recorded
            .quantities
            .iter()
            .find(|(key, _)| key.1 == "input")
            .unwrap()
            .1
            .units,
        10
    );
    assert_eq!(
        recorded
            .quantities
            .iter()
            .find(|(key, _)| key.1 == "output")
            .unwrap()
            .1
            .units,
        5
    );
    assert_eq!(
        conn.query_row(
            "SELECT sum(CAST(list_numerator AS INTEGER)) FROM attribution_valuation_cell",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        60
    );
}

#[test]
fn attribution_changes_do_not_block_in_flight_main_publication() {
    let mut conn = conn();
    event(&mut conn, "one", 1, "A", None, None, "metered_api", 4);
    rate(&conn, "A", 3);
    prepare(&mut conn);
    let mut summary = SummaryMaintenance::default();
    assert!(summary.read_chunk(&conn, 10_000).unwrap());
    attribution::bump(&conn, Hour { kind: 0, number: 0 });
    let tx = conn.transaction().unwrap();
    assert!(summary.publish(&tx, 10_000).unwrap().published);
    tx.commit().unwrap();
    assert!(summary_current(&conn, Hour { kind: 0, number: 0 }).unwrap());
    let mut valued = ValuationMaintenance::default();
    assert!(valued.read_chunk(&conn).unwrap());
    attribution::bump(&conn, Hour { kind: 0, number: 0 });
    let tx = conn.transaction().unwrap();
    assert!(valued.publish(&tx).unwrap());
    tx.commit().unwrap();
    assert_eq!(state(&conn, 0), "built");
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|c| c.numerator)
            .sum::<i128>(),
        24
    );
    assert_eq!(conn.query_row("SELECT count(*) FROM attribution_hour WHERE summary_attribution_revision IS NOT revision AND valuation_attribution_revision IS NOT revision",[],|r|r.get::<_,i64>(0)).unwrap(),1);
}

#[test]
fn attribution_only_backlog_writes_no_main_summary_or_valuation_rows() {
    let mut conn = conn();
    event(&mut conn, "one", 1, "A", None, None, "metered_api", 4);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    conn.execute("UPDATE summary_audit_state SET next_due_ms=100000", [])
        .unwrap();
    conn.execute_batch("CREATE TEMP TABLE main_writes(table_name TEXT);")
        .unwrap();
    for table in [
        "summary_hour",
        "summary_cell",
        "summary_quantity",
        "summary_money",
        "summary_cost_frequency",
        "summary_run_cell",
        "summary_dirty_hour",
        "summary_audit_state",
        "valuation_hour",
        "valuation_cell",
    ] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            conn.execute_batch(&format!("CREATE TEMP TRIGGER watch_{table}_{operation} AFTER {operation} ON {table} BEGIN INSERT INTO main_writes VALUES('{table}'); END;")).unwrap();
        }
    }
    attribution::bump(&conn, Hour { kind: 0, number: 0 });
    let mut summary = SummaryMaintenance::default();
    for step in 0..100 {
        assert!(step < 99, "attribution summary did not drain");
        if summary.read_chunk(&conn, 10_000).unwrap() {
            let tx = conn.transaction().unwrap();
            let outcome = summary.publish(&tx, 10_000).unwrap();
            if outcome.published {
                assert!(outcome.attribution_only);
            }
            tx.commit().unwrap();
        } else if !summary.has_pending() {
            break;
        }
    }
    assert_eq!(drain(&mut conn), vec![0]);
    assert_eq!(
        conn.query_row("SELECT count(*) FROM main_writes", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(conn.query_row("SELECT count(*) FROM attribution_hour a JOIN summary_hour s USING(kind,hour_number) JOIN valuation_hour v USING(kind,hour_number) WHERE a.summary_revision=s.revision AND a.summary_attribution_revision=a.revision AND a.valuation_revision=v.revision AND a.valuation_attribution_revision=a.revision",[],|r|r.get::<_,i64>(0)).unwrap(),1);
    assert_eq!(
        conn.query_row(
            "SELECT attribution,json_extract(payload,'$.cell.events') FROM attribution_cell",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        )
        .unwrap(),
        (0, 1)
    );
    assert_eq!(
        conn.query_row(
            "SELECT list_numerator FROM attribution_valuation_cell",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "24"
    );
    let bytes: String = conn
        .query_row("SELECT summary_key FROM valuation_cell", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&bytes)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(FORMAT, 6);
}

#[test]
fn attribution_own_revision_fences_summary_and_valuation_publication() {
    let mut conn = conn();
    event(&mut conn, "one", 1, "A", None, None, "metered_api", 4);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    attribution::bump(&conn, Hour { kind: 0, number: 0 });
    let mut summary = SummaryMaintenance::default();
    assert!(summary.read_chunk(&conn, 10_000).unwrap());
    attribution::bump(&conn, Hour { kind: 0, number: 0 });
    let tx = conn.transaction().unwrap();
    assert!(!summary.publish(&tx, 10_000).unwrap().published);
    tx.commit().unwrap();
    assert!(attribution::next_summary(&conn, None).unwrap().is_some());
    let mut valued = ValuationMaintenance::default();
    assert!(valued.read_chunk(&conn).unwrap());
    attribution::bump(&conn, Hour { kind: 0, number: 0 });
    let tx = conn.transaction().unwrap();
    assert!(!valued.publish(&tx).unwrap());
    tx.commit().unwrap();
    assert_eq!(state(&conn, 0), "built");
    assert_eq!(
        conn.query_row("SELECT sum(events_total) FROM summary_hour", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn valuation_summary_dirty_propagates_and_fences_publication() {
    let mut conn = conn();
    event(&mut conn, "one", 1, "A", None, None, "metered_api", 4);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    assert_eq!(state(&conn, 0), "built");
    event(&mut conn, "two", 2, "A", None, None, "metered_api", 6);
    assert_eq!(state(&conn, 0), "dirty");
    summaries(&mut conn);
    let mut maintenance = ValuationMaintenance::default();
    assert!(maintenance.read_chunk(&conn).unwrap());
    event(
        &mut conn,
        "during-build",
        3,
        "A",
        None,
        None,
        "metered_api",
        2,
    );
    let tx = conn.transaction().unwrap();
    assert!(!maintenance.publish(&tx).unwrap());
    tx.commit().unwrap();
    assert_eq!(state(&conn, 0), "dirty");
    summaries(&mut conn);
    drain(&mut conn);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|c| c.numerator)
            .sum::<i128>(),
        72
    );
}

#[test]
fn valuation_unfetched_models_wait_and_plan_intervals_dirty() {
    let mut conn = conn();
    for hour in 0..4 {
        event(
            &mut conn,
            &format!("event-{hour}"),
            hour * HOUR_MS + 1,
            "A",
            Some("account"),
            None,
            "subscription_included",
            5,
        );
    }
    summaries(&mut conn);
    drain(&mut conn);
    assert_eq!(state(&conn, 0), "not_yet_valued");
    assert!(stored_cells(&conn, 0).is_empty());
    rate(&conn, "A", 3);
    drain(&mut conn);
    assert_eq!(state(&conn, 0), "built");
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        HOUR_MS + 10,
        Some(2 * HOUR_MS),
    );
    assert_eq!(
        (state(&conn, 0), state(&conn, 1), state(&conn, 2)),
        ("built".into(), "dirty".into(), "built".into())
    );
    assert_eq!(drain(&mut conn), vec![1]);
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "credential",
        2 * HOUR_MS + 1,
        None,
    );
    assert_eq!(drain(&mut conn), vec![2, 3]);
    conn.execute(
        "UPDATE credential_subscription SET effective_to_ms=?1 WHERE credential_ref='credential'",
        [3 * HOUR_MS],
    )
    .unwrap();
    assert_eq!(drain(&mut conn), vec![3]);
}

#[test]
fn valuation_selective_invalidation_keeps_unaffected_hours_current_without_retagging() {
    let mut conn = conn();
    for (hour, model) in [(0, "A"), (1, "B"), (2, "B"), (3, "B"), (4, "B")] {
        event(
            &mut conn,
            &format!("event-{hour}"),
            hour * HOUR_MS + 1,
            model,
            Some("account"),
            None,
            "subscription_included",
            10,
        );
    }
    rate(&conn, "A", 3);
    rate(&conn, "B", 5);
    summaries(&mut conn);
    drain(&mut conn);
    // Leave one hour already dirty: a generation change cannot bless it current.
    event(
        &mut conn,
        "later",
        4 * HOUR_MS + 2,
        "B",
        Some("account"),
        None,
        "subscription_included",
        2,
    );
    let before: Vec<_> = (0..5).map(|h| stored_cells(&conn, h)).collect();
    rate(&conn, "A", 11);
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        2 * HOUR_MS,
        Some(3 * HOUR_MS),
    );
    prepare(&mut conn);
    assert_eq!(
        (
            state(&conn, 0),
            state(&conn, 1),
            state(&conn, 2),
            state(&conn, 3),
            state(&conn, 4)
        ),
        (
            "dirty".into(),
            "built".into(),
            "dirty".into(),
            "built".into(),
            "dirty".into()
        )
    );
    summaries(&mut conn);
    let rebuilt = drain(&mut conn);
    assert_eq!(
        rebuilt,
        vec![0, 2, 4],
        "only changed-model, changed-plan and already-dirty hours rebuild"
    );
    for hour in 0..5 {
        let mut full = Cells::new();
        reduce(
            &conn,
            &Hour {
                kind: 0,
                number: hour,
            }
            .filter(),
            &mut full,
        )
        .unwrap();
        assert_eq!(stored_cells(&conn, hour), full, "hour {hour}");
        let (current, retagged): (bool, bool) = conn.query_row("SELECT h.state='built' AND h.config_generation=s.config_generation,h.rate_generation=s.rate_generation AND h.plan_generation=s.plan_generation FROM valuation_hour h CROSS JOIN valuation_state s WHERE h.kind=0 AND h.hour_number=?1",[hour],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert!(current, "hour {hour} must be current");
        assert_eq!(
            retagged,
            rebuilt.contains(&hour),
            "only rebuilt hours receive current generation stamps"
        );
    }
    assert_eq!(stored_cells(&conn, 1), before[1]);
    assert_eq!(stored_cells(&conn, 3), before[3]);
    conn.execute("UPDATE valuation_state SET config='[{},[]]'", [])
        .unwrap();
    assert_eq!(drain(&mut conn), vec![0, 1, 2, 3, 4]);
}

#[test]
fn valuation_plan_membership_uses_active_account_or_credential_record() {
    let mut conn = conn();
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        0,
        Some(2),
    );
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "credential",
        0,
        None,
    );
    let member = |account, credential, basis, at| {
        plan_member(
            &PlanBook::load(&conn).unwrap(),
            "provider",
            basis,
            account,
            credential,
            at,
        )
    };
    assert_eq!(
        member(
            Some("account"),
            Some("credential"),
            "subscription_included",
            Some(1),
        ),
        PlanMember {
            plan: "provider".into(),
            plan_account: Some("account".into()),
            record: Some(record_id(PlanLedger::Account, "account", 0)),
        }
    );
    assert_eq!(
        member(None, Some("credential"), "unknown", Some(1)),
        PlanMember {
            plan: "provider".into(),
            plan_account: Some("credential".into()),
            record: Some(record_id(PlanLedger::Credential, "credential", 0)),
        }
    );
    for (account, credential, at) in [
        (None, Some("Credential"), Some(1)),
        (None, None, Some(1)),
        (Some(""), Some("credential"), Some(1)),
        (Some("account"), None, None),
        (Some("account"), None, Some(-1)),
        (Some("account"), None, Some(2)),
    ] {
        assert_eq!(
            member(account, credential, "unknown", at),
            PlanMember::non_pool("none")
        );
    }
    assert_eq!(
        member(Some("account"), Some("credential"), "metered_api", Some(1)),
        PlanMember::non_pool("api")
    );
    // A malformed ledger can contain overlapping records; a healthy ledger
    // refuses the second insert. Do not assign such a call to either record.
    conn.execute_batch("DROP TRIGGER account_subscription_no_overlap")
        .unwrap();
    plan(&conn, PlanLedger::Account, "provider", "account", 1, None);
    event(
        &mut conn,
        "overlap",
        1,
        "A",
        Some("account"),
        Some("credential"),
        "unknown",
        10,
    );
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    let cells = stored_cells(&conn, 0);
    assert_eq!(cells.len(), 1);
    let ((_, member), cell) = cells.first_key_value().unwrap();
    assert_eq!(member, &PlanMember::non_pool("pool_conflict"));
    assert_eq!(cell.numerator, 60);
}

#[test]
fn valuation_paying_credential_uses_resolved_else_auth_selection_exactly() {
    let mut conn = conn();
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "credential",
        0,
        None,
    );
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "selected",
        0,
        None,
    );
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    for (id, account, credential, quantity) in [
        ("resolved", None, Some("credential"), 2),
        ("selected", None, None, 4),
        ("case-mismatch", None, Some("Credential"), 6),
        ("account-wins", Some("account"), Some("credential"), 8),
    ] {
        event(
            &mut conn, id, 1, "A", account, credential, "unknown", quantity,
        );
    }
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    let actual: BTreeMap<_, _> = stored_cells(&conn, 0)
        .into_iter()
        .map(|((_, member), cell)| {
            (
                member.plan_account,
                (cell.recorded.cell.events, cell.numerator),
            )
        })
        .collect();
    assert_eq!(
        actual,
        BTreeMap::from([
            (Some("credential".into()), (1, 12)),
            (Some("selected".into()), (1, 24)),
            (None, (1, 36)),
            (Some("account".into()), (1, 48)),
        ])
    );
}

// Explicit ledger prices are deliberately unrelated to the list rates supplied
// by rate(), so the recorded and valuation amounts cannot accidentally alias.
fn recorded_price(conn: &Connection, key: i64, nanos: i64) -> i64 {
    let (id, basis): (String, String) = conn
        .query_row(
            "SELECT event_id,charge_basis FROM usage_event WHERE event_key=?1",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    if basis != "metered_api" {
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_key=?1",
            [key],
        )
        .unwrap();
        for role in ["input", "output"] {
            conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms) VALUES(?1,?2,'token',1,'covered','subscription_included',1,'USD',0,'pricing_pass',1)",params![key,role]).unwrap();
        }
        let tx = conn.unchecked_transaction().unwrap();
        ingest::record_adjustment_in_tx(
            &tx,
            &ingest::AdjustmentRecordRequest {
                event_id: id,
                currency: "USD".into(),
                amount_nanos: -nanos,
                reason: ingest::AdjustmentReason::Refund,
                occurred_at_ms: 2,
                note: None,
            },
            &ingest::AdjustmentWriteContext::correction_ingest(format!("refund-{key}")),
            3,
        )
        .unwrap();
        tx.commit().unwrap();
        return -nanos;
    }
    conn.execute(
        "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_key=?1",
        [key],
    )
    .unwrap();
    for (role, amount) in [("input", nanos), ("output", 0)] {
        conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms) SELECT ?1,?2,'token',1,'priced',charge_basis,0,'USD',?3,1,9,'none','fixture',1,'{}','pricing_pass',1 FROM usage_event WHERE event_key=?1",params![key,role,amount]).unwrap();
    }
    nanos
}

#[test]
fn valuation_randomized_cells_equal_per_call_ledger_and_split_recorded_spend() {
    use chrono::TimeZone;
    let mut conn = conn();
    let mut seed = 7129_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    let mut oracle: BTreeMap<(i64, Key, PlanMember), (i128, i128)> = BTreeMap::new();
    let mut calls = Vec::new();
    for index in 0..360 {
        let hour = i64::try_from(next() % 72).unwrap();
        let at = if index < 4 {
            18 * HOUR_MS
                + if index % 2 == 0 {
                    HOUR_MS / 4
                } else {
                    3 * HOUR_MS / 4
                }
        } else {
            hour * HOUR_MS + i64::try_from(next() % 3_599_999).unwrap()
        };
        let hour = at / HOUR_MS;
        let quantity = i64::try_from(next() % 1000).unwrap();
        let model = if index < 4 || next() % 2 == 0 {
            "A"
        } else {
            "B"
        };
        let basis = if index >= 4 && next() % 3 == 0 {
            "metered_api"
        } else {
            "subscription_included"
        };
        let account = if index < 2 || (index >= 4 && next() % 2 == 0) {
            Some("shared")
        } else {
            None
        };
        let credential = Some("shared");
        let key = event(
            &mut conn,
            &format!("random-{index}"),
            at,
            model,
            account,
            credential,
            basis,
            quantity,
        );
        let spend = i64::try_from(next() % 10000).unwrap();
        let spend = recorded_price(&conn, key, spend);
        // Plan effective dates can assign an otherwise identical summary key
        // to a recorded plan member or to none within the same hour.
        let member = if basis == "metered_api" {
            PlanMember::non_pool("api")
        } else if (HOUR_MS / 2..36 * HOUR_MS + HOUR_MS / 2).contains(&at) {
            PlanMember {
                plan: "provider".into(),
                plan_account: Some("shared".into()),
                record: Some(record_id(
                    if account.is_some() {
                        PlanLedger::Account
                    } else {
                        PlanLedger::Credential
                    },
                    "shared",
                    if at < 18 * HOUR_MS + HOUR_MS / 2 {
                        HOUR_MS / 2
                    } else {
                        18 * HOUR_MS + HOUR_MS / 2
                    },
                )),
            }
        } else {
            PlanMember::non_pool("none")
        };
        let unit_rate = if model == "A" { 3_i128 } else { 7 };
        let numerator = unit_rate * (i128::from(quantity) + 2 * i128::from(quantity / 2));
        let ledger =
            load_scoped_ledger(&conn, &DIMENSIONS, Some(&format!("event_key={key}"))).unwrap();
        let summary_key = super::super::key(&ledger, &ledger.events[0]);
        let sums = oracle
            .entry((hour, summary_key.clone(), member.clone()))
            .or_default();
        sums.0 += numerator;
        sums.1 += i128::from(spend);
        calls.push((at, summary_key, member, numerator, i128::from(spend)));
    }
    for ledger in [PlanLedger::Account, PlanLedger::Credential] {
        plan(
            &conn,
            ledger,
            "provider",
            "shared",
            HOUR_MS / 2,
            Some(18 * HOUR_MS + HOUR_MS / 2),
        );
        plan(
            &conn,
            ledger,
            "provider",
            "shared",
            18 * HOUR_MS + HOUR_MS / 2,
            Some(36 * HOUR_MS + HOUR_MS / 2),
        );
    }
    rate(&conn, "A", 3);
    rate(&conn, "B", 7);
    summaries(&mut conn);
    drain(&mut conn);
    let mut split = false;
    for hour in 0..72 {
        let cells = stored_cells(&conn, hour);
        let mut totals: BTreeMap<Key, Recorded> = BTreeMap::new();
        for ((key, member), cell) in &cells {
            assert_eq!(
                (cell.numerator, cell.recorded.cell.total),
                oracle[&(hour, key.clone(), member.clone())]
            );
            assert_eq!(cell.unvalued, Unvalued::default());
            let total = totals.entry(key.clone()).or_default();
            total.cell.merge(&cell.recorded.cell).unwrap();
            for (key, value) in &cell.recorded.quantities {
                merge_quantity(&mut total.quantities, key, value).unwrap();
            }
            for (key, value) in &cell.recorded.money {
                merge_money(&mut total.money, key, value).unwrap();
            }
            split |= cells
                .keys()
                .filter(|(candidate, _)| candidate == key)
                .count()
                > 1;
        }
        let source = stored(
            &conn,
            Hour {
                kind: 0,
                number: hour,
            },
        )
        .unwrap();
        assert_eq!(totals.len(), source.cells.len());
        for (key, total) in totals {
            assert_eq!(total.cell, source.cells[&key]);
            assert_eq!(
                total.quantities,
                source
                    .quantities
                    .iter()
                    .filter(|((candidate, _), _)| candidate == &key)
                    .map(|((_, key), value)| (key.clone(), value.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                total.money,
                source
                    .money
                    .iter()
                    .filter(|((candidate, _, _), _)| candidate == &key)
                    .map(|((_, kind, currency), value)| ((*kind, currency.clone()), value.clone()))
                    .collect::<Vec<_>>()
            );
        }
    }
    assert!(
        split,
        "fixture must actually split a summary key across plan members"
    );
    let records: BTreeSet<_> = stored_cells(&conn, 18)
        .into_keys()
        .filter_map(|(_, member)| member.record)
        .collect();
    assert_eq!(
        records.len(),
        4,
        "both namespaces and same-hour sequential records remain distinct"
    );
    // Non-integral UTC offsets put local-day boundaries inside UTC hours. The
    // reducer selects those partial hours by call time, then groups independent
    // call arithmetic by local day, model/account and pool member.
    for zone in [
        "UTC",
        "America/New_York",
        "Europe/Berlin",
        "Asia/Kolkata",
        "Australia/Adelaide",
    ] {
        let tz: chrono_tz::Tz = zone.parse().unwrap();
        for dimension in [Grouping::Model, Grouping::Account, Grouping::ChargeBasis] {
            for day in 1..=3 {
                let start = tz
                    .with_ymd_and_hms(1970, 1, day, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis();
                let end = tz
                    .with_ymd_and_hms(1970, 1, day + 1, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis();
                let mut direct: BTreeMap<(Option<String>, PlanMember), (i128, i128)> =
                    BTreeMap::new();
                for (at, key, member, numerator, spend) in &calls {
                    if (start..end).contains(at) {
                        let sums = direct
                            .entry((key.0[dimension as usize].clone(), member.clone()))
                            .or_default();
                        sums.0 += numerator;
                        sums.1 += spend;
                    }
                }
                let mut reduced = Cells::new();
                reduce(
                    &conn,
                    &format!("occurred_at_ms>={start} AND occurred_at_ms<{end}"),
                    &mut reduced,
                )
                .unwrap();
                let mut actual: BTreeMap<(Option<String>, PlanMember), (i128, i128)> =
                    BTreeMap::new();
                for ((key, member), cell) in reduced {
                    let sums = actual
                        .entry((key.0[dimension as usize].clone(), member))
                        .or_default();
                    sums.0 += cell.numerator;
                    sums.1 += cell.recorded.cell.total;
                }
                assert_eq!(actual, direct, "{zone} {dimension:?} day {day}");
            }
        }
    }
}

#[test]
fn valuation_event_error_discards_partial_value_without_stopping_hour() {
    let mut conn = conn();
    event(&mut conn, "good", 1, "A", None, None, "metered_api", 10);
    event(&mut conn, "bad", 2, "B", None, None, "metered_api", 10);
    rate(&conn, "A", 3);
    rate(&conn, "B", 7);
    // A malformed reference graph is isolated to its event rather than aborting
    // the entire hour or blessing that event's partially computed value.
    conn.execute(
        "UPDATE rate_model SET reference_provider='provider',reference_model='B' WHERE model='B'",
        [],
    )
    .unwrap();
    summaries(&mut conn);
    drain(&mut conn);
    assert_eq!(state(&conn, 0), "built");
    let cells = stored_cells(&conn, 0);
    assert_eq!(cells.values().map(|c| c.numerator).sum::<i128>(), 60);
    assert_eq!(
        cells.values().map(|c| c.unvalued.measurements).sum::<u64>(),
        2
    );
    assert_eq!(
        cells
            .values()
            .map(|c| c
                .unvalued
                .by_reason
                .get(&crate::valuation::UnvaluedReason::ValuationError)
                .copied()
                .unwrap_or(0))
            .sum::<u64>(),
        2
    );
}

#[test]
fn valuation_waits_for_summary_then_uses_second_rate_change_and_fences_rate_publication() {
    let mut conn = conn();
    event(&mut conn, "A", 1, "A", None, None, "metered_api", 10);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn);
    event(&mut conn, "new-B", 2, "B", None, None, "metered_api", 10);
    rate(&conn, "B", 5);
    prepare(&mut conn);
    let mut maintenance = ValuationMaintenance::default();
    assert!(!maintenance.read_chunk(&conn).unwrap());
    assert!(
        !maintenance.has_pending(),
        "valuation cannot get ahead of a dirty summary"
    );
    rate(&conn, "B", 11);
    summaries(&mut conn);
    drain(&mut conn);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|cell| cell.numerator)
            .sum::<i128>(),
        280
    );
    // A rate-cache update between reduction and commit must prevent publishing
    // the stale build, even before maintenance synchronizes the rate dirty queue.
    event(&mut conn, "another-A", 3, "A", None, None, "metered_api", 2);
    summaries(&mut conn);
    prepare(&mut conn);
    assert!(maintenance.read_chunk(&conn).unwrap());
    rate(&conn, "A", 13);
    let tx = conn.transaction().unwrap();
    assert!(!maintenance.publish(&tx).unwrap());
    tx.commit().unwrap();
    drain(&mut conn);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|cell| cell.numerator)
            .sum::<i128>(),
        532
    );
}

#[path = "fee_tests.rs"]
mod fee_tests;

#[path = "surface_tests.rs"]
mod surface_tests;

#[path = "perf_tests.rs"]
mod perf_tests;
