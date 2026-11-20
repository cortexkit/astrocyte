use super::*;
use crate::store::VmCounter;

fn calls(
    conn: &Connection,
    prefix: &str,
    per_hour: i64,
    hours: i64,
    start: i64,
    distinct: bool,
    covered: bool,
) {
    let total = per_hour * hours;
    let basis = if covered {
        "subscription_included"
    } else {
        "unknown"
    };
    conn.execute_batch(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<{total})
         INSERT INTO usage_event(event_id,producer,producer_ref,provider,model,account_ref,auth_selection,occurred_at_ms,recorded_at_ms,charge_basis,currency,pricing_generation)
         SELECT '{prefix}-'||i,'opencode','{prefix}-'||i,'provider','A',CASE WHEN {covered} THEN 'account' END,'selected-'||CASE WHEN {distinct} THEN (i-1)%{per_hour} ELSE 0 END,{start}+(i-1)/{per_hour}*{HOUR_MS}+(i-1)%{per_hour},1,'{basis}',NULL,0 FROM n;
         INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
         SELECT event_key,'input','token','billed','fixture',100,0 FROM usage_event WHERE producer_ref LIKE '{prefix}-%';
         INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
         SELECT event_key,'output','token','billed','fixture',50,0 FROM usage_event WHERE producer_ref LIKE '{prefix}-%';"
    )).unwrap();
    conn.execute(
        &format!("UPDATE usage_event SET currency='USD' WHERE producer_ref LIKE '{prefix}-%'"),
        [],
    )
    .unwrap();
    if covered {
        conn.execute(
            &format!(
                "UPDATE usage_event SET pricing_generation=1 WHERE producer_ref LIKE '{prefix}-%'"
            ),
            [],
        )
        .unwrap();
        conn.execute_batch(&format!("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms) SELECT m.event_key,m.role,m.unit,1,'covered','subscription_included',1,'USD',0,'pricing_pass',1 FROM measurement m JOIN usage_event e USING(event_key) WHERE e.producer_ref LIKE '{prefix}-%';")).unwrap();
    }
}

fn fixture(per_hour: i64, unrelated: i64) -> Connection {
    let mut conn = conn();
    rate(&conn, "A", 1_000_000_000);
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    calls(&conn, "target", per_hour, 1, 0, false, true);
    if unrelated > 0 {
        calls(
            &conn,
            "history",
            unrelated / 10,
            10,
            5 * fees::DAY_MS,
            false,
            true,
        );
    }
    summaries(&mut conn);
    prepare(&mut conn, &config());
    conn
}

fn build(conn: &Connection, expected: i64) -> (u64, usize) {
    let mut maintenance = ValuationMaintenance::default();
    let (_, statements, steps) = captured(conn, || {
        assert!(maintenance.read_chunk(conn, &config(), &[]).unwrap());
        assert_eq!(maintenance.pending.as_ref().unwrap().hour.number, 0);
        let tx = conn.unchecked_transaction().unwrap();
        assert!(maintenance.publish(&tx).unwrap());
        tx.commit().unwrap();
    });
    let cells = stored_cells(conn, 0);
    assert_eq!(
        cells.values().map(|c| c.recorded.cell.events).sum::<u64>(),
        u64::try_from(expected).unwrap()
    );
    assert_eq!(
        cells.values().map(|c| c.numerator).sum::<i128>(),
        i128::from(expected) * 200_000_000_000
    );
    (steps, statements.len())
}

#[test]
fn valuation_perf_hour_build_is_linear_at_100_and_1000_calls() {
    let mut measured = Vec::new();
    for count in [100, 1_000] {
        let conn = fixture(count, 0);
        let (steps, statements) = build(&conn, count);
        assert!(
            steps < 10_000 + u64::try_from(count).unwrap() * 400,
            "{count} calls consumed {steps} VM steps"
        );
        measured.push((steps, statements));
    }
    assert!(
        measured[1].0 < measured[0].0 * 11,
        "nonlinear hour build: {measured:?}"
    );
    assert_eq!(
        measured[0].1, measured[1].1,
        "builder added per-call SQL: {measured:?}"
    );
    eprintln!("valuation build (VM steps, statements) for 100 / 1000 calls: {measured:?}");
}

#[test]
fn ledger_regression_bounded_report_hour_seek_is_independent_of_history() {
    let measure = |history: i64| {
        let mut conn = conn();
        rate(&conn, "A", 1_000_000_000);
        let hour = 10_008;
        let from = hour * HOUR_MS;
        calls(&conn, "target", 1, 24, from, false, true);
        summaries(&mut conn);
        drain(&mut conn, &config());
        conn.execute_batch(&format!(
            "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i+1<{history})
             INSERT INTO summary_hour(kind,hour_number,revision,built_revision,format_version)
             SELECT 0,i,1,1,{FORMAT} FROM n;
             ANALYZE;"
        ))
        .unwrap();
        let request = ReportRequest {
            grain: "day".into(),
            timezone: "UTC".into(),
            from_ms: Some(from),
            to_ms: Some(from + fees::DAY_MS),
            ..ReportRequest::default()
        };
        let counter = VmCounter::start(&conn);
        let report = spend_report(&conn, &request, from + 2 * fees::DAY_MS).unwrap();
        assert_eq!(report.rows[0].events_total, 24);
        assert_eq!(report.rows[0].valuation.list_value.nanos, 4_800_000);
        counter.steps()
    };
    let counts = [measure(1_000), measure(10_000)];
    eprintln!("bounded spend report VM steps at 1,000 and 10,000 history hours: {counts:?}");
    assert!(
        counts[1] <= counts[0] + 100,
        "VM steps grew with history: {counts:?}"
    );
}

#[test]
fn ledger_regression_small_valuation_changes_do_not_retag_history() {
    let measure = |history| {
        let mut conn = conn();
        prepare(&mut conn, &config());
        conn.execute_batch(&format!(
            "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i+1<{history})
             INSERT INTO summary_hour(kind,hour_number,revision,built_revision,format_version)
             SELECT 0,i,1,1,{FORMAT} FROM n;
             UPDATE valuation_hour SET state='built',rate_generation=0,plan_generation=0,config_generation=1;"
        )).unwrap();
        let plan_counter = VmCounter::start(&conn);
        plan(
            &conn,
            PlanLedger::Account,
            "provider",
            "account",
            (history - 1) * HOUR_MS,
            Some(history * HOUR_MS),
        );
        let plan_steps = plan_counter.steps();
        conn.execute_batch("INSERT INTO rate_dirty_hour VALUES(0); UPDATE rate_cache_state SET generation=1 WHERE id=1;").unwrap();
        let rate_counter = VmCounter::start(&conn);
        let tx = conn.unchecked_transaction().unwrap();
        ValuationMaintenance::prepare(&tx, &config(), &[]).unwrap();
        tx.commit().unwrap();
        let rate_steps = rate_counter.steps();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM valuation_hour WHERE state='dirty'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        (plan_steps, rate_steps)
    };
    let (small, large) = (measure(1_000), measure(10_000));
    eprintln!("(plan, rate) VM steps at 1,000 and 10,000 history hours: {small:?}, {large:?}");
    assert!(
        large.0 <= small.0 + 100,
        "plan change rewrote unrelated history"
    );
    assert!(
        large.1 <= small.1 + 100,
        "rate change rewrote unrelated history"
    );
}

#[test]
fn valuation_perf_same_hour_is_independent_of_tenfold_unrelated_history() {
    let mut measured = Vec::new();
    for unrelated in [1_000, 10_000] {
        let conn = fixture(1_000, unrelated);
        measured.push(build(&conn, 1_000));
    }
    assert!(
        measured[1].0 <= measured[0].0 + 1_000,
        "hour build scanned unrelated history: {measured:?}"
    );
    eprintln!("same-hour VM steps with 1000 / 10000 unrelated calls: {measured:?}");
}

unsafe extern "C" fn trace(
    _: u32,
    context: *mut std::ffi::c_void,
    statement: *mut std::ffi::c_void,
    _: *mut std::ffi::c_void,
) -> std::ffi::c_int {
    unsafe {
        let sql = rusqlite::ffi::sqlite3_sql(statement.cast());
        if !sql.is_null() {
            (*context.cast::<Vec<String>>())
                .push(std::ffi::CStr::from_ptr(sql).to_string_lossy().into_owned());
        }
    }
    0
}

fn captured<T>(conn: &Connection, read: impl FnOnce() -> T) -> (T, Vec<String>, u64) {
    let mut statements = Box::new(Vec::<String>::new());
    let counter = VmCounter::start(conn);
    unsafe {
        rusqlite::ffi::sqlite3_trace_v2(
            conn.handle(),
            u32::try_from(rusqlite::ffi::SQLITE_TRACE_STMT).unwrap(),
            Some(trace),
            std::ptr::from_mut(statements.as_mut()).cast(),
        );
    }
    let result = read();
    unsafe {
        rusqlite::ffi::sqlite3_trace_v2(conn.handle(), 0, None, std::ptr::null_mut());
    }
    (result, *statements, counter.steps())
}

#[test]
fn valuation_perf_stale_seven_day_statements_do_not_grow_per_cell() {
    let mut measured = Vec::new();
    for cells in [1, 10] {
        let mut conn = conn();
        plan(
            &conn,
            PlanLedger::Credential,
            "provider",
            "resolved",
            0,
            None,
        );
        calls(&conn, "report", cells, 7 * 24, 0, true, false);
        summaries(&mut conn);
        prepare(&mut conn, &config());
        let request = ReportRequest {
            grouping: vec!["plan".into()],
            grain: "day".into(),
            timezone: "UTC".into(),
            from_ms: Some(0),
            to_ms: Some(7 * fees::DAY_MS),
            ..ReportRequest::default()
        };
        let (report, statements, steps) = captured(&conn, || {
            spend_report(&conn, &request, 8 * fees::DAY_MS).unwrap()
        });
        assert_eq!(report.rows.len(), 7);
        assert_eq!(
            report.totals[0].events_total,
            u64::try_from(cells * 7 * 24).unwrap()
        );
        assert_eq!(
            report.totals[0]
                .valuation
                .row
                .list_value
                .not_yet_valued_hours,
            7 * 24
        );
        assert!(report
            .rows
            .iter()
            .all(|r| r.grouping["plan"].as_deref() == Some("pending")));
        measured.push((statements.len(), steps));
    }
    assert_eq!(
        measured[0].0, measured[1].0,
        "stale report added per-cell statements: {measured:?}"
    );
    assert!(
        measured[1].1 < measured[0].1 * 11,
        "stale report work is not cell-linear: {measured:?}"
    );
    eprintln!("stale 7-day (statements, VM steps) for 1 / 10 cells per hour: {measured:?}");
}

#[test]
fn valuation_perf_actual_chunk_queries_pin_bounded_event_searches() {
    let conn = fixture(100, 0);
    let (_, statements, _) = captured(&conn, || {
        let mut maintenance = ValuationMaintenance::default();
        assert!(maintenance.read_chunk(&conn, &config(), &[]).unwrap());
    });
    let event_reads: Vec<_> = statements
        .iter()
        .filter(|sql| sql.contains("usage_event"))
        .collect();
    assert!(event_reads.len() >= 6);
    for sql in event_reads {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let plan = stmt
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(!plan.contains("SCAN usage_event"), "{sql}\n{plan}");
        assert!(
            plan.contains("SEARCH usage_event") || plan.contains("SEARCH e "),
            "{sql}\n{plan}"
        );
        assert!(
            plan.contains("usage_event_summary_time") || plan.contains("INTEGER PRIMARY KEY"),
            "{sql}\n{plan}"
        );
    }
    assert!(
        !statements
            .iter()
            .any(|sql| sql.contains("WITH selected AS MATERIALIZED")),
        "valuation invoked the runs reducer"
    );
}

#[test]
fn valuation_perf_built_seven_day_report_stays_within_vm_budget() {
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
    calls(&conn, "built-report", 40, 7 * 24, 0, true, false);
    summaries(&mut conn);
    drain(&mut conn, &config());
    // Fixed budget for 168 built hours, 40 cells per hour and two quantities
    // per cell. This pins total reader work without embedding an old reader.
    for ranged in [true, false] {
        for grouping in [
            vec![],
            vec!["producer".into()],
            vec!["plan".into(), "plan_account".into()],
        ] {
            let request = ReportRequest {
                grouping,
                grain: "day".into(),
                timezone: "UTC".into(),
                from_ms: ranged.then_some(0),
                to_ms: ranged.then_some(7 * fees::DAY_MS),
                ..ReportRequest::default()
            };
            let (report, statements, steps) = captured(&conn, || {
                spend_report(&conn, &request, 8 * fees::DAY_MS).unwrap()
            });
            assert_eq!(report.rows.len(), 7);
            assert_eq!(report.totals[0].events_total, 6_720);
            assert_eq!(
                report.totals[0].valuation.row.list_value.nanos,
                1_344_000_000
            );
            assert_eq!(
                report.totals[0]
                    .valuation
                    .row
                    .list_value
                    .not_yet_valued_hours,
                0
            );
            eprintln!(
                "built {:?} {:?}: {steps} VM steps, {} statements",
                request.from_ms,
                request.grouping,
                statements.len()
            );
            let lookups: Vec<_> = statements
                .iter()
                .filter(|sql| {
                    sql.starts_with("SELECT value_id,value FROM summary_value WHERE value_id IN (")
                })
                .collect();
            assert_eq!(
                lookups.len(),
                2,
                "built reader must resolve only cell and quantity dictionary ids"
            );
            for sql in lookups {
                let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
                let plan = stmt
                    .query_map([], |row| row.get::<_, String>(3))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
                    .join("\n");
                assert!(
                    plan.contains("SEARCH summary_value USING INTEGER PRIMARY KEY"),
                    "{sql}\n{plan}"
                );
                assert!(!plan.contains("SCAN"), "{sql}\n{plan}");
            }
            assert!(
                steps < 450_000,
                "built seven-day report exceeded VM budget: {steps}"
            );
        }
    }
}
