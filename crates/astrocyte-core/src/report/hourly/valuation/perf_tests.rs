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
