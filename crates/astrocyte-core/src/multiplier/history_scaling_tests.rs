fn query_plan(conn: &Connection, sql: &str) -> String {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let rows = stmt.raw_query().mapped(|row| row.get::<_, String>(3));
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n")
}

fn observation_history(days: u64, scoped: bool) -> Connection {
    let mut conn = conn();
    let mut observation = if scoped {
        let mut row = eligible_plan_observation(&conn);
        row.pool_scope = Some(PoolScope::All);
        row
    } else {
        sample_observation(&conn)
    };
    let tx = conn.transaction().unwrap();
    let first = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    for day in 0..days {
        // A day is named by the UTC date its interval ends on.
        let start = i64::try_from(day).unwrap() * DAY;
        observation.interval_start_ms = start;
        observation.interval_end_ms = start + DAY;
        observation.token_interval_start_ms = Some(start);
        observation.token_interval_end_ms = Some(start + DAY);
        observation.observation_day = (first + chrono::Days::new(day + 1)).to_string();
        if scoped {
            record_one(&tx, &observation);
        } else {
            insert_observation_in_tx(&tx, &observation).unwrap();
        }
    }
    tx.commit().unwrap();
    conn
}

#[test]
fn day_head_lookups_do_not_scan_other_days() {
    let measure = |days| {
        let conn = observation_history(days, false);
        let plan = conn
            .query_row(
                "SELECT plan FROM multiplier_observation LIMIT 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap();
        vm_steps(&conn, || {
            (
                observation_plan_head(&conn, "codex", "2019-06-01", plan.as_deref()).unwrap(),
                observation_plan_head(&conn, "codex", "1970-01-03", plan.as_deref()).unwrap(),
                observation_head(&conn, "codex", "2019-06-01")
                    .map(|id| id.is_some())
                    .unwrap(),
                observation_head(&conn, "codex", "1970-01-03").unwrap(),
            )
        })
    };
    let (small, small_steps) = measure(100);
    let (large, large_steps) = measure(1_000);
    assert_eq!(small, large);
    assert_eq!((small.0, small.2), (None, false));
    assert!(small.1.is_some() && small.1 == small.3);
    assert_eq!(
        large_steps, small_steps,
        "{small_steps} -> {large_steps} VM steps"
    );
    let conn = observation_history(1, false);
    for sql in [
        "SELECT o.observation_id FROM multiplier_observation o
          WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.plan IS ?3",
        "SELECT 1 FROM multiplier_observation WHERE quota_provider = ?1 AND observation_day = ?2",
    ] {
        let plan = query_plan(&conn, sql);
        assert!(
            plan.contains("USING COVERING INDEX multiplier_observation_provider_day_plan (quota_provider=? AND observation_day=?"),
            "{plan}"
        );
    }
}

/// Finding the newest provider day and its eligible pool must not scan the
/// provider's observation history once for every historical row.
#[test]
fn report_latest_head_is_not_quadratic_in_observations() {
    let measure = |days| {
        let conn = observation_history(days, true);
        let (report, steps) = vm_steps(&conn, || {
            multiplier_report(
                &conn,
                &MultiplierReportRequest {
                    from_ms: 0,
                    to_ms: 1,
                },
            )
            .unwrap()
        });
        (report["latest"].as_array().unwrap().len(), steps)
    };
    let (small, small_steps) = measure(100);
    let (large, large_steps) = measure(1_000);
    assert_eq!((small, large), (1, 1));
    // Linear work over ten times the heads is about ten times the steps; the
    // quadratic form was about a hundred times.
    assert!(
        large_steps < 20 * small_steps,
        "{small_steps} -> {large_steps} VM steps"
    );
}
