// Multiplier reads must cost work in proportion to their answer, not to all
// recorded quota or observation history. Each fixture below keeps the answer
// fixed and grows unrelated history tenfold; the SQLite VM instruction count
// must not follow it.

/// One quota reading in either sample table. `credential` picks
/// `credential_window_sample` keyed by `credential_ref`.
#[allow(clippy::too_many_arguments)]
fn quota_reading(
    conn: &Connection,
    credential: bool,
    provider: &str,
    identity: &str,
    pool: &str,
    window_minutes: Option<i64>,
    at_ns: i64,
    source: Option<&str>,
) {
    let (table, field) = if credential {
        ("credential_window_sample", "credential_ref")
    } else {
        ("window_sample", "account_ref")
    };
    conn.execute(
        &format!(
            "INSERT INTO {table}(provider,{field},pool_key,fetched_at_instant,used_percent,
                 resets_at,window_minutes,source)
             VALUES(?1,?2,?3,?4,1,'2026-09-30T00:00:00Z',?5,?6)"
        ),
        params![provider, identity, pool, at_ns, window_minutes, source],
    )
    .unwrap();
}

/// The single-column text answers of `sql` with `bounds` bound in order.
fn texts(conn: &Connection, sql: &str, bounds: &[&dyn rusqlite::ToSql]) -> Vec<String> {
    conn.prepare(sql)
        .unwrap()
        .query_map(bounds, |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn query_plan(conn: &Connection, sql: &str) -> String {
    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n")
}

/// The fixture's account readings, one a minute. `history` older readings sit
/// before the interval's seven-day lookback and `history` newer ones after
/// its tail, so they belong to the identity but to no requested range.
fn scaled_quota_history(history: i64) -> Connection {
    let conn = conn();
    let config = config(vec![rule("pi", "openai", None)]);
    let minute_ns = 60_000 * NANOS_PER_MS;
    for minute in 0..history {
        quota_reading(
            &conn,
            false,
            "codex",
            "acct",
            "primary",
            Some(10080),
            minute * minute_ns,
            Some("vault"),
        );
        let later = 40 * DAY * NANOS_PER_MS + minute * minute_ns;
        quota_reading(
            &conn,
            false,
            "codex",
            "acct",
            "primary",
            Some(10080),
            later,
            Some("vault"),
        );
    }
    // The answer: readings every five minutes across the requested day, for
    // the configured account and for one account the config does not name.
    for step in 0..=288 {
        let at = 20 * DAY * NANOS_PER_MS + step * 5 * minute_ns;
        quota_reading(
            &conn,
            false,
            "codex",
            "acct",
            "primary",
            Some(10080),
            at,
            Some("vault"),
        );
        quota_reading(
            &conn,
            false,
            "codex",
            "stranger",
            "primary",
            Some(10080),
            at,
            Some("vault"),
        );
    }
    // Install the fixture's plan history once, outside the measured reads.
    plan_observation(&conn, &config, "codex", 20 * DAY, 21 * DAY).unwrap();
    conn
}

#[test]
fn membership_and_seen_identities_follow_identities_not_readings() {
    let config = config(vec![rule("pi", "openai", None)]);
    let declaration = config.quota_providers["codex"].clone();
    let measure = |history| {
        let conn = scaled_quota_history(history);
        let (resolved, membership) = vm_steps(&conn, || {
            resolve_membership(&conn, &declaration, "codex", 21 * DAY).unwrap()
        });
        let (plan, planning) = vm_steps(&conn, || {
            super::plan_observation(&conn, &config, "codex", 20 * DAY, 21 * DAY).unwrap()
        });
        (resolved, plan, membership, planning)
    };
    let (small_resolved, small_plan, small_membership, small_planning) = measure(1_000);
    let (large_resolved, large_plan, large_membership, large_planning) = measure(10_000);
    assert_eq!(small_resolved, large_resolved, "same members at both sizes");
    assert_eq!(
        small_plan, large_plan,
        "same counters and unconfigured accounts"
    );
    assert_eq!(
        large_plan
            .unconfigured_accounts
            .iter()
            .map(|account| account.account_ref.as_str())
            .collect::<Vec<_>>(),
        ["stranger"]
    );
    // Each size adds 18,000 readings of the configured account outside the
    // requested day. Scanning them costs at least that many instructions.
    assert_eq!(
        large_membership, small_membership,
        "membership must not read the account's other readings: \
         {small_membership} -> {large_membership} VM steps"
    );
    assert_eq!(
        large_planning, small_planning,
        "planning a day must not read readings outside it: \
         {small_planning} -> {large_planning} VM steps"
    );
}

#[test]
fn counter_delta_stops_at_the_first_reading_after_the_interval() {
    let window = window();
    let measure = |later: i64| {
        let conn = conn();
        let minute_ns = 60_000 * NANOS_PER_MS;
        for step in 0..=288 {
            let at = 20 * DAY * NANOS_PER_MS + step * 5 * minute_ns;
            quota_reading(
                &conn,
                false,
                "codex",
                "acct",
                "primary",
                Some(10080),
                at,
                None,
            );
        }
        // The interval's tail reading is the first one past `end + 15 min`.
        for minute in 0..later {
            let at = 22 * DAY * NANOS_PER_MS + minute * minute_ns;
            quota_reading(
                &conn,
                false,
                "codex",
                "acct",
                "primary",
                Some(10080),
                at,
                None,
            );
        }
        vm_steps(&conn, || {
            clean::counter_delta(&conn, "codex", ("acct", false), &window, 20 * DAY, 21 * DAY)
                .unwrap()
        })
    };
    let (small, small_steps) = measure(1_000);
    let (large, large_steps) = measure(10_000);
    assert_eq!(small, large, "the same readings make the same counter");
    assert_eq!(
        large_steps, small_steps,
        "later readings past the tail must not be read: {small_steps} -> {large_steps} VM steps"
    );
}

/// The window is not part of the sample tables' primary keys. Readings of the
/// same pool under another window that come after an identity's first matching
/// reading cost nothing; the check stops at that first match.
#[test]
fn other_window_readings_after_the_first_match_are_not_read() {
    let config = config(vec![rule("pi", "openai", None)]);
    let declaration = config.quota_providers["codex"].clone();
    let measure = |other_window: i64| {
        let conn = conn();
        quota_reading(
            &conn,
            false,
            "codex",
            "acct",
            "primary",
            Some(10080),
            0,
            None,
        );
        for minute in 1..=other_window {
            quota_reading(
                &conn,
                false,
                "codex",
                "acct",
                "primary",
                Some(300),
                minute * 60_000 * NANOS_PER_MS,
                None,
            );
        }
        plan_observation(&conn, &config, "codex", 0, DAY).unwrap();
        vm_steps(&conn, || {
            resolve_membership(&conn, &declaration, "codex", 30 * DAY).unwrap()
        })
    };
    let (small, small_steps) = measure(1_000);
    let (large, large_steps) = measure(10_000);
    assert_eq!(small, large);
    assert!(large.accounts.contains_key("acct"));
    assert_eq!(
        large_steps, small_steps,
        "{small_steps} -> {large_steps} VM steps"
    );
    // Other-window readings that come BEFORE the first matching one are still
    // stepped over: the primary key cannot skip them. That cost follows those
    // readings, which only exist if a pool's window changed; printed so a
    // measurement run can report it.
    let before = |other_window: i64| {
        let conn = conn();
        for minute in 0..other_window {
            quota_reading(
                &conn,
                false,
                "codex",
                "acct",
                "primary",
                Some(300),
                minute * 60_000 * NANOS_PER_MS,
                None,
            );
        }
        quota_reading(
            &conn,
            false,
            "codex",
            "acct",
            "primary",
            Some(10080),
            other_window * 60_000 * NANOS_PER_MS,
            None,
        );
        plan_observation(&conn, &config, "codex", 0, DAY).unwrap();
        vm_steps(&conn, || {
            resolve_membership(&conn, &declaration, "codex", 30 * DAY).unwrap()
        })
    };
    let (small, small_steps) = before(1_000);
    let (large, large_steps) = before(10_000);
    assert_eq!(small, large);
    eprintln!(
        "other-window readings before the first match: {small_steps} -> {large_steps} VM steps"
    );
}

/// Same answers as the plain `DISTINCT` and `OR` forms these queries replaced,
/// over identities, pools, windows (including none), sources and bounds that
/// exercise every predicate.
#[test]
fn identity_and_counter_queries_match_their_unbounded_forms() {
    let conn = conn();
    let cutoff = clean::ANTIGRAVITY_WRONG_VAULT_CUTOFF_NS;
    let times = [0, 5, 9, 10, 11, 20, 35, cutoff - 1, cutoff, cutoff + 1];
    for credential in [false, true] {
        for (index, at) in times.iter().enumerate() {
            for provider in ["codex", "antigravity"] {
                let window = [Some(10080), Some(300), None][index % 3];
                let pool = ["primary", "secondary"][index % 2];
                let source = [Some("vault"), Some("oauth"), None][index % 3];
                for identity in ["a", "b", "c"][..=index % 3].iter() {
                    quota_reading(
                        &conn, credential, provider, identity, pool, window, *at, source,
                    );
                }
                quota_reading(
                    &conn,
                    credential,
                    provider,
                    "z",
                    "primary",
                    Some(10080),
                    at + 1,
                    Some("vault"),
                );
            }
        }
    }
    for (table, field) in [
        ("window_sample", "account_ref"),
        ("credential_window_sample", "credential_ref"),
    ] {
        let old_membership = format!(
            "SELECT DISTINCT {field} FROM {table} WHERE provider=?1 AND pool_key=?2
             AND window_minutes=?3 AND fetched_at_instant<?4 ORDER BY 1"
        );
        let old_seen = format!(
            "SELECT DISTINCT {field} FROM {table} WHERE provider=?1 AND pool_key=?2
             AND window_minutes=?3 AND fetched_at_instant>=?4 AND fetched_at_instant<?5
             AND NOT(provider='antigravity' AND source IS 'vault' AND fetched_at_instant<?6)
             ORDER BY 1"
        );
        let membership = sample_identities_sql(table, field, "s.fetched_at_instant<?4");
        let seen = sample_identities_sql(table, field, SEEN_READING);
        for provider in ["codex", "antigravity", "absent"] {
            for pool in ["primary", "secondary"] {
                for window in [10080_i64, 300] {
                    for start in times {
                        let end_bounds = times.iter().map(|at| at + 1);
                        for end in end_bounds.chain([i64::MAX]) {
                            let membership_bounds: [&dyn rusqlite::ToSql; 4] =
                                [&provider, &pool, &window, &end];
                            assert_eq!(
                                texts(&conn, &membership, &membership_bounds),
                                texts(&conn, &old_membership, &membership_bounds),
                                "{table} {provider} {pool} {window} <{end}"
                            );
                            let seen_bounds: [&dyn rusqlite::ToSql; 6] =
                                [&provider, &pool, &window, &start, &end, &cutoff];
                            assert_eq!(
                                texts(&conn, &seen, &seen_bounds),
                                texts(&conn, &old_seen, &seen_bounds),
                                "{table} {provider} {pool} {window} [{start},{end})"
                            );
                        }
                    }
                }
            }
        }
    }
    for credential in [false, true] {
        let old_counter = "SELECT fetched_at_instant FROM window_sample
             WHERE provider=?1 AND account_ref=?2 AND pool_key=?3
             AND window_minutes=?4 AND fetched_at_instant>=?5
             AND (fetched_at_instant<=?6 OR fetched_at_instant=(
                 SELECT MIN(tail.fetched_at_instant) FROM window_sample tail
                 WHERE tail.provider=?1 AND tail.account_ref=?2 AND tail.pool_key=?3
                   AND tail.window_minutes=?4 AND tail.fetched_at_instant>?6))
             ORDER BY fetched_at_instant, rowid";
        let old_counter = if credential {
            old_counter
                .replace("window_sample", "credential_window_sample")
                .replace("account_ref", "credential_ref")
        } else {
            old_counter.to_string()
        };
        let counter = clean::counter_samples_sql(credential).replacen(
            "fetched_at_instant, COALESCE(raw_used_percent, used_percent), resets_at, source",
            "fetched_at_instant",
            1,
        );
        for identity in ["a", "b", "c", "z"] {
            for pool in ["primary", "secondary"] {
                for window in [10080_i64, 300] {
                    for (index, lower) in times.iter().enumerate() {
                        for upper in &times[index..] {
                            let bounds: [&dyn rusqlite::ToSql; 6] =
                                [&"codex", &identity, &pool, &window, lower, upper];
                            let found: Vec<i64> = conn
                                .prepare(&counter)
                                .unwrap()
                                .query_map(bounds.as_slice(), |row| row.get(0))
                                .unwrap()
                                .collect::<rusqlite::Result<_>>()
                                .unwrap();
                            let expected: Vec<i64> = conn
                                .prepare(&old_counter)
                                .unwrap()
                                .query_map(bounds.as_slice(), |row| row.get(0))
                                .unwrap()
                                .collect::<rusqlite::Result<_>>()
                                .unwrap();
                            assert_eq!(
                                found, expected,
                                "{identity} {pool} {window} [{lower},{upper}]"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// A planner change must not fall back to scanning a provider's readings.
#[test]
fn quota_identity_and_counter_queries_seek_the_primary_key() {
    let conn = conn();
    for (table, field) in [
        ("window_sample", "account_ref"),
        ("credential_window_sample", "credential_ref"),
    ] {
        let key = format!("sqlite_autoindex_{table}_1");
        for (reading, range) in [
            ("s.fetched_at_instant<?4", "fetched_at_instant<?"),
            (
                SEEN_READING,
                "fetched_at_instant>? AND fetched_at_instant<?",
            ),
        ] {
            let plan = query_plan(&conn, &sample_identities_sql(table, field, reading));
            for expected in [
                format!("SEARCH {table} USING COVERING INDEX {key} (provider=?)"),
                format!("SEARCH n USING COVERING INDEX {key} (provider=? AND {field}>?)"),
                format!("SEARCH s USING INDEX {key} (provider=? AND {field}=? AND pool_key=? AND {range})"),
            ] {
                assert!(plan.contains(&expected), "{expected} missing from:\n{plan}");
            }
            assert!(!plan.contains(&format!("SCAN {table}")), "{plan}");
        }
        let plan = query_plan(&conn, &clean::counter_samples_sql(table != "window_sample"));
        assert!(
            plan.contains(&format!(
                "SEARCH {table} USING INDEX {key} (provider=? AND {field}=? AND pool_key=? \
                 AND fetched_at_instant>? AND fetched_at_instant<?)"
            )),
            "{plan}"
        );
    }
}

/// An observation per day for `days` days, all for the same provider and plan.
fn observation_history(days: u64) -> Connection {
    let mut conn = conn();
    let mut observation = sample_observation(&conn);
    let tx = conn.transaction().unwrap();
    let first = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    for day in 0..days {
        observation.observation_day = (first + chrono::Days::new(day)).to_string();
        insert_observation_in_tx(&tx, &observation).unwrap();
    }
    tx.commit().unwrap();
    conn
}

#[test]
fn day_head_lookups_do_not_scan_other_days() {
    let measure = |days| {
        let conn = observation_history(days);
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
                observation_plan_head(&conn, "codex", "2020-01-02", plan.as_deref()).unwrap(),
                observation_exists(&conn, "codex", "2019-06-01").unwrap(),
                observation_head(&conn, "codex", "2020-01-02").unwrap(),
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
    let conn = observation_history(1);
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

/// The report's latest heads correlated every head with a search of the
/// provider's other heads. With the provider/day index each search is a
/// backwards seek, so the report grows with the heads it returns in its
/// series, not with their square.
#[test]
fn report_latest_head_is_not_quadratic_in_observations() {
    let measure = |days| {
        let conn = observation_history(days);
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
