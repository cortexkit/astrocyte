fn v68_observation_insert(conn: &Connection, changes: &[(&str, &str)]) -> String {
    clone_insert(conn, "multiplier_observation", changes)
        .replace("LIMIT 1", "WHERE observation_id=1 LIMIT 1")
}

fn v66_observation_history(conn: &Connection) {
    assert_eq!(
        insert_multiplier_row(conn, "provider", "2026-01-02", Some(None)).unwrap(),
        1
    );
    for changes in [
        &[
            ("observation_id", "2"),
            ("supersedes", "1"),
            ("window_pool_key", "'secondary'"),
        ][..],
        &[("observation_id", "3"), ("plan", "'plan-a'")],
        &[
            ("observation_id", "4"),
            ("supersedes", "3"),
            ("plan", "'plan-a'"),
            ("window_pool_key", "'secondary'"),
        ],
    ] {
        assert_eq!(
            conn.execute(&v68_observation_insert(conn, changes), [])
                .unwrap(),
            1
        );
    }
}

#[test]
fn schema_v68_preserves_populated_rows_and_splits_observation_originals() {
    let (_directory, store) = migration_store();
    let (previous, _) = migration_parts(68);
    store.migrate("astrocyte", previous).unwrap();
    let before = store
        .with_conn(|conn| {
            v66_observation_history(conn);
            conn.execute_batch(
                "INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent)
                     VALUES ('provider','account','primary',1,12);
                 INSERT INTO credential_window_sample (provider,credential_ref,pool_key,fetched_at_instant,used_percent)
                     VALUES ('provider','credential','primary',1,15);
                 INSERT INTO usage_event (event_key,event_id,producer,producer_ref,charge_basis,recorded_at_ms)
                     VALUES (1,'call','fixture','call','metered_api',1);
                 INSERT INTO measurement VALUES (1,'input','token','count','reported',1,0);
                 UPDATE usage_event SET currency='USD',pricing_generation=1 WHERE event_key=1;
                 INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                     basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,
                     resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                 VALUES (1,1,'input','token',1,'priced','metered_api',0,'USD',3000,3000,9,'unknown',
                     'test.catalog',1,'{}','pricing_pass',1);",
            )?;
            Ok(all_rows(conn))
        })
        .unwrap();

    let result = migrate_store(&store).unwrap();
    assert_eq!((result.applied, result.recorded), (1, 68));
    store
        .with_conn(|conn| {
            assert!(table_rows(conn, "multiplier_provider_day").is_empty());
            for (table, mut rows) in before {
                if table == "cortexkit_schema_version" {
                    continue;
                }
                let added_columns = match table.as_str() {
                    "multiplier_observation" => 5,
                    "window_sample" | "credential_window_sample" => 1,
                    _ => 0,
                };
                for row in &mut rows {
                    row.extend(std::iter::repeat_n(
                        rusqlite::types::Value::Null,
                        added_columns,
                    ));
                }
                assert_eq!(table_rows(conn, &table), rows, "{table}");
            }

            let history = table_rows(conn, "multiplier_observation");
            for (id, pool) in [("5", "'primary'"), ("6", "'secondary'")] {
                conn.execute(
                    &v68_observation_insert(
                        conn,
                        &[
                            ("observation_id", id),
                            ("plan", "'plan-a'"),
                            ("pool_scope", "'all'"),
                            ("catalog_provider", "'catalog'"),
                            ("window_pool_key", pool),
                        ],
                    ),
                    [],
                )?;
            }
            let rows = table_rows(conn, "multiplier_observation");
            assert_eq!(rows.len(), 6);
            assert_eq!(&rows[..history.len()], history);
            Ok(())
        })
        .unwrap();
}

#[test]
fn schema_v68_refuses_observation_reuse_and_cross_lineage_corrections() {
    let conn = Connection::open_in_memory().unwrap();
    initialize_connection(&conn).unwrap();
    let (previous, migration) = migration_parts(68);
    for migration in previous {
        conn.execute_batch(migration.statements).unwrap();
    }
    v66_observation_history(&conn);
    conn.execute_batch(migration.statements).unwrap();
    conn.execute(
        &v68_observation_insert(
            &conn,
            &[
                ("observation_id", "5"),
                ("pool_scope", "'all'"),
                ("plan", "'plan-a'"),
            ],
        ),
        [],
    )
    .unwrap();
    conn.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA recursive_triggers=OFF;")
        .unwrap();

    let refuse = |verb: &str, changes: &[(&str, &str)], rule: &str| {
        let sql = v68_observation_insert(&conn, changes).replacen("INSERT", verb, 1);
        refuse_without_effect(&conn, "multiplier_observation", &sql, rule);
    };
    refuse(
        "INSERT",
        &[("observation_id", "7"), ("plan", "'plan-b'"), ("catalog_provider", "'catalog'")],
        "catalog_provider IS NULL",
    );
    for verb in ["INSERT", "INSERT OR REPLACE", "REPLACE"] {
        refuse(
            verb,
            &[
                ("observation_id", "4"),
                ("plan", "'plan-b'"),
                ("pool_scope", "'all'"),
            ],
            "multiplier_observation_insert_once",
        );
        refuse(
            verb,
            &[
                ("observation_id", "6"),
                ("plan", "'plan-a'"),
                ("pool_scope", "'unknown'"),
            ],
            "multiplier_observation_insert_once",
        );
        refuse(
            verb,
            &[
                ("observation_id", "6"),
                ("plan", "'plan-a'"),
                ("window_pool_key", "'secondary'"),
            ],
            "multiplier_observation_insert_once",
        );
        // An unused parent from a plan-wide chain, and a parent from a chain
        // with the wrong plan or pool, must all fail even without a fork.
        for (parent, plan, pool) in [
            ("4", "'plan-a'", "'secondary'"),
            ("5", "'plan-b'", "'primary'"),
            ("5", "'plan-a'", "'secondary'"),
        ] {
            refuse(
                verb,
                &[
                    ("observation_id", "6"),
                    ("plan", plan),
                    ("pool_scope", "'subset'"),
                    ("supersedes", parent),
                    ("window_pool_key", pool),
                ],
                "multiplier_observation_same_lineage",
            );
        }
        refuse(
            verb,
            &[
                ("observation_id", "6"),
                ("plan", "'plan-a'"),
                ("supersedes", "5"),
            ],
            "multiplier_observation_same_lineage",
        );
    }

    let reused_parent = [
        ("observation_id", "6"),
        ("plan", "'plan-a'"),
        ("pool_scope", "'all'"),
        ("supersedes", "3"),
    ];
    for verb in ["INSERT", "INSERT OR REPLACE", "REPLACE"] {
        refuse(verb, &reused_parent, "multiplier_observation_");
    }
    // Isolate the parent-reuse guard: the lineage trigger would also reject
    // this insert and could hide a guard that missed plan-wide corrections.
    conn.execute_batch("DROP TRIGGER multiplier_observation_same_lineage;")
        .unwrap();
    for verb in ["INSERT", "INSERT OR REPLACE", "REPLACE"] {
        refuse(verb, &reused_parent, "multiplier_observation_insert_once");
    }
}

#[test]
fn schema_v68_window_samples_accept_only_all_subset_or_unstated_scope() {
    let conn = Connection::open_in_memory().unwrap();
    initialize_connection(&conn).unwrap();
    for migration in MIGRATIONS {
        conn.execute_batch(migration.statements).unwrap();
    }
    for (table, login_column) in [
        ("window_sample", "account_ref"),
        ("credential_window_sample", "credential_ref"),
    ] {
        let insert = format!(
            "INSERT INTO {table} (provider,{login_column},pool_key,
                               fetched_at_instant,used_percent,model_scope) VALUES"
        );
        for (instant, scope) in [(1, Some("all")), (2, Some("subset")), (3, None)] {
            conn.execute(
                &format!("{insert} ('provider','login','primary',?1,10,?2)"),
                params![instant, scope],
            )
            .unwrap();
        }
        refuse_without_effect(
            &conn,
            table,
            &format!("{insert} ('provider','login','primary',4,10,'unknown')"),
            "model_scope",
        );
        let values: Vec<Option<String>> = conn
            .prepare(&format!(
                "SELECT model_scope FROM {table} ORDER BY fetched_at_instant"
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(values, [Some("all".into()), Some("subset".into()), None]);
    }
}

#[test]
fn schema_v68_provider_days_are_append_only() {
    let conn = Connection::open_in_memory().unwrap();
    initialize_connection(&conn).unwrap();
    for migration in MIGRATIONS {
        conn.execute_batch(migration.statements).unwrap();
    }
    conn.execute_batch(
        "PRAGMA recursive_triggers=OFF;
         INSERT INTO multiplier_provider_day VALUES ('provider','2026-01-02',1),
             ('provider','2026-01-03',2), ('another','2026-01-02',3);",
    )
    .unwrap();
    for (sql, guard) in [
        (
            "UPDATE multiplier_provider_day SET recorded_at_ms=4",
            "no_update",
        ),
        ("DELETE FROM multiplier_provider_day", "no_delete"),
        (
            "INSERT OR REPLACE INTO multiplier_provider_day VALUES ('provider','2026-01-02',4)",
            "insert_once",
        ),
    ] {
        refuse_without_effect(&conn, "multiplier_provider_day", sql, guard);
    }
    assert_eq!(table_rows(&conn, "multiplier_provider_day").len(), 3);
}
