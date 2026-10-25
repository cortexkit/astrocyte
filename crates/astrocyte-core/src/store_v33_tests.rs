fn v32_store() -> (TempStoreDirectory, SqliteStore) {
    let (directory, store) = migration_store();
    store.migrate("astrocyte", &MIGRATIONS[..32]).unwrap();
    store
        .with_conn(|conn| {
            initialize_connection(conn).unwrap();
            conn.execute_batch(
                "INSERT INTO usage_event(event_key,event_id,producer,producer_ref,session,
                    occurred_at_ms,charge_basis,recorded_at_ms) VALUES
                 (4,'first','pi','first','session',10,'metered_api',10),
                 (9,'second','opencode','second','session',20,'metered_api',20),
                 (19,'third','codex','third','session',30,'metered_api',30);
                 UPDATE cache_step_sequence SET cursor=100+cursor*13;
                 UPDATE sqlite_sequence SET seq=500 WHERE name='cache_step_sequence';",
            )
        })
        .unwrap();
    (directory, store)
}

fn cursor_bytes(conn: &Connection) -> Vec<u8> {
    conn.prepare("SELECT cursor,event_key FROM cache_step_sequence ORDER BY cursor")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .flat_map(|(cursor, key)| [cursor.to_be_bytes(), key.to_be_bytes()].concat())
        .collect()
}

#[test]
fn schema_v33_preserves_cursor_bytes_and_other_table_rows() {
    let (_directory, store) = v32_store();
    let before = store.with_conn(|conn| Ok(cursor_bytes(conn))).unwrap();
    let other_rows = |conn: &Connection| {
        all_rows(conn)
            .into_iter()
            .filter(|(name, _)| {
                !matches!(
                    name.as_str(),
                    "broca_step"
                        | "cache_step_sequence"
                        | "cortexkit_schema_version"
                        | "sqlite_sequence"
                )
            })
            .collect::<Vec<_>>()
    };
    let others = store.with_conn(|conn| Ok(other_rows(conn))).unwrap();
    let outcome = migrate_store(&store).unwrap();
    assert_eq!((outcome.applied, outcome.recorded), (1, 33));
    store
        .with_conn(|conn| {
            assert_eq!(cursor_bytes(conn), before);
            assert_eq!(other_rows(conn), others);
            let names: i64 = conn.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'broca_step%'
                 OR name='cache_broca_cursor'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(names, 0);
            let columns: Vec<String> = conn
                .prepare("SELECT name FROM pragma_table_info('cache_step_sequence')")?
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            assert_eq!(columns, ["cursor", "event_key"]);
            assert!(conn
                .execute(
                    "INSERT INTO cache_step_sequence(event_key) VALUES(NULL)",
                    []
                )
                .is_err());
            assert_eq!(
                conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))?,
                1
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn schema_v33_carries_sequence_above_max_and_new_harness_steps_get_cursors() {
    for empty in [false, true] {
        let (_directory, store) = v32_store();
        store
            .with_conn(|conn| {
                if empty {
                    conn.execute("DELETE FROM cache_step_sequence", [])?;
                }
                conn.execute(
                    "UPDATE sqlite_sequence SET seq=9000 WHERE name='cache_step_sequence'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        migrate_store(&store).unwrap();
        store
            .with_conn(|conn| {
                assert_eq!(crate::cache_diagnostics::last_event_key(conn)?, 9000);
                conn.execute_batch(
                    "INSERT INTO usage_event(event_id,producer,producer_ref,session,occurred_at_ms,
                        charge_basis,recorded_at_ms)
                     VALUES ('new-step','pi','new-step','session',40,'metered_api',40);",
                )?;
                let cursor: i64 = conn.query_row(
                    "SELECT s.cursor FROM cache_step_sequence s JOIN usage_event e
                     ON e.event_key=s.event_key WHERE e.event_id='new-step'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(cursor, 9001);
                let steps = crate::cache_diagnostics::steps_recorded_after(conn, 9000)?;
                assert_eq!(steps.len(), 1);
                assert_eq!(steps[0].step.as_ref().unwrap().cursor, 9001);
                assert_eq!(steps[0].step.as_ref().unwrap().step_ref, "new-step");
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn schema_v33_refuses_broca_cursors_and_restores_schema_rows_and_foreign_keys() {
    let (_directory, store) = v32_store();
    store
        .with_conn(|conn| {
            conn.execute_batch(
                "INSERT INTO broca_step(run_id,step_id,fact_id,project_root,harness,session,
                    provider,model,finished_at_ms,recorded_at_ms)
                 VALUES ('run',1,'fact','/project','broca','session','provider','model',1,1);",
            )
        })
        .unwrap();
    let before = store.with_conn(|conn| Ok(all_rows(conn))).unwrap();
    let schema = store
        .with_conn(|conn| Ok(table_rows(conn, "sqlite_schema")))
        .unwrap();
    let error = migrate_store(&store).unwrap_err().to_string();
    assert!(
        error.contains("cache_step_sequence_has_broca_steps"),
        "{error}"
    );
    store
        .with_conn(|conn| {
            assert_eq!(all_rows(conn), before);
            assert_eq!(table_rows(conn, "sqlite_schema"), schema);
            assert_eq!(
                conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))?,
                1
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn schema_v33_checks_foreign_keys_before_commit() {
    let (_directory, store) = v32_store();
    store
        .with_conn(|conn| {
            conn.execute_batch(
                "PRAGMA foreign_keys=OFF;
                 INSERT INTO cache_step_sequence(event_key) VALUES(99999);
                 PRAGMA foreign_keys=ON;",
            )
        })
        .unwrap();
    let before = store.with_conn(|conn| Ok(all_rows(conn))).unwrap();
    let schema = store
        .with_conn(|conn| Ok(table_rows(conn, "sqlite_schema")))
        .unwrap();
    let error = migrate_store(&store).unwrap_err().to_string();
    assert!(
        error.contains("cache_step_sequence_foreign_key_check"),
        "{error}"
    );
    store
        .with_conn(|conn| {
            assert_eq!(all_rows(conn), before);
            assert_eq!(table_rows(conn, "sqlite_schema"), schema);
            assert_eq!(
                conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))?,
                1
            );
            Ok(())
        })
        .unwrap();
}

#[test]
#[ignore = "builds a 2.3-million-row store to measure the cursor-table rebuild"]
fn schema_v33_large_cursor_rebuild_reports_elapsed() {
    let (_directory, store) = v32_store();
    store
        .with_conn(|conn| {
            conn.execute_batch(
                "BEGIN;
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<2301934)
                 INSERT INTO usage_event(event_id,producer,producer_ref,session,occurred_at_ms,
                     charge_basis,recorded_at_ms)
                 SELECT 'load-'||i,'pi','load-'||i,'bulk',i,'metered_api',i FROM n;
                 COMMIT;",
            )
        })
        .unwrap();
    let count: i64 = store
        .with_conn(|conn| {
            conn.query_row("SELECT count(*) FROM cache_step_sequence", [], |row| {
                row.get(0)
            })
        })
        .unwrap();
    assert_eq!(count, 2_301_937);
    let start = std::time::Instant::now();
    migrate_store(&store).unwrap();
    eprintln!(
        "migration 33 rebuilt {count} cache cursor rows in {:?}",
        start.elapsed()
    );
    let after: i64 = store
        .with_conn(|conn| {
            conn.query_row("SELECT count(*) FROM cache_step_sequence", [], |row| {
                row.get(0)
            })
        })
        .unwrap();
    assert_eq!(after, count);
}
