mod session_refs {
    use super::*;
    use crate::attribution::{commit_page, Page, Publisher};
    use serde_json::json;

    fn v58_store() -> (TempStoreDirectory, SqliteStore) {
        let (directory, store) = migration_store();
        let before = MIGRATIONS.partition_point(|m| m.version < 59);
        store.migrate("astrocyte", &MIGRATIONS[..before]).unwrap();
        (directory, store)
    }

    fn fact(id: &str, subject: serde_json::Value) -> serde_json::Value {
        json!({"fact_id":id,"subject":subject,"alfonso":"owner","parent":null,"workload":"mason","task":null,"project_id":null,"effective_from_ms":0,"recorded_at_ms":1,"supersedes":null})
    }

    /// Facts stored before the table existed get exactly the rows ingest writes
    /// for the same facts today, and the migration changes nothing else.
    #[test]
    fn session_ref_backfill_matches_ingest_and_changes_no_other_row() {
        let ingested = Connection::open_in_memory().unwrap();
        initialize_connection(&ingested).unwrap();
        for migration in MIGRATIONS {
            ingested.execute_batch(migration.statements).unwrap();
        }
        let page = Page {
            facts: vec![
                fact(
                    "a",
                    json!({"kind":"session","runner":"thalamus","harness":"opencode","session":"one"}),
                ),
                fact(
                    "b",
                    json!({"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"one"}),
                ),
                fact(
                    "c",
                    json!({"kind":"session","runner":"pi","harness":"pi","session":"two"}),
                ),
                fact("d", json!({"kind":"run","harness":"broca","run_id":"run"})),
                fact("e", json!({"kind":"flow","flow_id":"flow"})),
            ],
            next: None,
            through: json!(1),
        };
        let tx = ingested.unchecked_transaction().unwrap();
        commit_page(&tx, Publisher::Prefrontal, None, &page, 10).unwrap();
        tx.commit().unwrap();
        let expected = table_rows(&ingested, "attribution_session_ref");
        assert_eq!(expected.len(), 3, "one row per session fact");

        let (_directory, store) = v58_store();
        let facts = table_rows(&ingested, "attribution_fact");
        let before = store
            .with_conn(|conn| {
                for row in &facts {
                    conn.execute(
                        "INSERT INTO attribution_fact VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
                        rusqlite::params_from_iter(row.iter()),
                    )?;
                }
                Ok(all_rows(conn))
            })
            .unwrap();
        let outcome = migrate_store(&store).unwrap();
        let latest = MIGRATIONS.last().unwrap().version;
        assert_eq!((outcome.applied, outcome.recorded), (latest - 58, latest));
        store
            .with_conn(|conn| {
                assert_eq!(table_rows(conn, "attribution_session_ref"), expected);
                assert!(table_rows(conn, "broca_attempt_detail").is_empty());
                assert!(table_rows(conn, "broca_segment_evidence").is_empty());
                assert!(table_rows(conn, "broca_segment_measurement").is_empty());
                assert!(table_rows(conn, "broca_segment_mismatch").is_empty());
                assert!(table_rows(conn, "quota_reset_period").is_empty());
                assert_eq!(
                    conn.query_row(
                        "SELECT checked_through FROM broca_segment_check_state WHERE id=1",
                        [],
                        |row| row.get::<_, i64>(0)
                    )?,
                    0,
                );
                let others: Vec<_> = all_rows(conn)
                    .into_iter()
                    .filter(|(table, _)| {
                        !matches!(
                            table.as_str(),
                            "attribution_session_ref"
                                | "cortexkit_schema_version"
                                | "broca_attempt_detail"
                                | "broca_segment_evidence"
                                | "broca_segment_measurement"
                                | "broca_segment_mismatch"
                                | "broca_segment_check_state"
                                | "quota_reset_period"
                        )
                    })
                    .collect();
                let before: Vec<_> = before
                    .iter()
                    .filter(|(table, _)| table != "cortexkit_schema_version")
                    .cloned()
                    .collect();
                // Later migrations may append nullable columns to an existing
                // table. An existing row then gains only trailing NULLs, which
                // is not a change to it, so compare the columns it had before.
                assert_eq!(others.len(), before.len());
                let others: Vec<_> = others
                    .into_iter()
                    .zip(&before)
                    .map(|((table, rows), (_, old_rows))| {
                        assert_eq!(rows.len(), old_rows.len(), "{table}");
                        let rows = rows
                            .into_iter()
                            .zip(old_rows)
                            .map(|(mut row, old)| {
                                let added = row.split_off(old.len());
                                assert!(
                                    added.iter().all(|v| *v == rusqlite::types::Value::Null),
                                    "{table}: an appended column is not NULL"
                                );
                                row
                            })
                            .collect::<Vec<_>>();
                        (table, rows)
                    })
                    .collect();
                assert_eq!(others, before);
                Ok(())
            })
            .unwrap();
        // A second start finds the migration recorded and fills nothing again.
        assert_eq!(migrate_store(&store).unwrap().applied, 0);
    }

    #[test]
    fn session_refs_are_append_only_even_with_replace_and_default_trigger_settings() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.pragma_update(None, "recursive_triggers", false)
            .unwrap();
        conn.execute_batch("INSERT INTO attribution_session_ref VALUES('thalamus','ref','fact');")
            .unwrap();
        let table = "attribution_session_ref";
        let before = table_rows(&conn, table);
        for (sql, trigger) in [
            (format!("UPDATE {table} SET fact_id='other'"), "no_update"),
            (format!("DELETE FROM {table}"), "no_delete"),
            (
                format!("INSERT OR REPLACE INTO {table} SELECT * FROM {table}"),
                "insert_once",
            ),
            (
                format!("REPLACE INTO {table} SELECT * FROM {table}"),
                "insert_once",
            ),
        ] {
            let error = conn.execute_batch(&sql).unwrap_err().to_string();
            assert!(error.contains(&format!("{table}_{trigger}")), "{error}");
            assert_eq!(table_rows(&conn, table), before);
        }
    }

    /// Times the one-off backfill over a production-sized history. Run it with
    /// `--ignored --nocapture`; it reports, and asserts only the row count.
    #[test]
    #[ignore = "measurement"]
    fn session_ref_backfill_time_for_120k_facts() {
        let (_directory, store) = v58_store();
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<119999)
                     INSERT INTO attribution_fact(fact_id,publisher,body,subject_kind,subject_runner,
                         subject_harness,subject_session,workload,effective_from_ms,recorded_at_ms)
                     SELECT 'fact-'||i,'prefrontal','{}','session',
                         CASE i%2 WHEN 0 THEN 'thalamus' ELSE 'pi' END,'opencode','session-'||(i/3),
                         'mason',0,1 FROM n;",
                )
            })
            .unwrap();
        let started = std::time::Instant::now();
        assert_eq!(
            migrate_store(&store).unwrap().recorded,
            MIGRATIONS.last().unwrap().version
        );
        let elapsed = started.elapsed();
        let rows: i64 = store
            .with_conn(|conn| {
                conn.query_row("SELECT count(*) FROM attribution_session_ref", [], |r| {
                    r.get(0)
                })
            })
            .unwrap();
        assert_eq!(rows, 120_000);
        println!(
            "SESSION_REF_BACKFILL rows={rows} elapsed_ms={}",
            elapsed.as_millis()
        );
    }
}
