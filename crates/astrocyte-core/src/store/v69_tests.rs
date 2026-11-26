#[test]
fn schema_v69_adds_append_only_media_caller_without_rewriting_calls() {
    let (_directory, store) = migration_store();
    let (previous, _) = migration_parts(69);
    store.migrate("astrocyte", previous).unwrap();
    let before = store.with_conn(|conn| {
        conn.execute_batch(
            "INSERT INTO usage_event(event_key,event_id,producer,producer_ref,charge_basis,occurred_at_ms,recorded_at_ms)
                 VALUES (1,'first','colliculus','first','unknown',3600001,1),
                        (2,'second','colliculus','second','unknown',7200001,1);",
        )?;
        Ok(all_rows(conn))
    }).unwrap();

    let outcome = migrate_store(&store).unwrap();
    assert_eq!((outcome.applied, outcome.recorded), (1, 69));
    store
        .with_conn(|conn| {
            for (table, rows) in before {
                if table != "cortexkit_schema_version" {
                    assert_eq!(table_rows(conn, &table), rows, "{table}");
                }
            }
            assert!(table_rows(conn, "media_caller").is_empty());
            let revision = |hour| {
                conn.query_row(
                    "SELECT revision FROM attribution_hour WHERE kind=0 AND hour_number=?1",
                    [hour],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
            };
            let before = (revision(1), revision(2));
            assert!(conn
                .execute("INSERT INTO media_caller(event_key) VALUES(1)", [])
                .is_err());
            assert!(conn
                .execute(
                    "INSERT INTO media_caller(event_key,device) VALUES(1,'[]')",
                    []
                )
                .is_err());
            conn.execute(
                "INSERT INTO media_caller(event_key,agent_id) VALUES(1,'owner')",
                [],
            )?;
            assert_eq!((revision(1), revision(2)), (before.0 + 1, before.1));

            // REPLACE must fail even when SQLite would skip the delete trigger.
            conn.execute_batch("PRAGMA recursive_triggers=OFF;")?;
            for sql in [
                "INSERT INTO media_caller(event_key,agent_id) VALUES(1,'replacement')",
                "INSERT OR REPLACE INTO media_caller(event_key,agent_id) VALUES(1,'replacement')",
                "UPDATE media_caller SET agent_id='replacement'",
                "DELETE FROM media_caller",
            ] {
                refuse_without_effect(conn, "media_caller", sql, "media_caller is append-only");
            }
            assert_eq!((revision(1), revision(2)), (before.0 + 1, before.1));
            Ok(())
        })
        .unwrap();
}
