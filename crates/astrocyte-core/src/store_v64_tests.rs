#[test]
fn schema_v64_removes_only_redundant_unknown_basis_heads_and_stale_queue_rows() {
    let (_directory, store) = migration_store();
    let before = MIGRATIONS.partition_point(|migration| migration.version < 64);
    store.migrate("astrocyte", &MIGRATIONS[..before]).unwrap();
    let guard = |conn: &Connection| {
        conn.query_row(
            "SELECT sql FROM sqlite_schema WHERE name='priced_charge_no_delete'",
            [],
            |row| row.get::<_, String>(0),
        )
    };
    let guard_before = store.with_conn(|conn| {
        conn.execute_batch(
            "INSERT INTO usage_event(event_key,event_id,producer,producer_ref,occurred_at_ms,
                    recorded_at_ms,charge_basis) VALUES
                (1,'redundant','pi','redundant',1,1,'unknown'),
                (2,'other-reason','pi','other-reason',NULL,1,'unknown'),
                (3,'known','broca','known',2,1,'metered_api'),
                (4,'chain','pi','chain',3,1,'unknown'),
                (5,'pending','broca','pending',4,1,'metered_api');
             UPDATE usage_event SET pricing_generation=1 WHERE event_key<5;
             INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                SELECT event_key,'input','token','billed','fixture',5,0 FROM usage_event;
             INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                VALUES (1,'output','token','billed','fixture',0,0);
             INSERT INTO priced_charge(charge_key,event_key,role,unit,pricing_generation,
                    outcome,charge_basis,basis_covered,unpriced_reason,origin,recorded_at_ms) VALUES
                (101,1,'input','token',1,'unpriced','unknown',0,'charge_basis_unknown','pricing_pass',1),
                (102,1,'output','token',1,'unpriced','unknown',0,'charge_basis_unknown','pricing_pass',1),
                (103,2,'input','token',1,'unpriced','unknown',0,'undated_event','pricing_pass',1),
                (104,3,'input','token',1,'unpriced','metered_api',0,'charge_basis_unknown','unattributed',1),
                (105,4,'input','token',1,'unpriced','unknown',0,'charge_basis_unknown','pricing_pass',1);
             UPDATE usage_event SET pricing_generation=2 WHERE event_key=4;
             INSERT INTO priced_charge(charge_key,event_key,role,unit,pricing_generation,
                    outcome,charge_basis,basis_covered,unpriced_reason,supersedes,origin,recorded_at_ms)
                VALUES (106,4,'input','token',2,'unpriced','unknown',0,'charge_basis_unknown',105,'operator_reprice',2);
             INSERT INTO pricing_pending(event_key) VALUES (1),(2),(4),(5);",
        )?;
        let denied = conn.execute("DELETE FROM priced_charge WHERE charge_key=101", []).unwrap_err();
        assert!(denied.to_string().contains("priced_charge_no_delete"));
        guard(conn)
    }).unwrap();

    let migrated = store.migrate("astrocyte", &MIGRATIONS[..=before]).unwrap();
    store
        .with_conn(|conn| {
            let mut statement = conn
                .prepare("SELECT charge_key,supersedes FROM priced_charge ORDER BY charge_key")?;
            let charges = statement
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(
                charges,
                [(103, None), (104, None), (105, None), (106, Some(105))]
            );
            let mut statement =
                conn.prepare("SELECT event_key FROM pricing_pending ORDER BY event_key")?;
            let pending = statement
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(pending, [5]);
            assert_eq!(guard(conn)?, guard_before);
            let denied = conn
                .execute("DELETE FROM priced_charge WHERE charge_key=103", [])
                .unwrap_err();
            assert!(denied.to_string().contains("priced_charge_no_delete"));
            let generation: i64 = conn.query_row(
                "SELECT pricing_generation FROM usage_event WHERE event_key=1",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(generation, 1);
            assert_eq!(
                conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
                "ok"
            );
            assert!(conn
                .prepare("PRAGMA foreign_key_check")?
                .query([])?
                .next()?
                .is_none());
            Ok(())
        })
        .unwrap();
    assert_eq!((migrated.applied, migrated.recorded), (1, 64));
}
