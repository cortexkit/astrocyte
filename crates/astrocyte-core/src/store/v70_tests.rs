fn v70_cache_fixture(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "INSERT INTO usage_event(event_key,event_id,producer,producer_ref,session,charge_basis,occurred_at_ms,recorded_at_ms)
         VALUES (1,'early','fixture','early','mixed','metered_api',10,1),
                (2,'zero','fixture','zero','mixed','unknown',20,1),
                (3,'positive','fixture','positive','mixed','unknown',30,1),
                (4,'wrong-only','fixture','wrong-only','wrong-only','unknown',10,1),
                (5,'no-positive','fixture','no-positive','no-positive','unknown',10,1);
         INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
         VALUES (1,'cache_read','second','billed','vendor',25,0),
                (2,'cache_read','token','billed','vendor',0,0),
                (2,'cache_read','second','billed','vendor',10,0),
                (3,'cache_read','token_cache_5m','billed','vendor',5,0),
                (4,'cache_read','second','billed','vendor',25,0),
                (5,'cache_read','second','billed','vendor',25,0),
                (5,'cache_read','token','billed','vendor',0,0);",
    )
}

type CacheProjection = (Vec<(String, Option<i64>)>, Vec<(String, i64, i64)>);

fn v70_cache_projection(conn: &Connection) -> rusqlite::Result<CacheProjection> {
    let sessions = conn
        .prepare("SELECT session,first_positive_cursor FROM cache_read_session ORDER BY session")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let steps = conn
        .prepare(
            "SELECT session,occurred_at_ms,cursor FROM cache_read_step JOIN cache_read_session
             USING (session_id) ORDER BY session,occurred_at_ms",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    Ok((sessions, steps))
}

#[test]
fn catalog_regression_upgrade_repairs_pre_token_only_cache_projection() {
    let (_directory, upgraded) = migration_store();
    let (previous, _) = migration_parts(62);
    upgraded.migrate("astrocyte", previous).unwrap();
    let facts = |conn: &Connection| {
        (
            table_rows(conn, "measurement"),
            table_rows(conn, "priced_charge"),
            table_rows(conn, "cache_step_sequence"),
        )
    };
    let original = upgraded
        .with_conn(|conn| {
            v70_cache_fixture(conn)?;
            conn.execute_batch(
                "UPDATE usage_event SET currency='USD',pricing_generation=1 WHERE event_key=1;
                 INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,
                    currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                 VALUES (1,'cache_read','second',1,'priced','metered_api',0,'USD',25,1,9,'none','fixture',1,'{}','pricing_pass',1);",
            )?;
            Ok(facts(conn))
        })
        .unwrap();
    migrate_store(&upgraded).unwrap();

    let (_directory, fresh) = migration_store();
    migrate_store(&fresh).unwrap();
    let expected = fresh
        .with_conn(|conn| {
            v70_cache_fixture(conn)?;
            v70_cache_projection(conn)
        })
        .unwrap();
    assert_eq!(
        expected.0,
        [("mixed".into(), Some(3)), ("no-positive".into(), None)]
    );
    assert_eq!(
        expected.1,
        [
            ("mixed".into(), 20, 2),
            ("mixed".into(), 30, 3),
            ("no-positive".into(), 10, 5)
        ]
    );

    upgraded
        .with_conn(|conn| {
            assert_eq!(v70_cache_projection(conn)?, expected);
            assert_eq!(facts(conn), original);
            assert_eq!(table_rows(conn, "cache_read_step").len(), expected.1.len());
            Ok(())
        })
        .unwrap();
}

#[test]
fn catalog_regression_charge_schema_requires_priced_exponent_and_covered_amount() {
    let (_directory, store) = migration_store();
    migrate_store(&store).unwrap();
    store
        .with_conn(|conn| {
            conn.execute_batch(
                "INSERT INTO usage_event(event_key,event_id,producer,producer_ref,charge_basis,recorded_at_ms)
                 VALUES (1,'api','fixture','api','metered_api',1),
                        (2,'plan','fixture','plan','subscription_included',1);
                 UPDATE usage_event SET currency='USD',pricing_generation=1;
                 INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES (1,'input','token','billed','vendor',1,0),
                        (2,'input','token','billed','vendor',1,0);",
            )?;
            let priced = "INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,
                              currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                          VALUES (1,'input','token',1,'priced','metered_api',0,'USD',1,1000000,?1,'none','fixture',1,'{}','pricing_pass',1)";
            let covered = "INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,
                                currency,charge_nanos,origin,recorded_at_ms)
                           VALUES (2,'input','token',1,'covered','subscription_included',1,'USD',?1,'pricing_pass',1)";
            let refused = [priced, covered].map(|sql| conn.execute(sql, [None::<i64>]));
            for result in refused {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "priced_charge_required_outcome_fields",
                );
            }
            assert!(table_rows(conn, "priced_charge").is_empty());
            conn.execute(priced, [9_i64])?;
            conn.execute(covered, [0_i64])?;
            assert_eq!(table_rows(conn, "priced_charge").len(), 2);
            Ok(())
        })
        .unwrap();
}
