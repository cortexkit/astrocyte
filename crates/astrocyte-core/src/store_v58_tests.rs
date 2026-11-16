mod attribution_storage {
    use super::*;

    fn before_attribution() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 58) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    #[test]
    fn attribution_migration_preserves_ledger_and_requeues_derived_cells() {
        let conn = before_attribution();
        conn.execute_batch("INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,charge_basis,recorded_at_ms)
                VALUES('call','broca','call',20,'metered_api',21);
            INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                VALUES(1,'input','token','billed','fixture',12,0);
            UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_key=1;
            INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                VALUES(1,'input','token',1,'priced','metered_api',0,'USD',100,1,9,'none','fixture',1,'{}','pricing_pass',1);
            INSERT INTO summary_value VALUES(1,'broca'),(2,'metered_api'),(3,'USD'),(4,'token'),(5,'input');
            INSERT INTO summary_cell VALUES(1,0,0,1,0,0,0,0,2,0,0,3,'{}',0,0,0);
            INSERT INTO summary_quantity VALUES(1,4,5,0,'12',1,'0',0,1);
            INSERT INTO summary_money VALUES(1,0,3,'100',1);
            UPDATE summary_hour SET built_revision=revision,format_version=6,events_total=1;
            DELETE FROM summary_dirty_hour;
            UPDATE valuation_hour SET state='built';
            INSERT INTO valuation_cell VALUES(0,0,'[]','[]','api',NULL,NULL,'{}','100','{}');").unwrap();
        let before = all_rows(&conn);
        let valuation_revision: i64 = conn
            .query_row("SELECT revision FROM valuation_hour", [], |r| r.get(0))
            .unwrap();
        conn.execute_batch(MIGRATIONS.last().unwrap().statements)
            .unwrap();
        assert_eq!(MIGRATIONS.last().unwrap().version, 58);
        for (table, rows) in before {
            match table.as_str() {
                "summary_cell" | "summary_quantity" | "summary_money" | "valuation_cell" => {
                    assert!(table_rows(&conn, &table).is_empty(), "{table}");
                }
                "valuation_hour" => {
                    assert_eq!(
                        conn.query_row("SELECT state,revision FROM valuation_hour", [], |r| Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?
                        )))
                        .unwrap(),
                        ("dirty".into(), valuation_revision + 1)
                    );
                }
                _ => assert_eq!(table_rows(&conn, &table), rows, "{table} must survive"),
            }
        }
        assert_eq!(conn.query_row("SELECT count(*) FROM summary_hour INDEXED BY summary_hour_stale WHERE built_revision IS NOT revision OR format_version IS NOT 7", [], |r|r.get::<_,i64>(0)).unwrap(), 1);
        // Only the new tuple may distinguish otherwise identical summary cells.
        conn.execute_batch(
            "INSERT INTO summary_value VALUES(6,'tuple-a'),(7,'tuple-b');
            INSERT INTO summary_cell VALUES(1,0,0,1,0,0,0,0,2,0,0,3,'{}',0,0,0,6),
                (2,0,0,1,0,0,0,0,2,0,0,3,'{}',0,0,0,7);",
        )
        .unwrap();
        assert!(conn.execute_batch("INSERT INTO summary_cell SELECT 3,kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,payload,paying_credential,runner,harness,attribution FROM summary_cell WHERE cell_id=1").is_err());
        conn.execute_batch(
            "INSERT INTO summary_quantity VALUES(1,4,5,0,'12',1,'0',0,1);
            INSERT INTO summary_money VALUES(1,0,3,'100',1);
            DELETE FROM summary_cell WHERE cell_id=1;
            UPDATE usage_event SET pricing_generation=pricing_generation+1 WHERE event_key=1;",
        )
        .unwrap();
        assert!(table_rows(&conn, "summary_quantity").is_empty());
        assert!(table_rows(&conn, "summary_money").is_empty());
        assert_eq!(
            conn.query_row("SELECT hour_number FROM summary_dirty_hour", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    #[test]
    fn attribution_evidence_is_append_only_even_with_replace_and_default_trigger_settings() {
        let conn = before_attribution();
        conn.execute_batch(MIGRATIONS.last().unwrap().statements)
            .unwrap();
        conn.pragma_update(None, "recursive_triggers", false)
            .unwrap();
        conn.execute_batch("INSERT INTO attribution_fact(fact_id,publisher,body,subject_kind,subject_runner,subject_session,workload,effective_from_ms,recorded_at_ms)
                VALUES('fact','basal','{}','session','broca','session','flow',1,2);
            INSERT INTO placement_fact VALUES('placement','entorhinal','{}','project',NULL,1,2,NULL);
            INSERT INTO session_alias VALUES('thalamus','opencode2','host','bound');").unwrap();
        for table in ["attribution_fact", "placement_fact", "session_alias"] {
            let before = table_rows(&conn, table);
            for (sql, trigger) in [
                (
                    format!(
                        "UPDATE {table} SET {}",
                        if table == "session_alias" {
                            "bound_session='changed'"
                        } else {
                            "body='[]'"
                        }
                    ),
                    "no_update",
                ),
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
        // Corrections and independent alias pairs append rather than replacing evidence.
        conn.execute_batch("INSERT INTO attribution_fact(fact_id,publisher,body,subject_kind,subject_run_id,workload,effective_from_ms,recorded_at_ms,supersedes)
                VALUES('correction','basal','{}','run','run','flow',1,3,'fact');
            INSERT INTO placement_fact VALUES('move','entorhinal','{}','project','workspace',3,3,'placement');
            INSERT INTO session_alias VALUES('thalamus','opencode2','host','another-bound');
            INSERT INTO attribution_cursor VALUES('basal','attribution.read','1','2',3);
            UPDATE attribution_cursor SET cursor='2',last_read_at_ms=4 WHERE publisher='basal';").unwrap();
        for table in ["attribution_fact", "placement_fact", "session_alias"] {
            assert_eq!(table_rows(&conn, table).len(), 2);
        }
        assert_eq!(
            conn.query_row("SELECT cursor FROM attribution_cursor", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "2"
        );
    }

    #[test]
    fn attribution_publisher_vectors_match_pinned_sha256sums() {
        use sha2::{Digest, Sha256};
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test-vectors/attribution-facts-v1");
        let mut files = 0;
        for publisher in ["basal", "magic-context", "entorhinal"] {
            let dir = root.join(publisher);
            for line in std::fs::read_to_string(dir.join("SHA256SUMS"))
                .unwrap()
                .lines()
            {
                let mut fields = line.split_whitespace();
                let expected = fields.next().unwrap();
                let name = fields.next().unwrap().trim_start_matches('*');
                let bytes = std::fs::read(dir.join(name)).unwrap();
                assert_eq!(
                    hex::encode(Sha256::digest(&bytes)),
                    expected,
                    "{publisher}/{name}"
                );
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
                files += 1;
            }
        }
        assert_eq!(files, 12);
    }
}
