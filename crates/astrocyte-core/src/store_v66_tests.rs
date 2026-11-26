mod parent_session_refs {
    use super::*;

    fn parent_ref() -> String {
        cortexkit_role_llm_runner::session_ref(&cortexkit_role_llm_runner::SessionIdentity {
            runner: "thalamus-harness",
            harness: "opencode2",
            session: "parent",
        })
    }

    #[test]
    fn parent_session_ref_backfill_dirties_only_hours_with_matching_children() {
        let (_directory, store) = migration_store();
        let (before, _) = migration_parts(66);
        store.migrate("astrocyte", before).unwrap();
        let revisions = |conn: &Connection| {
            conn.prepare("SELECT revision FROM attribution_hour ORDER BY kind,hour_number")?
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
        };
        let before = store.with_conn(|conn| {
            let child = serde_json::json!({
                "fact_id":"child","subject":{"kind":"run","harness":"broca","run_id":"child-run"},
                "parent":{"kind":"session","runner":"thalamus-harness","harness":"opencode2","session":"parent"},
                "alfonso":"child-owner","workload":"mason","effective_from_ms":0,"recorded_at_ms":1
            });
            conn.execute(
                "INSERT INTO attribution_fact(fact_id,publisher,body,subject_kind,subject_run_id,
                    parent_kind,parent_runner,parent_harness,parent_session,workload,effective_from_ms,recorded_at_ms)
                 VALUES ('child','prefrontal',?1,'run','child-run','session','thalamus-harness','opencode2','parent','mason',0,1)",
                [child.to_string()],
            )?;
            conn.execute(
                "INSERT INTO session_alias VALUES('thalamus','opencode','owner-session',?1)",
                [parent_ref()],
            )?;
            conn.execute_batch(
                "INSERT INTO usage_event(event_id,producer,producer_ref,run_ref,occurred_at_ms,charge_basis,recorded_at_ms)
                 VALUES ('child-call','broca','child-call','child-run',1,'metered_api',1),
                        ('other-call','broca','other-call','other-run',3600001,'metered_api',1)",
            )?;
            revisions(conn)
        }).unwrap();

        assert_eq!(
            migrate_store(&store).unwrap().recorded,
            MIGRATIONS.last().unwrap().version
        );
        let after = store.with_conn(revisions).unwrap();
        assert_eq!(after, [before[0] + 1, before[1]]);
        // Reopening the migrated store does not mark the children again.
        assert_eq!(migrate_store(&store).unwrap().applied, 0);
        assert_eq!(store.with_conn(revisions).unwrap(), after);
    }

    #[test]
    fn parent_session_ref_backfill_is_atomic_and_append_only() {
        let (_directory, store) = migration_store();
        let (before, _) = migration_parts(66);
        store.migrate("astrocyte", before).unwrap();
        store
            .with_conn(|conn| {
                conn.execute_batch(
                    "INSERT INTO attribution_fact(fact_id,publisher,body,subject_kind,subject_run_id,
                         parent_kind,parent_runner,parent_harness,parent_session,workload,effective_from_ms,recorded_at_ms)
                     VALUES ('child','prefrontal','{}','run','run','session','thalamus-harness','opencode2','parent','mason',0,1),
                            ('unknown','prefrontal','{}','run','run','session',NULL,'opencode2','parent','mason',0,1),
                            ('flow','prefrontal','{}','run','run','flow',NULL,NULL,NULL,'mason',0,1)",
                )
            })
            .unwrap();

        let outcome = migrate_store(&store).unwrap();
        assert_eq!(outcome.recorded, MIGRATIONS.last().unwrap().version);
        assert_eq!(
            outcome.applied,
            u32::try_from(MIGRATIONS.len() - before.len()).unwrap()
        );
        assert_eq!(migrate_store(&store).unwrap().applied, 0);
        store
            .with_conn(|conn| {
                let read = || {
                    conn.prepare("SELECT parent_runner,session_ref,fact_id FROM attribution_parent_session_ref")?
                        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                };
                let expected = [("thalamus-harness".into(), parent_ref(), "child".into())];
                assert_eq!(read()?, expected);
                conn.pragma_update(None, "recursive_triggers", false)?;
                for (statement, trigger) in [
                    ("UPDATE attribution_parent_session_ref SET fact_id='changed'", "no_update"),
                    ("DELETE FROM attribution_parent_session_ref", "no_delete"),
                    ("INSERT OR REPLACE INTO attribution_parent_session_ref SELECT * FROM attribution_parent_session_ref", "insert_once"),
                    ("REPLACE INTO attribution_parent_session_ref SELECT * FROM attribution_parent_session_ref", "insert_once"),
                ] {
                    let error = conn.execute_batch(statement).unwrap_err().to_string();
                    assert!(error.contains(&format!("attribution_parent_session_ref_{trigger}")), "{error}");
                    assert_eq!(read()?, expected);
                }
                Ok(())
            })
            .unwrap();
    }
}
