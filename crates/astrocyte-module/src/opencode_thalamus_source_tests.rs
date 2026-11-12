// Payer-join regressions share the fixtures in opencode_thalamus_source.

impl Fixture {
    fn manage_payer_session(&self) {
        self.sql("INSERT INTO thalamus_payer_session VALUES ('opencode','session','owner',0)");
    }

    fn payer_row(&self, step: &str, basis: &str) {
        self.state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO thalamus_payer_record
                 (attempt_id,seq,incarnation,harness,session,request_kind,step_ref,
                  recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref)
                 VALUES (?1,1,'first','opencode','session','main',?1,?2,
                         'anthropic','claude-sonnet-4',?3,'vault-login','provider-account')",
                    params![step, T0, basis],
                )
                .map_err(|e| e.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    fn payer_call(&self, layout: Layout, id: &str, completed: bool, updated: i64) {
        self.payer_call_in(ONE, layout, id, completed, updated);
    }

    fn payer_call_in(&self, folder: &str, layout: Layout, id: &str, completed: bool, updated: i64) {
        let mut data = payload(layout, completed);
        // Creation predates completion by much more than the payer wait.
        data["time"]["completed"] = if completed { json!(T0) } else { Value::Null };
        data["finish"] = json!("stop");
        insert(
            &self.database(folder),
            layout,
            id,
            "assistant",
            updated,
            &data,
        );
    }

    fn outside_call(&self, layout: Layout, id: &str, updated: i64) {
        self.call(ONE, layout, id, true, updated);
        let table = match layout {
            Layout::V1 => "message",
            Layout::V2 => "session_message",
        };
        Connection::open(self.database(ONE))
            .unwrap()
            .execute(
                &format!("UPDATE {table} SET session_id='outside' WHERE id=?1"),
                [id],
            )
            .unwrap();
    }

    fn payer_facts(&self, step: &str) -> (String, Option<String>, Option<String>) {
        self.state.with_conn(|conn| {
            conn.query_row(
                "SELECT charge_basis,auth_selection,account_ref FROM usage_event WHERE producer_ref=?1",
                [step], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            ).map_err(|e| e.to_string())
        }).unwrap()
    }

    fn event_bytes(&self, step: &str) -> Vec<rusqlite::types::Value> {
        self.state
            .with_conn(|conn| {
                let mut stmt = conn
                    .prepare("SELECT * FROM usage_event WHERE producer_ref=?1")
                    .map_err(|e| e.to_string())?;
                let columns = stmt.column_count();
                // The integer key is a store-local foreign-key coordinate, not
                // producer evidence, and depends on unrelated insertion order.
                let evidence_columns = (0..columns)
                    .filter(|i| stmt.column_name(*i).unwrap() != "event_key")
                    .collect::<Vec<_>>();
                stmt.query_row([step], |row| {
                    evidence_columns.iter().map(|i| row.get(*i)).collect()
                })
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    fn cursor_positions(&self) -> Vec<(String, i64)> {
        self.state
            .with_conn(|conn| {
                conn.prepare(
                    "SELECT layout,consumed_ordinal FROM declared_store_cursor ORDER BY layout",
                )
                .map_err(|e| e.to_string())?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(|e| e.to_string())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    fn fast_tick(&self, at: i64) {
        astrocyte_readers::read_io::reset_counts();
        self.source
            .drain_fast(&self.state, at, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(
            astrocyte_readers::read_io::counts(),
            astrocyte_readers::read_io::Counts::default()
        );
    }
}

#[test]
fn opencode_payer_roots_owner_wins_and_counts_owner_disagreement() {
    let f = Fixture::new();
    f.manage_payer_session();
    f.payer_row("managed", "metered_api");
    f.payer_call(Layout::V2, "managed", true, CREATED);
    f.declare(vec![f.entry(ONE, false)]);
    f.tick(T0);
    assert_eq!(
        f.events(),
        vec![("managed".into(), "opencode".into(), Some(OWNER.into()))]
    );
    f.state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind='owner_disagreement'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                1
            );
            Ok(())
        })
        .unwrap();
    f.tick(T0 + TICK);
    f.state
        .with_conn(|conn| {
            let coverage = astrocyte_core::report::ledger_coverage(
                conn,
                &astrocyte_core::report::LedgerCoverageRequest {},
                T0 + TICK,
            )
            .unwrap();
            assert_eq!(coverage.thalamus_payer.unwrap().owner_disagreements, 1);
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                0
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn opencode_payer_join_matched_facts_and_unmanaged_bytes() {
    for layout in LAYOUTS {
        let control = Fixture::new();
        control.outside_call(layout, "unmanaged", CREATED);
        control.declare(vec![control.entry(ONE, false)]);
        control.tick(T0);
        let original = control.event_bytes("unmanaged");
        for basis in ["subscription_included", "metered_api"] {
            let f = Fixture::new();
            f.manage_payer_session();
            f.payer_row("managed", basis);
            f.payer_call(layout, "managed", true, CREATED);
            f.outside_call(layout, "unmanaged", CREATED + 1);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            assert_eq!(
                f.payer_facts("managed"),
                (
                    basis.into(),
                    Some("vault-login".into()),
                    Some("provider-account".into())
                ),
                "{layout:?}"
            );
            assert_eq!(f.event_bytes("unmanaged"), original, "{layout:?}");
            assert_eq!(f.refs(), vec!["managed", "unmanaged"]);
            assert_eq!(f.degraded(), None);
        }
    }
}

#[test]
fn opencode_payer_join_later_empty_full_or_fast_pass_records_once() {
    for layout in LAYOUTS {
        for fast in [false, true] {
            let f = Fixture::new();
            f.manage_payer_session();
            f.payer_call(layout, "later", true, CREATED);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            assert!(f.refs().is_empty(), "waiting for a payer: {layout:?}");
            assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
            // Seed each layout's in-memory rowid cursor with a second tick,
            // so the payer resolves on a pass with no new transcript rows.
            f.tick(T0 + 1);
            f.payer_row("later", "metered_api");
            if fast {
                f.fast_tick(T0 + 2);
                f.fast_tick(T0 + 3);
            } else {
                f.tick(T0 + 2);
                f.tick(T0 + 3);
            }
            assert_eq!(f.refs(), vec!["later"], "{layout:?}, fast={fast}");
            assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&0));
            assert_eq!(
                f.payer_facts("later"),
                (
                    "metered_api".into(),
                    Some("vault-login".into()),
                    Some("provider-account".into())
                )
            );
            f.tick(T0 + TICK);
            assert_eq!(f.refs(), vec!["later"]);
            f.state
                .with_conn(|conn| {
                    assert_eq!(
                        conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |r| r
                            .get::<_, i64>(0))
                            .unwrap(),
                        0
                    );
                    Ok(())
                })
                .unwrap();
        }
    }

    let f = Fixture::new();
    f.manage_payer_session();
    for folder in [ONE, TWO] {
        for layout in LAYOUTS {
            f.payer_call_in(
                folder,
                layout,
                &format!("{folder}-{layout:?}"),
                true,
                CREATED,
            );
        }
    }
    f.declare(vec![f.entry(ONE, false), f.entry(TWO, false)]);
    f.tick(T0);
    assert!(f.refs().is_empty());
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&4));
    f.payer_row("s-0001-V1", "metered_api");
    f.fast_tick(T0 + 1);
    assert_eq!(f.refs(), vec!["s-0001-V1"]);
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&3));
    f.fast_tick(T0 + 600_000);
    assert_eq!(f.refs().len(), 4);
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&0));
}

#[test]
fn opencode_payer_join_timeout_uses_completion_or_durable_first_seen() {
    for layout in LAYOUTS {
        for completed in [true, false] {
            let mut f = Fixture::new();
            f.manage_payer_session();
            f.payer_call(layout, "never", completed, CREATED);
            f.declare(vec![f.entry(ONE, true)]);
            f.tick(T0);
            assert!(f.refs().is_empty(), "{layout:?}, completed={completed}");
            f.restart();
            f.tick(T0 + 599_999);
            assert!(
                f.refs().is_empty(),
                "timeout starts at completion, not creation"
            );
            f.fast_tick(T0 + 600_000);
            assert_eq!(f.refs(), vec!["never"]);
            assert_eq!(f.payer_facts("never"), ("unknown".into(), None, None));
            f.tick(T0 + 600_001);
            assert_eq!(f.refs(), vec!["never"]);
            f.state
                .with_conn(|conn| {
                    assert_eq!(conn.query_row(
                    "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind='without_payer'", [],
                    |r| r.get::<_, i64>(0)).unwrap(), 1);
                    Ok(())
                })
                .unwrap();
            assert_eq!(f.degraded(), None);
        }
    }
}

#[test]
fn opencode_payer_join_restart_replays_wait_behind_newer_rows() {
    for layout in LAYOUTS {
        let mut f = Fixture::new();
        f.manage_payer_session();
        f.payer_call(layout, "waiting", true, CREATED);
        f.outside_call(layout, "newer", CREATED + 100);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0);
        assert_eq!(f.refs(), vec!["newer"], "{layout:?}");
        f.restart();
        f.tick(T0 + 1);
        assert_eq!(f.refs(), vec!["newer"]);
        f.payer_row("waiting", "subscription_included");
        f.tick(T0 + 2);
        f.restart();
        f.tick(T0 + 3);
        assert_eq!(f.refs(), vec!["newer", "waiting"]);
        assert_eq!(
            f.payer_facts("waiting"),
            (
                "subscription_included".into(),
                Some("vault-login".into()),
                Some("provider-account".into())
            )
        );
    }
}

#[test]
fn opencode_payer_join_wait_holds_ack_cursor_and_watermark_not_health() {
    for layout in LAYOUTS {
        let f = Fixture::new();
        store(&f.database(ONE), layout);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0 - TICK);
        let cursor = f.cursor_positions();
        let watermark = f.watermark();
        f.manage_payer_session();
        f.payer_call(layout, "waiting", true, CREATED);
        f.outside_call(layout, "newer", CREATED + 100);
        f.declare(vec![f.entry(ONE, true)]);
        for at in [T0, T0 + 1, T0 + 150_000] {
            f.tick(at);
            assert_eq!(f.cursor_positions(), cursor, "{layout:?}");
            assert_eq!(f.watermark(), watermark, "{layout:?}");
            assert_eq!(f.runtime(), Some((Some(at), true)));
            assert_eq!(f.degraded(), None);
        }
        let mut folder = Folder::new(f.database(ONE).parent().unwrap());
        let entry: Entry = serde_json::from_value(f.entry(ONE, true)).unwrap();
        assert!(matches!(
            read_folder(
                &mut folder,
                &entry,
                &f.database(ONE),
                &f.state,
                T0 + 150_001,
                &AtomicBool::new(false),
                0,
            )
            .unwrap(),
            FolderRead::Read {
                recorded_all: false,
                ..
            }
        ));
        f.payer_row("waiting", "metered_api");
        f.tick(T0 + 150_002);
        assert_eq!(f.watermark(), Some(T0 + 150_002 - WATERMARK_MARGIN_MS));
        assert_eq!(f.runtime(), Some((Some(T0 + 150_002), false)));
        assert_eq!(f.refs(), vec!["newer", "waiting"]);
    }
}

#[test]
fn opencode_payer_join_failed_record_keeps_wait_for_empty_fast_retry() {
    for layout in LAYOUTS {
        let f = Fixture::new();
        f.manage_payer_session();
        f.payer_call(layout, "retry", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0);
        assert!(f.refs().is_empty(), "{layout:?}");
        f.tick(T0 + 1);
        let cursor = f.cursor_positions();
        f.payer_row("retry", "metered_api");
        f.sql(
            "CREATE TRIGGER fail_payer_record BEFORE INSERT ON usage_event
               BEGIN SELECT RAISE(ABORT, 'injected record failure'); END;",
        );
        assert!(f
            .source
            .drain_fast(&f.state, T0 + 2, &AtomicBool::new(false))
            .is_err());
        assert!(f.refs().is_empty());
        assert_eq!(f.cursor_positions(), cursor);
        assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
        f.sql("DROP TRIGGER fail_payer_record;");
        f.fast_tick(T0 + 3);
        assert_eq!(f.refs(), vec!["retry"]);
        assert_eq!(f.payer_facts("retry").0, "metered_api");
    }

    let f = Fixture::new();
    f.manage_payer_session();
    f.payer_call(Layout::V1, "partial", true, CREATED);
    f.declare(vec![f.entry(ONE, false)]);
    // The v1 reader updates its in-memory wait queue after a successful ledger
    // transaction, before the v2 reader encounters bad schema.
    Connection::open(f.database(ONE))
        .unwrap()
        .execute_batch("CREATE TABLE session_v2(id TEXT);")
        .unwrap();
    assert!(f
        .source
        .drain(&f.state, T0, &AtomicBool::new(false))
        .unwrap()
        .is_some());
    assert!(f.refs().is_empty());
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
}

#[test]
fn opencode_payer_join_empty_fast_pass_never_starts_transaction_or_opens_files() {
    for layout in LAYOUTS {
        let f = Fixture::new();
        f.outside_call(layout, "recorded", CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0);
        let before = crate::state::transaction_commits_for_test();
        let health = f.runtime();
        astrocyte_readers::read_io::reset_counts();
        // A nested BEGIN fails even if the new transaction would roll back
        // unchanged. Empty fast queues must not open a transaction at all.
        f.sql("BEGIN;");
        let result = f
            .source
            .drain_fast(&f.state, T0 + 1, &AtomicBool::new(false));
        f.sql("ROLLBACK;");
        assert!(
            result.is_ok(),
            "idle fast pass started a transaction: {result:?}"
        );
        assert_eq!(crate::state::transaction_commits_for_test(), before);
        assert_eq!(
            astrocyte_readers::read_io::counts(),
            astrocyte_readers::read_io::Counts::default()
        );
        assert_eq!(f.runtime(), health);
        assert_eq!(f.refs(), vec!["recorded"]);
        f.fast_tick(T0 + 2);
        assert_eq!(crate::state::transaction_commits_for_test(), before);
    }
}
