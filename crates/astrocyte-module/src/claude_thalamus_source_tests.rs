const PAYER_COMPLETION_MS: i64 = 1_788_220_801_000;

#[test]
fn claude_payer_idle_fast_pass_starts_no_store_transaction() {
    use crate::state::transaction_attempts_for_test;

    let f = Fixture::new();
    let before = transaction_attempts_for_test();
    f.fast(PAYER_COMPLETION_MS);
    let after_source = transaction_attempts_for_test();
    let mut queue = WaitQueue::default();
    claude_code_source::record_requests_in_lane(
        &f.state,
        SOURCE_KEY,
        &[],
        &[],
        PAYER_COMPLETION_MS,
        &mut ClaudeCodeDrainOutcome::default(),
        &|| false,
        harness_drain::DrainMode::Fast,
        Some((&mut queue, &[])),
    )
    .unwrap();
    let after_recorder = transaction_attempts_for_test();
    assert_eq!(
        (after_source - before, after_recorder - after_source),
        (0, 0)
    );
}

impl Fixture {
    fn manage_payer_session(&self) {
        self.state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO thalamus_payer_session VALUES ('claude-code',?1,'owner',0)",
                    [SESSION],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
    }

    fn payer(&self, reference: &str, basis: &str) {
        self.state.ordered_write(|tx| {
            tx.execute("INSERT INTO thalamus_payer_record(attempt_id,seq,incarnation,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref) VALUES (?1,1,'first','claude-code',?2,'main',?1,0,'anthropic','claude-opus-5',?3,'vault-login','provider-account')", rusqlite::params![reference, SESSION, basis]).unwrap();
            Ok(((), true))
        }).unwrap();
    }

    fn payer_facts(&self) -> Vec<(String, String, Option<String>, Option<String>)> {
        self.state.with_conn(|conn| {
            Ok(conn.prepare("SELECT producer_ref,charge_basis,auth_selection,account_ref FROM usage_event ORDER BY producer_ref").unwrap()
                .query_map([], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().unwrap())
        }).unwrap()
    }

    fn fast(&self, at: i64) {
        self.source
            .drain_fast(&self.state, at, &AtomicBool::new(false))
            .unwrap();
    }
}

#[test]
fn claude_payer_mirrored_facts_are_stated_and_unmanaged_calls_stay_unchanged() {
    let f = Fixture::new();
    f.manage_payer_session();
    f.call(SESSION, "subscription", true);
    f.call(SESSION, "metered", true);
    f.payer("subscription", "subscription_included");
    f.payer("metered", "metered_api");
    f.declare(vec![f.root(SESSION, true)]);
    f.tick(PAYER_COMPLETION_MS);
    assert_eq!(
        f.payer_facts(),
        vec![
            (
                "metered".into(),
                "metered_api".into(),
                Some("vault-login".into()),
                Some("provider-account".into())
            ),
            (
                "subscription".into(),
                "subscription_included".into(),
                Some("vault-login".into()),
                Some("provider-account".into())
            ),
        ]
    );
    let unmanaged = Fixture::new();
    unmanaged.call(SESSION, "unmanaged", true);
    unmanaged.declare(vec![unmanaged.root(SESSION, true)]);
    unmanaged.tick(PAYER_COMPLETION_MS);
    assert_eq!(
        unmanaged.payer_facts(),
        vec![("unmanaged".into(), "unknown".into(), None, None)]
    );
    assert_eq!(unmanaged.acks()["read_through"][0]["session"], SESSION);
    let legacy = test_state();
    harness_drain::ensure_runtime_row(&legacy, SOURCE_KEY).unwrap();
    let read = claude_code_source::read_projects(
        &unmanaged.config(SESSION).join("projects"),
        &ClaudeCodeCache::default(),
        PAYER_COMPLETION_MS,
        600_000,
        Some("prefrontal-core verbatim"),
        &AtomicBool::new(false),
        harness_drain::DrainMode::Full,
    )
    .unwrap();
    claude_code_source::record_requests_in_lane(
        &legacy,
        SOURCE_KEY,
        &read.requests,
        &read.set_aside,
        PAYER_COMPLETION_MS,
        &mut ClaudeCodeDrainOutcome::default(),
        &|| false,
        harness_drain::DrainMode::Full,
        None,
    )
    .unwrap();
    crate::refresher::price_unpriced_events(&legacy, PAYER_COMPLETION_MS, &AtomicBool::new(false))
        .unwrap();
    fn snapshot(state: &SharedState) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
        state
            .with_conn(|conn| {
                ["usage_event", "measurement"]
                    .into_iter()
                    .map(|table| {
                        let mut statement = conn
                            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                            .unwrap();
                        let columns = statement.column_count();
                        statement
                            .query_map([], |row| {
                                (0..columns).map(|column| row.get(column)).collect()
                            })
                            .unwrap()
                            .collect::<rusqlite::Result<Vec<Vec<rusqlite::types::Value>>>>()
                            .map_err(|e| e.to_string())
                    })
                    .collect()
            })
            .unwrap()
    }
    assert_eq!(snapshot(&unmanaged.state), snapshot(&legacy));
}

#[test]
fn claude_payer_later_pass_records_once_in_fast_lane() {
    let f = Fixture::new();
    f.manage_payer_session();
    f.call(SESSION, "later", true);
    f.declare(vec![f.root(SESSION, true)]);
    f.tick(PAYER_COMPLETION_MS);
    assert!(f.events().is_empty());
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
    astrocyte_readers::read_io::reset_counts();
    f.fast(PAYER_COMPLETION_MS + 1);
    let idle = astrocyte_readers::read_io::counts();
    assert_eq!(idle.file_opens, 0);
    assert_eq!(idle.directory_reads, 0);
    assert_eq!(idle.connection_opens, 0);
    f.payer("later", "metered_api");
    f.state.ordered_write(|tx| {
        tx.execute_batch("CREATE TRIGGER reject_claude BEFORE INSERT ON usage_event BEGIN SELECT RAISE(ABORT,'injected recording failure'); END;").unwrap();
        Ok(((), true))
    }).unwrap();
    assert!(f
        .source
        .drain_fast(&f.state, PAYER_COMPLETION_MS + 2, &AtomicBool::new(false))
        .is_err());
    assert!(f.events().is_empty());
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
    f.state
        .ordered_write(|tx| {
            tx.execute_batch("DROP TRIGGER reject_claude").unwrap();
            Ok(((), true))
        })
        .unwrap();
    f.fast(PAYER_COMPLETION_MS + 2);
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&0));
    f.tick(PAYER_COMPLETION_MS + 3);
    assert_eq!(
        f.payer_facts(),
        vec![(
            "later".into(),
            "metered_api".into(),
            Some("vault-login".into()),
            Some("provider-account".into())
        )]
    );
}

#[test]
fn claude_payer_timeout_uses_completion_not_stream_start() {
    let f = Fixture::new();
    f.manage_payer_session();
    f.call(SESSION, "never", true);
    let path = f
        .config(SESSION)
        .join("projects/encoded-cwd/session/subagents/never.jsonl");
    let finish = fs::read_to_string(&path).unwrap();
    let mut start: Value = serde_json::from_str(finish.trim()).unwrap();
    start["timestamp"] = json!("2026-08-31T23:00:01.000Z");
    start["message"]["stop_reason"] = Value::Null;
    fs::write(path, format!("{start}\n{finish}")).unwrap();
    f.declare(vec![f.root(SESSION, true)]);
    f.tick(PAYER_COMPLETION_MS);
    assert!(f.events().is_empty());
    f.fast(PAYER_COMPLETION_MS + 599_999);
    assert!(f.events().is_empty());
    f.fast(PAYER_COMPLETION_MS + 600_000);
    assert_eq!(
        f.payer_facts(),
        vec![("never".into(), "unknown".into(), None, None)]
    );
    f.fast(PAYER_COMPLETION_MS + 600_001);
    f.tick(PAYER_COMPLETION_MS + 600_002);
    assert_eq!(f.events().len(), 1);
    f.state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT occurred_at_ms FROM usage_event", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                PAYER_COMPLETION_MS - 3_600_000
            );
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind='without_payer'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                1
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn claude_payer_wait_survives_reader_restart_and_records_once() {
    let mut f = Fixture::new();
    f.manage_payer_session();
    f.call(SESSION, "restart", true);
    f.declare(vec![f.root(SESSION, true)]);
    f.tick(PAYER_COMPLETION_MS);
    assert!(f.events().is_empty());
    let home = fs::canonicalize(f.temp.path()).unwrap();
    f.source = ClaudeThalamusSource::from_data_home(home);
    f.tick(PAYER_COMPLETION_MS + 1);
    assert!(f.events().is_empty());
    assert_eq!(f.acks()["read_through"], json!([]));
    f.payer("restart", "subscription_included");
    f.tick(PAYER_COMPLETION_MS + 2);
    f.tick(PAYER_COMPLETION_MS + 3);
    assert_eq!(
        f.payer_facts(),
        vec![(
            "restart".into(),
            "subscription_included".into(),
            Some("vault-login".into()),
            Some("provider-account".into())
        )]
    );
}

#[test]
fn claude_payer_wait_holds_deletion_ack_but_keeps_health_current() {
    let f = Fixture::new();
    f.call(SESSION, "old", true);
    f.declare(vec![f.root(SESSION, true)]);
    f.tick(PAYER_COMPLETION_MS - 1);
    assert_eq!(f.runtime(), Some((Some(PAYER_COMPLETION_MS - 1), false)));
    assert_eq!(f.acks()["read_through"][0]["session"], SESSION);
    f.manage_payer_session();
    f.call(SESSION, "waiting", true);
    f.tick(PAYER_COMPLETION_MS);
    assert_eq!(f.acks()["read_through"], json!([]));
    assert_eq!(f.runtime(), Some((Some(PAYER_COMPLETION_MS), false)));
    assert_eq!(f.events().len(), 1);
    f.tick(PAYER_COMPLETION_MS + 1);
    assert_eq!(f.runtime(), Some((Some(PAYER_COMPLETION_MS + 1), false)));
    f.payer("waiting", "metered_api");
    f.tick(PAYER_COMPLETION_MS + 2);
    assert_eq!(f.runtime(), Some((Some(PAYER_COMPLETION_MS + 2), false)));
    assert_eq!(
        f.acks()["read_through"][0]["at_ms"],
        PAYER_COMPLETION_MS + 2
    );
    assert_eq!(f.events().len(), 2);
}
