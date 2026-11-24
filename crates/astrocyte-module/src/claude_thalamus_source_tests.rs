const PAYER_COMPLETION_MS: i64 = 1_788_220_801_000;
const OWNER_SESSION: &str = "ses_core_bound_conversation";

#[test]
fn recorded_claude_refusal_and_retry_join_payer_rows_and_acknowledge_done_once() {
    use crate::refresher::thalamus_payer_source::ThalamusPayerSource;
    use crate::test_support::thalamus_payer::{insert_recorded_row, open_store};
    use sha2::{Digest, Sha256};

    const FIRST_DRAIN_MS: i64 = 1_795_516_046_200;
    const OWNER: &str = "cc-payer-refusal-sample";
    const REFUSED: &str = "req_sample_refused";
    const RETRY: &str = "req_sample_retry";
    let vector = Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/vectors/thalamus-payer-refusal-claude-code-v1"
    ));
    let manifest = fs::read_to_string(vector.join("SHA256SUMS")).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(manifest.as_bytes())),
        "6d78c751a92a376c4ff6c714682f3ea02df27d5c8df8dfd83d14fb06d7aaa70d"
    );
    let f = Fixture::with_state(test_state());
    let mut roots: Value =
        serde_json::from_slice(&fs::read(vector.join("claude-session/roots.json")).unwrap())
            .unwrap();
    let session = roots["roots"][0]["session"].as_str().unwrap().to_owned();
    let config = f.config(&session);
    for line in manifest.lines() {
        let (expected, file) = line.split_once("  ").unwrap();
        let bytes = fs::read(vector.join(file)).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), expected, "{file}");
        if let Some(relative) = file.strip_prefix("claude-session/config/") {
            let dest = config.join(relative);
            fs::create_dir_all(dest.parent().unwrap()).unwrap();
            fs::write(dest, bytes).unwrap();
        }
    }
    assert_eq!(roots["roots"][0]["state"], "active");
    roots["roots"][0]["path"] = json!(config);
    fs::write(&f.roots, roots.to_string()).unwrap();

    let home = fs::canonicalize(f.temp.path()).unwrap();
    let store = open_store(&home);
    let payer = ThalamusPayerSource::from_data_home(home);
    let captured: Value =
        serde_json::from_slice(&fs::read(vector.join("payer-rows.json")).unwrap()).unwrap();
    let declaration = &captured["payer_session"][0];
    store
        .execute(
            "INSERT INTO payer_session(harness,session,owner_principal,declared_at_ms) \
             VALUES (?1,?2,?3,?4)",
            rusqlite::params![
                declaration["harness"].as_str(),
                declaration["session"].as_str(),
                declaration["owner_principal"].as_str(),
                declaration["declared_at_ms"].as_i64(),
            ],
        )
        .unwrap();
    let stop = AtomicBool::new(false);
    let drain = |source: &dyn HarnessSource, at| {
        assert_eq!(source.drain_for_health(&f.state, at, &stop).unwrap(), None);
    };

    // Let the refusal's payer arrive first so the retry must wait for its own row.
    insert_recorded_row(&store, &captured["payer_record"][0]);
    drain(&payer, FIRST_DRAIN_MS);
    drain(&f.source, FIRST_DRAIN_MS);
    let refused_only = [(REFUSED.to_owned(), OWNER.to_owned())];
    assert_eq!(f.event_sessions(), refused_only);
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
    assert_eq!(f.acks()["read_through"], json!([]));

    // The capture is active. Mark only the working copy done while the retry is pending.
    let done_at = FIRST_DRAIN_MS + 1;
    roots["roots"][0]["state"] = json!("done");
    roots["roots"][0]["done_at_ms"] = json!(done_at);
    fs::write(&f.roots, roots.to_string()).unwrap();
    drain(&f.source, done_at);
    assert_eq!(f.event_sessions(), refused_only);
    assert_eq!(f.acks()["read_through"], json!([]));

    let joined_at = done_at + 1;
    insert_recorded_row(&store, &captured["payer_record"][1]);
    drain(&payer, joined_at);
    drain(&f.source, joined_at);
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&0));
    assert_eq!(
        f.acks(),
        json!({"version":1,"read_through":[{"session":session,"at_ms":joined_at}]})
    );
    let ack = fs::read(&f.ack).unwrap();
    drain(&payer, joined_at + 1);
    drain(&f.source, joined_at + 1);
    assert_eq!(fs::read(&f.ack).unwrap(), ack);

    assert_eq!(
        f.event_sessions(),
        [REFUSED, RETRY].map(|request| (request.to_owned(), OWNER.to_owned()))
    );
    assert_eq!(
        f.payer_facts(),
        [REFUSED, RETRY].map(|request| (
            request.to_owned(),
            "unknown".to_owned(),
            Some("oauth:anthropic:two".to_owned()),
            Some("acct-2".to_owned())
        ))
    );
    f.state
        .with_conn(|conn| {
            for table in [
                "rejected_delivery",
                "set_aside_record",
                "thalamus_payer_coverage",
            ] {
                let count: i64 = conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, 0, "{table}");
            }
            let mut measurements = conn
                .prepare(
                    "SELECT role, unit, quantity_units, quantity_exponent FROM measurement \
                     JOIN usage_event USING(event_key) WHERE producer_ref=?1 ORDER BY role, unit",
                )
                .unwrap();
            let mut retry_writes = Vec::new();
            for (request, stop_reason, cache_read) in
                [(REFUSED, "refusal", 12), (RETRY, "end_turn", 0)]
            {
                let event = conn
                    .query_row(
                        "SELECT vendor_request_id, provider, model, terminal_status, resolved_credential \
                         FROM usage_event WHERE producer_ref=?1",
                        [request],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                                r.get::<_, String>(3)?,
                                r.get::<_, Option<String>>(4)?,
                            ))
                        },
                    )
                    .unwrap();
                // The payer rows name the selected login, not a separate resolved credential.
                assert_eq!(
                    event,
                    (
                        request.to_owned(),
                        "anthropic".to_owned(),
                        "claude-fable-5-2".to_owned(),
                        stop_reason.to_owned(),
                        None,
                    )
                );
                let (writes, other): (Vec<_>, Vec<_>) = measurements
                    .query_map([request], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, i64>(3)?,
                        ))
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .partition(|(role, ..)| role == "cache_write");
                assert_eq!(
                    other,
                    [("cache_read", cache_read), ("input", 17), ("output", 6)]
                        .map(|(role, quantity)| (role.to_owned(), "token".to_owned(), quantity, 0)),
                    "{request}"
                );
                assert!(
                    writes.iter().all(|(_, _, _, exponent)| *exponent == 0),
                    "{request}: {writes:?}"
                );
                if request == REFUSED {
                    assert_eq!(
                        writes.iter().map(|(_, _, quantity, _)| quantity).sum::<i64>(),
                        0
                    );
                } else {
                    retry_writes = writes;
                }
            }
            assert_eq!(
                retry_writes
                    .iter()
                    .map(|(_, _, quantity, _)| quantity)
                    .sum::<i64>(),
                12,
                "{RETRY} cache-write measurements (role, unit, quantity, exponent): {retry_writes:?}"
            );
            Ok(())
        })
        .unwrap();
}

impl Fixture {
    fn live_call(&self, request: &str) {
        self.call(SESSION, request, true);
        let path = self.config(SESSION).join(format!(
            "projects/encoded-cwd/session/subagents/{request}.jsonl"
        ));
        let mut record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        // The feed retains only calls from the last day, unlike the ledger.
        record["timestamp"] = json!(chrono::Utc::now().to_rfc3339());
        fs::write(path, format!("{record}\n")).unwrap();
    }

    fn owned_root(&self, done: bool) -> Value {
        let mut root = self.root(SESSION, done);
        root["owner_session"] = json!(OWNER_SESSION);
        root
    }

    fn event_sessions(&self) -> Vec<(String, String)> {
        self.state
            .with_conn(|conn| {
                Ok(conn
                    .prepare("SELECT producer_ref,session FROM usage_event ORDER BY producer_ref")
                    .unwrap()
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap())
            })
            .unwrap()
    }

    fn feed_rows(&self) -> Value {
        let page = crate::step_feed::read(&self.state, json!({}))
            .unwrap_or_else(|error| panic!("{}: {}", error.code, error.message));
        serde_json::to_value(page).unwrap()["rows"].clone()
    }
}

#[test]
fn claude_owner_session_is_recorded_in_ledger_and_step_feed() {
    for (root_has_owner, expected) in [(true, OWNER_SESSION), (false, SESSION)] {
        let f = Fixture::new();
        f.live_call("one");
        f.declare(vec![if root_has_owner {
            f.owned_root(true)
        } else {
            f.root(SESSION, true)
        }]);
        f.tick(crate::state::now_ms());
        assert_eq!(f.event_sessions(), vec![("one".into(), expected.into())]);
        assert_eq!(
            f.events(),
            vec![(
                "one".into(),
                "claude-code".into(),
                Some("prefrontal-core verbatim".into())
            )]
        );
        let rows = f.feed_rows();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["session"], expected);
        assert_eq!(rows[0]["harness"], "claude-code");
        assert_eq!(rows[0]["step_ref"], "one");
        assert_eq!(f.acks()["read_through"][0]["session"], SESSION);
    }
}

#[test]
fn claude_owner_session_refuses_empty_or_wrong_type() {
    for value in [
        json!(""),
        json!(null),
        json!(42),
        json!(false),
        json!([]),
        json!({}),
    ] {
        let f = Fixture::new();
        f.call(SESSION, "unreadable", true);
        let mut root = f.root(SESSION, true);
        root["owner_session"] = value.clone();
        f.declare(vec![root]);
        f.tick(PAYER_COMPLETION_MS);
        assert!(f.events().is_empty(), "owner_session {value}");
        assert!(f.feed_rows().as_array().unwrap().is_empty());
        assert_eq!(f.runtime(), Some((Some(PAYER_COMPLETION_MS), true)));
        assert!(!f.ack.exists());
    }
}

#[test]
fn claude_owner_session_appearing_later_preserves_recorded_calls() {
    for managed in [false, true] {
        let mut f = Fixture::new();
        if managed {
            f.manage_payer_session();
            f.payer("old", "metered_api");
            f.payer("new", "metered_api");
        }
        f.live_call("old");
        f.declare(vec![f.root(SESSION, true)]);
        let at = crate::state::now_ms();
        f.tick(at);
        assert_eq!(f.event_sessions(), vec![("old".into(), SESSION.into())]);
        f.state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT runner FROM usage_event", [], |r| r
                        .get::<_, String>(0))
                        .unwrap(),
                    "thalamus"
                );
                Ok(())
            })
            .unwrap();
        f.declare(vec![f.owned_root(true)]);
        f.live_call("new");
        // A fresh reader must redeliver the old call, not skip it via file offsets.
        f.source = ClaudeThalamusSource::from_data_home(fs::canonicalize(f.temp.path()).unwrap());
        f.tick(at + 1);
        assert_eq!(
            f.event_sessions(),
            vec![
                ("new".into(), OWNER_SESSION.into()),
                ("old".into(), SESSION.into())
            ]
        );
        let rows = f.feed_rows();
        assert_eq!(rows.as_array().unwrap().len(), 2);
        assert_eq!(rows[0]["session"], SESSION);
        assert_eq!(rows[1]["session"], OWNER_SESSION);
        f.state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |row| row
                        .get::<_, i64>(0))
                        .unwrap(),
                    0
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(f.runtime(), Some((Some(at + 1), false)));
        assert_eq!(f.acks()["read_through"][0]["session"], SESSION);
    }
}

#[test]
fn claude_owner_session_updates_unrecorded_waiting_calls() {
    for (initial_owner, final_owner, expected) in [
        (None, Some(OWNER_SESSION), OWNER_SESSION),
        (Some(OWNER_SESSION), None, SESSION),
        (Some(OWNER_SESSION), Some("ses_rebound"), "ses_rebound"),
    ] {
        let f = Fixture::new();
        f.manage_payer_session();
        f.call(SESSION, "waiting", true);
        let mut root = f.root(SESSION, true);
        if let Some(owner) = initial_owner {
            root["owner_session"] = json!(owner);
        }
        f.declare(vec![root]);
        f.tick(PAYER_COMPLETION_MS);
        assert!(f.events().is_empty());
        assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
        assert_eq!(f.acks()["read_through"], json!([]));
        let mut root = f.root(SESSION, true);
        if let Some(owner) = final_owner {
            root["owner_session"] = json!(owner);
        }
        f.declare(vec![root]);
        f.payer("waiting", "metered_api");
        f.tick(PAYER_COMPLETION_MS + 1);
        assert_eq!(
            f.event_sessions(),
            vec![("waiting".into(), expected.into())]
        );
        assert_eq!(
            f.payer_facts(),
            vec![(
                "waiting".into(),
                "metered_api".into(),
                Some("vault-login".into()),
                Some("provider-account".into())
            )]
        );
        assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&0));
        assert_eq!(f.acks()["read_through"][0]["session"], SESSION);
    }
}

#[test]
fn claude_owner_session_keeps_native_payer_join_and_deletion_ack() {
    let f = Fixture::new();
    f.manage_payer_session();
    f.call(SESSION, "waiting", true);
    f.declare(vec![f.owned_root(true)]);
    f.tick(PAYER_COMPLETION_MS);
    assert!(f.events().is_empty());
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&1));
    assert_eq!(f.acks()["read_through"], json!([]));
    f.payer("waiting", "subscription_included");
    f.fast(PAYER_COMPLETION_MS + 1);
    assert_eq!(
        f.event_sessions(),
        vec![("waiting".into(), OWNER_SESSION.into())]
    );
    assert_eq!(
        f.payer_facts(),
        vec![(
            "waiting".into(),
            "subscription_included".into(),
            Some("vault-login".into()),
            Some("provider-account".into())
        )]
    );
    f.tick(PAYER_COMPLETION_MS + 2);
    assert_eq!(
        f.acks()["read_through"],
        json!([{"session":SESSION,"at_ms":PAYER_COMPLETION_MS + 2}])
    );
    assert_eq!(f.state.payer_waiting().get(SOURCE_KEY), Some(&0));
}

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
fn claude_payer_mirrored_facts_are_stated_and_other_call_facts_stay_unchanged() {
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
    let without_payer = Fixture::new();
    without_payer.call(SESSION, "unmanaged", true);
    without_payer.declare(vec![without_payer.root(SESSION, true)]);
    without_payer.tick(PAYER_COMPLETION_MS);
    assert_eq!(
        without_payer.payer_facts(),
        vec![("unmanaged".into(), "unknown".into(), None, None)]
    );
    assert_eq!(without_payer.acks()["read_through"][0]["session"], SESSION);
    let legacy = test_state();
    harness_drain::ensure_runtime_row(&legacy, SOURCE_KEY).unwrap();
    let read = claude_code_source::read_projects(
        &without_payer.config(SESSION).join("projects"),
        &ClaudeCodeCache::default(),
        PAYER_COMPLETION_MS,
        600_000,
        Some("prefrontal-core verbatim"),
        &AtomicBool::new(false),
        harness_drain::DrainMode::Full,
        false,
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
    for transaction in read.transactions {
        transaction.commit();
    }
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
                        // Thalamus-declared sessions now record their runner even without a
                        // payer row. All earlier call facts must still agree.
                        let columns: Vec<_> = (0..statement.column_count())
                            .filter(|column| statement.column_name(*column).unwrap() != "runner")
                            .collect();
                        statement
                            .query_map([], |row| {
                                columns.iter().map(|column| row.get(*column)).collect()
                            })
                            .unwrap()
                            .collect::<rusqlite::Result<Vec<Vec<rusqlite::types::Value>>>>()
                            .map_err(|e| e.to_string())
                    })
                    .collect()
            })
            .unwrap()
    }
    for (state, runner) in [
        (without_payer.state.as_ref(), Some("thalamus")),
        (&legacy, None),
    ] {
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT runner FROM usage_event", [], |r| r
                        .get::<_, Option<String>>(0))
                        .unwrap()
                        .as_deref(),
                    runner
                );
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(snapshot(&without_payer.state), snapshot(&legacy));
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
