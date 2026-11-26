// Payer-join regressions share the fixtures in opencode_thalamus_source.

fn recorded_cache_split_row(name: &str) -> Value {
    use sha2::{Digest, Sha256};

    const MANIFEST: &[u8] =
        include_bytes!("../tests/vectors/thalamus-payer-cache-split-v1/SHA256SUMS");
    let rows: [(&str, &[u8]); 3] = [
        (
            "split.json",
            include_bytes!("../tests/vectors/thalamus-payer-cache-split-v1/split.json"),
        ),
        (
            "total-only.json",
            include_bytes!("../tests/vectors/thalamus-payer-cache-split-v1/total-only.json"),
        ),
        (
            "one-part-only.json",
            include_bytes!("../tests/vectors/thalamus-payer-cache-split-v1/one-part-only.json"),
        ),
    ];
    assert_eq!(
        format!("{:x}", Sha256::digest(MANIFEST)),
        "0da88caaed8de7c821f3b8ca7ac13d05b43cadcd6afc0688afcd393cd9b153e5"
    );
    let hashes: String = rows
        .iter()
        .map(|(file, bytes)| format!("{:x}  {file}\n", Sha256::digest(bytes)))
        .collect();
    assert_eq!(hashes.as_bytes(), MANIFEST);
    let (_, bytes) = rows.iter().find(|(file, _)| *file == name).unwrap();
    serde_json::from_slice(bytes).unwrap()
}

impl Fixture {
    fn recorded_cache_split_payer(&self, row: &Value) {
        const COLUMNS: [&str; 16] = [
            "seq",
            "attempt_id",
            "harness",
            "session",
            "request_kind",
            "step_ref",
            "recorded_at_ms",
            "provider",
            "model",
            "charge_basis",
            "auth_selection",
            "account_ref",
            "outcome",
            "provider_request_id",
            "provider_response_id",
            "usage_json",
        ];
        assert_eq!(row.as_object().unwrap().len(), COLUMNS.len());
        // Preserve every stored column, including SQL NULL and the original
        // usage_json text, so the reader sees the recorded row rather than a reconstruction.
        let values = COLUMNS.map(|column| match &row[column] {
            Value::Null => rusqlite::types::Value::Null,
            Value::Number(number) => rusqlite::types::Value::Integer(number.as_i64().unwrap()),
            Value::String(text) => rusqlite::types::Value::Text(text.clone()),
            other => panic!("unexpected payer column {column}: {other}"),
        });
        let home = fs::canonicalize(self.temp.path()).unwrap();
        let path = home.join("cortexkit/thalamus-harness/payer/payer.db");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = Connection::open(path).unwrap();
        store.execute_batch("CREATE TABLE payer_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO payer_meta VALUES ('schema_version','1'),('incarnation','first');
            CREATE TABLE payer_session(harness TEXT,session TEXT,owner_principal TEXT,declared_at_ms INTEGER,PRIMARY KEY(harness,session));
            CREATE TABLE payer_record(seq INTEGER PRIMARY KEY AUTOINCREMENT,attempt_id TEXT UNIQUE,harness TEXT,session TEXT,request_kind TEXT,step_ref TEXT,recorded_at_ms INTEGER,provider TEXT,model TEXT,charge_basis TEXT,auth_selection TEXT,account_ref TEXT,outcome TEXT,provider_request_id TEXT,provider_response_id TEXT,usage_json TEXT);").unwrap();
        store
            .execute(
                "INSERT INTO payer_session VALUES (?1,?2,?3,0)",
                params![
                    row["harness"].as_str().unwrap(),
                    row["session"].as_str().unwrap(),
                    OWNER
                ],
            )
            .unwrap();
        store.execute(
            &format!("INSERT INTO payer_record ({}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)", COLUMNS.join(",")),
            rusqlite::params_from_iter(values),
        ).unwrap();
        assert_eq!(
            crate::refresher::thalamus_payer_source::ThalamusPayerSource::from_data_home(home)
                .drain_for_health(&self.state, T0, &AtomicBool::new(false))
                .unwrap(),
            None
        );
    }

    fn cache_split_payer(&self, attempt: &str, step: &str, usage: Option<&str>) {
        let home = fs::canonicalize(self.temp.path()).unwrap();
        let path = home.join("cortexkit/thalamus-harness/payer/payer.db");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = Connection::open(path).unwrap();
        store.execute_batch("CREATE TABLE IF NOT EXISTS payer_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT OR IGNORE INTO payer_meta VALUES ('schema_version','1'),('incarnation','first');
            CREATE TABLE IF NOT EXISTS payer_session(harness TEXT,session TEXT,owner_principal TEXT,declared_at_ms INTEGER,PRIMARY KEY(harness,session));
            CREATE TABLE IF NOT EXISTS payer_record(seq INTEGER PRIMARY KEY AUTOINCREMENT,attempt_id TEXT UNIQUE,harness TEXT,session TEXT,request_kind TEXT,step_ref TEXT,recorded_at_ms INTEGER,provider TEXT,model TEXT,charge_basis TEXT,auth_selection TEXT,account_ref TEXT,outcome TEXT,provider_request_id TEXT,provider_response_id TEXT,usage_json TEXT);").unwrap();
        store
            .execute(
                "INSERT OR IGNORE INTO payer_session VALUES ('opencode','session',?1,0)",
                [OWNER],
            )
            .unwrap();
        store.execute("INSERT INTO payer_record(attempt_id,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref,usage_json) VALUES (?1,'opencode','session','main',?2,?3,'anthropic','claude-sonnet-4','metered_api','vault-login','provider-account',?4)", params![attempt,step,T0,usage]).unwrap();
        crate::refresher::thalamus_payer_source::ThalamusPayerSource::from_data_home(home)
            .drain_for_health(&self.state, T0, &AtomicBool::new(false))
            .unwrap();
    }

    fn cache_writes(&self) -> Vec<(String, String, i64, i64, String)> {
        self.state.with_conn(|conn| {
            conn.prepare("SELECT unit,source,quantity_units,quantity_exponent,kind FROM measurement WHERE role='cache_write' ORDER BY unit").unwrap()
                .query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().map_err(|e| e.to_string())
        }).unwrap()
    }

    fn payer_coverage_count(&self, kind: &str) -> i64 {
        self.state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind=?1",
                    [kind],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

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

struct CacheLifetimeCatalog;

impl CatalogConnection for CacheLifetimeCatalog {
    fn catalog_status(&self, request: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
        DollarCatalog.catalog_status(request)
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        let mut response = serde_json::to_value(DollarCatalog.catalog_get(request)?).unwrap();
        for (key, units) in [
            ("rate.cache_write", 1_250_000_000_i64),
            ("rate.cache_write.ttl.5m", 1_250_000_000),
            ("rate.cache_write.ttl.1h", 2_000_000_000),
        ] {
            response["models"]["anthropic/claude-sonnet-4"][key] = json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
        }
        Ok(serde_json::from_value(response).unwrap())
    }
}

fn split_writes(five: i64, hour: i64) -> Vec<(String, String, i64, i64, String)> {
    vec![
        (
            "token_cache_1h".into(),
            "thalamus-payer.cache_write_1h_tokens".into(),
            hour,
            0,
            "billed".into(),
        ),
        (
            "token_cache_5m".into(),
            "thalamus-payer.cache_write_5m_tokens".into(),
            five,
            0,
            "billed".into(),
        ),
    ]
}

fn assert_recorded_cache_split_matches(
    name: &str,
    expected: Vec<(String, String, i64, i64, String)>,
) {
    let row = recorded_cache_split_row(name);
    assert_eq!(row["harness"], "opencode");
    let session = row["session"].as_str().unwrap();
    let step = row["step_ref"].as_str().unwrap();
    for layout in LAYOUTS {
        let f = Fixture::new();
        let mut data = payload(layout, true);
        data["time"]["completed"] = json!(T0);
        data["tokens"]["cache"]["write"] = json!(24_576);
        match layout {
            Layout::V1 => data["modelID"] = row["model"].clone(),
            Layout::V2 => data["model"]["id"] = row["model"].clone(),
        }
        let transcript = store(&f.database(session), layout);
        match layout {
            Layout::V1 => transcript.execute(
                "INSERT INTO message VALUES (?1,?2,?3,?3,?4)",
                params![step, session, CREATED, data.to_string()],
            ),
            Layout::V2 => transcript.execute(
                "INSERT INTO session_message VALUES (?1,?2,'assistant',1,?3,?3,?4)",
                params![step, session, CREATED, data.to_string()],
            ),
        }
        .unwrap();
        drop(transcript);
        f.declare(vec![f.entry(session, false)]);
        f.recorded_cache_split_payer(&row);
        f.tick(T0);
        assert_eq!(f.refs(), vec![step], "{name}: {layout:?}");
        assert_eq!(f.cache_writes(), expected, "{name}: {layout:?}");
        assert_eq!(
            f.payer_facts(step),
            (
                "subscription_included".into(),
                Some("synthetic-vault-anthropic-subscription".into()),
                Some("synthetic-anthropic-account".into()),
            ),
            "{name}: {layout:?}"
        );
        assert_eq!(f.payer_coverage_count("cache_split_mismatch"), 0);
        assert_eq!(f.payer_coverage_count("set_aside"), 0);
        assert_eq!(f.payer_coverage_count("without_payer"), 0);
        assert_eq!(f.degraded(), None);
    }
}

#[test]
fn opencode_payer_cache_split_recorded_split_replaces_plain_total() {
    assert_recorded_cache_split_matches("split.json", split_writes(8_192, 16_384));
}

#[test]
fn opencode_payer_cache_split_recorded_total_only_keeps_plain_total() {
    assert_recorded_cache_split_matches(
        "total-only.json",
        vec![(
            "token".into(),
            "usage.cacheWrite".into(),
            24_576,
            0,
            "billed".into(),
        )],
    );
}

#[test]
fn opencode_payer_cache_split_recorded_one_part_only_keeps_plain_total() {
    assert_recorded_cache_split_matches(
        "one-part-only.json",
        vec![(
            "token".into(),
            "usage.cacheWrite".into(),
            24_576,
            0,
            "billed".into(),
        )],
    );
}

#[test]
fn opencode_payer_cache_split_records_lifetimes_and_prices_hour() {
    for layout in LAYOUTS {
        let f = Fixture::new();
        f.state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(CacheLifetimeCatalog)).unwrap(),
            ))
            .unwrap();
        f.payer_call(layout, "split", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.cache_split_payer(
            "attempt",
            "split",
            Some(r#"{"cache_write_5m_tokens":0,"cache_write_1h_tokens":11}"#),
        );
        f.tick(T0);
        assert_eq!(f.cache_writes(), split_writes(0, 11));
        assert_eq!(f.payer_coverage_count("set_aside"), 0);
        assert_eq!(f.payer_coverage_count("cache_split_mismatch"), 0);
        f.state.with_conn(|conn| {
            let money = conn.prepare("SELECT unit,charge_nanos FROM priced_charge WHERE role='cache_write' ORDER BY unit").unwrap()
                .query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
            assert_eq!(money, vec![("token_cache_1h".into(),22_000),("token_cache_5m".into(),0)]);
            assert_eq!(conn.query_row("SELECT COUNT(*) FROM measurement WHERE role<>'cache_write'", [], |r| r.get::<_,i64>(0)).unwrap(), 4);
            Ok(())
        }).unwrap();
        assert_eq!(f.degraded(), None);
    }
}

#[test]
fn opencode_payer_cache_split_mismatch_keeps_total_and_counts_coverage() {
    for (usage, missing_total) in [
        (
            r#"{"cache_write_5m_tokens":3,"cache_write_1h_tokens":7}"#,
            false,
        ),
        (
            r#"{"cache_write_5m_tokens":0,"cache_write_1h_tokens":0}"#,
            true,
        ),
        (
            r#"{"cache_write_5m_tokens":18446744073709551615,"cache_write_1h_tokens":1}"#,
            false,
        ),
    ] {
        let f = Fixture::new();
        let mut data = payload(Layout::V2, true);
        data["time"]["completed"] = json!(T0);
        if missing_total {
            data["tokens"]["cache"]
                .as_object_mut()
                .unwrap()
                .remove("write");
        }
        insert(
            &f.database(ONE),
            Layout::V2,
            "mismatch",
            "assistant",
            CREATED,
            &data,
        );
        f.declare(vec![f.entry(ONE, false)]);
        f.cache_split_payer("attempt", "mismatch", Some(usage));
        f.tick(T0);
        assert_eq!(f.refs(), vec!["mismatch"]);
        assert_eq!(f.payer_facts("mismatch").0, "metered_api");
        assert_eq!(
            f.cache_writes(),
            if missing_total {
                vec![]
            } else {
                vec![(
                    "token".into(),
                    "usage.cacheWrite".into(),
                    11,
                    0,
                    "billed".into(),
                )]
            }
        );
        f.tick(T0 + TICK);
        assert_eq!(f.payer_coverage_count("cache_split_mismatch"), 1);
        assert_eq!(f.payer_coverage_count("set_aside"), 0);
        f.state
            .with_conn(|conn| {
                let report = astrocyte_core::report::ledger_coverage(
                    conn,
                    &astrocyte_core::report::LedgerCoverageRequest {},
                    T0,
                )
                .unwrap();
                assert_eq!(
                    serde_json::to_value(report).unwrap()["thalamus_payer"]
                        ["cache_split_mismatches"],
                    json!(1)
                );
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |r| r
                        .get::<_, i64>(0))
                        .unwrap(),
                    0
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(f.degraded(), None);
    }
}

#[test]
fn opencode_payer_cache_split_empty_preserves_total_and_malformed_stays_refused() {
    for usage in [
        None,
        Some(r#"{"cache_write_5m_tokens":4,"cache_write_1h_tokens":7,"extra":0}"#),
        Some(r#"{"cache_write_5m_tokens":11}"#),
        Some(r#"{"cache_write_1h_tokens":11}"#),
        Some(r#"{"cache_write_5m_tokens":-1,"cache_write_1h_tokens":12}"#),
        Some(r#"{"cache_write_5m_tokens":0.0,"cache_write_1h_tokens":11}"#),
        Some(r#"{"cache_write_5m_tokens":0,"cache_write_1h_tokens":null}"#),
        Some(r#"{"cache_write_5m_tokens":0,"cache_write_5m_tokens":0,"cache_write_1h_tokens":11}"#),
        Some(""),
        Some("{}"),
        Some("[0,11]"),
        Some(r#"{"usage":{"cache_write_tokens":11}}"#),
    ] {
        let f = Fixture::new();
        f.payer_call(Layout::V2, "plain", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.cache_split_payer("attempt", "plain", usage);
        f.tick(T0);
        if usage.is_some() {
            assert!(f.refs().is_empty());
        }
        f.tick(T0 + 600_000);
        assert_eq!(
            f.cache_writes(),
            vec![(
                "token".into(),
                "usage.cacheWrite".into(),
                11,
                0,
                "billed".into()
            )]
        );
        assert_eq!(
            f.payer_facts("plain").0,
            if usage.is_some() {
                "unknown"
            } else {
                "metered_api"
            }
        );
        assert_eq!(
            f.payer_coverage_count("set_aside"),
            i64::from(usage.is_some())
        );
        assert_eq!(f.payer_coverage_count("cache_split_mismatch"), 0);
    }
}

#[test]
fn opencode_payer_cache_split_non_anthropic_uses_matching_parts() {
    let f = Fixture::new();
    let mut data = payload(Layout::V2, true);
    data["model"]["providerID"] = json!("openai");
    data["model"]["id"] = json!("gpt-4.1");
    insert(
        &f.database(ONE),
        Layout::V2,
        "openai",
        "assistant",
        CREATED,
        &data,
    );
    f.declare(vec![f.entry(ONE, false)]);
    f.cache_split_payer(
        "attempt",
        "openai",
        Some(r#"{"cache_write_5m_tokens":4,"cache_write_1h_tokens":7}"#),
    );
    f.tick(T0);
    assert_eq!(f.cache_writes(), split_writes(4, 7));
    f.state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT provider FROM usage_event", [], |r| r
                    .get::<_, String>(0))
                    .unwrap(),
                "openai"
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn opencode_payer_cache_split_late_row_never_changes_recorded_call() {
    let mut f = Fixture::new();
    f.payer_call(Layout::V2, "recorded", true, CREATED);
    f.declare(vec![f.entry(ONE, false)]);
    f.cache_split_payer("first", "recorded", None);
    f.tick(T0);
    let event = f.event_bytes("recorded");
    let writes = f.cache_writes();
    f.cache_split_payer(
        "late",
        "recorded",
        Some(r#"{"cache_write_5m_tokens":0,"cache_write_1h_tokens":11}"#),
    );
    f.tick(T0 + TICK);
    f.restart();
    f.tick(T0 + 2 * TICK);
    assert_eq!(f.event_bytes("recorded"), event);
    assert_eq!(f.cache_writes(), writes);
    assert_eq!(f.payer_coverage_count("late"), 1);
    assert_eq!(f.payer_coverage_count("set_aside"), 0);
    assert_eq!(f.refs(), vec!["recorded"]);
}

#[test]
fn opencode_payer_cache_split_siblings_must_agree_but_absence_is_not_disagreement() {
    let split = Some(r#"{"cache_write_5m_tokens":4,"cache_write_1h_tokens":7}"#);
    let other = Some(r#"{"cache_write_5m_tokens":3,"cache_write_1h_tokens":8}"#);
    for (first, second, mismatch) in [
        (split, split, false),
        (None, split, false),
        (split, None, false),
        (split, other, true),
    ] {
        let f = Fixture::new();
        f.payer_call(Layout::V2, "siblings", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.cache_split_payer("first", "siblings", first);
        f.cache_split_payer("second", "siblings", second);
        f.tick(T0);
        assert_eq!(
            f.cache_writes(),
            if mismatch {
                vec![(
                    "token".into(),
                    "usage.cacheWrite".into(),
                    11,
                    0,
                    "billed".into(),
                )]
            } else {
                split_writes(4, 7)
            }
        );
        assert_eq!(f.payer_facts("siblings").0, "metered_api");
        assert_eq!(
            f.payer_coverage_count("cache_split_mismatch"),
            i64::from(mismatch)
        );
        assert_eq!(f.payer_coverage_count("set_aside"), 0);
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
        let database = f.database(ONE);
        let mut folder = Folder::new(database.parent().unwrap(), &database);
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
