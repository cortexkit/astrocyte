use super::*;
use astrocyte_core::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse, StatusRequest, StatusResponse};
use serde_json::json;
use sha2::{Digest, Sha256};

const RETRIED: &str = "retried-refused-and-unanswered.jsonl";
const HANDOFF: &str = "handed-on-inside-one-request.jsonl";
const LEGACY: &str = "no-attempt-before-the-usage-so-unmarked.jsonl";
const POOLED: &str = "pooled-key-fast-tier-cache-split.jsonl";

fn vector_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/broca-attempt-facts-1119")
}

fn load_vectors(name: &str, path: &Path) {
    create_store(path, "attempt-vectors", 1);
    for (index, line) in std::fs::read_to_string(vector_dir().join(name))
        .unwrap()
        .lines()
        .enumerate()
    {
        let payload: Value = serde_json::from_str(line).unwrap();
        add_segment(
            path,
            i64::try_from(index + 1).unwrap(),
            payload["fact_id"].as_str().unwrap(),
            line,
        );
    }
}

fn sample_rows(name: &str) -> Vec<Value> {
    std::fs::read_to_string(vector_dir().join(name))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn append_values(path: &Path, rows: &[Value]) {
    for (i, row) in rows.iter().enumerate() {
        add_segment(
            path,
            i64::try_from(i + 1).unwrap(),
            row["fact_id"].as_str().unwrap(),
            &row.to_string(),
        );
    }
}

fn read_rows<T>(
    state: &SharedState,
    sql: &str,
    map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Vec<T> {
    state
        .with_conn(|conn| {
            conn.prepare(sql)
                .and_then(|mut stmt| stmt.query_map([], map)?.collect())
                .map_err(|error| error.to_string())
        })
        .unwrap()
}

fn drain(state: &SharedState, path: &Path) -> crate::refresher::DrainOutcome {
    drain_once(state, &source(path), BROCA_SOURCE_KEY, PAGE_LIMIT, 3000).unwrap()
}

struct RateCatalog {
    fast: bool,
    tiered: bool,
}

impl CatalogConnection for RateCatalog {
    fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
        Ok(
            serde_json::from_value(json!({"source":"fake", "catalog_version":1,
            "model_count":1, "era_count":1, "recent_polls":[], "history_begins_at_ms":0}))
            .unwrap(),
        )
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        let at = request.at_ms.unwrap();
        let key = format!(
            "{}/{}",
            request.provider_id.unwrap(),
            request.model_id.unwrap()
        );
        let mut facts = serde_json::Map::new();
        let rate = |multiplier: i64| {
            json!({"state":"priced", "units":multiplier * 1_000_000_000_i64,
            "exponent":9, "currency":"USD", "unit_provenance":{"kind":"stated"}})
        };
        for (role, selectors, multiplier) in [
            ("input", "", 1),
            ("output", "", 1),
            ("cache_read", "", 1),
            ("cache_write", "", 1),
            ("cache_write", ".ttl.5m", 2),
            ("cache_write", ".ttl.1h", 3),
        ] {
            facts.insert(format!("rate.{role}{selectors}"), rate(multiplier));
            if self.fast {
                facts.insert(
                    format!("rate.{role}.mode.fast{selectors}"),
                    rate(4 * multiplier),
                );
            }
        }
        if self.tiered {
            facts.insert("rate.input.above_context.100".into(), rate(2));
        }
        Ok(serde_json::from_value(
            json!({"source":"fake", "resolved_at_ms":at, "catalog_version":1,
            "models":{key:facts}, "withheld":[], "uncertain":[]}),
        )
        .unwrap())
    }
}

fn price(state: &SharedState, fast: bool, tiered: bool) {
    let catalog = CatalogClient::connect(Arc::new(RateCatalog { fast, tiered })).unwrap();
    state.install_catalog(Some(catalog)).unwrap();
    crate::refresher::price_unpriced_events(state, 4000, &AtomicBool::new(false)).unwrap();
}

fn coverage(state: &SharedState) -> Value {
    crate::ops::dispatch(state, crate::ops::OP_LEDGER_COVERAGE, json!({})).unwrap()
}

#[test]
fn broca_attempt_vectors_match_sha256sums() {
    let directory = vector_dir();
    let checksums = std::fs::read_to_string(directory.join("SHA256SUMS")).unwrap();
    let names: Vec<_> = checksums
        .lines()
        .map(|line| {
            let (expected, name) = line.split_once("  ").unwrap();
            let bytes = std::fs::read(directory.join(name)).unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(bytes)), expected, "{name}");
            name
        })
        .collect();
    assert_eq!(names, [HANDOFF, LEGACY, POOLED, RETRIED, "README.md"]);
}

#[test]
fn broca_attempt_vectors_bill_attempts_without_double_billing() {
    // These are separate captures. Two deliberately reuse the same run and segment id.
    for (name, calls, totals) in [
        (RETRIED, 5, (60, 6, None, None)),
        (HANDOFF, 2, (6, 31_411, Some(40_136), Some(4))),
        (LEGACY, 1, (10, 2, None, None)),
        (POOLED, 2, (21, 700, Some(80_000), Some(6_000))),
    ] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        load_vectors(name, &path);
        let state = test_state();
        drain(&state, &path);

        assert_eq!(count(&state, "usage_event"), calls, "{name}");
        assert!(set_aside_rows(&state).is_empty(), "{name}");
        let cache_refusal = crate::ops::dispatch(
            &state,
            crate::ops::OP_CACHE_STEPS,
            json!({"harness":"broca","session":"session"}),
        )
        .unwrap_err();
        assert_eq!(cache_refusal.0, "unknown_harness", "{name}");
        assert_eq!(count(&state, "step_feed"), 0, "{name}");
        let actual = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT SUM(CASE WHEN role='input' THEN quantity_units END),
                            SUM(CASE WHEN role='output' THEN quantity_units END),
                            SUM(CASE WHEN role='cache_read' THEN quantity_units END),
                            SUM(CASE WHEN role='cache_write' THEN quantity_units END)
                       FROM measurement",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(totals, actual, "{name}");
        assert_eq!(
            coverage(&state)["broca_segment_usage_mismatches"],
            0,
            "{name}"
        );
        price(&state, false, false);
        match name {
            RETRIED => {
                let unknowns = read_rows(&state,
                    "SELECT a.attempt_id,a.outcome,a.content_received,
                            (SELECT count(*) FROM measurement m WHERE m.event_key=a.event_key),
                            (SELECT count(*) FROM priced_charge p WHERE p.event_key=a.event_key)
                     FROM broca_attempt_detail a WHERE a.segment_fact_id IS NULL ORDER BY a.step_id",
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, bool>(2)?, r.get::<_, i64>(3)?, r.get::<_, i64>(4)?)));
                assert_eq!(
                    unknowns,
                    [
                        ("att_20".into(), "chunk_error".into(), true, 0, 0),
                        ("att_90".into(), "connect_failed".into(), false, 0, 0)
                    ]
                );
                assert_eq!(
                    read_rows(&state, "SELECT SUM(charge_nanos) FROM priced_charge", |r| r
                        .get::<_, i64>(0)),
                    [66_000]
                );
            }
            HANDOFF => {
                assert_eq!(read_rows(&state,
                    "SELECT a.share_index,a.attempt_id,e.model,e.occurred_at_ms FROM broca_attempt_detail a
                     JOIN usage_event e USING(event_key) ORDER BY a.share_index",
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?))),
                    [(0,"att_15".into(),"claude-opus-5-5".into(),1015), (1,"att_15".into(),"claude-sonnet-5".into(),1015)]);
            }
            LEGACY => {
                assert_eq!(count(&state, "broca_segment_evidence"), 0);
                assert_eq!(count(&state, "broca_attempt_detail"), 0);
                assert_eq!(
                    read_rows(&state, "SELECT SUM(charge_nanos) FROM priced_charge", |r| r
                        .get::<_, i64>(0)),
                    [12_000]
                );
            }
            _ => {}
        }
    }
}

#[test]
fn broca_attempt_fast_mode_uses_only_fast_rates_and_retains_payer_and_split() {
    for fast_rates in [false, true] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        load_vectors(POOLED, &path);
        let state = test_state();
        drain(&state, &path);
        price(&state, fast_rates, false);

        let paying = read_rows(&state,
            "SELECT DISTINCT e.auth_selection,e.resolved_credential,e.account_ref,a.served_account_ref,
                    a.requested_service_tier FROM usage_event e JOIN broca_attempt_detail a USING(event_key)",
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?)));
        assert_eq!(
            paying,
            [(
                "apikey:anthropic".into(),
                "apikey:anthropic-pool-key-2".into(),
                "acct2".into(),
                "acct2".into(),
                "fast".into()
            )]
        );
        assert_eq!(read_rows(&state,
            "SELECT a.step_id,m.unit,m.quantity_units FROM broca_attempt_detail a
             JOIN measurement m USING(event_key) WHERE m.role='cache_write' ORDER BY a.step_id,m.unit",
             |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))),
            [(1,"token_cache_1h".into(),2000), (1,"token_cache_5m".into(),1000),
             (2,"token_cache_1h".into(),2000), (2,"token_cache_5m".into(),1000)]);

        let priced = read_rows(
            &state,
            "SELECT a.mode,a.served_service_tier,p.unpriced_reason,p.charge_nanos,
                    json_extract(p.catalog_entry,'$.fact_key'),p.resolved_at_ms
             FROM priced_charge p JOIN broca_attempt_detail a USING(event_key)
             WHERE p.role='input' ORDER BY a.step_id",
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            },
        );
        assert_eq!(
            priced,
            [
                if fast_rates {
                    (
                        "fast".into(),
                        "priority".into(),
                        None,
                        Some(48_000),
                        Some("rate.input.mode.fast".into()),
                        1020,
                    )
                } else {
                    (
                        "fast".into(),
                        "priority".into(),
                        Some("no_rate".into()),
                        None,
                        Some("rate.input.mode.fast".into()),
                        1020,
                    )
                },
                (
                    "standard".into(),
                    "standard".into(),
                    None,
                    Some(9_000),
                    Some("rate.input".into()),
                    1040
                ),
            ]
        );
        assert_eq!(
            count(&state, "priced_charge WHERE outcome='priced'"),
            if fast_rates { 10 } else { 5 }
        );
    }
}

#[test]
fn broca_attempt_same_model_shares_price_on_individual_prompt_sizes() {
    let mut rows = sample_rows(POOLED);
    rows[0]["usage"] = json!({"input_tokens":261});
    let share = |input| {
        json!({"provider":"anthropic", "model":"claude",
        "usage":{"input_tokens":input}, "segment_fact_id":rows[0]["fact_id"]})
    };
    let (under, over) = (json!([share(70), share(90)]), json!([share(101)]));
    rows[1]["usage_by_model"] = under;
    rows[1]["outcome"] = json!("a_new_outcome");
    rows[2]["usage_by_model"] = over;
    for row in &mut rows[1..] {
        row.as_object_mut().unwrap().remove("served_speed");
    }

    let directory = TempDir::new().unwrap();
    let path = directory.path().join("run-index.db");
    create_store(&path, "tiered-requests", 1);
    append_values(&path, &rows);
    let state = test_state();
    drain(&state, &path);
    price(&state, false, true);

    assert!(set_aside_rows(&state).is_empty());
    assert_eq!(coverage(&state)["broca_segment_usage_mismatches"], 0);
    assert_eq!(read_rows(&state,
        "SELECT a.attempt_id,a.outcome,a.share_index,e.model,m.quantity_units,p.charge_nanos,p.unpriced_reason
         FROM broca_attempt_detail a JOIN usage_event e USING(event_key) JOIN measurement m USING(event_key)
         JOIN priced_charge p ON (p.event_key,p.role,p.unit)=(m.event_key,m.role,m.unit)
         ORDER BY a.step_id,a.share_index",
        |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, i64>(2)?,r.get::<_, String>(3)?,r.get::<_, i64>(4)?,r.get::<_, Option<i64>>(5)?,r.get::<_, Option<String>>(6)?))),
        [("att_20".into(),"a_new_outcome".into(),0,"claude".into(),70,Some(70_000),None),
         ("att_20".into(),"a_new_outcome".into(),1,"claude".into(),90,Some(90_000),None),
         ("att_40".into(),"complete".into(),0,"claude".into(),101,None,Some("rate_tiered".into()))]);
}

#[test]
fn broca_attempt_redelivery_is_exact_or_quarantined_as_a_whole() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("run-index.db");
    load_vectors(HANDOFF, &path);
    let state = test_state();
    drain(&state, &path);
    price(&state, false, false);
    let tables = [
        "usage_event",
        "measurement",
        "priced_charge",
        "broca_segment_evidence",
        "broca_attempt_detail",
        "redelivery_log",
    ];
    let before = tables.map(|table| count(&state, table));
    assert_eq!(before[0], 2);
    let store = Connection::open(&path).unwrap();
    let replay = || {
        store
            .execute("UPDATE export_meta SET source_epoch=source_epoch+1", [])
            .unwrap();
        drain(&state, &path)
    };
    assert_eq!(replay().quarantined_redeliveries, 0);
    assert_eq!(count(&state, "rejected_delivery"), 0);
    assert_eq!(tables.map(|table| count(&state, table)), before);

    let mut changed = sample_rows(HANDOFF).pop().unwrap();
    changed["outcome"] = json!("changed_on_replay");
    let extra = changed["usage_by_model"][0].clone();
    changed["usage_by_model"]
        .as_array_mut()
        .unwrap()
        .push(extra);
    store
        .execute(
            "UPDATE export_facts SET segment_json=?1 WHERE export_seq=3",
            [changed.to_string()],
        )
        .unwrap();
    assert_eq!(replay().quarantined_redeliveries, 1);
    assert_eq!(tables.map(|table| count(&state, table)), before);
    assert_eq!(read_rows(&state,
        "SELECT json_extract(r.incoming_payload,'$.outcome'),a.outcome,a.share_index FROM rejected_delivery r
         JOIN broca_attempt_detail a ON a.event_key=r.stored_event_key",
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))),
        [("changed_on_replay".into(), "complete".into(), 0)]);
}

#[test]
fn broca_attempt_coverage_distinguishes_missing_usage_from_zero() {
    let mut rows = sample_rows(RETRIED);
    rows[0]["usage"]["reasoning_tokens"] = json!(0);
    rows[1]["usage"]["input_tokens"] = json!(21);
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("run-index.db");
    create_store(&path, "mismatched-usage", 1);
    append_values(&path, &rows);
    let state = test_state();
    drain(&state, &path);
    assert_eq!(coverage(&state)["broca_segment_usage_mismatches"], 2);
}

#[test]
fn broca_attempt_and_segment_evidence_are_append_only_even_with_replace() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("run-index.db");
    load_vectors(HANDOFF, &path);
    let state = test_state();
    drain(&state, &path);
    for table in ["broca_attempt_detail", "broca_segment_evidence"] {
        for (sql, trigger) in [
            (format!("UPDATE {table} SET fact_id=fact_id"), "no_update"),
            (format!("DELETE FROM {table}"), "no_delete"),
            (
                format!("INSERT OR REPLACE INTO {table} SELECT * FROM {table}"),
                "insert_once",
            ),
        ] {
            let result = state.ordered_write(|tx| {
                tx.execute(&sql, [])
                    .map_err(|e| e.to_string())
                    .map(|_| ((), true))
            });
            assert_eq!(result.unwrap_err(), format!("{table}_{trigger}"));
        }
        assert_eq!(count(&state, table), 2);
    }
}

#[test]
fn broca_attempt_runs_keep_warm_segments_and_serve_step_pricing_unit() {
    let mut rows = sample_rows(RETRIED);
    let mut warm = rows[0].clone();
    warm["fact_id"] = json!("warm-segment");
    warm["warm"] = json!(true);
    warm["usage"] = json!({"input_tokens":7});
    warm["occurred_at_ms"] = json!(200);
    warm.as_object_mut().unwrap().remove("usage_in_attempts");
    rows.insert(0, warm);
    let paid_run = |rows: &[Value]| {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "warm-run", 1);
        append_values(&path, rows);
        let state = test_state();
        drain(&state, &path);
        price(&state, false, false);
        assert!(set_aside_rows(&state).is_empty());
        let listed = crate::ops::dispatch(
            &state,
            crate::ops::OP_RUNS_LIST,
            json!({"from_ms":0,"to_ms":3000}),
        )
        .unwrap();
        let detail = crate::ops::dispatch(
            &state,
            crate::ops::OP_RUNS_DETAIL,
            json!({"key":{"kind":"run","harness":"broca","run_id":"run"}}),
        )
        .unwrap();
        (listed, detail)
    };
    let (list, detail) = paid_run(&rows);
    assert_eq!(list["rows"][0]["pricing_unit"], "step");
    assert_eq!(detail["pricing_unit"], "step");
    let items = detail["items"].as_array().unwrap();
    assert_eq!(items.len(), 6);
    assert_eq!(items[0]["segment_ref"], "warm-segment");
    assert!(items[0].get("step_ref").is_none());
    for item in &items[1..] {
        assert!(item["step_ref"]
            .as_str()
            .unwrap()
            .starts_with("model_attempt:"));
        assert!(item.get("segment_ref").is_none());
    }

    rows.truncate(3);
    for segment in &mut rows {
        segment.as_object_mut().unwrap().remove("usage_in_attempts");
    }
    let (legacy_list, legacy_detail) = paid_run(&rows);
    assert_eq!(legacy_list["rows"][0]["pricing_unit"], "run_segment");
    assert_eq!(legacy_detail["pricing_unit"], "run_segment");
    assert_eq!(detail["totals"]["tokens"], json!({"input":67,"output":6}));
    assert_eq!(detail["totals"]["cost"]["priced"], json!({"USD":73000}));
    assert_eq!(detail["totals"], legacy_detail["totals"]);
    assert_eq!(list["rows"][0]["tokens"], legacy_list["rows"][0]["tokens"]);
}

#[test]
fn broca_attempt_double_billing_guard_preserves_share_positions_and_redelivery() {
    let mut rows = sample_rows(HANDOFF);
    rows[0].as_object_mut().unwrap().remove("usage_in_attempts");
    for row in &mut rows {
        row["charge_basis"] = json!("metered_api");
    }
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("run-index.db");
    create_store(&path, "guard-share", 1);
    append_values(&path, &rows);
    let state = test_state();
    drain(&state, &path);
    price(&state, false, false);
    assert_eq!(count(&state, "usage_event"), 2);
    assert_eq!(read_rows(&state,
        "SELECT a.share_index,e.model FROM broca_attempt_detail a JOIN usage_event e USING(event_key)",
        |r| Ok((r.get::<_, i64>(0)?,r.get::<_, String>(1)?))), [(1,"claude-sonnet-5".into())]);
    assert_eq!(
        read_rows(&state, "SELECT SUM(charge_nanos) FROM priced_charge", |r| r
            .get::<_, i64>(0)),
        [71_557_000]
    );
    let assert_skipped_once = || {
        let rows = set_aside_rows(&state);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].0.ends_with(":export_seq:3:share:0"), "{rows:?}");
        assert!(rows[0].1.starts_with("segment_already_billed:"), "{rows:?}");
        assert_eq!(
            coverage(&state)["set_aside_by_source"]["broca"]["records"],
            1
        );
    };
    assert_skipped_once();

    let store = Connection::open(&path).unwrap();
    let replay = || {
        store
            .execute("UPDATE export_meta SET source_epoch=source_epoch+1", [])
            .unwrap();
        drain(&state, &path)
    };
    let tables = [
        "usage_event",
        "measurement",
        "priced_charge",
        "broca_attempt_detail",
        "broca_segment_evidence",
        "set_aside_record",
        "redelivery_log",
    ];
    let before = tables.map(|table| count(&state, table));
    assert_eq!(replay().quarantined_redeliveries, 0);
    assert_eq!(tables.map(|table| count(&state, table)), before);
    assert_eq!(count(&state, "rejected_delivery"), 0);

    rows[2]["outcome"] = json!("changed_after_partial_recording");
    store
        .execute(
            "UPDATE export_facts SET segment_json=?1 WHERE export_seq=3",
            [rows[2].to_string()],
        )
        .unwrap();
    assert_eq!(replay().quarantined_redeliveries, 1);
    assert_eq!(tables.map(|table| count(&state, table)), before);
    assert_eq!(read_rows(&state,
        "SELECT a.share_index,a.outcome FROM rejected_delivery r JOIN broca_attempt_detail a ON a.event_key=r.stored_event_key",
        |r| Ok((r.get::<_, i64>(0)?,r.get::<_, String>(1)?))), [(1,"complete".into())]);
    assert_skipped_once();
}

#[test]
fn broca_attempt_double_billing_guard_sets_aside_later_legacy_segment() {
    let mut rows = sample_rows(RETRIED);
    let mut legacy = rows.remove(0);
    rows.remove(0);
    let mut attempt = rows.remove(1);
    attempt["usage_by_model"][0]["usage"] = legacy["usage"].clone();
    legacy.as_object_mut().unwrap().remove("usage_in_attempts");
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("run-index.db");
    create_store(&path, "guard-segment", 1);
    // Both facts are available in one page. The earlier attempt wins.
    append_values(&path, &[attempt, legacy]);
    let state = test_state();
    assert_eq!(drain(&state, &path).consumed_ordinal, Some(2));
    price(&state, false, false);
    assert_eq!(count(&state, "usage_event"), 1);
    assert_eq!(count(&state, "broca_attempt_detail"), 1);
    assert_eq!(
        read_rows(&state, "SELECT SUM(charge_nanos) FROM priced_charge", |r| r
            .get::<_, i64>(0)),
        [43_000]
    );
    let aside = set_aside_rows(&state);
    assert_eq!(aside.len(), 1);
    assert!(aside[0].0.ends_with(":export_seq:2"), "{aside:?}");
    assert!(
        aside[0].1.starts_with("segment_already_billed:"),
        "{aside:?}"
    );
    assert_eq!(
        coverage(&state)["set_aside_by_source"]["broca"]["records"],
        1
    );
}
