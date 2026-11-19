use super::*;
use crate::test_support::conn;
use crate::valuation::{value_event, EventTokens, ValuationRules};
use sha2::{Digest, Sha256};

#[test]
fn regression_caught_up_rate_model_selection_seeks_version_index() {
    let mut costs = Vec::new();
    for models in [1_000, 10_000] {
        let conn = conn();
        prepare(&conn, 42, &BTreeMap::new()).unwrap();
        conn.execute(
            "WITH RECURSIVE ids(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM ids WHERE i<?1)
             INSERT INTO rate_model(provider,model,catalog_version)
             SELECT 'fixture',printf('model%06d',i),42 FROM ids",
            [models],
        )
        .unwrap();
        let counter = crate::store::VmCounter::start(&conn);
        assert!(pending(&conn, 256).unwrap().is_empty());
        costs.push(counter.steps());
    }
    eprintln!("caught-up rate model selection at 1000 / 10000 models: {costs:?} VM steps");
    assert!(costs[1] <= costs[0] + 40, "{costs:?}");
}

const OPUS: &[u8] =
    include_bytes!("../../test-vectors/fusiform-served-history-v1/anthropic--claude-opus-5-5.json");
const GPT: &[u8] =
    include_bytes!("../../test-vectors/fusiform-served-history-v1/openai--gpt-6.1-sol.json");
const KIMI: &[u8] = include_bytes!(
    "../../test-vectors/fusiform-served-history-v1/kimi-code-plan-global--k3.1-pro-highspeed.json"
);

fn captured(bytes: &[u8]) -> FetchedModel {
    let history: HistoryResponse = serde_json::from_slice(bytes).unwrap();
    let provider = history.provider_id.clone();
    let model = history.model_id.clone();
    fetched(history, &provider, &model, None).unwrap()
}

pub(crate) fn synthetic(provider: &str, model: &str, segments: Value) -> FetchedModel {
    // These intervals are synthetic: the live captures do not contain a revert or correction.
    let history = serde_json::from_value(serde_json::json!({
        "source": "synthetic", "provider_id": provider, "model_id": model,
        "fact_prefix": "rate.", "served_by_key": segments, "eras": []
    }))
    .unwrap();
    fetched(history, provider, model, None).unwrap()
}

fn numerator(
    conn: &Connection,
    provider: &str,
    model: &str,
    at_ms: i64,
    role: &str,
    unit: &str,
    quantity: i64,
) -> (i128, Vec<String>) {
    let event = EventTokens {
        producer: "opencode".into(),
        provider: provider.into(),
        model: Some(model.into()),
        mode: None,
        roles: vec![(
            role.into(),
            unit.into(),
            "fixture".into(),
            Some(quantity),
            Some(0),
        )],
    };
    let config = multiplier::MultiplierConfig {
        api_twins: BTreeMap::new(),
        plan_price_overrides: BTreeMap::new(),
        quota_providers: BTreeMap::new(),
    };
    let resolution = resolve(conn, provider, model, at_ms)
        .unwrap()
        .expect("fetched");
    let value = value_event(
        &ValuationRules::from_config(&config, &[]),
        Some(&resolution),
        &event,
    )
    .unwrap();
    (
        value.numerator,
        value.unpriced.into_iter().map(|row| row.reason).collect(),
    )
}

#[test]
fn rate_era_pinned_live_replies_decode_and_select_lifetime_boundary_and_context_tier() {
    let checksums = include_str!("../../test-vectors/fusiform-served-history-v1/SHA256SUMS");
    let conn = conn();
    for (name, bytes) in [
        ("anthropic--claude-opus-5-5.json", OPUS),
        ("openai--gpt-6.1-sol.json", GPT),
        ("kimi-code-plan-global--k3.1-pro-highspeed.json", KIMI),
    ] {
        let hash = hex::encode(Sha256::digest(bytes));
        assert!(checksums
            .lines()
            .any(|line| line == format!("{hash}  {name}")));
        let model = captured(bytes);
        publish(&conn, &model, 1).unwrap();
    }
    let boundary = 1_794_089_713_000;
    assert_eq!(
        numerator(
            &conn,
            "anthropic",
            "claude-opus-5-5",
            boundary - 1,
            "cache_write",
            "token_cache_1h",
            100
        ),
        (0, vec!["no_rate".into()])
    );
    assert_eq!(
        numerator(
            &conn,
            "anthropic",
            "claude-opus-5-5",
            boundary,
            "cache_write",
            "token_cache_1h",
            100
        ),
        (800_000_000_000, vec![])
    );
    assert_eq!(
        numerator(
            &conn,
            "anthropic",
            "claude-opus-5-5",
            boundary - 1,
            "input",
            "token",
            100
        ),
        (400_000_000_000, vec![])
    );
    assert_eq!(
        numerator(
            &conn,
            "anthropic",
            "claude-opus-5-5",
            boundary,
            "input",
            "token",
            100
        ),
        (400_000_000_000, vec![])
    );
    assert_eq!(
        numerator(
            &conn,
            "openai",
            "gpt-6.1-sol",
            boundary,
            "input",
            "token",
            300_000
        ),
        (1_200_000_000_000_000, vec![])
    );
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rate_era", [], |row| row.get(0))
        .unwrap();
    assert!(count > 50);
}

#[test]
fn rate_era_revert_within_hour_refused_and_non_usd_are_distinct() {
    let conn = conn();
    let model = synthetic(
        "p",
        "m",
        serde_json::json!({
            "rate.input": [
                {"from_ms":0,"until_ms":100,"state":"value","value":{"state":"priced","currency":"USD","units":1000000000,"exponent":9,"unit_provenance":{"kind":"stated"}}},
                {"from_ms":100,"until_ms":200,"state":"value","value":{"state":"priced","currency":"USD","units":2000000000,"exponent":9,"unit_provenance":{"kind":"stated"}}},
                {"from_ms":200,"until_ms":300,"state":"value","value":{"state":"priced","currency":"USD","units":1000000000,"exponent":9,"unit_provenance":{"kind":"stated"}}},
                {"from_ms":300,"until_ms":400,"state":"refused","reason":"correction"},
                {"from_ms":400,"until_ms":null,"state":"value","value":{"state":"priced","currency":"EUR","units":3000000000_i64,"exponent":9,"unit_provenance":{"kind":"stated"}}}
            ]
        }),
    );
    assert!(resolve(&conn, "p", "m", 0).unwrap().is_none());
    publish(&conn, &model, 1).unwrap();
    for (at, expected) in [
        (0, 1_000_000_000),
        (99, 1_000_000_000),
        (100, 2_000_000_000),
        (199, 2_000_000_000),
        (200, 1_000_000_000),
    ] {
        assert_eq!(
            numerator(&conn, "p", "m", at, "input", "token", 1),
            (expected, vec![])
        );
    }
    assert_eq!(
        numerator(&conn, "p", "m", 300, "input", "token", 1),
        (0, vec!["refused".into()])
    );
    assert_eq!(
        numerator(&conn, "p", "m", 400, "input", "token", 1),
        (0, vec!["non_usd".into()])
    );
    assert_eq!(
        numerator(&conn, "p", "m", -1, "input", "token", 1),
        (0, vec!["no_rate".into()])
    );
    assert_eq!(
        numerator(&conn, "p", "m", 300, "input", "token", 0),
        (0, vec![])
    );
}

fn summary_cell(conn: &Connection, provider: &str, model: &str, hour: i64) {
    for value in [provider, model, "producer", "basis"] {
        conn.execute(
            "INSERT OR IGNORE INTO summary_value(value) VALUES(?1)",
            [value],
        )
        .unwrap();
    }
    conn.execute(
        "INSERT OR IGNORE INTO summary_hour(kind,hour_number,revision) VALUES(0,?1,1)",
        [hour],
    )
    .unwrap();
    conn.execute("INSERT INTO summary_cell(kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,payload,paying_credential,runner,harness)
        VALUES(0,?1,(SELECT value_id FROM summary_value WHERE value='producer'),(SELECT value_id FROM summary_value WHERE value=?2),
        (SELECT value_id FROM summary_value WHERE value=?3),0,0,(SELECT value_id FROM summary_value WHERE value='basis'),0,0,0,'{}',0,0,0)", params![hour,provider,model]).unwrap();
}

#[test]
fn rate_era_first_fetch_and_target_changes_dirty_only_referring_summary_hours() {
    let conn = conn();
    summary_cell(&conn, "alias", "m", 1);
    summary_cell(&conn, "p", "m", 2);
    summary_cell(&conn, "unrelated", "m", 3);
    let mut alias = synthetic("alias", "m", serde_json::json!({}));
    alias.reference = Some(CatalogModel {
        provider_id: "p".into(),
        model_id: "m".into(),
    });
    publish(&conn, &alias, 1).unwrap();
    assert!(
        resolve(&conn, "alias", "m", 0).unwrap().is_none(),
        "target is not yet fetched"
    );
    conn.execute("DELETE FROM rate_dirty_hour", []).unwrap();
    let target = synthetic("p", "m", serde_json::json!({}));
    publish(&conn, &target, 1).unwrap();
    let hours = || {
        conn.prepare("SELECT hour_number FROM rate_dirty_hour ORDER BY 1")
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(hours(), vec![1, 2]);
    assert!(resolve(&conn, "alias", "m", 0).unwrap().is_some());
    conn.execute("DELETE FROM rate_dirty_hour", []).unwrap();
    assert!(
        !publish(&conn, &target, 2).unwrap(),
        "version alone does not dirty hours"
    );
    assert!(hours().is_empty());
    alias.reference = Some(CatalogModel {
        provider_id: "new".into(),
        model_id: "m".into(),
    });
    publish(&conn, &alias, 2).unwrap();
    assert_eq!(hours(), vec![1]);
    conn.execute("DELETE FROM rate_dirty_hour", []).unwrap();
    mark_dirty(&conn, "p", "m").unwrap();
    assert_eq!(hours(), vec![2], "old target no longer dirties the alias");
    prepare(&conn, 3, &BTreeMap::new()).unwrap();
    assert_eq!(pending(&conn, 10).unwrap().len(), 3);
}

#[test]
fn rate_era_dirty_lookup_large_summary_measurement() {
    let conn = conn();
    conn.execute_batch("BEGIN;
        CREATE TEMP TABLE seq(n INTEGER PRIMARY KEY);
        WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<160890)
        INSERT INTO seq SELECT n FROM numbers;
        INSERT INTO summary_value(value_id,value) VALUES(1,'p'),(2,'busy'),(3,'producer'),(4,'basis');
        INSERT INTO summary_value(value_id,value) SELECT n+10,'other-'||n FROM seq WHERE n<100;
        INSERT INTO summary_hour(kind,hour_number,revision) SELECT 0,n,1 FROM seq;
        INSERT INTO summary_cell(kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,payload,paying_credential,runner,harness)
        SELECT 0,n,3,1,CASE WHEN n%10=0 THEN 2 ELSE 11+(n%99) END,0,0,4,0,0,0,'{}',0,0,0 FROM seq;
        COMMIT;").unwrap();
    let started = std::time::Instant::now();
    let count = mark_dirty(&conn, "p", "busy").unwrap();
    println!(
        "rate_era lookup: 160890 summary cells, busiest model 16089 hours, {} us",
        started.elapsed().as_micros()
    );
    assert_eq!(count, 16089);
    let queued: i64 = conn
        .query_row("SELECT COUNT(*) FROM rate_dirty_hour", [], |row| row.get(0))
        .unwrap();
    assert_eq!(queued, 16089);
}

#[test]
fn rate_era_discovery_cursor_follows_new_calls_and_twins_invalidate_references() {
    let conn = conn();
    for id in 0..3 {
        conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,provider,model,charge_basis) VALUES(?1,'opencode',?1,0,'p',?1,'unknown')",[id.to_string()]).unwrap();
    }
    discover(&conn, 2).unwrap();
    assert_eq!(pending(&conn, 10).unwrap().len(), 2);
    discover(&conn, 2).unwrap();
    assert_eq!(pending(&conn, 10).unwrap().len(), 3);
    let fetched = synthetic("p", "0", serde_json::json!({}));
    publish(&conn, &fetched, 1).unwrap();
    prepare(&conn, 1, &BTreeMap::new()).unwrap();
    assert_eq!(pending(&conn, 10).unwrap().len(), 2);
    prepare(&conn, 1, &BTreeMap::from([("p".into(), "twin".into())])).unwrap();
    assert_eq!(pending(&conn, 10).unwrap().len(), 3);
    assert!(resolve(&conn, "p", "0", 0).unwrap().is_none());
}

#[test]
fn rate_era_failed_fetch_yields_to_other_pending_models() {
    let conn = conn();
    for model in ["a", "b"] {
        conn.execute(
            "INSERT INTO rate_model(provider,model) VALUES('p',?1)",
            [model],
        )
        .unwrap();
    }
    assert_eq!(pending(&conn, 1).unwrap(), vec![("p".into(), "a".into())]);
    record_failure(
        &conn,
        "p",
        "a",
        &MultiplierError::RateFactUnrecognized("rate.new_key".into()),
    )
    .unwrap();
    assert_eq!(pending(&conn, 1).unwrap(), vec![("p".into(), "b".into())]);
    assert!(matches!(
        resolved_rates(&conn, &BTreeSet::from([("p".into(), "a".into())]), 0),
        Err(MultiplierError::RateFactUnrecognized(_))
    ));
}

/// A store that already holds calls learns every model in use when it migrates,
/// instead of walking the whole ledger a few thousand calls per tick, and later
/// discovery starts after the newest call it already counted.
#[test]
fn rate_era_migration_seeds_models_from_existing_calls() {
    use crate::store::MIGRATIONS;
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    for migration in MIGRATIONS.iter().filter(|migration| migration.version < 57) {
        conn.execute_batch(migration.statements).unwrap();
    }
    for (id, provider, model) in [
        ("a", Some("anthropic"), Some("claude-opus-5-5")),
        ("b", Some("anthropic"), Some("claude-opus-5-5")),
        ("c", Some("openai"), Some("gpt-6.1-sol")),
        ("d", None, Some("no-provider")),
        ("e", Some("openai"), None),
    ] {
        conn.execute(
            "INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,charge_basis,provider,model) VALUES(?1,'p',?1,1,'unknown',?2,?3)",
            params![id, provider, model],
        )
        .unwrap();
    }
    let newest: i64 = conn
        .query_row("SELECT max(event_key) FROM usage_event", [], |row| {
            row.get(0)
        })
        .unwrap();
    conn.execute_batch(
        MIGRATIONS
            .iter()
            .find(|migration| migration.version == 57)
            .unwrap()
            .statements,
    )
    .unwrap();
    let models: Vec<(String, String)> = conn
        .prepare("SELECT provider,model FROM rate_model ORDER BY provider,model")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        models,
        [
            ("anthropic".to_string(), "claude-opus-5-5".to_string()),
            ("openai".to_string(), "gpt-6.1-sol".to_string()),
        ]
    );
    assert_eq!(
        pending(&conn, 10).unwrap().len(),
        2,
        "seeded models wait for a fetch"
    );
    let after: i64 = conn
        .query_row(
            "SELECT after_event_key FROM rate_cache_state WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after, newest);
    conn.execute(
        "INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,charge_basis,provider,model) VALUES('f','p','f',2,'unknown','xai','grok-5')",
        [],
    )
    .unwrap();
    discover(&conn, 2_000).unwrap();
    assert_eq!(
        pending(&conn, 10).unwrap().len(),
        3,
        "a later call is still discovered"
    );
}
