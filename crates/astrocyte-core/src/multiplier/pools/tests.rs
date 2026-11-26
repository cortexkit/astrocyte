use fusiform_protocol::PlanPricesResponse;
use rusqlite::{params, Connection};
use serde_json::json;

use crate::test_support::conn;
use crate::vault_plan::{self, VaultPlan};

use super::super::NANOS_PER_MS;
use super::*;

#[path = "call_tests.rs"]
mod call_tests;
#[path = "row_tests.rs"]
mod row_tests;

const DAY: i64 = 86_400_000;
const CUT: i64 = 10 * DAY;
const START: i64 = CUT + DAY;
const END: i64 = START + DAY;
const PLAN: &str = "anthropic/max";
const PROVIDER: &str = "claude";
const CATALOG_PROVIDER: &str = "anthropic";
const ACCOUNT: (PlanLedger, &str) = (PlanLedger::Account, "login");
const CREDENTIAL: (PlanLedger, &str) = (PlanLedger::Credential, "login");
const FABLE: &str = "extra:5:Fable";
const WEEK: Option<i64> = Some(10_080);
const SHORT: Option<i64> = Some(300);
const ALL: Option<&str> = Some("all");
const SUBSET: Option<&str> = Some("subset");

fn fixture() -> Connection {
    let conn = conn();
    // test_support applies the DDL directly, without the store's migration ledger.
    conn.execute_batch(
        "CREATE TABLE cortexkit_schema_version (
             namespace TEXT, version INTEGER, applied_at_unix INTEGER,
             PRIMARY KEY(namespace, version));
         INSERT INTO cortexkit_schema_version VALUES ('astrocyte', 67, 1), ('other', 68, 1);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO cortexkit_schema_version VALUES ('astrocyte', 68, ?1)",
        [CUT / 1000],
    )
    .unwrap();
    vault_plan(&conn, ACCOUNT, Some(PLAN), 0);
    conn
}

fn vault_plan(conn: &Connection, (ledger, id): (PlanLedger, &str), plan: Option<&str>, at: i64) {
    let (credential, account) = match ledger {
        PlanLedger::Account => (format!("oauth:{id}"), Some(id.into())),
        PlanLedger::Credential => (id.into(), None),
    };
    let tx = conn.unchecked_transaction().unwrap();
    vault_plan::append_in_tx(
        &tx,
        &VaultPlan {
            credential_id: credential,
            provider: Some("anthropic".into()),
            account_id: account,
            plan_tier: plan.map(|key| key.split_once('/').unwrap().1.into()),
            plan_key: plan.map(str::to_owned),
            source: "operator".into(),
            effective_from_ms: at,
            observed_at_ms: at,
        },
    )
    .unwrap();
    tx.commit().unwrap();
}

fn sample(
    conn: &Connection,
    login: (PlanLedger, &str),
    pool: (&str, &str),
    counter: (i64, i64),
    metadata: (Option<i64>, Option<&str>),
) {
    sample_with_api_provider(conn, login, pool, counter, metadata, Some(CATALOG_PROVIDER));
}

fn sample_with_api_provider(
    conn: &Connection,
    (ledger, login): (PlanLedger, &str),
    (provider, pool): (&str, &str),
    (at_ms, used): (i64, i64),
    (minutes, scope): (Option<i64>, Option<&str>),
    api_provider: Option<&str>,
) {
    let (table, field) = match ledger {
        PlanLedger::Account => ("window_sample", "account_ref"),
        PlanLedger::Credential => ("credential_window_sample", "credential_ref"),
    };
    conn.execute(
        &format!(
            "INSERT INTO {table}(provider,{field},pool_key,fetched_at_instant,
                   used_percent,window_minutes,model_scope,api_provider)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
        ),
        params![
            provider,
            login,
            pool,
            at_ms * NANOS_PER_MS,
            used,
            minutes,
            scope,
            api_provider
        ],
    )
    .unwrap();
}

fn pair(conn: &Connection, key: &str, minutes: Option<i64>, scope: Option<&str>) {
    for (at, used) in [(START, 10), (END, 40)] {
        sample(conn, ACCOUNT, (PROVIDER, key), (at, used), (minutes, scope));
    }
}

fn capture(conn: &Connection, start: i64, end: i64) -> PoolReadingCapture {
    let tx = conn.unchecked_transaction().unwrap();
    let readings = capture_pool_readings(&tx, PROVIDER, start, end).unwrap();
    tx.commit().unwrap();
    readings
}

fn list(provider: &str, pool_id: &str, models: &[&str]) -> QuotaPoolWire {
    serde_json::from_value(json!({
        "provider_id": provider, "pool_id": pool_id, "models": models,
        "established_at_ms": START, "review_by_ms": END, "quote": "test coverage",
        "source_ref": "https://example.invalid/quota"
    }))
    .unwrap()
}

fn by_id<'a, 'b>(pools: &'b [PlannedPool<'a>], pool_id: &str) -> &'b PlannedPool<'a> {
    pools.iter().find(|pool| pool.pool_id == pool_id).unwrap()
}

#[test]
fn nested_pools_use_raw_fable_id_and_cover_only_their_own_models() {
    let conn = fixture();
    pair(&conn, "primary", SHORT, ALL);
    pair(&conn, "secondary", WEEK, ALL);
    pair(&conn, FABLE, WEEK, SUBSET);
    pair(&conn, "extra:7:Missing", WEEK, SUBSET);
    pair(&conn, "extra:5:Empty", WEEK, SUBSET);
    pair(&conn, "extra:7:Unknown", WEEK, None);
    pair(&conn, "extra:10:NoDuration", None, ALL);
    let readings = capture(&conn, START, END);
    let lists = [
        list(CATALOG_PROVIDER, FABLE, &["anthropic/wrong-key"]),
        list("other-catalog", "Fable", &["anthropic/wrong-provider"]),
        list(CATALOG_PROVIDER, "Fable", &["anthropic/fable"]),
        list(CATALOG_PROVIDER, "Empty", &[]),
    ];
    let pools = plan_pools(&readings, &lists);
    assert_eq!(
        pools.iter().map(|pool| pool.pool_id).collect::<Vec<_>>(),
        ["Empty", "Fable", "Missing", "Unknown", "secondary"]
    );
    let fable = by_id(&pools, "Fable");
    assert_eq!(
        (fable.quota_provider, fable.plan, fable.window_pool_key),
        (PROVIDER, PLAN, FABLE)
    );
    assert_eq!(fable.pool_scope, PoolScope::Subset);
    assert_eq!(fable.pool_models, Some(lists[2].models.as_slice()));
    assert_eq!(fable.pool_models_at_ms, Some(END));
    let all = by_id(&pools, "secondary");
    assert_eq!(all.coverage(), Some(PoolCoverage::All));
    assert_eq!((all.pool_models, all.pool_models_at_ms), (None, None));
    let (all, subset) = (all.coverage().unwrap(), fable.coverage().unwrap());
    for (id, covered) in [
        (Some("anthropic/fable"), true),
        (Some("anthropic/other"), false),
        (None, false),
    ] {
        assert!(all.covers_model(id));
        assert_eq!(subset.covers_model(id), covered);
    }
    for key in ["Empty", "Missing"] {
        let missing = by_id(&pools, key);
        assert_eq!(missing.diagnostic, Some(PoolDiagnostic::PoolListMissing));
        assert_eq!(
            (
                missing.pool_scope,
                missing.pool_models,
                missing.pool_models_at_ms
            ),
            (PoolScope::Subset, None, Some(END))
        );
    }
    let unknown = by_id(&pools, "Unknown");
    assert_eq!(
        (unknown.diagnostic, unknown.pool_scope),
        (Some(PoolDiagnostic::ScopeUnstated), PoolScope::Unknown)
    );
}

#[test]
fn absent_empty_or_changed_catalog_provider_does_not_choose_a_subset_list() {
    let lists = [
        list(PROVIDER, "Fable", &["claude/not-a-fallback"]),
        list(CATALOG_PROVIDER, "Fable", &["anthropic/fable"]),
        list("other-catalog", "Fable", &["other-catalog/fable"]),
    ];
    for (opener, closer) in [
        (None, None),
        (Some(""), Some("")),
        (None, Some(CATALOG_PROVIDER)),
        (Some(CATALOG_PROVIDER), Some("other-catalog")),
    ] {
        let conn = fixture();
        for (at, used, api_provider) in [(START, 10, opener), (END, 40, closer)] {
            sample_with_api_provider(
                &conn,
                ACCOUNT,
                (PROVIDER, FABLE),
                (at, used),
                (WEEK, SUBSET),
                api_provider,
            );
        }
        let readings = capture(&conn, START, END);
        let pools = plan_pools(&readings, &lists);
        let fable = by_id(&pools, "Fable");
        assert_eq!(fable.pool_scope, PoolScope::Subset);
        assert_eq!(fable.catalog_provider, None);
        assert_eq!(
            fable.diagnostic,
            Some(PoolDiagnostic::PoolListMissing),
            "{opener:?} -> {closer:?}"
        );
        assert_eq!(
            (fable.pool_models, fable.pool_models_at_ms),
            (None, Some(END))
        );
    }
}

#[test]
fn account_and_credential_readings_must_agree_on_the_catalog_provider() {
    let lists = [
        list(CATALOG_PROVIDER, "Fable", &["anthropic/fable"]),
        list("other-catalog", "Fable", &["other-catalog/fable"]),
    ];
    for (provider, expected) in [
        (CATALOG_PROVIDER, Some(lists[0].models.as_slice())),
        ("other-catalog", None),
    ] {
        let conn = fixture();
        vault_plan(&conn, CREDENTIAL, Some(PLAN), 0);
        pair(&conn, FABLE, WEEK, SUBSET);
        for (at, used) in [(START, 20), (END, 60)] {
            sample_with_api_provider(
                &conn,
                CREDENTIAL,
                (PROVIDER, FABLE),
                (at, used),
                (WEEK, SUBSET),
                Some(provider),
            );
        }
        let readings = capture(&conn, START, END);
        let pools = plan_pools(&readings, &lists);
        let fable = by_id(&pools, "Fable");
        assert_eq!(fable.reading_runs.len(), 2);
        assert_eq!(fable.pool_models, expected);
        assert_eq!(fable.catalog_provider, expected.map(|_| CATALOG_PROVIDER));
        assert_eq!(
            fable.diagnostic,
            expected
                .is_none()
                .then_some(PoolDiagnostic::PoolListMissing)
        );
    }
}

#[test]
fn legacy_plan_prices_without_quota_pools_leave_all_coverage_measurable() {
    let conn = fixture();
    pair(&conn, "secondary", WEEK, ALL);
    pair(&conn, FABLE, WEEK, SUBSET);
    let legacy: PlanPricesResponse = serde_json::from_str(include_str!(
        "../../../test-vectors/fusiform-protocol-0.71.0/plan-prices.json"
    ))
    .unwrap();
    assert!(!legacy.prices.is_empty());
    let readings = capture(&conn, START, END);
    let pools = plan_pools(&readings, &legacy.quota_pools);
    assert_eq!(pools.len(), 2);
    assert_eq!(
        by_id(&pools, "secondary").coverage(),
        Some(PoolCoverage::All)
    );
    let fable = by_id(&pools, "Fable");
    assert_eq!(fable.diagnostic, Some(PoolDiagnostic::PoolListMissing));
    assert_eq!(
        (fable.pool_models, fable.pool_models_at_ms),
        (None, Some(END))
    );
}

#[test]
fn ordinal_extras_swapping_in_either_counter_direction_cannot_skip_a_window() {
    let conn = fixture();
    pair(&conn, "primary", SHORT, ALL);
    for (key, old, new) in [("extra:ordinal:0", 60, 10), ("extra:ordinal:1", 10, 60)] {
        sample(&conn, ACCOUNT, (PROVIDER, key), (START, old), (WEEK, ALL));
        sample(&conn, ACCOUNT, (PROVIDER, key), (END, new), (WEEK, ALL));
    }
    let readings = capture(&conn, START, END);
    let pools = plan_pools(&readings, &[]);
    assert_eq!(pools.len(), 1);
    assert_eq!(
        (pools[0].pool_id, pools[0].window_minutes),
        ("primary", 300)
    );
    assert_eq!(pools[0].coverage(), Some(PoolCoverage::All));
}

#[test]
fn only_uniform_known_windows_participate_in_longest_choice() {
    let conn = fixture();
    pair(&conn, "primary", SHORT, ALL);
    let second = (PlanLedger::Account, "other-login");
    vault_plan(&conn, second, Some(PLAN), 0);
    for (key, first, closer, scopes) in [
        ("extra:5:Mixed", WEEK, SHORT, (ALL, ALL)),
        ("extra:4:Null", WEEK, None, (ALL, ALL)),
        ("extra:7:Changed", WEEK, SHORT, (ALL, SUBSET)),
        ("extra:8:Unstated", WEEK, SHORT, (None, SUBSET)),
        ("extra:9:NoMinutes", None, None, (None, SUBSET)),
        ("extra:10:SubsetTime", SHORT, WEEK, (SUBSET, SUBSET)),
        ("extra:9:NoListYet", WEEK, SHORT, (SUBSET, SUBSET)),
    ] {
        sample(
            &conn,
            ACCOUNT,
            (PROVIDER, key),
            (START, 10),
            (first, scopes.0),
        );
        sample(
            &conn,
            ACCOUNT,
            (PROVIDER, key),
            (END, 40),
            (closer, scopes.1),
        );
    }
    // Two values at one instant use the larger duration, even if the
    // newest value at a strictly later instant would have been smaller.
    sample(
        &conn,
        second,
        (PROVIDER, "extra:5:Mixed"),
        (START, 10),
        (WEEK, ALL),
    );
    sample(
        &conn,
        second,
        (PROVIDER, "extra:5:Mixed"),
        (END, 40),
        (WEEK, ALL),
    );
    let lists = [list(CATALOG_PROVIDER, "SubsetTime", &["anthropic/fable"])];
    let readings = capture(&conn, START, END);
    let pools = plan_pools(&readings, &lists);
    assert_eq!(pools.len(), 7);
    for (id, minutes, diagnostic, scope) in [
        (
            "Mixed",
            10_080,
            Some(PoolDiagnostic::WindowChanged),
            PoolScope::All,
        ),
        (
            "Null",
            10_080,
            Some(PoolDiagnostic::WindowChanged),
            PoolScope::All,
        ),
        (
            "Changed",
            300,
            Some(PoolDiagnostic::ScopeChanged),
            PoolScope::Unknown,
        ),
        (
            "Unstated",
            300,
            Some(PoolDiagnostic::ScopeUnstated),
            PoolScope::Unknown,
        ),
        (
            "SubsetTime",
            10_080,
            Some(PoolDiagnostic::WindowChanged),
            PoolScope::Subset,
        ),
        (
            "NoListYet",
            300,
            Some(PoolDiagnostic::WindowChanged),
            PoolScope::Subset,
        ),
        ("primary", 300, None, PoolScope::All),
    ] {
        let row = by_id(&pools, id);
        assert_eq!(
            (row.window_minutes, row.diagnostic, row.pool_scope),
            (minutes, diagnostic, scope),
            "{id}"
        );
        assert_eq!(
            row.pool_models_at_ms,
            (scope == PoolScope::Subset).then_some(END)
        );
    }
    assert_eq!(
        by_id(&pools, "SubsetTime").pool_models,
        Some(lists[0].models.as_slice())
    );
    assert_eq!(by_id(&pools, "NoListYet").pool_models, None);
}

#[test]
fn equal_longest_model_sets_tie_independent_of_window_and_list_order() {
    for reversed in [false, true] {
        let conn = fixture();
        let mut windows = [("primary", WEEK), ("secondary", WEEK), ("tertiary", SHORT)];
        if reversed {
            windows.reverse();
        }
        for (key, duration) in windows {
            pair(&conn, key, duration, SUBSET);
        }
        let a = "anthropic/a";
        let b = "anthropic/b";
        let mut lists = [
            list(CATALOG_PROVIDER, "primary", &[a, b]),
            list(CATALOG_PROVIDER, "secondary", &[b, a, a]),
            list(CATALOG_PROVIDER, "tertiary", &[b, a]),
        ];
        if reversed {
            lists.reverse();
            for list in &mut lists {
                list.models.reverse();
            }
        }
        let readings = capture(&conn, START, END);
        let pools = plan_pools(&readings, &lists);
        assert_eq!(
            pools.iter().map(|pool| pool.pool_id).collect::<Vec<_>>(),
            ["primary", "secondary"]
        );
        for row in pools {
            assert_eq!(row.diagnostic, Some(PoolDiagnostic::WindowTie));
            assert_eq!(row.window_minutes, 10_080);
            assert_eq!(row.pool_models_at_ms, Some(END));
            assert!(row.coverage().unwrap().covers_model(Some(a)));
            assert_eq!(row.reading_runs[0].readings.len(), 2);
        }
    }
}

#[test]
fn six_pre_cut_days_inherit_the_scope_of_the_first_post_cut_reading() {
    let start = CUT - 6 * DAY;
    let end = CUT + DAY;
    for post_cut_scope in [ALL, None] {
        let conn = fixture();
        for day in 0..=7 {
            let at = start + day * DAY;
            let scope = if at < CUT { None } else { post_cut_scope };
            sample(
                &conn,
                ACCOUNT,
                (PROVIDER, "secondary"),
                (at, day * 5),
                (WEEK, scope),
            );
        }
        let readings = capture(&conn, start, end);
        let pools = plan_pools(&readings, &[]);
        let [pool] = pools.as_slice() else {
            panic!("expected one pool: {pools:?}")
        };
        let expected_scope = post_cut_scope.map(|_| WindowModelScope::All);
        if expected_scope.is_some() {
            assert_eq!(pool.coverage(), Some(PoolCoverage::All));
        } else {
            assert_eq!(pool.diagnostic, Some(PoolDiagnostic::ScopeUnstated));
        }
        let [run] = pool.reading_runs.as_slice() else {
            panic!("expected one run: {pool:?}")
        };
        assert_eq!(run.readings.len(), 8);
        assert_eq!(run.readings[0].fetched_at_ms(), start);
        assert_eq!(run.readings[7].fetched_at_ms(), end);
        assert!(run
            .readings
            .iter()
            .all(|reading| reading.model_scope == expected_scope));
        let stored_scope: Option<String> = conn
            .query_row(
                "SELECT model_scope FROM window_sample WHERE fetched_at_instant=?1",
                [start * NANOS_PER_MS],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_scope, None);
    }
}

#[test]
fn a_post_cut_unstated_opener_does_not_inherit_from_an_all_closer() {
    let conn = fixture();
    sample(
        &conn,
        ACCOUNT,
        (PROVIDER, "primary"),
        (START - 1, 9),
        (SHORT, None),
    );
    sample(
        &conn,
        ACCOUNT,
        (PROVIDER, "primary"),
        (END + 1, 40),
        (SHORT, ALL),
    );
    let readings = capture(&conn, START, END);
    let pools = plan_pools(&readings, &[]);
    let [pool] = pools.as_slice() else {
        panic!("expected one pool: {pools:?}")
    };
    assert_eq!(pool.diagnostic, Some(PoolDiagnostic::ScopeUnstated));
    assert_eq!(pool.pool_scope, PoolScope::Unknown);
}

#[test]
fn inheritance_uses_the_earliest_post_cut_reading_even_outside_the_interval() {
    for earliest_scope in [None, ALL] {
        let conn = fixture();
        vault_plan(&conn, CREDENTIAL, Some(PLAN), 0);
        let (start, end) = (CUT - DAY, CUT - 1);
        for (at, used) in [(start, 10), (end, 20)] {
            sample(
                &conn,
                CREDENTIAL,
                (PROVIDER, FABLE),
                (at, used),
                (WEEK, None),
            );
        }
        // An account and credential with the same id are different logins.
        // So are an identically named pool at another provider or another pool.
        sample(&conn, ACCOUNT, (PROVIDER, FABLE), (CUT, 30), (WEEK, SUBSET));
        sample(
            &conn,
            CREDENTIAL,
            ("other-provider", FABLE),
            (CUT, 30),
            (WEEK, SUBSET),
        );
        sample(
            &conn,
            CREDENTIAL,
            (PROVIDER, "primary"),
            (CUT, 30),
            (WEEK, SUBSET),
        );
        sample(
            &conn,
            CREDENTIAL,
            (PROVIDER, FABLE),
            (CUT + DAY, 30),
            (WEEK, earliest_scope),
        );
        sample(
            &conn,
            CREDENTIAL,
            (PROVIDER, FABLE),
            (CUT + 2 * DAY, 40),
            (WEEK, ALL),
        );
        let readings = capture(&conn, start, end);
        let pools = plan_pools(&readings, &[]);
        let [pool] = pools.as_slice() else {
            panic!("expected one pool: {pools:?}")
        };
        assert_eq!(pool.pool_id, "Fable");
        assert_eq!(pool.reading_runs[0].login.ledger, PlanLedger::Credential);
        assert_eq!(
            pool.diagnostic,
            earliest_scope
                .is_none()
                .then_some(PoolDiagnostic::ScopeUnstated)
        );
    }
}

#[test]
fn login_plan_pairs_do_not_bridge_a_switch_or_mix_denominators() {
    let conn = fixture();
    let other_plan = "anthropic/team";
    let middle = START + DAY / 2;
    let no_plan = (PlanLedger::Account, "no-plan");
    vault_plan(&conn, CREDENTIAL, Some(other_plan), START);
    vault_plan(&conn, ACCOUNT, Some(other_plan), middle);
    vault_plan(&conn, ACCOUNT, Some(PLAN), middle + 1);
    for (at, used) in [(START, 10), (START + 100, 20), (END - 100, 30), (END, 40)] {
        sample(
            &conn,
            ACCOUNT,
            (PROVIDER, "primary"),
            (at, used),
            (SHORT, ALL),
        );
        sample(
            &conn,
            CREDENTIAL,
            (PROVIDER, "primary"),
            (at, used),
            (SHORT, ALL),
        );
        sample(
            &conn,
            ACCOUNT,
            (PROVIDER, "secondary"),
            (at, used),
            (WEEK, ALL),
        );
        sample(
            &conn,
            no_plan,
            (PROVIDER, "secondary"),
            (at, used),
            (WEEK, None),
        );
    }
    // A reading pair whose opening instant predates its first plan gets no row.
    let late = (PlanLedger::Account, "late-login");
    vault_plan(&conn, late, Some(PLAN), middle);
    for at in [START, END] {
        sample(&conn, late, (PROVIDER, FABLE), (at, 10), (WEEK, SUBSET));
    }
    let readings = capture(&conn, START, END);
    let pools = plan_pools(&readings, &[]);
    assert_eq!(pools.len(), 2);
    let account = by_id(&pools, "secondary");
    assert_eq!((account.plan, account.diagnostic), (PLAN, None));
    assert_eq!(account.reading_runs.len(), 2);
    for (run, from, to) in [
        (&account.reading_runs[0], START, START + 100),
        (&account.reading_runs[1], END - 100, END),
    ] {
        assert_eq!(run.login.ledger, PlanLedger::Account);
        assert_eq!(run.login.id, ACCOUNT.1);
        assert_eq!(
            run.readings
                .iter()
                .map(PoolReading::fetched_at_ms)
                .collect::<Vec<_>>(),
            [from, to]
        );
    }
    let credential = by_id(&pools, "primary");
    assert_eq!((credential.plan, credential.diagnostic), (other_plan, None));
    assert_eq!(credential.reading_runs.len(), 1);
    assert_eq!(
        credential.reading_runs[0].login.ledger,
        PlanLedger::Credential
    );
    assert_eq!(
        readings.plan_at(PlanLedger::Account, ACCOUNT.1, middle),
        Some(other_plan)
    );
    assert_eq!(
        readings.plan_at(PlanLedger::Account, ACCOUNT.1, middle + 1),
        Some(PLAN)
    );
}
