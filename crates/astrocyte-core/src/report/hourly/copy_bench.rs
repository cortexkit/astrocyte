//! Opt-in native measurements against an explicitly supplied disposable copy.
//! Replies are compared in memory; artifacts contain hashes, never account/project values.
use super::*;
use crate::{store, verdict};
use cortexkit_store::{open_sqlite, Isolation, SqliteStore, StorageBackend, StorageDescriptor};
use rusqlite::OpenFlags;
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

const NOW: i64 = 1_790_000_000_000;
fn emit(value: serde_json::Value) {
    let line = serde_json::to_string(&value).unwrap();
    println!("{line}");
    if let Ok(path) = std::env::var("ASTROCYTE_SUMMARY_BENCH_OUT") {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    }
}
fn load() -> String {
    std::process::Command::new("uptime")
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|_| "uptime unavailable".into())
}
#[cfg(all(
    target_pointer_width = "64",
    any(target_os = "macos", target_os = "linux")
))]
fn cpu_time() -> Option<std::time::Duration> {
    #[repr(C)]
    struct Timespec {
        seconds: i64,
        nanos: i64,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: i32, time: *mut Timespec) -> i32;
    }
    // CLOCK_PROCESS_CPUTIME_ID differs between Darwin (12) and Linux (2).
    #[cfg(target_os = "macos")]
    let clock = 12;
    #[cfg(target_os = "linux")]
    let clock = 2;
    let mut time = Timespec {
        seconds: 0,
        nanos: 0,
    };
    // Both supported 64-bit ABIs use two signed longs, valid through this call.
    if unsafe { clock_gettime(clock, &mut time) } != 0 {
        return None;
    }
    Some(std::time::Duration::new(
        u64::try_from(time.seconds).ok()?,
        u32::try_from(time.nanos).ok()?,
    ))
}
#[cfg(not(all(
    target_pointer_width = "64",
    any(target_os = "macos", target_os = "linux")
)))]
fn cpu_time() -> Option<std::time::Duration> {
    None
}
fn cpu_since(start: Option<std::time::Duration>) -> Option<f64> {
    Some(cpu_time()?.checked_sub(start?)?.as_secs_f64())
}

fn writer(path: &Path) -> SqliteStore {
    let store = open_sqlite(&StorageDescriptor {
        module_id: "astrocyte-summary-benchmark".into(),
        storage_namespace: "astrocyte".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: path.to_string_lossy().into_owned(),
        },
    })
    .unwrap();
    store
        .with_conn(|conn| {
            store::initialize_connection(conn).unwrap();
            conn.pragma_update(None, "synchronous", "FULL")
        })
        .unwrap();
    store
}
fn reader(path: &Path) -> Connection {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    conn.pragma_update(None, "foreign_keys", true).unwrap();
    configure_report_reader(&conn).unwrap();
    conn.pragma_update(None, "query_only", true).unwrap();
    conn
}
fn count(conn: &Connection, table: &str) -> u64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}
fn bytes(conn: &Connection) -> u64 {
    let pages: u64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    let size: u64 = conn
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    pages.checked_mul(size).unwrap()
}
fn advance(
    store: &SqliteStore,
    reader: &Connection,
    maintenance: &mut SummaryMaintenance,
) -> SummaryOutcome {
    let tx = reader.unchecked_transaction().unwrap();
    let ready = maintenance.read_chunk(&tx, NOW).unwrap();
    tx.commit().unwrap();
    if !ready {
        return SummaryOutcome::default();
    }
    store
        .with_conn(|conn| {
            let tx = conn.unchecked_transaction()?;
            let outcome = maintenance.publish(&tx, NOW).unwrap();
            tx.commit()?;
            Ok(outcome)
        })
        .unwrap()
}
fn build(path: &Path) {
    let store = writer(path);
    let before = store.with_conn(|conn| Ok(bytes(conn))).unwrap();
    let original_facts = store
        .with_conn(|conn| {
            Ok([
                count(conn, "usage_event"),
                count(conn, "measurement"),
                count(conn, "priced_charge"),
                count(conn, "adjustment"),
            ])
        })
        .unwrap();
    let started = Instant::now();
    let initial_load = load();
    let outcome = store::migrate_store(&store).unwrap();
    emit(
        serde_json::json!({"mode":"migration","seconds":started.elapsed().as_secs_f64(),"recorded":outcome.recorded,"applied":outcome.applied,"load_before":initial_load,"load_after":load()}),
    );
    let reader = reader(path);
    let facts_before = [
        count(&reader, "usage_event"),
        count(&reader, "measurement"),
        count(&reader, "priced_charge"),
        count(&reader, "adjustment"),
    ];
    assert_eq!(
        original_facts, facts_before,
        "migration must preserve per-call facts"
    );
    let started = Instant::now();
    let initial_load = load();
    let mut maintenance = SummaryMaintenance::default();
    let mut chunks = 0_u64;
    let mut hours = 0_u64;
    let mut examined = 0_u64;
    let mut maximum = 0.0_f64;
    while count(&reader, "summary_dirty_hour") > 0 {
        maintenance.prefer_audit = false;
        let chunk = Instant::now();
        let outcome = advance(&store, &reader, &mut maintenance);
        maximum = maximum.max(chunk.elapsed().as_secs_f64());
        chunks += 1;
        assert!(
            !outcome.deferred,
            "a real hour exceeds the publication row budget"
        );
        if outcome.published {
            hours += 1;
            examined += outcome.examined;
        }
        if chunks.is_multiple_of(100) {
            emit(
                serde_json::json!({"mode":"build_progress","chunks":chunks,"hours":hours,"examined":examined,"remaining_hours":count(&reader,"summary_dirty_hour"),"seconds":started.elapsed().as_secs_f64()}),
            );
        }
    }
    let after = bytes(&reader);
    assert_eq!(
        facts_before,
        [
            count(&reader, "usage_event"),
            count(&reader, "measurement"),
            count(&reader, "priced_charge"),
            count(&reader, "adjustment")
        ],
        "building summaries must not change immutable ledger rows"
    );
    emit(
        serde_json::json!({"mode":"build_complete","seconds":started.elapsed().as_secs_f64(),"chunks":chunks,"hours":hours,"examined":examined,"longest_chunk_seconds":maximum,"database_bytes_before_migration":before,"database_bytes_after_fill":after,"additional_bytes":after-before,"base_row_counts":facts_before,"load_before":initial_load,"load_after":load()}),
    );
    for table in [
        "summary_value",
        "summary_hour",
        "summary_dirty_hour",
        "summary_cell",
        "summary_quantity",
        "summary_money",
        "summary_cost_frequency",
        "summary_audit_state",
    ] {
        emit(serde_json::json!({"mode":"row_count","table":table,"rows":count(&reader,table)}));
    }
    store
        .with_conn(|conn| conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)"))
        .unwrap();
}
fn audit(path: &Path) {
    let store = writer(path);
    store.with_conn(|conn|conn.execute_batch("UPDATE summary_audit_state SET after_kind=NULL,after_hour=NULL,next_due_ms=0,examined=0,mismatches=0,hours=0,chunks=0,spent_ms=0,max_chunk_ms=0,started_at_ms=NULL WHERE id=1")).unwrap();
    let reader = reader(path);
    let mut maintenance = SummaryMaintenance::default();
    let started = Instant::now();
    let initial_load = load();
    let mut chunks = 0_u64;
    loop {
        maintenance.prefer_audit = true;
        let outcome = advance(&store, &reader, &mut maintenance);
        chunks += 1;
        assert!(
            !outcome.mismatch,
            "copy audit found a base/summary mismatch"
        );
        if outcome.audit_completed {
            assert_eq!(outcome.mismatches, 0);
            assert_eq!(outcome.examined, count(&reader, "usage_event"));
            emit(
                serde_json::json!({"mode":"audit_complete","seconds":started.elapsed().as_secs_f64(),"chunks":chunks,"hours":outcome.hours,"examined":outcome.examined,"mismatches":outcome.mismatches,"reduce_compare_ms":outcome.spent_ms,"load_before":initial_load,"load_after":load()}),
            );
            break;
        }
    }
}
fn same(name: &str, expected: Vec<u8>, actual: Vec<u8>, seconds: f64, accelerated: bool) {
    if expected != actual {
        let position = expected
            .iter()
            .zip(&actual)
            .position(|(left, right)| left != right)
            .unwrap_or(expected.len().min(actual.len()));
        panic!(
            "{name}: byte mismatch at {position}; lengths expected={} actual={}",
            expected.len(),
            actual.len()
        );
    }
    emit(
        serde_json::json!({"mode":"equivalence","case":name,"bytes":actual.len(),"base_sha256":hex::encode(Sha256::digest(&expected)),"summary_sha256":hex::encode(Sha256::digest(&actual)),"byte_identical":true,"accelerated":accelerated,"seconds":seconds}),
    );
}
fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}
fn requests() -> Vec<ReportRequest> {
    let names = [
        "provider",
        "model",
        "account",
        "auth_selection",
        "charge_basis",
        "work_class",
        "project",
    ];
    let mut groups = vec![Vec::new()];
    for name in names {
        groups.push(vec![name]);
    }
    for left in names {
        for right in names {
            if left != right {
                groups.push(vec![left, right]);
            }
        }
    }
    groups.push(names.to_vec());
    groups.push(names.into_iter().rev().collect());
    let mut cases = Vec::new();
    for timezone in [
        "UTC",
        "Europe/Madrid",
        "Asia/Kolkata",
        "America/St_Johns",
        "Pacific/Chatham",
    ] {
        for grain in ["day", "week", "month"] {
            for grouping in &groups {
                cases.push(ReportRequest {
                    timezone: timezone.into(),
                    grain: grain.into(),
                    grouping: grouping.iter().map(|name| (*name).into()).collect(),
                });
            }
        }
    }
    cases
}

fn budget_requests(conn: &Connection) -> Vec<(&'static str, verdict::VerdictRequest)> {
    let project = conn
        .query_row(
            "SELECT project_root FROM usage_event WHERE project_root IS NOT NULL LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap();
    let provider = conn
        .query_row(
            "SELECT provider FROM usage_event WHERE provider IS NOT NULL LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap();
    let model = conn
        .query_row(
            "SELECT model FROM usage_event WHERE model IS NOT NULL LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap();
    vec![
        (
            "budget_all",
            verdict::VerdictRequest {
                project_root: None,
                provider: None,
                model: None,
                est_cost_nanos: Some(123),
            },
        ),
        (
            "budget_project",
            verdict::VerdictRequest {
                project_root: project,
                provider: None,
                model: None,
                est_cost_nanos: None,
            },
        ),
        (
            "budget_provider",
            verdict::VerdictRequest {
                project_root: None,
                provider,
                model: None,
                est_cost_nanos: None,
            },
        ),
        (
            "budget_model",
            verdict::VerdictRequest {
                project_root: None,
                provider: None,
                model,
                est_cost_nanos: None,
            },
        ),
    ]
}
fn matrix(path: &Path, half_dirty: bool) {
    if half_dirty {
        let store = writer(path);
        store.with_conn(|conn| {
            conn.execute("INSERT OR IGNORE INTO summary_dirty_hour SELECT kind,hour_number FROM (SELECT kind,hour_number,ROW_NUMBER() OVER(ORDER BY kind,hour_number) AS ordinal FROM summary_hour) WHERE ordinal%2=0",[])?;Ok(())
        }).unwrap();
    }
    let conn = reader(path);
    let tx = conn.unchecked_transaction().unwrap();
    let dirty = count(&tx, "summary_dirty_hour");
    let hours = count(&tx, "summary_hour");
    assert_eq!(dirty, if half_dirty { hours / 2 } else { 0 });
    let started = Instant::now();
    let initial_load = load();
    // The unchanged base reducer may reuse this one immutable source snapshot.
    // Every optimized answer still executes its real summary/dirty read path.
    let oracle = load_ledger(&tx, &DIMENSIONS).unwrap();
    let offset = std::env::var("ASTROCYTE_SUMMARY_CASE_FROM")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(0);
    let limit = std::env::var("ASTROCYTE_SUMMARY_CASE_LIMIT")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(usize::MAX);
    let mut cases = 0_u64;
    let mut accelerated = 0_u64;
    let requests = requests();
    let expected_spend_cases = requests.len().saturating_sub(offset).min(limit);
    assert!(
        expected_spend_cases > 0,
        "the selected matrix must compare spend reports"
    );
    let mut spend_cases = 0;
    for (index, request) in requests.into_iter().enumerate().skip(offset).take(limit) {
        let started = Instant::now();
        let expected = spend_report_from_ledger(
            &tx,
            &oracle,
            &parse_grouping(&request.grouping).unwrap(),
            Grain::parse(&request.grain).unwrap(),
            request.timezone.parse().unwrap(),
            NOW,
        )
        .unwrap();
        let summary = spend(&tx, &request, NOW).unwrap();
        let fast = summary.is_some();
        assert!(fast || half_dirty, "caught-up spend unexpectedly fell back");
        let actual =
            summary.unwrap_or_else(|| spend_report_base(&tx, &request, NOW, None).unwrap());
        same(
            &format!(
                "spend/{index:04}/{}/{}/{}",
                request.timezone,
                request.grain,
                request.grouping.join(",")
            ),
            json(&expected),
            json(&actual),
            started.elapsed().as_secs_f64(),
            fast,
        );
        cases += 1;
        spend_cases += 1;
        accelerated += u64::from(fast);
    }
    assert_eq!(spend_cases, expected_spend_cases);
    let start = Instant::now();
    let expected = subscription_report_base(&tx, &SubscriptionReportRequest {}, None).unwrap();
    let actual = subscription(&tx, &SubscriptionReportRequest {})
        .unwrap()
        .expect("subscription summary path unavailable");
    same(
        "subscription",
        json(&expected),
        json(&actual),
        start.elapsed().as_secs_f64(),
        true,
    );
    cases += 1;
    let start = Instant::now();
    let expected = work_class_report_base(&tx, &WorkClassReportRequest {}).unwrap();
    let actual = workclass(&tx)
        .unwrap()
        .expect("workclass summary path unavailable");
    same(
        "workclass",
        json(&expected),
        json(&actual),
        start.elapsed().as_secs_f64(),
        true,
    );
    cases += 1;
    let start = Instant::now();
    let expected = ledger_coverage_base(&tx, &LedgerCoverageRequest {}, NOW).unwrap();
    let actual = coverage(&tx, NOW)
        .unwrap()
        .expect("coverage summary path unavailable");
    same(
        "coverage",
        json(&expected),
        json(&actual),
        start.elapsed().as_secs_f64(),
        true,
    );
    cases += 1;
    for (name, request) in budget_requests(&tx) {
        let start = Instant::now();
        let expected = verdict::evaluate_base(&tx, &request).unwrap();
        let actual = super::verdict(&tx, &request)
            .unwrap()
            .expect("budget summary path unavailable");
        same(
            name,
            json(&expected),
            json(&actual),
            start.elapsed().as_secs_f64(),
            true,
        );
        cases += 1;
    }
    emit(
        serde_json::json!({"mode":"matrix_complete","half_dirty":half_dirty,"dirty_hours":dirty,"hours":hours,"cases":cases,"accelerated_spend_cases":accelerated,"case_offset":offset,"case_limit":if limit==usize::MAX {None}else{Some(limit)},"seconds":started.elapsed().as_secs_f64(),"load_before":initial_load,"load_after":load(),"oracle":"one full source-table snapshot, original reducer; optimized paths re-read summaries and live dirty/seam facts"}),
    );
    tx.commit().unwrap();
}

enum Case {
    Spend(&'static str, ReportRequest),
    Subscription,
    Workclass,
    Coverage,
    Budget(&'static str, verdict::VerdictRequest),
}
impl Case {
    fn name(&self) -> &str {
        match self {
            Self::Spend(name, _) | Self::Budget(name, _) => name,
            Self::Subscription => "subscription",
            Self::Workclass => "workclass",
            Self::Coverage => "coverage",
        }
    }
    fn run(&self, conn: &Connection, summary: bool) -> Vec<u8> {
        match self {
            Self::Spend(_, request) => {
                if summary {
                    json(
                        &spend(conn, request, NOW)
                            .unwrap()
                            .expect("spend summary path unavailable"),
                    )
                } else {
                    json(&spend_report_base(conn, request, NOW, None).unwrap())
                }
            }
            Self::Subscription => {
                if summary {
                    json(
                        &subscription(conn, &SubscriptionReportRequest {})
                            .unwrap()
                            .expect("subscription summary path unavailable"),
                    )
                } else {
                    json(
                        &subscription_report_base(conn, &SubscriptionReportRequest {}, None)
                            .unwrap(),
                    )
                }
            }
            Self::Workclass => {
                if summary {
                    json(
                        &workclass(conn)
                            .unwrap()
                            .expect("workclass summary path unavailable"),
                    )
                } else {
                    json(&work_class_report_base(conn, &WorkClassReportRequest {}).unwrap())
                }
            }
            Self::Coverage => {
                if summary {
                    json(
                        &coverage(conn, NOW)
                            .unwrap()
                            .expect("coverage summary path unavailable"),
                    )
                } else {
                    json(&ledger_coverage_base(conn, &LedgerCoverageRequest {}, NOW).unwrap())
                }
            }
            Self::Budget(_, request) => {
                if summary {
                    json(
                        &super::verdict(conn, request)
                            .unwrap()
                            .expect("budget summary path unavailable"),
                    )
                } else {
                    json(&verdict::evaluate_base(conn, request).unwrap())
                }
            }
        }
    }
}
fn timings(path: &Path) {
    let conn = reader(path);
    assert_eq!(count(&conn, "summary_dirty_hour"), 0);
    let make = |timezone: &str, grain: &str, grouping: &[&str]| ReportRequest {
        timezone: timezone.into(),
        grain: grain.into(),
        grouping: grouping.iter().map(|value| (*value).into()).collect(),
    };
    let mut cases = vec![
        Case::Spend("spend_month_utc", make("UTC", "month", &[])),
        Case::Spend(
            "spend_day_madrid_provider_model",
            make("Europe/Madrid", "day", &["provider", "model"]),
        ),
        Case::Spend(
            "spend_day_kolkata_project_workclass",
            make("Asia/Kolkata", "day", &["project", "work_class"]),
        ),
        Case::Spend(
            "spend_week_stjohns_account_auth",
            make("America/St_Johns", "week", &["account", "auth_selection"]),
        ),
        Case::Spend(
            "spend_month_chatham_all",
            make(
                "Pacific/Chatham",
                "month",
                &[
                    "provider",
                    "model",
                    "account",
                    "auth_selection",
                    "charge_basis",
                    "work_class",
                    "project",
                ],
            ),
        ),
        Case::Subscription,
        Case::Workclass,
        Case::Coverage,
    ];
    for (name, request) in budget_requests(&conn) {
        cases.push(Case::Budget(name, request));
    }
    let tx = conn.unchecked_transaction().unwrap();
    for case in cases {
        let mut before = Vec::new();
        let mut after = Vec::new();
        let mut before_cpu = Vec::new();
        let mut after_cpu = Vec::new();
        for repetition in 0..3 {
            let initial_load = load();
            let cpu = cpu_time();
            let started = Instant::now();
            let expected = case.run(&tx, false);
            let base_seconds = started.elapsed().as_secs_f64();
            let base_cpu = cpu_since(cpu);
            let cpu = cpu_time();
            let started = Instant::now();
            let actual = case.run(&tx, true);
            let summary_seconds = started.elapsed().as_secs_f64();
            let summary_cpu = cpu_since(cpu);
            if expected != actual {
                panic!(
                    "{}: timing run replies differ (private payload withheld)",
                    case.name()
                );
            }
            before.push(base_seconds);
            after.push(summary_seconds);
            if let Some(cpu) = base_cpu {
                before_cpu.push(cpu);
            }
            if let Some(cpu) = summary_cpu {
                after_cpu.push(cpu);
            }
            emit(
                serde_json::json!({"mode":"timing","case":case.name(),"repetition":repetition,"before_seconds":base_seconds,"after_seconds":summary_seconds,"before_cpu_seconds":base_cpu,"after_cpu_seconds":summary_cpu,"byte_identical":true,"bytes":actual.len(),"load_before_pair":initial_load,"load_after_pair":load(),"scope":"domain read plus JSON serialization, no RPC transport or concurrent-reader queue"}),
            );
        }
        before.sort_by(f64::total_cmp);
        after.sort_by(f64::total_cmp);
        before_cpu.sort_by(f64::total_cmp);
        after_cpu.sort_by(f64::total_cmp);
        emit(
            serde_json::json!({"mode":"timing_median","case":case.name(),"samples":3,"before_seconds":before[1],"after_seconds":after[1],"before_cpu_seconds":before_cpu.get(1),"after_cpu_seconds":after_cpu.get(1),"under_one_second_warm":after[1]<1.0,"under_load":true}),
        );
    }
    tx.commit().unwrap();
}

#[test]
#[ignore = "native benchmark only; requires the authorized disposable store copy"]
fn copy_report_equivalence_and_timings() {
    let path = PathBuf::from(
        std::env::var("ASTROCYTE_SUMMARY_BENCH_DB")
            .expect("ASTROCYTE_SUMMARY_BENCH_DB is required"),
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let expected = root
        .join(".cortexkit/scratch/store.db")
        .canonicalize()
        .unwrap();
    assert_eq!(
        path.canonicalize().unwrap(),
        expected,
        "refuse any database except the authorized worktree copy"
    );
    assert!(
        expected.starts_with(&root),
        "the copy must remain inside this worktree"
    );
    let mode = std::env::var("ASTROCYTE_SUMMARY_BENCH_MODE").expect("benchmark mode required");
    emit(
        serde_json::json!({"mode":"start","requested_mode":mode,"uptime":load(),"snapshot_never_transferred":true}),
    );
    match mode.as_str() {
        "build" => build(&path),
        "audit" => audit(&path),
        "matrix-clean" => matrix(&path, false),
        "matrix-dirty" => matrix(&path, true),
        "timings" => timings(&path),
        _ => panic!("unknown benchmark mode"),
    }
}
