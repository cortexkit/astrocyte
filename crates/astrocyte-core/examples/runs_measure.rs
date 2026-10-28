//! Opt-in native benchmark for an authorized disposable store copy. Never send
//! private stores to a build server. Output contains counts, not stored ids.
use std::{path::Path, time::Instant};

use astrocyte_core::{ingest, report::SummaryMaintenance, runs, store};
use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
use rusqlite::{params, Connection};
use serde_json::json;

fn emit(value: serde_json::Value) {
    println!("{value}");
}
fn count(c: &Connection, table: &str) -> i64 {
    c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}
fn fill(path: &Path) {
    let store = open_sqlite(&StorageDescriptor {
        module_id: "astrocyte-runs-measure".into(),
        storage_namespace: "astrocyte".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: path.to_string_lossy().into_owned(),
        },
    })
    .unwrap();
    store
        .with_conn(|c| {
            store::initialize_connection(c).unwrap();
            c.pragma_update(None, "synchronous", "FULL")
        })
        .unwrap();
    let start = Instant::now();
    store::migrate_store(&store).unwrap();
    emit(json!({"check":"migration","seconds":start.elapsed().as_secs_f64()}));
    let c = Connection::open(path).unwrap();
    c.execute(
        "UPDATE summary_audit_state SET next_due_ms=9223372036854775807 WHERE id=1",
        [],
    )
    .unwrap();
    let mut maintenance = SummaryMaintenance::default();
    let start = Instant::now();
    let mut chunks = 0_u64;
    while count(&c, "summary_dirty_hour") != 0 {
        let tx = c.unchecked_transaction().unwrap();
        let ready = maintenance.read_chunk(&tx, 1).unwrap();
        tx.commit().unwrap();
        if ready {
            let tx = c.unchecked_transaction().unwrap();
            let outcome = maintenance.publish(&tx, 1).unwrap();
            assert!(
                !outcome.deferred,
                "hour exceeded the existing publication budget"
            );
            tx.commit().unwrap();
        }
        chunks += 1;
        if chunks.is_multiple_of(100) {
            emit(
                json!({"check":"backfill_progress","chunks":chunks,"dirty_hours":count(&c,"summary_dirty_hour"),"seconds":start.elapsed().as_secs_f64()}),
            );
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    let size: i64 = c
        .query_row(
            "SELECT sum(pgsize) FROM dbstat WHERE name LIKE 'summary_run_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    emit(
        json!({"check":"backfill","seconds":seconds,"cells":count(&c,"summary_run_cell"),"bytes":size,"chunks":chunks}),
    );
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}
fn reads(path: &Path) {
    let c = Connection::open(path).unwrap();
    let latest: i64 = c
        .query_row("SELECT max(occurred_at_ms) FROM usage_event", [], |r| {
            r.get(0)
        })
        .unwrap();
    let to = latest + 1;
    let today = latest.div_euclid(86_400_000) * 86_400_000;
    for (label, from) in [
        ("Today", today),
        ("7 days", to - 7 * 86_400_000),
        ("30 days", to - 30 * 86_400_000),
    ] {
        eprintln!("{}", json!({"case":label}));
        let tx = c.unchecked_transaction().unwrap();
        let start = Instant::now();
        let reply = runs::list(
            &tx,
            &runs::ListRequest {
                from_ms: from,
                to_ms: to,
                harness: None,
                limit: None,
                after: None,
            },
        )
        .unwrap();
        let body = serde_json::to_vec(&reply).unwrap();
        emit(
            json!({"check":"list","range":label,"milliseconds":start.elapsed().as_secs_f64()*1000.0,"rows":reply.totals.rows,"steps":reply.totals.steps,"reply_bytes":body.len()}),
        );
        tx.commit().unwrap();
    }
    let session: String = c.query_row("SELECT session FROM usage_event WHERE producer='opencode' AND session IS NOT NULL GROUP BY session ORDER BY count(*) DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    let run: String = c.query_row("SELECT run_ref FROM usage_event WHERE producer='broca' AND run_ref IS NOT NULL GROUP BY run_ref ORDER BY count(*) DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    for (label, key) in [
        (
            "largest OpenCode session",
            runs::RunKey::Session {
                harness: "opencode".into(),
                session,
            },
        ),
        (
            "BROCA run",
            runs::RunKey::Run {
                harness: "broca".into(),
                run_id: run,
            },
        ),
    ] {
        eprintln!("{}", json!({"case":label}));
        let tx = c.unchecked_transaction().unwrap();
        let start = Instant::now();
        let reply = runs::detail(
            &tx,
            &runs::DetailRequest {
                key,
                limit: None,
                after: None,
            },
        )
        .unwrap();
        let body = serde_json::to_vec(&reply).unwrap();
        emit(
            json!({"check":"detail","key_type":label,"milliseconds":start.elapsed().as_secs_f64()*1000.0,"items_on_page":reply.items.len(),"reply_bytes":body.len()}),
        );
        tx.commit().unwrap();
    }
}

// A progress callback counts every executed VM instruction, including trigger
// programs and statements prepared inside ingest. Timing this callback is not a
// benchmark: only its integer count and the committed WAL frames are compared.
unsafe extern "C" fn tick(p: *mut std::ffi::c_void) -> std::ffi::c_int {
    unsafe {
        *p.cast::<u64>() += 1;
    }
    0
}
fn measured(c: &mut Connection, work: impl FnOnce(&mut Connection)) -> (u64, i64) {
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    let mut steps = 0_u64;
    unsafe {
        rusqlite::ffi::sqlite3_progress_handler(
            c.handle(),
            1,
            Some(tick),
            std::ptr::from_mut(&mut steps).cast(),
        );
    }
    work(c);
    unsafe {
        rusqlite::ffi::sqlite3_progress_handler(c.handle(), 0, None, std::ptr::null_mut());
    }
    let frames = c
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| r.get(1))
        .unwrap();
    (steps, frames)
}
fn writes(path: &Path, kind: &str) {
    let mut c = Connection::open(path).unwrap();
    store::initialize_connection(&c).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        .unwrap();
    let latest: i64 = c
        .query_row("SELECT max(occurred_at_ms) FROM usage_event", [], |r| {
            r.get(0)
        })
        .unwrap();
    for producer in ["opencode", "broca"] {
        let requests: Vec<ingest::EventRecordRequest> = (0..100).map(|i| serde_json::from_value(json!({
            "producer":producer,"producer_ref":format!("runs-measure-{producer}-{i}"),"session":"ses_synthetic_measurement",
            "run_ref":if producer=="broca" { Some(format!("run-sid-SyntheticMeasurement{i}")) } else {None},
            "occurred_at_ms":latest+i,"provider":"synthetic","model":"synthetic","charge_basis":"metered_api",
            "measurements":(["input","output","reasoning","cache_read","cache_write"].map(|role|json!({"role":role,"unit":"token","kind":"billed","source":"synthetic","quantity_units":100,"quantity_exponent":0})))
        })).unwrap()).collect();
        let mut ids = Vec::new();
        let (steps, frames) = measured(&mut c, |c| {
            for request in &requests {
                ids.push(ingest::record_event(c, request, latest).unwrap().event_id);
            }
        });
        emit(
            json!({"check":"ingest","schema":kind,"producer":producer,"events":requests.len(),"vm_steps":steps,"wal_frames":frames}),
        );
        let (steps, frames) = measured(&mut c, |c| {
            for id in &ids {
                let tx = c.transaction().unwrap();
                tx.execute(
                    "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id=?1",
                    [id],
                )
                .unwrap();
                for role in ["input", "output", "reasoning", "cache_read", "cache_write"] {
                    tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token',1,'priced','metered_api',0,'USD',100,1,9,'none','synthetic',1,'{}','pricing_pass',1)",params![id,role]).unwrap();
                }
                tx.commit().unwrap();
            }
        });
        emit(
            json!({"check":"pricing","schema":kind,"producer":producer,"events":requests.len(),"vm_steps":steps,"wal_frames":frames}),
        );
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "runs_measure <fill|reads|writes|compare> <disposable-copy.db>"
    );
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "fill" => fill(path),
        "reads" => {
            std::env::set_var("ASTROCYTE_RUNS_PROFILE", "1");
            for round in 1..=5 {
                let load = std::process::Command::new("uptime").output().unwrap();
                let load = String::from_utf8_lossy(&load.stdout);
                emit(json!({"round":round,"uptime":load.trim()}));
                eprintln!("{}", json!({"round":round,"uptime":load.trim()}));
                reads(path);
            }
        }
        "writes" => writes(path, "supplied"),
        "compare" => compare_key_ranges(path),
        _ => panic!("unknown mode"),
    }
}

fn compare_key_ranges(path: &Path) {
    let c = Connection::open(path).unwrap();
    let latest: i64 = c
        .query_row("SELECT max(occurred_at_ms) FROM usage_event", [], |r| {
            r.get(0)
        })
        .unwrap();
    let to = latest + 1;
    let hour = 3_600_000_i64;
    let keys:Vec<String>=c.prepare("SELECT DISTINCT run_key FROM summary_run_cell WHERE kind=0 AND hour_number=?1 ORDER BY run_key").unwrap().query_map([latest.div_euclid(hour)],|r|r.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
    let values = keys
        .iter()
        .map(|key| format!("('{}')", key.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    for (case, from) in [
        ("Today", latest.div_euclid(86_400_000) * 86_400_000),
        ("7 days", to - 7 * 86_400_000),
        ("30 days", to - 30 * 86_400_000),
    ] {
        let mut stmt=c.prepare("SELECT hour_number FROM summary_hour h WHERE kind=0 AND built_revision=revision AND format_version=2 AND hour_number*3600000>=?1 AND (hour_number+1)*3600000<=?2 AND NOT EXISTS(SELECT 1 FROM summary_dirty_hour d WHERE d.kind=0 AND d.hour_number=h.hour_number) ORDER BY hour_number").unwrap();
        let clean = stmt
            .query_map(params![from, to], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let low = *clean.first().unwrap();
        let high = *clean.last().unwrap();
        let holes = (low..=high)
            .filter(|h| !clean.contains(h))
            .map(|h| h.to_string())
            .collect::<Vec<_>>();
        let filters = [
            (
                "hour_IN",
                format!(
                    "c.kind=0 AND c.hour_number IN ({})",
                    clean
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ),
            (
                "hour_range",
                format!(
                    "c.kind=0 AND c.hour_number BETWEEN {low} AND {high}{}",
                    if holes.is_empty() {
                        String::new()
                    } else {
                        format!(" AND c.hour_number NOT IN ({})", holes.join(","))
                    }
                ),
            ),
        ];
        let mut expected = None;
        for (shape, filter) in filters {
            let sql=format!("WITH keys(run_key) AS (VALUES {values}) SELECT keys.run_key,MAX(c.last_ms) FROM keys CROSS JOIN summary_run_cell c INDEXED BY summary_run_key_time WHERE c.run_key=keys.run_key AND ({filter}) GROUP BY keys.run_key");
            let mut stmt = c.prepare(&sql).unwrap();
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            if let Some(expected) = &expected {
                assert_eq!(expected, &rows);
            } else {
                expected = Some(rows.clone());
            }
            emit(
                json!({"check":"key_range_vm_comparison","case":case,"shape":shape,"sample_keys":keys.len(),"clean_hours":clean.len(),"vm_steps":stmt.get_status(rusqlite::StatementStatus::VmStep),"returned_keys":rows.len()}),
            );
        }
    }
}
