use super::*;
use crate::state::test_state;
use rusqlite::OptionalExtension;
use serde_json::{json, Value};
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;

const TRANSCRIPT: &str = include_str!("../tests/vectors/pi-thalamus/session.jsonl");
const ROOTS: &str = include_str!("../tests/vectors/pi-thalamus/roots.json");
static NEVER_STOP: AtomicBool = AtomicBool::new(false);

#[test]
fn synthetic_vector_bytes_are_pinned() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        format!("{:x}", Sha256::digest(ROOTS.as_bytes())),
        "599c836daf4dc78a82bf37a848e295e76fd403cb70e77a259028bd68d94f2603"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(TRANSCRIPT.as_bytes())),
        "ac7d32053fe552cb002aeaef2b26b78d4d0a85afbcd47dc808381e5a13dca1ea"
    );
}

struct Fixture {
    home: TempDir,
    roots: PathBuf,
    folder: PathBuf,
    ack: PathBuf,
    source: PiThalamusSource,
    state: Arc<SharedState>,
}

impl Fixture {
    fn new() -> Self {
        let home = TempDir::new().unwrap();
        let path = fs::canonicalize(home.path()).unwrap();
        let roots = path.join("cortexkit/thalamus-harness/pi/roots.json");
        fs::create_dir_all(roots.parent().unwrap()).unwrap();
        let folder = path.join("cortexkit/thalamus-harness/pi-sessions/worker-one");
        fs::create_dir_all(&folder).unwrap();
        Self {
            roots,
            folder,
            ack: path.join("cortexkit/astrocyte/acks/pi.json"),
            source: PiThalamusSource::from_data_home(path),
            home,
            state: Arc::new(test_state()),
        }
    }
    fn root(&self) -> Value {
        let mut value: Value = serde_json::from_str(ROOTS).unwrap();
        value["roots"][0]["path"] = json!(self.folder);
        value["roots"][0].clone()
    }
    fn declare(&self, entries: Vec<Value>) {
        fs::write(
            &self.roots,
            json!({"version":1,"roots":entries}).to_string(),
        )
        .unwrap();
    }
    fn call(&self) {
        fs::write(self.folder.join("session.jsonl"), TRANSCRIPT).unwrap();
    }
    fn tick(&self, at: i64) -> Option<String> {
        self.source
            .drain_for_health(&self.state, at, &NEVER_STOP)
            .unwrap()
    }
    fn count(&self, table: &str) -> i64 {
        self.state
            .with_conn(|conn| {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                    .map_err(|e| e.to_string())
            })
            .unwrap()
    }
    fn acks(&self) -> Value {
        serde_json::from_slice(&fs::read(&self.ack).unwrap()).unwrap()
    }
    fn managed(&self) {
        self.state.ordered_write(|tx| {
            tx.execute("INSERT INTO thalamus_payer_session VALUES ('pi','pi-synthetic-one','synthetic-owner',0)", []).unwrap();
            Ok(((), true))
        }).unwrap();
    }
}

#[test]
fn missing_roots_is_absent_and_health_unchanged() {
    let f = Fixture::new();
    assert_eq!(f.tick(1001), None);
    let runtime: Option<i64> = f
        .state
        .with_conn(|conn| {
            conn.query_row(
                "SELECT last_success_ms FROM source_runtime WHERE source_key='pi-thalamus'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())
        })
        .unwrap();
    assert_eq!(runtime, None);
    assert!(!f.ack.exists());
}

#[test]
fn null_session_uses_header_for_payer_and_records_owner_and_work_class() {
    let f = Fixture::new();
    f.call();
    f.managed();
    f.declare(vec![f.root()]);
    assert_eq!(f.tick(1001), None);
    assert_eq!(f.count("usage_event"), 0);
    assert_eq!(f.acks()["read_through"], json!([]));
    f.state.ordered_write(|tx| {
        tx.execute("INSERT INTO thalamus_payer_record(attempt_id,seq,incarnation,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref) VALUES ('attempt',1,'first','pi','pi-synthetic-one','main','pi:response:pi-synthetic-response',1002,'anthropic','claude-sonnet-4','metered_api','synthetic-login','synthetic-account')", []).unwrap();
        Ok(((),true))
    }).unwrap();
    f.tick(1002);
    f.state.with_conn(|conn| {
        let row = conn.query_row("SELECT producer_ref,session,work_class,charge_basis,auth_selection,account_ref FROM usage_event", [], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?))).unwrap();
        assert_eq!(row, ("pi:response:pi-synthetic-response".into(),"core-conversation".into(),"synthetic-owner".into(),"metered_api".into(),"synthetic-login".into(),"synthetic-account".into()));
        assert_eq!(conn.query_row("SELECT quantity_units FROM measurement WHERE role='input'", [], |r| r.get::<_,i64>(0)).unwrap(), 11);
        Ok(())
    }).unwrap();
    assert_eq!(
        f.acks()["read_through"],
        json!([{"session":"core-conversation","at_ms":1002}])
    );
}

#[test]
fn header_mismatch_refuses_whole_folder_by_name() {
    let f = Fixture::new();
    f.call();
    fs::write(
        f.folder.join("another.jsonl"),
        TRANSCRIPT
            .replace("pi-synthetic-one", "another-pi-session")
            .replace("pi-synthetic-response", "another-response"),
    )
    .unwrap();
    let mut root = f.root();
    root["session"] = json!("pi-synthetic-one");
    f.declare(vec![root]);
    let reason = f.tick(1001).unwrap();
    assert!(
        reason.contains("worker-one") && reason.contains("session"),
        "{reason}"
    );
    assert_eq!(f.count("usage_event"), 0);
    assert_eq!(f.acks()["read_through"], json!([]));
}

#[test]
fn refuses_path_escapes_and_pi_root_and_nested_folders() {
    for suffix in [
        "outside",
        ".pi/agent/sessions/project",
        "cortexkit/thalamus-harness/pi-sessions/worker-one/nested",
        "cortexkit/thalamus-harness/pi-sessions",
    ] {
        let f = Fixture::new();
        let outside = fs::canonicalize(f.home.path()).unwrap().join(suffix);
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("call.jsonl"), TRANSCRIPT).unwrap();
        let mut root = f.root();
        root["path"] = json!(outside);
        f.declare(vec![root]);
        assert!(f.tick(1001).is_some(), "{suffix}");
        assert_eq!(f.count("usage_event"), 0);
        assert_eq!(f.acks()["read_through"], json!([]));
    }
}

#[cfg(unix)]
#[test]
fn refuses_symlink_escape_and_canonical_aliases() {
    for escape in [true, false] {
        let f = Fixture::new();
        f.call();
        let alias = f.folder.with_file_name("alias");
        let target = if escape {
            f.home.path().join("outside")
        } else {
            f.folder.clone()
        };
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("call.jsonl"), TRANSCRIPT).unwrap();
        std::os::unix::fs::symlink(target, &alias).unwrap();
        let mut root = f.root();
        root["path"] = json!(alias);
        root["owner_session"] = json!("different-owner");
        f.declare(if escape {
            vec![root]
        } else {
            vec![root, f.root()]
        });
        assert!(f.tick(1001).is_some());
        assert_eq!(f.count("usage_event"), 0);
        assert_eq!(f.acks()["read_through"], json!([]));
    }
}

#[test]
fn refuses_duplicate_or_empty_owner_sessions() {
    for duplicate in [true, false] {
        let f = Fixture::new();
        f.call();
        let mut root = f.root();
        if duplicate {
            let other = f.folder.with_file_name("other");
            fs::create_dir_all(&other).unwrap();
            root["path"] = json!(other);
            f.declare(vec![root, f.root()]);
        } else {
            root["owner_session"] = json!("");
            f.declare(vec![root]);
        }
        assert!(f.tick(1001).is_some());
        assert_eq!(f.count("usage_event"), 0);
        assert_eq!(f.acks()["read_through"], json!([]));
    }
}

#[test]
fn acknowledgement_follows_commit_and_failed_commit_retries_calls() {
    let f = Fixture::new();
    f.call();
    f.declare(vec![f.root()]);
    f.state.with_conn(|conn| conn.execute_batch("CREATE TRIGGER refuse_commit BEFORE UPDATE OF last_success_ms ON source_runtime BEGIN SELECT RAISE(ABORT,'refuse commit'); END;").map_err(|e| e.to_string())).unwrap();
    assert!(f
        .source
        .drain_for_health(&f.state, 1001, &NEVER_STOP)
        .is_err());
    assert_eq!(f.count("usage_event"), 0);
    assert!(!f.ack.exists());
    f.state
        .with_conn(|conn| {
            conn.execute_batch("DROP TRIGGER refuse_commit")
                .map_err(|e| e.to_string())
        })
        .unwrap();
    f.tick(1002);
    assert_eq!(f.count("usage_event"), 1);
    assert_eq!(
        f.acks()["read_through"],
        json!([{"session":"core-conversation","at_ms":1002}])
    );
}

#[test]
fn done_empty_folder_and_no_call_file_are_acknowledged_not_before_done_time() {
    for file in [None, Some(""), Some("header")] {
        let f = Fixture::new();
        if file == Some("header") {
            fs::write(
                f.folder.join("empty.jsonl"),
                format!("{}\n", TRANSCRIPT.lines().next().unwrap()),
            )
            .unwrap();
        } else if let Some(contents) = file {
            fs::write(f.folder.join("empty.jsonl"), contents).unwrap();
        }
        let mut root = f.root();
        root["session"] = json!("pi-synthetic-one");
        f.declare(vec![root]);
        f.tick(999);
        assert_eq!(f.acks()["read_through"], json!([]));
        f.tick(1000);
        assert_eq!(
            f.acks()["read_through"],
            json!([{"session":"core-conversation","at_ms":1000}])
        );
        f.tick(2000);
        assert_eq!(f.acks()["read_through"][0]["at_ms"], 1000);
    }
}

#[test]
fn same_call_under_both_roots_is_recorded_once_without_quarantine() {
    let f = Fixture::new();
    f.call();
    let normal = f.home.path().join("pi-root");
    fs::create_dir_all(&normal).unwrap();
    fs::write(normal.join("copy.jsonl"), TRANSCRIPT).unwrap();
    let pi = crate::refresher::pi_source::PiSource::at(&normal);
    crate::refresher::run_iteration_with_pi(&f.state, &[], &pi, &NEVER_STOP);
    assert_eq!(f.count("usage_event"), 1);
    f.declare(vec![f.root()]);
    f.tick(1001);
    assert_eq!(f.count("usage_event"), 1);
    assert_eq!(f.count("rejected_delivery"), 0);
    f.state
        .with_conn(|conn| {
            assert_eq!(
                conn.query_row("SELECT session FROM usage_event", [], |r| r
                    .get::<_, String>(0))
                    .unwrap(),
                "pi-synthetic-one"
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(f.acks()["read_through"].as_array().unwrap().len(), 1);
}

#[test]
fn payer_wait_blocks_ack_through_bound_and_idle_fast_lane_opens_nothing() {
    let f = Fixture::new();
    f.call();
    f.managed();
    f.declare(vec![f.root()]);
    f.tick(1001);
    assert_eq!(f.count("usage_event"), 0);
    astrocyte_readers::read_io::reset_counts();
    f.source.drain_fast(&f.state, 600_999, &NEVER_STOP).unwrap();
    assert_eq!(astrocyte_readers::read_io::counts().file_opens, 0);
    assert_eq!(f.count("usage_event"), 0);
    assert_eq!(f.acks()["read_through"], json!([]));
    f.source.drain_fast(&f.state, 601_000, &NEVER_STOP).unwrap();
    assert_eq!(f.count("usage_event"), 1);
    f.tick(601_001);
    assert_eq!(
        f.acks()["read_through"],
        json!([{"session":"core-conversation","at_ms":601_001}])
    );
    astrocyte_readers::read_io::reset_counts();
    f.source.drain_fast(&f.state, 601_002, &NEVER_STOP).unwrap();
    assert_eq!(astrocyte_readers::read_io::counts().file_opens, 0);
}

#[test]
fn shared_ack_writer_pins_claude_bytes() {
    let f = Fixture::new();
    let path = f.ack.with_file_name("claude-code.json");
    crate::refresher::declared_roots::write_ack(
        &path,
        &crate::refresher::declared_roots::Ack {
            version: 1,
            read_through: vec![crate::refresher::declared_roots::ReadThrough {
                session: "claude-session".into(),
                at_ms: 42,
            }],
        },
    )
    .unwrap();
    assert_eq!(
        fs::read(path).unwrap(),
        b"{\"version\":1,\"read_through\":[{\"session\":\"claude-session\",\"at_ms\":42}]}\n"
    );
}

#[test]
fn refuses_normal_pi_root_even_when_overridden_inside_private_layout() {
    let mut f = Fixture::new();
    f.source.normal_root = Ok(f.folder.parent().unwrap().to_path_buf());
    f.call();
    f.declare(vec![f.root()]);
    assert!(f.tick(1001).is_some());
    assert_eq!(f.count("usage_event"), 0);
    assert_eq!(f.acks()["read_through"], json!([]));
}

#[test]
fn copied_call_in_two_declared_folders_keeps_first_event_and_acknowledges_both() {
    let f = Fixture::new();
    f.call();
    let other = f.folder.with_file_name("other");
    fs::create_dir_all(&other).unwrap();
    fs::write(other.join("copy.jsonl"), TRANSCRIPT).unwrap();
    let mut root = f.root();
    root["path"] = json!(other);
    root["owner_session"] = json!("other-owner");
    f.declare(vec![f.root(), root]);
    assert_eq!(f.tick(1001), None);
    assert_eq!(f.count("usage_event"), 1);
    assert_eq!(f.count("rejected_delivery"), 0);
    assert_eq!(f.acks()["read_through"].as_array().unwrap().len(), 2);
}

#[test]
fn active_sessions_are_read_without_ack_and_bad_roots_keep_history_without_new_ack() {
    let f = Fixture::new();
    f.call();
    let mut root = f.root();
    root["state"] = json!("active");
    root["done_at_ms"] = Value::Null;
    f.declare(vec![root]);
    assert_eq!(f.tick(1001), None);
    assert_eq!(f.count("usage_event"), 1);
    assert_eq!(f.acks()["read_through"], json!([]));
    fs::write(&f.roots, "{").unwrap();
    assert!(f.tick(1002).is_some());
    assert_eq!(f.count("usage_event"), 1);
    assert_eq!(f.acks()["read_through"], json!([]));
    f.declare(vec![f.root()]);
    f.tick(1003);
    assert_eq!(
        f.acks()["read_through"],
        json!([{"session":"core-conversation","at_ms":1003}])
    );
}

#[test]
fn source_is_registered_and_old_config_shape_stays_enabled_and_can_exclude_it() {
    let f = Fixture::new();
    f.call();
    f.declare(vec![f.root()]);
    assert!(crate::config::INGEST_SOURCES.contains(&SOURCE_KEY));
    assert!(crate::refresher::refresher_harness_sources()
        .iter()
        .any(|s| s.source_key() == SOURCE_KEY));
    let old: crate::config::HarnessIngest =
        serde_json::from_value(json!({"enabled":true,"exclude":[]})).unwrap();
    assert!(old.ingests(SOURCE_KEY));
    let config = f.home.path().join("astrocyte.jsonc");
    fs::write(&config, json!({"poll_interval_secs":30,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":[SOURCE_KEY]}}).to_string()).unwrap();
    crate::refresher::reload_config(&f.state, Ok(config)).unwrap();
    fs::write(&f.roots, "{").unwrap();
    crate::refresher::run_iteration_with_harness(&f.state, &[], &f.source, &NEVER_STOP);
    assert_eq!(f.count("usage_event"), 0);
    assert!(!f.ack.exists());
}
