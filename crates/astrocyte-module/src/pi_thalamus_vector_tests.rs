use super::Fixture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

const VECTOR_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/vectors/thalamus-pi-session-folder-v1"
);
const OWNER_SESSION: &str = "ses_core_bound_conversation";
const WORK_CLASS: &str = "prefrontal-core";
// Mark the recorded Pi session done just after its last reply (2026-11-20T18:08:55.861Z).
const DONE_AT_MS: i64 = 1_795_198_136_000;

#[test]
fn recorded_pi_vector_bytes_match_manifest() {
    let dir = Path::new(VECTOR_DIR);
    let manifest = fs::read_to_string(dir.join("SHA256SUMS")).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(manifest.as_bytes())),
        "adb7ef4efd0a271ee85353b3051425679c368bbc423e7d8acf4757d226a39e54"
    );
    for line in manifest.lines() {
        let (expected, file) = line.split_once("  ").unwrap();
        let bytes = fs::read(dir.join(file)).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(bytes)), expected, "{file}");
    }
}

fn recorded_fixture() -> (Fixture, Value) {
    let vector = Path::new(VECTOR_DIR).join("unedited");
    let mut roots: Value =
        serde_json::from_slice(&fs::read(vector.join("roots.json")).unwrap()).unwrap();
    let path = Path::new(roots["roots"][0]["path"].as_str().unwrap());
    let fixture = Fixture::with_folder_name(path.file_name().unwrap().to_str().unwrap());
    for file in ["session.jsonl", ".prompt-handed"] {
        fs::copy(vector.join("session").join(file), fixture.folder.join(file)).unwrap();
    }
    roots["roots"][0]["path"] = json!(fixture.folder);
    (fixture, roots)
}

fn assert_recorded_calls(fixture: &Fixture, messages: &[Value]) {
    assert_eq!(fixture.count("usage_event"), 2);
    assert_eq!(fixture.count("set_aside_record"), 0);
    assert_eq!(fixture.count("rejected_delivery"), 0);
    fixture
        .state
        .with_conn(|conn| {
            for message in messages {
                let response_id = message["responseId"].as_str().unwrap();
                let (event_key, session, work_class, provider, model) = conn
                    .query_row(
                        "SELECT event_key, session, work_class, provider, model FROM usage_event \
                         WHERE producer_ref = ?1",
                        [format!("pi:response:{response_id}")],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                            ))
                        },
                    )
                    .unwrap();
                assert_eq!(session, OWNER_SESSION, "{response_id}");
                assert_eq!(work_class, WORK_CLASS, "{response_id}");
                assert_eq!(
                    provider,
                    message["provider"].as_str().unwrap(),
                    "{response_id}"
                );
                assert_eq!(model, message["model"].as_str().unwrap(), "{response_id}");

                let measurements = conn
                    .prepare(
                        "SELECT role, quantity_units, quantity_exponent FROM measurement \
                         WHERE event_key = ?1 ORDER BY role",
                    )
                    .unwrap()
                    .query_map([event_key], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                let usage = &message["usage"];
                let expected = [
                    ("cache_read", "cacheRead"),
                    ("cache_write", "cacheWrite"),
                    ("input", "input"),
                    ("output", "output"),
                    ("reasoning", "reasoning"),
                ]
                .map(|(role, field)| (role.to_string(), usage[field].as_i64().unwrap(), 0));
                assert_eq!(measurements, expected, "{response_id}");
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn recorded_pi_vector_reads_active_calls_and_acknowledges_done_once() {
    let (fixture, mut roots) = recorded_fixture();
    let transcript = fs::read_to_string(fixture.folder.join("session.jsonl")).unwrap();
    let mut journal = transcript
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap());
    let header = journal.next().unwrap();
    assert_eq!(header["type"], "session");
    assert_eq!(roots["roots"][0]["session"], header["id"]);
    assert_eq!(roots["roots"][0]["state"], "active");
    let messages: Vec<_> = journal
        .filter_map(|entry| {
            let message = &entry["message"];
            (message["role"] == "assistant" && message["usage"].is_object())
                .then(|| message.clone())
        })
        .collect();
    assert_eq!(messages.len(), 2);

    fs::write(&fixture.roots, roots.to_string()).unwrap();
    assert_eq!(fixture.tick(DONE_AT_MS - 1), None);
    assert_recorded_calls(&fixture, &messages);
    assert_eq!(fixture.acks()["read_through"], json!([]));

    roots["roots"][0]["state"] = json!("done");
    roots["roots"][0]["done_at_ms"] = json!(DONE_AT_MS);
    fs::write(&fixture.roots, roots.to_string()).unwrap();
    assert_eq!(fixture.tick(DONE_AT_MS + 1), None);
    assert_recorded_calls(&fixture, &messages);
    let ack = fs::read(&fixture.ack).unwrap();
    let parsed: Value = serde_json::from_slice(&ack).unwrap();
    let entries = parsed["read_through"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["session"], OWNER_SESSION);
    assert!(entries[0]["at_ms"].as_i64().unwrap() >= DONE_AT_MS);

    assert_eq!(fixture.tick(DONE_AT_MS + 2), None);
    assert_recorded_calls(&fixture, &messages);
    assert_eq!(fs::read(&fixture.ack).unwrap(), ack);
}

#[test]
fn recorded_pi_vector_refuses_done_folder_with_wrong_native_session() {
    let (fixture, mut roots) = recorded_fixture();
    let root = &mut roots["roots"][0];
    root["session"] = json!("01a1fa25-a68e-725e-9e24-4850ae7f2ab7");
    root["state"] = json!("done");
    root["done_at_ms"] = json!(DONE_AT_MS);
    fs::write(&fixture.roots, roots.to_string()).unwrap();

    let reason = fixture
        .tick(DONE_AT_MS + 1)
        .expect("refuse the mismatched session");
    assert!(reason.contains("session header mismatch"), "{reason}");
    assert_eq!(fixture.count("usage_event"), 0);
    assert_eq!(fixture.acks()["read_through"], json!([]));
}
