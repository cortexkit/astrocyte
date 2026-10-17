use std::process::{Command, Output};

use astrocyte_module::test_support::{catalog::event, output_within};
use serde_json::json;
use sha2::{Digest, Sha256};

fn drive(bytes: &[u8], event_count: usize) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let corpus = dir.path().join("corpus.jsonl");
    std::fs::write(&corpus, bytes).unwrap();
    std::fs::write(
        corpus.with_extension("jsonl.manifest.json"),
        serde_json::to_vec(&json!({
            "event_count": event_count,
            "sha256": format!("{:x}", Sha256::digest(bytes)),
            "capture_files_read": 1,
            "generator_commit": "test-fixture",
            "generator_modified": false,
        }))
        .unwrap(),
    )
    .unwrap();
    output_within(
        Command::new(env!("CARGO_BIN_EXE_corpus_drive")).arg(corpus),
        "corpus_drive",
    )
}

#[test]
fn empty_corpus_is_refused_without_panicking() {
    let output = drive(b"", 0);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("empty corpus"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
}

#[test]
fn mixed_corpus_reports_ingest_and_exits_nonzero_for_refusals() {
    let bytes = format!("{}\n{{}}\n", serde_json::to_string(&event(100)).unwrap());
    let output = drive(bytes.as_bytes(), 2);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ingested            1"), "{stdout}");
    assert!(stdout.contains("REFUSED      1"), "{stdout}");
}

#[test]
fn fully_refused_corpus_still_reports_missing_coverage() {
    let output = drive(b"{}\n{ }\n", 2);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ingested            0"), "{stdout}");
    assert!(stdout.contains("charge_basis: NO COVERAGE"), "{stdout}");
    assert!(stdout.contains("model: NO COVERAGE"), "{stdout}");
    assert!(stdout.contains("REFUSED      2"), "{stdout}");
    assert_eq!(stdout.matches("decode:").count(), 1, "{stdout}");
}

#[test]
fn accepted_corpus_exits_successfully() {
    let bytes = format!("{}\n", serde_json::to_string(&event(100)).unwrap());
    let output = drive(bytes.as_bytes(), 1);
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("ingested            1"));
}
