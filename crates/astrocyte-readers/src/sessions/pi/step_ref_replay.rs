//! Local-only compatibility replay for Pi's durable call identities.

use super::*;
use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

// Keep the former builder as an independent oracle for the ignored replay.
// It must not call the shared identity function or use its normalized output.
fn legacy_step_ref(line: &str) -> Option<String> {
    let entry: PiSessionEntry = serde_json::from_str(line).ok()?;
    if matches!(entry.entry_type.as_str(), "compaction" | "usage") {
        let operation: PiOperationEntry = serde_json::from_str(line).ok()?;
        if entry.entry_type != "compaction" && operation.kind.as_deref() != Some("cache_warm") {
            return None;
        }
        operation.usage?;
        let id = operation.id.filter(|id| !id.trim().is_empty())?;
        return Some(format!("pi:operation:{id}"));
    }
    let PiEmittedRecord {
        message,
        usage,
        recorded_model: model,
    } = pi_emitted_record(&entry, PiParseOptions::standard_deduped(), false)?;
    let provider = message
        .provider
        .as_deref()
        .filter(|provider| !provider.is_empty())
        .or_else(|| inferred_provider_from_model(model))
        .unwrap_or("pi");
    let response_key = message
        .response_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .map(|id| format!("pi:response:{id}"));
    response_key.or_else(|| {
        entry
            .id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .map(|id| {
                let timestamp = entry
                    .timestamp
                    .as_deref()
                    .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
                    .map(|timestamp| timestamp.timestamp_millis().to_string())
                    .unwrap_or_else(|| "missing".to_owned());
                let tokens = usage.to_breakdown().tokens;
                format!(
                    "pi:message:{id}:{timestamp}:{provider}:{model}:{:?}:{:?}:{:?}:{:?}",
                    tokens.input, tokens.output, tokens.cache_read, tokens.cache_write,
                )
            })
    })
}

fn session_files(root: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            session_files(&entry.path(), files)?;
        } else if kind.is_file() && entry.path().extension().is_some_and(|ext| ext == "jsonl") {
            files.push(entry.path());
        }
    }
    Ok(())
}

#[derive(Default)]
struct ReplayCounts {
    lines: u64,
    records: u64,
    keyed_records: u64,
    differing_keys: u64,
    ledger_matches: u64,
    different_ledger_keys: u64,
    skipped_lines: u64,
    rejected_files: u64,
}

fn replay_file(
    path: &Path,
    ledger: &HashSet<String>,
    produced: &mut HashSet<String>,
    counts: &mut ReplayCounts,
) {
    let context = PiLineContext {
        path,
        client: "pi",
        fallback_provider: "pi",
        dedup_namespace: Some("pi"),
        options: PiParseOptions::standard_deduped(),
    };
    let mut state = PiParseState::default();
    let mut observer = NoopPiFormatObserver;
    let mut buffer = Vec::new();
    let mut bytes = Vec::new();
    let mut reader = BufReader::new(std::fs::File::open(path).unwrap());
    let mut messages = Vec::new();
    let mut line_number = 0_u64;
    loop {
        bytes.clear();
        if reader.read_until(b'\n', &mut bytes).unwrap() == 0 {
            break;
        }
        line_number += 1;
        counts.lines += 1;
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
        }
        let bytes = if line_number == 1 {
            bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&bytes)
        } else {
            &bytes
        };
        let text = String::from_utf8_lossy(bytes);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            continue;
        }
        let legacy = if state.session_id.is_some() && std::str::from_utf8(bytes).is_ok() {
            legacy_step_ref(trimmed)
        } else {
            None
        };
        messages.clear();
        let flow = parse_pi_line(
            &context,
            &mut state,
            &mut buffer,
            &mut observer,
            &mut messages,
            &mut counts.skipped_lines,
            JsonLineBytes {
                bytes,
                trimmed,
                valid_utf8: std::str::from_utf8(bytes).is_ok(),
            },
        );
        if flow.is_break() {
            counts.rejected_files += 1;
            break;
        }
        counts.records += u64::try_from(messages.len()).unwrap();
        let shared = messages
            .first()
            .and_then(|record| record.message.dedup_key.as_ref());
        if legacy.as_ref() != shared {
            counts.differing_keys += 1;
            eprintln!(
                "KEY_DIFFERENCE {}:{line_number} old={legacy:?} shared={shared:?}",
                path.display()
            );
            if legacy.as_ref().is_some_and(|key| ledger.contains(key)) {
                counts.different_ledger_keys += 1;
                eprintln!("CALL_UNDER_DIFFERENT_LEDGER_KEY {}:{line_number} old={legacy:?} shared={shared:?}", path.display());
            }
        }
        if let Some(key) = shared {
            counts.keyed_records += 1;
            counts.ledger_matches += u64::from(ledger.contains(key));
            produced.insert(key.clone());
        }
    }
}

/// Compare every source-readable Pi call against the former builder and an
/// exported ledger reference set. Inputs stay on the machine running the test.
#[test]
#[ignore = "requires private local Pi sessions and a gzip ledger-ref export"]
fn live_pi_step_ref_replay() {
    let root = PathBuf::from(
        std::env::var_os("PI_STEP_REF_REPLAY_ROOT").expect("set PI_STEP_REF_REPLAY_ROOT"),
    );
    let ledger_path =
        std::env::var_os("PI_STEP_REF_LEDGER_GZIP").expect("set PI_STEP_REF_LEDGER_GZIP");
    let mut gzip = std::process::Command::new("gzip")
        .arg("-dc")
        .arg(ledger_path)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("start gzip to read the ledger export");
    let mut ledger = HashSet::new();
    let mut ledger_lines = 0_u64;
    for line in BufReader::new(gzip.stdout.take().unwrap()).lines() {
        let key = line.expect("read ledger producer_ref");
        assert!(key.starts_with("pi:"), "expected a Pi producer_ref");
        ledger.insert(key);
        ledger_lines += 1;
    }
    assert!(
        gzip.wait().unwrap().success(),
        "gzip refused the ledger export"
    );
    assert!(!ledger.is_empty(), "the ledger export must not be empty");

    let mut files = Vec::new();
    session_files(&root, &mut files).expect("walk every Pi session directory");
    files.sort();
    assert!(!files.is_empty(), "the session root must not be empty");
    let mut produced = HashSet::new();
    let mut counts = ReplayCounts::default();
    for path in &files {
        replay_file(path, &ledger, &mut produced, &mut counts);
    }
    let ledger_without_file = ledger.difference(&produced).count();
    let unique_matches = ledger.intersection(&produced).count();
    println!(
        "PI_STEP_REF_REPLAY files={} lines={} records={} keyed_records={} unique_file_keys={} differing_keys={} ledger_lines={} ledger_keys={} ledger_matches={} unique_ledger_matches={} ledger_keys_without_file={} calls_under_different_key={} skipped_lines={} rejected_files={}",
        files.len(), counts.lines, counts.records, counts.keyed_records, produced.len(),
        counts.differing_keys, ledger_lines, ledger.len(), counts.ledger_matches, unique_matches,
        ledger_without_file, counts.different_ledger_keys, counts.skipped_lines, counts.rejected_files,
    );
    assert_eq!(counts.differing_keys, 0, "Pi call identity changed");
    assert_eq!(
        counts.different_ledger_keys, 0,
        "a call exists under a different ledger key"
    );
}

#[test]
fn shared_pi_keys_keep_raw_usage_and_provider_resolution() {
    let source = concat!(
        "{\"type\":\"session\",\"id\":\"s1\"}\n",
        "{\"type\":\"message\",\"id\":\"response-entry\",\"message\":{\"role\":\"assistant\",\"responseId\":\" resp:1 \",\"model\":\"gpt-5\",\"usage\":{\"output\":17,\"reasoning\":5}}}\n",
        "{\"type\":\"message\",\"id\":\"fallback\",\"timestamp\":\"1970-01-01T00:00:01.1239Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"\",\"model\":\"gpt-5\",\"usage\":{\"input\":0,\"output\":17,\"reasoning\":5,\"cacheRead\":0}}}\n",
        "{\"type\":\"message\",\"id\":\"unknown-provider\",\"message\":{\"role\":\"assistant\",\"model\":\"unrecognized-model\",\"usage\":{\"output\":2,\"reasoning\":5,\"cacheWrite\":0}}}\n",
        "{\"type\":\"message\",\"id\":\"explicit-provider\",\"message\":{\"role\":\"assistant\",\"provider\":\"custom\",\"model\":\"gpt-5\",\"usage\":{\"output\":17,\"reasoning\":5}}}\n",
        "{\"type\":\"compaction\",\"id\":\" op:1 \",\"usage\":{\"output\":17,\"reasoning\":5}}\n",
    );
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::with_suffix(".jsonl").unwrap();
    file.write_all(source.as_bytes()).unwrap();
    file.flush().unwrap();
    let records = parse_pi_format_file_inner(
        file.path(),
        "pi",
        "pi",
        Some("pi"),
        PiParseOptions::standard_deduped(),
        &mut NoopPiFormatObserver,
    );
    let keys: Vec<_> = records
        .iter()
        .map(|record| record.message.dedup_key.as_deref())
        .collect();
    assert_eq!(
        keys,
        [
            Some("pi:response: resp:1 "),
            Some("pi:message:fallback:1123:openai:gpt-5:Some(0):Some(12):Some(0):None"),
            Some(
                "pi:message:unknown-provider:missing:pi:unrecognized-model:None:None:None:Some(0)"
            ),
            Some("pi:message:explicit-provider:missing:custom:gpt-5:None:Some(12):None:None"),
            Some("pi:operation: op:1 "),
        ]
    );
    assert_eq!(records[1].message.tokens.output, Some(12));
    assert!(records[2].message.output_reasoning_conflicted);
    assert!(records[4].operation.is_some());
}
