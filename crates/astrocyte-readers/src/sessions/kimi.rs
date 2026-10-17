// Copied from tokscale v4.17.0 (tag commit
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7),
// crates/tokscale-core/src/sessions/kimi.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
//
// ASTROCYTE DIVERGENCE (whole file): upstream folds a Kimi CLI `wire.jsonl`
// into `UnifiedMessage` totals for reporting. Astrocyte prices each API call
// as its own ledger row, so this copy keeps only the legacy Kimi CLI record
// shapes (`WireLine`, `WireMessage`, `StatusPayload`, `TokenUsage`), the
// `StatusUpdate` selection, the session id taken from the session directory,
// and the `message_id` de-duplication. It adds: counters that stay absent
// instead of decoding as zero, a refusal (not a max-total pick) of a
// `message_id` repeated with different usage, and a resumable byte-offset
// read. Dropped: the kimi-code layout under `~/.kimi-code` (a different root
// this reader does not scan), the `config.json` model lookup and the
// hard-coded default model and provider (a current setting or a constant,
// not something the record states), the file-mtime stand-in for a missing
// timestamp, and the skip of all-zero usage (a reported zero is a
// measurement). Divergences inside retained code are marked inline.

//! Kimi CLI session reader.
//!
//! Reads `~/.kimi/sessions/<GROUP>/<SESSION_UUID>/wire.jsonl`. The first line
//! is `{"type":"metadata",...}`; every later line is
//! `{"timestamp":<unix seconds, float>,"message":{"type":...,"payload":...}}`.
//! Kimi CLI writes one `StatusUpdate` message after each model response, and
//! its `payload.token_usage` is that response's usage. One `StatusUpdate`
//! with usage and a `message_id` is one [`KimiCall`].
//!
//! Token convention, measured 2026-09-24 on the development machine (1
//! `wire.jsonl`, 2 `StatusUpdate` records, too few to establish a convention
//! the way the Codex reader does): usage is already split into
//! `input_other`, `input_cache_read`, `input_cache_creation` and `output`,
//! and no record carries a total. In 2 of 2 records `context_usage` times
//! the configured 262,144-token context equals `input_other +
//! input_cache_read` exactly (6,726 and 19,351), so `input_other` excludes
//! cached input. `input_cache_creation` is 0 in both, so whether a cache
//! write sits inside `input_other` is NOT established: a non-zero one leaves
//! `input` underivable. No record reports reasoning separately; `output` is
//! stored as reported and no reasoning role is emitted.

use super::jsonl_resume::{read_appended_lines, JsonlResumePoint, PrefixCheck};
use super::roles::DerivedRole;
use super::utils::parse_json_line;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// The `StatusUpdate` payload fields this reader uses.
///
/// ASTROCYTE DIVERGENCE: decoded only for `StatusUpdate` messages, so a
/// payload of another message type is never mistaken for an unreadable
/// line.
#[derive(Debug, Deserialize)]
struct StatusPayload {
    token_usage: Option<TokenUsage>,
    message_id: Option<String>,
}

/// One usage block as Kimi CLI reported it.
///
/// ASTROCYTE DIVERGENCE: upstream decodes `Option<i64>` and turns absent and
/// negative values into zero. Here every counter is `Option<u64>`: an absent
/// counter stays absent, and a negative or non-integer one fails the
/// record's decode, which is counted as a skipped line. Upstream's kimi-code
/// camelCase aliases are dropped with that layout.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct TokenUsage {
    input_other: Option<u64>,
    output: Option<u64>,
    input_cache_read: Option<u64>,
    input_cache_creation: Option<u64>,
}

/// Usage of one call in disjoint roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KimiCallUsage {
    /// `input_other` verbatim; [`DerivedRole::Underivable`] when a non-zero
    /// `input_cache_creation` is reported beside it.
    pub input: DerivedRole,
    /// `input_cache_read` verbatim.
    pub cache_read: Option<u64>,
    /// `input_cache_creation` verbatim.
    pub cache_write: Option<u64>,
    /// `output` verbatim, including any reasoning (not reported apart).
    pub output: Option<u64>,
}

impl KimiCallUsage {
    fn from_usage(usage: &TokenUsage) -> Self {
        let cache_write_unmeasured = usage.input_cache_creation.is_some_and(|write| write > 0);
        Self {
            input: DerivedRole::verbatim(usage.input_other).unless(cache_write_unmeasured),
            cache_read: usage.input_cache_read,
            cache_write: usage.input_cache_creation,
            output: usage.output,
        }
    }
}

/// One model response, from one `StatusUpdate` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KimiCall {
    /// The session directory's name (the session UUID).
    pub session_id: String,
    /// `payload.message_id` (an OpenAI-style `chatcmpl-...` id): unique per
    /// response.
    pub message_id: String,
    /// The record's `timestamp`, converted from seconds. Absent when it has
    /// none or it is not a positive finite number: never a file mtime.
    pub occurred_at_ms: Option<i64>,
    pub usage: KimiCallUsage,
}

/// Counts for one parse call. Every count covers only the lines this call
/// consumed, so a resumed parse reports the appended lines alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KimiParseCounts {
    /// Complete (newline-terminated) lines consumed.
    pub lines: u64,
    /// Complete lines that could not be read: not JSON, or a `StatusUpdate`
    /// whose payload does not decode (a negative counter, a wrong type).
    /// Reading continues past them.
    pub skipped_lines: u64,
    /// `StatusUpdate` records seen.
    pub status_updates: u64,
    /// `StatusUpdate` records without `token_usage`.
    pub status_updates_without_usage: u64,
    /// Usage records whose `message_id` was already read with identical
    /// usage: a re-send, not a new response.
    pub repeated_updates: u64,
    /// Usage records whose `message_id` was already read with different
    /// usage. Not ingested; none measured.
    pub conflicting_updates: u64,
    /// Usage records without a `message_id`. Not ingested; none measured.
    pub updates_without_message_id: u64,
    /// Emitted calls whose `timestamp` was present but not a positive
    /// finite number; they are undated.
    pub unreadable_timestamps: u64,
    /// Emitted calls whose `input` is [`DerivedRole::Underivable`].
    pub underivable_roles: u64,
}

impl KimiParseCounts {
    /// Usage records the reader saw and did not emit for a reason other than
    /// a benign repeat: a shape nobody measured.
    pub fn usage_not_ingested(&self) -> u64 {
        self.conflicting_updates + self.updates_without_message_id
    }

    super::utils::add_parse_counts!(
        lines,
        skipped_lines,
        status_updates,
        status_updates_without_usage,
        repeated_updates,
        conflicting_updates,
        updates_without_message_id,
        unreadable_timestamps,
        underivable_roles,
    );
}

/// Parse state for one file, carried across resumed reads: the usage of
/// every `message_id` read so far.
#[derive(Debug, Default)]
pub(crate) struct KimiParseState {
    seen: HashMap<String, TokenUsage>,
}

/// Seconds since the epoch, as a positive finite JSON number, to
/// milliseconds; `Err` for a present value that is not one.
///
/// ASTROCYTE DIVERGENCE: upstream substitutes the file's mtime for a
/// missing or non-positive timestamp. Here it is undated.
fn timestamp_ms(value: Option<&serde_json::Value>) -> Result<Option<i64>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.as_f64() {
        Some(seconds) if seconds.is_finite() && seconds > 0.0 => {
            let millis = (seconds * 1000.0).round();
            if millis >= i64::MAX as f64 {
                return Ok(Some(i64::MAX));
            }
            // The upper-bound check keeps the rounded positive timestamp in i64.
            #[allow(clippy::cast_possible_truncation)]
            let millis = millis as i64;
            Ok(Some(millis))
        }
        _ => Err(()),
    }
}

fn parse_line(
    payload: &[u8],
    session_id: &str,
    buffer: &mut Vec<u8>,
    state: &mut KimiParseState,
    counts: &mut KimiParseCounts,
    calls: &mut Vec<KimiCall>,
) {
    let Ok(text) = std::str::from_utf8(payload) else {
        counts.skipped_lines += 1;
        return;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let Some(line) = parse_json_line::<serde_json::Value>(trimmed, buffer) else {
        counts.skipped_lines += 1;
        return;
    };
    let Some(message) = line.get("message") else {
        return;
    };
    if message.get("type").and_then(|kind| kind.as_str()) != Some("StatusUpdate") {
        return;
    }
    counts.status_updates += 1;
    let Some(status) = message
        .get("payload")
        .and_then(|payload| StatusPayload::deserialize(payload).ok())
    else {
        counts.skipped_lines += 1;
        return;
    };
    let Some(usage) = status.token_usage else {
        counts.status_updates_without_usage += 1;
        return;
    };
    let Some(message_id) = status.message_id.filter(|id| !id.is_empty()) else {
        counts.updates_without_message_id += 1;
        return;
    };
    // ASTROCYTE DIVERGENCE: upstream keeps whichever copy of a `message_id`
    // has the larger total. A copy with different usage is a revision nobody
    // measured, so it is counted and the first copy stands.
    match state.seen.get(&message_id) {
        Some(earlier) if *earlier == usage => {
            counts.repeated_updates += 1;
            return;
        }
        Some(_) => {
            counts.conflicting_updates += 1;
            return;
        }
        None => {}
    }
    let occurred_at_ms = timestamp_ms(line.get("timestamp")).unwrap_or_else(|()| {
        counts.unreadable_timestamps += 1;
        None
    });
    let call_usage = KimiCallUsage::from_usage(&usage);
    if call_usage.input == DerivedRole::Underivable {
        counts.underivable_roles += 1;
    }
    state.seen.insert(message_id.clone(), usage);
    calls.push(KimiCall {
        session_id: session_id.to_string(),
        message_id,
        occurred_at_ms,
        usage: call_usage,
    });
}

/// Where a previous [`parse_kimi_wire_resumable`] stopped.
#[derive(Debug)]
pub struct KimiResumePoint(JsonlResumePoint<KimiParseState>);

/// Result of one [`parse_kimi_wire_resumable`] call.
#[derive(Debug, Default)]
pub struct KimiResumableParse {
    /// Calls read by this call, in file order.
    pub calls: Vec<KimiCall>,
    /// True when the previous point's prefix verified and parsing started at
    /// its offset; false for a full read from the start.
    pub resumed: bool,
    /// Where the next call may resume, or `None` when the file could not be
    /// read, so the next call reads it in full.
    pub resume: Option<KimiResumePoint>,
    /// Bytes fed to SHA-256 by this call: O(window + appended) on a resume.
    pub hashed_bytes: u64,
    pub counts: KimiParseCounts,
}

/// The session id of a `wire.jsonl`: its directory's name.
fn session_id_of(path: &Path) -> String {
    path.parent()
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Parses a Kimi CLI `wire.jsonl`, continuing from `previous` when the bytes
/// it consumed are unchanged (the shared resume in `jsonl_resume.rs`).
///
/// Only newline-terminated lines are consumed. Kimi CLI writes a
/// `StatusUpdate` once a response has finished, so a response's usage is
/// final when its line is complete. A full re-read re-emits every call with
/// the same identity and content, which the ledger absorbs as exact
/// redeliveries.
pub fn parse_kimi_wire_resumable(
    path: &Path,
    previous: Option<KimiResumePoint>,
    check: PrefixCheck,
) -> KimiResumableParse {
    let session_id = session_id_of(path);
    let mut counts = KimiParseCounts::default();
    let mut calls = Vec::new();
    let mut buffer = Vec::with_capacity(4096);
    let Some(read) = read_appended_lines(
        path,
        previous.map(|point| point.0),
        check,
        KimiParseState::default,
        |state, payload| {
            counts.lines += 1;
            parse_line(
                payload,
                &session_id,
                &mut buffer,
                state,
                &mut counts,
                &mut calls,
            );
        },
    ) else {
        return KimiResumableParse::default();
    };
    KimiResumableParse {
        calls,
        resumed: read.resumed,
        resume: read
            .cursor
            .map(|cursor| KimiResumePoint(cursor.with_state(read.state))),
        hashed_bytes: read.hashed_bytes,
        counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const SESSION: &str = "5777b661-8d37-4b53-a3f5-8635024bb016";

    fn status(id: &str, usage: &str) -> String {
        format!(
            r#"{{"timestamp": 1771258960.612596, "message": {{"type": "StatusUpdate", "payload": {{"context_usage": 0.0256, "token_usage": {usage}, "message_id": "{id}"}}}}}}"#
        ) + "\n"
    }

    fn usage(counters: [u64; 4]) -> String {
        let [other, output, read, creation] = counters;
        format!(
            r#"{{"input_other": {other}, "output": {output}, "input_cache_read": {read}, "input_cache_creation": {creation}}}"#
        )
    }

    fn head() -> String {
        concat!(
            r#"{"type": "metadata", "protocol_version": "1.3"}"#,
            "\n",
            r#"{"timestamp": 1771258835.76289, "message": {"type": "TurnBegin", "payload": {"user_input": [{"type": "text", "text": "hi"}]}}}"#,
            "\n",
            r#"{"timestamp": 1771258836.1, "message": {"type": "ContentPart", "payload": "a plain string"}}"#,
            "\n",
        )
        .to_string()
    }

    fn file_with(contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("group").join(SESSION).join("wire.jsonl");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn parse(path: &Path, previous: Option<KimiResumePoint>) -> KimiResumableParse {
        parse_kimi_wire_resumable(path, previous, PrefixCheck::Window)
    }

    /// Kimi splits input itself: `input_other` is stored as uncached input,
    /// the cache counters beside it, output as reported. Identity is the
    /// response's `message_id` within the session directory's id.
    #[test]
    fn a_status_update_is_one_call_in_disjoint_roles() {
        let (_dir, path) =
            file_with(&(head() + &status("chatcmpl-a", &usage([1862, 12591, 4864, 0]))));
        let parse = parse(&path, None);
        assert_eq!(parse.counts.skipped_lines, 0);
        assert_eq!(parse.calls.len(), 1);
        let call = &parse.calls[0];
        assert_eq!(call.session_id, SESSION);
        assert_eq!(call.message_id, "chatcmpl-a");
        assert_eq!(call.occurred_at_ms, Some(1_771_258_960_613));
        assert_eq!(
            call.usage,
            KimiCallUsage {
                input: DerivedRole::Measured(1862),
                cache_read: Some(4864),
                cache_write: Some(0),
                output: Some(12591),
            }
        );
    }

    /// An absent counter is an omitted measurement, never a zero.
    #[test]
    fn an_absent_counter_is_omitted_not_zero() {
        let (_dir, path) = file_with(&status("chatcmpl-a", r#"{"input_other": 5, "output": 7}"#));
        let usage = parse(&path, None).calls[0].usage;
        assert_eq!(usage.cache_read, None);
        assert_eq!(usage.cache_write, None);
        assert_eq!(usage.input, DerivedRole::Measured(5));
    }

    /// A negative or non-integer counter fails the record: a counted
    /// skipped line, never a zero; reading continues.
    #[test]
    fn a_malformed_counter_is_a_counted_skipped_line() {
        let (_dir, path) = file_with(
            &[
                status("chatcmpl-a", &usage([1, 2, 3, 0]).replace(": 1,", ": -1,")),
                status(
                    "chatcmpl-b",
                    &usage([1, 2, 3, 0]).replace(": 2,", ": \"2\","),
                ),
                "not json\n".to_string(),
                status("chatcmpl-c", &usage([1, 2, 3, 0])),
            ]
            .concat(),
        );
        let parse = parse(&path, None);
        assert_eq!(parse.counts.skipped_lines, 3);
        assert_eq!(parse.calls.len(), 1);
        assert_eq!(parse.calls[0].message_id, "chatcmpl-c");
    }

    /// A non-zero cache write's place relative to `input_other` was never
    /// measured, so uncached input is not claimed.
    #[test]
    fn a_non_zero_cache_write_leaves_input_underivable() {
        let (_dir, path) = file_with(&status("chatcmpl-a", &usage([10, 2, 3, 4])));
        let parse = parse(&path, None);
        assert_eq!(parse.calls[0].usage.input, DerivedRole::Underivable);
        assert_eq!(parse.calls[0].usage.cache_write, Some(4));
        assert_eq!(parse.counts.underivable_roles, 1);
    }

    /// A re-sent `message_id` is not a second call; one with different
    /// usage is counted and not ingested; a resumed read neither re-emits
    /// nor forgets.
    #[test]
    fn repeats_are_not_duplicated_across_a_resume() {
        let first = status("chatcmpl-a", &usage([1, 2, 3, 0]));
        let (_dir, path) = file_with(&(head() + &first));
        let initial = parse(&path, None);
        assert_eq!(initial.calls.len(), 1);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(first.as_bytes()).unwrap();
        file.write_all(status("chatcmpl-a", &usage([9, 2, 3, 0])).as_bytes())
            .unwrap();
        file.write_all(status("chatcmpl-b", &usage([4, 5, 6, 0])).as_bytes())
            .unwrap();
        let resumed = parse(&path, initial.resume);
        assert!(resumed.resumed);
        assert_eq!(resumed.counts.lines, 3);
        assert_eq!(resumed.counts.repeated_updates, 1);
        assert_eq!(resumed.counts.conflicting_updates, 1);
        assert_eq!(resumed.counts.usage_not_ingested(), 1);
        assert_eq!(resumed.calls.len(), 1);
        assert_eq!(resumed.calls[0].message_id, "chatcmpl-b");

        let full = parse(&path, None);
        let refs: Vec<_> = full
            .calls
            .iter()
            .map(|call| call.message_id.as_str())
            .collect();
        assert_eq!(refs, ["chatcmpl-a", "chatcmpl-b"]);
    }

    /// No timestamp, or an unusable one: undated, never the file's mtime.
    #[test]
    fn a_missing_or_unusable_timestamp_is_undated() {
        let line = status("chatcmpl-a", &usage([1, 2, 3, 0]));
        let (_dir, path) = file_with(
            &[
                line.replace(r#""timestamp": 1771258960.612596, "#, ""),
                line.replace("chatcmpl-a", "chatcmpl-b")
                    .replace("1771258960.612596", "-1.5"),
            ]
            .concat(),
        );
        let parse = parse(&path, None);
        assert_eq!(parse.calls.len(), 2);
        assert!(parse.calls.iter().all(|call| call.occurred_at_ms.is_none()));
        assert_eq!(parse.counts.unreadable_timestamps, 1);
    }
}
