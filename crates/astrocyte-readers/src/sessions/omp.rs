// Copied from tokscale v4.17.0 (tag commit
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7),
// crates/tokscale-core/src/sessions/omp.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
//
// ASTROCYTE DIVERGENCE (whole file): upstream delegates to the Pi parser
// (`parse_pi_format_file(path, "omp", "omp")`) and folds a transcript into
// `UnifiedMessage` totals. Astrocyte prices each API call as its own ledger
// row, and Oh My Pi's usage block differs from Pi's in the one field that
// matters for disjoint roles: it reports `reasoningTokens`, which the Pi
// record shape (`reasoning`) does not read. This copy therefore keeps
// upstream's layout notes and the Pi record format (a `session` header, then
// `message` entries whose assistant messages carry `usage`), and parses it
// itself through the shared resumable read. Dropped with the delegation:
// Pi's cross-session de-duplication key, subagent display names, RLM and
// Prime handling, provider inference from the model name, and the file-mtime
// stand-in for a missing timestamp.

//! Oh My Pi (omp) session reader.
//!
//! Oh My Pi is a pi-mono descendant and writes the Pi session JSONL format.
//! A top-level session is
//! `~/.omp/agent/sessions/<encoded-cwd>/<timestamp>_<uuid>.jsonl`; subagents
//! are sibling files in a directory of the same name
//! (`<timestamp>_<uuid>/<AgentName>.jsonl`, advisors as
//! `__advisor.<name>.jsonl`). Every one of those is a complete session file
//! with its own header, so each is read on its own.
//!
//! Token convention, measured 2026-09-24 on the development machine (2
//! files, 14 assistant records with usage; few, so each rule below is
//! checked per record where the record allows it): `totalTokens == input +
//! output + cacheRead + cacheWrite` in 14 of 14. `cacheRead` is non-zero in
//! 12 and is added on top, so it is NOT inside `input`. `reasoningTokens` is
//! present (and non-zero) in 5 and is not added on top, so it is inside
//! `output`. `cacheWrite` is 0 in all 14, so its place is not established;
//! a non-zero `cacheWrite` leaves `input` underivable unless that record's
//! own `totalTokens` shows it was added on top. `reasoningTokens` is absent
//! from 9 records; there `output` is stored gross (see [`OmpOutput`]).

use super::jsonl_resume::{read_appended_lines, JsonlResumePoint, PrefixCheck};
use super::normalize_workspace_key;
use super::roles::DerivedRole;
use super::utils::parse_json_line_with_probe;
use serde::Deserialize;
use std::path::Path;

/// One transcript record: the `session` header or a later entry.
#[derive(Debug, Deserialize)]
struct OmpEntry {
    #[serde(rename = "type")]
    entry_type: String,
    id: Option<String>,
    timestamp: Option<String>,
    cwd: Option<String>,
    message: Option<OmpMessage>,
}

#[derive(Debug, Deserialize)]
struct OmpMessage {
    role: Option<String>,
    model: Option<String>,
    provider: Option<String>,
    #[serde(rename = "responseId")]
    response_id: Option<String>,
    #[serde(rename = "stopReason")]
    stop_reason: Option<String>,
    usage: Option<OmpUsage>,
}

/// The usage block as Oh My Pi reported it.
///
/// ASTROCYTE DIVERGENCE: Pi's `PiUsage` reads `reasoning`; Oh My Pi writes
/// `reasoningTokens`. Every counter is `Option<u64>`: an absent counter
/// stays absent, and a negative or non-integer one fails the record's
/// decode, which is counted as a skipped line.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OmpUsage {
    input: Option<u64>,
    output: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    total_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
}

/// Visible output of one call, and how it was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OmpOutput {
    /// `output - reasoningTokens`: reasoning was reported, and is inside
    /// `output`.
    Visible(DerivedRole),
    /// `output` as reported, because the record has no `reasoningTokens`.
    /// Whether an absent `reasoningTokens` means none was spent is not
    /// established, so no reasoning role is stored beside it; the gross
    /// output still counts every output token exactly once.
    Gross(Option<u64>),
}

/// Usage of one call in disjoint roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OmpCallUsage {
    /// `input` verbatim, or underivable beside a non-zero `cacheWrite` the
    /// record's total does not place outside it.
    pub input: DerivedRole,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
    pub output: OmpOutput,
    /// `reasoningTokens` verbatim.
    pub reasoning: Option<u64>,
}

impl OmpCallUsage {
    fn from_usage(usage: &OmpUsage) -> Self {
        let cache_write_outside_input = || {
            let parts = [
                usage.input,
                usage.output,
                usage.cache_read,
                usage.cache_write,
            ];
            let sum = parts
                .into_iter()
                .try_fold(0_u64, |sum, part| sum.checked_add(part?));
            sum.is_some() && sum == usage.total_tokens
        };
        let cache_write_unmeasured =
            usage.cache_write.is_some_and(|write| write > 0) && !cache_write_outside_input();
        let output = match usage.reasoning_tokens {
            Some(reasoning) => {
                OmpOutput::Visible(DerivedRole::difference(usage.output, Some(reasoning)))
            }
            None => OmpOutput::Gross(usage.output),
        };
        Self {
            input: DerivedRole::verbatim(usage.input).unless(cache_write_unmeasured),
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
            output,
            reasoning: usage.reasoning_tokens,
        }
    }
}

/// One assistant message with usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OmpCall {
    /// The `session` header's `id`.
    pub session_id: String,
    /// The entry's own `id` (unique within its session).
    pub entry_id: String,
    /// `message.responseId`: the provider's response id, when reported.
    pub response_id: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    /// `message.stopReason` verbatim.
    pub stop_reason: Option<String>,
    /// The header's `cwd`, normalized.
    pub project_root: Option<String>,
    /// The entry's RFC 3339 `timestamp`; absent when it has none or it does
    /// not parse. Never a file mtime.
    pub occurred_at_ms: Option<i64>,
    pub usage: OmpCallUsage,
}

/// Counts for one parse call. Every count covers only the lines this call
/// consumed, so a resumed parse reports the appended lines alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OmpParseCounts {
    /// Complete (newline-terminated) lines consumed.
    pub lines: u64,
    /// Complete lines that could not be read: not JSON, or a `session` or
    /// `message` record whose fields do not decode (a negative counter, a
    /// wrong type). Reading continues past them.
    pub skipped_lines: u64,
    /// Assistant messages with usage.
    pub usage_records: u64,
    /// Usage records before any `session` header, or without an entry id.
    /// Not ingested; none measured.
    pub records_unidentifiable: u64,
    /// Calls stored with gross output (no `reasoningTokens`).
    pub gross_outputs: u64,
    /// Calls whose visible output is [`DerivedRole::Conflicted`].
    pub output_conflicts: u64,
    /// Calls whose `input` or visible output is [`DerivedRole::Underivable`].
    pub underivable_roles: u64,
}

impl OmpParseCounts {
    /// Usage records the reader saw and did not emit.
    pub fn usage_not_ingested(&self) -> u64 {
        self.records_unidentifiable
    }

    super::utils::add_parse_counts!(
        lines,
        skipped_lines,
        usage_records,
        records_unidentifiable,
        gross_outputs,
        output_conflicts,
        underivable_roles,
    );
}

/// Parse state carried across resumed reads: the session header.
#[derive(Debug, Default)]
pub(crate) struct OmpParseState {
    session_id: Option<String>,
    project_root: Option<String>,
}

/// Only the record type, for telling an unreadable record this reader
/// depends on (a skipped line) from a record of another type whose shape the
/// typed decode does not expect (not skipped).
#[derive(Deserialize)]
struct EntryTypeProbe {
    #[serde(rename = "type")]
    entry_type: Option<String>,
}

fn parse_line(
    payload: &[u8],
    buffer: &mut Vec<u8>,
    state: &mut OmpParseState,
    counts: &mut OmpParseCounts,
    calls: &mut Vec<OmpCall>,
) {
    let Ok(text) = std::str::from_utf8(payload) else {
        counts.skipped_lines += 1;
        return;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let entry = match parse_json_line_with_probe::<OmpEntry, EntryTypeProbe>(trimmed, buffer) {
        Ok(entry) => entry,
        Err(probe) => {
            match probe {
                Some(EntryTypeProbe { entry_type })
                    if !matches!(entry_type.as_deref(), Some("session") | Some("message")) => {}
                _ => counts.skipped_lines += 1,
            }
            return;
        }
    };
    if entry.entry_type == "session" {
        // The first header names the file's session; a later one never
        // re-labels it.
        if state.session_id.is_none() {
            state.session_id = entry.id.filter(|id| !id.is_empty());
            state.project_root = entry.cwd.as_deref().and_then(normalize_workspace_key);
        }
        return;
    }
    if entry.entry_type != "message" {
        return;
    }
    let Some(message) = entry.message else {
        return;
    };
    if message.role.as_deref() != Some("assistant") {
        return;
    }
    let Some(usage) = message.usage else {
        return;
    };
    counts.usage_records += 1;
    let (Some(session_id), Some(entry_id)) = (
        state.session_id.clone(),
        entry.id.filter(|id| !id.is_empty()),
    ) else {
        counts.records_unidentifiable += 1;
        return;
    };
    let usage = OmpCallUsage::from_usage(&usage);
    let visible = match usage.output {
        OmpOutput::Visible(visible) => Some(visible),
        OmpOutput::Gross(_) => {
            counts.gross_outputs += 1;
            None
        }
    };
    if visible == Some(DerivedRole::Conflicted) {
        counts.output_conflicts += 1;
    }
    counts.underivable_roles += [Some(usage.input), visible]
        .into_iter()
        .filter(|role| *role == Some(DerivedRole::Underivable))
        .count() as u64;
    calls.push(OmpCall {
        session_id,
        entry_id,
        response_id: message.response_id.filter(|id| !id.is_empty()),
        model: message.model,
        provider: message.provider,
        stop_reason: message.stop_reason,
        project_root: state.project_root.clone(),
        occurred_at_ms: entry
            .timestamp
            .as_deref()
            .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
            .map(|timestamp| timestamp.timestamp_millis()),
        usage,
    });
}

/// Where a previous [`parse_omp_file_resumable`] stopped.
#[derive(Debug)]
pub struct OmpResumePoint(JsonlResumePoint<OmpParseState>);

impl OmpResumePoint {
    /// Complete bytes consumed, excluding an unterminated tail.
    pub fn offset(&self) -> u64 {
        self.0.offset()
    }
}

/// Result of one [`parse_omp_file_resumable`] call.
#[derive(Debug, Default)]
pub struct OmpResumableParse {
    /// Calls read by this call, in file order.
    pub calls: Vec<OmpCall>,
    /// True when the previous point's prefix verified and parsing started at
    /// its offset; false for a full read from the start.
    pub resumed: bool,
    /// Where the next call may resume, or `None` when the file could not be
    /// read, so the next call reads it in full.
    pub resume: Option<OmpResumePoint>,
    /// Bytes fed to SHA-256 by this call: O(window + appended) on a resume.
    pub hashed_bytes: u64,
    pub counts: OmpParseCounts,
}

/// Parses an Oh My Pi session file, continuing from `previous` when the
/// bytes it consumed are unchanged (the shared resume in `jsonl_resume.rs`).
///
/// Only newline-terminated lines are consumed. Oh My Pi appends an assistant
/// message once its response has finished (measured: every usage record has
/// a `stopReason`), so its usage is final when its line is complete.
pub fn parse_omp_file_resumable(
    path: &Path,
    previous: Option<OmpResumePoint>,
    check: PrefixCheck,
) -> OmpResumableParse {
    let mut counts = OmpParseCounts::default();
    let mut calls = Vec::new();
    let mut buffer = Vec::with_capacity(4096);
    let Some(read) = read_appended_lines(
        path,
        previous.map(|point| point.0),
        check,
        OmpParseState::default,
        |state, payload| {
            counts.lines += 1;
            parse_line(payload, &mut buffer, state, &mut counts, &mut calls);
        },
    ) else {
        return OmpResumableParse::default();
    };
    OmpResumableParse {
        calls,
        resumed: read.resumed,
        resume: read
            .cursor
            .map(|cursor| OmpResumePoint(cursor.with_state(read.state))),
        hashed_bytes: read.hashed_bytes,
        counts,
    }
}

#[cfg(test)]
mod tests {
    use super::super::utils::parse_json_line;
    use super::*;

    #[test]
    fn reusing_the_tape_preserves_typed_and_probe_decoding() {
        let records = [
            r#"{"type":"session","id":"s","cwd":"/tmp"}"#,
            r#"{"type":"message","id":"m","message":{"role":"assistant","usage":{"input":1}}}"#,
            r#"{"type":"message","message":{"usage":{"input":-1}}}"#,
            r#"{"type":"message","message":{"usage":{"input":1.5}}}"#,
            r#"{"type":"message","message":{"usage":{"input":"1"}}}"#,
            r#"{"type":"message","message":false}"#,
            r#"{"type":"other","message":false}"#,
            r#"{"type":"other","id":42}"#,
            r#"{"type":"session","id":42}"#,
            r#"{"type":"message","type":"other","message":false}"#,
            r#"{"type":"other","type":"message","message":false}"#,
            r#"{"type":"message","type":"message"}"#,
            r#"{"type":null,"message":false}"#,
            r#"{"type":42}"#,
            r#"{"message":false}"#,
            r#"{"type":"message"} {"type":"other"}"#,
            r#"{"type":"message","unknown":{"nested":[null,"a\"b",42]}}"#,
            "{bad",
            "null",
            "[]",
            "",
        ];
        let mut buffer = Vec::new();
        for record in records {
            let legacy = match parse_json_line::<OmpEntry>(record, &mut buffer) {
                Some(entry) => format!("entry:{entry:?}"),
                None => format!(
                    "probe:{:?}",
                    parse_json_line::<EntryTypeProbe>(record, &mut buffer)
                        .map(|probe| probe.entry_type)
                ),
            };
            let reused =
                match parse_json_line_with_probe::<OmpEntry, EntryTypeProbe>(record, &mut buffer) {
                    Ok(entry) => format!("entry:{entry:?}"),
                    Err(probe) => format!("probe:{:?}", probe.map(|probe| probe.entry_type)),
                };
            assert_eq!(reused, legacy, "{record}");
        }
    }

    #[test]
    fn a_failed_usage_decode_parses_json_only_once() {
        let mut buffer = Vec::new();
        let mut state = OmpParseState::default();
        let mut counts = OmpParseCounts::default();
        let mut calls = Vec::new();
        let before = super::super::utils::json_decodes();
        parse_line(
            br#"{"type":"message","message":{"role":"assistant","usage":{"input":-1}}}"#,
            &mut buffer,
            &mut state,
            &mut counts,
            &mut calls,
        );
        let decodes = super::super::utils::json_decodes() - before;
        assert_eq!(counts.skipped_lines, 1);
        assert!(calls.is_empty());
        eprintln!("malformed usage record: {decodes} JSON decodes");
        assert_eq!(decodes, 1, "the type probe must reuse the decoded tape");
    }
    use std::io::Write;

    const SESSION: &str = "019f6989-7600-7000-a3a3-0194dcbc61cc";

    fn header() -> String {
        format!(
            r#"{{"type":"session","version":3,"id":"{SESSION}","timestamp":"2026-07-16T06:07:24.160Z","cwd":"/work/brocatui/","title":"t"}}"#
        ) + "\n"
    }

    fn assistant(id: &str, usage: &str) -> String {
        format!(
            r#"{{"type":"message","id":"{id}","parentId":"b172d944","timestamp":"2026-07-16T06:10:29.569Z","message":{{"role":"assistant","content":[],"api":"openai-codex-responses","provider":"openai-codex","model":"gpt-5.6-luna","usage":{usage},"stopReason":"stop","responseId":"resp_{id}"}}}}"#
        ) + "\n"
    }

    fn file_with(contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn parse(path: &Path, previous: Option<OmpResumePoint>) -> OmpResumableParse {
        parse_omp_file_resumable(path, previous, PrefixCheck::Window)
    }

    /// Cached input is outside `input` and stored beside it; reasoning is
    /// inside `output` and subtracted from it.
    #[test]
    fn reasoning_is_subtracted_from_output_and_cache_read_kept_apart() {
        let usage = r#"{"input":858,"output":68,"cacheRead":28416,"cacheWrite":0,"totalTokens":29342,"reasoningTokens":30,"cost":{"total":0.01}}"#;
        let (_dir, path) = file_with(&(header() + &assistant("6b8f2d9d", usage)));
        let parse = parse(&path, None);
        assert_eq!(parse.calls.len(), 1);
        let call = &parse.calls[0];
        assert_eq!(
            call.usage,
            OmpCallUsage {
                input: DerivedRole::Measured(858),
                cache_read: Some(28416),
                cache_write: Some(0),
                output: OmpOutput::Visible(DerivedRole::Measured(38)),
                reasoning: Some(30),
            }
        );
        assert_eq!(call.session_id, SESSION);
        assert_eq!(call.response_id.as_deref(), Some("resp_6b8f2d9d"));
        assert_eq!(call.project_root.as_deref(), Some("/work/brocatui"));
        assert_eq!(call.provider.as_deref(), Some("openai-codex"));
        assert_eq!(call.stop_reason.as_deref(), Some("stop"));
        assert!(call.occurred_at_ms.is_some());
    }

    /// An absent counter is omitted, never zero; without `reasoningTokens`
    /// the output is stored gross and no reasoning is claimed.
    #[test]
    fn absent_counters_are_omitted_not_zero() {
        let usage = r#"{"input":17,"output":10,"totalTokens":27}"#;
        let (_dir, path) = file_with(&(header() + &assistant("b2", usage)));
        let parse = parse(&path, None);
        let usage = parse.calls[0].usage;
        assert_eq!(usage.cache_read, None);
        assert_eq!(usage.cache_write, None);
        assert_eq!(usage.reasoning, None);
        assert_eq!(usage.output, OmpOutput::Gross(Some(10)));
        assert_eq!(parse.counts.gross_outputs, 1);
    }

    /// A negative or non-integer counter fails the record: a counted
    /// skipped line, never a zero. A record of another type with an
    /// unexpected shape is not a skipped line.
    #[test]
    fn a_malformed_counter_is_a_counted_skipped_line() {
        let (_dir, path) = file_with(
            &[
                header(),
                assistant("a1", r#"{"input":-1,"output":1}"#),
                assistant("a2", r#"{"input":1.5,"output":1}"#),
                r#"{"type":"custom","data":"a plain string","message":7}"#.to_string() + "\n",
                "not json\n".to_string(),
                assistant("a3", r#"{"input":1,"output":1}"#),
            ]
            .concat(),
        );
        let parse = parse(&path, None);
        assert_eq!(parse.counts.skipped_lines, 3);
        assert_eq!(parse.calls.len(), 1);
        assert_eq!(parse.calls[0].entry_id, "a3");
    }

    /// A non-zero cache write is kept outside `input` only when the record's
    /// own total says so; otherwise input is underivable.
    #[test]
    fn a_non_zero_cache_write_needs_the_total_to_place_it() {
        let (_dir, path) = file_with(
            &[
                header(),
                assistant(
                    "w1",
                    r#"{"input":10,"output":2,"cacheRead":3,"cacheWrite":4,"totalTokens":19,"reasoningTokens":0}"#,
                ),
                assistant(
                    "w2",
                    r#"{"input":10,"output":2,"cacheRead":3,"cacheWrite":4,"totalTokens":15,"reasoningTokens":0}"#,
                ),
            ]
            .concat(),
        );
        let parse = parse(&path, None);
        assert_eq!(parse.calls[0].usage.input, DerivedRole::Measured(10));
        assert_eq!(parse.calls[1].usage.input, DerivedRole::Underivable);
        assert_eq!(parse.counts.underivable_roles, 1);
    }

    /// Reasoning above output contradicts the nested convention: conflicted,
    /// never clamped.
    #[test]
    fn reasoning_above_output_is_conflicted() {
        let usage = r#"{"input":1,"output":5,"cacheRead":0,"cacheWrite":0,"totalTokens":6,"reasoningTokens":9}"#;
        let (_dir, path) = file_with(&(header() + &assistant("c1", usage)));
        let parse = parse(&path, None);
        assert_eq!(
            parse.calls[0].usage.output,
            OmpOutput::Visible(DerivedRole::Conflicted)
        );
        assert_eq!(parse.counts.output_conflicts, 1);
    }

    /// A resumed read yields exactly the appended calls, and it and the
    /// first read together equal one full read: no duplicate on re-read.
    #[test]
    fn a_resumed_read_neither_repeats_nor_loses_calls() {
        let usage = r#"{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2}"#;
        let (_dir, path) = file_with(&(header() + &assistant("r1", usage)));
        let first = parse(&path, None);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(assistant("r2", usage).as_bytes()).unwrap();
        let second = parse(&path, first.resume);
        assert!(second.resumed);
        assert_eq!(second.counts.lines, 1);
        assert_eq!(second.calls.len(), 1);
        assert_eq!(second.calls[0].session_id, SESSION, "the header is kept");
        let full = parse(&path, None);
        assert_eq!(full.calls, [first.calls, second.calls].concat());
    }

    /// Usage before any header has no session to belong to: counted, not
    /// ingested under a made-up identity.
    #[test]
    fn usage_before_a_header_is_counted_not_emitted() {
        let usage = r#"{"input":1,"output":1}"#;
        let (_dir, path) = file_with(&assistant("h1", usage));
        let parse = parse(&path, None);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.usage_not_ingested(), 1);
    }
}
