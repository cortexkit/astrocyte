// Copied from tokscale v4.17.0 (tag commit
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7),
// crates/tokscale-core/src/sessions/gemini.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
//
// ASTROCYTE DIVERGENCE (whole file): upstream folds Gemini CLI transcripts
// into `UnifiedMessage` totals for reporting. Astrocyte prices each API call
// as its own ledger row, so this copy keeps only the chat-recording shapes
// (`GeminiSession`, `GeminiMessage`, the `tokens` block), the layout filter
// (a `session-*` file, or a file in a `chats/` directory), the JSONL
// recording's header and message lines, and the replace-by-message-id rule
// (as a refusal of a changed copy rather than a silent replace). It adds:
// counters that stay absent instead of decoding as zero, disjoint roles
// checked against each record's own `total`, a resumable byte-offset read
// for the JSONL recording, and a content digest for the rewritten JSON
// document. Dropped: the headless `gemini -p` output shapes (`init` lines,
// `stats`/`result.stats` session aggregates, direct top-level `tokens`
// lines) and the token-key aliases they need (`prompt`, `candidates`,
// `input_tokens`, `promptTokenCount` and the rest), because they are
// session aggregates, not per-call records, and none exist on the measured
// machine; `projectHash`, `startTime` and `lastUpdated`; the hard-coded
// `google` provider; and the file-mtime stand-in for a missing timestamp.

//! Gemini CLI session reader.
//!
//! Reads chat recordings below `~/.gemini/tmp/` (or `$GEMINI_CLI_HOME/tmp/`):
//! `<project>/chats/session-*.json`, a JSON document Gemini CLI rewrites in
//! full as the chat grows, and the newer `<project>/chats/session-*.jsonl`,
//! a header line followed by appended message lines and `{"$set":...}`
//! updates. Every message of type `gemini` that carries `tokens` is one
//! model response, one [`GeminiCall`].
//!
//! Token convention, measured 2026-09-24 on the development machine (24
//! files, 7 messages with `tokens`, all in JSON documents; too few to rule
//! out every shape, so each record's own `total` is checked):
//! `total == input + output + thoughts + tool` in 7 of 7. `cached` is
//! non-zero in 4 and is NOT added on top, so it is inside `input`.
//! `thoughts` is non-zero in 3 and IS added on top, so it is outside
//! `output`. `tool` is 0 in all 7, so its place is not established: a
//! non-zero `tool` leaves `input` underivable. A record whose `total`
//! contradicts the identity has both `input` and `output` underivable.

use super::jsonl_resume::{read_appended_lines, JsonlResumePoint, PrefixCheck};
use super::roles::DerivedRole;
use super::utils::parse_json_line;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;

/// The chat-recording header fields this reader uses.
///
/// ASTROCYTE DIVERGENCE: upstream requires every header field and a
/// `messages` array to accept a document; only `sessionId` identifies a
/// call, and the messages are decoded one by one so one bad message is a
/// counted skip, not a lost document.
#[derive(Debug, Deserialize)]
struct GeminiSession {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    messages: Option<Vec<serde_json::Value>>,
}

/// One chat message.
#[derive(Debug, Deserialize)]
struct GeminiMessage {
    id: Option<String>,
    timestamp: Option<String>,
    model: Option<String>,
    tokens: Option<GeminiTokens>,
}

/// The `tokens` block as Gemini CLI reported it.
///
/// ASTROCYTE DIVERGENCE: upstream reads each counter as `Option<i64>` from
/// a loose map and clamps absent and negative values to zero. Here every
/// counter is `Option<u64>`: an absent counter stays absent, and a negative
/// or non-integer one fails the message's decode, which is counted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct GeminiTokens {
    input: Option<u64>,
    output: Option<u64>,
    cached: Option<u64>,
    thoughts: Option<u64>,
    tool: Option<u64>,
    total: Option<u64>,
}

/// Usage of one call in disjoint roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeminiCallUsage {
    /// `input - cached`: uncached input.
    pub input: DerivedRole,
    /// `cached` verbatim.
    pub cache_read: Option<u64>,
    /// `output` verbatim (thoughts are outside it).
    pub output: DerivedRole,
    /// `thoughts` verbatim.
    pub reasoning: Option<u64>,
}

impl GeminiCallUsage {
    fn from_tokens(tokens: &GeminiTokens) -> Self {
        let identity_holds = match tokens.total {
            None => true,
            Some(total) => {
                [tokens.input, tokens.output, tokens.thoughts, tokens.tool]
                    .into_iter()
                    .try_fold(0_u64, |sum, part| sum.checked_add(part?))
                    == Some(total)
            }
        };
        let tool_unmeasured = tokens.tool.is_some_and(|tool| tool > 0);
        Self {
            input: DerivedRole::difference(tokens.input, tokens.cached)
                .unless(tool_unmeasured || !identity_holds),
            cache_read: tokens.cached,
            output: DerivedRole::verbatim(tokens.output).unless(!identity_holds),
            reasoning: tokens.thoughts,
        }
    }
}

/// One model response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiCall {
    /// The recording's `sessionId`. One session can be saved in more than
    /// one file (measured: a session in three files, and a project directory
    /// copied under a second name); its calls keep one identity.
    pub session_id: String,
    /// The message's `id`: unique within its session.
    pub message_id: String,
    /// `model` verbatim.
    pub model: Option<String>,
    /// The message's RFC 3339 `timestamp`; absent when it has none or it
    /// does not parse. Never a file mtime.
    pub occurred_at_ms: Option<i64>,
    pub usage: GeminiCallUsage,
}

/// Counts for one parse call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeminiParseCounts {
    /// Complete (newline-terminated) JSONL lines consumed.
    pub lines: u64,
    /// JSONL lines that are not JSON, plus messages (in either format)
    /// whose fields do not decode (a negative counter, a wrong type).
    /// Reading continues past them.
    pub skipped_lines: u64,
    /// JSON documents read.
    pub documents: u64,
    /// JSON documents that are not a readable chat recording (not JSON, or
    /// not an object); nothing in them is ingested.
    pub unreadable_documents: u64,
    /// Messages with a `tokens` block.
    pub token_messages: u64,
    /// Token messages whose `id` was already read in this file with
    /// identical content: a re-send, not a new response.
    pub repeated_messages: u64,
    /// Token messages whose `id` was already read in this file with other
    /// content. Not ingested; none measured.
    pub conflicting_messages: u64,
    /// Token messages without a message id, or in a file without a
    /// `sessionId`. Not ingested; none measured.
    pub messages_unidentifiable: u64,
    /// Emitted calls with `input` or `output` underivable.
    pub underivable_roles: u64,
    /// Emitted calls whose `input` subtraction went negative.
    pub input_conflicts: u64,
}

impl GeminiParseCounts {
    /// Usage the reader saw and did not ingest for a reason other than a
    /// benign repeat.
    pub fn usage_not_ingested(&self) -> u64 {
        self.conflicting_messages + self.messages_unidentifiable + self.unreadable_documents
    }

    super::utils::add_parse_counts!(
        lines,
        skipped_lines,
        documents,
        unreadable_documents,
        token_messages,
        repeated_messages,
        conflicting_messages,
        messages_unidentifiable,
        underivable_roles,
        input_conflicts,
    );
}

/// What one file has shown so far: its session and every token message
/// read, by id.
#[derive(Debug, Default)]
pub(crate) struct GeminiParseState {
    session_id: Option<String>,
    seen: HashMap<String, (Option<String>, Option<String>, GeminiTokens)>,
}

impl GeminiParseState {
    /// Read one message value; emits a call for a new token message.
    fn observe(
        &mut self,
        value: &serde_json::Value,
        counts: &mut GeminiParseCounts,
        calls: &mut Vec<GeminiCall>,
    ) {
        if value.get("tokens").is_none() {
            return;
        }
        counts.token_messages += 1;
        let Ok(message) = GeminiMessage::deserialize(value) else {
            counts.skipped_lines += 1;
            return;
        };
        let Some(tokens) = message.tokens else {
            // `"tokens": null`: nothing reported.
            return;
        };
        let (Some(session_id), Some(message_id)) = (
            self.session_id.clone(),
            message.id.filter(|id| !id.is_empty()),
        ) else {
            counts.messages_unidentifiable += 1;
            return;
        };
        let content = (message.model, message.timestamp, tokens);
        match self.seen.get(&message_id) {
            Some(earlier) if *earlier == content => {
                counts.repeated_messages += 1;
                return;
            }
            // ASTROCYTE DIVERGENCE: upstream lets a later copy of a message
            // id replace the earlier one. A changed copy is a revision
            // nobody measured, so it is counted and the first copy stands.
            Some(_) => {
                counts.conflicting_messages += 1;
                return;
            }
            None => {}
        }
        let (model, timestamp, tokens) = &content;
        let usage = GeminiCallUsage::from_tokens(tokens);
        counts.underivable_roles += [usage.input, usage.output]
            .into_iter()
            .filter(|role| *role == DerivedRole::Underivable)
            .count() as u64;
        counts.input_conflicts += u64::from(usage.input == DerivedRole::Conflicted);
        calls.push(GeminiCall {
            session_id,
            message_id: message_id.clone(),
            model: model.clone(),
            occurred_at_ms: timestamp
                .as_deref()
                .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
                .map(|timestamp| timestamp.timestamp_millis()),
            usage,
        });
        self.seen.insert(message_id, content);
    }
}

/// Whether `path` is a chat recording upstream's layout filter accepts: a
/// `.json` or `.jsonl` file named `session-*`, or any such file directly in
/// a `chats/` directory. `logs.json` and other project files are not.
pub fn is_gemini_chat_recording(path: &Path) -> bool {
    let extension_ok = path
        .extension()
        .is_some_and(|extension| extension == "json" || extension == "jsonl");
    let named_session = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("session-"));
    let in_chats = path
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "chats");
    extension_ok && (named_session || in_chats)
}

/// Result of one [`parse_gemini_json_document`] call.
#[derive(Debug, Default)]
pub struct GeminiDocumentParse {
    /// Every call in the document, in document order.
    pub calls: Vec<GeminiCall>,
    /// SHA-256 of the bytes parsed, for telling a later rewrite apart.
    pub sha256: [u8; 32],
    /// Bytes read and hashed: the whole document.
    pub hashed_bytes: u64,
    pub counts: GeminiParseCounts,
}

/// Reads a JSON chat recording once, then hashes and parses those same bytes.
/// Gemini CLI rewrites the document as the chat grows, so there is no
/// append-only prefix to resume from; the caller decides from the digest
/// whether a document changed. Returns `None` when the file cannot be read.
pub fn read_gemini_json_document(path: &Path) -> Option<GeminiDocumentParse> {
    let bytes = std::fs::read(path).ok()?;
    Some(parse_gemini_json_document_bytes(&bytes))
}

/// Parses a JSON chat recording in full by delegating to
/// [`read_gemini_json_document`]. Each parsed token message is emitted again on
/// every full read; the ledger absorbs those exact redeliveries. Returns
/// `None` when the file cannot be read.
pub fn parse_gemini_json_document(path: &Path) -> Option<GeminiDocumentParse> {
    read_gemini_json_document(path)
}

fn parse_gemini_json_document_bytes(bytes: &[u8]) -> GeminiDocumentParse {
    let mut parse = GeminiDocumentParse {
        sha256: Sha256::digest(bytes).into(),
        hashed_bytes: bytes.len() as u64,
        ..GeminiDocumentParse::default()
    };
    parse.counts.documents = 1;
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim_start_matches('\u{feff}');
    let Ok(session) = serde_json::from_str::<GeminiSession>(text) else {
        parse.counts.unreadable_documents = 1;
        return parse;
    };
    let mut state = GeminiParseState {
        session_id: session.session_id.filter(|id| !id.is_empty()),
        ..GeminiParseState::default()
    };
    for message in session.messages.iter().flatten() {
        state.observe(message, &mut parse.counts, &mut parse.calls);
    }
    parse
}

fn parse_jsonl_line(
    payload: &[u8],
    buffer: &mut Vec<u8>,
    state: &mut GeminiParseState,
    counts: &mut GeminiParseCounts,
    calls: &mut Vec<GeminiCall>,
) {
    let Ok(text) = std::str::from_utf8(payload) else {
        counts.skipped_lines += 1;
        return;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let Some(value) = parse_json_line::<serde_json::Value>(trimmed, buffer) else {
        counts.skipped_lines += 1;
        return;
    };
    if let Some(update) = value.get("$set") {
        // An update of the conversation's fields. A replaced message list
        // is read like appended messages: copies of messages already read
        // are repeats.
        for message in update
            .get("messages")
            .and_then(|messages| messages.as_array())
            .into_iter()
            .flatten()
        {
            state.observe(message, counts, calls);
        }
        return;
    }
    if state.session_id.is_none() {
        if let Some(id) = value.get("sessionId").and_then(|id| id.as_str()) {
            // The header line; a later one never re-labels the file.
            state.session_id = Some(id.to_string()).filter(|id| !id.is_empty());
            return;
        }
    }
    state.observe(&value, counts, calls);
}

/// Where a previous [`parse_gemini_jsonl_resumable`] stopped.
#[derive(Debug)]
pub struct GeminiResumePoint(JsonlResumePoint<GeminiParseState>);

impl GeminiResumePoint {
    /// Complete bytes consumed, excluding an unterminated tail.
    pub fn offset(&self) -> u64 {
        self.0.offset()
    }
}

/// Result of one [`parse_gemini_jsonl_resumable`] call.
#[derive(Debug, Default)]
pub struct GeminiResumableParse {
    /// Calls read by this call, in file order.
    pub calls: Vec<GeminiCall>,
    /// True when the previous point's prefix verified and parsing started at
    /// its offset; false for a full read from the start.
    pub resumed: bool,
    /// Where the next call may resume, or `None` when the file could not be
    /// read, so the next call reads it in full.
    pub resume: Option<GeminiResumePoint>,
    /// Bytes fed to SHA-256 by this call: O(window + appended) on a resume.
    pub hashed_bytes: u64,
    pub counts: GeminiParseCounts,
}

/// Parses a JSONL chat recording, continuing from `previous` when the bytes
/// it consumed are unchanged (the shared resume in `jsonl_resume.rs`). Only
/// newline-terminated lines are consumed.
pub fn parse_gemini_jsonl_resumable(
    path: &Path,
    previous: Option<GeminiResumePoint>,
    check: PrefixCheck,
) -> GeminiResumableParse {
    let mut counts = GeminiParseCounts::default();
    let mut calls = Vec::new();
    let mut buffer = Vec::with_capacity(4096);
    let Some(read) = read_appended_lines(
        path,
        previous.map(|point| point.0),
        check,
        GeminiParseState::default,
        |state, payload| {
            counts.lines += 1;
            parse_jsonl_line(payload, &mut buffer, state, &mut counts, &mut calls);
        },
    ) else {
        return GeminiResumableParse::default();
    };
    GeminiResumableParse {
        calls,
        resumed: read.resumed,
        resume: read
            .cursor
            .map(|cursor| GeminiResumePoint(cursor.with_state(read.state))),
        hashed_bytes: read.hashed_bytes,
        counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const SESSION: &str = "2fd93fbe-0000-4000-8000-000000000001";

    /// `[input, output, cached, thoughts, tool]`; `total` follows the
    /// measured identity (cached not added).
    fn tokens(counters: [u64; 5]) -> String {
        let [input, output, cached, thoughts, tool] = counters;
        format!(
            r#"{{"input":{input},"output":{output},"cached":{cached},"thoughts":{thoughts},"tool":{tool},"total":{}}}"#,
            input + output + thoughts + tool
        )
    }

    fn message(id: &str, tokens: &str) -> String {
        format!(
            r#"{{"id":"{id}","timestamp":"2026-02-19T22:20:48.450Z","type":"gemini","content":"ok","thoughts":[],"tokens":{tokens},"model":"gemini-3-flash-preview"}}"#
        )
    }

    fn document(messages: &[String]) -> String {
        format!(
            r#"{{"sessionId":"{SESSION}","projectHash":"665a","startTime":"2026-02-19T22:18:00.000Z","lastUpdated":"2026-02-19T22:22:00.000Z","messages":[{{"id":"u1","timestamp":"2026-02-19T22:20:40.000Z","type":"user","content":"hi"}},{}]}}"#,
            messages.join(",")
        )
    }

    fn file_with(name: &str, contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj").join("chats").join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    /// Cached input is inside `input` and subtracted; thoughts are outside
    /// `output` and stored beside it.
    #[test]
    fn cached_input_is_subtracted_and_thoughts_kept_apart() {
        let (_dir, path) = file_with(
            "session-a.json",
            &document(&[message("fc", &tokens([8075, 66, 6031, 23, 0]))]),
        );
        let parse = parse_gemini_json_document(&path).unwrap();
        assert_eq!(parse.calls.len(), 1);
        let call = &parse.calls[0];
        assert_eq!(
            call.usage,
            GeminiCallUsage {
                input: DerivedRole::Measured(2044),
                cache_read: Some(6031),
                output: DerivedRole::Measured(66),
                reasoning: Some(23),
            }
        );
        assert_eq!(
            (call.session_id.as_str(), call.message_id.as_str()),
            (SESSION, "fc")
        );
        assert_eq!(call.model.as_deref(), Some("gemini-3-flash-preview"));
        assert!(call.occurred_at_ms.is_some());
    }

    /// An absent counter is omitted, never zero: without `cached` the
    /// uncached part of `input` is unknown; without `thoughts` nothing is
    /// claimed about reasoning.
    #[test]
    fn absent_counters_are_omitted_not_zero() {
        let (_dir, path) = file_with(
            "session-a.json",
            &document(&[message("m", r#"{"input":10,"output":3}"#)]),
        );
        let usage = parse_gemini_json_document(&path).unwrap().calls[0].usage;
        assert_eq!(usage.input, DerivedRole::Underivable);
        assert_eq!(usage.cache_read, None);
        assert_eq!(usage.reasoning, None);
        assert_eq!(usage.output, DerivedRole::Measured(3));
    }

    /// A negative or non-integer counter fails that message: counted, never
    /// zeroed, and the document's other messages still read.
    #[test]
    fn a_malformed_counter_is_counted_and_the_rest_reads() {
        let (_dir, path) = file_with(
            "session-a.json",
            &document(&[
                message("bad", r#"{"input":-1,"output":3}"#),
                message("str", r#"{"input":"10","output":3}"#),
                message("ok", &tokens([10, 3, 0, 0, 0])),
            ]),
        );
        let parse = parse_gemini_json_document(&path).unwrap();
        assert_eq!(parse.counts.skipped_lines, 2);
        assert_eq!(parse.calls.len(), 1);
        assert_eq!(parse.calls[0].message_id, "ok");
    }

    /// A `total` that contradicts the measured identity, or a non-zero
    /// `tool` count, leaves the affected roles underivable.
    #[test]
    fn a_contradicting_total_or_a_tool_count_is_not_priced() {
        let (_dir, path) = file_with(
            "session-a.json",
            &document(&[
                message(
                    "t",
                    r#"{"input":10,"output":3,"cached":4,"thoughts":1,"tool":0,"total":18}"#,
                ),
                message("u", &tokens([10, 3, 4, 1, 2])),
            ]),
        );
        let parse = parse_gemini_json_document(&path).unwrap();
        assert_eq!(parse.calls[0].usage.input, DerivedRole::Underivable);
        assert_eq!(parse.calls[0].usage.output, DerivedRole::Underivable);
        assert_eq!(parse.calls[1].usage.input, DerivedRole::Underivable);
        assert_eq!(parse.calls[1].usage.output, DerivedRole::Measured(3));
        assert_eq!(parse.counts.underivable_roles, 3);
    }

    /// A document that is not a readable recording is counted.
    #[test]
    fn an_unreadable_document_is_counted() {
        let (_dir, path) = file_with("session-a.json", "{\"sessionId\":");
        let parse = parse_gemini_json_document(&path).unwrap();
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.usage_not_ingested(), 1);
    }

    /// The JSONL recording: header, appended messages, `$set` updates. A
    /// resumed read yields only new calls; a copy in `$set` is a repeat; a
    /// resumed read plus the first equals one full read.
    #[test]
    fn a_jsonl_recording_resumes_without_duplicates() {
        let header = format!(
            r#"{{"sessionId":"{SESSION}","projectHash":"0f39","startTime":"2026-06-16T21:45:21.315Z","lastUpdated":"2026-06-16T21:45:21.315Z","kind":"main"}}"#
        ) + "\n";
        let first = message("g1", &tokens([100, 5, 60, 2, 0]));
        let (_dir, path) = file_with(
            "session-b.jsonl",
            &(header
                + r#"{"id":"u1","timestamp":"2026-06-16T21:45:21.315Z","type":"user","content":[{"text":"hi"}]}"#
                + "\n"
                + &first
                + "\n"),
        );
        let initial = parse_gemini_jsonl_resumable(&path, None, PrefixCheck::Window);
        assert_eq!(initial.calls.len(), 1);
        assert_eq!(initial.calls[0].usage.input, DerivedRole::Measured(40));
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, r#"{{"$set":{{"messages":[{first}]}}}}"#).unwrap();
        writeln!(file, "{}", message("g2", &tokens([7, 1, 0, 0, 0]))).unwrap();
        writeln!(file, "not json").unwrap();
        let resumed = parse_gemini_jsonl_resumable(&path, initial.resume, PrefixCheck::Window);
        assert!(resumed.resumed);
        assert_eq!(resumed.counts.lines, 3);
        assert_eq!(resumed.counts.repeated_messages, 1);
        assert_eq!(resumed.counts.skipped_lines, 1);
        assert_eq!(resumed.calls.len(), 1);
        assert_eq!(resumed.calls[0].session_id, SESSION);

        let full = parse_gemini_jsonl_resumable(&path, None, PrefixCheck::Full);
        assert_eq!(full.calls, [initial.calls, resumed.calls].concat());
    }

    /// A message id re-sent with different tokens is counted, not ingested.
    #[test]
    fn a_changed_copy_of_a_message_is_counted_not_emitted() {
        let (_dir, path) = file_with(
            "session-a.json",
            &document(&[
                message("m", &tokens([10, 3, 0, 0, 0])),
                message("m", &tokens([11, 3, 0, 0, 0])),
            ]),
        );
        let parse = parse_gemini_json_document(&path).unwrap();
        assert_eq!(parse.calls.len(), 1);
        assert_eq!(parse.counts.conflicting_messages, 1);
    }

    #[test]
    fn the_layout_filter_accepts_chat_recordings_only() {
        assert!(is_gemini_chat_recording(Path::new(
            "/h/.gemini/tmp/p/chats/session-1.json"
        )));
        assert!(is_gemini_chat_recording(Path::new(
            "/h/.gemini/tmp/p/chats/6b3c.jsonl"
        )));
        assert!(is_gemini_chat_recording(Path::new(
            "/h/.gemini/tmp/p/session-1.json"
        )));
        assert!(!is_gemini_chat_recording(Path::new(
            "/h/.gemini/tmp/p/logs.json"
        )));
        assert!(!is_gemini_chat_recording(Path::new(
            "/h/.gemini/tmp/p/chats/notes.txt"
        )));
    }
}
