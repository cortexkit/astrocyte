// Copied from tokscale v4.17.0 (tag commit
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7),
// crates/tokscale-core/src/sessions/codex.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
//
// ASTROCYTE DIVERGENCE (whole file): upstream folds a rollout into
// `UnifiedMessage` totals for reporting. Astrocyte prices each API call as
// its own ledger row, so this copy keeps only the record shapes
// (`CodexEntry`, `CodexPayload`, `CodexInfo`, `CodexTokenUsage`), the
// timestamp parse, the "unchanged cumulative total is a repeat" rule, the
// `session_meta` fork markers (`forked_from_id` and
// `source.subagent.thread_spawn.parent_thread_id`) and the UUID v7
// millisecond comparison upstream uses to tell a fork's replayed parent turns
// from its own. It adds: counters that stay absent instead of decoding as
// zero, disjoint roles that refuse (rather than clamp) an impossible
// subtraction, a content-stable per-call identity, a per-turn working
// directory, and a resumable byte-offset read. Dropped as not needed for
// ingest: headless `codex exec --json` stdout lines, agent/subagent display
// buckets, OpenClaw ownership, turn-start and duration bookkeeping, the
// deferred-model queue, and the "stale regression" heuristic (a regressing
// total is counted and not ingested instead). Divergences inside retained
// code are marked inline. Archive bounds, request settlement and activity
// diagnostics are adapted from tokscale v4.20.0 (c1973901fc2f7ba0d134252bfd923c714cc123db).

//! Codex CLI session reader.
//!
//! Reads `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` and `.jsonl.zst`
//! (and the same shapes under `~/.codex/archived_sessions/`). Every record is
//! `{"timestamp","ordinal","type","payload"}`.
//!
//! Codex writes an `event_msg` `token_count` record after every completed
//! model response. `last_token_usage` is that response's usage and
//! `total_token_usage` the thread's running total. The same totals are
//! re-sent in later `token_count` records (rate-limit updates) without a new
//! response; those repeats are recognised by an unchanged total and skipped.
//! One advancing `token_count` is one [`CodexCall`].
//!
//! Token convention, measured 2026-09-24 over every `token_count` on the
//! development machine (29 files, 494 `token_count` records, 487 with usage):
//! `total_tokens == input_tokens + output_tokens` in 487 of 487 running
//! totals and 486 of 487 per-response blocks (the exception is a repeat whose
//! per-response block is all zeros beside a non-zero `total_tokens`, and it
//! is skipped as a repeat). Reasoning is non-zero in 480 totals and still not
//! added on top, so `reasoning_output_tokens` is inside `output_tokens`. No
//! record has `cached_input_tokens > input_tokens` (486 have cached input) or
//! `reasoning_output_tokens > output_tokens`, as a subset relation requires.
//! `cache_write_input_tokens` is 0 in every record, so whether a cache write
//! is inside `input_tokens` is NOT established; see [`CodexDerived`].

use super::jsonl_resume::{JsonlResumePoint, PrefixCheck, PREFIX_WINDOW_BYTES};
use super::normalize_workspace_key;
use super::utils::{parse_json_line, parse_rfc3339_millis};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::path::Path;

#[path = "codex_archive.rs"]
mod archive;

/// Whether the rollout is a compressed archive rather than appendable JSONL.
pub fn is_codex_archive(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "zst")
        && path
            .with_extension("")
            .extension()
            .is_some_and(|extension| extension == "jsonl")
}

/// Bytes of the already-consumed prefix that a [`CodexPrefixCheck::Window`]
/// resume re-hashes.
///
/// ASTROCYTE DIVERGENCE: tokscale's incremental Codex parse trusts the stored
/// offset. This reader resumes through the shared append-only JSONL resume in
/// `jsonl_resume.rs`, which the Claude Code reader also uses.
pub const CODEX_PREFIX_WINDOW_BYTES: u64 = PREFIX_WINDOW_BYTES;

/// How much of a file's already-consumed prefix to re-hash before trusting a
/// resume point: the shared [`PrefixCheck`].
pub type CodexPrefixCheck = PrefixCheck;

/// One rollout record.
///
/// ASTROCYTE DIVERGENCE: adds `ordinal`, the record's position in its
/// thread, used only when a usage record has no turn id.
#[derive(Debug, Deserialize)]
struct CodexEntry {
    #[serde(rename = "type")]
    entry_type: String,
    timestamp: Option<String>,
    ordinal: Option<u64>,
    payload: Option<CodexPayload>,
}

/// The payload fields this reader uses, from `session_meta`, `turn_context`
/// and `event_msg` payloads alike; every field is optional because each
/// payload kind carries only some of them.
///
/// ASTROCYTE DIVERGENCE: adds `session_id` (the conversation a subagent
/// thread belongs to; `id` is the thread's own id) and drops the display and
/// ownership fields upstream uses.
#[derive(Debug, Deserialize)]
struct CodexPayload {
    #[serde(rename = "type")]
    payload_type: Option<String>,
    id: Option<String>,
    session_id: Option<String>,
    forked_from_id: Option<String>,
    source: Option<serde_json::Value>,
    cwd: Option<String>,
    model_provider: Option<String>,
    model: Option<String>,
    turn_id: Option<String>,
    info: Option<CodexInfo>,
    thread_id: Option<String>,
    response_id: Option<String>,
    usage: Option<CodexTokenUsage>,
}

#[derive(Debug, Deserialize)]
struct CodexInfo {
    last_token_usage: Option<CodexTokenUsage>,
    total_token_usage: Option<CodexTokenUsage>,
    /// The model's context window, kept for cache diagnostics. Decoded
    /// leniently so a malformed value cannot make a record that decoded
    /// before fail now.
    #[serde(default, deserialize_with = "super::lenient::unsigned")]
    model_context_window: Option<u64>,
}

/// One usage block as Codex reported it.
///
/// ASTROCYTE DIVERGENCE: upstream decodes `Option<i64>` and then turns absent
/// and negative values into zero (`unwrap_or(0).max(0)`), which is harmless
/// in a total and wrong in a priced row. Here every counter is
/// `Option<u64>`: an absent counter stays absent, and a negative one fails
/// the record's decode, which is counted as a skipped line. Upstream's
/// `cache_read_input_tokens` alias is dropped (no measured record has it) and
/// `cache_write_input_tokens` is added.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct CodexTokenUsage {
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    cache_write_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl CodexTokenUsage {
    fn fields(&self) -> [Option<u64>; 6] {
        [
            self.input_tokens,
            self.cached_input_tokens,
            self.cache_write_input_tokens,
            self.output_tokens,
            self.reasoning_output_tokens,
            self.total_tokens,
        ]
    }

    /// True when any counter reported in both is lower here than in
    /// `previous`: a running total that went backwards.
    fn regresses_from(&self, previous: &Self) -> bool {
        self.fields()
            .into_iter()
            .zip(previous.fields())
            .any(|(now, before)| matches!((now, before), (Some(now), Some(before)) if now < before))
    }

    /// True when `self - previous`, counter by counter, is exactly `last`.
    /// A counter present on one side only cannot be checked, so it fails.
    fn advances_by(&self, previous: &Self, last: &Self) -> bool {
        self.fields()
            .into_iter()
            .zip(previous.fields())
            .zip(last.fields())
            .all(|((now, before), step)| match (now, before, step) {
                (None, None, None) => true,
                (Some(now), Some(before), Some(step)) => now.checked_sub(before) == Some(step),
                _ => false,
            })
    }
}

/// One disjoint role derived from Codex's nested counters.
///
/// Codex reports `cached_input_tokens` inside `input_tokens` and
/// `reasoning_output_tokens` inside `output_tokens`; the ledger stores
/// disjoint roles, so `input` and `output` are differences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexDerived {
    /// The disjoint quantity.
    Measured(u64),
    /// The gross counter was not reported: nothing to store.
    Absent,
    /// The gross counter was reported but the disjoint part cannot be known
    /// without a guess: the counter to subtract is absent, or (for input) a
    /// non-zero cache write was reported, whose place inside or outside
    /// `input_tokens` no measured record establishes.
    Underivable,
    /// The subtraction would go negative (`cached > input` or
    /// `reasoning > output`), which the nested convention forbids. Stored
    /// without a quantity so it never prices; never clamped.
    Conflicted,
}

impl CodexDerived {
    pub fn quantity(self) -> Option<u64> {
        match self {
            Self::Measured(quantity) => Some(quantity),
            Self::Absent | Self::Underivable | Self::Conflicted => None,
        }
    }
}

/// Usage of one call in disjoint roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexCallUsage {
    /// `input_tokens - cached_input_tokens`: uncached input.
    pub input: CodexDerived,
    /// `cached_input_tokens` verbatim.
    pub cache_read: Option<u64>,
    /// `cache_write_input_tokens` verbatim.
    pub cache_write: Option<u64>,
    /// `output_tokens - reasoning_output_tokens`: visible output.
    pub output: CodexDerived,
    /// `reasoning_output_tokens` verbatim.
    pub reasoning: Option<u64>,
}

impl CodexCallUsage {
    fn from_usage(usage: &CodexTokenUsage) -> Self {
        let input = match (usage.input_tokens, usage.cached_input_tokens) {
            (None, _) => CodexDerived::Absent,
            (Some(_), None) => CodexDerived::Underivable,
            (Some(input), Some(cached)) => match input.checked_sub(cached) {
                None => CodexDerived::Conflicted,
                Some(_)
                    if usage
                        .cache_write_input_tokens
                        .is_some_and(|write| write > 0) =>
                {
                    CodexDerived::Underivable
                }
                Some(uncached) => CodexDerived::Measured(uncached),
            },
        };
        let output = match (usage.output_tokens, usage.reasoning_output_tokens) {
            (None, _) => CodexDerived::Absent,
            // Reasoning is inside output, so without the reasoning count the
            // visible part is unknown. Storing the gross output as visible
            // output would be a guess that reasoning was zero.
            (Some(_), None) => CodexDerived::Underivable,
            (Some(output), Some(reasoning)) => output
                .checked_sub(reasoning)
                .map_or(CodexDerived::Conflicted, CodexDerived::Measured),
        };
        Self {
            input,
            cache_read: usage.cached_input_tokens,
            cache_write: usage.cache_write_input_tokens,
            output,
            reasoning: usage.reasoning_output_tokens,
        }
    }
}

/// One model response, from one advancing `token_count` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexCall {
    /// True for calls emitted from `token_usage_record`; these use request
    /// identities rather than native `token_count` response positions.
    pub supplementary: bool,
    /// One-based line of the token-count record, retained across resumes.
    pub source_line: u64,
    pub set_aside_reason: Option<String>,
    /// `session_meta.session_id`: the conversation. A subagent thread
    /// carries its parent's value here, so it is shared by every thread of
    /// one conversation. Falls back to the thread id when absent.
    pub session_id: String,
    /// `session_meta.id`: this thread (one rollout file).
    pub thread_id: String,
    /// Stable per call: `<turn_id>:<n>` for the n-th response of the turn
    /// (1-based). See [`parse_codex_file_resumable`] for why.
    pub producer_ref: String,
    pub turn_id: Option<String>,
    /// From the latest `turn_context` before the record, verbatim.
    pub model: Option<String>,
    /// `session_meta.model_provider` verbatim.
    pub provider: Option<String>,
    /// The latest `turn_context` cwd, else the `session_meta` cwd; normalized.
    pub project_root: Option<String>,
    /// The `token_count` record's timestamp. Absent when it has none: never
    /// a file mtime.
    pub occurred_at_ms: Option<i64>,
    pub usage: CodexCallUsage,
    /// `info.model_context_window` of the `token_count` record, for cache
    /// diagnostics only. `None` for a `token_usage_record` call.
    pub context_limit: Option<u64>,
}

/// Counts for one parse call. Every count covers only the lines this call
/// consumed, so a resumed parse reports the appended lines alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CodexParseCounts {
    /// Complete (newline-terminated) lines consumed.
    pub lines: u64,
    /// Complete lines that could not be read: not JSON, or a `session_meta`,
    /// `turn_context`, `task_started` or `token_count` record whose fields do
    /// not decode (a negative counter, a wrong type). Reading continues past
    /// them. An unterminated final line is never consumed nor counted.
    pub skipped_lines: u64,
    /// `token_count` records seen.
    pub token_count_records: u64,
    /// `token_count` records with no usage (`info: null`), sent for
    /// rate-limit updates before the first response.
    pub token_counts_without_info: u64,
    /// `token_count` records whose running total equals the previous one:
    /// a re-send, not a new response.
    pub repeated_totals: u64,
    /// Advancing `token_count` records: one per response, including fork
    /// replays.
    pub usage_records: u64,
    /// Responses in a forked thread's replay of its parent's history,
    /// recorded by the parent's own file and not emitted again here.
    pub fork_replays_skipped: u64,
    /// `token_count` records whose running total went backwards. Not
    /// ingested; none measured.
    pub regressed_totals: u64,
    /// Usage records lacking `total_token_usage` or `last_token_usage`.
    /// Not ingested: without the running total a repeat cannot be told from
    /// a new response. None measured.
    pub records_without_usage_pair: u64,
    /// Usage records with no preceding `session_meta` thread id, or with
    /// neither a turn id nor an ordinal. Not ingested; none measured.
    pub records_unidentifiable: u64,
    /// Ingested records whose `last_token_usage` is not exactly the growth
    /// of the running total since the previous response. None measured.
    pub last_disagrees_with_total: u64,
    /// Calls whose `input` is [`CodexDerived::Conflicted`].
    pub input_conflicts: u64,
    /// Calls whose `output` is [`CodexDerived::Conflicted`].
    pub output_conflicts: u64,
    /// Calls whose `input` or `output` is [`CodexDerived::Underivable`].
    pub underivable_roles: u64,
    pub supplementary_requests: u64,
}

impl CodexParseCounts {
    /// Usage records the reader saw and did not emit for a reason other than
    /// a benign repeat or a fork replay: a new shape nobody measured.
    pub fn usage_not_ingested(&self) -> u64 {
        self.regressed_totals + self.records_without_usage_pair + self.records_unidentifiable
    }

    super::utils::add_parse_counts!(
        lines,
        skipped_lines,
        token_count_records,
        token_counts_without_info,
        repeated_totals,
        usage_records,
        fork_replays_skipped,
        regressed_totals,
        records_without_usage_pair,
        records_unidentifiable,
        last_disagrees_with_total,
        input_conflicts,
        output_conflicts,
        underivable_roles,
        supplementary_requests,
    );
}

/// Only the record type, for telling an unreadable record this reader
/// depends on (a skipped line) from a record of another kind whose shape the
/// typed decode does not expect (not skipped).
#[derive(Deserialize)]
struct EntryTypeProbe {
    #[serde(rename = "type")]
    entry_type: Option<String>,
}

/// Only an `event_msg` payload's type, for the same purpose.
#[derive(Deserialize)]
struct EventPayloadProbe {
    payload: Option<PayloadTypeProbe>,
}

#[derive(Deserialize)]
struct PayloadTypeProbe {
    #[serde(rename = "type")]
    payload_type: Option<String>,
}

/// Whether a line the typed decode rejected is a skipped line: not JSON, or
/// a record kind this reader depends on. Other kinds (`response_item` and
/// the rest) may carry shapes the typed decode was never meant to accept.
fn unreadable_line_is_skipped(trimmed: &str, buffer: &mut Vec<u8>) -> bool {
    let Some(probe) = parse_json_line::<EntryTypeProbe>(trimmed, buffer) else {
        return true;
    };
    match probe.entry_type.as_deref() {
        Some("session_meta") | Some("turn_context") | Some("token_usage_record") => true,
        Some("event_msg") => match parse_json_line::<EventPayloadProbe>(trimmed, buffer) {
            Some(EventPayloadProbe {
                payload: Some(PayloadTypeProbe { payload_type }),
            }) => matches!(
                payload_type.as_deref(),
                Some("token_count") | Some("task_started")
            ),
            _ => true,
        },
        _ => false,
    }
}

#[derive(Debug, Clone)]
struct ThreadInfo {
    thread_id: String,
    session_id: String,
    provider: Option<String>,
    cwd: Option<String>,
    /// `session_meta` names a parent: the file starts with a replay of the
    /// parent's history.
    is_fork: bool,
    /// Millisecond timestamp in the thread id, when it is a UUID v7.
    created_ms: Option<u64>,
}

/// Parse state for one file, carried across resumed reads.
#[derive(Debug, Clone, Default)]
struct CodexParseState {
    lines: u64,
    thread: Option<ThreadInfo>,
    current_turn: Option<String>,
    current_model: Option<String>,
    current_cwd: Option<String>,
    previous_total: Option<CodexTokenUsage>,
    responses_in_turn: HashMap<String, u32>,
    pending: Option<PendingRequest>,
    confirmations: VecDeque<(Option<String>, CodexTokenUsage)>,
    settled: VecDeque<(Option<String>, CodexTokenUsage)>,
    activity: bool,
}

#[derive(Debug, Clone)]
struct PendingRequest {
    call: CodexCall,
    usage: CodexTokenUsage,
    compacted: bool,
}

fn same_response(left: &CodexTokenUsage, right: &CodexTokenUsage) -> bool {
    matches!((left.input_tokens, left.output_tokens, right.input_tokens, right.output_tokens),
        (Some(li), Some(lo), Some(ri), Some(ro)) if li == ri && lo == ro)
}

fn take_confirmation(
    records: &mut VecDeque<(Option<String>, CodexTokenUsage)>,
    turn: &Option<String>,
    usage: &CodexTokenUsage,
) -> bool {
    let found = records.iter().position(|(record_turn, record_usage)| {
        record_turn == turn && same_response(record_usage, usage)
    });
    found.is_some_and(|index| records.remove(index).is_some())
}

fn remember_response(
    records: &mut VecDeque<(Option<String>, CodexTokenUsage)>,
    turn: Option<String>,
    usage: CodexTokenUsage,
) {
    // Late confirmations are local to recent requests, not a transcript-sized cache.
    if records.len() == 256 {
        records.pop_front();
    }
    records.push_back((turn, usage));
}

fn settle_pending(
    state: &mut CodexParseState,
    counts: &mut CodexParseCounts,
    calls: &mut Vec<CodexCall>,
) {
    if let Some(pending) = state.pending.take() {
        remember_response(
            &mut state.settled,
            pending.call.turn_id.clone(),
            pending.usage,
        );
        counts.supplementary_requests += 1;
        calls.push(pending.call);
    }
}

/// Millisecond timestamp of a UUID v7 (its first 48 bits), or `None` for any
/// other id. Validate the same hyphenated hexadecimal spelling as upstream,
/// including the unused suffix, without allocating normalized strings.
fn uuid_v7_millis(id: &str) -> Option<u64> {
    let bytes = id.as_bytes();
    if bytes.len() != 36 || bytes[14] != b'7' {
        return None;
    }
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return None;
            }
        } else if !byte.is_ascii_hexdigit() {
            return None;
        }
    }
    let mut millis = 0_u64;
    for byte in bytes[..13].iter().filter(|byte| **byte != b'-') {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        millis = (millis << 4) | u64::from(digit);
    }
    Some(millis)
}

fn fork_parent(payload: &CodexPayload) -> Option<&str> {
    payload
        .forked_from_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .or_else(|| {
            payload
                .source
                .as_ref()?
                .get("subagent")?
                .get("thread_spawn")?
                .get("parent_thread_id")?
                .as_str()
                .filter(|id| !id.is_empty())
        })
}

fn parse_line(
    payload: &[u8],
    buffer: &mut Vec<u8>,
    state: &mut CodexParseState,
    counts: &mut CodexParseCounts,
    calls: &mut Vec<CodexCall>,
) {
    let Ok(text) = std::str::from_utf8(payload) else {
        counts.skipped_lines += 1;
        return;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let Some(entry) = parse_json_line::<CodexEntry>(trimmed, buffer) else {
        if unreadable_line_is_skipped(trimmed, buffer) {
            counts.skipped_lines += 1;
        }
        return;
    };
    let Some(payload) = entry.payload else {
        if entry.entry_type == "compacted" {
            if let Some(pending) = state.pending.as_mut() {
                pending.compacted = true;
            }
        }
        return;
    };
    if matches!(entry.entry_type.as_str(), "response_item" | "compacted")
        || (entry.entry_type == "event_msg"
            && payload.payload_type.as_deref() == Some("task_started"))
    {
        state.activity = true;
    }
    if entry.entry_type == "compacted" {
        if let Some(pending) = state.pending.as_mut() {
            pending.compacted = true;
        }
        return;
    }
    if ((entry.entry_type == "turn_context"
        || (entry.entry_type == "event_msg"
            && payload.payload_type.as_deref() == Some("task_started")))
        && payload.turn_id.is_some()
        && payload.turn_id != state.current_turn)
        || (entry.entry_type == "event_msg"
            && matches!(
                payload.payload_type.as_deref(),
                Some("task_complete" | "turn_aborted")
            )
            && (payload.turn_id.is_none()
                || state
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.call.turn_id == payload.turn_id)))
    {
        settle_pending(state, counts, calls);
    }
    let mut suppress_token_count = false;
    if entry.entry_type == "event_msg" && payload.payload_type.as_deref() == Some("token_count") {
        let accepted_last = payload.info.as_ref().and_then(|info| {
            let total = info.total_token_usage.as_ref()?;
            let last = info.last_token_usage.as_ref()?;
            if state
                .previous_total
                .as_ref()
                .is_some_and(|previous| previous == total || total.regresses_from(previous))
            {
                None
            } else {
                Some(last)
            }
        });
        if let Some(last) = accepted_last {
            if state.pending.as_ref().is_some_and(|pending| {
                pending.call.turn_id == state.current_turn && same_response(&pending.usage, last)
            }) {
                state.pending = None;
            } else if take_confirmation(&mut state.settled, &state.current_turn, last) {
                suppress_token_count = true;
            } else {
                remember_response(
                    &mut state.confirmations,
                    state.current_turn.clone(),
                    last.clone(),
                );
            }
        }
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.compacted)
        {
            settle_pending(state, counts, calls);
        }
    }
    match (entry.entry_type.as_str(), payload.payload_type.as_deref()) {
        ("token_usage_record", _) => {
            let Some(thread) = state.thread.as_ref() else {
                counts.records_unidentifiable += 1;
                return;
            };
            if payload
                .thread_id
                .as_ref()
                .is_some_and(|id| id != &thread.thread_id)
            {
                return;
            }
            let turn = payload
                .turn_id
                .clone()
                .or_else(|| state.current_turn.clone());
            if thread.is_fork
                && matches!((turn.as_deref().and_then(uuid_v7_millis), thread.created_ms), (Some(turn), Some(thread)) if turn < thread)
            {
                return;
            }
            let Some(usage) = payload.usage else {
                return;
            };
            if usage.fields().iter().all(Option::is_none) {
                return;
            }
            if take_confirmation(&mut state.confirmations, &turn, &usage) {
                return;
            }
            let request_id = payload
                .response_id
                .filter(|id| !id.is_empty())
                .map(|id| format!("response:{id}"))
                .or_else(|| entry.ordinal.map(|ordinal| format!("ordinal:{ordinal}")));
            let Some(request_id) = request_id else {
                counts.records_unidentifiable += 1;
                return;
            };
            let call = CodexCall {
                supplementary: true,
                source_line: state.lines,
                set_aside_reason: (thread.is_fork && turn.is_none())
                    .then(|| "fork replay cannot be identified without a turn id".into()),
                session_id: thread.session_id.clone(),
                thread_id: thread.thread_id.clone(),
                producer_ref: format!("{}:request:{request_id}", thread.thread_id),
                turn_id: turn,
                model: state.current_model.clone(),
                provider: thread.provider.clone(),
                project_root: state
                    .current_cwd
                    .as_deref()
                    .or(thread.cwd.as_deref())
                    .and_then(normalize_workspace_key),
                occurred_at_ms: parse_rfc3339_millis(entry.timestamp.as_deref()),
                usage: CodexCallUsage::from_usage(&usage),
                context_limit: None,
            };
            settle_pending(state, counts, calls);
            state.pending = Some(PendingRequest {
                call,
                usage,
                compacted: false,
            });
        }
        ("session_meta", _) => {
            // The first `session_meta` names the file's thread. Measured: one
            // per file, at ordinal 0. Upstream notes newer fork logs may embed
            // the parent's too; a later one never re-labels the thread.
            if state.thread.is_some() {
                return;
            }
            let thread_id = payload.id.clone().or_else(|| payload.session_id.clone());
            let session_id = payload.session_id.clone().or_else(|| payload.id.clone());
            if let (Some(thread_id), Some(session_id)) = (thread_id, session_id) {
                state.thread = Some(ThreadInfo {
                    created_ms: uuid_v7_millis(&thread_id),
                    thread_id,
                    session_id,
                    provider: payload.model_provider.clone(),
                    cwd: payload.cwd.clone(),
                    is_fork: fork_parent(&payload).is_some(),
                });
            }
        }
        ("turn_context", _) => {
            // ASTROCYTE DIVERGENCE: upstream clears the turn id on a
            // `turn_context` without one. Older Codex wrote a `turn_context`
            // without an id for every request of a turn (117 measured), each
            // after the turn's `task_started` announced its id, so the id is
            // kept until another turn announces itself.
            if payload.turn_id.is_some() {
                state.current_turn = payload.turn_id;
            }
            state.current_model = payload.model;
            state.current_cwd = payload.cwd;
        }
        ("event_msg", Some("task_started")) => {
            state.current_turn = payload.turn_id;
        }
        ("event_msg", Some("token_count")) => {
            counts.token_count_records += 1;
            let Some(info) = payload.info else {
                counts.token_counts_without_info += 1;
                return;
            };
            let context_limit = info.model_context_window;
            let (Some(total), Some(last)) = (info.total_token_usage, info.last_token_usage) else {
                counts.records_without_usage_pair += 1;
                return;
            };
            if let Some(previous) = &state.previous_total {
                if *previous == total {
                    counts.repeated_totals += 1;
                    return;
                }
                if total.regresses_from(previous) {
                    counts.regressed_totals += 1;
                    return;
                }
                if !total.advances_by(previous, &last) {
                    counts.last_disagrees_with_total += 1;
                }
            }
            state.previous_total = Some(total);
            counts.usage_records += 1;
            // Counted before the replay check so a fork's copy of a parent
            // response gets the same position in the turn as the original.
            let position = state.current_turn.as_ref().map(|turn| {
                let position = state.responses_in_turn.entry(turn.clone()).or_default();
                *position += 1;
                *position
            });
            if suppress_token_count {
                return;
            }
            let Some(thread) = &state.thread else {
                counts.records_unidentifiable += 1;
                return;
            };
            let replayed_turn = thread.is_fork
                && match (
                    state.current_turn.as_deref().and_then(uuid_v7_millis),
                    thread.created_ms,
                ) {
                    // A parent turn replayed into the fork was minted before
                    // the fork's own thread; the fork's turns at or after it.
                    (Some(turn_ms), Some(thread_ms)) => turn_ms < thread_ms,
                    // A copied turn keeps the same turn id and response position,
                    // hence the same producer_ref, even when its timestamp cannot
                    // be compared with the fork's. Without a turn id the source
                    // retains evidence instead of ingesting a child-local identity.
                    _ => false,
                };
            if replayed_turn {
                counts.fork_replays_skipped += 1;
                return;
            }
            let producer_ref = match (&state.current_turn, position, entry.ordinal) {
                (Some(turn), Some(position), _) => format!("{turn}:{position}"),
                (None, _, Some(ordinal)) => format!("{}:ordinal:{ordinal}", thread.thread_id),
                _ => {
                    counts.records_unidentifiable += 1;
                    return;
                }
            };
            let usage = CodexCallUsage::from_usage(&last);
            if usage.input == CodexDerived::Conflicted {
                counts.input_conflicts += 1;
            }
            if usage.output == CodexDerived::Conflicted {
                counts.output_conflicts += 1;
            }
            counts.underivable_roles += [usage.input, usage.output]
                .into_iter()
                .filter(|role| *role == CodexDerived::Underivable)
                .count() as u64;
            calls.push(CodexCall {
                supplementary: false,
                source_line: state.lines,
                set_aside_reason: (thread.is_fork && state.current_turn.is_none())
                    .then(|| "fork replay cannot be identified without a turn id".to_string()),
                session_id: thread.session_id.clone(),
                thread_id: thread.thread_id.clone(),
                producer_ref,
                turn_id: state.current_turn.clone(),
                model: state.current_model.clone(),
                provider: thread.provider.clone(),
                project_root: state
                    .current_cwd
                    .as_deref()
                    .or(thread.cwd.as_deref())
                    .and_then(normalize_workspace_key),
                occurred_at_ms: parse_rfc3339_millis(entry.timestamp.as_deref()),
                usage,
                context_limit,
            });
        }
        _ => {}
    }
}

/// Where a previous [`parse_codex_file_resumable`] stopped.
#[derive(Debug, Clone)]
pub struct CodexResumePoint(JsonlResumePoint<CodexParseState>);

impl CodexResumePoint {
    /// Bytes of the file already consumed; always just past a newline.
    pub fn offset(&self) -> u64 {
        self.0.offset()
    }
}

/// Result of one [`parse_codex_file_resumable`] call.
#[derive(Debug, Default)]
pub struct CodexResumableParse {
    pub io_error: Option<String>,
    pub set_aside: Vec<(u64, String)>,
    /// Calls read by this call, in file order.
    pub calls: Vec<CodexCall>,
    /// True when the previous point's prefix verified and parsing started at
    /// its offset; false for a full read from the start.
    pub resumed: bool,
    /// Where the next call may resume, or `None` when the file could not be
    /// read, so the next call reads it in full.
    pub resume: Option<CodexResumePoint>,
    /// Bytes fed to SHA-256 by this call. A steady-state resume is
    /// O(window + appended), never O(file): the deterministic cost counter.
    pub hashed_bytes: u64,
    /// Decoded archive bytes, including records not used for accounting.
    pub decompressed_bytes: u64,
    pub counts: CodexParseCounts,
}

/// Parses a Codex rollout, continuing from `previous` when the bytes it
/// consumed are unchanged.
///
/// The resume check is the shared one in `jsonl_resume.rs`: a shrunk file or
/// a changed inode forces a full re-read, then `check` re-hashes either the
/// trailing window or the whole consumed prefix. A full re-read re-emits
/// every call, which the ledger absorbs as exact redeliveries because each
/// call's identity and content come from the file alone.
///
/// Only newline-terminated plain-file lines are consumed; an unterminated
/// tail waits for the next read. Request usage stays pending until confirmed
/// by token counts or settled by compaction, a terminal event or a later request
/// or turn. An unresolved live EOF is not a settlement signal. Archives require
/// complete decoding before any calls are returned.
///
/// Native call identity is `<turn_id>:<n>`, the n-th response of the turn in
/// file order, not the record's `ordinal` or byte offset. UUID-v7 turn ids can
/// identify a fork's parent replay. Older synthetic turn ids remain scoped by
/// the session lineage in the ledger; sharing a positional reference alone does
/// not establish that two calls are copies. Supplementary request keys use a
/// separate namespace and never consume native response positions.
pub fn parse_codex_file_resumable(
    path: &Path,
    previous: Option<CodexResumePoint>,
    check: CodexPrefixCheck,
) -> CodexResumableParse {
    if is_codex_archive(path) {
        return archive::parse_archive(path);
    }
    let mut counts = CodexParseCounts::default();
    let mut set_aside = Vec::new();
    let mut calls = Vec::new();
    let mut buffer = Vec::with_capacity(4096);
    let read = super::jsonl_resume::try_read_appended_lines(
        path,
        previous.map(|point| point.0),
        check,
        CodexParseState::default,
        |state, payload| {
            state.lines += 1;
            counts.lines += 1;
            let skipped = counts.skipped_lines;
            parse_line(payload, &mut buffer, state, &mut counts, &mut calls);
            if counts.skipped_lines > skipped {
                set_aside.push((state.lines, "Codex record could not be decoded".into()));
            }
        },
    );
    let read = match read {
        Ok(read) => read,
        Err(error) => {
            return CodexResumableParse {
                io_error: Some(error.to_string()),
                ..CodexResumableParse::default()
            }
        }
    };

    CodexResumableParse {
        io_error: None,
        set_aside,
        calls,
        resumed: read.resumed,
        resume: read
            .cursor
            .map(|cursor| CodexResumePoint(cursor.with_state(read.state))),
        hashed_bytes: read.hashed_bytes,
        decompressed_bytes: 0,
        counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocating_uuid_reference(id: &str) -> Option<u64> {
        let parts: Vec<&str> = id.split('-').collect();
        let [first, second, third, fourth, fifth] = parts.as_slice() else {
            return None;
        };
        if ![first, second, third, fourth, fifth]
            .iter()
            .map(|part| part.len())
            .eq([8, 4, 4, 4, 12])
            || !parts
                .iter()
                .all(|part| part.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || !third.starts_with('7')
        {
            return None;
        }
        u64::from_str_radix(&format!("{first}{second}"), 16).ok()
    }

    #[test]
    fn uuid_v7_byte_decode_preserves_every_spelling_check() {
        let id = "019f48b9-8c8f-7351-8841-9387ce1e709f";
        for spelling in [
            id.to_string(),
            id.to_uppercase(),
            format!(" {id}"),
            format!("{id} "),
            id.replace('-', ""),
            id.replace('-', "--"),
        ] {
            assert_eq!(
                uuid_v7_millis(&spelling),
                allocating_uuid_reference(&spelling),
                "{spelling}"
            );
        }
        for index in 0..id.len() {
            for replacement in *b"07AfG- " {
                let mut bytes = id.as_bytes().to_vec();
                bytes[index] = replacement;
                let spelling = String::from_utf8(bytes).unwrap();
                assert_eq!(
                    uuid_v7_millis(&spelling),
                    allocating_uuid_reference(&spelling),
                    "{spelling}"
                );
            }
        }
    }

    #[test]
    #[ignore = "UUID decoder wall-time measurement"]
    fn uuid_v7_decode_cost_before_and_after() {
        let id = "019f48b9-8c8f-7351-8841-9387ce1e709f";
        for (name, decode) in [
            (
                "allocating reference",
                allocating_uuid_reference as fn(&str) -> Option<u64>,
            ),
            ("byte decoder", uuid_v7_millis),
        ] {
            let started = std::time::Instant::now();
            for _ in 0..100_000 {
                assert_eq!(
                    std::hint::black_box(decode(std::hint::black_box(id))),
                    Some(0x019f_48b9_8c8f)
                );
            }
            eprintln!("{name}, 100,000 UUIDs: {:?}", started.elapsed());
        }
    }
    use std::io::Write;

    const THREAD: &str = "019f48b9-8c8f-7351-8841-9387ce1e709f";
    const TURN: &str = "019f48b9-8d15-7612-a4ec-17a0ce833015";

    fn session_meta(id: &str, session_id: &str, fork_parent: Option<&str>) -> String {
        let fork = fork_parent.map_or(String::new(), |parent| {
            format!(
                r#","forked_from_id":"{parent}","thread_source":"subagent","source":{{"subagent":{{"thread_spawn":{{"parent_thread_id":"{parent}","depth":1}}}}}}"#
            )
        });
        format!(
            r#"{{"timestamp":"2026-07-09T21:12:27.672Z","ordinal":0,"type":"session_meta","payload":{{"session_id":"{session_id}","id":"{id}","cwd":"/work/a.b-c","originator":"Codex Desktop","cli_version":"0.144.0","model_provider":"openai"{fork}}}}}"#
        ) + "\n"
    }

    fn task_started(ordinal: u64, turn: &str) -> String {
        format!(
            r#"{{"timestamp":"2026-07-09T21:12:27.672Z","ordinal":{ordinal},"type":"event_msg","payload":{{"type":"task_started","turn_id":"{turn}","started_at":1788103735,"model_context_window":258400}}}}"#
        ) + "\n"
    }

    fn turn_context(ordinal: u64, turn: &str, model: &str, cwd: &str) -> String {
        format!(
            r#"{{"timestamp":"2026-07-09T21:12:28.501Z","ordinal":{ordinal},"type":"turn_context","payload":{{"turn_id":"{turn}","cwd":"{cwd}","approval_policy":"never","model":"{model}","personality":"pragmatic"}}}}"#
        ) + "\n"
    }

    /// `[input, cached, cache_write, output, reasoning]`; `total_tokens` is
    /// input + output, Codex's measured convention.
    fn usage_block(counters: [u64; 5]) -> String {
        let [input, cached, write, output, reasoning] = counters;
        format!(
            r#"{{"input_tokens":{input},"cached_input_tokens":{cached},"cache_write_input_tokens":{write},"output_tokens":{output},"reasoning_output_tokens":{reasoning},"total_tokens":{}}}"#,
            input + output
        )
    }

    fn token_count_raw(ordinal: u64, total: &str, last: &str) -> String {
        format!(
            r#"{{"timestamp":"2026-07-09T21:12:37.335Z","ordinal":{ordinal},"type":"event_msg","payload":{{"type":"token_count","info":{{"total_token_usage":{total},"last_token_usage":{last},"model_context_window":258400}},"rate_limits":{{"limit_id":"codex","plan_type":"pro"}}}}}}"#
        ) + "\n"
    }

    fn token_count(ordinal: u64, total: [u64; 5], last: [u64; 5]) -> String {
        token_count_raw(ordinal, &usage_block(total), &usage_block(last))
    }

    fn file_with(contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout.jsonl");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn append(path: &Path, text: &str) {
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn parse(path: &Path, previous: Option<CodexResumePoint>) -> CodexResumableParse {
        parse_codex_file_resumable(path, previous, CodexPrefixCheck::Window)
    }

    fn head() -> String {
        [
            session_meta(THREAD, THREAD, None),
            task_started(1, TURN),
            turn_context(2, TURN, "gpt-5.6-sol", "/work/a.b-c"),
        ]
        .concat()
    }

    /// Codex nests cached input inside input and reasoning inside output;
    /// the ledger's roles are disjoint, so both are subtracted, and the
    /// nested counts are stored verbatim beside them.
    #[test]
    fn disjoint_roles_subtract_cached_input_and_reasoning_output() {
        let call = [100, 60, 0, 30, 12];
        let (_dir, path) = file_with(&(head() + &token_count(3, call, call)));
        let parse = parse(&path, None);
        assert_eq!(parse.calls.len(), 1);
        let usage = parse.calls[0].usage;
        assert_eq!(usage.input, CodexDerived::Measured(40));
        assert_eq!(usage.cache_read, Some(60));
        assert_eq!(usage.cache_write, Some(0));
        assert_eq!(usage.output, CodexDerived::Measured(18));
        assert_eq!(usage.reasoning, Some(12));
        let call = &parse.calls[0];
        assert_eq!(call.producer_ref, format!("{TURN}:1"));
        assert_eq!(call.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(call.provider.as_deref(), Some("openai"));
        assert_eq!(call.project_root.as_deref(), Some("/work/a.b-c"));
        assert_eq!(call.session_id, THREAD);
        assert!(call.occurred_at_ms.is_some());
    }

    /// Codex re-sends the running total (rate-limit updates) without a new
    /// response. An unchanged total is a repeat and counts once, even when
    /// the repeat's per-response block differs (measured: one repeat carries
    /// an all-zero block beside a non-zero `total_tokens`).
    #[test]
    fn a_repeated_token_count_with_unchanged_totals_is_counted_once() {
        let first = [100, 60, 0, 30, 12];
        let second = [50, 10, 0, 5, 0];
        let total = [150, 70, 0, 35, 12];
        let (_dir, path) = file_with(
            &[
                head(),
                token_count(3, first, first),
                token_count(4, first, first),
                token_count(5, total, second),
                token_count_raw(
                    6,
                    &usage_block(total),
                    r#"{"input_tokens":0,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0,"total_tokens":185}"#,
                ),
                r#"{"timestamp":"2026-07-09T21:12:37.335Z","ordinal":7,"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":null}}"#.to_string() + "\n",
            ]
            .concat(),
        );
        let parse = parse(&path, None);
        assert_eq!(parse.counts.token_count_records, 5);
        assert_eq!(parse.counts.repeated_totals, 2);
        assert_eq!(parse.counts.token_counts_without_info, 1);
        assert_eq!(parse.counts.usage_records, 2);
        assert_eq!(parse.counts.last_disagrees_with_total, 0);
        let refs: Vec<_> = parse
            .calls
            .iter()
            .map(|call| call.producer_ref.clone())
            .collect();
        assert_eq!(refs, vec![format!("{TURN}:1"), format!("{TURN}:2")]);
        assert_eq!(parse.calls[1].usage.input, CodexDerived::Measured(40));
    }

    /// An absent counter is an omitted measurement, never a zero. Without
    /// `reasoning_output_tokens` the reasoning role is absent and visible
    /// output cannot be derived (reasoning is inside output).
    #[test]
    fn a_missing_reasoning_counter_is_omitted_not_zero() {
        let block = r#"{"input_tokens":100,"cached_input_tokens":60,"cache_write_input_tokens":0,"output_tokens":30,"total_tokens":130}"#;
        let (_dir, path) = file_with(&(head() + &token_count_raw(3, block, block)));
        let parse = parse(&path, None);
        let usage = parse.calls[0].usage;
        assert_eq!(usage.reasoning, None);
        assert_eq!(usage.output, CodexDerived::Underivable);
        assert_eq!(usage.input, CodexDerived::Measured(40));
        assert_eq!(parse.counts.underivable_roles, 1);
    }

    /// A subtraction that would go negative is a contradiction of the nested
    /// convention: the role is conflicted (no quantity, never clamped), the
    /// call still lands with its other roles, and nothing panics.
    #[test]
    fn a_subtraction_conflict_marks_the_role_and_keeps_the_call() {
        let call = [10, 60, 0, 5, 9];
        let (_dir, path) = file_with(&(head() + &token_count(3, call, call)));
        let parse = parse(&path, None);
        assert_eq!(parse.calls.len(), 1);
        let usage = parse.calls[0].usage;
        assert_eq!(usage.input, CodexDerived::Conflicted);
        assert_eq!(usage.output, CodexDerived::Conflicted);
        assert_eq!(usage.cache_read, Some(60));
        assert_eq!(usage.reasoning, Some(9));
        assert_eq!(
            (parse.counts.input_conflicts, parse.counts.output_conflicts),
            (1, 1)
        );
    }

    /// No measured record has a non-zero cache write, so whether it sits
    /// inside `input_tokens` is unknown and uncached input is not derived.
    #[test]
    fn a_non_zero_cache_write_leaves_input_underivable() {
        let call = [100, 60, 20, 30, 12];
        let (_dir, path) = file_with(&(head() + &token_count(3, call, call)));
        let usage = parse(&path, None).calls[0].usage;
        assert_eq!(usage.input, CodexDerived::Underivable);
        assert_eq!(usage.cache_write, Some(20));
    }

    /// A subagent fork's file starts by replaying its parent's turn,
    /// including the parent's responses, then runs its own turn with running
    /// totals continuing from the replay. The replay is skipped; the fork's
    /// own responses land. The replayed response has the same identity as
    /// the parent file's original, so an escaped replay could never be a
    /// second call.
    #[test]
    fn a_forked_thread_skips_its_parents_replayed_responses() {
        let child = "019f48b9-cb62-7b40-9bb3-db9ad8062d69";
        let child_turn = "019f48b9-cb87-7b90-9834-5937afde433b";
        let parent_call = [18000, 9000, 0, 500, 300];
        let parent_second = [18100, 17000, 0, 100, 0];
        let parent_total = [36100, 26000, 0, 600, 300];
        let own = [19000, 18000, 0, 200, 50];
        let own_total = [55100, 44000, 0, 800, 350];
        let (_parent_dir, parent_path) = file_with(
            &[
                head(),
                token_count(3, parent_call, parent_call),
                token_count(4, parent_total, parent_second),
            ]
            .concat(),
        );
        let (_child_dir, child_path) = file_with(
            &[
                session_meta(child, THREAD, Some(THREAD)),
                task_started(1, TURN),
                turn_context(2, TURN, "gpt-5.6-sol", "/work/a.b-c"),
                token_count(12, parent_call, parent_call),
                token_count(14, parent_total, parent_second),
                task_started(16, child_turn),
                turn_context(17, child_turn, "gpt-5.6-terra", "/work/a.b-c"),
                token_count(26, own_total, own),
            ]
            .concat(),
        );
        let parent = parse(&parent_path, None);
        let fork = parse(&child_path, None);
        assert_eq!(fork.counts.fork_replays_skipped, 2);
        assert_eq!(fork.counts.last_disagrees_with_total, 0);
        assert_eq!(fork.calls.len(), 1);
        let call = &fork.calls[0];
        assert_eq!(call.producer_ref, format!("{child_turn}:1"));
        assert_eq!(call.thread_id, child);
        assert_eq!(call.session_id, THREAD, "a fork shares its conversation");
        assert_eq!(call.model.as_deref(), Some("gpt-5.6-terra"));
        assert_eq!(call.usage.input, CodexDerived::Measured(1000));

        let parent_refs: Vec<_> = parent
            .calls
            .iter()
            .map(|c| c.producer_ref.clone())
            .collect();
        assert_eq!(parent_refs, vec![format!("{TURN}:1"), format!("{TURN}:2")]);
    }

    /// Resuming a Codex session appends to the same rollout (measured: one
    /// file resumed a month later in another directory). The appended turn
    /// continues the running total, takes its own cwd, and a resumed read
    /// yields exactly what a full read of the whole file yields.
    #[test]
    fn a_resumed_session_appends_and_resumed_reads_equal_a_full_read() {
        let first = [100, 60, 0, 30, 12];
        let later_turn = "019f0f1b-d703-7231-8c04-91bfd128749d";
        let (_dir, path) = file_with(&(head() + &token_count(3, first, first)));
        let initial = parse(&path, None);
        let appended = [
            task_started(4, later_turn),
            turn_context(5, later_turn, "gpt-5.6-sol", "/elsewhere"),
            token_count(6, [150, 70, 0, 35, 12], [50, 10, 0, 5, 0]),
        ]
        .concat();
        append(&path, &appended);
        let resumed = parse(&path, initial.resume);
        assert!(resumed.resumed);
        assert_eq!(resumed.counts.lines, 3);
        assert_eq!(resumed.calls.len(), 1);
        assert_eq!(resumed.calls[0].project_root.as_deref(), Some("/elsewhere"));
        assert_eq!(resumed.calls[0].producer_ref, format!("{later_turn}:1"));

        let full = parse(&path, None);
        assert_eq!(
            full.calls,
            [initial.calls, resumed.calls].concat(),
            "a resumed read yields the same calls as one full read"
        );
    }

    /// A corrupt line, or a needed record whose counters do not decode, is a
    /// counted skipped line and reading continues; a record of another kind
    /// with an unexpected shape is not. An unterminated tail is not consumed.
    #[test]
    fn unreadable_lines_are_counted_and_a_partial_tail_is_not() {
        let call = [100, 60, 0, 30, 12];
        let negative = r#"{"input_tokens":-1,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0,"total_tokens":0}"#;
        let tail = token_count(9, [200, 120, 0, 60, 24], call);
        let (_dir, path) = file_with(
            &[
                head(),
                "{\"type\":\"event_msg\",not json\n".to_string(),
                token_count_raw(4, negative, negative),
                r#"{"timestamp":"x","ordinal":5,"type":"response_item","payload":"a plain string"}"#
                    .to_string() + "\n",
                "\n".to_string(),
                token_count(6, call, call),
                tail[..tail.len() / 2].to_string(),
            ]
            .concat(),
        );
        let first = parse(&path, None);
        assert_eq!(first.counts.skipped_lines, 2);
        assert_eq!(
            first.calls.len(),
            1,
            "the line after the corrupt ones reads"
        );
        append(&path, &tail[tail.len() / 2..]);
        let second = parse(&path, first.resume);
        assert!(second.resumed);
        assert_eq!((second.counts.lines, second.calls.len()), (1, 1));
    }

    /// No timestamp: undated, never the file's mtime.
    #[test]
    fn a_record_without_a_timestamp_is_undated() {
        let call = [100, 60, 0, 30, 12];
        let line =
            token_count(3, call, call).replace(r#""timestamp":"2026-07-09T21:12:37.335Z","#, "");
        let (_dir, path) = file_with(&(head() + &line));
        assert_eq!(parse(&path, None).calls[0].occurred_at_ms, None);
    }

    /// Usage before any `session_meta` has no thread to belong to: counted,
    /// not ingested under a made-up identity.
    #[test]
    fn usage_without_a_session_meta_is_counted_not_emitted() {
        let call = [100, 60, 0, 30, 12];
        let (_dir, path) = file_with(&(task_started(1, TURN) + &token_count(3, call, call)));
        let parse = parse(&path, None);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.records_unidentifiable, 1);
        assert_eq!(parse.counts.usage_not_ingested(), 1);
    }

    #[test]
    fn a_rewritten_prefix_rereads_from_scratch() {
        let call = [100, 60, 0, 30, 12];
        let (_dir, path) = file_with(&(head() + &token_count(3, call, call)));
        let first = parse(&path, None);
        let other = [7, 1, 0, 3, 1];
        std::fs::write(&path, head() + &token_count(3, other, other)).unwrap();
        let second = parse(&path, first.resume);
        assert!(!second.resumed);
        assert_eq!(second.calls[0].usage.input, CodexDerived::Measured(6));
    }

    #[test]
    fn uuid_v7_millis_reads_only_version_7_ids() {
        assert_eq!(
            uuid_v7_millis("019f48b9-8c8f-7351-8841-9387ce1e709f"),
            Some(0x019f_48b9_8c8f)
        );
        assert_eq!(uuid_v7_millis("0b1e2c3d-8c8f-4351-8841-9387ce1e709f"), None);
        assert_eq!(uuid_v7_millis("not-a-uuid"), None);
    }
}
