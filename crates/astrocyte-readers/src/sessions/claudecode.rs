// Copied from tokscale v4.17.0 (tag commit
// dcf8d3656bbcecf05d112c6f35f9be328e4f66b7),
// crates/tokscale-core/src/sessions/claudecode.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
//
// ASTROCYTE DIVERGENCE (whole file): upstream parses a whole transcript into
// `UnifiedMessage`s for reporting. Astrocyte ingests each API call once into
// a write-once ledger, so this copy keeps only the record shapes
// (`ClaudeEntry`, `ClaudeMessage`, the Anthropic usage block), the timestamp
// parse, the `<synthetic>` placeholder check and the per-field-max merge of
// streaming duplicates, and adds: a call aggregate that REFUSES a decreasing
// copy instead of absorbing it, the 5m/1h cache-write split, a per-record
// `cwd`, the three completion states, a resumable byte-offset read, and
// recognition of the zeroed copies Claude Code writes after a compaction.
// Dropped as not needed for ingest: headless `.json` transcripts, cc-mirror
// variants, tool-result token estimation, subagent display-name resolution,
// turn-start and duration bookkeeping, and model alias canonicalization.
// Divergences inside retained code are marked inline.

//! Claude Code session reader.
//!
//! Reads `~/.claude/projects/**/*.jsonl`. Subagent transcripts live one level
//! deeper (`<project>/<session>/subagents/agent-*.jsonl`), so callers must
//! recurse.
//!
//! Claude Code writes one JSONL record per content block of a response
//! (thinking, text, tool_use), and every one of those records carries the
//! usage of the WHOLE response. The records of one call share a `requestId`.
//! This reader folds them back into one [`ClaudeCodeCall`] per `requestId`.

use super::jsonl_resume::{JsonlResumePoint, PrefixCheck, PREFIX_WINDOW_BYTES};
use super::normalize_workspace_key;
use super::utils::{parse_json_line, parse_rfc3339_millis};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// How long a file must go without growing before a call that never got a
/// `stop_reason` is ingested as interrupted.
///
/// Derivation (measured 2026-09-23 over `~/.claude/projects`, 43 files, 1,469
/// calls): among the 1,457 calls that DID get a `stop_reason`, 1,024 have more
/// than one record, and the longest gap between two consecutive records of
/// one call is 56.2 s (main sessions; the subagent maximum is 23.1 s). A live
/// call therefore grows its file at least every minute or so. Ten minutes is
/// over ten times that longest gap, so a slow-but-live call is not mistaken
/// for an interrupted one; the 12 interrupted calls in that corpus had been
/// quiet for 11 hours to 11 days. Tightening this below the fixture's
/// straddling gap reddens
/// `a_call_whose_copies_straddle_a_gap_just_under_the_window_stays_live`.
pub const CLAUDE_CODE_QUIET_WINDOW_MS: i64 = 10 * 60 * 1000;

/// Bytes of the already-consumed prefix that a [`ClaudePrefixCheck::Window`]
/// resume re-hashes.
///
/// ASTROCYTE DIVERGENCE: tokscale has no resumable Claude Code parse. This
/// reader resumes through the shared append-only JSONL resume in
/// `jsonl_resume.rs`, which the Codex reader also uses. The Pi reader keeps
/// its own variant (see that module's documentation).
pub const CLAUDE_PREFIX_WINDOW_BYTES: u64 = PREFIX_WINDOW_BYTES;

/// How much of a file's already-consumed prefix to re-hash before trusting a
/// resume point: the shared [`PrefixCheck`].
pub type ClaudePrefixCheck = PrefixCheck;

/// Claude Code entry structure (from JSONL files).
///
/// ASTROCYTE DIVERGENCE: adds `cwd` (the project root is taken from each
/// usage-bearing record, never decoded from the project directory's name,
/// which collapses `/`, `-` and `.` into one character) and drops the
/// sidechain, agent and provider-hint fields upstream uses for display names.
#[derive(Debug, Deserialize)]
struct ClaudeEntry {
    #[serde(rename = "type")]
    entry_type: String,
    /// Set on `system` records; `compact_boundary` marks a `/compact` (or an
    /// automatic compaction), after which Claude Code replays kept turns.
    subtype: Option<String>,
    timestamp: Option<String>,
    message: Option<ClaudeMessage>,
    /// Request ID for deduplication.
    #[serde(rename = "requestId")]
    request_id: Option<String>,
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    cwd: Option<String>,
}

/// ASTROCYTE DIVERGENCE: adds `stop_reason`, which decides whether a call is
/// complete and is stored verbatim as the event's terminal status.
#[derive(Debug, Deserialize)]
struct ClaudeMessage {
    model: Option<String>,
    usage: Option<ClaudeUsage>,
    /// Message ID; one-to-one with `requestId` in the measured corpus.
    id: Option<String>,
    stop_reason: Option<String>,
}

/// The Anthropic `usage` block.
///
/// ASTROCYTE DIVERGENCE: upstream's `AnthropicUsage` uses `Option<i64>` and
/// clamps absent and negative values to zero. Here every counter is
/// `Option<u64>`: an absent counter stays absent instead of decoding as zero,
/// and a negative one fails the record's decode, which is counted as a
/// skipped line. Upstream also has no `cache_creation` split.
#[derive(Debug, Deserialize)]
struct ClaudeUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    cache_creation: Option<ClaudeCacheCreation>,
}

#[derive(Debug, Deserialize)]
struct ClaudeCacheCreation {
    ephemeral_5m_input_tokens: Option<u64>,
    ephemeral_1h_input_tokens: Option<u64>,
}

/// Only the record type, for telling an unreadable assistant record (a
/// skipped line) from a record of another type whose shape the typed decode
/// does not expect (not a usage record, so not skipped).
#[derive(Deserialize)]
struct EntryTypeProbe {
    #[serde(rename = "type")]
    entry_type: Option<String>,
}

/// Token counters of one call, as Claude Code reported them.
///
/// Cache writes are kept by lifetime because the two are priced differently.
/// When a record has the `cache_creation` split, only the split is used: the
/// flat `cache_creation_input_tokens` is not a reliable sum of it (measured
/// 2026-09-23: 8 of 2,671 records report a flat 0 beside a non-zero 1h
/// split). When a record has only the flat field, it lands in
/// `cache_write_unsplit` and is stored without a lifetime, because
/// attributing it to 5m or 1h would be a guess.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaudeCallUsage {
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write_5m: Option<u64>,
    pub cache_write_1h: Option<u64>,
    pub cache_write_unsplit: Option<u64>,
}

impl ClaudeCallUsage {
    fn from_usage(usage: &ClaudeUsage) -> Self {
        let (cache_write_5m, cache_write_1h, cache_write_unsplit) = match &usage.cache_creation {
            Some(split) => (
                split.ephemeral_5m_input_tokens,
                split.ephemeral_1h_input_tokens,
                None,
            ),
            None => (None, None, usage.cache_creation_input_tokens),
        };
        Self {
            input: usage.input_tokens,
            output: usage.output_tokens,
            cache_read: usage.cache_read_input_tokens,
            cache_write_5m,
            cache_write_1h,
            cache_write_unsplit,
        }
    }

    /// Per-field maximum of `self` and a LATER copy of the same call.
    ///
    /// ASTROCYTE DIVERGENCE: upstream's `merge_claude_duplicate` takes the
    /// per-field max silently. Measured copies only ever grow (88 of 1,469
    /// calls have differing copies; every step is non-decreasing and only
    /// `output_tokens` moves), so a later copy with a LOWER value, or with a
    /// counter that disappeared, means Claude Code changed how it writes
    /// usage. That call is refused (`Err`) rather than stored at a maximum
    /// nobody can vouch for. `Ok(true)` means some field grew.
    fn merge_later(&mut self, later: &Self) -> Result<bool, ()> {
        fn field(current: &mut Option<u64>, later: Option<u64>) -> Result<bool, ()> {
            match (*current, later) {
                (Some(held), Some(next)) if next < held => Err(()),
                (Some(_), None) => Err(()),
                (Some(held), Some(next)) => {
                    *current = Some(next);
                    Ok(next != held)
                }
                (None, Some(next)) => {
                    *current = Some(next);
                    Ok(true)
                }
                (None, None) => Ok(false),
            }
        }
        let mut grew = false;
        grew |= field(&mut self.input, later.input)?;
        grew |= field(&mut self.output, later.output)?;
        grew |= field(&mut self.cache_read, later.cache_read)?;
        grew |= field(&mut self.cache_write_5m, later.cache_write_5m)?;
        grew |= field(&mut self.cache_write_1h, later.cache_write_1h)?;
        grew |= field(&mut self.cache_write_unsplit, later.cache_write_unsplit)?;
        Ok(grew)
    }
}

/// One API call, folded from every record Claude Code wrote for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeCodeCall {
    /// One-based line of the first record folded into this call.
    pub source_line: u64,
    /// `requestId`: unique per call across the measured corpus (1,469
    /// distinct values over 2,671 records, none in two files, one-to-one with
    /// `message.id`), and the same value a gateway stores as its vendor
    /// request id.
    pub request_id: String,
    pub message_id: Option<String>,
    pub session_id: Option<String>,
    /// The record's own `cwd`, normalized.
    pub project_root: Option<String>,
    pub model: Option<String>,
    /// Earliest record timestamp of the call. Absent when no record of the
    /// call carries a parseable timestamp: never a file mtime.
    pub occurred_at_ms: Option<i64>,
    /// Claude Code's `stop_reason` verbatim, or `None` for a call that never
    /// got one and was ingested after its file went quiet.
    pub stop_reason: Option<String>,
    pub usage: ClaudeCallUsage,
    /// How many records were folded into this call.
    pub records: u32,
}

/// Where one call stands in a file's parse.
#[derive(Debug, Clone)]
struct CallAggregate {
    call: ClaudeCodeCall,
    /// File order of the call's first record, so emission order is stable.
    first_seen: u64,
    /// Emitted at least once.
    emitted: bool,
}

#[cfg(not(test))]
type CallHistory = HashMap<String, CallAggregate>;
#[cfg(test)]
use tests::CallHistory;

/// Call aggregates for one file, carried across resumed reads.
///
/// Every call seen in the file is kept, emitted or not, so a late copy of an
/// already-ingested call is compared against what was stored instead of
/// being ingested as a new, partial call. A drain clones the committed state
/// and only replaces it after commit, so failed reads and writes retain it.
#[derive(Debug, Clone, Default)]
struct ClaudeParseState {
    lines: u64,
    calls: CallHistory,
    /// Keys of calls with new or changed usage since last handed to ingest.
    /// Every aggregate stays in `calls` so later copies of a request can still
    /// be compared against the usage already read for that request.
    dirty: HashSet<String>,
    pending: usize,
    refused: HashSet<String>,
    next_ordinal: u64,
    /// Timestamp of the latest `compact_boundary` record read in this file.
    last_compact_boundary_ms: Option<i64>,
    /// Request ids seen in this file only as compaction replays (see
    /// [`UsageRecord::is_compaction_replay`]). They are never emitted.
    replay_only: HashSet<String>,
}

impl ClaudeParseState {
    fn observe_compact_boundary(&mut self, timestamp_ms: Option<i64>) {
        if let Some(timestamp) = timestamp_ms {
            self.last_compact_boundary_ms = Some(
                self.last_compact_boundary_ms
                    .map_or(timestamp, |held| held.max(timestamp)),
            );
        }
    }

    fn observe(&mut self, record: UsageRecord, counts: &mut ClaudeParseCounts) {
        if self.refused.contains(&record.request_id) {
            return;
        }
        if record.is_compaction_replay(self.last_compact_boundary_ms) {
            match self.calls.get(&record.request_id) {
                // The original was read (in this parse or an earlier resumed
                // one: the aggregates survive resumes). The copy is not a
                // later snapshot, so it is neither merged nor refused.
                Some(aggregate) if !identity_disagrees(&aggregate.call, &record) => {
                    counts.replayed_records += 1;
                    return;
                }
                // A copy that names a different message, session, cwd, model
                // or stop reason than its original is a real disagreement:
                // it falls through to the merge, which refuses it.
                Some(_) => {}
                // No original in this file. The copy's counters are zeroed,
                // so emitting it would store a call at a usage Claude Code
                // never reported; it is counted and never emitted.
                None => {
                    counts.replayed_records += 1;
                    if self.replay_only.insert(record.request_id) {
                        counts.replay_only_calls += 1;
                    }
                    return;
                }
            }
        }
        let Some(aggregate) = self.calls.get_mut(&record.request_id) else {
            // A genuine record for a request id seen so far only in replays
            // starts the call as usual.
            self.replay_only.remove(&record.request_id);
            let ordinal = self.next_ordinal;
            self.next_ordinal += 1;
            self.dirty.insert(record.request_id.clone());
            self.pending += 1;
            self.calls.insert(
                record.request_id.clone(),
                CallAggregate {
                    call: ClaudeCodeCall {
                        source_line: self.lines,
                        request_id: record.request_id,
                        message_id: record.message_id,
                        session_id: record.session_id,
                        project_root: record.project_root,
                        model: record.model,
                        occurred_at_ms: record.timestamp_ms,
                        stop_reason: record.stop_reason,
                        usage: record.usage,
                        records: 1,
                    },
                    first_seen: ordinal,
                    emitted: false,
                },
            );
            return;
        };
        match merge_record(&mut aggregate.call, &record) {
            Ok(changed) => {
                aggregate.call.records += 1;
                if changed {
                    self.dirty.insert(record.request_id);
                }
            }
            Err(()) => {
                counts.calls_refused += 1;
                if aggregate.emitted {
                    counts.calls_refused_after_emission += 1;
                } else {
                    self.pending -= 1;
                }
                self.dirty.remove(&record.request_id);
                self.calls.remove(&record.request_id);
                self.refused.insert(record.request_id);
            }
        }
    }

    /// Calls to hand to ingest now, in file order; marks them emitted.
    fn take_emissions(&mut self, file_quiet: bool) -> Vec<ClaudeCodeCall> {
        let mut ready: Vec<String> = self
            .dirty
            .iter()
            .filter(|key| {
                let aggregate = &self.calls[*key];
                // A complete call is ready immediately. An incomplete call
                // waits for quiet unless it was emitted before: later changes
                // must reach the ledger's contradiction check.
                aggregate.call.stop_reason.is_some() || aggregate.emitted || file_quiet
            })
            .cloned()
            .collect();
        ready.sort_by_key(|key| self.calls[key].first_seen);
        ready
            .into_iter()
            .map(|key| {
                let aggregate = self.calls.get_mut(&key).expect("dirty call is retained");
                if !aggregate.emitted {
                    self.pending -= 1;
                }
                aggregate.emitted = true;
                self.dirty.remove(&key);
                aggregate.call.clone()
            })
            .collect()
    }

    fn pending_calls(&self) -> usize {
        self.pending
    }
}

/// True when a record names a different call than the one held: another
/// message id, session, cwd or model, or a different stop reason where both
/// have one.
///
/// Measured 2026-09-23: 0 of 1,469 calls disagree across their records on
/// model, session, cwd, message id or stop_reason. A disagreement is not a
/// streaming snapshot, so it is refused rather than resolved by a guess.
fn identity_disagrees(call: &ClaudeCodeCall, record: &UsageRecord) -> bool {
    call.message_id != record.message_id
        || call.session_id != record.session_id
        || call.project_root != record.project_root
        || call.model != record.model
        || matches!(
            (&call.stop_reason, &record.stop_reason),
            (Some(held), Some(next)) if held != next
        )
}

/// Fold a LATER record into its call. `Err` refuses the call: a counter
/// decreased or disappeared, or the records disagree on what call this is.
/// `Ok(true)` when anything the ledger stores changed.
fn merge_record(call: &mut ClaudeCodeCall, record: &UsageRecord) -> Result<bool, ()> {
    if identity_disagrees(call, record) {
        return Err(());
    }
    let mut changed = call.usage.merge_later(&record.usage)?;
    if let (None, Some(next)) = (&call.stop_reason, &record.stop_reason) {
        call.stop_reason = Some(next.clone());
        changed = true;
    }
    if let Some(timestamp) = record.timestamp_ms {
        if call.occurred_at_ms.is_none_or(|held| timestamp < held) {
            call.occurred_at_ms = Some(timestamp);
            changed = true;
        }
    }
    Ok(changed)
}

/// One usage-bearing assistant record.
struct UsageRecord {
    request_id: String,
    message_id: Option<String>,
    session_id: Option<String>,
    project_root: Option<String>,
    model: Option<String>,
    timestamp_ms: Option<i64>,
    stop_reason: Option<String>,
    usage: ClaudeCallUsage,
    /// The four flat counters (`input_tokens`, `output_tokens`,
    /// `cache_read_input_tokens`, `cache_creation_input_tokens`) are all
    /// present and zero. The `cache_creation` split is not part of this: a
    /// compaction replay keeps its original split.
    flat_counters_zero: bool,
}

impl UsageRecord {
    /// A copy Claude Code wrote while compacting the transcript, not an API
    /// call.
    ///
    /// After a `compact_boundary` record, Claude Code rewrites the turns it
    /// keeps. Each copied assistant record carries the original `requestId`,
    /// message id, content, model, stop_reason and timestamp, a new `uuid`,
    /// the original `cache_creation` split, and ZERO in all four flat
    /// counters (the true counters survive only in `usage.iterations`, which
    /// this reader does not read). Read as a later snapshot, it would look
    /// like every counter decreased and the call would be refused.
    ///
    /// The test is: flat counters all zero, and a timestamp strictly before
    /// the latest `compact_boundary` already read, which a record written
    /// after the boundary can only have if it is a copy of an older turn.
    /// The timestamp keeps a genuine zero-usage call made after a compaction
    /// from being mistaken for a replay.
    ///
    /// Measured 2026-09-27 over `~/.claude/projects` (77 files, 12
    /// `compact_boundary` records, 1,642 request ids): 15 usage records have
    /// all four flat counters zero. All 15 follow a boundary and are
    /// timestamped before it; none precedes any boundary. 7 of them repeat a
    /// call whose earlier record in the same file had non-zero counters; no
    /// record outside those 15 is all zero after a non-zero record of its
    /// call, so a genuine later snapshot never looks like this. The other 8 (4 calls, all in one file) are copies of calls whose
    /// original records were never written to the file. Of the 1,234
    /// non-zero records that follow a boundary, none is timestamped before
    /// it.
    fn is_compaction_replay(&self, last_compact_boundary_ms: Option<i64>) -> bool {
        self.flat_counters_zero
            && matches!(
                (self.timestamp_ms, last_compact_boundary_ms),
                (Some(record), Some(boundary)) if record < boundary
            )
    }
}

fn is_claude_synthetic_placeholder_model(model: &str) -> bool {
    model.trim().eq_ignore_ascii_case("<synthetic>")
}

/// Counts for one parse call. Every count covers only the lines this call
/// consumed, so a resumed parse reports the appended lines alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClaudeParseCounts {
    /// Complete (newline-terminated) lines consumed.
    pub lines: u64,
    /// Complete lines that could not be read: not JSON, or an assistant
    /// record whose fields do not decode. Reading continues past them. An
    /// unterminated final line is never consumed and never counted here.
    pub skipped_lines: u64,
    /// Assistant records carrying usage and a `requestId`.
    pub usage_records: u64,
    /// Assistant records carrying usage but no `requestId`. Every one in the
    /// measured corpus is a local `<synthetic>` notice with zero usage; with
    /// no request id there is no call identity to ingest under.
    pub records_without_request_id: u64,
    /// Assistant records with the `<synthetic>` placeholder model, which
    /// Claude Code writes for local notices, not API calls.
    pub synthetic_records: u64,
    /// Calls refused in this parse because a later copy decreased a counter
    /// or disagreed on the call's identity.
    pub calls_refused: u64,
    /// The subset of `calls_refused` that had already been emitted.
    pub calls_refused_after_emission: u64,
    /// Usage records ignored as compaction replays: copies Claude Code wrote
    /// after a `compact_boundary`, with zeroed flat counters and a timestamp
    /// older than the boundary. A subset of `usage_records`. They are neither
    /// merged nor refused.
    pub replayed_records: u64,
    /// Calls whose only records in the file are compaction replays: the
    /// original records were never written. Their counters are zeroed, so
    /// they are not emitted; the calls happened, but this reader cannot say
    /// what they used.
    pub replay_only_calls: u64,
}

/// Parser state carried between resumed reads, plus when, on the caller's
/// clock, the file was last seen to grow. Growth is judged against the
/// resume point's observed file length, which includes an unterminated tail.
#[derive(Debug, Clone)]
struct ClaudeResumeState {
    parse: ClaudeParseState,
    last_growth_ms: i64,
}

/// Where a previous [`parse_claude_file_resumable`] stopped.
#[derive(Debug, Clone)]
pub struct ClaudeResumePoint(JsonlResumePoint<ClaudeResumeState>);

impl ClaudeResumePoint {
    /// Bytes of the file already consumed; always just past a newline.
    pub fn offset(&self) -> u64 {
        self.0.offset()
    }

    /// Calls seen but not yet emitted: in flight, waiting for a stop_reason
    /// or for the file to go quiet.
    pub fn pending_calls(&self) -> usize {
        self.0.state.parse.pending_calls()
    }
}

/// Result of one [`parse_claude_file_resumable`] call.
#[derive(Debug, Default)]
pub struct ClaudeResumableParse {
    pub io_error: Option<String>,
    pub set_aside: Vec<(u64, String)>,
    /// Calls ready for ingest now, in file order. A call appears here once
    /// when it completes (or goes quiet); it appears again only if a later
    /// copy changed it, which the ledger then quarantines.
    pub calls: Vec<ClaudeCodeCall>,
    /// True when the previous point's prefix verified and parsing started at
    /// its offset; false for a full read from the start.
    pub resumed: bool,
    /// Where the next call may resume, or `None` when the file could not be
    /// read, so the next call reads it in full.
    pub resume: Option<ClaudeResumePoint>,
    /// Bytes fed to SHA-256 by this call. A steady-state resume is
    /// O(window + appended), never O(file): the deterministic cost counter.
    pub hashed_bytes: u64,
    pub counts: ClaudeParseCounts,
    /// Calls still waiting for completion after this call.
    pub pending_calls: usize,
}

/// Read one complete line into the state.
fn parse_line(
    payload: &[u8],
    buffer: &mut Vec<u8>,
    state: &mut ClaudeParseState,
    counts: &mut ClaudeParseCounts,
) {
    state.lines += 1;
    let Ok(text) = std::str::from_utf8(payload) else {
        counts.skipped_lines += 1;
        return;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let Some(entry) = parse_json_line::<ClaudeEntry>(trimmed, buffer) else {
        // A record of another type may legitimately have a shape the typed
        // decode rejects; only an unreadable line or an unreadable assistant
        // record is a skipped line.
        match parse_json_line::<EntryTypeProbe>(trimmed, buffer) {
            Some(probe) if probe.entry_type.as_deref() != Some("assistant") => {}
            _ => counts.skipped_lines += 1,
        }
        return;
    };
    if entry.entry_type == "system" && entry.subtype.as_deref() == Some("compact_boundary") {
        state.observe_compact_boundary(parse_rfc3339_millis(entry.timestamp.as_deref()));
        return;
    }
    // Filter by record TYPE, never by position: bookkeeping records carry no
    // cwd and can fill the head of a file.
    if entry.entry_type != "assistant" {
        return;
    }
    let Some(message) = entry.message else {
        return;
    };
    let Some(usage) = message.usage.as_ref() else {
        return;
    };
    if message
        .model
        .as_deref()
        .is_some_and(is_claude_synthetic_placeholder_model)
    {
        counts.synthetic_records += 1;
        return;
    }
    // ASTROCYTE DIVERGENCE: upstream falls back to a `message:{id}` key when
    // `requestId` is absent. Without a request id the call cannot be joined
    // to a gateway's record of it, and every such record measured is a
    // zero-usage local notice, so it is counted and not ingested.
    let Some(request_id) = entry.request_id.filter(|id| !id.is_empty()) else {
        counts.records_without_request_id += 1;
        return;
    };
    counts.usage_records += 1;
    let record = UsageRecord {
        request_id,
        message_id: message.id,
        session_id: entry.session_id,
        project_root: entry.cwd.as_deref().and_then(normalize_workspace_key),
        model: message.model,
        timestamp_ms: parse_rfc3339_millis(entry.timestamp.as_deref()),
        stop_reason: message.stop_reason,
        usage: ClaudeCallUsage::from_usage(usage),
        flat_counters_zero: [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_input_tokens,
            usage.cache_creation_input_tokens,
        ]
        .iter()
        .all(|counter| *counter == Some(0)),
    };
    state.observe(record, counts);
}

/// Parses a Claude Code session file, continuing from `previous` when the
/// bytes it consumed are unchanged.
///
/// The resume check is the shared one in `jsonl_resume.rs`: a shrunk file or
/// a changed inode forces a full re-read, then `check` re-hashes either the
/// trailing window or the whole consumed prefix. A full re-read starts from
/// empty call state and re-emits every complete call, which the ledger
/// absorbs as exact redeliveries.
///
/// Only newline-terminated lines are consumed; an unterminated tail is left
/// for the next call and is not parsed.
///
/// Completion, per call:
/// - any record carries `stop_reason`: complete, emitted now;
/// - no `stop_reason` and the file grew since the last read (or within
///   `quiet_window_ms` of `now_ms`): in flight, kept for a later call;
/// - no `stop_reason` and the file has not grown for `quiet_window_ms`:
///   interrupted, emitted once with no stop reason.
///
/// Growth is judged on the caller's clock from the file's observed length,
/// never from its mtime. A first read, or a full re-read, counts as growth.
pub fn parse_claude_file_resumable(
    path: &Path,
    previous: Option<ClaudeResumePoint>,
    check: ClaudePrefixCheck,
    now_ms: i64,
    quiet_window_ms: i64,
) -> ClaudeResumableParse {
    let mut counts = ClaudeParseCounts::default();
    let mut set_aside = Vec::new();
    let mut buffer = Vec::with_capacity(4096);
    let read = super::jsonl_resume::try_read_appended_lines(
        path,
        previous.map(|point| point.0),
        check,
        // A first read, or a full re-read, counts as growth.
        || ClaudeResumeState {
            parse: ClaudeParseState::default(),
            last_growth_ms: now_ms,
        },
        |state, payload| {
            counts.lines += 1;
            let skipped = counts.skipped_lines;
            parse_line(payload, &mut buffer, &mut state.parse, &mut counts);
            if counts.skipped_lines > skipped {
                set_aside.push((
                    state.parse.lines,
                    "Claude Code record could not be decoded".into(),
                ));
            }
        },
    );
    let read = match read {
        Ok(read) => read,
        Err(error) => {
            return ClaudeResumableParse {
                io_error: Some(error.to_string()),
                ..ClaudeResumableParse::default()
            }
        }
    };
    let mut state = read.state;

    state.last_growth_ms = match read.previous_observed_len {
        Some(len) if len == read.observed_len => state.last_growth_ms,
        _ => now_ms,
    };
    let file_quiet = now_ms.saturating_sub(state.last_growth_ms) >= quiet_window_ms;
    let calls = state.parse.take_emissions(file_quiet);
    let pending_calls = state.parse.pending_calls();

    ClaudeResumableParse {
        io_error: None,
        set_aside,
        calls,
        resumed: read.resumed,
        resume: read
            .cursor
            .map(|cursor| ClaudeResumePoint(cursor.with_state(state))),
        hashed_bytes: read.hashed_bytes,
        counts,
        pending_calls,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Count accesses to the retained history itself, not just the dirty set.
    /// No Deref exposes the underlying map: new traversal APIs must count their
    /// visits here before the parser can use them in a test build.
    #[derive(Debug, Clone, Default)]
    pub(super) struct CallHistory {
        rows: HashMap<String, CallAggregate>,
        visits: std::cell::Cell<usize>,
    }

    impl CallHistory {
        fn visit(&self) {
            self.visits.set(self.visits.get() + 1);
        }

        pub(super) fn get(&self, key: &String) -> Option<&CallAggregate> {
            self.visit();
            self.rows.get(key)
        }

        pub(super) fn get_mut(&mut self, key: &String) -> Option<&mut CallAggregate> {
            self.visit();
            self.rows.get_mut(key)
        }

        pub(super) fn insert(&mut self, key: String, value: CallAggregate) {
            self.rows.insert(key, value);
        }

        pub(super) fn remove(&mut self, key: &String) {
            self.visit();
            self.rows.remove(key);
        }

        fn iter(&self) -> impl Iterator<Item = (&String, &CallAggregate)> {
            self.rows.iter().inspect(|_| self.visit())
        }

        fn len(&self) -> usize {
            self.rows.len()
        }
    }

    impl std::ops::Index<&String> for CallHistory {
        type Output = CallAggregate;

        fn index(&self, key: &String) -> &Self::Output {
            self.visit();
            &self.rows[key]
        }
    }

    const WINDOW: i64 = CLAUDE_CODE_QUIET_WINDOW_MS;

    #[test]
    fn quiet_polls_do_not_walk_emitted_history() {
        let mut state = ClaudeParseState::default();
        let mut counts = ClaudeParseCounts::default();
        let mut buffer = Vec::new();
        for index in 0..32 {
            parse_line(
                record(&format!("r{index}"), 1, Some("end_turn")).as_bytes(),
                &mut buffer,
                &mut state,
                &mut counts,
            );
        }
        assert_eq!(state.take_emissions(false).len(), 32);
        assert_eq!(state.pending_calls(), 0);
        state.calls.visits.set(0);
        assert_eq!(state.calls.iter().count(), 32);
        assert_eq!(state.calls.visits.get(), 32, "history walks are observed");
        state.calls.visits.set(0);
        for _ in 0..3 {
            assert!(state.take_emissions(true).is_empty());
            assert_eq!(state.pending_calls(), 0);
        }
        assert_eq!(
            state.calls.len(),
            32,
            "late-copy comparisons keep their history"
        );
        assert_eq!(
            state.calls.visits.get(),
            0,
            "quiet polls must not visit emitted aggregates"
        );
    }

    #[test]
    fn dirty_scheduling_preserves_order_pending_refusals_and_late_changes() {
        let mut state = ClaudeParseState::default();
        let mut counts = ClaudeParseCounts::default();
        let mut buffer = Vec::new();
        for (id, output, stop) in [
            ("z", 3, None),
            ("a", 2, Some("end_turn")),
            ("refused", 3, None),
        ] {
            parse_line(
                record(id, output, stop).as_bytes(),
                &mut buffer,
                &mut state,
                &mut counts,
            );
        }
        let first = state.take_emissions(false);
        assert_eq!(
            first
                .iter()
                .map(|call| call.request_id.as_str())
                .collect::<Vec<_>>(),
            ["a"]
        );
        assert_eq!(state.pending_calls(), 2);
        parse_line(
            record("refused", 1, None).as_bytes(),
            &mut buffer,
            &mut state,
            &mut counts,
        );
        assert_eq!(state.pending_calls(), 1);
        parse_line(
            record("a", 4, Some("end_turn")).as_bytes(),
            &mut buffer,
            &mut state,
            &mut counts,
        );
        let quiet = state.take_emissions(true);
        assert_eq!(
            quiet
                .iter()
                .map(|call| call.request_id.as_str())
                .collect::<Vec<_>>(),
            ["z", "a"]
        );
        assert_eq!(quiet[0].usage.output, Some(3));
        assert_eq!(quiet[1].usage.output, Some(4));
        assert_eq!(state.pending_calls(), 0);
        assert_eq!(counts.calls_refused, 1);
        assert_eq!(counts.calls_refused_after_emission, 0);
        parse_line(
            record("a", 4, Some("end_turn")).as_bytes(),
            &mut buffer,
            &mut state,
            &mut counts,
        );
        assert!(state.take_emissions(true).is_empty());
    }

    fn record(request: &str, output: u64, stop: Option<&str>) -> String {
        let stop = stop.map_or("null".to_string(), |stop| format!("\"{stop}\""));
        format!(
            r#"{{"type":"assistant","timestamp":"2026-09-01T00:00:01.000Z","requestId":"{request}","sessionId":"s1","cwd":"/work/a.b-c","message":{{"id":"msg_{request}","model":"claude-opus-5","stop_reason":{stop},"usage":{{"input_tokens":3,"output_tokens":{output},"cache_read_input_tokens":5,"cache_creation_input_tokens":7,"cache_creation":{{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":5}}}}}}}}"#
        ) + "\n"
    }

    fn file_with(contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn append(path: &Path, text: &str) {
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn parse(path: &Path, previous: Option<ClaudeResumePoint>, now: i64) -> ClaudeResumableParse {
        parse_claude_file_resumable(path, previous, ClaudePrefixCheck::Window, now, WINDOW)
    }

    #[test]
    fn bookkeeping_records_and_other_shapes_are_not_skipped_lines() {
        let (_dir, path) = file_with(concat!(
            "{\"type\":\"last-prompt\",\"text\":\"hi\"}\n",
            "{\"type\":\"user\",\"message\":\"a plain string\"}\n",
            "\n"
        ));
        let parse = parse(&path, None, 0);
        assert_eq!(parse.counts.lines, 3);
        assert_eq!(parse.counts.skipped_lines, 0);
        assert!(parse.calls.is_empty());
    }

    #[test]
    fn absent_counter_stays_absent_and_negative_counter_is_a_skipped_line() {
        let (_dir, path) = file_with(concat!(
            r#"{"type":"assistant","requestId":"r1","message":{"model":"claude-opus-5","stop_reason":"end_turn","usage":{"input_tokens":3}}}"#,
            "\n",
            r#"{"type":"assistant","requestId":"r2","message":{"model":"claude-opus-5","stop_reason":"end_turn","usage":{"input_tokens":-3}}}"#,
            "\n"
        ));
        let parse = parse(&path, None, 0);
        assert_eq!(parse.counts.skipped_lines, 1);
        assert_eq!(parse.calls.len(), 1);
        let call = &parse.calls[0];
        assert_eq!(call.usage.input, Some(3));
        assert_eq!(call.usage.output, None);
        assert_eq!(call.usage.cache_write_unsplit, None);
        assert_eq!(
            call.occurred_at_ms, None,
            "no timestamp: undated, not mtime"
        );
    }

    #[test]
    fn flat_cache_write_without_split_is_kept_unsplit() {
        let (_dir, path) = file_with(concat!(
            r#"{"type":"assistant","requestId":"r1","message":{"model":"claude-opus-5","stop_reason":"end_turn","usage":{"input_tokens":3,"cache_creation_input_tokens":9}}}"#,
            "\n"
        ));
        let call = &parse(&path, None, 0).calls[0];
        assert_eq!(
            (
                call.usage.cache_write_5m,
                call.usage.cache_write_1h,
                call.usage.cache_write_unsplit
            ),
            (None, None, Some(9))
        );
    }

    #[test]
    fn synthetic_and_request_less_records_are_counted_not_ingested() {
        let (_dir, path) = file_with(concat!(
            r#"{"type":"assistant","message":{"model":"<synthetic>","usage":{"input_tokens":0}}}"#,
            "\n",
            r#"{"type":"assistant","message":{"model":"claude-opus-5","stop_reason":"end_turn","usage":{"input_tokens":0}}}"#,
            "\n"
        ));
        let parse = parse(&path, None, 0);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.synthetic_records, 1);
        assert_eq!(parse.counts.records_without_request_id, 1);
    }

    #[test]
    fn a_complete_call_is_emitted_once_and_an_identical_late_copy_is_not_re_emitted() {
        let (_dir, path) = file_with(&record("r1", 10, Some("tool_use")));
        let first = parse(&path, None, 0);
        assert_eq!(first.calls.len(), 1);
        append(&path, &record("r1", 10, Some("tool_use")));
        let second = parse(&path, first.resume, 1);
        assert!(second.resumed);
        assert!(second.calls.is_empty());
        assert_eq!(second.counts.usage_records, 1);
    }

    #[test]
    fn a_late_copy_that_grows_an_emitted_call_is_emitted_again_for_quarantine() {
        let (_dir, path) = file_with(&record("r1", 10, Some("tool_use")));
        let first = parse(&path, None, 0);
        append(&path, &record("r1", 12, Some("tool_use")));
        let second = parse(&path, first.resume, 1);
        assert_eq!(second.calls.len(), 1);
        assert_eq!(second.calls[0].usage.output, Some(12));
    }

    /// An assistant record shaped like a live Claude Code 2.1 record, with
    /// the four flat counters `[input, output, cache_read, cache_creation]`
    /// and a 1h-only cache-write split.
    fn live_record(
        request: &str,
        timestamp: &str,
        model: &str,
        flat: [u64; 4],
        split_1h: u64,
    ) -> String {
        let [input, output, cache_read, cache_creation] = flat;
        format!(
            r#"{{"type":"assistant","uuid":"u-{request}-{input}","timestamp":"{timestamp}","requestId":"{request}","sessionId":"s1","cwd":"/work/hook-live","message":{{"id":"msg_{request}","model":"{model}","stop_reason":"tool_use","usage":{{"input_tokens":{input},"cache_creation_input_tokens":{cache_creation},"cache_read_input_tokens":{cache_read},"output_tokens":{output},"cache_creation":{{"ephemeral_1h_input_tokens":{split_1h},"ephemeral_5m_input_tokens":0}},"iterations":[{{"input_tokens":2,"output_tokens":115}}]}}}}}}"#
        ) + "\n"
    }

    /// The original record of a call; its values come from a real Claude
    /// Code 2.1.282 transcript that was later compacted.
    fn original(output: u64) -> String {
        live_record(
            "req_A",
            "2026-09-25T10:40:42.064Z",
            "claude-sonnet-5",
            [2, output, 109_580, 354],
            354,
        )
    }

    /// Claude Code's copy of the original after a compaction: same request,
    /// message, timestamp and split; zero flat counters.
    fn replay_copy() -> String {
        live_record(
            "req_A",
            "2026-09-25T10:40:42.064Z",
            "claude-sonnet-5",
            [0, 0, 0, 0],
            354,
        )
    }

    fn compact_boundary() -> String {
        concat!(
            r#"{"type":"system","subtype":"compact_boundary","content":"Conversation compacted","compactMetadata":{"trigger":"manual","preTokens":110244,"postTokens":129},"timestamp":"2026-09-25T10:41:27.755Z","sessionId":"s1","cwd":"/work/hook-live"}"#,
            "\n",
            r#"{"type":"user","isCompactSummary":true,"timestamp":"2026-09-25T10:41:27.753Z","message":{"role":"user","content":"summary"}}"#,
            "\n"
        )
        .to_string()
    }

    fn assert_original_usage(call: &ClaudeCodeCall) {
        assert_eq!(
            call.usage,
            ClaudeCallUsage {
                input: Some(2),
                output: Some(115),
                cache_read: Some(109_580),
                cache_write_5m: Some(0),
                cache_write_1h: Some(354),
                cache_write_unsplit: None,
            }
        );
        assert_eq!(call.records, 1, "the replay copy is not folded in");
    }

    #[test]
    fn a_compaction_replay_is_ignored_not_refused_and_the_original_is_emitted_once() {
        let (_dir, path) = file_with(&(original(115) + &compact_boundary() + &replay_copy()));
        let parse = parse(&path, None, 0);
        assert_eq!(parse.calls.len(), 1);
        assert_original_usage(&parse.calls[0]);
        assert_eq!(parse.counts.calls_refused, 0);
        assert_eq!(parse.counts.replayed_records, 1);
        assert_eq!(parse.counts.replay_only_calls, 0);
        assert_eq!(parse.counts.usage_records, 2);
    }

    #[test]
    fn a_compaction_replay_read_in_a_later_resumed_parse_is_ignored_and_not_re_emitted() {
        // Split once right after the original, and once between the boundary
        // and the copy: the call aggregate and the boundary both survive the
        // resume.
        let whole = [original(115), compact_boundary(), replay_copy()];
        for split in 1..=2 {
            let (_dir, path) = file_with(&whole[..split].concat());
            let first = parse(&path, None, 0);
            assert_eq!(first.calls.len(), 1, "split {split}");
            assert_original_usage(&first.calls[0]);
            append(&path, &whole[split..].concat());
            let second = parse(&path, first.resume, 1);
            assert!(second.resumed, "split {split}");
            assert!(second.calls.is_empty(), "split {split}: no second emission");
            assert_eq!(second.counts.calls_refused, 0, "split {split}");
            assert_eq!(second.counts.replayed_records, 1, "split {split}");
            assert_eq!(second.pending_calls, 0, "split {split}");
        }
    }

    #[test]
    fn a_replay_with_no_original_in_the_file_is_counted_and_never_emitted() {
        let (_dir, path) = file_with(&(compact_boundary() + &replay_copy() + &replay_copy()));
        let parse = parse(&path, None, 0);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.pending_calls, 0);
        assert_eq!(parse.counts.calls_refused, 0);
        assert_eq!(parse.counts.replayed_records, 2);
        assert_eq!(parse.counts.replay_only_calls, 1);
    }

    #[test]
    fn a_partial_counter_decrease_is_still_refused_after_a_compaction() {
        let decreased = original(115).replace(r#""output_tokens":115,"#, r#""output_tokens":100,"#);
        assert_ne!(decreased, original(115));
        let (_dir, path) = file_with(&(original(115) + &compact_boundary() + &decreased));
        let parse = parse(&path, None, 0);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.calls_refused, 1);
        assert_eq!(parse.counts.replayed_records, 0);
    }

    #[test]
    fn a_zeroed_copy_that_names_another_model_is_refused() {
        let other_model = live_record(
            "req_A",
            "2026-09-25T10:40:42.064Z",
            "claude-opus-5",
            [0, 0, 0, 0],
            354,
        );
        let (_dir, path) = file_with(&(original(115) + &compact_boundary() + &other_model));
        let parse = parse(&path, None, 0);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.calls_refused, 1);
        assert_eq!(parse.counts.replayed_records, 0);
    }

    #[test]
    fn a_zeroed_copy_with_no_boundary_before_it_is_still_refused() {
        let (_dir, path) = file_with(&(original(115) + &replay_copy()));
        let parse = parse(&path, None, 0);
        assert!(parse.calls.is_empty());
        assert_eq!(parse.counts.calls_refused, 1);
        assert_eq!(parse.counts.replayed_records, 0);
    }

    #[test]
    fn an_all_zero_call_that_is_not_a_replay_is_still_emitted() {
        // Before any boundary, and after one but timestamped after it: both
        // are genuine records of a zero-usage call.
        let before = live_record(
            "req_Z1",
            "2026-09-25T10:40:00.000Z",
            "claude-sonnet-5",
            [0, 0, 0, 0],
            0,
        );
        let after = live_record(
            "req_Z2",
            "2026-09-25T10:42:00.000Z",
            "claude-sonnet-5",
            [0, 0, 0, 0],
            0,
        );
        let (_dir, path) = file_with(&(before + &compact_boundary() + &after));
        let parse = parse(&path, None, 0);
        let ids: Vec<&str> = parse
            .calls
            .iter()
            .map(|call| call.request_id.as_str())
            .collect();
        assert_eq!(ids, ["req_Z1", "req_Z2"]);
        for call in &parse.calls {
            assert_eq!((call.usage.input, call.usage.output), (Some(0), Some(0)));
        }
        assert_eq!(parse.counts.replayed_records, 0);
        assert_eq!(parse.counts.calls_refused, 0);
    }

    #[test]
    fn a_rewritten_prefix_rereads_from_scratch() {
        let (_dir, path) = file_with(&record("r1", 10, Some("end_turn")));
        let first = parse(&path, None, 0);
        std::fs::write(&path, record("r2", 10, Some("end_turn"))).unwrap();
        let second = parse(&path, first.resume, 1);
        assert!(!second.resumed);
        assert_eq!(second.calls.len(), 1);
        assert_eq!(second.calls[0].request_id, "r2");
    }
}
