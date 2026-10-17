// Copied from tokscale v4.17.0, crates/tokscale-core/src/sessions/pi.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.

//! Pi (badlogic/pi-mono) session parser
//!
//! Parses JSONL files from `~/.pi/agent/sessions/<encoded-cwd>/*.jsonl`. Current
//! OMP builds write a `title` metadata record before the `session` header in
//! newly-created session files; see `PRE_SESSION_METADATA_TYPES`.
//!
//! tokscale v4.17.0 shares this parser with Pi descendants that reuse the same
//! record layout. This crate exposes only Pi; its Prime-compatible entry point
//! exists only in tests so shared-parser coverage does not make Prime supported.
//!
//! [`PI_FORMAT_PARSER_BASE_VERSION`] stays equal to tokscale's format version so
//! parser changes remain comparable. Parser caching and daemon ingestion are not
//! part of this reader crate.

use super::utils::{
    for_each_json_line_with_bytes, hash_file_range, parse_json_line, JsonLineBytes,
};
use super::{normalize_workspace_key, workspace_label_from_key, UnifiedMessage};
use crate::file_identity::{inode_of, same_identity};
use crate::provider_identity::inferred_provider_from_model;
use crate::TokenBreakdown;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::Path;

/// Pi parser format version from tokscale, the upstream usage-reporting tool.
/// Retained for comparison with upstream; this crate does not use the version
/// to invalidate ingestion or document caches.
pub const PI_FORMAT_PARSER_BASE_VERSION: u32 = 1;

/// Pi session header (first line of JSONL)
#[derive(Debug, Deserialize)]
pub struct PiSessionHeader {
    #[serde(rename = "type")]
    pub entry_type: String,
    pub id: String,
    pub timestamp: Option<String>,
    pub cwd: Option<String>,
    #[serde(rename = "parentSession")]
    pub parent_session: Option<String>,
    #[serde(rename = "rlmDepth")]
    pub rlm_depth: Option<u32>,
}

impl PiSessionHeader {
    pub(crate) fn has_invalid_lineage(&self) -> bool {
        let is_rlm_child = self.rlm_depth.unwrap_or(0) > 0;
        is_rlm_child
            && self
                .parent_session
                .as_deref()
                .is_none_or(|parent| parent.trim().is_empty() || has_replacement_character(parent))
    }
}

fn damaged_key_may_name(key: &str, expected: &str) -> bool {
    if !has_replacement_character(key) {
        return false;
    }

    let pattern: Vec<char> = key.chars().collect();
    let expected: Vec<char> = expected.chars().collect();
    let mut matches = vec![vec![false; expected.len() + 1]; pattern.len() + 1];
    matches[0][0] = true;
    for pattern_index in 0..pattern.len() {
        for expected_index in 0..=expected.len() {
            if !matches[pattern_index][expected_index] {
                continue;
            }
            if pattern[pattern_index] == char::REPLACEMENT_CHARACTER {
                // A replacement in the middle may represent either damaged
                // bytes that replaced key characters or an undecodable byte
                // inserted between otherwise intact characters. At an edge it
                // must consume at least one expected character, so complete
                // known keys with a damaged extension prefix/suffix are not
                // mistaken for the known key itself.
                let minimum = if pattern_index > 0 && pattern_index + 1 < pattern.len() {
                    expected_index
                } else {
                    expected_index + 1
                };
                for matched in matches[pattern_index + 1].iter_mut().skip(minimum) {
                    *matched = true;
                }
            } else if expected_index < expected.len()
                && pattern[pattern_index] == expected[expected_index]
            {
                matches[pattern_index + 1][expected_index + 1] = true;
            }
        }
    }
    matches[pattern.len()][expected.len()]
}

const PRIME_LINEAGE_HEADER_KEYS: &[&str] = &["parentSession", "rlmDepth"];

/// Inspect every top-level JSON key after JSON string decoding and reject a
/// replacement-bearing spelling that may be a damaged Prime lineage key.
///
/// Valid U+FFFD characters are not inherently damage: unrelated extension
/// keys, including `rlmDepth�`, remain valid. Invalid UTF-8 is tracked
/// separately because a replacement immediately beside a complete structural
/// key may have replaced rather than extended that key.
pub(crate) fn raw_json_has_damaged_lineage_header_key(raw: &[u8]) -> bool {
    let mut index = 0usize;
    let mut depth = 0usize;
    while index < raw.len() {
        match raw[index] {
            b'{' | b'[' => {
                depth += 1;
                index += 1;
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            b'"' => {
                let quoted_start = index;
                index += 1;
                let mut escaped = false;
                while index < raw.len() {
                    let byte = raw[index];
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        break;
                    }
                    index += 1;
                }
                if index >= raw.len() {
                    return false;
                }

                let quoted = &raw[quoted_start..=index];
                let key_bytes = &raw[quoted_start + 1..index];
                let mut after = index + 1;
                while after < raw.len() && raw[after].is_ascii_whitespace() {
                    after += 1;
                }
                if depth == 1 && raw.get(after) == Some(&b':') {
                    let key_had_invalid_utf8 = std::str::from_utf8(key_bytes).is_err();
                    let lossy_quoted = String::from_utf8_lossy(quoted);
                    if let Ok(decoded) = serde_json::from_str::<String>(&lossy_quoted) {
                        let without_replacements: String = decoded
                            .chars()
                            .filter(|character| *character != char::REPLACEMENT_CHARACTER)
                            .collect();
                        if PRIME_LINEAGE_HEADER_KEYS.iter().any(|expected| {
                            damaged_key_may_name(&decoded, expected)
                                || (key_had_invalid_utf8 && without_replacements == *expected)
                        }) {
                            return true;
                        }
                    }
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    false
}

/// Loose type-only probe for a JSONL line, used to identify pre-session
/// metadata records without requiring their full schema.
#[derive(Debug, Deserialize)]
struct PiEntryTypeProbe {
    #[serde(rename = "type")]
    entry_type: String,
}

/// Record types OMP may write before the `session` header (e.g. an
/// auto-generated-title record). The parser skips these while looking for
/// `session` rather than discarding the whole file. Any other unrecognized
/// type before `session` is still treated as a malformed file.
pub(crate) const PRE_SESSION_METADATA_TYPES: &[&str] = &["title"];

/// A lossy pre-header line is skippable only when it could not be parsed for
/// its record type. A replacement-bearing line that parses as a real type is
/// still treated as a foreign/malformed file, keeping both Prime scans aligned.
pub(crate) fn has_replacement_character(value: &str) -> bool {
    value.contains(char::REPLACEMENT_CHARACTER)
}

pub(crate) fn pre_header_line_is_skippable(trimmed: &str, parsed_type: Option<&str>) -> bool {
    parsed_type.is_none() && has_replacement_character(trimmed)
}

/// Pi session entry (subsequent lines of JSONL)
#[derive(Debug, Deserialize)]
pub struct PiSessionEntry {
    #[serde(rename = "type")]
    pub entry_type: String,
    pub id: Option<String>,
    #[serde(rename = "parentId")]
    pub parent_id: Option<String>,
    pub timestamp: Option<String>,
    pub message: Option<PiMessage>,
    pub name: Option<String>,
    #[serde(rename = "targetId")]
    pub target_id: Option<String>,
    #[serde(rename = "childUsage")]
    pub child_usage: Option<PiUsage>,
    #[serde(rename = "aggregateUsage")]
    pub aggregate_usage: Option<PiUsage>,
    #[serde(flatten)]
    extra_fields: BTreeMap<String, serde_json::Value>,
}

impl PiSessionEntry {
    fn has_damaged_timestamp(&self) -> bool {
        self.timestamp
            .as_deref()
            .is_some_and(has_replacement_character)
            || self
                .extra_fields
                .keys()
                .any(|key| damaged_key_may_name(key, "timestamp"))
    }
}

#[derive(Debug, Deserialize)]
pub struct PiMessage {
    pub role: Option<String>,
    pub usage: Option<PiUsage>,
    pub model: Option<String>,
    pub provider: Option<String>,
    #[serde(rename = "responseId")]
    pub response_id: Option<String>,
}

/// The camelCase usage block of a Pi record: `utils::CamelUsage`'s
/// `{input, output, cacheRead, cacheWrite, totalTokens}` plus `reasoning` and
/// a flattened map of every remaining key. See the note on `CamelUsage` for
/// why the two shapes are not merged.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PiUsage {
    // ASTROCYTE DIVERGENCE: tokscale uses signed counters and clamps negatives.
    // Harness counters are non-negative measurements; unsigned options reject
    // corrupt negatives while preserving absent separately from reported zero.
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
    pub total_tokens: Option<u64>,
    pub reasoning: Option<u64>,
    #[serde(flatten)]
    extra_fields: BTreeMap<String, serde_json::Value>,
}

// Ingest rejects invalid UTF-8 and does not inspect unknown extension keys,
// so these structs skip their values. The whole-file `parse_pi_file` retains
// the upstream decode shape; Prime Agent's copied tests use its `extra_fields`
// maps to recognize known keys damaged by invalid UTF-8. Converting these
// structs to `PiSessionEntry` preserves all known fields, including fields
// ingest ignores: a malformed known field still makes its message unreadable.
//
// A derived struct without `#[serde(flatten)]` also accepts a JSON array,
// read positionally. The upstream structs' flattened maps make serde decode
// the entry and every usage block as maps only, so ingest wraps those
// positions in `MapOnly` to skip the same array-shaped lines. `message` stays
// unwrapped: upstream `PiMessage` has no flattened map and accepts an array.
#[derive(Deserialize)]
struct IngestPiEntry {
    #[serde(rename = "type")]
    entry_type: String,
    id: Option<String>,
    #[serde(rename = "parentId")]
    parent_id: Option<String>,
    timestamp: Option<String>,
    message: Option<IngestPiMessage>,
    name: Option<String>,
    #[serde(rename = "targetId")]
    target_id: Option<String>,
    #[serde(rename = "childUsage")]
    child_usage: Option<MapOnly<IngestPiUsage>>,
    #[serde(rename = "aggregateUsage")]
    aggregate_usage: Option<MapOnly<IngestPiUsage>>,
}

#[derive(Deserialize)]
struct IngestPiMessage {
    role: Option<String>,
    usage: Option<MapOnly<IngestPiUsage>>,
    model: Option<String>,
    provider: Option<String>,
    #[serde(rename = "responseId")]
    response_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IngestPiUsage {
    input: Option<u64>,
    output: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    total_tokens: Option<u64>,
    reasoning: Option<u64>,
}

/// Decodes `T` from a JSON object only and refuses an array. The object's
/// entries are handed straight to `T`'s derived visitor, so unknown keys are
/// still skipped rather than buffered.
struct MapOnly<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for MapOnly<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MapOnlyVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for MapOnlyVisitor<T> {
            type Value = MapOnly<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(MapOnly)
            }
        }

        deserializer.deserialize_map(MapOnlyVisitor(std::marker::PhantomData))
    }
}

impl From<MapOnly<IngestPiUsage>> for PiUsage {
    fn from(MapOnly(usage): MapOnly<IngestPiUsage>) -> Self {
        Self {
            input: usage.input,
            output: usage.output,
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
            total_tokens: usage.total_tokens,
            reasoning: usage.reasoning,
            extra_fields: BTreeMap::new(),
        }
    }
}

impl From<MapOnly<IngestPiEntry>> for PiSessionEntry {
    fn from(MapOnly(entry): MapOnly<IngestPiEntry>) -> Self {
        Self {
            entry_type: entry.entry_type,
            id: entry.id,
            parent_id: entry.parent_id,
            timestamp: entry.timestamp,
            message: entry.message.map(|message| PiMessage {
                role: message.role,
                usage: message.usage.map(Into::into),
                model: message.model,
                provider: message.provider,
                response_id: message.response_id,
            }),
            name: entry.name,
            target_id: entry.target_id,
            child_usage: entry.child_usage.map(Into::into),
            aggregate_usage: entry.aggregate_usage.map(Into::into),
            extra_fields: BTreeMap::new(),
        }
    }
}

struct PiNormalizedUsage {
    tokens: TokenBreakdown,
    output_reasoning_conflicted: bool,
}

impl PiUsage {
    /// Convert Pi's nested reasoning count into disjoint output buckets.
    ///
    /// ASTROCYTE DIVERGENCE: tokscale maps reasoning to zero because its totals
    /// are additive while Pi includes reasoning inside output. Astrocyte needs
    /// the per-row reasoning measurement, so it subtracts reasoning from output
    /// and preserves both. If the reported values contradict that relationship,
    /// output remains absent and the record is marked conflicted so downstream
    /// pricing can refuse it without losing the rest of the transcript.
    fn to_breakdown(&self) -> PiNormalizedUsage {
        let (output, output_reasoning_conflicted) = match (self.output, self.reasoning) {
            (Some(source_output), Some(reasoning)) => match source_output.checked_sub(reasoning) {
                Some(disjoint_output) => {
                    assert_eq!(
                        disjoint_output.checked_add(reasoning),
                        Some(source_output),
                        "Pi reasoning normalization must conserve source output"
                    );
                    (Some(disjoint_output), false)
                }
                None => (None, true),
            },
            (output, None) => (output, false),
            (None, Some(0)) => (None, false),
            (None, Some(_)) => (None, true),
        };

        PiNormalizedUsage {
            tokens: TokenBreakdown {
                input: self.input,
                output,
                cache_read: self.cache_read,
                cache_write: self.cache_write,
                reasoning: self.reasoning,
            },
            output_reasoning_conflicted,
        }
    }

    pub(crate) fn has_damaged_key(&self) -> bool {
        const TOKEN_COUNTER_KEYS: &[&str] = &[
            "input",
            "output",
            "cacheRead",
            "cacheWrite",
            "totalTokens",
            "reasoning",
        ];

        self.extra_fields.keys().any(|key| {
            TOKEN_COUNTER_KEYS
                .iter()
                .any(|expected| damaged_key_may_name(key, expected))
        })
    }
}

fn is_generated_id(value: &str) -> bool {
    (value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        || (value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            }))
}

fn strip_generated_id(value: &str) -> Option<&str> {
    for id_len in [36, 8] {
        if value.len() <= id_len || value.as_bytes()[value.len() - id_len - 1] != b'-' {
            continue;
        }
        let id = &value[value.len() - id_len..];
        if is_generated_id(id) {
            return Some(&value[..value.len() - id_len - 1]);
        }
    }
    None
}

fn pi_subagent_name(session_name: &str) -> Option<String> {
    let name = session_name.strip_prefix("subagent-")?;
    let without_id = strip_generated_id(name).or_else(|| {
        let (without_index, index) = name.rsplit_once('-')?;
        if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        strip_generated_id(without_index)
    })?;

    (!without_id.is_empty()).then(|| without_id.to_string())
}

/// Parse a whole Pi JSONL session file into tokscale's `UnifiedMessage`s.
///
/// Astrocyte's drain does not call this: it uses `parse_pi_file_resumable`,
/// which resumes at a byte offset and keeps undated records. This entry point
/// stays for two reasons. It is the shape of the upstream tokscale reader this
/// file is copied from, so keeping it keeps upstream fixes easy to port. And it
/// is an independent whole-file parse that tests, including the live-corpus
/// cross-check in astrocyte-module's pi_source, compare against the resumable
/// drain. Undated records are omitted, because `UnifiedMessage` requires a
/// timestamp.
pub fn parse_pi_file(path: &Path) -> Vec<UnifiedMessage> {
    parse_pi_format_file(path, "pi", "pi")
}

/// Parse a JSONL session file written in the Pi record format.
///
/// `client` is the tokscale client id stamped on every emitted message, and
/// `fallback_provider` is used only when the message carries no provider and
/// the model name is not recognizable.
///
/// Deduplication is cross-session (`PiParseOptions::standard_deduped`):
/// Pi branch/fork creates a new session file and copies prior assistant
/// records into it verbatim (#1306), so a per-session key would count every
/// copied record once per file. The parser emits a session-independent key —
/// `message.responseId` preferred, the entry id plus immutable event fields
/// as fallback — and the Pi lane drops repeats during aggregation.
pub(crate) fn parse_pi_format_file(
    path: &Path,
    client: &str,
    fallback_provider: &'static str,
) -> Vec<UnifiedMessage> {
    let mut observer = NoopPiFormatObserver;
    // ASTROCYTE DIVERGENCE: an undated record is omitted here. This
    // tokscale-shaped API returns `UnifiedMessage`, whose timestamp is
    // required, and any number put there (a file mtime, the epoch) would be
    // an invented time. The ingest path, `parse_pi_file_resumable`, keeps the
    // record and the ledger stores it as an undated event.
    parse_pi_format_file_inner(
        path,
        client,
        fallback_provider,
        Some(client),
        PiParseOptions::standard_deduped(),
        &mut observer,
    )
    .into_iter()
    .filter(|record| record.occurred_at_ms.is_some())
    .map(|record| record.message)
    .collect()
}

/// Receives already-decoded Pi records while the shared parser walks a file.
///
/// Prime Agent uses this hook to derive its fork/child accounting metadata in
/// the same pass that emits messages. The emitted message is supplied only for
/// an assistant record that passed the shared parser's validation.
pub(crate) trait PiFormatObserver {
    fn observe_header(&mut self, _header: &PiSessionHeader) {}

    fn observe_entry(&mut self, _entry: &PiSessionEntry, _emitted: Option<&UnifiedMessage>) {}
}

struct NoopPiFormatObserver;

impl PiFormatObserver for NoopPiFormatObserver {}

/// Parse the Prime Agent Pi-compatible format whose `session_info.name`
/// identifies an RLM subagent when the session header has `rlmDepth > 0`.
///
/// Deduplication is intentionally cross-session: Prime Agent forks copy prior
/// message entries into a file with a new session id. Provider response ids are
/// preferred; the message id plus immutable event fields is the fallback.
///
/// Prime Agent's append-only JSONL may contain a UTF-8 BOM or undecodable
/// records. Lossy line handling keeps malformed records local to their own
/// line without changing the historical behavior of other Pi clients.
// ASTROCYTE DIVERGENCE: Prime Agent is not a reader in this slice; compile its
// shared-format entry point only to retain the copied Pi parser tests.
#[cfg(test)]
pub(crate) fn parse_pi_format_rlm_file_with_observer(
    path: &Path,
    client: &str,
    fallback_provider: &'static str,
    observer: &mut impl PiFormatObserver,
) -> Vec<UnifiedMessage> {
    // Undated records are kept, as upstream: the observer has already seen
    // each one as emitted, and Prime's accounting walk matches records by
    // position, so dropping one here would misalign every later record.
    parse_pi_format_file_inner(
        path,
        client,
        fallback_provider,
        Some(client),
        PiParseOptions::prime_agent(),
        observer,
    )
    .into_iter()
    .map(|record| record.message)
    .collect()
}

#[derive(Clone, Copy)]
struct PiParseOptions {
    rlm_session_name_as_agent: bool,
    lossy_line_reader: bool,
    retain_extra_fields: bool,
}

impl PiParseOptions {
    /// Standard byte-strict parsing with cross-session deduplication: the
    /// Pi/Senpi/Omp lanes, whose clients all fork sessions by copying prior
    /// assistant records into a new file (#1306).
    const fn standard_deduped() -> Self {
        Self {
            rlm_session_name_as_agent: false,
            retain_extra_fields: true,
            lossy_line_reader: false,
        }
    }

    // ASTROCYTE DIVERGENCE: retained only by the copied Prime-mode parser tests.
    #[cfg(test)]
    const fn prime_agent() -> Self {
        Self {
            rlm_session_name_as_agent: true,
            retain_extra_fields: true,
            lossy_line_reader: true,
        }
    }
}

fn accepts_replacement_field(value: &str, lossy_line_reader: bool) -> bool {
    !lossy_line_reader || !has_replacement_character(value)
}

fn damaged_cross_session_dedup_key(namespace: &str, raw_line: &[u8]) -> String {
    let mut hasher = Sha256::new();
    // Exact source bytes distinguish invalid UTF-8 sequences that lossy decode
    // maps to the same U+FFFD while keeping copied fork records stable.
    hasher.update(raw_line);
    format!("{namespace}:damaged:{:x}", hasher.finalize())
}

fn damaged_session_placeholder(path: &Path) -> String {
    let source_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(source_path.as_os_str().as_encoded_bytes());
    format!("unknown:path:{:x}", hasher.finalize())
}

/// One record, one decision: does this entry become a [`UnifiedMessage`]?
///
/// Two walks read the same transcript. [`parse_pi_format_file_inner`] streams it
/// and builds messages; Prime Agent's accounting walk replays already-parsed
/// messages positionally against the same records
/// (`sessions::prime_agent::analyze_prime_agent_accounting`). A record that only
/// one of them counts shifts every later index in the other, which silently
/// re-targets Prime's usage reconciliation. Both therefore ask this function
/// rather than restating the rules, so neither can drift from the other.
struct PiEmittedRecord<'a> {
    message: &'a PiMessage,
    usage: &'a PiUsage,
    recorded_model: &'a str,
}

fn pi_emitted_record<'a>(
    entry: &'a PiSessionEntry,
    options: PiParseOptions,
    is_rlm_subagent: bool,
) -> Option<PiEmittedRecord<'a>> {
    if entry.entry_type != "message" {
        return None;
    }

    let message = entry.message.as_ref()?;
    if message.role.as_deref() != Some("assistant") {
        return None;
    }

    // An RLM child's completion timestamp participates in matching its
    // usage back to the parent attribution. Recovering a replacement-
    // damaged value as "missing" would make that match impossible while
    // still emitting the child, so the parent's aggregate and child would
    // both be counted. Other Pi messages do not use this timestamp as a
    // reconciliation join and remain recoverable.
    if options.lossy_line_reader
        && is_rlm_subagent
        && (entry.has_damaged_timestamp()
            || entry
                .timestamp
                .as_deref()
                .is_some_and(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).is_err()))
    {
        return None;
    }

    let usage = message.usage.as_ref()?;
    if options.lossy_line_reader && usage.has_damaged_key() {
        return None;
    }

    let recorded_model = message.model.as_deref()?;
    Some(PiEmittedRecord {
        message,
        usage,
        recorded_model,
    })
}

fn parse_pi_format_file_inner(
    path: &Path,
    client: &str,
    fallback_provider: &'static str,
    dedup_namespace: Option<&str>,
    options: PiParseOptions,
    observer: &mut impl PiFormatObserver,
) -> Vec<PiUsageRecord> {
    let context = PiLineContext {
        path,
        client,
        fallback_provider,
        dedup_namespace,
        options,
    };

    let mut records: Vec<PiUsageRecord> = Vec::with_capacity(64);
    let mut buffer = Vec::with_capacity(4096);
    let mut state = PiParseState::default();
    // This entry point has no place to report skipped lines; the resumable
    // reader that feeds ingest reports them.
    let mut skipped_lines = 0_u64;
    // A header this parser rejects discards the whole transcript, not one
    // record, so the sink stops the scan and nothing is returned.
    let mut malformed_transcript = false;

    for_each_json_line_with_bytes(path, &mut |line| {
        let flow = parse_pi_line(
            &context,
            &mut state,
            &mut buffer,
            observer,
            &mut records,
            &mut skipped_lines,
            line,
        );
        malformed_transcript |= flow.is_break();
        flow
    });

    if malformed_transcript {
        return Vec::new();
    }

    records
}

/// One assistant usage record read from a Pi transcript.
#[derive(Debug, Clone, PartialEq)]
pub struct PiUsageRecord {
    /// Byte offset of the record in a resumable read; not an event identity.
    pub source_offset: u64,
    /// The record in tokscale's shape. For an undated record its `timestamp`
    /// is 0, because [`UnifiedMessage`] cannot say "absent"; read
    /// `occurred_at_ms` instead. [`parse_pi_file`] omits such a record rather
    /// than hand out that 0.
    pub message: UnifiedMessage,
    /// When the call happened, from the entry's RFC 3339 `timestamp`, or
    /// `None` when the entry has none or it does not parse. Never derived
    /// from the file.
    pub occurred_at_ms: Option<i64>,
}

// ASTROCYTE DIVERGENCE: upstream keeps the per-line body inline in
// `parse_pi_format_file_inner`. It is lifted into `parse_pi_line`, with the
// context it reads and the state it carries between lines made explicit, so
// the resumable reader below can stop at a byte offset and later continue
// from exactly the parser state that offset had.

/// Inputs that stay fixed for every line of one parse.
struct PiLineContext<'a> {
    path: &'a Path,
    client: &'a str,
    fallback_provider: &'static str,
    dedup_namespace: Option<&'a str>,
    options: PiParseOptions,
}

/// What earlier lines of a transcript tell the parser about later ones: the
/// session header and the most recent `session_info` name.
#[derive(Clone, Debug, Default)]
struct PiParseState {
    session_id: Option<String>,
    workspace_key: Option<String>,
    workspace_label: Option<String>,
    agent: Option<String>,
    is_rlm_subagent: bool,
    #[cfg(test)]
    materialized_extension_bytes: usize,
}

/// Parses one non-empty line. `Break` means the transcript's header was
/// rejected and the whole transcript must be discarded.
///
/// `skipped_lines` is incremented for a line that is dropped on its own while
/// reading continues: bytes that are not valid UTF-8, text that is not JSON,
/// or a `message` record whose fields do not decode (a negative counter, a
/// field of the wrong type). A readable record of another type is not a
/// skipped line, even when this parser ignores it.
fn parse_pi_line(
    context: &PiLineContext<'_>,
    state: &mut PiParseState,
    buffer: &mut Vec<u8>,
    observer: &mut impl PiFormatObserver,
    messages: &mut Vec<PiUsageRecord>,
    skipped_lines: &mut u64,
    line: JsonLineBytes<'_>,
) -> ControlFlow<()> {
    let options = context.options;
    // Pi, Senpi and Kimchi keep the byte-strict record skipping of
    // `BufRead::lines()`: a record whose bytes are not valid UTF-8 is
    // dropped rather than read through its replacement characters.
    // Reading it lossily would make them emit messages they do not emit
    // today, which is the opt-in `PiParseOptions::standard` defers.
    if !options.lossy_line_reader && !line.valid_utf8 {
        *skipped_lines += 1;
        return ControlFlow::Continue(());
    }
    let trimmed = line.trimmed;

    if state.session_id.is_none() {
        let entry_type = match parse_json_line::<PiEntryTypeProbe>(trimmed, buffer) {
            Some(probe) => probe.entry_type,
            None if options.lossy_line_reader && pre_header_line_is_skippable(trimmed, None) => {
                *skipped_lines += 1;
                return ControlFlow::Continue(());
            }
            None => return ControlFlow::Break(()),
        };

        if entry_type != "session" {
            if PRE_SESSION_METADATA_TYPES.contains(&entry_type.as_str()) {
                return ControlFlow::Continue(());
            }
            return ControlFlow::Break(());
        }

        let Some(header) = parse_json_line::<PiSessionHeader>(trimmed, buffer) else {
            return ControlFlow::Break(());
        };
        if options.lossy_line_reader
            && (header.has_invalid_lineage() || raw_json_has_damaged_lineage_header_key(line.bytes))
        {
            return ControlFlow::Break(());
        }

        observer.observe_header(&header);
        let clean_cwd = header
            .cwd
            .as_deref()
            .filter(|cwd| accepts_replacement_field(cwd, options.lossy_line_reader));
        state.session_id = Some(
            if !accepts_replacement_field(&header.id, options.lossy_line_reader) {
                damaged_session_placeholder(context.path)
            } else {
                header.id.clone()
            },
        );
        state.workspace_key = clean_cwd.and_then(normalize_workspace_key);
        state.workspace_label = state
            .workspace_key
            .as_deref()
            .and_then(workspace_label_from_key);
        state.is_rlm_subagent = header.rlm_depth.unwrap_or(0) > 0;
        return ControlFlow::Continue(());
    }

    let entry = if options.retain_extra_fields {
        parse_json_line::<PiSessionEntry>(trimmed, buffer)
    } else {
        parse_json_line::<MapOnly<IngestPiEntry>>(trimmed, buffer).map(Into::into)
    };
    let Some(entry) = entry else {
        // ASTROCYTE DIVERGENCE: upstream drops an unreadable line without a
        // trace. It is counted here, as the Claude Code reader counts its
        // own, so a torn write shows up instead of a record vanishing.
        match parse_json_line::<PiEntryTypeProbe>(trimmed, buffer) {
            Some(probe) if probe.entry_type != "message" => {}
            _ => *skipped_lines += 1,
        }
        return ControlFlow::Continue(());
    };

    #[cfg(test)]
    {
        let string_bytes = |fields: &BTreeMap<String, serde_json::Value>| {
            fields
                .values()
                .filter_map(|value| value.as_str())
                .map(str::len)
                .sum::<usize>()
        };
        state.materialized_extension_bytes += string_bytes(&entry.extra_fields)
            + entry
                .message
                .as_ref()
                .and_then(|message| message.usage.as_ref())
                .map_or(0, |usage| string_bytes(&usage.extra_fields));
    }

    if entry.entry_type == "session_info" {
        state.agent = if options.rlm_session_name_as_agent && state.is_rlm_subagent {
            entry
                .name
                .as_ref()
                .filter(|name| {
                    !name.trim().is_empty()
                        && accepts_replacement_field(name, options.lossy_line_reader)
                })
                .cloned()
        } else {
            entry
                .name
                .as_deref()
                .filter(|name| accepts_replacement_field(name, options.lossy_line_reader))
                .and_then(pi_subagent_name)
        };
        observer.observe_entry(&entry, None);
        return ControlFlow::Continue(());
    }

    let Some(PiEmittedRecord {
        message,
        usage,
        recorded_model,
    }) = pi_emitted_record(&entry, options, state.is_rlm_subagent)
    else {
        observer.observe_entry(&entry, None);
        return ControlFlow::Continue(());
    };

    let model = if !accepts_replacement_field(recorded_model, options.lossy_line_reader) {
        "unknown"
    } else {
        recorded_model
    };

    // A missing/blank provider field is recoverable: infer it from the
    // model name (e.g. a Pi "gpt-5" message with no provider maps to
    // "openai"), falling back to "pi" only when inference can't
    // identify the model, rather than dropping a message that carries
    // valid tokens.
    let provider = match message.provider.as_deref() {
        Some(provider)
            if !provider.is_empty()
                && accepts_replacement_field(provider, options.lossy_line_reader) =>
        {
            provider.to_string()
        }
        _ => inferred_provider_from_model(model)
            .unwrap_or(context.fallback_provider)
            .to_string(),
    };

    // The entry's own `timestamp` is the only time read, as upstream: Pi's
    // `message.timestamp` is not deserialized, so when both are present the
    // entry's wins and when only the message's is present the record is
    // undated.
    let recorded_timestamp = entry
        .timestamp
        .as_deref()
        .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.timestamp_millis());
    // ASTROCYTE DIVERGENCE: upstream substitutes the file's mtime for an
    // absent or unparseable timestamp. mtime is when the file was last
    // written, not when the call happened, and it moves on every append, so a
    // full re-read would give the same record a different time and the ledger
    // would reject it as a changed redelivery. The record stays undated
    // instead; `UnifiedMessage` needs a number, and 0 is a fixed one that
    // no re-read can change.
    let timestamp = recorded_timestamp.unwrap_or(0);

    let normalized_usage = usage.to_breakdown();
    let mut unified = UnifiedMessage::new_with_agent(
        context.client,
        model,
        provider.as_str(),
        state
            .session_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        timestamp,
        normalized_usage.tokens,
        0.0,
        state.agent.clone(),
    );
    unified.output_reasoning_conflicted = normalized_usage.output_reasoning_conflicted;
    if let Some(namespace) = context.dedup_namespace {
        let clean_response_key = message
            .response_id
            .as_deref()
            .filter(|id| {
                !id.trim().is_empty() && accepts_replacement_field(id, options.lossy_line_reader)
            })
            .map(|id| format!("{namespace}:response:{id}"));
        let clean_message_key = entry.id.as_deref().filter(|id| {
            !id.trim().is_empty() && accepts_replacement_field(id, options.lossy_line_reader)
        });
        unified.dedup_key = clean_response_key.or_else(|| {
                clean_message_key.map(|id| {
                    let stable_timestamp = recorded_timestamp
                        .map(|timestamp| timestamp.to_string())
                        .unwrap_or_else(|| "missing".to_string());
                    // ASTROCYTE DIVERGENCE: optional bucket spelling keeps an
                    // absent measurement distinct from a reported zero in the
                    // fallback identity as well as in the output record.
                    format!(
                        "{namespace}:message:{id}:{stable_timestamp}:{provider}:{model}:{:?}:{:?}:{:?}:{:?}",
                        unified.tokens.input,
                        unified.tokens.output,
                        unified.tokens.cache_read,
                        unified.tokens.cache_write,
                    )
                })
            });
        if unified.dedup_key.is_none() && options.lossy_line_reader {
            let has_damaged_id = entry
                .id
                .as_deref()
                .is_some_and(|id| !accepts_replacement_field(id, options.lossy_line_reader))
                || message
                    .response_id
                    .as_deref()
                    .is_some_and(|id| !accepts_replacement_field(id, options.lossy_line_reader));
            if has_damaged_id {
                unified.dedup_key = Some(damaged_cross_session_dedup_key(namespace, line.bytes));
            }
        }
    }
    unified.set_workspace(state.workspace_key.clone(), state.workspace_label.clone());
    observer.observe_entry(&entry, Some(&unified));
    messages.push(PiUsageRecord {
        source_offset: 0,
        message: unified,
        occurred_at_ms: recorded_timestamp,
    });
    ControlFlow::Continue(())
}

// ASTROCYTE DIVERGENCE: tokscale has no resumable Pi parse. The resume check
// descends from tokscale's Codex incremental cache (`codex_prefix_matches` in
// `message_cache.rs`): a SHA-256 over consumed bytes, compared on the next
// read, and a consumed offset that always ends on a newline. Upstream pays
// the full-prefix hash on every read; astrocyte pays it only on a cadence
// (see `PiPrefixCheck`) and re-hashes a bounded trailing window otherwise.

/// Bytes of the already-consumed prefix that a [`PiPrefixCheck::Window`]
/// resume re-hashes: the window ending at the stored offset. 64 KiB is one
/// pread and a few microseconds of SHA-256, so a drain tick on a ~300 MB
/// session file hashes ~0.02% of the bytes the full check would, while still
/// catching the rewrite shapes append-only logs actually suffer —
/// truncate-and-rewrite and tail edits — on the very next tick.
pub const PI_PREFIX_WINDOW_BYTES: u64 = 64 * 1024;

/// How much of a file's already-consumed prefix to re-hash before trusting a
/// resume point. The caller picks per drain: `Window` on ordinary ticks,
/// `Full` on the periodic verification cadence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PiPrefixCheck {
    /// Re-hash only the trailing [`PI_PREFIX_WINDOW_BYTES`] of the consumed
    /// prefix. Catches truncate-and-rewrite and tail edits immediately; a
    /// rewrite of older middle bytes escapes until the next `Full` check.
    Window,
    /// Re-hash every consumed byte. This is the check upstream performs on
    /// every read; here it runs on a cadence, not per tick.
    Full,
}

/// Where a previous [`parse_pi_file_resumable`] stopped: the byte offset just
/// past the last complete line it read, the running SHA-256 of every byte
/// before that offset (finalized lazily by the periodic full check), a
/// SHA-256 of the trailing hash window, the file's inode, and the parser
/// state those bytes produced.
#[derive(Clone)]
pub struct PiResumePoint {
    offset: u64,
    prefix_hasher: Sha256,
    window_start: u64,
    window_sha256: [u8; 32],
    inode: Option<(u64, u64)>,
    state: PiParseState,
}

impl std::fmt::Debug for PiResumePoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PiResumePoint")
            .field("offset", &self.offset)
            .field("window_start", &self.window_start)
            .field("inode", &self.inode)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl PiResumePoint {
    /// Bytes of the file already consumed; always just past a newline.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

/// Result of one [`parse_pi_file_resumable`] call.
#[derive(Debug, Default)]
pub struct PiResumableParse {
    pub set_aside: Vec<(u64, String)>,
    /// Records from the lines this call parsed. After a resume these are the
    /// appended records only; the consumed prefix is not parsed again.
    pub messages: Vec<PiUsageRecord>,
    /// True when the previous point's prefix was byte-identical and parsing
    /// started at its offset; false for a full read from the start.
    pub resumed: bool,
    /// Where the next call may resume, or `None` when the file could not be
    /// read or its header was rejected, so the next call reads it in full.
    pub resume: Option<PiResumePoint>,
    /// Bytes this call fed to SHA-256: prefix verification, per-line digests,
    /// and window anchoring. A steady-state resume of a grown file is
    /// O(window + appended), never O(file); callers and tests assert that
    /// bound here instead of measuring wall time. A resume that finds nothing
    /// new hashes only the verification window: the stored window digest is
    /// still correct, so it is kept rather than recomputed.
    pub hashed_bytes: u64,
    /// Bytes read past the starting offset, including an unterminated final
    /// line. Zero for a resume of a file that has not grown.
    pub read_bytes: u64,
    /// Complete (newline-terminated) lines this call consumed that could not
    /// be read: not UTF-8, not JSON, or a `message` record whose fields do
    /// not decode. Reading continues past them. An unterminated final line is
    /// an in-flight write, never consumed and never counted here.
    pub skipped_lines: u64,
}

/// Parses a Pi session file, continuing from `previous` when the file has only
/// grown since that point.
///
/// Pi transcripts are append-only JSONL. Before the stored offset is trusted,
/// the already-consumed prefix is re-verified according to `check`:
/// [`PiPrefixCheck::Window`] re-hashes the trailing [`PI_PREFIX_WINDOW_BYTES`]
/// ending at the offset, [`PiPrefixCheck::Full`] every consumed byte. Two
/// cheaper gates run first: a truncated file (size below the stored offset)
/// and an inode change (an atomic replace, which a trailing-content check
/// alone cannot distinguish from an untouched file) both force a full
/// re-read, as does any digest mismatch. A full re-read is safe for ingest
/// because event identity is derived from content.
///
/// Only newline-terminated lines are consumed. A trailing line without its
/// newline, as a writer mid-append can leave, is still parsed (a whole record
/// written without its newline yields its message, as the full parser does),
/// but the offset and digests stop before it and the parser state it would
/// produce is discarded, so the next call reads that line again.
pub fn parse_pi_file_resumable(
    path: &Path,
    previous: Option<&PiResumePoint>,
    check: PiPrefixCheck,
) -> PiResumableParse {
    use std::io::{BufRead, BufReader, Seek, SeekFrom};

    let Ok(mut file) = std::fs::File::open(path) else {
        return PiResumableParse::default();
    };

    let mut hasher = Sha256::new();
    let mut hashed_bytes = 0_u64;
    let mut resumed = None;
    if let Some(previous) = previous {
        let metadata = file.metadata().ok();
        let long_enough = metadata
            .as_ref()
            .is_some_and(|metadata| metadata.len() >= previous.offset);
        let same_inode = same_identity(
            metadata
                .as_ref()
                .and_then(|metadata| inode_of(&file, metadata)),
            previous.inode,
        );
        let prefix_matches = long_enough
            && same_inode
            && match check {
                PiPrefixCheck::Window => hash_file_range(
                    &mut file,
                    previous.window_start,
                    previous.offset - previous.window_start,
                    &mut hashed_bytes,
                )
                .is_some_and(|digest| digest == previous.window_sha256),
                PiPrefixCheck::Full => {
                    hash_file_range(&mut file, 0, previous.offset, &mut hashed_bytes).is_some_and(
                        |digest| {
                            digest == <[u8; 32]>::from(previous.prefix_hasher.clone().finalize())
                        },
                    )
                }
            };
        if prefix_matches {
            hasher = previous.prefix_hasher.clone();
            resumed = Some(previous);
        }
    }
    let (start, mut state) = match resumed {
        Some(previous) => (previous.offset, previous.state.clone()),
        None => {
            if file.seek(SeekFrom::Start(0)).is_err() {
                return PiResumableParse::default();
            }
            (0, PiParseState::default())
        }
    };

    let context = PiLineContext {
        path,
        client: "pi",
        fallback_provider: "pi",
        dedup_namespace: Some("pi"),
        options: PiParseOptions {
            retain_extra_fields: false,
            ..PiParseOptions::standard_deduped()
        },
    };
    let mut observer = NoopPiFormatObserver;
    let mut messages = Vec::new();
    let mut buffer = Vec::with_capacity(4096);
    let mut reader = BufReader::new(file);
    let mut raw = Vec::new();
    let mut consumed = start;
    let mut read_bytes = 0_u64;
    let mut skipped_lines = 0_u64;
    let mut set_aside = Vec::new();
    let bom = "\u{feff}".as_bytes();
    loop {
        raw.clear();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) => break,
            Ok(_) => {}
            // Stop at the last complete line read; the next call resumes there.
            Err(_) => break,
        }
        read_bytes += raw.len() as u64;
        let terminated = raw.last() == Some(&b'\n');
        let mut payload: &[u8] = &raw;
        if terminated {
            payload = &payload[..payload.len() - 1];
            if payload.last() == Some(&b'\r') {
                payload = &payload[..payload.len() - 1];
            }
        }
        if consumed == 0 && payload.starts_with(bom) {
            payload = &payload[bom.len()..];
        }
        let text = String::from_utf8_lossy(payload);
        let trimmed = text.trim();

        if !terminated {
            // A partial final line is parsed against a copy of the state, so
            // nothing it implies (a header, a session name) is committed.
            // Nor is a failure to read it a skipped line: it is usually a
            // writer mid-append, and the next call reads it again whole.
            if !trimmed.is_empty() {
                let previous_len = messages.len();
                let mut scratch = state.clone();
                let mut uncounted = 0_u64;
                let _ = parse_pi_line(
                    &context,
                    &mut scratch,
                    &mut buffer,
                    &mut observer,
                    &mut messages,
                    &mut uncounted,
                    JsonLineBytes {
                        bytes: payload,
                        trimmed,
                        valid_utf8: matches!(text, std::borrow::Cow::Borrowed(_)),
                    },
                );
                for record in &mut messages[previous_len..] {
                    record.source_offset = consumed;
                }
            }
            break;
        }

        if !trimmed.is_empty() {
            let previous_len = messages.len();
            let skipped = skipped_lines;
            let flow = parse_pi_line(
                &context,
                &mut state,
                &mut buffer,
                &mut observer,
                &mut messages,
                &mut skipped_lines,
                JsonLineBytes {
                    bytes: payload,
                    trimmed,
                    valid_utf8: matches!(text, std::borrow::Cow::Borrowed(_)),
                },
            );
            for record in &mut messages[previous_len..] {
                record.source_offset = consumed;
            }
            if skipped_lines > skipped {
                set_aside.push((consumed, "Pi record could not be decoded".into()));
            }
            if flow.is_break() {
                // A rejected header discards the whole transcript, exactly as
                // the full parser does; no resume point, so it is read again.
                return PiResumableParse {
                    set_aside,
                    messages: Vec::new(),
                    resumed: resumed.is_some(),
                    resume: None,
                    hashed_bytes,
                    read_bytes,
                    skipped_lines,
                };
            }
        }
        hasher.update(&raw);
        hashed_bytes += raw.len() as u64;
        consumed += raw.len() as u64;
    }

    let mut file = reader.into_inner();
    // A resume that consumed nothing ends where the previous point did, and
    // the window it just verified (or the whole prefix, which contains it) is
    // unchanged, so the stored window digest is kept. Recomputing it would
    // hash the same bytes twice on every tick of every quiet file.
    let unchanged_window = resumed
        .filter(|previous| previous.offset == consumed)
        .map(|previous| (previous.window_start, previous.window_sha256));
    let window_start = consumed.saturating_sub(PI_PREFIX_WINDOW_BYTES);
    let Some((window_start, window_sha256)) = unchanged_window.or_else(|| {
        hash_file_range(
            &mut file,
            window_start,
            consumed - window_start,
            &mut hashed_bytes,
        )
        .map(|digest| (window_start, digest))
    }) else {
        // The tail that would anchor the next call's window check is
        // unreadable. The messages already parsed remain valid; without a
        // resume point the next call reads the file in full.
        return PiResumableParse {
            set_aside,
            messages,
            resumed: resumed.is_some(),
            resume: None,
            hashed_bytes,
            read_bytes,
            skipped_lines,
        };
    };
    let inode = file
        .metadata()
        .ok()
        .as_ref()
        .and_then(|metadata| inode_of(&file, metadata));

    PiResumableParse {
        set_aside,
        messages,
        resumed: resumed.is_some(),
        resume: Some(PiResumePoint {
            offset: consumed,
            prefix_hasher: hasher,
            window_start,
            window_sha256,
            inode,
            state,
        }),
        hashed_bytes,
        read_bytes,
        skipped_lines,
    }
}

// The fixed resume window and fixture sizes fit usize on supported targets.
#[allow(clippy::cast_possible_truncation)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    fn resume_fixture_line(entry: &str, input: u64) -> String {
        format!(
            "{{\"type\":\"message\",\"id\":\"{entry}\",\"timestamp\":\"2026-09-01T00:00:0{input}Z\",\"message\":{{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-sonnet-4\",\"usage\":{{\"input\":{input},\"output\":5}}}}}}\n"
        )
    }

    #[test]
    fn production_decode_does_not_materialize_extension_strings() {
        let extension = "x".repeat(10_000);
        let mut contents = "{\"type\":\"session\",\"id\":\"s\"}\n".to_string();
        for index in 0..1_000 {
            contents.push_str(&format!(
                "{{\"type\":\"message\",\"id\":\"{index}\",\"timestamp\":\"2026-09-01T00:00:00Z\",\"future\":\"{extension}\",\"message\":{{\"role\":\"assistant\",\"model\":\"gpt-5\",\"usage\":{{\"input\":1,\"output\":2,\"future\":\"{extension}\"}}}}}}\n"
            ));
        }
        let file = create_test_file(&contents);
        let parse = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Full);
        assert_eq!(parse.messages.len(), 1_000);
        assert_eq!(parse.skipped_lines, 0);
        let reference = parse_pi_file(file.path());
        assert_eq!(reference.len(), 1_000);
        for (record, expected) in parse.messages.iter().zip(reference) {
            assert_eq!(record.message, expected);
        }
        let materialized = parse.resume.unwrap().state.materialized_extension_bytes;
        eprintln!("1,000 records, two 10,000-byte extensions each: {materialized} materialized extension bytes");
        assert_eq!(materialized, 0, "ingest must skip unused extension values");
    }

    #[test]
    fn ingest_and_reference_decoders_keep_known_field_validation_equivalent() {
        let valid = r#"{"type":"message","id":"m","timestamp":"2026-09-01T00:00:00Z","message":{"role":"assistant","provider":"custom","model":"gpt-5","responseId":"r","usage":{"input":3,"output":10,"cacheRead":0,"cacheWrite":2,"reasoning":4,"totalTokens":15}}}"#;
        let cases = [
            (valid.to_string(), 1, 0),
            (
                valid.replace("\"totalTokens\":15", "\"totalTokens\":-1"),
                0,
                1,
            ),
            (valid.replace("\"input\":3", "\"input\":1.5"), 0, 1),
            (valid.replace("\"input\":3", "\"input\":\"3\""), 0, 1),
            (
                valid.replace("\"input\":3", "\"input\":3,\"input\":4"),
                0,
                1,
            ),
            (valid.replace("\"id\":\"m\"", "\"id\":42"), 0, 1),
            (
                valid.replace("\"responseId\":\"r\"", "\"responseId\":42"),
                0,
                1,
            ),
            (
                valid.replace("\"id\":\"m\"", "\"id\":\"m\",\"parentId\":42"),
                0,
                1,
            ),
            (
                valid.replace("\"id\":\"m\"", "\"id\":\"m\",\"childUsage\":{\"input\":-1}"),
                0,
                1,
            ),
            (
                valid.replace(
                    "\"id\":\"m\"",
                    "\"id\":\"m\",\"aggregateUsage\":{\"input\":-1}",
                ),
                0,
                1,
            ),
            (
                valid.replace(
                    "\"input\":3",
                    "\"in�put\":3,\"future\":{\"array\":[null,1,\"x\"]}",
                ),
                1,
                0,
            ),
            (
                valid.replace("\"input\":3", "\"future\":1,\"future\":2,\"input\":3"),
                1,
                0,
            ),
        ];
        for (record, expected_messages, expected_skips) in cases {
            let file = create_test_file(&format!(
                "{{\"type\":\"session\",\"id\":\"s\"}}\n{record}\n"
            ));
            let parsed = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Window);
            assert_eq!(parsed.messages.len(), expected_messages, "{record}");
            assert_eq!(parsed.skipped_lines, expected_skips, "{record}");
            assert_eq!(
                parsed
                    .messages
                    .into_iter()
                    .map(|record| record.message)
                    .collect::<Vec<_>>(),
                parse_pi_file(file.path()),
                "{record}"
            );
        }
    }

    #[test]
    fn ingest_skips_array_shaped_entries_and_usage_like_the_reference_decoder() {
        let object_usage = "{\"input\":1,\"output\":2}";
        let line = |id: &str, usage: &str| {
            format!(
                "{{\"type\":\"message\",\"id\":\"{id}\",\"timestamp\":\"2026-09-01T00:00:00Z\",\"message\":{{\"role\":\"assistant\",\"provider\":\"custom\",\"model\":\"gpt-5\",\"responseId\":\"{id}\",\"usage\":{usage}}}}}"
            )
        };
        let usage_array = line("r1", "[3,6,0,0,9,0]");
        // Serde reads a struct from an array only when it holds one element
        // per field, so these arrays are full length to reach that path.
        let entry_array = "[\"message\",\"r2\",null,\"2026-09-01T00:00:00Z\",{\"role\":\"assistant\",\"provider\":\"custom\",\"model\":\"gpt-5\",\"responseId\":\"r2\",\"usage\":{\"input\":1,\"output\":2}},null,null,null,null]";
        let object = line("r3", "{\"input\":5,\"output\":6}");
        // The upstream `PiMessage` has no flattened map, so the reference
        // decoder reads an array-shaped message positionally; ingest must too.
        let message_array = "{\"type\":\"message\",\"id\":\"r4\",\"timestamp\":\"2026-09-01T00:00:00Z\",\"message\":[\"assistant\",{\"input\":7,\"output\":8},\"gpt-5\",\"custom\",\"r4\"]}";
        let aggregate_array = line("r5", object_usage).replacen(
            "\"id\":\"r5\"",
            "\"id\":\"r5\",\"aggregateUsage\":[1,2,0,0,3,0]",
            1,
        );
        let file = create_test_file(&format!(
            "{{\"type\":\"session\",\"id\":\"s\"}}\n{usage_array}\n{entry_array}\n{object}\n{message_array}\n{aggregate_array}\n"
        ));

        let parsed = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Full);
        let reference = parse_pi_file(file.path());
        let summary = |messages: &[UnifiedMessage]| {
            messages
                .iter()
                .map(|message| {
                    (
                        message.dedup_key.clone(),
                        message.tokens.input,
                        message.tokens.output,
                    )
                })
                .collect::<Vec<_>>()
        };
        let ingested = parsed
            .messages
            .into_iter()
            .map(|record| record.message)
            .collect::<Vec<_>>();
        assert_eq!(
            summary(&ingested),
            summary(&reference),
            "ingest decoder diverges from the reference decoder"
        );
        assert_eq!(ingested, reference);
        assert_eq!(
            ingested
                .iter()
                .map(|message| message.dedup_key.as_deref())
                .collect::<Vec<_>>(),
            [Some("pi:response:r3"), Some("pi:response:r4")]
        );
        assert_eq!(
            parsed.skipped_lines, 3,
            "array-shaped entry, usage and aggregateUsage lines are skipped and counted"
        );
    }

    #[test]
    fn unknown_identity_reparses_the_consumed_messages() {
        let content = format!(
            "{{\"type\":\"session\",\"id\":\"s-1\"}}\n{}",
            resume_fixture_line("a", 1)
        );
        let file = create_test_file(&content);
        let mut point = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Window)
            .resume
            .unwrap();
        point.inode = None;
        let reread = parse_pi_file_resumable(file.path(), Some(&point), PiPrefixCheck::Window);
        assert!(!reread.resumed);
        assert_eq!(reread.messages.len(), 1);
        assert_eq!(reread.read_bytes, content.len() as u64);
    }

    /// Reading a file in appended pieces, resuming each time, yields exactly
    /// the messages one full parse yields, including header-derived fields
    /// carried across the resume point in the parser state.
    #[test]
    fn resumed_pieces_parse_to_the_same_messages_as_one_full_parse() {
        let pieces = [
            "{\"type\":\"title\",\"title\":\"t\"}\n{\"type\":\"session\",\"id\":\"s-1\",\"cwd\":\"/tmp/project\"}\n".to_string(),
            resume_fixture_line("a", 1),
            format!(
                "{{\"type\":\"session_info\",\"name\":\"agent-x\"}}\n{}",
                resume_fixture_line("b", 2)
            ),
            resume_fixture_line("c", 3),
        ];
        let mut file = NamedTempFile::new().unwrap();
        let mut resumed = Vec::new();
        let mut point: Option<PiResumePoint> = None;
        for (index, piece) in pieces.iter().enumerate() {
            file.write_all(piece.as_bytes()).unwrap();
            file.flush().unwrap();
            let parse = parse_pi_file_resumable(file.path(), point.as_ref(), PiPrefixCheck::Window);
            assert_eq!(parse.resumed, index > 0);
            resumed.extend(parse.messages.into_iter().map(|record| record.message));
            point = parse.resume;
        }
        let full = parse_pi_file(file.path());
        assert_eq!(full.len(), 3);
        assert_eq!(format!("{resumed:?}"), format!("{full:?}"));
        assert_eq!(
            point.unwrap().offset(),
            std::fs::metadata(file.path()).unwrap().len()
        );
    }

    /// The file fits inside the trailing hash window, so the window check and
    /// the full check must reject the same rewritten prefix and the same
    /// shrunk file.
    #[test]
    fn resume_point_rejects_a_rewritten_prefix_and_a_shrunk_file() {
        let mut file = NamedTempFile::new().unwrap();
        let header = "{\"type\":\"session\",\"id\":\"s-1\"}\n";
        write!(file, "{header}{}", resume_fixture_line("a", 1)).unwrap();
        file.flush().unwrap();
        let point = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Window)
            .resume
            .unwrap();

        // Same length, different byte inside the consumed prefix.
        std::fs::write(
            file.path(),
            format!(
                "{header}{}{}",
                resume_fixture_line("z", 1),
                resume_fixture_line("b", 2)
            ),
        )
        .unwrap();
        for check in [PiPrefixCheck::Window, PiPrefixCheck::Full] {
            let rewritten = parse_pi_file_resumable(file.path(), Some(&point), check);
            assert!(!rewritten.resumed, "{check:?} must reject the rewrite");
            assert_eq!(rewritten.messages.len(), 2);
        }

        std::fs::write(file.path(), header).unwrap();
        for check in [PiPrefixCheck::Window, PiPrefixCheck::Full] {
            let shrunk = parse_pi_file_resumable(file.path(), Some(&point), check);
            assert!(!shrunk.resumed, "{check:?} must reject the shrunk file");
            assert!(shrunk.messages.is_empty());
        }
    }

    /// With a file larger than the hash window, a rewrite older than the
    /// window escapes the per-tick check and is caught by the periodic full
    /// check. This pair is the guarantee the cadence moves rather than drops:
    /// neither check alone shows both directions.
    #[test]
    fn window_check_misses_a_middle_rewrite_that_the_full_check_catches() {
        let mut file = NamedTempFile::new().unwrap();
        let header = "{\"type\":\"session\",\"id\":\"s-1\"}\n";
        write!(file, "{header}").unwrap();
        let mut padding = String::new();
        while padding.len() < PI_PREFIX_WINDOW_BYTES as usize + 4096 {
            padding.push_str("{\"type\":\"session_info\",\"name\":\"pad\"}\n");
        }
        write!(file, "{padding}{}", resume_fixture_line("a", 1)).unwrap();
        file.flush().unwrap();
        let point = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Window)
            .resume
            .unwrap();

        // Same-length rewrite well before the window, then an ordinary append.
        let mut bytes = std::fs::read(file.path()).unwrap();
        bytes[header.len() + 30] = b'X';
        bytes.extend_from_slice(resume_fixture_line("b", 2).as_bytes());
        std::fs::write(file.path(), &bytes).unwrap();

        let windowed = parse_pi_file_resumable(file.path(), Some(&point), PiPrefixCheck::Window);
        assert!(
            windowed.resumed,
            "a middle rewrite outside the window escapes the per-tick check"
        );
        assert_eq!(windowed.messages.len(), 1, "only the appended record");
        let full = parse_pi_file_resumable(file.path(), Some(&point), PiPrefixCheck::Full);
        assert!(!full.resumed, "the periodic full check catches it");
    }

    fn parse_prime_test_file(path: &Path) -> Vec<UnifiedMessage> {
        let mut observer = NoopPiFormatObserver;
        parse_pi_format_rlm_file_with_observer(path, "prime-agent", "prime-agent", &mut observer)
    }

    #[test]
    fn prime_rejects_replacement_mangled_usage_keys() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"{\"type\":\"session\",\"id\":\"root\",\"cwd\":\"/tmp/project\"}\n")
            .unwrap();
        file.write_all(
            b"{\"type\":\"message\",\"id\":\"damaged-byte\",\"timestamp\":\"2026-08-08T00:00:01Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"in\xffput\":100,\"output\":5}}}\n",
        )
        .unwrap();
        file.write_all(
            "{\"type\":\"message\",\"id\":\"damaged-unicode\",\"timestamp\":\"2026-08-08T00:00:02Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"out�put\":7,\"input\":10}}}\n"
                .as_bytes(),
        )
        .unwrap();
        file.write_all(
            b"{\"type\":\"message\",\"id\":\"clean\",\"timestamp\":\"2026-08-08T00:00:03Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":20,\"output\":8}}}\n",
        )
        .unwrap();
        file.flush().unwrap();

        let messages = parse_prime_test_file(file.path());

        assert_eq!(messages.len(), 1);
        assert!(messages[0]
            .dedup_key
            .as_deref()
            .is_some_and(|key| key.starts_with("prime-agent:message:clean:")));
        assert_eq!(messages[0].tokens.total(), 28);
    }

    #[test]
    fn damaged_usage_extension_keys_do_not_hide_known_counters() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"{\"type\":\"session\",\"id\":\"root\"}\n")
            .unwrap();
        file.write_all(
            b"{\"type\":\"message\",\"id\":\"kept\",\"timestamp\":\"2026-08-08T00:00:01Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":20,\"output\":8,\"future-\xff-field\":999}}}\n",
        )
        .unwrap();
        file.flush().unwrap();

        let messages = parse_prime_test_file(file.path());
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].tokens.total(), 28);
    }

    #[test]
    fn standard_pi_skips_invalid_utf8_line_and_reads_later_records() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"{\"type\":\"session\",\"id\":\"root\"}\n")
            .unwrap();
        file.write_all(b"invalid \xff record\n").unwrap();
        file.write_all(
            b"{\"type\":\"message\",\"id\":\"later\",\"timestamp\":\"2026-08-08T00:00:01Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":20,\"output\":8}}}\n",
        )
        .unwrap();
        file.flush().unwrap();

        let messages = parse_pi_file(file.path());
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].tokens.total(), 28);
    }

    #[test]
    fn utf8_bom_before_the_header_keeps_the_transcript() {
        // A BOM decodes cleanly, so `str::trim` leaves U+FEFF glued to the
        // front of the header (it is not White_Space) and the header fails to
        // parse. That failure drops the entire transcript, not one record, so
        // every Pi-format client has to strip it the way the lossy reader
        // already does for Prime Agent.
        let file = create_test_file(concat!(
            "\u{feff}",
            r#"{"type":"session","id":"session-with-bom","timestamp":"2026-08-08T00:00:00.000Z","cwd":"/tmp/project"}"#,
            "\n",
            r#"{"type":"message","id":"assistant-1","timestamp":"2026-08-08T00:00:01.000Z","message":{"role":"assistant","provider":"anthropic","model":"claude-opus-5","usage":{"input":20,"output":8}}}"#,
            "\n",
        ));

        // ASTROCYTE DIVERGENCE: this crate intentionally ships only the Pi
        // reader, so the shared-format Senpi and Kimchi wrapper assertions stay
        // upstream while Pi and the shared Prime-mode path remain covered here.
        for messages in [
            parse_pi_file(file.path()),
            parse_prime_test_file(file.path()),
        ] {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].session_id, "session-with-bom");
            assert_eq!(messages[0].tokens.total(), 28);
        }
    }

    #[test]
    fn a_marker_after_the_first_record_still_costs_only_its_own_record() {
        // The strip is scoped to the file's first line, where a byte-order mark
        // can actually appear. A U+FEFF anywhere else stays an ordinary
        // malformed record, which the reader already loses alone.
        let file = create_test_file(concat!(
            r#"{"type":"session","id":"session-clean","timestamp":"2026-08-08T00:00:00.000Z","cwd":"/tmp/project"}"#,
            "\n\u{feff}",
            r#"{"type":"message","id":"assistant-1","timestamp":"2026-08-08T00:00:01.000Z","message":{"role":"assistant","provider":"anthropic","model":"claude-opus-5","usage":{"input":1,"output":1}}}"#,
            "\n",
            r#"{"type":"message","id":"assistant-2","timestamp":"2026-08-08T00:00:02.000Z","message":{"role":"assistant","provider":"anthropic","model":"claude-opus-5","usage":{"input":20,"output":8}}}"#,
            "\n",
        ));

        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].session_id, "session-clean");
        assert_eq!(messages[0].tokens.total(), 28);
    }

    #[test]
    fn prime_rejects_only_matching_critical_child_timestamps() {
        for (depth, expected_ids) in [(1, vec!["clean"]), (0, vec!["damaged", "clean"])] {
            let mut file = NamedTempFile::new().unwrap();
            writeln!(
                file,
                "{{\"type\":\"session\",\"id\":\"root\",\"parentSession\":\"/tmp/parent.jsonl\",\"rlmDepth\":{depth}}}"
            )
            .unwrap();
            file.write_all(
                b"{\"type\":\"message\",\"id\":\"damaged\",\"timestamp\":\"2026-08-08T00:00:0\xffZ\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":10}}}\n",
            ).unwrap();
            file.write_all(b"{\"type\":\"message\",\"id\":\"clean\",\"timestamp\":\"2026-08-08T00:00:02Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":20}}}\n").unwrap();
            file.flush().unwrap();

            let messages = parse_prime_test_file(file.path());
            let ids = messages
                .iter()
                .map(|message| {
                    message
                        .dedup_key
                        .as_deref()
                        .unwrap()
                        .split(':')
                        .nth(2)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, expected_ids, "rlmDepth={depth}");
        }
    }

    #[test]
    fn prime_rejects_child_message_with_damaged_timestamp_key() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"{\"type\":\"session\",\"id\":\"child\",\"parentSession\":\"/tmp/parent.jsonl\",\"rlmDepth\":1}\n")
            .unwrap();
        file.write_all(b"{\"type\":\"message\",\"id\":\"damaged\",\"time\xffstamp\":\"2026-08-08T00:00:01Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":10}}}\n").unwrap();
        file.write_all(b"{\"type\":\"message\",\"id\":\"clean\",\"timestamp\":\"2026-08-08T00:00:02Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":20}}}\n").unwrap();
        file.flush().unwrap();

        let messages = parse_prime_test_file(file.path());
        assert_eq!(messages.len(), 1);
        assert!(messages[0]
            .dedup_key
            .as_deref()
            .unwrap()
            .contains(":clean:"));
    }

    #[test]
    fn damaged_prime_session_placeholders_are_unique_by_source_path() {
        let temp = tempfile::tempdir().unwrap();
        let first_dir = temp.path().join("first");
        let second_dir = temp.path().join("second");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = first_dir.join("session.jsonl");
        let second = second_dir.join("session.jsonl");
        let contents = b"{\"type\":\"session\",\"id\":\"root-\xff\",\"cwd\":\"/tmp/project\"}\n{\"type\":\"message\",\"id\":\"clean\",\"timestamp\":\"2026-08-08T00:00:03Z\",\"message\":{\"role\":\"assistant\",\"provider\":\"anthropic\",\"model\":\"claude-opus-5\",\"usage\":{\"input\":20,\"output\":8}}}\n";
        std::fs::write(&first, contents).unwrap();
        std::fs::write(&second, contents).unwrap();

        let first_messages = parse_prime_test_file(&first);
        let second_messages = parse_prime_test_file(&second);

        assert_eq!(first_messages.len(), 1);
        assert_eq!(second_messages.len(), 1);
        assert!(first_messages[0].session_id.starts_with("unknown:path:"));
        assert_ne!(first_messages[0].session_id, second_messages[0].session_id);
    }

    #[test]
    fn test_parse_pi_jsonl_valid_assistant_message() {
        // Set up the Pi session shape exercised by this test.
        let content = r#"{"type":"session","id":"pi_ses_001","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
{"type":"message","id":"msg_001","parentId":null,"timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"assistant","model":"claude-3-5-sonnet","provider":"anthropic","usage":{"input":100,"output":50,"cacheRead":10,"cacheWrite":5,"totalTokens":165}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].client, "pi");
        assert_eq!(messages[0].session_id, "pi_ses_001");
        assert_eq!(messages[0].model_id, "claude-3-5-sonnet");
        assert_eq!(messages[0].provider_id, "anthropic");
        // ASTROCYTE DIVERGENCE: reported measurements remain optional.
        assert_eq!(messages[0].tokens.input, Some(100));
        assert_eq!(messages[0].tokens.output, Some(50));
        assert_eq!(messages[0].tokens.cache_read, Some(10));
        assert_eq!(messages[0].tokens.cache_write, Some(5));
        assert_eq!(messages[0].workspace_key, Some("/tmp".to_string()));
        assert_eq!(messages[0].workspace_label, Some("tmp".to_string()));
    }

    #[test]
    fn test_parse_pi_infers_provider_from_model_when_absent() {
        // given: no "provider" key at all — a missing provider must be
        // inferred from the model name (gpt-5 -> openai), not hardcoded
        // to "pi".
        let content = r#"{"type":"session","id":"pi_ses_005","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
{"type":"message","id":"msg_001","parentId":null,"timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"assistant","model":"gpt-5","usage":{"input":100,"output":50,"cacheRead":0,"cacheWrite":0,"totalTokens":150}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].model_id, "gpt-5");
        assert_eq!(messages[0].provider_id, "openai");
    }

    #[test]
    fn test_parse_pi_infers_provider_from_model_when_blank() {
        // given: "provider" present but blank — same inference path as
        // fully absent.
        let content = r#"{"type":"session","id":"pi_ses_006","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
{"type":"message","id":"msg_001","parentId":null,"timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"assistant","model":"gpt-5","provider":"","usage":{"input":100,"output":50,"cacheRead":0,"cacheWrite":0,"totalTokens":150}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].provider_id, "openai");
    }

    #[test]
    fn test_parse_pi_falls_back_to_pi_when_provider_unrecoverable() {
        // given: no provider and a model name inference can't identify —
        // falls back to "pi" rather than dropping the message.
        let content = r#"{"type":"session","id":"pi_ses_007","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
{"type":"message","id":"msg_001","parentId":null,"timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"assistant","model":"totally-unrecognized-model-xyz","usage":{"input":100,"output":50,"cacheRead":0,"cacheWrite":0,"totalTokens":150}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].provider_id, "pi");
    }

    #[test]
    fn test_parse_pi_subagent_session_name_as_agent() {
        let content = r#"{"type":"session","id":"pi_subagent_001","timestamp":"2026-07-10T00:00:00.000Z","cwd":"/tmp"}
{"type":"session_info","id":"info_001","parentId":null,"timestamp":"2026-07-10T00:00:00.100Z","name":"subagent-go-reviewer-e2e7405c-cb84-4f0a-a6da-9d987494d130-1"}
{"type":"message","id":"msg_001","parentId":"info_001","timestamp":"2026-07-10T00:00:01.000Z","message":{"role":"assistant","model":"gpt-5","provider":"openai","usage":{"input":100,"output":50,"cacheRead":0,"cacheWrite":0,"totalTokens":150}}}"#;
        let file = create_test_file(content);

        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].agent.as_deref(), Some("go-reviewer"));
        assert_eq!(
            pi_subagent_name("subagent-context-builder-208242ce-1").as_deref(),
            Some("context-builder")
        );
        assert_eq!(pi_subagent_name("Refactor auth module"), None);
    }

    #[test]
    fn test_parse_pi_skips_non_assistant_messages() {
        // Set up the Pi session shape exercised by this test.
        let content = r#"{"type":"session","id":"pi_ses_002","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
{"type":"message","timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"user","model":"claude-3-5-sonnet","provider":"anthropic","usage":{"input":100,"output":50,"cacheRead":0,"cacheWrite":0,"totalTokens":150}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert!(messages.is_empty());
    }

    /// An undated record is absent from `parse_pi_file`, whose message type
    /// has no way to say "no time", and present, undated, on the resumable
    /// path that feeds ingest.
    #[test]
    fn undated_record_is_omitted_by_parse_pi_file_and_kept_undated_for_ingest() {
        let content = concat!(
            r#"{"type":"session","id":"s-1","cwd":"/tmp/project"}"#,
            "\n",
            r#"{"type":"message","id":"dated","timestamp":"2026-09-01T00:00:01Z","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"r-dated","usage":{"input":1,"output":2}}}"#,
            "\n",
            r#"{"type":"message","id":"undated","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"r-undated","usage":{"input":3,"output":4}}}"#,
            "\n"
        );
        let file = create_test_file(content);

        let messages = parse_pi_file(file.path());
        assert_eq!(messages.len(), 1, "only the dated record");
        assert_eq!(
            messages[0].dedup_key.as_deref(),
            Some("pi:response:r-dated")
        );

        let records = parse_pi_file_resumable(file.path(), None, PiPrefixCheck::Full).messages;
        let undated: Vec<_> = records
            .iter()
            .filter(|record| record.occurred_at_ms.is_none())
            .map(|record| record.message.dedup_key.as_deref())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(undated, vec![Some("pi:response:r-undated")]);
    }

    #[test]
    fn test_parse_pi_skips_missing_usage() {
        // Set up the Pi session shape exercised by this test.
        let content = r#"{"type":"session","id":"pi_ses_003","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
{"type":"message","timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"assistant","model":"claude-3-5-sonnet","provider":"anthropic"}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert!(messages.is_empty());
    }

    #[test]
    fn test_parse_pi_skips_malformed_json_lines() {
        // Set up the Pi session shape exercised by this test.
        let content = r#"{"type":"session","id":"pi_ses_004","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp"}
not valid json
{"type":"message","timestamp":"2026-01-01T00:00:01.000Z","message":{"role":"assistant","model":"gpt-4o-mini","provider":"openai","usage":{"input":10,"output":5,"cacheRead":0,"cacheWrite":0,"totalTokens":15}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].model_id, "gpt-4o-mini");
        assert_eq!(messages[0].provider_id, "openai");
    }

    #[test]
    fn test_parse_pi_skips_leading_title_record() {
        // given: current OMP builds write a `title` metadata record before
        // `session` (tokscale#802) — the parser must skip it, not discard
        // the whole file.
        let content = r#"{"type":"title","v":1,"title":"Comment on GitHub issue","source":"auto","updatedAt":"2026-07-02T18:08:49.723Z"}
{"type":"session","id":"pi_ses_005","timestamp":"2026-07-02T18:07:14.690Z","cwd":"/tmp"}
{"type":"message","timestamp":"2026-07-02T18:08:53.229Z","message":{"role":"assistant","model":"claude-sonnet-5","provider":"anthropic","usage":{"input":2,"output":180,"cacheRead":0,"cacheWrite":70844,"totalTokens":71026}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].session_id, "pi_ses_005");
        assert_eq!(messages[0].model_id, "claude-sonnet-5");
        assert_eq!(messages[0].provider_id, "anthropic");
        // ASTROCYTE DIVERGENCE: reported measurements remain optional.
        assert_eq!(messages[0].tokens.input, Some(2));
        assert_eq!(messages[0].tokens.output, Some(180));
        assert_eq!(messages[0].tokens.cache_write, Some(70844));
    }

    #[test]
    fn test_parse_pi_skips_multiple_leading_title_records() {
        // given: defensive against more than one pre-session metadata line
        // in a row (e.g. a title record rewritten by a later auto-rename).
        let content = r#"{"type":"title","v":1,"title":"first"}
{"type":"title","v":1,"title":"renamed"}
{"type":"session","id":"pi_ses_006","timestamp":"2026-07-02T18:07:14.690Z","cwd":"/tmp"}
{"type":"message","timestamp":"2026-07-02T18:08:53.229Z","message":{"role":"assistant","model":"gpt-4o-mini","provider":"openai","usage":{"input":10,"output":5,"cacheRead":0,"cacheWrite":0,"totalTokens":15}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].session_id, "pi_ses_006");
    }

    #[test]
    fn test_parse_pi_rejects_unknown_leading_record_type() {
        // given: an unrecognized type before `session` is still treated as
        // a malformed file rather than silently scanned through.
        let content = r#"{"type":"totally_unknown_thing","foo":"bar"}
{"type":"session","id":"pi_ses_007","timestamp":"2026-07-02T18:07:14.690Z","cwd":"/tmp"}
{"type":"message","timestamp":"2026-07-02T18:08:53.229Z","message":{"role":"assistant","model":"gpt-4o-mini","provider":"openai","usage":{"input":10,"output":5,"cacheRead":0,"cacheWrite":0,"totalTokens":15}}}"#;
        let file = create_test_file(content);

        // Parse the temporary Pi session file.
        let messages = parse_pi_file(file.path());

        assert!(messages.is_empty());
    }

    /// The record shape from the token-monitor synthetic repro
    /// (Javis603/token-monitor#627, tokscale#1306): a Pi branch copies the
    /// parent's assistant entries into a new session file verbatim —
    /// same `message.responseId`, same entry id, same usage — and only the
    /// session header differs.
    fn fork_fixture(session_id: &str, extra_message: Option<&str>) -> String {
        let copied = format!(
            r#"{{"type":"message","id":"entry-fork-copy","parentId":"{session_id}","timestamp":"2026-09-06T12:00:00.000Z","message":{{"role":"assistant","provider":"openai-codex","model":"gpt-6-astra","responseId":"resp-demo-fork","usage":{{"input":100,"output":20,"cacheRead":300,"cacheWrite":0,"totalTokens":420}}}}}}"#
        );
        let mut lines = vec![
            format!(
                r#"{{"type":"session","id":"{session_id}","timestamp":"2026-09-06T12:00:00.000Z","cwd":"/tmp/demo"}}"#
            ),
            copied,
        ];
        if let Some(message) = extra_message {
            lines.push(message.to_string());
        }
        lines.join("\n") + "\n"
    }

    #[test]
    fn fork_copies_across_session_files_share_a_response_level_key() {
        let parent = create_test_file(&fork_fixture("session-demo-1", None));
        let child = create_test_file(&fork_fixture("session-demo-2", None));

        let parent_messages = parse_pi_file(parent.path());
        let child_messages = parse_pi_file(child.path());

        assert_eq!(parent_messages.len(), 1);
        assert_eq!(child_messages.len(), 1);
        // Session-independent identity: the key must not embed the session id.
        let parent_key = parent_messages[0].dedup_key.as_deref().unwrap();
        let child_key = child_messages[0].dedup_key.as_deref().unwrap();
        assert_eq!(parent_key, child_key);
        assert_eq!(parent_key, "pi:response:resp-demo-fork");
    }

    #[test]
    fn fork_copy_plus_new_turns_only_the_prefix_collapses() {
        // given: the child file copies the parent's record and then continues
        // with a genuinely new response. Only the copied prefix must share a
        // key; the new turn must survive.
        let new_turn = r#"{"type":"message","id":"entry-child-new","parentId":"session-demo-2","timestamp":"2026-09-06T12:01:00.000Z","message":{"role":"assistant","provider":"openai-codex","model":"gpt-6-astra","responseId":"resp-demo-child-new","usage":{"input":7,"output":3,"cacheRead":0,"cacheWrite":0,"totalTokens":10}}}"#;
        let parent = create_test_file(&fork_fixture("session-demo-1", None));
        let child = create_test_file(&fork_fixture("session-demo-2", Some(new_turn)));

        let parent_messages = parse_pi_file(parent.path());
        let child_messages = parse_pi_file(child.path());

        assert_eq!(parent_messages.len(), 1);
        assert_eq!(child_messages.len(), 2);
        let child_keys: Vec<&str> = child_messages
            .iter()
            .map(|message| message.dedup_key.as_deref().unwrap())
            .collect();
        assert_eq!(child_keys[0], "pi:response:resp-demo-fork");
        assert_eq!(child_keys[1], "pi:response:resp-demo-child-new");
        assert!(child_keys[1] != child_keys[0]);
    }

    #[test]
    fn different_response_ids_stay_separate_even_with_identical_content() {
        // given: two genuinely separate calls with the same usage but
        // different response ids must not merge (regenerate/retry case).
        let file = create_test_file(concat!(
            r#"{"type":"session","id":"session-demo-1","timestamp":"2026-09-06T12:00:00.000Z","cwd":"/tmp/demo"}"#,
            "\n",
            r#"{"type":"message","id":"entry-a","parentId":"session-demo-1","timestamp":"2026-09-06T12:00:00.000Z","message":{"role":"assistant","provider":"openai-codex","model":"gpt-6-astra","responseId":"resp-A","usage":{"input":100,"output":20,"cacheRead":300,"cacheWrite":0,"totalTokens":420}}}"#,
            "\n",
            r#"{"type":"message","id":"entry-b","parentId":"session-demo-1","timestamp":"2026-09-06T12:00:01.000Z","message":{"role":"assistant","provider":"openai-codex","model":"gpt-6-astra","responseId":"resp-B","usage":{"input":100,"output":20,"cacheRead":300,"cacheWrite":0,"totalTokens":420}}}"#,
            "\n",
        ));

        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 2);
        assert_ne!(messages[0].dedup_key, messages[1].dedup_key);
    }

    #[test]
    fn missing_response_id_falls_back_to_entry_identity_across_files() {
        // given: no responseId, but the fork preserves the entry id and the
        // immutable event fields — those form the fallback key, and it must
        // still be session-independent.
        let record = r#"{"type":"message","id":"entry-no-resp","parentId":"SESSION","timestamp":"2026-09-06T12:00:00.000Z","message":{"role":"assistant","provider":"openai-codex","model":"gpt-6-astra","usage":{"input":100,"output":20,"cacheRead":300,"cacheWrite":0,"totalTokens":420}}}"#;
        let make = |session: &str| {
            format!(
                r#"{{"type":"session","id":"{session}","timestamp":"2026-09-06T12:00:00.000Z","cwd":"/tmp/demo"}}"#
            ) + "\n"
                + &record.replace("SESSION", session)
                + "\n"
        };
        let parent = create_test_file(&make("session-a"));
        let child = create_test_file(&make("session-b"));

        let parent_messages = parse_pi_file(parent.path());
        let child_messages = parse_pi_file(child.path());

        assert_eq!(parent_messages.len(), 1);
        assert_eq!(child_messages.len(), 1);
        let parent_key = parent_messages[0].dedup_key.as_deref().unwrap();
        let child_key = child_messages[0].dedup_key.as_deref().unwrap();
        assert_eq!(parent_key, child_key);
        assert!(parent_key.starts_with("pi:message:entry-no-resp:"));
    }

    fn usage_fixture_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi_usage.jsonl")
    }

    fn fixture_source_usages() -> Vec<PiUsage> {
        std::fs::read_to_string(usage_fixture_path())
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|entry| entry.get("message")?.get("usage").cloned())
            .map(|usage| serde_json::from_value(usage).unwrap())
            .collect()
    }

    #[test]
    fn reasoning_normalization_splits_pi_output_into_disjoint_buckets() {
        let messages = parse_pi_file(&usage_fixture_path());
        let normalized = &messages[0].tokens;

        assert_eq!(normalized.output, Some(70));
        assert_eq!(normalized.reasoning, Some(30));
        assert!(!messages[0].output_reasoning_conflicted);
    }

    #[test]
    fn reasoning_exceeding_output_marks_conflict_without_panicking() {
        let file = create_test_file(concat!(
            r#"{"type":"session","id":"conflicted-reasoning","timestamp":"2026-09-20T00:00:00.000Z","cwd":"/tmp"}"#,
            "\n",
            r#"{"type":"message","id":"conflicted","timestamp":"2026-09-20T00:00:01.000Z","message":{"role":"assistant","model":"claude-sonnet-4","provider":"anthropic","usage":{"input":1,"output":5,"reasoning":9,"totalTokens":6}}}"#,
            "\n",
        ));

        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].tokens.output, None);
        assert_eq!(messages[0].tokens.reasoning, Some(9));
        assert!(messages[0].output_reasoning_conflicted);
    }

    #[test]
    fn positive_reasoning_without_output_marks_conflict() {
        let file = create_test_file(concat!(
            r#"{"type":"session","id":"missing-output","timestamp":"2026-09-20T00:00:00.000Z","cwd":"/tmp"}"#,
            "\n",
            r#"{"type":"message","id":"conflicted","timestamp":"2026-09-20T00:00:01.000Z","message":{"role":"assistant","model":"claude-sonnet-4","provider":"anthropic","usage":{"input":1,"reasoning":9,"totalTokens":1}}}"#,
            "\n",
        ));

        let messages = parse_pi_file(file.path());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].tokens.output, None);
        assert_eq!(messages[0].tokens.reasoning, Some(9));
        assert!(messages[0].output_reasoning_conflicted);
    }

    #[test]
    fn reasoning_normalization_conserves_source_output_across_fixture() {
        let source = fixture_source_usages();
        let messages = parse_pi_file(&usage_fixture_path());
        assert_eq!(messages.len(), source.len());

        for (source, normalized) in source.iter().zip(&messages) {
            assert!(!normalized.output_reasoning_conflicted);
            let source_output = source.output.expect("fixture reports output");
            let disjoint_total =
                normalized.tokens.output.unwrap_or(0) + normalized.tokens.reasoning.unwrap_or(0);
            assert_eq!(
                disjoint_total, source_output,
                "normalization did not conserve source output"
            );
        }
    }

    #[test]
    fn absent_cache_write_remains_distinct_from_reported_zero() {
        let messages = parse_pi_file(&usage_fixture_path());
        let absent = &messages[1].tokens;
        let reported_zero = &messages[2].tokens;

        assert_eq!(absent.cache_write, None);
        assert_eq!(reported_zero.cache_write, Some(0));
        assert_ne!(absent, reported_zero);
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct LiveCorpusTotals {
        files: u64,
        assistant_usage_objects: u64,
        assistant_numeric_output: u64,
        input: u64,
        source_output: u64,
        normalized_output: u64,
        cache_read: u64,
        cache_write: u64,
        reasoning: u64,
        total_tokens: u64,
        positive_reasoning_records: u64,
        reasoning_inside_output_identities: u64,
        reasoning_additive_identities: u64,
        reasoning_greater_than_output: u64,
        first_timestamp: Option<String>,
        last_timestamp: Option<String>,
    }

    fn jsonl_files(root: &Path) -> Vec<std::path::PathBuf> {
        fn visit(path: &Path, files: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, files);
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "jsonl")
                {
                    files.push(path);
                }
            }
        }

        let mut files = Vec::new();
        visit(root, &mut files);
        files.sort();
        files
    }

    fn measure_live_corpus(root: &Path) -> LiveCorpusTotals {
        const SNAPSHOT_START: &str = "2026-02-28T16:16:45.881Z";
        const SNAPSHOT_END: &str = "2026-09-20T13:06:38.259Z";

        let files = jsonl_files(root);
        let mut totals = LiveCorpusTotals::default();

        for path in files {
            let mut file_has_qualifying_record = false;
            for_each_json_line_with_bytes(&path, &mut |line| {
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(line.bytes) else {
                    return ControlFlow::Continue(());
                };
                let Some(timestamp) = value.get("timestamp").and_then(serde_json::Value::as_str)
                else {
                    return ControlFlow::Continue(());
                };
                if !(SNAPSHOT_START..=SNAPSHOT_END).contains(&timestamp) {
                    return ControlFlow::Continue(());
                }
                let Some(message) = value.get("message").and_then(serde_json::Value::as_object)
                else {
                    return ControlFlow::Continue(());
                };
                if message.get("role").and_then(serde_json::Value::as_str) != Some("assistant") {
                    return ControlFlow::Continue(());
                }
                let Some(usage) = message.get("usage").and_then(serde_json::Value::as_object)
                else {
                    return ControlFlow::Continue(());
                };

                file_has_qualifying_record = true;
                totals.assistant_usage_objects += 1;
                totals.assistant_numeric_output += u64::from(
                    usage
                        .get("output")
                        .and_then(serde_json::Value::as_u64)
                        .is_some(),
                );

                let input = usage.get("input").and_then(serde_json::Value::as_u64);
                let output = usage.get("output").and_then(serde_json::Value::as_u64);
                let cache_read = usage.get("cacheRead").and_then(serde_json::Value::as_u64);
                let cache_write = usage.get("cacheWrite").and_then(serde_json::Value::as_u64);
                let reasoning = usage.get("reasoning").and_then(serde_json::Value::as_u64);
                let total_tokens = usage.get("totalTokens").and_then(serde_json::Value::as_u64);

                totals.input = totals.input.saturating_add(input.unwrap_or(0));
                totals.source_output = totals.source_output.saturating_add(output.unwrap_or(0));
                totals.cache_read = totals.cache_read.saturating_add(cache_read.unwrap_or(0));
                totals.cache_write = totals.cache_write.saturating_add(cache_write.unwrap_or(0));
                totals.reasoning = totals.reasoning.saturating_add(reasoning.unwrap_or(0));
                totals.total_tokens = totals
                    .total_tokens
                    .saturating_add(total_tokens.unwrap_or(0));

                let pi_usage: PiUsage =
                    serde_json::from_value(serde_json::Value::Object(usage.clone())).unwrap();
                let normalized = pi_usage.to_breakdown();
                totals.normalized_output = totals
                    .normalized_output
                    .saturating_add(normalized.tokens.output.unwrap_or(0));

                if let (
                    Some(input),
                    Some(output),
                    Some(cache_read),
                    Some(cache_write),
                    Some(reasoning),
                    Some(total_tokens),
                ) = (
                    input,
                    output,
                    cache_read,
                    cache_write,
                    reasoning,
                    total_tokens,
                ) {
                    if reasoning > 0 {
                        totals.positive_reasoning_records += 1;
                        totals.reasoning_greater_than_output += u64::from(reasoning > output);
                        totals.reasoning_inside_output_identities += u64::from(
                            total_tokens
                                == input
                                    .saturating_add(output)
                                    .saturating_add(cache_read)
                                    .saturating_add(cache_write),
                        );
                        totals.reasoning_additive_identities += u64::from(
                            total_tokens
                                == input
                                    .saturating_add(output)
                                    .saturating_add(cache_read)
                                    .saturating_add(cache_write)
                                    .saturating_add(reasoning),
                        );
                    }
                }

                if totals
                    .first_timestamp
                    .as_deref()
                    .is_none_or(|first| timestamp < first)
                {
                    totals.first_timestamp = Some(timestamp.to_owned());
                }
                if totals
                    .last_timestamp
                    .as_deref()
                    .is_none_or(|last| timestamp > last)
                {
                    totals.last_timestamp = Some(timestamp.to_owned());
                }

                ControlFlow::Continue(())
            });
            totals.files += u64::from(file_has_qualifying_record);
        }

        totals
    }

    #[test]
    #[ignore = "reads the mutable ~/.pi/agent/sessions corpus (about 413 MB)"]
    fn live_corpus_totals_match_established_ground_truth() {
        let root = std::env::var_os("PI_SESSION_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(std::env::var_os("HOME").unwrap())
                    .join(".pi/agent/sessions")
            });
        let actual = measure_live_corpus(&root);
        eprintln!(
            "predicate: timestamp in [2026-02-28T16:16:45.881Z, 2026-09-20T13:06:38.259Z], message.role == assistant, and message.usage is an object"
        );
        eprintln!("{actual:#?}");

        // This snapshot was measured on 2026-09-20. The explicit upper bound
        // keeps later appends from changing it; a mismatch means records inside
        // the measured window changed and must be reconciled before refreshing.
        let expected = LiveCorpusTotals {
            files: 64,
            assistant_usage_objects: 40_257,
            assistant_numeric_output: 40_257,
            input: 366_604_652,
            source_output: 11_453_885,
            normalized_output: 8_987_822,
            cache_read: 5_689_825_824,
            cache_write: 11_483_620,
            reasoning: 2_466_063,
            total_tokens: 6_080_177_555,
            positive_reasoning_records: 14_980,
            reasoning_inside_output_identities: 14_980,
            reasoning_additive_identities: 0,
            reasoning_greater_than_output: 0,
            first_timestamp: Some("2026-02-28T16:16:45.881Z".to_string()),
            last_timestamp: Some("2026-09-20T13:06:38.259Z".to_string()),
        };

        assert_eq!(actual, expected);
        assert_eq!(
            actual.normalized_output.saturating_add(actual.reasoning),
            actual.source_output,
            "disjoint output plus reasoning must conserve Pi source output"
        );
    }
}
