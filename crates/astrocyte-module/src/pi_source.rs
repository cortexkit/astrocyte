//! Pi's on-disk session producer and `UnifiedMessage` ingest mapping.

use std::collections::{BTreeMap, HashMap};
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::EventRecordRequest;
#[cfg(test)]
use astrocyte_readers::sessions::pi::{parse_pi_file, PI_PREFIX_WINDOW_BYTES};
use astrocyte_readers::sessions::pi::{
    parse_pi_file_resumable, PiPrefixCheck, PiResumePoint, PiUsageRecord,
};
use astrocyte_readers::sessions::UnifiedMessage;
use chrono::DateTime;
#[cfg(test)]
use rusqlite::params;
use serde::Deserialize;

use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const PI_SOURCE_KEY: &str = "pi";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PiTotals {
    pub records: u64,
    pub input: u64,
    pub source_output: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub reasoning: u64,
}

impl PiTotals {
    fn add_message(&mut self, message: &UnifiedMessage) {
        self.records = self.records.saturating_add(1);
        self.input = self.input.saturating_add(message.tokens.input.unwrap_or(0));
        self.output = self
            .output
            .saturating_add(message.tokens.output.unwrap_or(0));
        self.reasoning = self
            .reasoning
            .saturating_add(message.tokens.reasoning.unwrap_or(0));
        self.source_output = self.source_output.saturating_add(
            message
                .tokens
                .output
                .unwrap_or(0)
                .saturating_add(message.tokens.reasoning.unwrap_or(0)),
        );
        self.cache_read = self
            .cache_read
            .saturating_add(message.tokens.cache_read.unwrap_or(0));
        self.cache_write = self
            .cache_write
            .saturating_add(message.tokens.cache_write.unwrap_or(0));
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PiDrainOutcome {
    pub root_absent: bool,
    pub files_read: usize,
    /// Files read from their previous byte offset because only an append
    /// happened since; a subset of `files_read`.
    pub files_resumed: usize,
    /// Complete lines that could not be read during this drain, counted per
    /// file; only files with at least one such line appear. Distinct from an
    /// unterminated tail, which is never consumed and never counted.
    pub skipped_lines_by_file: BTreeMap<PathBuf, u64>,
    pub parsed: PiTotals,
    pub observations_deduplicated: usize,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
    /// Bytes this drain fed to SHA-256 while reading session files. A
    /// steady-state drain is O(window + appended) per grown file, never
    /// O(file); tests assert that bound here instead of measuring wall time.
    pub hashed_bytes: u64,
    /// Bytes read past each file's resume offset. A drain over files that
    /// did not grow reads none.
    pub read_bytes: u64,
    /// Bytes read while looking for files' session headers. A file whose
    /// header was already found in its verified prefix costs none; a file
    /// with no header yet costs only the bytes past what earlier drains
    /// already searched.
    pub header_scan_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionRank {
    header_timestamp_ms: i64,
    session_id: String,
    project_root: Option<String>,
}

/// What earlier drains learned about where one file's session header is.
///
/// It describes the complete lines in `[0, scanned_to)`, and `scanned_to` is
/// never past the offset of the resume point committed beside it. A later
/// drain uses it only when that resume point's prefix verified, so the bytes
/// it describes are the bytes on disk; a file read again from the start
/// searches again from the start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeaderScan {
    scanned_to: u64,
    /// The header's timestamp (`i64::MAX` when it has none) when a session
    /// header line ends inside the range; `None` when no line there is one,
    /// so the next search starts at `scanned_to`.
    header_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct CachedIdentity {
    owner: SessionRank,
    request: EventRecordRequest,
}

/// How often a file's whole consumed prefix is re-hashed instead of just the
/// trailing window, measured against the drain's `recorded_at_ms` clock.
/// Ordinary drains verify only `PI_PREFIX_WINDOW_BYTES` before resuming; a
/// drain at or past this interval verifies every consumed byte.
///
/// The gap this accepts: a rewrite of already-consumed bytes OUTSIDE the
/// trailing window is not seen until the cadence fires. What that costs:
/// records ingested before the rewrite changed on disk. The ledger is
/// write-once, so when the periodic check (or any window mismatch) catches
/// the rewrite, the full re-read re-delivers the changed records as
/// same-identity-different-payload into `rejected_delivery` — visible, never
/// silently absorbed — and records appended in the gap were read correctly
/// throughout. Do not "fix" this back to a per-tick full hash: that is what
/// re-read and re-hashed ~300 MB every drain on the one active Pi session
/// file (~10 MB/s of continuous SHA-256 — the dominant share of the daemon's
/// 6-8% of one core, measured in production 2026-09-23), growing with the
/// file forever.
const PI_FULL_PREFIX_VERIFY_INTERVAL_MS: i64 = 60 * 60 * 1000;

#[derive(Debug, Default)]
struct PiCache {
    /// Where each file's next read may resume. It is committed together with
    /// `identities`, which pins an owner for every record in the consumed
    /// prefix; that is what lets a tail-only read still dedup fork copies.
    resume: HashMap<PathBuf, PiResumePoint>,
    /// When each file's consumed prefix was last verified end to end, on the
    /// drain's `recorded_at_ms` clock. Committed together with `resume`.
    prefix_verified_ms: HashMap<PathBuf, i64>,
    /// For each file, how far its session-header search has read and whether
    /// it found the header (see `HeaderScan`). Committed together with
    /// `resume`, and trusted only for a file whose resume verified.
    header_scans: HashMap<PathBuf, HeaderScan>,
    identities: HashMap<String, CachedIdentity>,
}

#[derive(Debug, Clone)]
enum PiRoot {
    Path(PathBuf),
    Unresolved(String),
}

/// A process-local incremental view over Pi's durable session files.
///
/// Resume points only avoid work. Event identity remains the correctness
/// boundary, so a restart may safely read every file again.
pub(crate) struct PiSource {
    root: PiRoot,
    cache: Mutex<PiCache>,
}

impl PiSource {
    #[cfg_attr(
        any(test, feature = "test-support"),
        expect(
            dead_code,
            reason = "tests inject an environment instead of reading process variables"
        )
    )]
    pub(crate) fn from_environment() -> Self {
        Self::from_env(&cortexkit_paths::SystemEnv)
    }

    pub(crate) fn from_env(env: &impl cortexkit_paths::EnvSource) -> Self {
        let root = match crate::harness_paths::pi(env) {
            Ok(path) => PiRoot::Path(path),
            Err(error) => PiRoot::Unresolved(error.to_string()),
        };
        Self {
            root,
            cache: Mutex::new(PiCache::default()),
        }
    }

    /// HOME-only fixture layout keeps existing drain/health tests independent of
    /// process variables. Production discovery uses `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>) -> Self {
        let root = match home {
            Some(home) if Path::new(&home).is_absolute() => {
                PiRoot::Path(PathBuf::from(home).join(".pi/agent/sessions"))
            }
            Some(home) => PiRoot::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the Pi session root"
            )),
            None => {
                PiRoot::Unresolved("HOME is unset; cannot resolve the Pi session root".to_string())
            }
        };
        Self {
            root,
            cache: Mutex::new(PiCache::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn at(root: impl Into<PathBuf>) -> Self {
        Self {
            root: PiRoot::Path(root.into()),
            cache: Mutex::new(PiCache::default()),
        }
    }

    /// Read every session file from where the last drain stopped and commit
    /// what it holds. Setting
    /// `stop` abandons the drain between files or between events and returns
    /// `SourceDrainError::Stopped` having committed nothing and updated no
    /// cache.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<PiDrainOutcome, SourceDrainError> {
        self.drain_inner(state, recorded_at_ms, None, stop)
    }

    fn drain_inner(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        occurred_window: Option<(i64, i64)>,
        stop: &AtomicBool,
    ) -> Result<PiDrainOutcome, SourceDrainError> {
        let root = match &self.root {
            PiRoot::Path(root) => root,
            PiRoot::Unresolved(reason) => {
                crate::refresher::harness_drain::ensure_runtime_row(state, PI_SOURCE_KEY)?;
                return Err(reason.clone().into());
            }
        };

        match fs::metadata(root) {
            Ok(_) => {
                crate::refresher::harness_drain::record_source_discovery(
                    state,
                    PI_SOURCE_KEY,
                    root.display(),
                    true,
                );
                crate::refresher::harness_drain::ensure_runtime_row(state, PI_SOURCE_KEY)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                crate::refresher::harness_drain::record_source_discovery(
                    state,
                    PI_SOURCE_KEY,
                    root.display(),
                    false,
                );
                crate::refresher::harness_drain::remove_runtime_row(state, PI_SOURCE_KEY)?;
                return Ok(PiDrainOutcome {
                    root_absent: true,
                    ..PiDrainOutcome::default()
                });
            }
            Err(error) => {
                crate::refresher::harness_drain::ensure_runtime_row(state, PI_SOURCE_KEY)?;
                return Err(format!("inspect Pi session root {}: {error}", root.display()).into());
            }
        }

        let files = jsonl_files(root)
            .map_err(|error| format!("read Pi session root {}: {error}", root.display()))?;
        let (cached_resume, cached_verified, cached_header_scans) = {
            let cache = self
                .cache
                .lock()
                .map_err(|_| "Pi file cache lock poisoned".to_string())?;
            (
                cache.resume.clone(),
                cache.prefix_verified_ms.clone(),
                cache.header_scans.clone(),
            )
        };
        // A windowed drain skips records outside the requested time range.
        // Saving its offset would make later drains resume past those skipped
        // records and never ingest them, so a windowed drain neither resumes
        // nor saves resume points.
        let resumable = occurred_window.is_none();

        let mut outcome = PiDrainOutcome::default();
        let mut resume_points = HashMap::with_capacity(files.len());
        let mut verified_ms = HashMap::with_capacity(files.len());
        let mut header_scans = HashMap::with_capacity(files.len());
        let mut observations = Vec::new();
        let mut set_aside = Vec::new();
        for path in files {
            // Nothing has been written yet and the caches below are replaced
            // only after the commit, so abandoning the pass here leaves the
            // store and the caches as they were. A restart rereads every file
            // and content-derived identity absorbs what was already recorded.
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            // Every file is read every drain. Size and mtime are never enough
            // to skip one: Linux file timestamps are coarse, so a same-size
            // rewrite within one timestamp tick keeps both. A file that did
            // not change costs one trailing-window hash and a zero-byte read.
            //
            // A file resumes from its last complete line when the bytes
            // before it are unchanged (checked by digest, not by size or
            // mtime), and is read in full otherwise; content-derived identity
            // makes the full read safe. Ordinary ticks re-hash only the
            // trailing window of the consumed prefix; the whole prefix is
            // re-hashed once the verification cadence is due.
            //
            // The reader answers an unopenable file with an empty parse, so the
            // file is opened here first to fail the drain visibly instead.
            fs::File::open(&path)
                .map_err(|error| format!("open Pi session file {}: {error}", path.display()))?;
            let previous = resumable.then(|| cached_resume.get(&path)).flatten();
            let check = match previous {
                Some(_)
                    if cached_verified.get(&path).is_none_or(|&verified| {
                        recorded_at_ms.saturating_sub(verified) >= PI_FULL_PREFIX_VERIFY_INTERVAL_MS
                    }) =>
                {
                    PiPrefixCheck::Full
                }
                _ => PiPrefixCheck::Window,
            };
            let parse = parse_pi_file_resumable(&path, previous, check);
            set_aside.extend(parse.set_aside.into_iter().map(|(offset, reason)| {
                crate::refresher::SetAsideRecord {
                    locator: format!("{path:?}:byte:{offset}"),
                    reason,
                }
            }));
            // The next drain re-hashes the bytes before this file's new resume
            // offset before it resumes. A header-search result is cached only
            // for lines before that offset, so it is never trusted for bytes
            // that check does not cover; without a resume point nothing is
            // cached.
            let header_cacheable_to = resumable
                .then(|| parse.resume.as_ref().map(PiResumePoint::offset))
                .flatten();
            // A header-search result from an earlier drain covers lines before
            // the previous resume offset. It is trusted only when this file
            // resumed, which means those bytes verified unchanged.
            let previous_header_scan = parse
                .resumed
                .then(|| cached_header_scans.get(&path).copied())
                .flatten();
            if resumable {
                if let Some(point) = parse.resume {
                    resume_points.insert(path.clone(), point);
                }
                // A full read hashed every byte of the file this drain, and a
                // due Full check re-verified the whole consumed prefix; both
                // restart the cadence. A window-checked resume carries the
                // previous verification time forward.
                if check == PiPrefixCheck::Full || !parse.resumed {
                    verified_ms.insert(path.clone(), recorded_at_ms);
                } else if let Some(&verified) = cached_verified.get(&path) {
                    verified_ms.insert(path.clone(), verified);
                }
            }
            if parse.resumed {
                outcome.files_resumed += 1;
            }
            outcome.hashed_bytes += parse.hashed_bytes;
            outcome.read_bytes += parse.read_bytes;
            if parse.skipped_lines > 0 {
                outcome
                    .skipped_lines_by_file
                    .insert(path.clone(), parse.skipped_lines);
            }
            outcome.files_read += 1;
            if parse.messages.is_empty() {
                // The verified prefix only grew, so the earlier header-search
                // result still holds under the new resume point.
                if let (Some(scan), Some(_)) = (previous_header_scan, header_cacheable_to) {
                    header_scans.insert(path.clone(), scan);
                }
                continue;
            }
            // The header is looked up only when there are records to rank, so
            // a file that did not grow costs no read beyond the resume check.
            let header =
                session_header_timestamp(&path, previous_header_scan, header_cacheable_to)?;
            outcome.header_scan_bytes += header.scanned_bytes;
            if let Some(scan) = header.scan {
                header_scans.insert(path.clone(), scan);
            }

            for record in parse.messages {
                // An undated record lies in no time window.
                if occurred_window.is_some_and(|(start, end)| {
                    record
                        .occurred_at_ms
                        .is_none_or(|occurred| occurred < start || occurred > end)
                }) {
                    continue;
                }
                outcome.parsed.add_message(&record.message);
                let rank = SessionRank {
                    header_timestamp_ms: header.header_timestamp_ms,
                    session_id: record.message.session_id.clone(),
                    project_root: record.message.workspace_key.clone(),
                };
                let locator = format!("{path:?}:byte:{}", record.source_offset);
                match map_message(record) {
                    Ok(request) => observations.push(Observation { request, rank }),
                    Err(reason) => {
                        set_aside.push(crate::refresher::SetAsideRecord { locator, reason })
                    }
                }
            }
        }

        // select_requests only reads the identity cache, so the lock is held
        // across the selection instead of cloning the whole map every drain.
        // The lock is released before record_requests, and the new identities
        // are merged after the commit exactly as before.
        let Selection {
            requests,
            additions,
            observations_deduplicated,
        } = {
            let cache = self
                .cache
                .lock()
                .map_err(|_| "Pi identity cache lock poisoned".to_string())?;
            select_requests(observations, &cache.identities)
        };
        outcome.observations_deduplicated = observations_deduplicated;
        record_requests(
            state,
            &requests,
            &set_aside,
            recorded_at_ms,
            true,
            &mut outcome,
            &|| stop.load(Ordering::Relaxed),
        )?;

        let mut cache = self
            .cache
            .lock()
            .map_err(|_| "Pi cache lock poisoned".to_string())?;
        if resumable {
            cache.resume = resume_points;
            cache.prefix_verified_ms = verified_ms;
            cache.header_scans = header_scans;
        }
        cache.identities.extend(additions);
        Ok(outcome)
    }

    #[cfg(test)]
    fn drain_in_window(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<PiDrainOutcome, SourceDrainError> {
        self.drain_inner(
            state,
            recorded_at_ms,
            Some((start_ms, end_ms)),
            &AtomicBool::new(false),
        )
    }
}

impl crate::refresher::harness_drain::HarnessSource for PiSource {
    fn source_key(&self) -> &'static str {
        PI_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        Ok((outcome.quarantined_redeliveries > 0).then(|| {
            format!(
                "{} Pi deliveries were quarantined",
                outcome.quarantined_redeliveries
            )
        }))
    }
}

fn jsonl_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    crate::refresher::harness_drain::jsonl_files(root)
}

#[derive(Debug, Deserialize)]
struct HeaderProbe {
    #[serde(rename = "type")]
    entry_type: String,
    /// Never read, but still decoded: a line whose `id` is present and not a
    /// string fails to decode and so is not taken as the session header.
    #[expect(
        dead_code,
        reason = "decoded so a non-string id keeps a line from being the header"
    )]
    id: Option<String>,
    timestamp: Option<String>,
}

/// The result of looking up one file's session header.
struct HeaderLookup {
    /// The first session header's timestamp, or `i64::MAX` when the file has
    /// no header or the header has no readable timestamp.
    header_timestamp_ms: i64,
    /// What a later drain may reuse, or `None` when nothing may be cached.
    scan: Option<HeaderScan>,
    /// Bytes read by this lookup.
    scanned_bytes: u64,
}

/// Finds the first line of `path` that is a session header, exactly as a
/// search from the start of the file would, but starting from what `previous`
/// already established about the file's verified prefix.
///
/// A header already found there is answered without reading. Otherwise the
/// search starts at `previous.scanned_to` (no earlier line is a header) and
/// runs to the end of the file, including an unterminated last line, as the
/// full search does. Only complete lines that end at or before
/// `cacheable_to`, the offset the next drain verifies, are remembered.
fn session_header_timestamp(
    path: &Path,
    previous: Option<HeaderScan>,
    cacheable_to: Option<u64>,
) -> Result<HeaderLookup, String> {
    use std::io::{Seek, SeekFrom};

    let start = match previous {
        Some(HeaderScan {
            header_timestamp_ms: Some(header_timestamp_ms),
            ..
        }) => {
            return Ok(HeaderLookup {
                header_timestamp_ms,
                scan: previous,
                scanned_bytes: 0,
            })
        }
        Some(scan) => scan.scanned_to,
        None => 0,
    };
    let read_error =
        |error: std::io::Error| format!("read Pi session header {}: {error}", path.display());
    let mut file = fs::File::open(path)
        .map_err(|error| format!("open Pi session file {}: {error}", path.display()))?;
    file.seek(SeekFrom::Start(start)).map_err(read_error)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut line_start = start;
    let mut scanned_bytes = 0_u64;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line).map_err(read_error)?;
        if read == 0 {
            // `line_start` is just past the last complete line: an
            // unterminated last line does not advance it.
            return Ok(HeaderLookup {
                header_timestamp_ms: i64::MAX,
                scan: cacheable_to.map(|limit| HeaderScan {
                    scanned_to: line_start.min(limit),
                    header_timestamp_ms: None,
                }),
                scanned_bytes,
            });
        }
        let read = u64::try_from(read).map_err(|error| error.to_string())?;
        scanned_bytes += read;
        let complete = line.last() == Some(&b'\n');
        let line_end = line_start + read;
        let header = serde_json::from_slice::<HeaderProbe>(&line)
            .ok()
            .filter(|probe| probe.entry_type == "session");
        let Some(probe) = header else {
            if complete {
                line_start = line_end;
            }
            continue;
        };
        let header_timestamp_ms = probe
            .timestamp
            .as_deref()
            .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
            .map_or(i64::MAX, |timestamp| timestamp.timestamp_millis());
        let scan = cacheable_to.map(|limit| {
            if complete && line_end <= limit {
                HeaderScan {
                    scanned_to: line_end,
                    header_timestamp_ms: Some(header_timestamp_ms),
                }
            } else {
                // The header line may still change (an unterminated line) or
                // lies past what the next drain verifies, so only the lines
                // before it are remembered, as headerless.
                HeaderScan {
                    scanned_to: line_start.min(limit),
                    header_timestamp_ms: None,
                }
            }
        });
        return Ok(HeaderLookup {
            header_timestamp_ms,
            scan,
            scanned_bytes,
        });
    }
}

fn map_message(record: PiUsageRecord) -> Result<EventRecordRequest, String> {
    let PiUsageRecord {
        message,
        occurred_at_ms,
        ..
    } = record;
    if message.output_reasoning_conflicted {
        return Err("Pi reasoning exceeds output".into());
    }
    let producer_ref = message
        .dedup_key
        .clone()
        .ok_or_else(|| "Pi assistant record has no stable dedup key".to_string())?;
    let measurements = [
        ("input", "usage.input", message.tokens.input),
        ("cache_read", "usage.cacheRead", message.tokens.cache_read),
        (
            "cache_write",
            "usage.cacheWrite",
            message.tokens.cache_write,
        ),
        ("output", "usage.output", message.tokens.output),
        ("reasoning", "usage.reasoning", message.tokens.reasoning),
    ]
    .into_iter()
    .filter_map(|(role, source, quantity)| quantity.map(|quantity| (role, source, quantity)))
    .map(|(role, source, quantity)| {
        crate::refresher::harness_drain::measurement(role, source, Some(quantity))
            .map_err(|error| format!("Pi {error}"))
    })
    .collect::<Result<Vec<_>, String>>()?;

    Ok(EventRecordRequest {
        producer: PI_SOURCE_KEY.to_string(),
        // Both response keys and the fallback composite are globally scoped,
        // content-stable references. The fallback contains the entry id,
        // recorded timestamp, resolved provider, model, input, normalized
        // output, cache-read and cache-write counts. A process/session lineage
        // would turn a Pi fork's copied call into a second event instead of one
        // observation.
        producer_lineage: None,
        producer_ref,
        prior_event_ref: None,
        harness: None,
        project_root: message.workspace_key,
        session: Some(message.session_id),
        run_ref: None,
        work_class: None,
        // `None` when the record carries no timestamp: the ledger keeps it
        // undated rather than dated by a guess.
        occurred_at_ms,
        emitted_at_ms: None,
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider: Some(message.provider_id),
        model: Some(message.model_id),
        account_ref: None,
        auth_selection: None,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        charge_basis: ChargeBasis::Unknown,
        measurements,
    })
}

#[derive(Debug)]
struct Observation {
    request: EventRecordRequest,
    rank: SessionRank,
}

struct Selection {
    requests: Vec<EventRecordRequest>,
    additions: HashMap<String, CachedIdentity>,
    observations_deduplicated: usize,
}

fn select_requests(
    observations: Vec<Observation>,
    cached: &HashMap<String, CachedIdentity>,
) -> Selection {
    let mut groups: BTreeMap<String, Vec<Observation>> = BTreeMap::new();
    for observation in observations {
        groups
            .entry(observation.request.producer_ref.clone())
            .or_default()
            .push(observation);
    }

    let mut requests = Vec::new();
    let mut additions = HashMap::new();
    let mut selected_benign = 0usize;
    for (producer_ref, mut group) in groups {
        group.sort_by(|left, right| left.rank.cmp(&right.rank));
        if let Some(existing) = cached.get(&producer_ref) {
            let mut owner_submitted = false;
            for observation in group {
                if !owner_submitted && observation.rank == existing.owner {
                    requests.push(observation.request);
                    owner_submitted = true;
                    continue;
                }
                if same_call(&existing.request, &observation.request) {
                    selected_benign += 1;
                } else {
                    // A shared producer key with different call content is not
                    // a fork copy. Let ingest quarantine the contradiction.
                    requests.push(observation.request);
                }
            }
            continue;
        }

        let mut iter = group.into_iter();
        let canonical = iter.next().expect("group is non-empty");
        let canonical_index = requests.len();
        requests.push(canonical.request);
        additions.insert(
            producer_ref,
            CachedIdentity {
                owner: canonical.rank,
                request: requests[canonical_index].clone(),
            },
        );
        for observation in iter {
            if same_call(&requests[canonical_index], &observation.request) {
                selected_benign += 1;
            } else {
                requests.push(observation.request);
            }
        }
    }

    Selection {
        observations_deduplicated: selected_benign,
        requests,
        additions,
    }
}

/// Session and project are observation coordinates in Pi files: forks copy one
/// call under a later session, and the live corpus also contains the same
/// session copied under two cwd directories. The drain keeps the occurrence
/// with the earliest header timestamp, then session id and project root, losing
/// the fact that one call appeared in N transcripts rather than asserting that
/// the call happened N times. Every fact coordinate must still agree.
fn same_call(left: &EventRecordRequest, right: &EventRecordRequest) -> bool {
    // Destructure exhaustively so a new fact coordinate cannot be silently
    // omitted from this comparison. Only the two observation coordinates differ.
    let EventRecordRequest {
        project_root: _,
        session: _,
        producer,
        producer_lineage,
        producer_ref,
        prior_event_ref,
        harness,
        run_ref,
        work_class,
        occurred_at_ms,
        emitted_at_ms,
        terminal_status,
        attempt_ordinal,
        vendor_request_id,
        provider,
        model,
        account_ref,
        auth_selection,
        charge_basis_mapping_version,
        charge_basis_overrides,
        charge_basis,
        measurements,
    } = left;
    producer == &right.producer
        && producer_lineage == &right.producer_lineage
        && producer_ref == &right.producer_ref
        && prior_event_ref == &right.prior_event_ref
        && harness == &right.harness
        && run_ref == &right.run_ref
        && work_class == &right.work_class
        && occurred_at_ms == &right.occurred_at_ms
        && emitted_at_ms == &right.emitted_at_ms
        && terminal_status == &right.terminal_status
        && attempt_ordinal == &right.attempt_ordinal
        && vendor_request_id == &right.vendor_request_id
        && provider == &right.provider
        && model == &right.model
        && account_ref == &right.account_ref
        && auth_selection == &right.auth_selection
        && charge_basis_mapping_version == &right.charge_basis_mapping_version
        && charge_basis_overrides == &right.charge_basis_overrides
        && charge_basis == &right.charge_basis
        && measurements == &right.measurements
}

/// Commit good records and set-aside evidence together. With `update_runtime`,
/// also mark `source_runtime` current and clear pending work. The test helper
/// `drain_file_without_cache` passes false because it creates no runtime row.
fn record_requests(
    state: &SharedState,
    requests: &[EventRecordRequest],
    set_aside: &[crate::refresher::SetAsideRecord],
    recorded_at_ms: i64,
    update_runtime: bool,
    outcome: &mut PiDrainOutcome,
    stop_requested: &dyn Fn() -> bool,
) -> Result<(), SourceDrainError> {
    use crate::refresher::harness_drain::{
        mark_current_in_tx, record_events_in_tx, record_set_aside_in_tx, RecordTally,
    };
    let mut stopped = false;
    let committed = state.ordered_write(|tx| {
        if requests.is_empty() && stop_requested() {
            stopped = true;
            return Err("Pi ingest stopped".into());
        }
        let newly_seen = record_set_aside_in_tx(tx, PI_SOURCE_KEY, set_aside, recorded_at_ms)?;
        let dispositions = record_events_in_tx(
            tx,
            "Pi",
            requests,
            recorded_at_ms,
            stop_requested,
            &mut stopped,
        )?;
        if update_runtime {
            mark_current_in_tx(tx, PI_SOURCE_KEY, "Pi", recorded_at_ms)?;
        }
        Ok(((dispositions, newly_seen), true))
    });
    let (dispositions, newly_seen) = match committed {
        Ok(committed) => committed,
        Err(_) if stopped => return Err(SourceDrainError::Stopped),
        Err(reason) => return Err(SourceDrainError::Failed(reason)),
    };
    crate::refresher::harness_drain::warn_set_aside(PI_SOURCE_KEY, &newly_seen);
    let tally = RecordTally::from_dispositions(dispositions);
    outcome.rows_recorded = tally.recorded;
    outcome.exact_redeliveries = tally.exact_redeliveries;
    outcome.quarantined_redeliveries = tally.quarantined_redeliveries;
    Ok(())
}

#[cfg(test)]
fn drain_file_without_cache(
    state: &SharedState,
    path: &Path,
    recorded_at_ms: i64,
) -> Result<PiDrainOutcome, SourceDrainError> {
    let header = session_header_timestamp(path, None, None)?;
    let mut parsed = PiTotals::default();
    let observations = parse_pi_file_resumable(path, None, PiPrefixCheck::Full)
        .messages
        .into_iter()
        .map(|record| {
            parsed.add_message(&record.message);
            let rank = SessionRank {
                header_timestamp_ms: header.header_timestamp_ms,
                session_id: record.message.session_id.clone(),
                project_root: record.message.workspace_key.clone(),
            };
            Ok(Observation {
                request: map_message(record)?,
                rank,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let selection = select_requests(observations, &HashMap::new());
    let mut outcome = PiDrainOutcome {
        files_read: 1,
        parsed,
        observations_deduplicated: selection.observations_deduplicated,
        ..PiDrainOutcome::default()
    };
    record_requests(
        state,
        &selection.requests,
        &[],
        recorded_at_ms,
        false,
        &mut outcome,
        &|| false,
    )?;
    Ok(outcome)
}

// Test recordings use the fixed prefix window and a small fixture count.
#[allow(clippy::cast_possible_truncation)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::{price_unpriced_events, run_iteration_with_pi, ExportPage, Producer};
    use crate::state::test_state;
    use astrocyte_core::ingest::EventRecordRequest;
    use std::sync::Arc;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    #[test]
    #[ignore = "mechanism timing for request comparison"]
    fn same_call_comparison_timing() {
        let directory = TempDir::new().unwrap();
        let path = write_fixture(
            directory.path(),
            "project",
            &fixture("s", "/project", Some("response")),
        );
        let parsed = parse_pi_file_resumable(&path, None, PiPrefixCheck::Full);
        let request = map_message(parsed.messages.into_iter().next().unwrap()).unwrap();
        let mut copy = request.clone();
        copy.session = Some("fork".into());
        copy.project_root = Some("/copy".into());
        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            assert!(std::hint::black_box(same_call(
                std::hint::black_box(&request),
                std::hint::black_box(&copy)
            )));
        }
        eprintln!(
            "same_call: 100000 comparisons, {} us",
            start.elapsed().as_micros()
        );
        copy.measurements[0].quantity_units = Some(999_999);
        assert!(!same_call(&request, &copy));
    }

    #[test]
    fn set_aside_pi_bad_record_keeps_good_calls_and_resume() {
        for conflict in [false, true] {
            let temp = TempDir::new().unwrap();
            let before = fixture("session", "/repo", Some("before"));
            let mut bad = fixture("session", "/repo", None)
                .lines()
                .nth(1)
                .unwrap()
                .to_string();
            if conflict {
                bad = bad.replace("\"reasoning\":2", "\"reasoning\":9");
            } else {
                bad = bad.replace("\"id\":\"entry-1\",", "");
            }
            let after = fixture("session", "/repo", Some("after"))
                .lines()
                .nth(1)
                .unwrap()
                .to_string();
            let path = write_fixture(
                temp.path(),
                "project",
                &(before + &bad + "\n" + &after + "\n"),
            );
            let state = test_state();
            let source = PiSource::at(temp.path());
            for now in [100, 200] {
                let result = source.drain(&state, now, &NEVER_STOP);
                assert!(
                    result.is_ok(),
                    "conflict={conflict}, tick {now}: {result:?}"
                );
                let count = state
                    .with_conn(|conn| {
                        conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .map_err(|e| e.to_string())
                    })
                    .unwrap();
                assert_eq!(count, 2);
                assert_eq!(
                    source.cache.lock().unwrap().resume[&path].offset(),
                    fs::metadata(&path).unwrap().len()
                );
                crate::refresher::harness_drain::assert_set_aside(&state, "pi", 1);
            }
        }
    }

    #[test]
    fn set_aside_good_pi_rows_are_byte_identical() {
        let temp = TempDir::new().unwrap();
        write_fixture(
            temp.path(),
            "project",
            &fixture("session", "/repo", Some("before")),
        );
        let state = test_state();
        PiSource::at(temp.path())
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();
        crate::refresher::harness_drain::assert_good_snapshot(
            &state,
            "pi",
            "52f73639e3ab975cad24d00d1db73f6e6addf95ae74faa6ce45c131bba151c9f",
        );
    }

    fn fixture(session: &str, cwd: &str, response_id: Option<&str>) -> String {
        let response = response_id
            .map(|id| format!(r#","responseId":"{id}""#))
            .unwrap_or_default();
        format!(
            concat!(
                r#"{{"type":"session","id":"{session}","timestamp":"2026-09-01T00:00:00.000Z","cwd":"{cwd}"}}"#,
                "\n",
                r#"{{"type":"message","id":"entry-1","timestamp":"2026-09-01T00:00:01.000Z","message":{{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4"{response},"usage":{{"input":11,"output":7,"cacheRead":13,"cacheWrite":17,"reasoning":2,"totalTokens":48}}}}}}"#,
                "\n"
            ),
            session = session,
            cwd = cwd,
            response = response,
        )
    }

    fn write_fixture(root: &Path, encoded_dir: &str, contents: &str) -> PathBuf {
        let directory = root.join(encoded_dir);
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("session.jsonl");
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn drain_finds_sessions_through_pi_session_override() {
        let temp = TempDir::new().unwrap();
        let sessions = temp.path().join("relocated-sessions");
        write_fixture(
            &sessions,
            "project",
            &fixture("override-session", "/repo", None),
        );
        let env = crate::harness_paths::TestEnv::default()
            .set("PI_CODING_AGENT_SESSION_DIR", sessions.as_os_str());
        let state = test_state();
        let outcome = PiSource::from_env(&env)
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();
        assert!(!outcome.root_absent);
        let recorded: (i64, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),COALESCE(SUM(m.quantity_units),0) FROM usage_event e
                 JOIN measurement m ON m.event_key=e.event_key
                 WHERE e.producer='pi' AND m.role='output'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(recorded, (1, 5));
        assert!(state.producer_degraded_reason("pi").is_none());
    }

    fn clear_runtime(state: &SharedState) {
        state
            .ordered_write(|tx| {
                tx.execute("DELETE FROM source_runtime", [])
                    .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    fn assert_cold_pi_runtime_degrades(state: &SharedState) {
        let cold: (i64, Option<String>, Option<i64>) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MAX(lineage),MAX(last_success_ms)
                     FROM source_runtime WHERE source_key='pi'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(cold, (1, None, None));
        assert_eq!(state.public_projection().health(100).status, "degraded");
    }

    #[test]
    fn relative_home_refuses_with_a_cold_degraded_pi_lane() {
        let state = test_state();
        clear_runtime(&state);
        let error = PiSource::from_home(Some(OsString::from("relative-home")))
            .drain(&state, 100, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("HOME is relative"), "{error}");
        assert_cold_pi_runtime_degrades(&state);
    }

    #[test]
    fn unset_home_refuses_with_a_cold_degraded_pi_lane() {
        let state = test_state();
        clear_runtime(&state);
        let error = PiSource::from_home(None)
            .drain(&state, 100, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("HOME is unset"), "{error}");
        assert_cold_pi_runtime_degrades(&state);
    }

    #[test]
    fn absolute_home_resolves_the_pi_sessions_path() {
        let temp = TempDir::new().unwrap();
        let expected = temp.path().join(".pi/agent/sessions");
        let source = PiSource::from_home(Some(temp.path().as_os_str().to_owned()));
        match &source.root {
            PiRoot::Path(actual) => assert_eq!(actual, &expected),
            PiRoot::Unresolved(reason) => panic!("absolute home was refused: {reason}"),
        }

        fs::create_dir_all(&expected).unwrap();
        let state = test_state();
        clear_runtime(&state);
        let outcome = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert!(!outcome.root_absent);
        let runtime: (Option<String>, Option<i64>, Option<i64>, i64, i64, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,high_water_ordinal,last_success_ms,
                            pending_known,pending_unknown
                     FROM source_runtime WHERE source_key='pi'",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(runtime, (None, None, None, 100, 0, 0));
        assert_eq!(state.public_projection().health(100).status, "ok");
    }

    #[test]
    fn drained_pi_event_uses_pi_session_and_header_cwd() {
        let temp = TempDir::new().unwrap();
        write_fixture(
            temp.path(),
            "--encoded-directory-name--",
            &fixture("pi-session-1", "/actual/project", Some("response-1")),
        );
        let state = test_state();
        let source = PiSource::at(temp.path());
        let outcome = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(outcome.rows_recorded, 1);
        let unchanged = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(unchanged.files_read, 1, "every file is read every drain");
        assert_eq!(unchanged.files_resumed, 1);
        assert_eq!(unchanged.parsed.records, 0);
        assert_eq!(unchanged.rows_recorded, 0);

        let stored: (String, Option<String>, Option<String>, Option<String>) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT producer,producer_lineage,session,project_root
                     FROM usage_event WHERE producer='pi'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            stored,
            (
                "pi".to_string(),
                None,
                Some("pi-session-1".to_string()),
                Some("/actual/project".to_string())
            )
        );
    }

    fn message_line(entry: &str, response: &str, input: u64) -> String {
        format!(
            r#"{{"type":"message","id":"{entry}","timestamp":"2026-09-01T00:00:02.000Z","message":{{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"{response}","usage":{{"input":{input},"output":7,"cacheRead":13,"cacheWrite":17,"reasoning":2,"totalTokens":48}}}}}}"#
        )
    }

    fn append(path: &Path, text: &str) {
        use std::io::Write;
        let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn table_count(state: &SharedState, table: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// The second drain of a grown file parses only the appended records. The
    /// parsed-record count is asserted because ingest ends identical whether
    /// the prefix was re-read or not.
    #[test]
    fn grown_pi_file_resumes_from_its_offset_and_parses_only_new_records() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &fixture("pi-session-1", "/actual/project", Some("response-1")),
        );
        let state = test_state();
        let source = PiSource::at(temp.path());
        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!((first.parsed.records, first.files_resumed), (1, 0));

        append(
            &path,
            &format!(
                "{}\n{}\n",
                message_line("entry-2", "response-2", 21),
                message_line("entry-3", "response-3", 31)
            ),
        );
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(second.files_read, 1);
        assert_eq!(second.files_resumed, 1);
        assert_eq!(second.parsed.records, 2, "only the appended records");
        assert_eq!(second.rows_recorded, 2);
        assert_eq!(second.exact_redeliveries, 0);
        assert_eq!(table_count(&state, "usage_event"), 3);
    }

    /// A rewritten prefix cannot be trusted, even when the file also grew, so
    /// the drain reads the whole file again. Its unchanged record comes back
    /// as a read-path exact redelivery, which must not be logged.
    #[test]
    fn pi_file_with_changed_prefix_falls_back_to_a_full_reread() {
        let temp = TempDir::new().unwrap();
        let header = r#"{"type":"session","id":"pi-session-1","timestamp":"2026-09-01T00:00:00.000Z","cwd":"/actual/project"}"#;
        let path = write_fixture(
            temp.path(),
            "encoded",
            &format!(
                "{header}\n{}\n{}\n",
                message_line("entry-1", "response-1", 11),
                message_line("entry-2", "response-2", 21)
            ),
        );
        let state = test_state();
        let source = PiSource::at(temp.path());
        assert_eq!(
            source
                .drain(&state, 100, &NEVER_STOP)
                .unwrap()
                .parsed
                .records,
            2
        );

        // Same length rewrite of the first record, plus an append: the size
        // grows exactly as a pure append would.
        fs::write(
            &path,
            format!(
                "{header}\n{}\n{}\n{}\n",
                message_line("entry-1", "response-9", 11),
                message_line("entry-2", "response-2", 21),
                message_line("entry-3", "response-3", 31)
            ),
        )
        .unwrap();
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(second.files_read, 1);
        assert_eq!(second.files_resumed, 0);
        assert_eq!(second.parsed.records, 3, "the whole file is parsed again");
        assert_eq!(second.rows_recorded, 2, "response-9 and response-3 are new");
        assert_eq!(second.exact_redeliveries, 1);
        assert_eq!(table_count(&state, "redelivery_log"), 0);
    }

    /// `base` padded with inert `session_info` lines until it is at least
    /// `min_bytes` long. Padding lines are consumed into the resume prefix
    /// but emit no messages.
    fn padded(mut base: String, min_bytes: usize) -> String {
        let mut index = 0_u32;
        while base.len() < min_bytes {
            base.push_str(&format!(
                "{{\"type\":\"session_info\",\"name\":\"pad-{index}\"}}\n"
            ));
            index += 1;
        }
        base
    }

    const PAD_TEST_HEADER: &str = r#"{"type":"session","id":"pi-session-1","timestamp":"2026-09-01T00:00:00.000Z","cwd":"/actual/project"}"#;

    /// A rewrite inside the trailing hash window is caught on the very next
    /// tick: the window digest no longer matches, so the file is read in full
    /// and the changed record comes back as a quarantined redelivery.
    #[test]
    fn pi_tail_rewrite_inside_the_window_is_caught_on_the_next_tick() {
        let temp = TempDir::new().unwrap();
        // Larger than the window so the early padding sits outside it; the
        // rewritten record sits at the tail of the consumed prefix, inside it.
        let mut contents = padded(
            format!(
                "{PAD_TEST_HEADER}\n{}\n",
                message_line("entry-1", "response-1", 11)
            ),
            PI_PREFIX_WINDOW_BYTES as usize * 2,
        );
        contents.push_str(&message_line("entry-2", "response-2", 21));
        contents.push('\n');
        let path = write_fixture(temp.path(), "encoded", &contents);
        let state = test_state();
        let source = PiSource::at(temp.path());
        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!((first.parsed.records, first.files_resumed), (2, 0));

        // Same-length rewrite of the tail record, plus an append: the size
        // grows exactly as a pure append would.
        fs::write(
            &path,
            format!(
                "{}{}\n",
                contents.replacen(
                    &message_line("entry-2", "response-2", 21),
                    &message_line("entry-2", "response-2", 31),
                    1
                ),
                message_line("entry-3", "response-3", 41)
            ),
        )
        .unwrap();
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(
            second.files_resumed, 0,
            "the window check must catch a tail rewrite"
        );
        assert_eq!(second.parsed.records, 3, "the whole file is parsed again");
        assert_eq!(second.rows_recorded, 1, "response-3 is new");
        assert_eq!(second.exact_redeliveries, 1, "response-1 is unchanged");
        assert_eq!(
            second.quarantined_redeliveries, 1,
            "response-2 kept its identity but changed payload"
        );
    }

    /// A rewrite of already-consumed bytes OUTSIDE the trailing hash window
    /// is not caught per tick — the drain resumes and reads only the append —
    /// and IS caught once the periodic full verification is due: the file is
    /// read in full and the changed record is quarantined. This is the test
    /// that proves the full-prefix guarantee was moved to a cadence rather
    /// than dropped; removing the periodic check reddens it.
    #[test]
    fn pi_middle_rewrite_outside_the_window_waits_for_the_periodic_full_check() {
        let temp = TempDir::new().unwrap();
        // The early record is followed by more than a window of padding, so
        // rewriting it leaves the trailing window's bytes untouched.
        let contents = padded(
            format!(
                "{PAD_TEST_HEADER}\n{}\n",
                message_line("entry-1", "response-1", 11)
            ),
            PI_PREFIX_WINDOW_BYTES as usize * 2,
        );
        let path = write_fixture(temp.path(), "encoded", &contents);
        let state = test_state();
        let source = PiSource::at(temp.path());
        assert_eq!(
            source
                .drain(&state, 100, &NEVER_STOP)
                .unwrap()
                .parsed
                .records,
            1
        );

        // Same-length rewrite of the early record, plus an append.
        let rewritten = contents.replacen(
            &message_line("entry-1", "response-1", 11),
            &message_line("entry-1", "response-1", 19),
            1,
        );
        fs::write(
            &path,
            format!("{rewritten}{}\n", message_line("entry-2", "response-2", 21)),
        )
        .unwrap();
        let per_tick = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(
            per_tick.files_resumed, 1,
            "a rewrite outside the window escapes the per-tick check"
        );
        assert_eq!(
            per_tick.parsed.records, 1,
            "only the appended record is read"
        );
        assert_eq!(per_tick.quarantined_redeliveries, 0);

        // One more append, and the full verification interval has elapsed.
        fs::write(
            &path,
            format!(
                "{rewritten}{}\n{}\n",
                message_line("entry-2", "response-2", 21),
                message_line("entry-3", "response-3", 31)
            ),
        )
        .unwrap();
        let due = source
            .drain(
                &state,
                200 + PI_FULL_PREFIX_VERIFY_INTERVAL_MS + 1_000,
                &NEVER_STOP,
            )
            .unwrap();
        assert_eq!(
            due.files_resumed, 0,
            "the due full check must catch the rewrite"
        );
        assert_eq!(due.parsed.records, 3, "the whole file is parsed again");
        assert_eq!(due.rows_recorded, 1, "response-3 is new");
        assert_eq!(due.exact_redeliveries, 1, "response-2 is unchanged");
        assert_eq!(
            due.quarantined_redeliveries, 1,
            "response-1 kept its identity but changed payload"
        );
    }

    /// The production shape the single-jump test above cannot see: the active
    /// session file grows on EVERY tick, so every tick resumes it through
    /// the window check. A middle rewrite outside the window must stay
    /// uncaught while less than the full-verification interval has elapsed
    /// since the last full verification, and the first drain at or past that
    /// point must catch it — no matter how many window-verified resumes
    /// happened in between. Counting a window-verified resume as a full
    /// verification (refreshing the stored verification time on the
    /// carry-forward branch) would push the full check out forever on
    /// exactly the file it exists for; that mutant reddens this test.
    #[test]
    fn pi_file_growing_every_tick_still_gets_its_periodic_full_verification() {
        let temp = TempDir::new().unwrap();
        let contents = padded(
            format!(
                "{PAD_TEST_HEADER}\n{}\n",
                message_line("entry-1", "response-1", 11)
            ),
            PI_PREFIX_WINDOW_BYTES as usize * 2,
        );
        let path = write_fixture(temp.path(), "encoded", &contents);
        let state = test_state();
        let source = PiSource::at(temp.path());
        // t=0: the process-start full read, which sets the verification clock.
        assert_eq!(
            source.drain(&state, 0, &NEVER_STOP).unwrap().parsed.records,
            1
        );

        // Tick every 30 s of the drain clock, appending one record per tick.
        // On tick 5 the same append also carries a same-length rewrite of
        // entry-1, far outside the trailing window.
        let tick_ms = 30_000_i64;
        let ticks_per_interval = PI_FULL_PREFIX_VERIFY_INTERVAL_MS / tick_ms;
        let mut current = contents;
        for tick in 1..=ticks_per_interval {
            if tick == 5 {
                current = current.replacen(
                    &message_line("entry-1", "response-1", 11),
                    &message_line("entry-1", "response-1", 19),
                    1,
                );
            }
            current.push_str(&format!(
                "{}\n",
                message_line(&format!("entry-t{tick}"), &format!("response-t{tick}"), 21)
            ));
            fs::write(&path, &current).unwrap();
            let outcome = source.drain(&state, tick * tick_ms, &NEVER_STOP).unwrap();
            if tick * tick_ms < PI_FULL_PREFIX_VERIFY_INTERVAL_MS {
                assert_eq!(
                    outcome.files_resumed, 1,
                    "tick {tick}: no full verification is due yet"
                );
                assert_eq!(outcome.parsed.records, 1, "tick {tick}: append only");
                assert_eq!(outcome.quarantined_redeliveries, 0, "tick {tick}");
            } else {
                assert_eq!(
                    outcome.files_resumed, 0,
                    "the first drain at the interval must re-read in full"
                );
                assert_eq!(
                    outcome.parsed.records,
                    u64::try_from(ticks_per_interval).unwrap() + 1,
                    "the whole file is parsed again"
                );
                assert_eq!(outcome.quarantined_redeliveries, 1, "entry-1 changed");
            }
        }
    }

    /// A truncated file (size below the stored offset) and an atomic replace
    /// (same path, new inode) are both untrusted without any hashing: the
    /// drain reads the file in full. The inode gate is what a content check
    /// alone cannot see — an atomically replaced file can carry a
    /// byte-identical trailing window. Unix-only: the inode gate exists only
    /// where inodes do.
    #[cfg(unix)]
    #[test]
    fn pi_truncation_and_inode_change_force_a_full_reread() {
        let temp = TempDir::new().unwrap();
        let contents = format!(
            "{PAD_TEST_HEADER}\n{}\n{}\n",
            message_line("entry-1", "response-1", 11),
            message_line("entry-2", "response-2", 21)
        );
        let path = write_fixture(temp.path(), "encoded", &contents);
        let state = test_state();
        let source = PiSource::at(temp.path());
        assert_eq!(
            source
                .drain(&state, 100, &NEVER_STOP)
                .unwrap()
                .parsed
                .records,
            2
        );

        fs::write(
            &path,
            format!(
                "{PAD_TEST_HEADER}\n{}\n",
                message_line("entry-1", "response-1", 11)
            ),
        )
        .unwrap();
        let truncated = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(truncated.files_resumed, 0);
        assert_eq!(truncated.parsed.records, 1);
        assert_eq!(truncated.exact_redeliveries, 1);

        // Grow the file back with an ordinary append: sanity that the drain
        // still resumes when nothing suspicious happened.
        let grown = format!("{contents}{}\n", message_line("entry-3", "response-3", 31));
        fs::write(&path, &grown).unwrap();
        let rebuilt = source.drain(&state, 300, &NEVER_STOP).unwrap();
        assert_eq!(rebuilt.files_resumed, 1);
        assert_eq!(rebuilt.parsed.records, 2);

        // Replace the file atomically: new inode, byte-identical prefix
        // window, one new record.
        let staging = temp.path().join("staging.jsonl");
        fs::write(
            &staging,
            format!("{grown}{}\n", message_line("entry-4", "response-4", 41)),
        )
        .unwrap();
        fs::rename(&staging, &path).unwrap();
        let replaced = source.drain(&state, 400, &NEVER_STOP).unwrap();
        assert_eq!(
            replaced.files_resumed, 0,
            "an inode change forces a full re-read even with identical window bytes"
        );
        assert_eq!(replaced.parsed.records, 4);
        assert_eq!(replaced.rows_recorded, 1, "response-4 is new");
        assert_eq!(replaced.exact_redeliveries, 3);
    }

    /// Steady state — a grown file with an unchanged prefix: a tick must hash
    /// O(window + appended) bytes, not O(file). Asserted on the reader's
    /// hashed-byte counter, never on wall time. Hashing the full prefix per
    /// tick reddens this test.
    #[test]
    fn pi_steady_state_tick_hashes_a_bounded_window_not_the_whole_file() {
        let temp = TempDir::new().unwrap();
        let contents = padded(
            format!(
                "{PAD_TEST_HEADER}\n{}\n",
                message_line("entry-1", "response-1", 11)
            ),
            PI_PREFIX_WINDOW_BYTES as usize * 8,
        );
        let path = write_fixture(temp.path(), "encoded", &contents);
        let state = test_state();
        let source = PiSource::at(temp.path());
        source.drain(&state, 100, &NEVER_STOP).unwrap();

        let appended = format!("{}\n", message_line("entry-2", "response-2", 21));
        append(&path, &appended);
        let tick = source.drain(&state, 200, &NEVER_STOP).unwrap();

        assert_eq!(tick.files_resumed, 1);
        assert_eq!(tick.parsed.records, 1);
        // One window verification, one window re-anchoring, and the appended
        // line itself; the whole prefix alone is eight windows.
        let bound = 2 * PI_PREFIX_WINDOW_BYTES + appended.len() as u64 + 1024;
        assert!(
            tick.hashed_bytes <= bound,
            "steady-state tick hashed {} bytes (bound {bound}, file {})",
            tick.hashed_bytes,
            contents.len()
        );
        assert!(
            tick.hashed_bytes < contents.len() as u64 / 2,
            "the bound is only meaningful well below a full-file hash"
        );
    }

    /// An assistant record with no `timestamp` field.
    fn undated_message_line(entry: &str, response: &str, input: u64) -> String {
        format!(
            r#"{{"type":"message","id":"{entry}","message":{{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"{response}","usage":{{"input":{input},"output":7}}}}}}"#
        )
    }

    fn set_mtime(path: &Path, modified: std::time::SystemTime) {
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
    }

    fn epoch_plus_secs(secs: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)
    }

    /// A record without a timestamp reaches the ledger with no
    /// `occurred_at_ms` and is priced as undated. Proves the stored VALUE;
    /// the re-read test below proves it stays the same across reads.
    #[test]
    fn pi_record_without_timestamp_lands_undated() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &format!(
                "{PAD_TEST_HEADER}\n{}\n",
                undated_message_line("entry-1", "response-1", 11)
            ),
        );
        // A distinctive mtime, so a value copied from it would be obvious.
        set_mtime(&path, epoch_plus_secs(1_800_000_000));
        let state = test_state();
        let outcome = PiSource::at(temp.path())
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();
        assert_eq!(outcome.rows_recorded, 1);
        price_unpriced_events(&state, 200, &NEVER_STOP).unwrap();

        let stored: (Option<i64>, String, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT event.occurred_at_ms,MIN(charge.outcome),MIN(charge.unpriced_reason)
                     FROM usage_event AS event
                     JOIN priced_charge AS charge ON charge.event_key=event.event_key
                     WHERE event.producer='pi'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            stored,
            (None, "unpriced".to_string(), "undated_event".to_string())
        );
    }

    /// A full re-read of an unchanged file whose mtime moved (a restart does
    /// exactly this) delivers the undated record as an exact redelivery: no
    /// quarantine, no `rejected_delivery` row. Proves the CONSEQUENCE of a
    /// read-dependent time; the test above proves the value itself.
    #[test]
    fn pi_undated_record_reread_after_mtime_moves_is_an_exact_redelivery() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &format!(
                "{PAD_TEST_HEADER}\n{}\n",
                undated_message_line("entry-1", "response-1", 11)
            ),
        );
        set_mtime(&path, epoch_plus_secs(1_800_000_000));
        let state = test_state();
        let first = PiSource::at(temp.path())
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();
        assert_eq!(first.rows_recorded, 1);

        // Same bytes, later mtime, and a fresh source so nothing resumes.
        set_mtime(&path, epoch_plus_secs(1_800_086_400));
        let reread = PiSource::at(temp.path())
            .drain(&state, 200, &NEVER_STOP)
            .unwrap();
        assert_eq!(reread.files_resumed, 0, "a full re-read");
        assert_eq!(reread.parsed.records, 1);
        assert_eq!(reread.rows_recorded, 0);
        assert_eq!(reread.exact_redeliveries, 1);
        assert_eq!(reread.quarantined_redeliveries, 0);
        assert_eq!(table_count(&state, "rejected_delivery"), 0);
    }

    /// A complete line that is not a readable record is counted against its
    /// file and later lines still ingest. An unterminated final line is a
    /// write in flight, not a skipped line.
    #[test]
    fn pi_corrupt_complete_line_is_counted_and_a_partial_tail_is_not() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &format!(
                "{PAD_TEST_HEADER}\n{}\n{{\"type\":\"message\",\"id\":\"torn\n{}\n",
                message_line("entry-1", "response-1", 11),
                message_line("entry-2", "response-2", 21)
            ),
        );
        let state = test_state();
        let source = PiSource::at(temp.path());
        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(first.parsed.records, 2, "the record after the torn line");
        assert_eq!(first.rows_recorded, 2);
        assert_eq!(
            first.skipped_lines_by_file,
            BTreeMap::from([(path.clone(), 1)])
        );

        let line = message_line("entry-3", "response-3", 31);
        let (head, tail) = line.split_at(line.len() / 2);
        append(&path, head);
        let partial = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(partial.parsed.records, 0);
        assert!(
            partial.skipped_lines_by_file.is_empty(),
            "an in-flight tail is not a skipped line: {:?}",
            partial.skipped_lines_by_file
        );

        append(&path, &format!("{tail}\n"));
        let completed = source.drain(&state, 300, &NEVER_STOP).unwrap();
        assert_eq!(completed.rows_recorded, 1);
        assert!(completed.skipped_lines_by_file.is_empty());
    }

    /// A same-size rewrite inside the trailing window, with the mtime forced
    /// back to its old value, is caught on the next tick. Forcing the mtime
    /// makes this independent of the host's timestamp granularity: size and
    /// mtime both say "unchanged", and only reading the file can tell.
    #[test]
    fn pi_same_size_same_mtime_rewrite_is_caught_on_the_next_tick() {
        let temp = TempDir::new().unwrap();
        let contents = format!(
            "{PAD_TEST_HEADER}\n{}\n{}\n",
            message_line("entry-1", "response-1", 11),
            message_line("entry-2", "response-2", 21)
        );
        let path = write_fixture(temp.path(), "encoded", &contents);
        let state = test_state();
        let source = PiSource::at(temp.path());
        assert_eq!(
            source
                .drain(&state, 100, &NEVER_STOP)
                .unwrap()
                .rows_recorded,
            2
        );
        let before = fs::metadata(&path).unwrap();

        let rewritten = contents.replacen(
            &message_line("entry-2", "response-2", 21),
            &message_line("entry-2", "response-2", 29),
            1,
        );
        assert_eq!(rewritten.len(), contents.len());
        fs::write(&path, &rewritten).unwrap();
        set_mtime(&path, before.modified().unwrap());
        let after = fs::metadata(&path).unwrap();
        assert_eq!(
            (after.len(), after.modified().unwrap()),
            (before.len(), before.modified().unwrap())
        );

        let tick = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(tick.files_resumed, 0, "the window check must see it");
        assert_eq!(tick.parsed.records, 2, "the whole file is parsed again");
        assert_eq!(tick.exact_redeliveries, 1, "response-1 is unchanged");
        assert_eq!(
            tick.quarantined_redeliveries, 1,
            "response-2 kept its identity but changed payload"
        );
    }

    /// A tick over N files that did not change hashes at most one window per
    /// file (exactly each file's trailing window) and reads no new bytes.
    /// Asserted on the reader's counters, never on wall time.
    #[test]
    fn pi_steady_state_tick_over_unchanged_files_hashes_one_window_each() {
        let temp = TempDir::new().unwrap();
        let window = PI_PREFIX_WINDOW_BYTES as usize;
        let sizes = [window * 3, window + 1, window / 4];
        let mut expected_hashed = 0_u64;
        for (index, size) in sizes.iter().enumerate() {
            let contents = padded(
                format!(
                    "{PAD_TEST_HEADER}\n{}\n",
                    message_line(&format!("entry-{index}"), &format!("response-{index}"), 11)
                ),
                *size,
            );
            expected_hashed += (contents.len() as u64).min(PI_PREFIX_WINDOW_BYTES);
            write_fixture(temp.path(), &format!("encoded-{index}"), &contents);
        }
        let state = test_state();
        let source = PiSource::at(temp.path());
        assert_eq!(
            source
                .drain(&state, 100, &NEVER_STOP)
                .unwrap()
                .rows_recorded,
            3
        );

        let tick = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!((tick.files_read, tick.files_resumed), (3, 3));
        assert_eq!(tick.read_bytes, 0);
        assert_eq!(tick.parsed.records, 0);
        assert!(tick.hashed_bytes <= sizes.len() as u64 * PI_PREFIX_WINDOW_BYTES);
        assert_eq!(tick.hashed_bytes, expected_hashed);
    }

    /// A writer mid-append can leave a line without its newline. It is not
    /// consumed, so once completed it is read and ingested on the next drain.
    #[test]
    fn unterminated_trailing_pi_line_is_not_consumed_until_completed() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &fixture("pi-session-1", "/actual/project", Some("response-1")),
        );
        let state = test_state();
        let source = PiSource::at(temp.path());
        source.drain(&state, 100, &NEVER_STOP).unwrap();

        let line = message_line("entry-2", "response-2", 21);
        let (head, tail) = line.split_at(line.len() / 2);
        append(&path, head);
        let partial = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(partial.files_resumed, 1);
        assert_eq!(partial.parsed.records, 0);
        assert_eq!(table_count(&state, "usage_event"), 1);

        append(&path, &format!("{tail}\n"));
        let completed = source.drain(&state, 300, &NEVER_STOP).unwrap();
        assert_eq!(completed.files_resumed, 1, "the resume point stayed valid");
        assert_eq!(completed.parsed.records, 1);
        assert_eq!(completed.rows_recorded, 1);
        assert_eq!(table_count(&state, "usage_event"), 2);
    }

    #[test]
    fn same_pi_file_and_copied_path_redeliver_exactly_without_new_rows() {
        let temp = TempDir::new().unwrap();
        let contents = fixture("pi-session-1", "/actual/project", Some("response-1"));
        let original = write_fixture(temp.path(), "encoded-a", &contents);
        let copied = write_fixture(temp.path(), "encoded-b", &contents);
        let state = test_state();

        let first = drain_file_without_cache(&state, &original, 100).unwrap();
        let second = drain_file_without_cache(&state, &original, 200).unwrap();
        let copied_path = drain_file_without_cache(&state, &copied, 300).unwrap();
        assert_eq!(first.rows_recorded, 1);
        assert_eq!(second.rows_recorded, 0);
        assert_eq!(second.exact_redeliveries, 1);
        assert_eq!(copied_path.rows_recorded, 0);
        assert_eq!(copied_path.exact_redeliveries, 1);
        let rows: i64 = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM usage_event WHERE producer='pi'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(rows, 1, "file path and read time are not event identity");
    }

    /// A stop that arrives partway through recording a Pi drain rolls back
    /// every event of that drain. The stop is driven by a counter so it lands
    /// exactly after two events were written inside the transaction.
    #[test]
    fn stop_during_pi_ingest_rolls_back_every_event_of_the_drain() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &fixture("pi-session-1", "/actual/project", Some("response-1")),
        );
        let template = map_message(
            parse_pi_file_resumable(&path, None, PiPrefixCheck::Full)
                .messages
                .remove(0),
        )
        .unwrap();
        let requests = (0..5)
            .map(|index| EventRecordRequest {
                producer_ref: format!("response-{index}"),
                ..template.clone()
            })
            .collect::<Vec<_>>();
        let state = test_state();
        let asked = std::cell::Cell::new(0);

        let result = record_requests(
            &state,
            &requests,
            &[],
            100,
            false,
            &mut PiDrainOutcome::default(),
            &|| {
                asked.set(asked.get() + 1);
                asked.get() == 3
            },
        );

        assert_eq!(result.unwrap_err(), SourceDrainError::Stopped);
        assert_eq!(
            asked.get(),
            3,
            "the stop must land after two events were written, not before the first"
        );
        let recorded: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(recorded, 0);
    }

    #[test]
    fn pi_event_is_unpriced_because_charge_basis_is_unknown() {
        let temp = TempDir::new().unwrap();
        let path = write_fixture(
            temp.path(),
            "encoded",
            &fixture("pi-session-1", "/actual/project", Some("response-1")),
        );
        let state = test_state();
        drain_file_without_cache(&state, &path, 100).unwrap();
        assert_eq!(price_unpriced_events(&state, 200, &NEVER_STOP).unwrap(), 1);

        let result: (i64, String, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MIN(outcome),MIN(unpriced_reason)
                     FROM priced_charge
                     WHERE event_key=(SELECT event_key FROM usage_event WHERE producer='pi')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert!(result.0 > 0);
        assert_eq!(result.1, "unpriced");
        assert_eq!(result.2, "charge_basis_unknown");
    }

    #[test]
    fn absent_pi_root_is_no_lane_but_unreadable_root_is_cold_and_degraded() {
        let temp = TempDir::new().unwrap();
        let absent_state = test_state();
        clear_runtime(&absent_state);
        crate::refresher::harness_drain::ensure_runtime_row(&absent_state, PI_SOURCE_KEY).unwrap();
        let absent = PiSource::at(temp.path().join("missing"))
            .drain(&absent_state, 100, &NEVER_STOP)
            .unwrap();
        assert!(absent.root_absent);
        let absent_rows: i64 = absent_state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM source_runtime WHERE source_key='pi'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(absent_rows, 0, "an absent installation is not a Pi lane");
        assert_eq!(absent_state.public_projection().health(100).status, "ok");

        let unreadable = temp.path().join("not-a-directory");
        fs::write(&unreadable, b"present but cannot be traversed").unwrap();
        let unreadable_state = test_state();
        clear_runtime(&unreadable_state);
        let error = PiSource::at(&unreadable)
            .drain(&unreadable_state, 100, &NEVER_STOP)
            .unwrap_err()
            .to_string();
        assert!(error.contains("read Pi session root"), "{error}");
        assert_cold_pi_runtime_degrades(&unreadable_state);
    }

    struct BrocaEventSource;

    impl crate::refresher::SourceClient for BrocaEventSource {
        fn pull(
            &self,
            _since: Option<crate::refresher::SourceCursor>,
            _limit: u32,
        ) -> Result<ExportPage, crate::refresher::PullError> {
            let event: EventRecordRequest = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            let cursor = crate::refresher::SourceCursor {
                lineage: "broca-test:1".to_string(),
                ordinal: 7,
            };
            Ok(ExportPage {
                set_aside: Vec::new(),
                cursor: cursor.clone(),
                high_water: cursor,
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    #[test]
    fn pi_read_failure_does_not_prevent_broca_rows_committing_in_same_tick() {
        let temp = TempDir::new().unwrap();
        let unreadable = temp.path().join("not-a-directory");
        fs::write(&unreadable, b"present but cannot be traversed").unwrap();
        let pi = PiSource::at(unreadable);
        let state = test_state();
        let broca = Producer {
            source_key: "broca".to_string(),
            client: Arc::new(BrocaEventSource),
        };

        run_iteration_with_pi(&state, &[broca], &pi, &AtomicBool::new(false));

        let broca_rows: i64 = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM usage_event WHERE producer='broca'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            broca_rows, 1,
            "BROCA facts, not only its cursor, must commit"
        );
        assert!(state.producer_degraded_reason("pi").is_some());
        assert!(state.producer_degraded_reason("broca").is_none());

        let readable = TempDir::new().unwrap();
        write_fixture(
            readable.path(),
            "encoded",
            &fixture("pi-session-2", "/actual/project", Some("response-2")),
        );
        let state = test_state();
        let unavailable_broca = Producer {
            source_key: "broca".to_string(),
            client: Arc::new(crate::refresher::UnavailableSource),
        };
        run_iteration_with_pi(
            &state,
            &[unavailable_broca],
            &PiSource::at(readable.path()),
            &AtomicBool::new(false),
        );
        let pi_rows: i64 = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM usage_event WHERE producer='pi'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(pi_rows, 1, "a BROCA refusal must not roll back Pi facts");
        assert!(state.producer_degraded_reason("broca").is_some());
        assert!(state.producer_degraded_reason("pi").is_none());
    }

    #[derive(Debug, PartialEq, Eq)]
    struct LedgerTotals {
        events: u64,
        input: u64,
        source_output: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        reasoning: u64,
    }

    fn ledger_totals(state: &SharedState, start_ms: i64, end_ms: i64) -> LedgerTotals {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(DISTINCT event.event_id),
                            COALESCE(SUM(CASE WHEN measurement.role='input' THEN measurement.quantity_units ELSE 0 END),0),
                            COALESCE(SUM(CASE WHEN measurement.role IN ('output','reasoning') THEN measurement.quantity_units ELSE 0 END),0),
                            COALESCE(SUM(CASE WHEN measurement.role='output' THEN measurement.quantity_units ELSE 0 END),0),
                            COALESCE(SUM(CASE WHEN measurement.role='cache_read' THEN measurement.quantity_units ELSE 0 END),0),
                            COALESCE(SUM(CASE WHEN measurement.role='cache_write' THEN measurement.quantity_units ELSE 0 END),0),
                            COALESCE(SUM(CASE WHEN measurement.role='reasoning' THEN measurement.quantity_units ELSE 0 END),0)
                     FROM usage_event AS event
                     JOIN measurement ON measurement.event_key=event.event_key
                     WHERE event.producer='pi' AND event.occurred_at_ms BETWEEN ?1 AND ?2",
                    params![start_ms, end_ms],
                    |row| {
                        Ok(LedgerTotals {
                            events: row.get(0)?,
                            input: row.get(1)?,
                            source_output: row.get(2)?,
                            output: row.get(3)?,
                            cache_read: row.get(4)?,
                            cache_write: row.get(5)?,
                            reasoning: row.get(6)?,
                        })
                    },
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    #[test]
    #[ignore = "reads and temporarily ingests the mutable ~/.pi/agent/sessions corpus"]
    /// Reports the cost of a second, steady-state drain over the live Pi tree
    /// into a temporary in-memory store. When nothing grew between the two
    /// drains the tick must hash at most one window per file.
    fn live_pi_steady_state_tick_cost() {
        let root = std::env::var_os("PI_SESSION_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap()).join(".pi/agent/sessions")
            });
        let files = jsonl_files(&root).unwrap().len();
        let state = test_state();
        let source = PiSource::at(root);
        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();
        eprintln!("files={files} first={first:#?}");
        eprintln!("steady={second:#?}");
        if second.read_bytes == 0 {
            assert!(second.hashed_bytes <= files as u64 * PI_PREFIX_WINDOW_BYTES);
        }
    }

    #[test]
    #[ignore = "reads and temporarily ingests the mutable ~/.pi/agent/sessions corpus"]
    fn live_pi_corpus_parse_and_ledger_totals_are_distinct_ground_truths() {
        let root = std::env::var_os("PI_SESSION_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap()).join(".pi/agent/sessions")
            });
        let start_ms = DateTime::parse_from_rfc3339("2026-02-28T16:16:45.881Z")
            .unwrap()
            .timestamp_millis();
        let end_ms = DateTime::parse_from_rfc3339("2026-09-20T13:06:38.259Z")
            .unwrap()
            .timestamp_millis();
        // `test_state` is an in-memory temporary store. The live production
        // store is never opened by this reconciliation.
        let state = test_state();
        clear_runtime(&state);
        let files = jsonl_files(&root).unwrap();
        let outcome = PiSource::at(root)
            .drain_in_window(&state, end_ms, start_ms, end_ms)
            .unwrap();

        // The sibling reader test establishes a third population: 40,257 raw
        // assistant+usage objects. It is a corpus denominator, not a reader
        // test. `parse_pi_file` additionally requires a model and emits this
        // smaller population; the ledger below is reached through that API, so
        // an empty or lossy reader cannot pass by keeping identity self-consistent.
        let reader_expected = PiTotals {
            records: 33_198,
            input: 311_535_017,
            source_output: 9_481_286,
            output: 7_015_223,
            cache_read: 4_985_502_012,
            cache_write: 8_914_936,
            reasoning: 2_466_063,
        };
        assert_eq!(outcome.parsed, reader_expected);
        // `parse_pi_file` omits undated records, which the drain keeps. The
        // live corpus has none, so both paths emit the same population.
        let mut parse_pi_file_totals = PiTotals::default();
        for message in files.iter().flat_map(|path| parse_pi_file(path)) {
            if (start_ms..=end_ms).contains(&message.timestamp) {
                parse_pi_file_totals.add_message(&message);
            }
        }
        assert_eq!(parse_pi_file_totals, reader_expected);

        // Established 2026-09-20 for the bounded window above. Reader-emitted
        // totals fence the reader; ledger totals fence identity. They differ
        // because Pi forks copy prior records and duplicate session files
        // exist, not because the drain dropped unread data. Neither assertion
        // can replace the other: a skipped file can leave ledger totals
        // self-consistent, while an identity collision leaves reader totals
        // unchanged.
        let ledger_expected = LedgerTotals {
            events: 30_947,
            input: 293_775_400,
            source_output: 8_860_104,
            output: 6_394_041,
            cache_read: 4_617_829_454,
            cache_write: 6_548_429,
            reasoning: 2_466_063,
        };
        let ledger = ledger_totals(&state, start_ms, end_ms);
        eprintln!("Pi parse totals: {:#?}", outcome.parsed);
        eprintln!("Pi ledger totals: {ledger:#?}");
        eprintln!("Pi drain outcome: {outcome:#?}");
        assert_eq!(ledger, ledger_expected);
        assert_eq!(outcome.observations_deduplicated, 2_251);
        assert_eq!(
            outcome.quarantined_redeliveries, 0,
            "clean fork copies must be deduplicated before ingest, not quarantined"
        );
    }

    fn header_line(session: &str, timestamp: &str) -> String {
        format!(
            r#"{{"type":"session","id":"{session}","timestamp":"{timestamp}","cwd":"/{session}"}}"#
        )
    }

    /// A session header the reader accepts but the rank search does not
    /// recognise: the reader strips a leading byte-order mark and the search
    /// does not, so records in a file that starts with this line are accepted
    /// and ranked as headerless (`i64::MAX`).
    fn bom_header_line() -> String {
        format!("\u{feff}{}", header_line("bom", "2026-07-01T00:00:00.000Z"))
    }

    /// Every recorded event, measurement and refused delivery, every column,
    /// in a stable order, so two stores can be compared whole.
    fn ledger_dump(state: &SharedState) -> Vec<String> {
        state
            .with_conn(|conn| {
                let mut dump = Vec::new();
                for (table, order) in [
                    ("usage_event", "event_id"),
                    ("measurement", "event_key,role,unit,kind,source"),
                    ("rejected_delivery", "rowid"),
                ] {
                    let mut statement = conn
                        .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))
                        .map_err(|error| error.to_string())?;
                    let columns = statement.column_count();
                    let rows = statement
                        .query_map([], |row| {
                            let values = (0..columns)
                                .map(|index| Ok(format!("{:?}", row.get_ref(index)?)))
                                .collect::<rusqlite::Result<Vec<_>>>()?;
                            Ok(format!("{table}: {}", values.join(" | ")))
                        })
                        .map_err(|error| error.to_string())?;
                    for row in rows {
                        dump.push(row.map_err(|error| error.to_string())?);
                    }
                }
                Ok(dump)
            })
            .unwrap()
    }

    /// Caching a file's session-header search changes no recorded byte. One
    /// source keeps its header cache across drains; the other has it emptied
    /// before every drain, so it searches every file from its first byte the
    /// way every drain did before the cache existed. Both drain the same files
    /// through the same appends into their own stores, and the stores must be
    /// identical after every drain.
    ///
    /// The fixture covers each case the cache could get wrong: fork copies
    /// whose owner is decided by header rank, a headerless file (rank
    /// `i64::MAX`) whose copies lose to a headed file, a header that appears
    /// only after a headerless file was already searched and then outranks
    /// the other copy, an unterminated header line that is not yet
    /// remembered, and a rewritten file whose earlier search must not be
    /// reused.
    #[test]
    fn cached_pi_header_rank_records_exactly_what_a_full_header_search_does() {
        let temp = TempDir::new().unwrap();
        let headed = write_fixture(
            temp.path(),
            "headed",
            &format!(
                "{}\n{}\n",
                header_line("headed", "2026-09-01T00:00:00.000Z"),
                message_line("entry-1", "response-1", 11)
            ),
        );
        let headerless = write_fixture(
            temp.path(),
            "headerless",
            &format!(
                "{}\n{}\n{}\n",
                bom_header_line(),
                message_line("entry-1", "response-1", 11),
                message_line("entry-2", "response-2", 21)
            ),
        );
        let cached_state = test_state();
        let cached = PiSource::at(temp.path());
        let searching_state = test_state();
        let searching = PiSource::at(temp.path());
        let mut now = 100;
        let mut drain_both = |step: &str| {
            searching.cache.lock().unwrap().header_scans.clear();
            cached.drain(&cached_state, now, &NEVER_STOP).unwrap();
            searching.drain(&searching_state, now, &NEVER_STOP).unwrap();
            now += 100;
            let expected = ledger_dump(&searching_state);
            assert!(!expected.is_empty(), "{step}: the fixture recorded nothing");
            assert_eq!(ledger_dump(&cached_state), expected, "{step}");
        };
        drain_both("first read");

        // Both files gain a copy of the same call; the headed file owns it.
        append(
            &headed,
            &format!("{}\n", message_line("entry-3", "response-3", 31)),
        );
        append(
            &headerless,
            &format!("{}\n", message_line("entry-3", "response-3", 31)),
        );
        drain_both("copies split across a headed and a headerless file");

        // An unterminated header line ranked earlier than the headed file:
        // the full search reads it, so the new copy in the headerless file
        // outranks the headed one. It is not complete, so it is not cached.
        append(
            &headerless,
            &format!(
                "{}\n{}",
                message_line("entry-4", "response-4", 41),
                header_line("late", "2026-08-01T00:00:00.000Z")
            ),
        );
        append(
            &headed,
            &format!("{}\n", message_line("entry-4", "response-4", 41)),
        );
        drain_both("an unterminated header line");

        // The header line completes, so later copies in the formerly
        // headerless file outrank copies in the headed file.
        append(
            &headerless,
            &format!("\n{}\n", message_line("entry-5", "response-5", 51)),
        );
        append(
            &headed,
            &format!("{}\n", message_line("entry-5", "response-5", 51)),
        );
        drain_both("a header that arrived after the file was searched");

        append(
            &headerless,
            &format!("{}\n", message_line("entry-6", "response-6", 61)),
        );
        append(
            &headed,
            &format!("{}\n", message_line("entry-6", "response-6", 61)),
        );
        drain_both("both headers already found");

        // A rewritten file is read again from the start, and its header is
        // searched again from the start. The rewritten headed file's header
        // is dated before the other file's, so its copy now ranks first.
        fs::write(
            &headed,
            format!(
                "{}\n{}\n",
                header_line("headed", "2026-06-01T00:00:00.000Z"),
                message_line("entry-7", "response-7", 71)
            ),
        )
        .unwrap();
        append(
            &headerless,
            &format!("{}\n", message_line("entry-7", "response-7", 71)),
        );
        drain_both("a rewritten file");
        drain_both("nothing changed");

        // The fixture did reach both outcomes of the rank: the headed file
        // owns the copies made while the other file had no recognised header
        // (responses 1 and 3), the other file owns every copy made once its
        // later, earlier-dated header was readable (responses 4 to 6), plus
        // response 2, which only it holds, and the rewritten headed file owns
        // response 7 under its new, still earlier header.
        let owners: Vec<(Option<String>, i64)> = cached_state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT session,COUNT(*) FROM usage_event GROUP BY session ORDER BY session")
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(|error| error.to_string())?;
                rows.collect::<rusqlite::Result<_>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            owners,
            vec![
                (Some("bom".to_string()), 4),
                (Some("headed".to_string()), 3)
            ]
        );
    }

    /// A file's session header is searched for once. A file whose header was
    /// found costs no header read on later drains, an unchanged file costs
    /// none, and a file with no header yet costs only its appended bytes.
    #[test]
    fn pi_header_rank_is_not_searched_again_for_unchanged_or_grown_files() {
        let temp = TempDir::new().unwrap();
        let header = header_line("headed", "2026-09-01T00:00:00.000Z");
        let headed = write_fixture(
            temp.path(),
            "headed",
            &format!(
                "{header}\n{}\n{}\n",
                message_line("entry-1", "response-1", 11),
                message_line("entry-2", "response-2", 21)
            ),
        );
        let headerless_contents = format!(
            "{}\n{}\n",
            bom_header_line(),
            message_line("entry-9", "response-9", 91)
        );
        let headerless = write_fixture(temp.path(), "headerless", &headerless_contents);
        let state = test_state();
        let source = PiSource::at(temp.path());

        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(first.rows_recorded, 3);
        assert_eq!(
            first.header_scan_bytes,
            (header.len() + 1 + headerless_contents.len()) as u64,
            "the first drain reads the headed file's header line and all of the headerless file"
        );

        let unchanged = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(unchanged.parsed.records, 0);
        assert_eq!(unchanged.header_scan_bytes, 0, "unchanged files");

        let appended = format!("{}\n", message_line("entry-10", "response-10", 101));
        append(
            &headed,
            &format!("{}\n", message_line("entry-3", "response-3", 31)),
        );
        append(&headerless, &appended);
        let grown = source.drain(&state, 300, &NEVER_STOP).unwrap();
        assert_eq!(grown.files_resumed, 2);
        assert_eq!(grown.rows_recorded, 2);
        assert_eq!(
            grown.header_scan_bytes,
            appended.len() as u64,
            "only the headerless file's appended bytes are searched"
        );
    }
}
