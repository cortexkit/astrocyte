//! Shared harness drain transactions and resume plumbing live here so file readers use one policy.

pub(crate) fn record_set_aside_in_tx(
    tx: &rusqlite::Transaction<'_>,
    source: &str,
    records: &[super::SetAsideRecord],
    now_ms: i64,
) -> Result<Vec<super::SetAsideRecord>, String> {
    let mut newly_seen = Vec::new();
    for record in records {
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO set_aside_record (source_key,locator,reason,first_seen_ms,last_seen_ms) VALUES (?1,?2,?3,?4,?4)",
            rusqlite::params![source, record.locator, record.reason, now_ms],
        ).map_err(|e| format!("record set-aside evidence: {e}"))?;
        if inserted == 1 {
            newly_seen.push(record.clone());
        } else {
            tx.execute("UPDATE set_aside_record SET last_seen_ms=MAX(last_seen_ms,?3) WHERE source_key=?1 AND locator=?2", rusqlite::params![source, record.locator, now_ms]).map_err(|e| format!("refresh set-aside evidence: {e}"))?;
        }
    }
    Ok(newly_seen)
}

pub(crate) fn warn_set_aside(source: &str, newly_seen: &[super::SetAsideRecord]) {
    for record in newly_seen {
        tracing::warn!(source, locator = %record.locator, reason = %record.reason, "producer record set aside");
    }
}
#[cfg(test)]
pub(crate) fn assert_set_aside(state: &crate::state::SharedState, source: &str, expected: i64) {
    state.with_conn(|conn| {
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM set_aside_record WHERE source_key=?1", [source], |row| row.get(0)).map_err(|e| e.to_string())?;
        assert_eq!(count, expected);
        let (locator, reason, first, last): (String, String, i64, i64) = conn.query_row("SELECT locator,reason,first_seen_ms,last_seen_ms FROM set_aside_record WHERE source_key=?1 LIMIT 1", [source], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(|e| e.to_string())?;
        assert!(!locator.is_empty() && !reason.is_empty());
        assert!(first <= last);
        let coverage = astrocyte_core::report::ledger_coverage(conn, &astrocyte_core::report::LedgerCoverageRequest {}, 200).map_err(|e| format!("{e:?}"))?;
        assert_eq!(
            coverage.set_aside_by_source[source].records,
            u64::try_from(expected).expect("expected record count is nonnegative")
        );
        assert_eq!(coverage.set_aside_by_source[source].newest_last_seen_ms, Some(last));
        Ok(())
    }).unwrap();
}

#[cfg(test)]
pub(crate) fn assert_good_snapshot(
    state: &crate::state::SharedState,
    source: &str,
    expected: &str,
) {
    use sha2::{Digest, Sha256};
    let bytes = state
        .with_conn(|conn| {
            let mut bytes = Vec::new();
            for sql in [
                // Expected hashes cover the original event, measurement and
                // settlement values. Integer IDs can change on a rebuild, so
                // omit the local event key and replace child keys with their
                // event_id strings. Changes to the facts still change the hash.
                "SELECT * FROM usage_event ORDER BY event_id",
                "SELECT e.event_id,m.role,m.unit,m.kind,m.source,m.quantity_units,m.quantity_exponent FROM measurement m JOIN usage_event e USING(event_key) ORDER BY e.event_id,m.role,m.unit",
                "SELECT e.event_id,t.from_state,t.to_state,t.reason,t.resolved_by,t.occurred_at_ms FROM chargeability_transition t JOIN usage_event e USING(event_key) ORDER BY e.event_id",
            ] {
                let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
                let columns: Vec<usize> = (0..stmt.column_count())
                    .filter(|col| stmt.column_name(*col).unwrap() != "event_key")
                    .collect();
                // These columns were added after the hashes were pinned.
                // Missing flow, media, paying-credential and runner facts stay NULL.
                // Leave only those NULLs out so each pinned hash still proves
                // every earlier column is unchanged; values are always hashed.
                let added_columns: Vec<usize> = (0..stmt.column_count())
                    .filter(|col| matches!(stmt.column_name(*col).unwrap(), "flow_ref" | "media_facts" | "resolved_credential" | "runner"))
                    .collect();
                let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
                while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                    for col in &columns {
                        let value = row.get_ref(*col).map_err(|e| e.to_string())?;
                        if added_columns.contains(col)
                            && matches!(value, rusqlite::types::ValueRef::Null)
                        {
                            continue;
                        }
                        bytes.extend(format!("{value:?}|").as_bytes());
                    }
                    bytes.push(b'\n');
                }
            }
            Ok(bytes)
        })
        .unwrap();
    let hash = format!("{:x}", Sha256::digest(&bytes));
    assert_eq!(hash, expected, "{source}: stored ledger bytes changed");
}

/// Every `step_detail` row as `(producer_ref, finish, context_limit)`,
/// ordered by `producer_ref`.
#[cfg(test)]
pub(crate) fn step_details(
    state: &crate::state::SharedState,
) -> Vec<(String, Option<String>, Option<i64>)> {
    state
        .with_conn(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT e.producer_ref,d.finish,d.context_limit FROM step_detail d
                         JOIN usage_event e USING(event_key) ORDER BY e.producer_ref",
                )
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string());
            rows
        })
        .unwrap()
}
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use astrocyte_core::ingest::{EventRecordRequest, MeasurementInput, RecordDisposition};
use astrocyte_readers::sessions::jsonl_resume::PrefixCheck;
use astrocyte_readers::sessions::roles::DerivedRole;
use rusqlite::params;

use super::SourceDrainError;
use crate::state::SharedState;

/// A harness store drained once per refresher tick.
pub(crate) trait HarnessSource: Send + Sync {
    /// The `source_runtime` key and the ledger's `producer`.
    fn source_key(&self) -> &'static str;

    /// Drain the store and commit what was read. `Ok(None)` is a healthy
    /// drain; `Ok(Some(reason))` committed but saw something that must
    /// be surfaced (a quarantine, usage it could not ingest).
    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError>;

    /// A durable notice requires an actual read, not an absence-backoff hit.
    fn drain_after_notice(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        self.drain_for_health(state, recorded_at_ms, stop)
    }

    /// Read only cheap tails. Sources without an incremental path stay on full ticks.
    fn drain_fast(
        &self,
        _state: &SharedState,
        _recorded_at_ms: i64,
        _stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainMode {
    Full,
    Fast,
}

impl DrainMode {
    pub(crate) fn files(
        self,
        windows: &std::collections::HashMap<PathBuf, WindowSchedule>,
        now: i64,
        walk: impl FnOnce() -> std::io::Result<Vec<PathBuf>>,
    ) -> std::io::Result<Vec<PathBuf>> {
        if self.is_full() {
            walk()
        } else {
            let mut files: Vec<_> = windows
                .iter()
                .filter(|(_, window)| window.hot(now))
                .map(|(path, _)| path.clone())
                .collect();
            files.sort();
            Ok(files)
        }
    }
    pub(crate) fn write<T>(
        self,
        state: &SharedState,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<(T, bool), String>,
    ) -> Result<T, String> {
        if self.is_full() {
            state.ordered_write(operation)
        } else {
            state.ordered_tail_write(operation)
        }
    }
    pub(crate) fn is_full(self) -> bool {
        self == Self::Full
    }

    pub(crate) fn has_work(self, events: usize, evidence: usize) -> bool {
        self.is_full() || events != 0 || evidence != 0
    }

    pub(crate) fn prefix_check(
        self,
        path: &Path,
        resumed: bool,
        verified: Option<i64>,
        now: i64,
    ) -> PrefixCheck {
        if self.is_full() {
            prefix_check(path, resumed, verified, now)
        } else {
            PrefixCheck::Window
        }
    }

    pub(crate) fn verified_after(
        self,
        check: PrefixCheck,
        resumed: bool,
        verified: Option<i64>,
        now: i64,
    ) -> Option<i64> {
        if self.is_full() {
            verified_after(check, resumed, verified, now)
        } else {
            verified
        }
    }

    pub(crate) fn ensure_runtime(self, state: &SharedState, key: &str) -> Result<(), String> {
        if self.is_full() {
            ensure_runtime_row(state, key)?;
        }
        Ok(())
    }

    pub(crate) fn remove_runtime(self, state: &SharedState, key: &str) -> Result<(), String> {
        if self.is_full() {
            remove_runtime_row(state, key)?;
        }
        Ok(())
    }
}

pub(crate) const PAGE_LIMIT: u32 = 10_000;

/// How often a file's whole consumed prefix is re-hashed instead of only
/// the trailing window, on the drain's clock. Gemini JSON documents retain
/// their separate hourly policy.
pub(crate) const FULL_PREFIX_VERIFY_INTERVAL_MS: i64 = super::jsonl_schedule::DAILY_VERIFY_MS;
pub(crate) use super::jsonl_schedule::{FileStat, WindowSchedule};

/// Resume points and full-verification times for one source's files.
///
/// Resume points are moved into the next read and published after commit.
/// A failed or stopped drain leaves no points, so the next drain rereads
/// the files; content-derived identity makes that conservative replay safe.
#[derive(Debug)]
pub(crate) struct ResumeCache<P> {
    pub resume: HashMap<PathBuf, P>,
    pub prefix_verified_ms: HashMap<PathBuf, i64>,
    pub windows: HashMap<PathBuf, WindowSchedule>,
}

impl<P> Default for ResumeCache<P> {
    fn default() -> Self {
        Self {
            resume: HashMap::new(),
            prefix_verified_ms: HashMap::new(),
            windows: HashMap::new(),
        }
    }
}

/// A resumed file gets a full check at its next daily phase, when its
/// verification time is unknown, or when the drain clock moves backwards.
pub(crate) fn prefix_check(
    path: &Path,
    has_resume_point: bool,
    verified_ms: Option<i64>,
    recorded_at_ms: i64,
) -> PrefixCheck {
    if has_resume_point
        && super::jsonl_schedule::full_due(
            path,
            verified_ms,
            recorded_at_ms,
            FULL_PREFIX_VERIFY_INTERVAL_MS,
        )
    {
        PrefixCheck::Full
    } else {
        PrefixCheck::Window
    }
}

/// When the file's whole consumed prefix is next known verified: now,
/// after a full check or a read from the start; else the old time.
pub(crate) fn verified_after(
    check: PrefixCheck,
    resumed: bool,
    previous_verified_ms: Option<i64>,
    recorded_at_ms: i64,
) -> Option<i64> {
    if check == PrefixCheck::Full || !resumed {
        Some(recorded_at_ms)
    } else {
        previous_verified_ms
    }
}

/// Every regular file below `root` at any depth whose name `keep`
/// accepts, sorted.
pub(crate) fn files_below(
    root: &Path,
    keep: &dyn Fn(&Path) -> bool,
) -> std::io::Result<Vec<PathBuf>> {
    matching_files(root, keep, &|path, _| path)
}

/// Sorted regular files and their directory depth below the root.
/// Do not follow symlinks into other transcript trees or directory cycles.
pub(crate) fn files_with_depth(
    root: &Path,
    keep: &dyn Fn(&Path) -> bool,
) -> std::io::Result<Vec<(PathBuf, usize)>> {
    matching_files(root, keep, &|path, depth| (path, depth))
}

fn matching_files<T: Ord>(
    root: &Path,
    keep: &dyn Fn(&Path) -> bool,
    map: &dyn Fn(PathBuf, usize) -> T,
) -> std::io::Result<Vec<T>> {
    fn visit<T>(
        path: &Path,
        depth: usize,
        keep: &dyn Fn(&Path) -> bool,
        map: &dyn Fn(PathBuf, usize) -> T,
        files: &mut Vec<T>,
    ) -> std::io::Result<()> {
        for entry in astrocyte_readers::read_io::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                visit(&path, depth + 1, keep, map, files)?;
            } else if file_type.is_file() && keep(&path) {
                files.push(map(path, depth));
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, 0, keep, map, &mut files)?;
    files.sort();
    Ok(files)
}

pub(crate) fn jsonl_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    files_below(root, &|path| {
        path.extension()
            .is_some_and(|extension| extension == "jsonl")
    })
}

#[derive(Debug, Clone)]
pub(crate) enum SourceRoot {
    Path(PathBuf),
    Unresolved(String),
}

/// Return `Ok(None)` for a missing path, but an error when it cannot be
/// inspected: a failed inspection must not hide an unreadable source.
pub(crate) fn present_root_in_lane<'a>(
    state: &SharedState,
    key: &str,
    root: &'a SourceRoot,
    label: &str,
    mode: DrainMode,
) -> Result<Option<&'a Path>, String> {
    let path = match root {
        SourceRoot::Path(path) => path,
        SourceRoot::Unresolved(reason) => {
            mode.ensure_runtime(state, key)?;
            return Err(reason.clone());
        }
    };
    if !mode.is_full() {
        return Ok(Some(path));
    }
    match astrocyte_readers::read_io::metadata(path) {
        Ok(_) => {
            record_source_discovery(state, key, path.display(), true);
            mode.ensure_runtime(state, key)?;
            Ok(Some(path))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            record_source_discovery(state, key, path.display(), false);
            mode.remove_runtime(state, key)?;
            Ok(None)
        }
        Err(error) => {
            mode.ensure_runtime(state, key)?;
            Err(format!("inspect {label} root {}: {error}", path.display()))
        }
    }
}

pub(crate) fn record_source_discovery(
    state: &SharedState,
    source_key: &str,
    path: impl std::fmt::Display,
    present: bool,
) {
    if !state.source_discovery_changed(source_key, present) {
        return;
    }
    if present {
        tracing::info!(
            target: "drain",
            source = source_key,
            path = %path,
            outcome = "present",
            "source store found"
        );
    } else {
        tracing::info!(
            target: "drain",
            source = source_key,
            path = %path,
            outcome = "absent",
            "no source store at the resolved path; the source is not ingested"
        );
    }
}

pub(crate) fn ensure_runtime_row(state: &SharedState, key: &str) -> Result<(), String> {
    state.ordered_write(|tx| {
        let changed = ensure_runtime_row_in_tx(tx, key)
            .map_err(|error| format!("initialize {key} source runtime: {error}"))?;
        Ok(((), changed > 0))
    })
}

pub(crate) fn ensure_runtime_row_in_tx(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> rusqlite::Result<usize> {
    tx.execute(
        "INSERT OR IGNORE INTO source_runtime
             (source_key,source_module,desired_generation,pending_known,pending_unknown)
             SELECT ?1,?1,generation,0,1 FROM desired_config WHERE id=1",
        [key],
    )
}

pub(crate) fn remove_runtime_row(state: &SharedState, key: &str) -> Result<(), String> {
    state.ordered_write(|tx| {
        let changed = tx
            .execute("DELETE FROM source_runtime WHERE source_key=?1", [key])
            .map_err(|error| format!("remove absent {key} source runtime: {error}"))?;
        Ok(((), changed > 0))
    })
}

/// A token measurement: `billed` counters whose source names the
/// harness field, as the other session readers store them.
pub(crate) fn measurement(
    role: &str,
    source: &str,
    quantity: Option<u64>,
) -> Result<MeasurementInput, String> {
    measurement_with_unit(role, "token", source, quantity)
}

pub(crate) fn measurement_with_unit(
    role: &str,
    unit: &str,
    source: &str,
    quantity: Option<u64>,
) -> Result<MeasurementInput, String> {
    let quantity_units = quantity
        .map(|quantity| {
            i64::try_from(quantity).map_err(|_| format!("{source} exceeds stored integer range"))
        })
        .transpose()?;
    Ok(MeasurementInput {
        role: role.to_string(),
        unit: unit.to_string(),
        kind: "billed".to_string(),
        source: source.to_string(),
        quantity_exponent: quantity_units.map(|_| 0),
        quantity_units,
    })
}

/// A measurement from a derived role. A conflicted or underivable role
/// is stored WITHOUT a quantity: the ledger shows the role was reported
/// and could not be priced, instead of dropping it or storing a guess.
pub(crate) fn derived_measurement(
    role: &str,
    source: &str,
    derived: DerivedRole,
) -> Result<Option<MeasurementInput>, String> {
    let (source, quantity) = match derived {
        DerivedRole::Absent => return Ok(None),
        DerivedRole::Measured(quantity) => (source.to_string(), Some(quantity)),
        DerivedRole::Conflicted => (format!("{source}:conflicted"), None),
        DerivedRole::Underivable => (format!("{source}:underivable"), None),
    };
    measurement(role, &source, quantity).map(Some)
}

/// A measurement for a counter stored as reported, or none when absent.
pub(crate) fn verbatim_measurement(
    role: &str,
    source: &str,
    quantity: Option<u64>,
) -> Result<Option<MeasurementInput>, String> {
    quantity
        .map(|quantity| measurement(role, source, Some(quantity)))
        .transpose()
}

/// What one commit did with its requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RecordTally {
    pub recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
}

impl RecordTally {
    pub(crate) fn from_dispositions(
        dispositions: impl IntoIterator<Item = RecordDisposition>,
    ) -> Self {
        let mut tally = Self::default();
        for disposition in dispositions {
            match disposition {
                RecordDisposition::Recorded => tally.recorded += 1,
                RecordDisposition::ExactRedelivery => tally.exact_redeliveries += 1,
                RecordDisposition::QuarantinedRedelivery => tally.quarantined_redeliveries += 1,
            }
        }
        tally
    }
}

pub(crate) fn mark_current_in_tx(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
    label: &str,
    recorded_at_ms: i64,
) -> Result<(), String> {
    let updated = tx
        .execute(
            "UPDATE source_runtime
             SET lineage=NULL,consumed_ordinal=NULL,high_water_ordinal=NULL,
                 last_success_ms=?2,pending_known=0,pending_unknown=0
             WHERE source_key=?1",
            params![key, recorded_at_ms],
        )
        .map_err(|error| format!("record {label} source runtime: {error}"))?;
    if updated != 1 {
        return Err(format!(
            "{label} source runtime row disappeared before commit"
        ));
    }
    Ok(())
}

pub(crate) fn record_events_in_tx(
    tx: &rusqlite::Transaction<'_>,
    label: &str,
    requests: &[EventRecordRequest],
    recorded_at_ms: i64,
    stop_requested: &dyn Fn() -> bool,
    stopped: &mut bool,
) -> Result<Vec<RecordDisposition>, String> {
    let mut dispositions = Vec::with_capacity(requests.len());
    for request in requests {
        if stop_requested() {
            *stopped = true;
            return Err(format!("{label} ingest stopped"));
        }
        let result = astrocyte_core::ingest::record_event_in_tx(
            tx,
            request,
            recorded_at_ms,
            astrocyte_core::ingest::DeliveryPath::Read,
        )
        .map_err(|error| format!("record {label} event: {error:?}"))?;
        dispositions.push(result.disposition);
    }
    Ok(dispositions)
}

/// Record every request in one transaction and mark the source current.
///
/// `stop_requested` is asked before every event; when it answers true the
/// transaction is rolled back, so no event from this drain is left
/// behind.
pub(crate) fn record_requests(
    state: &SharedState,
    key: &str,
    requests: &[EventRecordRequest],
    recorded_at_ms: i64,
    stop_requested: &dyn Fn() -> bool,
) -> Result<RecordTally, SourceDrainError> {
    record_requests_in_lane(
        state,
        key,
        requests,
        recorded_at_ms,
        stop_requested,
        DrainMode::Full,
    )
}

pub(crate) fn record_requests_in_lane(
    state: &SharedState,
    key: &str,
    requests: &[EventRecordRequest],
    recorded_at_ms: i64,
    stop_requested: &dyn Fn() -> bool,
    mode: DrainMode,
) -> Result<RecordTally, SourceDrainError> {
    if !mode.has_work(requests.len(), 0) {
        return Ok(RecordTally::default());
    }
    let mut stopped = false;
    let committed = mode.write(state, |tx| {
        if requests.is_empty() && stop_requested() {
            stopped = true;
            return Err(format!("{key} ingest stopped"));
        }
        let dispositions = record_events_in_tx(
            tx,
            key,
            requests,
            recorded_at_ms,
            stop_requested,
            &mut stopped,
        )?;
        if mode.is_full() {
            mark_current_in_tx(tx, key, key, recorded_at_ms)?;
        }
        Ok((dispositions, true))
    });
    let dispositions = match committed {
        Ok(dispositions) => dispositions,
        Err(_) if stopped => return Err(SourceDrainError::Stopped),
        Err(reason) => return Err(SourceDrainError::Failed(reason)),
    };
    Ok(RecordTally::from_dispositions(dispositions))
}

/// Mark a harness source degraded with `reason` and its pending work
/// unknown: after a failed or anomalous drain, what is left to read is
/// not known.
pub(crate) fn mark_drain_failed(state: &SharedState, key: &str, reason: String) {
    let degraded = state.ordered_write(|tx| {
        let changed = tx
            .execute(
                "UPDATE source_runtime SET pending_unknown=1 WHERE source_key=?1",
                [key],
            )
            .map_err(|error| error.to_string())?;
        if changed != 1 {
            return Err(format!(
                "{key} source runtime row missing while marking drain failed"
            ));
        }
        Ok(((), true))
    });
    let health_reason = match &degraded {
        Ok(()) => reason.clone(),
        Err(error) => format!("{reason}; store failure marking drain: {error}"),
    };
    state.mark_producer_degraded(key, health_reason);
    tracing::warn!(target: "drain", producer = key, %reason, ?degraded, "producer drain failed");
}

/// The health reason for a committed drain, or `None` when healthy.
pub(crate) fn health_reason(
    label: &str,
    quarantined_redeliveries: usize,
    usage_not_ingested: u64,
) -> Option<String> {
    (quarantined_redeliveries > 0 || usage_not_ingested > 0).then(|| {
        format!(
            "{quarantined_redeliveries} {label} deliveries were quarantined and \
                 {usage_not_ingested} usage records were not ingested"
        )
    })
}

/// Per-drain file and ingest counters. Adapters use quarantine and
/// usage-refusal counts when reporting producer health.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct JsonlDrainOutcome<C> {
    pub root_absent: bool,
    pub files_read: usize,
    pub files_resumed: usize,
    pub counts: C,
    pub skipped_lines_by_file: std::collections::BTreeMap<PathBuf, u64>,
    pub calls_emitted: usize,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
    pub hashed_bytes: u64,
}

pub(crate) struct JsonlRead<P, C, Call> {
    pub resumed: bool,
    pub hashed_bytes: u64,
    pub counts: C,
    pub calls: Vec<Call>,
    pub resume: Option<P>,
}

/// OMP and Kimi share commit and resume ordering, not their record formats.
/// Their readers consume resume points; failed drains conservatively reread.
#[allow(clippy::too_many_arguments)]
pub(crate) fn drain_jsonl<P, C: Default, Call>(
    state: &SharedState,
    key: &str,
    label: &str,
    root: &SourceRoot,
    cache: &std::sync::Mutex<ResumeCache<P>>,
    recorded_at_ms: i64,
    stop: &AtomicBool,
    keep: &dyn Fn(&Path) -> bool,
    parse: impl Fn(&Path, Option<P>, PrefixCheck) -> JsonlRead<P, C, Call>,
    map: impl Fn(Call) -> Result<EventRecordRequest, String>,
    add_counts: impl Fn(&mut C, &C) -> u64,
    offset: impl Fn(&P) -> u64,
    mode: DrainMode,
) -> Result<JsonlDrainOutcome<C>, SourceDrainError> {
    let root_label = format!("{label} sessions");
    let Some(root) = present_root_in_lane(state, key, root, &root_label, mode)? else {
        return Ok(JsonlDrainOutcome {
            root_absent: true,
            ..JsonlDrainOutcome::default()
        });
    };
    let (mut previous_points, previous_verified, previous_windows) = {
        let mut previous = cache
            .lock()
            .map_err(|_| format!("{label} cache lock poisoned"))?;
        (
            std::mem::take(&mut previous.resume),
            previous.prefix_verified_ms.clone(),
            previous.windows.clone(),
        )
    };
    let files = mode
        .files(&previous_windows, recorded_at_ms, || {
            files_below(root, keep)
        })
        .map_err(|error| format!("read {root_label} root {}: {error}", root.display()))?;
    let mut outcome = JsonlDrainOutcome::default();
    let mut next = ResumeCache::default();
    let mut requests = Vec::new();
    for path in files {
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(SourceDrainError::Stopped);
        }
        let stat = FileStat::read_in_lane(&path, mode)
            .map_err(|error| format!("stat {label} session {}: {error}", path.display()))?;
        if !mode.is_full() && !stat.fast_changed(previous_windows.get(&path), recorded_at_ms) {
            if let Some(point) = previous_points.remove(&path) {
                next.resume.insert(path.clone(), point);
            }
            if let Some(verified) = previous_verified.get(&path) {
                next.prefix_verified_ms.insert(path.clone(), *verified);
            }
            if let Some(window) = previous_windows.get(&path) {
                next.windows.insert(path, window.clone());
            }
            continue;
        }
        let point = previous_points.remove(&path);
        let verified = previous_verified.get(&path).copied();
        let check = mode.prefix_check(&path, point.is_some(), verified, recorded_at_ms);
        let window = previous_windows.get(&path);
        if let Some(window) = window.filter(|window| {
            point.is_some() && window.defer(&stat, recorded_at_ms, check == PrefixCheck::Full)
        }) {
            next.resume
                .insert(path.clone(), point.expect("checked resume point"));
            if let Some(verified) = verified {
                next.prefix_verified_ms.insert(path.clone(), verified);
            }
            next.windows.insert(path, window.clone());
            continue;
        }
        let previous_offset = point.as_ref().map(&offset);
        let parsed = parse(&path, point, check);
        let next_offset = parsed.resume.as_ref().map(&offset);
        next.windows.insert(
            path.clone(),
            WindowSchedule::after_read(
                window,
                stat.clone(),
                recorded_at_ms,
                parsed.resumed && next_offset == previous_offset,
                next_offset == Some(stat.len),
            ),
        );
        if let Some(verified) = mode.verified_after(check, parsed.resumed, verified, recorded_at_ms)
        {
            next.prefix_verified_ms.insert(path.clone(), verified);
        }
        outcome.files_read += 1;
        outcome.files_resumed += usize::from(parsed.resumed);
        outcome.hashed_bytes += parsed.hashed_bytes;
        let skipped = add_counts(&mut outcome.counts, &parsed.counts);
        if skipped > 0 {
            outcome.skipped_lines_by_file.insert(path.clone(), skipped);
        }
        outcome.calls_emitted += parsed.calls.len();
        if let Some(point) = parsed.resume {
            next.resume.insert(path, point);
        }
        for call in parsed.calls {
            requests.push(map(call)?);
        }
    }
    if !mode.is_full() {
        next.resume.extend(previous_points);
        for (path, window) in previous_windows {
            next.windows.entry(path).or_insert(window);
        }
        for (path, verified) in previous_verified {
            next.prefix_verified_ms.entry(path).or_insert(verified);
        }
    }
    let tally = record_requests_in_lane(
        state,
        key,
        &requests,
        recorded_at_ms,
        &|| stop.load(std::sync::atomic::Ordering::Relaxed),
        mode,
    )?;
    outcome.rows_recorded = tally.recorded;
    outcome.exact_redeliveries = tally.exact_redeliveries;
    outcome.quarantined_redeliveries = tally.quarantined_redeliveries;
    *cache
        .lock()
        .map_err(|_| format!("{label} cache lock poisoned"))? = next;
    Ok(outcome)
}
