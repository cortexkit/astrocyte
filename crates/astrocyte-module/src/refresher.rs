//! Producer-runtime draining.
//!
//! A producer's read is deliberately completed before `drain_once` takes the
//! store writer. The successful observation and its cursor state are committed
//! in one transaction, so a failed fact write cannot make the producer look
//! current.

#[path = "broca_source.rs"]
pub(crate) mod broca_source;
#[path = "claude_code_source.rs"]
mod claude_code_source;
#[path = "claude_workers_source.rs"]
mod claude_workers_source;
#[path = "codex_source.rs"]
mod codex_source;
#[path = "declared_roots.rs"]
mod declared_roots;
#[cfg(all(test, target_os = "linux"))]
#[path = "fast_lane_measure.rs"]
mod fast_lane_measure;
#[cfg(test)]
#[path = "fast_lane_tests.rs"]
mod fast_lane_tests;
#[path = "gemini_source.rs"]
mod gemini_source;
#[path = "goose_source.rs"]
mod goose_source;
#[path = "jsonl_schedule.rs"]
pub(crate) mod jsonl_schedule;
#[path = "kimi_source.rs"]
mod kimi_source;
#[path = "omp_source.rs"]
mod omp_source;
#[path = "opencode_source.rs"]
pub(crate) mod opencode_source;
#[path = "opencode_thalamus_source.rs"]
pub(crate) mod opencode_thalamus_source;
#[path = "opencode_v2_source.rs"]
pub(crate) mod opencode_v2_source;
#[path = "pi_source.rs"]
mod pi_source;

use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_core::{
    capacity::{apply_snapshot_in_tx, record_failed_attempt, CapacitySnapshot},
    catalog_client::{CatalogClient, CatalogClientError, CatalogConnection},
    multiplier::{self, MultiplierConfig, PlanPriceSource},
};
use rusqlite::{params, Transaction};

use crate::{
    config,
    state::{self, now_ms as current_time_ms, SharedState},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCursor {
    pub lineage: String,
    pub ordinal: i64,
}

#[derive(Debug, Clone)]
pub struct ExportPage {
    /// The producer position this page's facts run up to and the drain commits.
    pub cursor: SourceCursor,
    /// The producer's newest position when it served this page.
    pub high_water: SourceCursor,
    pub events: Vec<EventRecordRequest>,
    pub model_steps: Vec<astrocyte_core::broca_step::ModelStepFact>,
    pub set_aside: Vec<SetAsideRecord>,
    /// This is a producer-provided exact count, not a sequence-number
    /// subtraction. Unknown lag remains explicit when the producer could not
    /// enumerate all pending records.
    pub pending_known: i64,
    pub pending_unknown: bool,
}

/// Evidence locators are independent of event identity: unreadable records never
/// need an invented producer reference in order to be retained.
#[derive(Debug, Clone)]
pub struct SetAsideRecord {
    pub locator: String,
    pub reason: String,
}

pub trait SourceClient: Send + Sync {
    fn pull(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError>;

    fn pull_fast(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError> {
        self.pull(since, limit)
    }

    /// The resolved store path, if known, used when logging whether the store is
    /// present or absent.
    fn resolved_path(&self) -> Option<PathBuf> {
        None
    }

    fn refuse_regression(
        &self,
        stored: Option<&SourceCursor>,
        page: &ExportPage,
    ) -> Result<(), PullError> {
        refuse_regression(stored, page)
    }
}

pub trait CapacitySource: Send + Sync {
    fn poll(&self) -> Result<CapacitySnapshot, PullError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullError {
    Unavailable(String),
    /// The producer's store does not exist: the producer is not installed.
    /// There is nothing to read and nothing to go stale, so the drain removes
    /// the source's runtime row instead of recording a failure.
    SourceAbsent,
    /// Shutdown interrupted the read. Nothing was committed and nothing about
    /// the source's health was learned.
    Stopped,
    InvalidResponse(String),
    /// The producer answered under the identity we hold but from a position
    /// behind our cursor: a restore, a clock rollback, or both. Advancing
    /// would either skip past facts (strand) or re-read them (double count);
    /// refusing leaves the cursor where it was and lets freshness age into a
    /// visible degradation with this reason on the log.
    LineageRegressed {
        stored: SourceCursor,
        offered: SourceCursor,
    },
    Store(String),
}

/// A producer whose connection cannot be opened. Test fixture only: production
/// producers come from `refresher_producers`, and a loop fed this stub looks
/// alive while draining nothing.
#[cfg(test)]
pub struct UnavailableSource;

#[cfg(test)]
impl SourceClient for UnavailableSource {
    fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
        Err(PullError::Unavailable(
            "producer connection is unavailable".into(),
        ))
    }
}

/// Why a file-backed drain (OpenCode, Pi) returned without an outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceDrainError {
    /// Shutdown was requested and the drain abandoned its work uncommitted.
    /// This is not a failure: it must not degrade the source's health or
    /// write anything into `source_runtime`.
    Stopped,
    /// The drain failed.
    Failed(String),
}

impl From<String> for SourceDrainError {
    fn from(reason: String) -> Self {
        Self::Failed(reason)
    }
}

impl std::fmt::Display for SourceDrainError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => formatter.write_str("drain stopped"),
            Self::Failed(reason) => formatter.write_str(reason),
        }
    }
}

/// Plumbing shared by the drain sources of the smaller harnesses (Gemini
/// CLI, Goose, Oh My Pi, Kimi CLI), which the refresher drains through one
/// [`harness_drain::HarnessSource`] interface instead of one argument each.
/// The functions repeat what `codex_source.rs` does for Codex (runtime row
/// upkeep, one-transaction commit, prefix-check cadence, failure marking),
/// taking the source key as a parameter.
pub(crate) mod harness_drain {
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
                    // `flow_ref` was added to usage_event after these hashes
                    // were pinned, and only BROCA batch facts set it. A NULL
                    // there is left out, so a pinned hash still proves every
                    // earlier column is stored exactly as before; a set
                    // flow_ref is hashed like any other value.
                    let flow_ref = (0..stmt.column_count())
                        .find(|col| stmt.column_name(*col).unwrap() == "flow_ref");
                    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
                    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                        for col in &columns {
                            let value = row.get_ref(*col).map_err(|e| e.to_string())?;
                            if Some(*col) == flow_ref
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
                i64::try_from(quantity)
                    .map_err(|_| format!("{source} exceeds stored integer range"))
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
            if let Some(verified) =
                mode.verified_after(check, parsed.resumed, verified, recorded_at_ms)
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainOutcome {
    /// Events the page carried, whatever the ledger did with each.
    pub events_recorded: usize,
    /// Events whose identity was already stored with a different payload.
    pub quarantined_redeliveries: usize,
    pub consumed_ordinal: Option<i64>,
    pub high_water_ordinal: Option<i64>,
}

/// Connect through the supplied fusiform transport and preserve the history
/// boundary discovered by the startup probe. No local catalog file is read.
pub fn connect_catalog(
    connection: Arc<dyn CatalogConnection>,
) -> Result<CatalogClient, CatalogClientError> {
    CatalogClient::connect(connection)
}

pub fn catalog_history_boundary(client: &CatalogClient) -> i64 {
    client.history_start_ms()
}

pub fn drain_once(
    state: &SharedState,
    source: &dyn SourceClient,
    source_key: &str,
    limit: u32,
    now_ms: i64,
) -> Result<DrainOutcome, PullError> {
    drain_once_in_lane(
        state,
        source,
        source_key,
        limit,
        now_ms,
        harness_drain::DrainMode::Full,
    )
}

fn drain_once_in_lane(
    state: &SharedState,
    source: &dyn SourceClient,
    source_key: &str,
    limit: u32,
    now_ms: i64,
    mode: harness_drain::DrainMode,
) -> Result<DrainOutcome, PullError> {
    let current = durable_cursor(state, source_key).map_err(PullError::Store)?;
    // The read is the only I/O boundary, and is intentionally before the
    // transaction below.
    let discovery_path = source
        .resolved_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<unavailable: source did not provide a resolved path>".into());
    let read = if mode.is_full() {
        source.pull(current.clone(), limit)
    } else {
        source.pull_fast(current.clone(), limit)
    };
    let page = match read {
        Err(PullError::SourceAbsent) => {
            harness_drain::record_source_discovery(state, source_key, &discovery_path, false);
            mode.remove_runtime(state, source_key)
                .map_err(PullError::Store)?;
            return Ok(DrainOutcome::default());
        }
        result => {
            let page = result?;
            harness_drain::record_source_discovery(state, source_key, &discovery_path, true);
            page
        }
    };
    validate_page(&page)?;
    source.refuse_regression(current.as_ref(), &page)?;
    if !mode.has_work(
        page.events.len() + page.model_steps.len(),
        page.set_aside.len(),
    ) {
        return Ok(DrainOutcome::default());
    }
    let cursor = &page.cursor;
    let high_water = &page.high_water;
    // The schema stores one lineage for both ordinals. If the producer already
    // reports a later lineage but this page consumed only the old one, retain a
    // same-lineage high water and keep lag unknown until a new-lineage event lands.
    let persisted_high_water_ordinal = if high_water.lineage == cursor.lineage {
        high_water.ordinal
    } else {
        cursor.ordinal
    };
    let source_key = source_key.to_owned();
    let (outcome, newly_seen) = mode
        .write(state, |tx| {
            // The row is removed while the producer's store is absent or the
            // producer is excluded, so the first drain after either recreates
            // it in the same transaction as the cursor it records.
            harness_drain::ensure_runtime_row_in_tx(tx, &source_key)
                .map_err(|error| format!("initialize source runtime: {error}"))?;
            let mut newly_seen =
                harness_drain::record_set_aside_in_tx(tx, &source_key, &page.set_aside, now_ms)?;
            let mut events_recorded = 0;
            let mut quarantined_redeliveries = 0;
            for event in &page.events {
                // Read, not Pushed: this process fetched the record from the
                // producer's store, so reading it again says nothing about the
                // producer and must not write redelivery_log.
                let outcome = astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    event,
                    now_ms,
                    astrocyte_core::ingest::DeliveryPath::Read,
                )
                .map_err(|error| format!("record producer event: {error:?}"))?;
                events_recorded += 1;
                if outcome.disposition
                    == astrocyte_core::ingest::RecordDisposition::QuarantinedRedelivery
                {
                    quarantined_redeliveries += 1;
                }
            }
            for step in &page.model_steps {
                if let Some(reason) = astrocyte_core::broca_step::record_in_tx(tx, step, now_ms)
                    .map_err(|error| format!("record model_step: {error}"))?
                {
                    newly_seen.extend(harness_drain::record_set_aside_in_tx(
                        tx,
                        &source_key,
                        &[SetAsideRecord {
                            locator: format!(
                                "model_step:{}:{}#{}",
                                step.fact_id, step.run_id, step.step_id
                            ),
                            reason,
                        }],
                        now_ms,
                    )?);
                }
            }
            let updated = tx
                .execute(
                    "UPDATE source_runtime
                 SET lineage=?2, consumed_ordinal=?3, high_water_ordinal=?4,
                      last_success_ms=CASE WHEN ?8 THEN ?5 ELSE last_success_ms END,
                      pending_known=?6, pending_unknown=?7
                 WHERE source_key=?1",
                    params![
                        source_key,
                        cursor.lineage,
                        cursor.ordinal,
                        persisted_high_water_ordinal,
                        now_ms,
                        page.pending_known,
                        i64::from(page.pending_unknown),
                        mode.is_full(),
                    ],
                )
                .map_err(|error| format!("record source runtime: {error}"))?;
            if updated != 1 {
                return Err(format!("{source_key} source runtime row missing at commit"));
            }
            Ok((
                (
                    DrainOutcome {
                        events_recorded,
                        quarantined_redeliveries,
                        consumed_ordinal: Some(cursor.ordinal),
                        high_water_ordinal: Some(persisted_high_water_ordinal),
                    },
                    newly_seen,
                ),
                true,
            ))
        })
        .map_err(PullError::Store)?;
    harness_drain::warn_set_aside(&source_key, &newly_seen);
    Ok(outcome)
}

/// The stored cursor, or `None` when there is none yet or no row at all.
fn durable_cursor(state: &SharedState, source_key: &str) -> Result<Option<SourceCursor>, String> {
    use rusqlite::OptionalExtension;
    state.with_conn(|conn| {
        conn.query_row(
            "SELECT lineage,consumed_ordinal FROM source_runtime WHERE source_key=?1",
            [source_key],
            |row| {
                let lineage = row.get::<_, Option<String>>(0)?;
                let ordinal = row.get::<_, Option<i64>>(1)?;
                Ok(match (lineage, ordinal) {
                    (Some(lineage), Some(ordinal)) => Some(SourceCursor { lineage, ordinal }),
                    _ => None,
                })
            },
        )
        .optional()
        .map(Option::flatten)
        .map_err(|error| error.to_string())
    })
}

/// The generic guard preserves the same-lineage ordinal regression rule. Each
/// adapter additionally validates lineage ordering using its own wire semantics.
fn refuse_regression(stored: Option<&SourceCursor>, page: &ExportPage) -> Result<(), PullError> {
    let Some(stored) = stored else {
        return Ok(());
    };
    let high_water = &page.high_water;
    if high_water.lineage == stored.lineage && high_water.ordinal < stored.ordinal {
        return Err(PullError::LineageRegressed {
            stored: stored.clone(),
            offered: high_water.clone(),
        });
    }
    Ok(())
}

fn validate_page(page: &ExportPage) -> Result<(), PullError> {
    if page.pending_known < 0
        || page.cursor.lineage.is_empty()
        || page.cursor.ordinal < 0
        || page.high_water.lineage.is_empty()
        || page.high_water.ordinal < 0
    {
        return Err(PullError::InvalidResponse(
            "source identity and cursors must be nonnegative and ordered".into(),
        ));
    }
    if !page.pending_unknown && page.pending_known == 0 && page.cursor != page.high_water {
        return Err(PullError::InvalidResponse(
            "a complete observation with lag must report known pending records".into(),
        ));
    }
    // Within one lineage the producer cannot have consumed past its own
    // newest position. Across lineages the two ordinals count different
    // sequences and are not compared; `drain_once` then keeps the consumed
    // ordinal as the high water. The store's CHECK refuses the same-lineage
    // case too, but only as a failed write; this names the producer's page as
    // the cause before anything is recorded from it.
    if page.cursor.lineage == page.high_water.lineage
        && page.cursor.ordinal > page.high_water.ordinal
    {
        return Err(PullError::InvalidResponse(format!(
            "consumed ordinal {} is past high water {} in lineage {}",
            page.cursor.ordinal, page.high_water.ordinal, page.cursor.lineage
        )));
    }
    Ok(())
}

pub fn hydrate_startup(state: &Arc<SharedState>) -> Result<(), String> {
    reload_config(state, state::user_config_path())?;
    // Runs on a blocking thread before serving starts; a failure only leaves
    // `usage.cache.steps` reading the store.
    state.rebuild_recent_steps();
    Ok(())
}

/// Exercise retention/refusal for invalid startup bytes, not successful publication.
#[cfg(any(test, feature = "test-support"))]
pub fn reject_startup_bytes_for_test(
    state: &Arc<SharedState>,
    contents: &[u8],
) -> Result<(), String> {
    match config::decode_bytes(contents) {
        Ok(_) => Err("test fixture unexpectedly decoded".into()),
        Err(error) => retain_committed_generation_or_refuse_seed(state, error.to_string()),
    }
}

fn retain_committed_generation_or_refuse_seed(
    state: &Arc<SharedState>,
    failure: String,
) -> Result<(), String> {
    state.mark_config_degraded(failure.clone());
    if state.has_committed_desired_generation() {
        log_config_retained(&failure);
        Ok(())
    } else {
        Err(format!(
            "startup desired configuration unavailable with no committed generation: {failure}"
        ))
    }
}

/// `resolved_path` is injected so a test can drive the production arms with a
/// temp file and without touching process environment; production passes
/// `state::user_config_path()`.
fn reload_config(
    state: &Arc<SharedState>,
    resolved_path: Result<std::path::PathBuf, String>,
) -> Result<(), String> {
    // An unresolvable path is not a missing file: Missing publishes
    // intentional-empty as a new generation, which is only correct when the
    // operator removed the file. Every failure arm below goes through
    // retain_committed_generation_or_refuse_seed, never straight to Err. An
    // early `?` here used to exit the daemon on a malformed or unreadable file
    // even when a committed generation existed to serve -- the retain arm was
    // written, tested through a helper, and reached by nothing on the
    // production path. The lint that reported it dead was the only witness.
    let path = match resolved_path {
        Ok(path) => path,
        Err(error) => return retain_committed_generation_or_refuse_seed(state, error),
    };
    let desired = match config::classify_read_result(std::fs::read(path)) {
        config::ConfigReadOutcome::Contents(contents) => match config::decode_bytes(&contents) {
            Ok(desired) => desired,
            Err(error) => {
                return retain_committed_generation_or_refuse_seed(state, error.to_string())
            }
        },
        config::ConfigReadOutcome::Missing => config::intentional_empty(),
        config::ConfigReadOutcome::Unreadable(error) => {
            return retain_committed_generation_or_refuse_seed(state, error.to_string())
        }
    };
    let now_ms = current_time_ms();
    let generation = state.ordered_write(|tx| publish_desired_transaction(tx, &desired, now_ms))?;
    state.publish_desired(
        desired.config,
        generation.unwrap_or_else(|| state.desired_generation()),
    );
    Ok(())
}

fn publish_desired_transaction(
    tx: &Transaction<'_>,
    desired: &config::DesiredContent,
    now_ms: i64,
) -> Result<(Option<u64>, bool), String> {
    let current: (i64, String) = tx
        .query_row(
            "SELECT generation,content_hash FROM desired_config WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| error.to_string())?;
    if current.1 == desired.content_hash {
        return Ok((None, false));
    }
    let generation = current
        .0
        .checked_add(1)
        .ok_or_else(|| "desired generation overflow".to_string())?;
    let multiplier =
        config::multiplier_column(&desired.config).map_err(|error| error.to_string())?;
    let ingest =
        config::harness_ingest_column(&desired.config).map_err(|error| error.to_string())?;
    // `broca_connection_file` is a column the store still has but the
    // configuration no longer carries (BROCA is read from its store), so each
    // generation records it as NULL.
    tx.execute("UPDATE desired_config SET generation=?1,canonical_bytes=?2,content_hash=?3,poll_interval_secs=?4,qta_connection_file=?5,broca_connection_file=NULL,fusiform_connection_file=?6,updated_at_ms=?7,subscription_multiplier=?8,harness_ingest=?9 WHERE id=1", params![generation, desired.canonical_bytes, desired.content_hash, i64::try_from(desired.config.poll_interval_secs).map_err(|_| "poll interval exceeds SQLite integer".to_string())?, desired.config.qta_connection_file, desired.config.fusiform_connection_file, now_ms, multiplier, ingest]).map_err(|error| error.to_string())?;
    tx.execute(
        "UPDATE source_runtime SET desired_generation=?1",
        [generation],
    )
    .map_err(|error| error.to_string())?;
    Ok((
        Some(u64::try_from(generation).map_err(|_| "negative generation".to_string())?),
        true,
    ))
}

pub struct Producer {
    pub source_key: String,
    pub client: Arc<dyn SourceClient>,
}

/// The producers drained through `drain_once`: BROCA's store reader, which
/// needs no configuration. `stop` interrupts a read in progress on shutdown.
fn refresher_producers(stop: &Arc<AtomicBool>) -> Vec<Producer> {
    vec![Producer {
        source_key: broca_source::BROCA_SOURCE_KEY.into(),
        client: Arc::new(refresher_broca_source(stop)),
    }]
}

fn refresher_broca_source(stop: &Arc<AtomicBool>) -> broca_source::BrocaSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's live BROCA
        // store. Store resolution itself is exercised through `from_data_home`.
        let data_home = std::env::temp_dir().join(format!(
            "astrocyte-test-data-home-without-broca-{}",
            std::process::id()
        ));
        broca_source::BrocaSource::from_data_home(data_home, Arc::clone(stop))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        broca_source::BrocaSource::from_environment(Arc::clone(stop))
    }
}

pub(crate) fn configured_capacity(
    state: &SharedState,
    config: &config::AstrocyteConfig,
) -> Result<Option<Arc<dyn CapacitySource>>, String> {
    let source = config
        .qta_connection_file
        .as_ref()
        .map(crate::qta_client::QtaClient::new)
        .transpose()?
        .map(|client| Arc::new(client) as Arc<dyn CapacitySource>);
    state.configure_capacity_runtime(
        source.is_some(),
        config.poll_interval_ms(),
        current_time_ms(),
    )?;
    Ok(source)
}

#[derive(Default)]
struct FileSources {
    opencode: Option<Arc<opencode_source::OpenCodeSource>>,
    pi: Option<Arc<pi_source::PiSource>>,
    claude_code: Option<Arc<claude_code_source::ClaudeCodeSource>>,
    codex: Option<Arc<codex_source::CodexSource>>,
    /// Additional transcript readers: Gemini CLI, Goose, Oh My Pi, Kimi CLI,
    /// and Claude workers listed in the harness module's roots.json.
    harnesses: Vec<Arc<dyn harness_drain::HarnessSource>>,
}

fn refresher_opencode_source() -> opencode_source::OpenCodeSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-opencode-{}",
            std::process::id()
        ));
        opencode_source::OpenCodeSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        opencode_source::OpenCodeSource::from_environment()
    }
}

fn refresher_pi_source() -> pi_source::PiSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-pi-{}",
            std::process::id()
        ));
        pi_source::PiSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        pi_source::PiSource::from_environment()
    }
}

fn refresher_claude_code_source() -> claude_code_source::ClaudeCodeSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-claude-code-{}",
            std::process::id()
        ));
        claude_code_source::ClaudeCodeSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        claude_code_source::ClaudeCodeSource::from_environment()
    }
}

fn refresher_codex_source() -> codex_source::CodexSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-codex-{}",
            std::process::id()
        ));
        codex_source::CodexSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        codex_source::CodexSource::from_environment()
    }
}

fn refresher_gemini_source() -> gemini_source::GeminiSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-gemini-{}",
            std::process::id()
        ));
        gemini_source::GeminiSource::from_home(Some(home.into_os_string()), None)
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        gemini_source::GeminiSource::from_environment()
    }
}

fn refresher_goose_source() -> goose_source::GooseSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-goose-{}",
            std::process::id()
        ));
        goose_source::GooseSource::from_home(Some(home.into_os_string()), None)
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        goose_source::GooseSource::from_environment()
    }
}

fn refresher_omp_source() -> omp_source::OmpSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-omp-{}",
            std::process::id()
        ));
        omp_source::OmpSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        omp_source::OmpSource::from_environment()
    }
}

fn refresher_kimi_source() -> kimi_source::KimiSource {
    #[cfg(any(test, feature = "test-support"))]
    {
        // Production-loop tests must not ingest a developer's mutable live
        // corpus. Root classification itself is exercised through `from_home`.
        let home = std::env::temp_dir().join(format!(
            "astrocyte-test-home-without-kimi-{}",
            std::process::id()
        ));
        kimi_source::KimiSource::from_home(Some(home.into_os_string()))
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        kimi_source::KimiSource::from_environment()
    }
}

fn refresher_harness_sources() -> Vec<Arc<dyn harness_drain::HarnessSource>> {
    vec![
        Arc::new(opencode_v2_source::registered()),
        Arc::new(refresher_gemini_source()),
        Arc::new(refresher_goose_source()),
        Arc::new(refresher_omp_source()),
        Arc::new(refresher_kimi_source()),
        Arc::new(refresher_claude_workers_source()),
        Arc::new(refresher_opencode_thalamus_source()),
    ]
}

fn refresher_opencode_thalamus_source() -> opencode_thalamus_source::OpenCodeThalamusSource {
    #[cfg(any(test, feature = "test-support"))]
    let home = std::env::temp_dir().join(format!(
        "astrocyte-test-data-without-opencode-sessions-{}",
        std::process::id()
    ));
    #[cfg(not(any(test, feature = "test-support")))]
    let home = PathBuf::from(cortexkit_store_types::resolve_data_home());
    opencode_thalamus_source::OpenCodeThalamusSource::from_data_home(home)
}

fn refresher_claude_workers_source() -> claude_workers_source::ClaudeWorkersSource {
    #[cfg(any(test, feature = "test-support"))]
    let home = std::env::temp_dir().join(format!(
        "astrocyte-test-data-without-claude-workers-{}",
        std::process::id()
    ));
    #[cfg(not(any(test, feature = "test-support")))]
    let home = PathBuf::from(cortexkit_store_types::resolve_data_home());
    claude_workers_source::ClaudeWorkersSource::from_data_home(home)
}

/// Releases the pricing-pass hold on drop, so a failed assertion cannot keep
/// the pass held for the rest of the test process.
#[cfg(any(test, feature = "test-support"))]
pub struct PricingPassHold(SharedState);

#[cfg(any(test, feature = "test-support"))]
impl Drop for PricingPassHold {
    fn drop(&mut self) {
        self.0.set_pricing_pass_hold(false);
    }
}

/// Skip the loop's pricing pass on `state` until the returned guard drops.
/// A test that must be the one to price an event -- an operator-frame test
/// whose fake catalog peer serves a fixed number of calls -- holds this for
/// the frame's duration. Without it the loop's first pass can price the same
/// event first, and the frame's own catalog resolution then runs against a
/// peer that has already served its last answer. The hold lives on the state
/// rather than in a process-wide flag because tests run as threads in one
/// process: a global hold would also silence the pricing pass of every
/// `run_iteration_*` helper a parallel test drives on its own state, failing
/// that test's priced-row assertions. Discovery, drains, and capacity polls
/// are unaffected, and direct `price_unpriced_events` calls still price.
#[cfg(any(test, feature = "test-support"))]
pub fn hold_pricing_pass_for_test(state: &SharedState) -> PricingPassHold {
    state.set_pricing_pass_hold(true);
    PricingPassHold(state.clone())
}

/// Own blocking transport runtimes on the dedicated refresher thread so they
/// are never constructed or dropped on a tokio worker.
pub fn start_refresher(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    spawn_refresher(
        state,
        stop,
        refresher_opencode_source(),
        refresher_pi_source(),
        refresher_claude_code_source(),
        refresher_codex_source(),
        refresher_harness_sources(),
    )
}

/// The production refresher reading OpenCode from `opencode_database`, so a
/// test can put a slow OpenCode stage inside the real loop and shut it down.
#[cfg(any(test, feature = "test-support"))]
pub fn start_refresher_with_opencode(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
    opencode_database: std::path::PathBuf,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    spawn_refresher(
        state,
        stop,
        opencode_source::OpenCodeSource::at(opencode_database),
        refresher_pi_source(),
        refresher_claude_code_source(),
        refresher_codex_source(),
        refresher_harness_sources(),
    )
}

fn spawn_refresher(
    state: Arc<SharedState>,
    stop: Arc<AtomicBool>,
    opencode_source: opencode_source::OpenCodeSource,
    pi_source: pi_source::PiSource,
    claude_code_source: claude_code_source::ClaudeCodeSource,
    codex_source: codex_source::CodexSource,
    harness_sources: Vec<Arc<dyn harness_drain::HarnessSource>>,
) -> Result<std::thread::JoinHandle<Result<(), String>>, String> {
    let (ready, initialized) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name("astrocyte-refresher".into())
        .spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let startup = (|| {
                    let config = state.desired_config();
                    let producers = refresher_producers(&stop);
                    let opencode_source = Arc::new(opencode_source);
                    let pi_source = Arc::new(pi_source);
                    let claude_code_source = Arc::new(claude_code_source);
                    let codex_source = Arc::new(codex_source);
                    let capacity = configured_capacity(&state, &config)?;
                    let (pending_catalog, plan_prices) = configure_catalog(&state, &config)?;
                    Ok((
                        producers,
                        opencode_source,
                        pi_source,
                        claude_code_source,
                        codex_source,
                        capacity,
                        pending_catalog,
                        plan_prices,
                        config.poll_interval_secs,
                    ))
                })();
                let ready_result = startup
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error: &String| error.clone());
                let _ = ready.send(ready_result);
                let (
                    producers,
                    opencode_source,
                    pi_source,
                    claude_code_source,
                    codex_source,
                    capacity,
                    pending_catalog,
                    plan_prices,
                    poll_interval_secs,
                ) = startup?;
                let bus_reader = state.desired_config().qta_connection_file.and_then(|path| {
                    crate::quota_events::Reader::start(
                        Arc::clone(&state),
                        path.into(),
                        Arc::clone(&stop),
                    )
                });
                run_loop_full(
                    Arc::clone(&state),
                    producers,
                    FileSources {
                        opencode: Some(opencode_source),
                        pi: Some(pi_source),
                        claude_code: Some(claude_code_source),
                        codex: Some(codex_source),
                        harnesses: harness_sources,
                    },
                    capacity,
                    pending_catalog,
                    MultiplierStage::new(plan_prices),
                    poll_interval_secs,
                    Arc::clone(&stop),
                );
                if let Some(reader) = bus_reader {
                    reader.join();
                }
                Ok(())
            }));
            // The client owns a current-thread runtime. Removing the state's last
            // reference even after a panic keeps its drop on this OS thread.
            let cleanup = state.install_catalog(None);
            match outcome {
                Ok(result) => result.and(cleanup),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
        .map_err(|error| error.to_string())?;
    match initialized.recv() {
        Ok(Ok(())) => Ok(thread),
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error)
        }
        Err(error) => {
            let _ = thread.join();
            Err(format!("refresher initialization channel: {error}"))
        }
    }
}

/// A configured catalog whose history boundary has not yet been discovered.
/// Built at startup without network; the loop retries discovery each tick.
///
/// Discovery is the one catalog call that cannot be deferred to a per-event
/// pricing failure, because every later call needs the boundary. It used to
/// run at startup and a refusal exited the process. On a daemon restart every
/// module boots at once, fusiform is not yet registered, and three refusals
/// inside half a second spent the crash budget: the module was `failed` for
/// a peer that was seconds from live. The dependency contract is register
/// first and fail at USE, never block boot on a peer.
pub struct PendingCatalog {
    connection: Arc<dyn CatalogConnection>,
}

impl PendingCatalog {
    /// One discovery attempt. `Ok(true)` installed the catalog; `Ok(false)`
    /// means it did not and the caller tries again next tick; `Err` is a
    /// local (store) failure.
    ///
    /// NO REFUSAL FROM THE PEER IS FATAL HERE. This once returned Err on a
    /// non-retryable refusal, which killed the refresher thread at startup
    /// and spent the crash budget in seconds -- against a healthy producer
    /// one protocol version behind, whose boundary refusal carried a code the
    /// classifier did not expect. A catalog that cannot be discovered costs
    /// pricing; it must not cost ingest or capacity, which do not depend on
    /// it. The distinction the kind still buys is the log line: a retryable
    /// refusal is the peer warming, anything else is a contract question and
    /// is named as one on every tick until someone reads it.
    pub fn try_connect(&self, state: &SharedState) -> Result<bool, String> {
        match connect_catalog(Arc::clone(&self.connection)) {
            Ok(catalog) => {
                let boundary = catalog_history_boundary(&catalog);
                state.install_catalog(Some(catalog))?;
                tracing::info!(target: "pricing", boundary, "fusiform history boundary discovered");
                Ok(true)
            }
            Err(error) if error.is_retryable() => {
                tracing::warn!(target: "pricing", ?error, "catalog not yet reachable, pricing deferred");
                Ok(false)
            }
            Err(error) => {
                tracing::error!(target: "pricing", ?error,
                    "catalog discovery refused (not a warming peer; check the producer's protocol version), pricing deferred"
                );
                Ok(false)
            }
        }
    }
}

/// The pending catalog (if discovery has not yet succeeded) and the plan-price
/// source, both on one fusiform client so they share its connection.
type ConfiguredFusiform = (Option<PendingCatalog>, Option<Arc<dyn PlanPriceSource>>);

fn configure_catalog(
    state: &SharedState,
    config: &config::AstrocyteConfig,
) -> Result<ConfiguredFusiform, String> {
    let Some(path) = &config.fusiform_connection_file else {
        state.install_catalog(None)?;
        return Ok((None, None));
    };
    // Constructing the transport reads no socket; only discovery does.
    let client = Arc::new(crate::fusiform_client::FusiformClient::new(path)?);
    let plan_prices: Arc<dyn PlanPriceSource> = client.clone();
    let connection: Arc<dyn CatalogConnection> = client;
    let pending = PendingCatalog { connection };
    // First attempt now so a healthy fleet prices from the first tick; a
    // refusal here is the retry case, not a startup failure.
    if pending.try_connect(state)? {
        return Ok((None, Some(plan_prices)));
    }
    Ok((Some(pending), Some(plan_prices)))
}

/// Test fixture over the production loop: no capacity source, no catalog.
/// Production enters through `start_refresher`, which supplies all three.
#[cfg(test)]
pub fn run_loop(
    state: Arc<SharedState>,
    producers: Vec<Producer>,
    poll_secs: u64,
    stop: Arc<AtomicBool>,
) {
    run_loop_full(
        state,
        producers,
        FileSources::default(),
        None,
        None,
        MultiplierStage::new(None),
        poll_secs,
        stop,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_loop_full(
    state: Arc<SharedState>,
    producers: Vec<Producer>,
    file_sources: FileSources,
    capacity: Option<Arc<dyn CapacitySource>>,
    mut pending_catalog: Option<PendingCatalog>,
    mut multiplier: MultiplierStage,
    poll_secs: u64,
    stop: Arc<AtomicBool>,
) {
    let interval = Duration::from_secs(poll_secs.max(1));
    let mut reconciler = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET);
    let mut pricing = PricingWalk::new(PricingBudget::PER_TICK);
    let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
    while !stop.load(Ordering::Relaxed) {
        run_tick(
            &state,
            &producers,
            &file_sources,
            capacity.as_deref(),
            &mut pending_catalog,
            &mut pricing,
            &mut reconciler,
            &stop,
        );
        summary_tick(&state, &mut summaries, &stop);
        if !stop.load(Ordering::Relaxed) {
            multiplier.tick(&state, pending_catalog.is_some(), current_time_ms());
        }
        let started = std::time::Instant::now();
        let mut next_fast = FAST_LANE_INTERVAL;
        #[cfg(test)]
        state.note_interval_wait_for_test();
        while !stop.load(Ordering::Relaxed) && started.elapsed() < interval {
            crate::quota_events::drain(&state, &stop);
            fast_lane_if_due(
                &state,
                &producers,
                &file_sources,
                &stop,
                started.elapsed(),
                interval,
                &mut next_fast,
            );
            std::thread::sleep(
                Duration::from_millis(250).min(interval.saturating_sub(started.elapsed())),
            );
        }
    }
}

const FAST_LANE_INTERVAL: Duration = Duration::from_secs(2);

/// The elapsed-time argument lets tests advance the same scheduler without wall-clock sleeps.
fn fast_lane_if_due(
    state: &SharedState,
    producers: &[Producer],
    sources: &FileSources,
    stop: &AtomicBool,
    elapsed: Duration,
    full_interval: Duration,
    next_fast: &mut Duration,
) {
    #[cfg(all(test, target_os = "linux"))]
    if !fast_lane_measure::enabled() {
        return;
    }
    if elapsed < *next_fast || elapsed >= full_interval || stop.load(Ordering::Relaxed) {
        return;
    }
    run_fast_lane(state, producers, sources, current_time_ms(), stop);
    // Do not replay missed cycles after a slow read.
    *next_fast = elapsed.saturating_add(FAST_LANE_INTERVAL);
}

fn run_fast_lane(
    state: &SharedState,
    producers: &[Producer],
    sources: &FileSources,
    at: i64,
    stop: &AtomicBool,
) {
    #[cfg(all(test, target_os = "linux"))]
    let _measurement = fast_lane_measure::cycle(true);
    let ingest = state.desired_config().harness_ingest;
    let older: [Option<&dyn harness_drain::HarnessSource>; 4] = [
        sources
            .opencode
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
        sources
            .pi
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
        sources
            .claude_code
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
        sources
            .codex
            .as_deref()
            .map(|source| source as &dyn harness_drain::HarnessSource),
    ];
    for source in older
        .into_iter()
        .flatten()
        .chain(sources.harnesses.iter().map(AsRef::as_ref))
    {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let ingest_key = match source.source_key() {
            opencode_v2_source::SOURCE_KEY => opencode_source::OPENCODE_SOURCE_KEY,
            key => key,
        };
        if !ingest.ingests(ingest_key) {
            continue;
        }
        if let Err(SourceDrainError::Failed(error)) = source.drain_fast(state, at, stop) {
            // Full drains own health and retries; a failed tail must not make a source look fresh.
            tracing::debug!(source = source.source_key(), %error, "fast tail read failed");
        }
    }
    for producer in producers {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if producer.source_key != broca_source::BROCA_SOURCE_KEY
            || !ingest.ingests(&producer.source_key)
        {
            continue;
        }
        if let Err(error) = drain_once_in_lane(
            state,
            producer.client.as_ref(),
            &producer.source_key,
            broca_source::PAGE_LIMIT,
            at,
            harness_drain::DrainMode::Fast,
        ) {
            tracing::debug!(
                source = producer.source_key,
                ?error,
                "fast producer tail read failed"
            );
        }
    }
}

/// Fresh summary work follows ingestion and pricing, with short snapshot chunks
/// and a separate revision-checked writer transaction for publication.
fn summary_tick(
    state: &SharedState,
    summaries: &mut astrocyte_core::report::SummaryMaintenance,
    stop: &AtomicBool,
) {
    let budget = Duration::from_millis(1_500);
    let started = std::time::Instant::now();
    let mut slowest = Duration::ZERO;
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let chunk = std::time::Instant::now();
        let now = current_time_ms();
        let ready = state.with_read_op(|conn| {
            interrupt_on_stop(conn, stop, || Ok(summaries.read_chunk(conn, now)))
                .map_err(|error| ("internal".into(), error.to_string()))?
                .map_err(|error| ("internal".into(), format!("summary read: {error:?}")))
        });
        let ready = match ready {
            Ok(ready) => ready,
            Err((_, error)) => {
                if !stop.load(Ordering::Relaxed) {
                    tracing::warn!(target: "summary", %error, "summary chunk failed; retry after one hour");
                    summaries.defer_after_error(now);
                }
                return;
            }
        };
        if stop.load(Ordering::Relaxed) {
            return;
        }
        if ready {
            let result = state.ordered_write(|tx| {
                let outcome = interrupt_on_stop(tx, stop, || Ok(summaries.publish(tx, now)))
                    .map_err(|error| error.to_string())?
                    .map_err(|error| format!("summary publish: {error:?}"))?;
                // Derived rows do not change the cached config/capacity projection.
                Ok((outcome, false))
            });
            match result {
                Ok(outcome) => {
                    if outcome.deferred {
                        tracing::warn!(target: "summary", hour=?outcome.hour,
                            "hour exceeds bounded summary publication; retaining live aggregation");
                    }
                    if outcome.mismatch {
                        tracing::warn!(target: "summary", hour=?outcome.hour,
                            examined=outcome.examined, "clean hourly summary differs from base facts; requeued");
                    }
                    if outcome.audit_completed {
                        tracing::info!(target: "summary", examined=outcome.examined,
                            hours=outcome.hours, mismatches=outcome.mismatches,
                            chunks=outcome.chunks, reduce_compare_ms=outcome.spent_ms,
                            max_step_ms=outcome.max_chunk_ms, elapsed_ms=outcome.audit_elapsed_ms,
                            "hourly summary full-ledger audit completed");
                    }
                }
                Err(error) => {
                    if !stop.load(Ordering::Relaxed) {
                        tracing::warn!(target: "summary", %error, "summary publication failed; retry after one hour");
                        summaries.defer_after_error(now);
                    }
                    return;
                }
            }
        } else if !summaries.has_pending() {
            return;
        }
        slowest = slowest.max(chunk.elapsed());
        if started.elapsed().saturating_add(slowest) > budget {
            return;
        }
    }
}

/// One pass of the refresher loop, without the sleep that follows it.
#[allow(clippy::too_many_arguments)]
fn run_tick(
    state: &SharedState,
    producers: &[Producer],
    file_sources: &FileSources,
    capacity: Option<&dyn CapacitySource>,
    pending_catalog: &mut Option<PendingCatalog>,
    pricing: &mut PricingWalk,
    reconciler: &mut QueueReconciler,
    stop: &AtomicBool,
) {
    #[cfg(all(test, target_os = "linux"))]
    let _measurement = fast_lane_measure::cycle(false);
    if let Some(pending) = pending_catalog.as_ref() {
        match pending.try_connect(state) {
            Ok(true) => *pending_catalog = None,
            Ok(false) => {}
            Err(error) => tracing::error!(target: "pricing", %error, "install catalog failed"),
        }
    }
    run_iteration_all(
        state,
        producers,
        file_sources.opencode.as_deref(),
        file_sources.pi.as_deref(),
        file_sources.claude_code.as_deref(),
        file_sources.codex.as_deref(),
        &file_sources
            .harnesses
            .iter()
            .map(|source| source.as_ref())
            .collect::<Vec<_>>(),
        capacity,
        pending_catalog.is_some(),
        pricing,
        stop,
    );
    // After the iteration, so every tick drains and prices before spending
    // time on the queue check, and the check only ever takes its budget out of
    // a tick, never a whole ledger walk. Anything it enqueues is priced on the
    // next tick.
    reconciler.tick(state, stop);
    if !stop.load(Ordering::Relaxed) {
        if let Err(error) = retain_broca_steps(state, current_time_ms()) {
            tracing::warn!(target: "astrocyte", %error, "BROCA step retention failed");
        }
    }
}

/// Retention shares the refresher's maintenance cadence, one bounded transaction
/// per tick. It refreshes memory under the writer fence when rows expire.
pub(crate) fn retain_broca_steps(state: &SharedState, now_ms: i64) -> Result<usize, String> {
    state.ordered_write(|tx| {
        let deleted = astrocyte_core::broca_step::expire_in_tx(tx, now_ms)
            .map_err(|error| format!("BROCA step retention: {error}"))?;
        Ok((deleted, deleted > 0))
    })
}

/// Test fixture over `run_iteration_full` with capacity and catalog absent.
#[cfg(test)]
pub(crate) fn run_iteration(state: &SharedState, producers: &[Producer], stop: &AtomicBool) {
    run_iteration_full(state, producers, None, false, stop);
}

/// Test fixture over `run_iteration_full` with the catalog absent.
#[cfg(test)]
pub(crate) fn run_iteration_with_capacity(
    state: &SharedState,
    producers: &[Producer],
    capacity: Option<&dyn CapacitySource>,
    stop: &AtomicBool,
) {
    run_iteration_full(state, producers, capacity, false, stop);
}

/// `catalog_pending` is true while a configured catalog has not yet answered
/// its discovery probe. Pricing is SKIPPED for that tick rather than run
/// without a catalog: the without-catalog path records `catalog_unconfigured`,
/// which means nobody asked, and here somebody asked and was refused. An
/// event left with no priced head is examined on the first tick after the
/// catalog connects; a wrong reason would have to be repriced by hand.
#[cfg(test)]
pub(crate) fn run_iteration_full(
    state: &SharedState,
    producers: &[Producer],
    capacity: Option<&dyn CapacitySource>,
    catalog_pending: bool,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        None,
        None,
        &[],
        capacity,
        catalog_pending,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_pi(
    state: &SharedState,
    producers: &[Producer],
    pi_source: &pi_source::PiSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        Some(pi_source),
        None,
        None,
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_claude_code(
    state: &SharedState,
    producers: &[Producer],
    claude_code_source: &claude_code_source::ClaudeCodeSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        Some(claude_code_source),
        None,
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_codex(
    state: &SharedState,
    producers: &[Producer],
    codex_source: &codex_source::CodexSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        None,
        Some(codex_source),
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_harness(
    state: &SharedState,
    producers: &[Producer],
    harness_source: &dyn harness_drain::HarnessSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        None,
        None,
        None,
        None,
        &[harness_source],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[cfg(test)]
pub(crate) fn run_iteration_with_file_sources(
    state: &SharedState,
    producers: &[Producer],
    opencode_source: &opencode_source::OpenCodeSource,
    pi_source: &pi_source::PiSource,
    stop: &AtomicBool,
) {
    run_iteration_all(
        state,
        producers,
        Some(opencode_source),
        Some(pi_source),
        None,
        None,
        &[],
        None,
        false,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
        stop,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_iteration_all(
    state: &SharedState,
    producers: &[Producer],
    opencode_source: Option<&opencode_source::OpenCodeSource>,
    pi_source: Option<&pi_source::PiSource>,
    claude_code_source: Option<&claude_code_source::ClaudeCodeSource>,
    codex_source: Option<&codex_source::CodexSource>,
    harness_sources: &[&dyn harness_drain::HarnessSource],
    capacity: Option<&dyn CapacitySource>,
    catalog_pending: bool,
    pricing: &mut PricingWalk,
    stop: &AtomicBool,
) {
    crate::quota_events::drain(state, stop);
    // Read once per tick from the committed desired configuration, so the
    // producers skipped here always match the generation in force.
    let ingest = state.desired_config().harness_ingest;
    let opencode_source =
        opencode_source.filter(|_| ingesting(state, &ingest, opencode_source::OPENCODE_SOURCE_KEY));
    let pi_source = pi_source.filter(|_| ingesting(state, &ingest, pi_source::PI_SOURCE_KEY));
    let claude_code_source = claude_code_source
        .filter(|_| ingesting(state, &ingest, claude_code_source::CLAUDE_CODE_SOURCE_KEY));
    let codex_source =
        codex_source.filter(|_| ingesting(state, &ingest, codex_source::CODEX_SOURCE_KEY));

    let older: [Option<&dyn harness_drain::HarnessSource>; 4] = [
        opencode_source.map(|source| source as &dyn harness_drain::HarnessSource),
        pi_source.map(|source| source as &dyn harness_drain::HarnessSource),
        claude_code_source.map(|source| source as &dyn harness_drain::HarnessSource),
        codex_source.map(|source| source as &dyn harness_drain::HarnessSource),
    ];
    for source in older.into_iter().flatten() {
        if !stop.load(Ordering::Relaxed) {
            drain_harness(state, source, stop);
        }
    }

    for harness in harness_sources {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let key = harness.source_key();
        if !ingesting(state, &ingest, key) {
            continue;
        }
        drain_harness(state, *harness, stop);
    }

    for producer in producers {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        if !ingesting(state, &ingest, &producer.source_key) {
            continue;
        }
        match drain_once(
            state,
            producer.client.as_ref(),
            &producer.source_key,
            broca_source::PAGE_LIMIT,
            current_time_ms(),
        ) {
            Ok(outcome) if outcome.quarantined_redeliveries == 0 => {
                state.clear_producer_degraded(&producer.source_key);
            }
            // The page committed, but a stored event came back with a
            // different payload; that is surfaced like any reader's quarantine.
            Ok(outcome) => harness_drain::mark_drain_failed(
                state,
                &producer.source_key,
                format!(
                    "{} {} deliveries were quarantined",
                    outcome.quarantined_redeliveries, producer.source_key
                ),
            ),
            // Shutdown interrupted the read: nothing was committed, and a stop
            // is neither success nor failure, so health is left as it was.
            Err(PullError::Stopped) => {}
            Err(error) => {
                let reason = format!("{error:?}");
                state.mark_producer_degraded(&producer.source_key, reason.clone());
                // A lineage-regression refusal must leave the durable producer row
                // byte-for-byte unchanged. Other failures retain the cursor but
                // expose unknown pending work durably, in a row created if the
                // source had none, so a store that exists but cannot be read
                // shows as a failed source rather than an absent one.
                let degraded = if matches!(error, PullError::LineageRegressed { .. }) {
                    Ok(())
                } else {
                    state.ordered_write(|tx| {
                        harness_drain::ensure_runtime_row_in_tx(tx, &producer.source_key)
                            .map_err(|error| error.to_string())?;
                        tx.execute(
                            "UPDATE source_runtime SET pending_unknown=1 WHERE source_key=?1",
                            [&producer.source_key],
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(((), true))
                    })
                };
                log_producer_drain_failed(&producer.source_key, &reason, &degraded);
            }
        }
    }
    if !stop.load(Ordering::Relaxed) {
        if let Some((capacity, ticket)) = capacity.and_then(|capacity| {
            state
                .capacity_poll_ticket(current_time_ms())
                .map(|ticket| (capacity, ticket))
        }) {
            let result = capacity.poll();
            let now_ms = current_time_ms();
            let succeeded = result.is_ok();
            let persisted = match result {
                Ok(snapshot) => state.ordered_write(|tx| {
                    apply_snapshot_in_tx(tx, &snapshot, now_ms)
                        .map_err(|error| format!("apply capacity snapshot: {error}"))?;
                    Ok(((), true))
                }),
                Err(error) => state.ordered_write(|tx| {
                    record_failed_attempt(tx, now_ms, &format!("{error:?}"))
                        .map_err(|failure| format!("record failed capacity poll: {failure}"))?;
                    Ok(((), true))
                }),
            };
            state
                .quota_bus()
                .finish_poll(ticket, succeeded && persisted.is_ok());
            if let Err(error) = persisted {
                log_capacity_failed(&error);
            }
        }
    }
    if catalog_pending || stop.load(Ordering::Relaxed) {
        return;
    }
    #[cfg(any(test, feature = "test-support"))]
    if state.pricing_pass_held() {
        return;
    }
    if let Err(error) = run_pricing_walk(state, current_time_ms(), stop, pricing) {
        log_pricing_failed(&error);
    }
}

/// Whether `harness_ingest` lets this producer drain on this tick. A skipped
/// producer is not read at all -- no file, no store, no peer call -- and is
/// treated the way an absent harness store is: its runtime row is removed
/// and its health cleared, so it never reads as stale in health or
/// `ingest_freshness`. Events it already recorded stay. Re-enabling it starts
/// from no row: a file reader reads its files again, and BROCA, whose cursor
/// was in the removed row, is read from its first fact; content-derived event
/// ids make both re-reads record nothing twice.
fn ingesting(state: &SharedState, ingest: &config::HarnessIngest, source_key: &str) -> bool {
    let config_key = if source_key == opencode_v2_source::SOURCE_KEY {
        opencode_source::OPENCODE_SOURCE_KEY
    } else {
        source_key
    };
    if ingest.ingests(config_key) {
        return true;
    }
    if let Err(error) = harness_drain::remove_runtime_row(state, source_key) {
        tracing::warn!(target: "drain", producer = source_key, %error, "remove excluded producer runtime row failed");
    }
    state.clear_producer_degraded(source_key);
    false
}

fn drain_harness(
    state: &SharedState,
    harness: &dyn harness_drain::HarnessSource,
    stop: &AtomicBool,
) {
    let key = harness.source_key();
    match harness.drain_for_health(state, current_time_ms(), stop) {
        Ok(None) => state.clear_producer_degraded(key),
        // A stopped drain committed nothing and says nothing about health.
        Err(SourceDrainError::Stopped) => {}
        Ok(Some(reason)) | Err(SourceDrainError::Failed(reason)) => {
            harness_drain::mark_drain_failed(state, key, reason)
        }
    }
}

#[cfg(test)]
fn mark_claude_code_drain_failed(state: &SharedState, reason: String) {
    harness_drain::mark_drain_failed(state, claude_code_source::CLAUDE_CODE_SOURCE_KEY, reason);
}

#[cfg(test)]
fn mark_codex_drain_failed(state: &SharedState, reason: String) {
    harness_drain::mark_drain_failed(state, codex_source::CODEX_SOURCE_KEY, reason);
}

#[cfg(test)]
fn mark_pi_drain_failed(state: &SharedState, reason: String) {
    harness_drain::mark_drain_failed(state, pi_source::PI_SOURCE_KEY, reason);
}

/// How many events that already carry an uncovered charge head one pass may
/// re-resolve against newly declared subscription coverage.
///
/// Measurements with no head at all are freshly recorded facts and keep the
/// priority they have today: this bounds only the events added on top of them,
/// never the ones the pass already had.
///
/// The size trades two costs against each other. Coverage is answered from our
/// own `account_subscription` table with no catalog call, so one of these
/// re-resolves is a local read, a decision and a small write; several hundred
/// of them take a fraction of a second, which is a low single-digit share of
/// the sixty-second default tick and never long enough for an operator's write
/// to queue behind the pass. Repricing a whole backlog in one tick is the other
/// cost: the backlog this was written for is 34,378 events, and taking the
/// store writer for all of them at once would stall ingest and operator writes
/// for minutes. At this size that backlog drains in roughly an hour of ordinary
/// running, and a routine declaration covering a few hundred events is absorbed
/// by a single tick.
const COVERAGE_BACKFILL_PER_PASS: usize = 512;

/// How many priced events one write transaction commits.
///
/// Each commit is one fsync of the write-ahead log (the store runs WAL with
/// `synchronous` left at FULL), so committing event by event costs one fsync
/// per event. On a first boot over a large harness history that was about
/// 142,000 fsyncs, and 9 GB written in six minutes into a 146 MB store.
///
/// Measured on a copy of our production store (7.3 GB) with 10,000 events
/// re-queued, release build: one event per transaction made 10,001 commits and
/// wrote 997 MB of log; groups of 50, 100, 500 and 1,000 made 201, 101, 21 and
/// 11 commits and wrote 505, 495, 483 and 479 MB. The log volume halves at any
/// group size and is flat from about 50 on, and the commit count is already
/// one per hundred events at 100. What keeps growing with the group is how
/// long one transaction holds the store writer: about 1 to 3 seconds for a
/// group of 500 on that store, so ingest and operator writes would queue
/// behind it for that long, against roughly a fifth of that at 100. A smaller
/// group also lets the tick budget below stop closer to its limit.
const PRICING_COMMIT_GROUP: usize = 100;

/// How many queued events the refresher loop prices in one tick at most.
///
/// With the time budget below, this bounds how long pricing holds up a tick,
/// so a large backlog is spread over ticks and every tick still drains the
/// producers first. A first boot of about 530,000 queued events on a small
/// store, where pricing is cheap and this cap binds, takes 106 ticks: about
/// an hour at the production 30-second poll and two hours at the 60-second
/// default, with 50 commits a tick where the old pass made one per event.
const PRICING_TICK_EVENTS: usize = 5_000;

/// Wall time one tick may spend pricing queued events before it returns to
/// the loop and lets the next tick drain the sources. Checked before each
/// event's catalog resolution: an in-flight call may finish past the budget,
/// but no later event starts. The resolved prefix still commits as one group.
/// Five seconds is a small share of a 30- or 60-second poll, so ingest stays
/// fresh.
///
/// On a large store this budget binds before the event cap. On the 7.3 GB
/// production copy (see `PRICING_COMMIT_GROUP`) pricing cost about 2.5 to 3.5
/// ms an event with a warm page cache, catalog lookups included, and up to 8.5
/// ms cold: 1,400 to 2,000 events a tick warm, about 600 cold. A tick runs
/// for its poll interval plus up to this budget, so 530,000 events take
/// 265 to 380 ticks warm, under seven hours at the 60-second default and
/// under four at the 30-second production poll; cold, about 900 ticks, under
/// seventeen hours at the default and under nine at the production poll.
const PRICING_TICK_BUDGET: Duration = Duration::from_secs(5);

/// How much of the pricing queue one call to the pass may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PricingBudget {
    /// Events committed per write transaction.
    pub(crate) group_events: usize,
    /// Queued events one call may take before it stops for this tick.
    pub(crate) tick_events: usize,
    /// Wall time one call may spend on the queue before it stops for this
    /// tick.
    pub(crate) tick_time: Duration,
    /// Most coverage-backfill events one call reads; each one read is
    /// repriced unless the stop flag or the time budget ends the call first.
    pub(crate) backfill_events: usize,
}

impl PricingBudget {
    /// The refresher loop's budget: bounded per tick.
    pub(crate) const PER_TICK: Self = Self {
        group_events: PRICING_COMMIT_GROUP,
        tick_events: PRICING_TICK_EVENTS,
        tick_time: PRICING_TICK_BUDGET,
        backfill_events: COVERAGE_BACKFILL_PER_PASS,
    };

    /// The whole queue in one call, in the loop's commit groups: what the
    /// test fixtures that price directly use.
    #[cfg(test)]
    pub(crate) const WHOLE_QUEUE: Self = Self {
        group_events: PRICING_COMMIT_GROUP,
        tick_events: usize::MAX,
        tick_time: Duration::MAX,
        backfill_events: COVERAGE_BACKFILL_PER_PASS,
    };
}

/// The pricing queue walk, carried across refresher ticks.
///
/// The walk reads the queue in event-id order and resumes each tick after
/// the last event it reached, wrapping to the start once it reaches the end.
/// Starting every tick from the beginning instead would let a run of events
/// that keep failing at the head of the queue use up every tick's budget, so
/// the events behind them would never be priced.
#[derive(Debug)]
pub(crate) struct PricingWalk {
    budget: PricingBudget,
    /// Last queued event id the walk reached; `None` starts at the beginning.
    after: Option<String>,
    /// A budget stop was logged and the queue has not yet been walked to its
    /// end since, so the log says once when a backlog starts and once when it
    /// is caught up rather than on every tick in between.
    backlog_logged: bool,
    /// Where the coverage backfill resumes; see [`BackfillSweep`].
    backfill: BackfillSweep,
}

/// The coverage backfill's position, carried across refresher ticks.
///
/// Each tick reads one bounded batch of the backfill set in event-id order,
/// starting after the last event the previous tick reached, and wraps to the
/// start once a batch reaches the end of the set. An event whose reprice
/// fails keeps its uncovered head, so it stays in the set and is retried when
/// the sweep comes round to it again; it is never dropped, and it cannot hold
/// every later tick on the same batch while the events behind it wait.
///
/// A sweep is one pass from the start of the set to its end, possibly over
/// many ticks. The plan generation is marked drained only at the end of a
/// sweep in which every event was repriced: an event that failed anywhere in
/// the sweep is still in the set, so the backfill is not finished.
#[derive(Debug, Default)]
struct BackfillSweep {
    /// The plan generation this sweep is draining. A plan write moves the
    /// generation and can put events anywhere in the set, including behind
    /// the cursor, so a new generation starts a new sweep from the start.
    generation: Option<i64>,
    /// Last backfill event id the sweep reached; `None` starts at the
    /// beginning of the set.
    after: Option<String>,
    /// An event failed to reprice since this sweep started.
    failed: bool,
}

impl BackfillSweep {
    /// Start a fresh sweep from the beginning of the set.
    fn restart(&mut self, generation: i64) {
        *self = Self {
            generation: Some(generation),
            after: None,
            failed: false,
        };
    }
}

impl PricingWalk {
    pub(crate) fn new(budget: PricingBudget) -> Self {
        Self {
            budget,
            after: None,
            backlog_logged: false,
            backfill: BackfillSweep::default(),
        }
    }

    /// Log a backlog's start and end, once each. Called after the queue part
    /// of every pass: `paused` when it stopped on its budget, `reached_end`
    /// when it read to the end of the queue.
    fn note_progress(&mut self, state: &SharedState, paused: bool, reached_end: bool) {
        let should_log = (paused && !self.backlog_logged) || (reached_end && self.backlog_logged);
        if !should_log {
            return;
        }
        let queued = state.with_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM pricing_pending", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|error| error.to_string())
        });
        let queued = match queued {
            Ok(queued) => queued,
            Err(error) => {
                tracing::warn!(target: "pricing", %error, "could not count the pricing queue");
                -1
            }
        };
        if paused {
            self.backlog_logged = true;
            tracing::info!(
                target: "pricing",
                queued,
                tick_events = self.budget.tick_events,
                tick_budget_ms = u64::try_from(self.budget.tick_time.as_millis()).unwrap_or(u64::MAX),
                "pricing pass stopped on its tick budget with events still queued; it continues on the next tick"
            );
        } else {
            self.backlog_logged = false;
            // What is still queued here is only events whose pricing failed on
            // this walk; each was reported as a pricing failure.
            tracing::info!(
                target: "pricing",
                still_queued = queued,
                "pricing backlog caught up"
            );
        }
    }
}

/// What one pricing pass did, for the refresher's callers and for tests that
/// must see which queries ran rather than only what they produced.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PricingPass {
    /// Events whose reprice committed.
    pub(crate) priced: usize,
    /// Whether the coverage backfill query ran. A drained backfill returns
    /// nothing either way, so its result cannot say whether it was skipped.
    pub(crate) backfill_queried: bool,
    /// SQLite virtual-machine steps spent reading the pending queue.
    pub(crate) queue_selection_steps: i64,
    /// The pass stopped on its per-tick budget before reaching all selected
    /// queue or coverage-backfill events.
    pub(crate) paused: bool,
}

/// An event's reprice plan and its resolution, both produced with no
/// transaction open.
struct ResolvedEvent<'a> {
    event_id: &'a str,
    plan: astrocyte_core::subscription::SubscriptionRepricePlan,
    resolved: astrocyte_core::subscription::ResolvedSubscriptionReprice,
}

fn resolve_event<'a>(
    state: &SharedState,
    catalog: Option<&CatalogClient>,
    event_id: &'a str,
    deadline: Option<std::time::Instant>,
) -> Result<Option<ResolvedEvent<'a>>, String> {
    let plan = state.with_conn(|conn| {
        astrocyte_core::subscription::read_reprice(conn, event_id)
            .map_err(|error| format!("{}: {}", error.code(), error.message()))
    })?;
    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
        return Ok(None);
    }
    // Plan reading committed above; catalog resolution therefore never
    // runs while a SQLite transaction or store mutex is held.
    let resolved = match catalog {
        Some(catalog) => astrocyte_core::subscription::resolve_reprice(
            &plan,
            catalog,
            &state.desired_config().reasoning_billed_as_output,
        ),
        None => astrocyte_core::subscription::resolve_reprice_without_catalog(&plan),
    }
    .map_err(|error| format!("{}: {}", error.code(), error.message()))?;
    Ok(Some(ResolvedEvent {
        event_id,
        plan,
        resolved,
    }))
}

/// Commit one resolved event inside the group's transaction, under its own
/// savepoint. The outer `Result` is a failure of the savepoint statements
/// themselves, which the group cannot survive; the inner one is this event's
/// own outcome.
///
/// The reprice re-checks the event against the plan it was resolved from
/// (its pricing generation and the subscription coverage it assumed) and
/// refuses if either moved. A refusal, or any other error part-way through
/// its writes, rolls back to the savepoint: exactly this event's writes are
/// undone, the events already committed in the group keep theirs, and the
/// event stays queued because its dequeue was among the writes undone.
fn commit_in_savepoint(
    tx: &Transaction<'_>,
    event: &ResolvedEvent<'_>,
    recorded_at_ms: i64,
) -> Result<Result<(), String>, String> {
    tx.execute_batch("SAVEPOINT pricing_event")
        .map_err(|error| format!("open pricing savepoint: {error}"))?;
    match astrocyte_core::subscription::commit_reprice_in_tx(
        tx,
        &event.plan,
        &event.resolved,
        recorded_at_ms,
        astrocyte_core::pricing::PricingOrigin::PricingPass,
    ) {
        Ok(_) => {
            tx.execute_batch("RELEASE pricing_event")
                .map_err(|error| format!("release pricing savepoint: {error}"))?;
            Ok(Ok(()))
        }
        Err(error) => {
            tx.execute_batch("ROLLBACK TO pricing_event; RELEASE pricing_event")
                .map_err(|failure| format!("roll back pricing savepoint: {failure}"))?;
            Ok(Err(format!("{}: {}", error.code(), error.message())))
        }
    }
}

struct PricedGroup {
    attempted: usize,
    outcomes: Vec<(String, Result<(), String>)>,
}

/// Price a group of events with one write transaction, and return each
/// event's outcome and the number attempted. `None` means `stop` was set before the group committed:
/// nothing in the group was written, and every event in it stays queued.
///
/// Every plan is read and every resolution (which may call the catalog) is
/// done first, with no transaction open. Then the whole group commits in one
/// `ordered_write`, one fsync for the group, with a savepoint per event so
/// that an event refused as stale (see `commit_in_savepoint`) does not take
/// the others with it. A savepoint is used rather than retrying the group
/// without the refused event because it keeps the group at one transaction
/// however many of its events are refused, and because it undoes exactly one
/// event's writes even when an error comes after some of them were made.
///
/// An event whose plan cannot be read or resolved writes nothing and stays
/// queued. If the group's transaction itself fails to begin or commit, no
/// event in it was written and each is reported with that error.
/// A time-budget stop commits only the resolved prefix; the untouched suffix
/// stays queued and is not included in the attempted count.
fn price_event_group(
    state: &SharedState,
    catalog: Option<&CatalogClient>,
    event_ids: &[String],
    recorded_at_ms: i64,
    stop: &AtomicBool,
    deadline: Option<std::time::Instant>,
) -> Option<PricedGroup> {
    let mut outcomes = Vec::with_capacity(event_ids.len());
    let mut resolved = Vec::with_capacity(event_ids.len());
    for event_id in event_ids {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            break;
        }
        match resolve_event(state, catalog, event_id, deadline) {
            Ok(Some(event)) => resolved.push(event),
            Ok(None) => break,
            Err(error) => outcomes.push((event_id.clone(), Err(error))),
        }
    }
    if stop.load(Ordering::Relaxed) {
        return None;
    }
    let attempted = outcomes.len() + resolved.len();
    if resolved.is_empty() {
        return Some(PricedGroup {
            attempted,
            outcomes,
        });
    }
    let committed = state.ordered_write(|tx| {
        let mut results = Vec::with_capacity(resolved.len());
        let mut any_committed = false;
        for event in &resolved {
            let result = commit_in_savepoint(tx, event, recorded_at_ms)?;
            any_committed |= result.is_ok();
            results.push((event.event_id.to_string(), result));
        }
        Ok((results, any_committed))
    });
    match committed {
        Ok(results) => outcomes.extend(results),
        Err(error) => outcomes.extend(resolved.iter().map(|event| {
            (
                event.event_id.to_string(),
                Err(format!("group commit failed: {error}")),
            )
        })),
    }
    Some(PricedGroup {
        attempted,
        outcomes,
    })
}

/// Price every queued event, then any event a newly declared or closed plan
/// now covers, the whole queue in one call. The refresher loop uses
/// `run_pricing_walk` with a per-tick budget instead; this is for tests.
#[cfg(test)]
pub fn price_unpriced_events(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
) -> Result<usize, String> {
    run_pricing_pass(state, recorded_at_ms, stop).map(|pass| pass.priced)
}

#[cfg(test)]
pub(crate) fn run_pricing_pass(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
) -> Result<PricingPass, String> {
    run_pricing_walk(
        state,
        recorded_at_ms,
        stop,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
    )
}

/// Price queued events from where `walk` left off until its budget is spent
/// or the queue ends, then any event a newly declared or closed plan now
/// covers.
///
/// An event whose pricing fails -- a catalog that cannot be reached or refuses,
/// a commit refused because the event moved since its plan was read -- writes
/// no head and so stays queued; the walk comes back to it on a later pass.
/// Any failure makes the pass return an error naming every failed event,
/// after the rest of the pass has run.
///
/// Setting `stop` ends the pass before the next event, and commits nothing
/// partial: a group that has not committed when the stop is seen is dropped
/// whole, each committed group was one transaction, and within it each event
/// was fully written or rolled back to its savepoint. So every event is
/// either fully priced or untouched, and an untouched event is still queued
/// for the next pass. A stopped pass returns the count priced so far;
/// stopping is not a pricing failure.
pub(crate) fn run_pricing_walk(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
    walk: &mut PricingWalk,
) -> Result<PricingPass, String> {
    let catalog = state.catalog_client();
    let budget = walk.budget;
    let group_events = budget.group_events.max(1);
    let mut pass = PricingPass::default();
    let mut failures = Vec::new();
    let mut failed = HashSet::new();
    let mut attempted = HashSet::new();

    // Keyset pagination: each group starts after the last id of the one
    // before, so an event that stays queued because it failed is not read
    // again in this pass and cannot hold the pass on one group.
    let tick_started = std::time::Instant::now();
    let deadline = tick_started.checked_add(budget.tick_time);
    let mut slowest_group = Duration::ZERO;
    let mut taken = 0_usize;
    let mut reached_end = false;
    loop {
        let remaining = budget.tick_events.saturating_sub(taken);
        if remaining == 0 {
            pass.paused = true;
            break;
        }
        let wanted = group_events.min(remaining);
        let (mut group, steps) = match state.with_conn(|conn| {
            astrocyte_core::pricing::pending_event_ids(conn, walk.after.as_deref(), wanted)
                .map_err(|error| format!("read pricing queue: {error}"))
        }) {
            Ok(group) => group,
            Err(error) => {
                failures.push(error);
                return finish_pricing_pass(pass, failures);
            }
        };
        pass.queue_selection_steps += steps;
        let complete = group.len() < wanted;
        let group_started = std::time::Instant::now();
        let Some(PricedGroup {
            attempted: processed,
            outcomes,
        }) = price_event_group(
            state,
            catalog.as_ref(),
            &group,
            recorded_at_ms,
            stop,
            deadline,
        )
        else {
            // The cursor stays before this group, so the next pass starts
            // with it.
            return finish_pricing_pass(pass, failures);
        };
        for (event_id, outcome) in outcomes {
            match outcome {
                Ok(()) => pass.priced += 1,
                Err(error) => {
                    failures.push(format!("event {event_id}: {error}"));
                    failed.insert(event_id);
                }
            }
        }
        if processed < group.len() {
            group.truncate(processed);
            if let Some(last) = group.last() {
                walk.after = Some(last.clone());
            }
            attempted.extend(group);
            pass.paused = true;
            break;
        }
        taken += group.len();
        if complete {
            walk.after = None;
            reached_end = true;
            attempted.extend(group);
            break;
        }
        walk.after = group.last().cloned();
        attempted.extend(group);
        slowest_group = slowest_group.max(group_started.elapsed());
        // Start another group only if one as slow as the slowest seen this
        // tick would still finish inside the budget.
        if tick_started.elapsed().saturating_add(slowest_group) > budget.tick_time {
            pass.paused = true;
            break;
        }
    }
    walk.note_progress(state, pass.paused, reached_end);

    // An event a newly declared plan covers already has a head saying no plan
    // covered it, so it is not queued. Those are found by the backfill query,
    // which is only run while a plan write has not yet been drained. Its batch
    // is bounded by the budget's `backfill_events` on its own, so it runs on a
    // tick whose queue walk paused on the event cap too, but shares its
    // wall-time budget. Each tick's batch resumes where the last one ended;
    // see `BackfillSweep`.
    if stop.load(Ordering::Relaxed)
        || deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
    {
        return finish_pricing_pass(pass, failures);
    }
    let due = match state.with_conn(|conn| {
        astrocyte_core::subscription::coverage_backfill_due(conn)
            .map_err(|error| format!("{}: {}", error.code(), error.message()))
    }) {
        Ok(due) => due,
        Err(error) => {
            failures.push(error);
            return finish_pricing_pass(pass, failures);
        }
    };
    if let Some(generation) = due {
        pass.backfill_queried = true;
        let sweep = &mut walk.backfill;
        if sweep.generation != Some(generation) {
            sweep.restart(generation);
        }
        let batch_events = budget.backfill_events.max(1);
        let backfill = match state.with_conn(|conn| {
            astrocyte_core::subscription::coverage_backfill_event_ids(
                conn,
                sweep.after.as_deref(),
                batch_events,
            )
            .map_err(|error| format!("{}: {}", error.code(), error.message()))
        }) {
            Ok(backfill) => backfill,
            Err(error) => {
                failures.push(error);
                return finish_pricing_pass(pass, failures);
            }
        };
        // A short batch read to the end of the set, so it ends the sweep.
        let reaches_end = backfill.len() < batch_events;
        let batch_last = backfill.last().cloned();
        // Every event in the batch was attempted; false when the pass
        // stopped or ran out of time partway through it.
        let mut batch_attempted = true;
        // The last event of the batch whose reprice was attempted, which is
        // where the next tick resumes when this one stops partway.
        let mut last_attempted = None;
        // An event answers both the queue and the backfill if one of its
        // measurements has no head while another carries the uncovered
        // head. Recording writes all of an event's measurements with the
        // event, so nothing produces that today, and the guard is here
        // because the failure would be silent and misleading: the same
        // event priced twice in one pass makes the second attempt refuse
        // as already priced, which reports a healthy pass as a failure.
        let mut fresh = Vec::with_capacity(backfill.len());
        for event_id in backfill {
            if attempted.contains(&event_id) {
                sweep.failed |= failed.contains(&event_id);
            } else {
                fresh.push(event_id);
            }
        }
        for group in fresh.chunks(group_events) {
            let Some(PricedGroup {
                attempted: processed,
                outcomes,
            }) = price_event_group(
                state,
                catalog.as_ref(),
                group,
                recorded_at_ms,
                stop,
                deadline,
            )
            else {
                // Nothing in this group was written, so the sweep resumes
                // with it on the next tick.
                batch_attempted = false;
                break;
            };
            for (event_id, outcome) in outcomes {
                match outcome {
                    Ok(()) => pass.priced += 1,
                    Err(error) => {
                        // The event keeps its uncovered head, so it stays in
                        // the set and the next sweep retries it.
                        sweep.failed = true;
                        failures.push(format!("event {event_id}: {error}"));
                    }
                }
            }
            // The attempted events are always a prefix of the group.
            if let Some(last) = group.get(..processed).and_then(<[String]>::last) {
                last_attempted = Some(last.clone());
            }
            if processed < group.len() {
                batch_attempted = false;
                pass.paused = true;
                break;
            }
        }
        if !batch_attempted {
            if last_attempted.is_some() {
                sweep.after = last_attempted;
            }
        } else if !reaches_end {
            sweep.after = batch_last;
        } else {
            // The sweep has read the whole set from its start. An event that
            // failed anywhere in it is still there, so only a sweep with no
            // failure proves nothing covered is left. Either way the next
            // tick starts a new sweep from the beginning, which is how a
            // failed event is retried.
            let drained = !sweep.failed;
            sweep.restart(generation);
            if drained {
                if let Err(error) = state.ordered_write(|tx| {
                    let marked =
                        astrocyte_core::subscription::mark_coverage_backfill_drained_in_tx(
                            tx, generation,
                        )
                        .map_err(|error| format!("{}: {}", error.code(), error.message()))?;
                    Ok(((), marked))
                }) {
                    failures.push(error);
                }
            }
        }
    }

    finish_pricing_pass(pass, failures)
}

fn finish_pricing_pass(pass: PricingPass, failures: Vec<String>) -> Result<PricingPass, String> {
    if failures.is_empty() {
        Ok(pass)
    } else {
        Err(failures.join("; "))
    }
}

/// How long after one pricing queue check finishes the next one starts. The
/// first check starts on the first tick after startup.
///
/// The check re-runs the old whole-ledger selection. It is expensive: on the
/// production store (1.26 million events, 6.3 million measurements, 251
/// chunks) a full pass took about four minutes of wall time in the running
/// daemon, roughly one second per chunk, almost all of it inside
/// `reconcile_pending_range_in_tx`; on an idle copy of that store it took
/// 146 seconds, 0.3 to 1.5 seconds per chunk with a median of 0.5. That is
/// why a pass is spread over loop ticks under `QUEUE_RECONCILE_TICK_BUDGET`
/// instead of run in one go: run in one go it held up every drain for those
/// four minutes. Spread out, the copy's pass took 88 ticks, about 45 minutes
/// at a 30-second poll.
///
/// What it guards against is a write path that records an event without
/// enqueuing it; such an event would otherwise never be priced and nothing
/// would say so. Recording enqueues in the same transaction, so a miss means
/// a write path that bypassed it. A pass reads the whole ledger, so the check
/// runs daily: an unqueued event is found within one day plus one pass, which
/// is short for a spend ledger read in days.
const QUEUE_RECONCILE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Events per reconciliation chunk. Each chunk is one short write transaction
/// over a range of event ids, so the whole-ledger check never holds the store
/// for longer than one chunk and a stop is noticed between chunks.
const QUEUE_RECONCILE_CHUNK: NonZeroUsize = NonZeroUsize::new(5_000).unwrap();

/// Wall time one loop tick may spend on the pricing queue check before it
/// returns to the loop and lets the next tick drain the sources.
///
/// A chunk costs about half a second to one second on the production store
/// (see `QUEUE_RECONCILE_INTERVAL`), so two seconds is two to four chunks per
/// tick: small next to a poll interval of tens of
/// seconds, so ingest stays fresh, while still making steady progress. A tick
/// always runs at least one chunk, so a pass finishes however slow a chunk
/// is, and it starts another only when the slowest chunk seen this tick would
/// still fit, so a tick overruns the budget by less than one chunk only when
/// chunk times vary.
const QUEUE_RECONCILE_TICK_BUDGET: Duration = Duration::from_secs(2);

/// How often a running reconciliation statement looks at the stop flag.
const RECONCILE_INTERRUPT_POLL: Duration = Duration::from_millis(20);

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileOutcome {
    /// Every event was checked; `enqueued` were needing pricing but unqueued.
    Completed { enqueued: usize },
    /// The stop flag ended the check early; chunks already committed keep
    /// what they enqueued.
    Stopped { enqueued: usize },
}

/// Check the pricing queue against the whole ledger in one call and enqueue
/// every event that needs pricing but is not queued. The loop does not use
/// this; it spreads the same walk over ticks with `QueueReconciler`.
#[cfg(test)]
pub(crate) fn reconcile_pricing_queue(
    state: &SharedState,
    stop: &AtomicBool,
) -> Result<ReconcileOutcome, String> {
    reconcile_pricing_queue_in_chunks(state, stop, QUEUE_RECONCILE_CHUNK)
}

#[cfg(test)]
fn reconcile_pricing_queue_in_chunks(
    state: &SharedState,
    stop: &AtomicBool,
    chunk_events: NonZeroUsize,
) -> Result<ReconcileOutcome, String> {
    let mut walk = ReconcileWalk::default();
    loop {
        match walk.advance(state, stop, chunk_events, Duration::MAX)? {
            WalkProgress::Paused => {}
            WalkProgress::Completed => {
                return Ok(ReconcileOutcome::Completed {
                    enqueued: walk.enqueued,
                })
            }
            WalkProgress::Stopped => {
                return Ok(ReconcileOutcome::Stopped {
                    enqueued: walk.enqueued,
                })
            }
        }
    }
}

/// One reconciliation chunk's result.
enum ChunkStep {
    /// The stop flag ended the chunk; it committed nothing.
    Stopped,
    /// The chunk committed. `through` is the last event id it covered, or
    /// `None` when it was the final, open-ended chunk.
    Advanced {
        through: Option<String>,
        added: usize,
        examined: usize,
    },
}

/// Check the events in the id range after `after` (from the start when
/// `None`), up to `chunk_events` of them, in one write transaction. A stop
/// during the chunk interrupts its SQLite statement, which rolls it back.
fn reconcile_chunk(
    state: &SharedState,
    stop: &AtomicBool,
    after: Option<&str>,
    chunk_events: NonZeroUsize,
) -> Result<ChunkStep, String> {
    let mut stopped = false;
    let chunk = state.ordered_write(|tx| {
        let result = interrupt_on_stop(tx, stop, || {
            let through = astrocyte_core::pricing::reconcile_chunk_end(tx, after, chunk_events)?;
            let added = astrocyte_core::pricing::reconcile_pending_range_in_tx(
                tx,
                after,
                through.as_deref(),
            )?;
            // A bounded chunk covers exactly `chunk_events` events by
            // construction; only the open-ended last one has to be counted.
            let examined = match through {
                Some(_) => chunk_events.get(),
                None => {
                    let (comparison, cursor) = match after {
                        Some(after) => (">", after),
                        None => (">=", ""),
                    };
                    let count: i64 = tx.query_row(
                        &format!("SELECT COUNT(*) FROM usage_event WHERE event_id {comparison} ?1"),
                        [cursor],
                        |row| row.get(0),
                    )?;
                    usize::try_from(count).unwrap_or(0)
                }
            };
            Ok((through, added, examined))
        });
        match result {
            Ok((through, added, examined)) => Ok(((through, added, examined), added > 0)),
            // SQLITE_INTERRUPT counts as a stop only when our own stop
            // flag is set; an interrupt nobody here asked for is a failure.
            Err(error)
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted)
                    && stop.load(Ordering::Relaxed) =>
            {
                stopped = true;
                Err("pricing queue reconciliation stopped".to_string())
            }
            Err(error) => Err(format!("reconcile pricing queue: {error}")),
        }
    });
    if stopped {
        return Ok(ChunkStep::Stopped);
    }
    let (through, added, examined) = chunk?;
    Ok(ChunkStep::Advanced {
        through,
        added,
        examined,
    })
}

/// Where one tick left a reconciliation walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkProgress {
    /// The tick's budget ran out; the walk resumes at its cursor next tick.
    Paused,
    /// Every event was checked.
    Completed,
    /// The stop flag ended the walk.
    Stopped,
}

/// A pass over the ledger in event-id chunks that can be paused between any
/// two chunks and resumed later.
///
/// The chunks are contiguous id ranges `(after, through]`, each starting where
/// the last one ended, so a pass examines every event that existed when its
/// chunk ran exactly once, whether the chunks ran in one tick or in many, the
/// same as the old whole-ledger selection. Events recorded while a pass is
/// paused may fall behind the cursor and not be seen until the next pass;
/// that is fine, because every write path enqueues its own events and this
/// walk is only the backstop for one that does not.
#[derive(Debug, Default)]
struct ReconcileWalk {
    /// Last event id checked; `None` before the first chunk.
    after: Option<String>,
    enqueued: usize,
    examined: usize,
    ticks: usize,
    spent: Duration,
}

impl ReconcileWalk {
    /// Run chunks from the cursor until the walk ends or `budget` is spent.
    /// At least one chunk runs, so a walk always progresses.
    fn advance(
        &mut self,
        state: &SharedState,
        stop: &AtomicBool,
        chunk_events: NonZeroUsize,
        budget: Duration,
    ) -> Result<WalkProgress, String> {
        let tick_started = std::time::Instant::now();
        self.ticks += 1;
        let mut slowest_chunk = Duration::ZERO;
        let progress = loop {
            if stop.load(Ordering::Relaxed) {
                break Ok(WalkProgress::Stopped);
            }
            let chunk_started = std::time::Instant::now();
            match reconcile_chunk(state, stop, self.after.as_deref(), chunk_events) {
                Err(error) => break Err(error),
                Ok(ChunkStep::Stopped) => break Ok(WalkProgress::Stopped),
                Ok(ChunkStep::Advanced {
                    through,
                    added,
                    examined,
                }) => {
                    self.enqueued += added;
                    self.examined += examined;
                    match through {
                        Some(through) => self.after = Some(through),
                        None => break Ok(WalkProgress::Completed),
                    }
                }
            }
            slowest_chunk = slowest_chunk.max(chunk_started.elapsed());
            // Start another chunk only if one as slow as the slowest seen
            // this tick would still finish inside the budget.
            if tick_started.elapsed().saturating_add(slowest_chunk) > budget {
                break Ok(WalkProgress::Paused);
            }
        };
        self.spent += tick_started.elapsed();
        progress
    }
}

/// Schedules the pricing queue check and carries a pass across loop ticks.
struct QueueReconciler {
    walk: Option<ReconcileWalk>,
    /// When the next pass may start. Due at once, so every start checks the
    /// queue before settling into the interval.
    next_due: std::time::Instant,
    chunk_events: NonZeroUsize,
    tick_budget: Duration,
}

impl QueueReconciler {
    fn new(chunk_events: NonZeroUsize, tick_budget: Duration) -> Self {
        Self {
            walk: None,
            next_due: std::time::Instant::now(),
            chunk_events,
            tick_budget,
        }
    }

    #[cfg(test)]
    fn in_progress(&self) -> bool {
        self.walk.is_some()
    }

    /// Spend at most one tick's budget on the check: resume the pass in
    /// progress, or start one if the last finished at least an interval ago.
    ///
    /// A failed chunk abandons the pass and the next one starts a full
    /// interval later from the beginning. Retrying the same chunk every tick
    /// would spend the budget on every tick against what is most likely a
    /// store fault, and a fault that clears is covered by the next pass.
    fn tick(&mut self, state: &SharedState, stop: &AtomicBool) {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let mut walk = match self.walk.take() {
            Some(walk) => walk,
            None if std::time::Instant::now() >= self.next_due => ReconcileWalk::default(),
            None => return,
        };
        match walk.advance(state, stop, self.chunk_events, self.tick_budget) {
            Ok(WalkProgress::Paused) => {
                self.walk = Some(walk);
                return;
            }
            Ok(WalkProgress::Completed) => {
                // One line per pass at info, so the cost of the check is
                // visible in the log even when it finds nothing.
                tracing::info!(
                    target: "pricing",
                    examined = walk.examined,
                    enqueued = walk.enqueued,
                    ticks = walk.ticks,
                    spent_ms = u64::try_from(walk.spent.as_millis()).unwrap_or(u64::MAX),
                    "pricing queue reconciliation pass completed"
                );
                if walk.enqueued > 0 {
                    log_unqueued_events(walk.enqueued, true);
                }
            }
            Ok(WalkProgress::Stopped) => {
                if walk.enqueued > 0 {
                    log_unqueued_events(walk.enqueued, false);
                }
            }
            Err(error) => {
                if walk.enqueued > 0 {
                    log_unqueued_events(walk.enqueued, false);
                }
                tracing::error!(
                    target: "pricing",
                    %error,
                    after = walk.after.as_deref().unwrap_or(""),
                    examined = walk.examined,
                    ticks = walk.ticks,
                    "pricing queue reconciliation failed; the pass is abandoned and the next one starts after the interval"
                );
            }
        }
        self.next_due = std::time::Instant::now() + QUEUE_RECONCILE_INTERVAL;
    }
}

/// Run `work` on `conn` while a watcher interrupts the connection's running
/// statement if `stop` is set. A stop flag checked between statements is not
/// enough on its own: one chunk statement can take a noticeable time on a
/// large ledger. The watcher keeps interrupting while the flag is set,
/// because an interrupt that lands between two statements does not carry over
/// to the next one, and it is joined before this returns, so it can never
/// interrupt whatever the connection runs afterwards.
fn interrupt_on_stop<T>(
    conn: &rusqlite::Connection,
    stop: &AtomicBool,
    work: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let interrupt = conn.get_interrupt_handle();
    let (work_done, watcher_exit) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|scope| {
        // The watcher ends when `work_done` is dropped, which disconnects the
        // channel as soon as the work returns.
        scope.spawn(move || {
            while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                watcher_exit.recv_timeout(RECONCILE_INTERRUPT_POLL)
            {
                if stop.load(Ordering::Relaxed) {
                    interrupt.interrupt();
                }
            }
        });
        let result = work();
        drop(work_done);
        result
    })
}

/// A non-zero count means some path recorded an event without enqueuing it in
/// the same transaction. The events are queued now and will be priced, but
/// the write path is a defect and this warning is how it is noticed.
fn log_unqueued_events(enqueued: usize, complete: bool) {
    tracing::warn!(
        target: "pricing",
        enqueued,
        complete,
        "pricing queue reconciliation found events needing pricing that were never queued; a write path is missing its enqueue"
    );
}

const DAY_MS: i64 = 86_400_000;
/// Each observation covers the seven UTC days that end at the day boundary.
const MULTIPLIER_INTERVAL_MS: i64 = 7 * DAY_MS;

/// Records one subscription multiplier observation per quota provider per UTC
/// day, over the trailing seven days. Missing rows are retried for the seven
/// most recent completed UTC days, oldest first.
///
/// A computation failure writes nothing and is retried on every later tick
/// until recorded or outside the look-back. The table's partial unique index
/// makes original-row retries idempotent, even across restarts. A failure is
/// logged once per day and reason; a configuration-only failure once per reason.
pub(crate) struct MultiplierStage {
    plan_prices: Option<Arc<dyn PlanPriceSource>>,
    /// Provider/day pairs known to be recorded within the current look-back.
    recorded: BTreeSet<(String, String)>,
    /// Provider, day (`None` for configuration-only failures), and reason.
    /// Keep every reason so interleaved retries of missed days do not repeat logs.
    failure_logged: BTreeSet<(String, Option<String>, String)>,
    /// The UTC day the "member not recorded" skip was last logged.
    not_recorded_logged: Option<String>,
}

/// Why a day was not recorded, and whether only a configuration change can
/// cure it (in which case retrying is still correct, but logging it every day
/// would repeat a known fact).
struct DayFailure {
    reason: String,
    needs_config: bool,
}

impl MultiplierStage {
    pub(crate) fn new(plan_prices: Option<Arc<dyn PlanPriceSource>>) -> Self {
        Self {
            plan_prices,
            recorded: BTreeSet::new(),
            failure_logged: BTreeSet::new(),
            not_recorded_logged: None,
        }
    }

    /// `catalog_pending`: fusiform is configured but its history boundary is
    /// not yet discovered, so rates cannot be read yet. The day waits for it
    /// rather than failing.
    pub(crate) fn tick(&mut self, state: &SharedState, catalog_pending: bool, now_ms: i64) {
        let interval_end_ms = now_ms - now_ms.rem_euclid(DAY_MS);
        // The seven days the look-back covers, oldest first, each with the
        // end of its interval. A clock so far out that one of them has no
        // calendar day records nothing this tick rather than a made-up day.
        let days = match (0..7_i64)
            .rev()
            .map(|days_back| {
                let end_ms = days_back
                    .checked_mul(DAY_MS)
                    .and_then(|back_ms| interval_end_ms.checked_sub(back_ms))
                    .ok_or_else(|| format!("{days_back} days before {interval_end_ms} ms"))?;
                let day = multiplier::utc_day(end_ms).map_err(|error| error.message())?;
                Ok((end_ms, day))
            })
            .collect::<Result<Vec<(i64, String)>, String>>()
        {
            Ok(days) => days,
            Err(error) => {
                tracing::warn!(
                    target: "multiplier",
                    now_ms,
                    %error,
                    "daily multiplier skipped: the clock is outside the representable UTC calendar"
                );
                return;
            }
        };
        let (Some((_, oldest_day)), Some((_, day))) = (days.first(), days.last()) else {
            return;
        };
        let (oldest_day, day) = (oldest_day.clone(), day.clone());
        let config = state.desired_config();
        let multiplier_config = match &config.subscription_multiplier {
            config::MultiplierSetting::Recorded(multiplier_config) => multiplier_config,
            config::MultiplierSetting::NotRecorded => {
                if self.not_recorded_logged.as_ref() != Some(&day) {
                    tracing::info!(
                        target: "multiplier",
                        %day,
                        "daily multiplier skipped: the committed desired generation does not record \
                         the subscription_multiplier member"
                    );
                    self.not_recorded_logged = Some(day);
                }
                return;
            }
        };
        self.recorded.retain(|(_, day)| day >= &oldest_day);
        self.failure_logged
            .retain(|(_, day, _)| day.as_ref().is_none_or(|day| day >= &oldest_day));
        for quota_provider in multiplier_config.quota_providers.keys() {
            for (interval_end_ms, day) in &days {
                let (interval_end_ms, day) = (*interval_end_ms, day.clone());
                let recorded_key = (quota_provider.clone(), day.clone());
                if self.recorded.contains(&recorded_key) {
                    continue;
                }
                match state.with_read_op(|conn| {
                    multiplier::observation_exists(conn, quota_provider, &day)
                        .map_err(|error| ("internal".to_string(), error.to_string()))
                }) {
                    Ok(true) => {
                        self.recorded.insert(recorded_key);
                        continue;
                    }
                    Ok(false) => {}
                    Err((_, error)) => {
                        self.log_failure(
                            quota_provider,
                            &day,
                            DayFailure {
                                reason: format!("existence check: {error}"),
                                needs_config: false,
                            },
                        );
                        continue;
                    }
                }
                let not_ingested = scoped_producers_not_ingested(&config, quota_provider);
                if !not_ingested.is_empty() {
                    self.log_failure(
                        quota_provider,
                        &day,
                        DayFailure {
                            reason: format!(
                                "scoped_producer_not_ingested: {}",
                                not_ingested.join(", ")
                            ),
                            needs_config: true,
                        },
                    );
                    continue;
                }
                let catalog = state.catalog_client();
                if catalog.is_none() && catalog_pending {
                    continue;
                }
                let outcome = match catalog {
                    Some(catalog) => record_multiplier_day(
                        state,
                        &catalog,
                        self.plan_prices.as_deref(),
                        multiplier_config,
                        quota_provider,
                        interval_end_ms,
                        now_ms,
                    ),
                    None => Err(DayFailure {
                        reason: "no fusiform catalog is configured, so no API rate can be read"
                            .into(),
                        needs_config: true,
                    }),
                };
                // A day that fails, including one whose catalog cannot be read
                // (`rate_fact_unrecognized`), writes nothing and is retried here
                // on every later tick; `log_failure` logs it once per day and reason
                // so a failure that lasts all day is not repeated every minute.
                match outcome {
                    Ok(observation) => {
                        self.recorded.insert(recorded_key);
                        tracing::info!(
                            target: "multiplier",
                            quota_provider,
                            %day,
                            multiplier = ?observation.multiplier,
                            fee_multiplier = ?observation.fee_multiplier,
                            delta_percent = observation.delta_percent,
                            api_nanos = observation.api_nanos,
                            subscription_nanos = observation.subscription_nanos,
                            "multiplier observation recorded"
                        );
                    }
                    Err(failure) => {
                        self.log_failure(quota_provider, &day, failure);
                    }
                }
            }
        }
    }

    /// Logs a failed day unless the same failure was already logged: the same
    /// day and reason, or (for a failure only the configuration can cure) the
    /// same reason on any day.
    /// Returns whether it logged.
    fn log_failure(&mut self, quota_provider: &str, day: &str, failure: DayFailure) -> bool {
        let key = (
            quota_provider.to_string(),
            (!failure.needs_config).then(|| day.to_string()),
            failure.reason,
        );
        if self.failure_logged.contains(&key) {
            return false;
        }
        tracing::warn!(
            target: "multiplier",
            quota_provider,
            %day,
            reason = %key.2,
            needs_config = failure.needs_config,
            "multiplier observation not recorded; retried on every later tick"
        );
        self.failure_logged.insert(key);
        true
    }
}

/// The producers a quota provider's scope names that the configuration in
/// force does not ingest: excluded by `harness_ingest`, or all of ingestion
/// off. The quota's used percent still counts their traffic, so a day
/// computed without their tokens would understate the multiplier with nothing
/// to show it; the day fails by name instead.
///
/// This is checked against the configuration, not against `source_runtime`
/// rows: a producer whose store is absent (BROCA not installed included) also
/// has no row, but "nothing on disk to read" loses no tokens, while "told not
/// to read it" does.
///
/// The check reads only the configuration in force now. A producer excluded
/// for part of the trailing seven days and re-enabled before the day is
/// computed still leaves a gap in that interval; nothing records when
/// ingestion was off.
fn scoped_producers_not_ingested(
    config: &config::AstrocyteConfig,
    quota_provider: &str,
) -> Vec<String> {
    let config::MultiplierSetting::Recorded(multiplier) = &config.subscription_multiplier else {
        return Vec::new();
    };
    let Some(provider) = multiplier.quota_providers.get(quota_provider) else {
        return Vec::new();
    };
    provider
        .scope
        .iter()
        .map(|rule| rule.producer.as_str())
        .filter(|producer| !config.harness_ingest.ingests(producer))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Why a day's observation could not be computed: a wire code, its message,
/// and whether only a configuration change can cure it.
pub(crate) struct DayError {
    code: String,
    message: String,
    needs_config: bool,
}

impl From<DayError> for DayFailure {
    fn from(error: DayError) -> Self {
        Self {
            reason: format!("{}: {}", error.code, error.message),
            needs_config: error.needs_config,
        }
    }
}

/// A completed day's tokens may still be waiting in any ingested source, not
/// just a source in this provider's scope. Absent and skipped sources have no
/// runtime row. Bound the wait so a broken source cannot leave a permanent gap.
fn multiplier_stale_sources(
    state: &SharedState,
    interval_end_ms: i64,
    snapshot_at_ms: i64,
) -> Result<Vec<String>, DayError> {
    let sources = state
        .with_read_op(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT source_key FROM source_runtime
                     WHERE last_success_ms IS NULL OR last_success_ms < ?1
                     ORDER BY source_key",
                )
                .map_err(|error| ("internal".to_string(), error.to_string()))?;
            let sources = statement
                .query_map([interval_end_ms], |row| row.get::<_, String>(0))
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
                .map_err(|error| ("internal".to_string(), error.to_string()))?;
            Ok(sources)
        })
        .map_err(|(code, message)| DayError {
            code,
            message,
            needs_config: false,
        })?;
    if !sources.is_empty() && snapshot_at_ms < interval_end_ms + 2 * 3_600_000 {
        return Err(DayError {
            code: "source_reads_pending".into(),
            message: format!(
                "waiting for a successful read after midnight: {}",
                sources.join(", ")
            ),
            needs_config: false,
        });
    }
    Ok(sources)
}

fn disclose_stale_sources(observation: &mut multiplier::Observation, sources: &[String]) {
    if !sources.is_empty() {
        observation.error_deflating.push_str(&format!(
            " stale_sources: {}; no successful read after the interval ended, so late usage may be missing.",
            sources.join(", ")
        ));
    }
}

/// Computes one day and appends it as the day's original row.
fn record_multiplier_day(
    state: &SharedState,
    catalog: &CatalogClient,
    plan_prices: Option<&dyn PlanPriceSource>,
    config: &MultiplierConfig,
    quota_provider: &str,
    interval_end_ms: i64,
    snapshot_at_ms: i64,
) -> Result<multiplier::Observation, DayFailure> {
    let stale_sources = multiplier_stale_sources(state, interval_end_ms, snapshot_at_ms)?;
    let mut observation = compute_multiplier_day(
        state,
        catalog,
        plan_prices,
        config,
        quota_provider,
        interval_end_ms,
        snapshot_at_ms,
    )?;
    disclose_stale_sources(&mut observation, &stale_sources);
    state
        .ordered_write(|tx| {
            multiplier::insert_observation_in_tx(tx, &observation)
                .map_err(|error| format!("{}: {}", error.code(), error.message()))?;
            Ok(((), true))
        })
        .map_err(|reason| DayFailure {
            reason,
            needs_config: false,
        })?;
    Ok(observation)
}

/// Resolve plan prices, read, resolve rates, then compute; nothing is written.
/// Plan prices come first because they need no store read, so an unreachable
/// fusiform fails the day before the store is scanned. The store reads run in
/// read transactions on the reader connection and no fusiform call happens
/// inside a transaction, so the caller's append can be its own short write.
fn compute_multiplier_day(
    state: &SharedState,
    catalog: &CatalogClient,
    plan_prices: Option<&dyn PlanPriceSource>,
    config: &MultiplierConfig,
    quota_provider: &str,
    interval_end_ms: i64,
    snapshot_at_ms: i64,
) -> Result<multiplier::Observation, DayError> {
    let failed = |error: multiplier::MultiplierError| DayError {
        code: error.code().to_string(),
        message: error.message(),
        needs_config: false,
    };
    let read_failed = |(code, message): (String, String)| DayError {
        code,
        message,
        needs_config: false,
    };
    let provider = config
        .quota_providers
        .get(quota_provider)
        .ok_or_else(|| DayError {
            code: "unknown_quota_provider".into(),
            message: format!("quota provider '{quota_provider}' is not configured"),
            needs_config: true,
        })?;
    let plan_prices = multiplier::resolve_plans(plan_prices, config, provider, snapshot_at_ms)
        .map_err(|error| DayError {
            // Without a plan-price source only a configuration change helps;
            // a failed call to a configured one may succeed on the next tick.
            needs_config: plan_prices.is_none(),
            ..failed(error)
        })?;
    let plan = state
        .with_read_op(|conn| {
            multiplier::plan_observation(
                conn,
                config,
                quota_provider,
                interval_end_ms - MULTIPLIER_INTERVAL_MS,
                interval_end_ms,
            )
            .map_err(|error| (error.code().to_string(), error.message()))
        })
        .map_err(read_failed)?;
    let models = multiplier::catalog_models(config, &plan);
    let rates = multiplier::resolve_rates(catalog, &models, snapshot_at_ms).map_err(failed)?;
    let observation = state
        .with_read_op(|conn| {
            multiplier::compute_observation(
                conn,
                config,
                &plan,
                &rates,
                &plan_prices,
                snapshot_at_ms,
                &state.desired_config().reasoning_billed_as_output,
            )
            .map_err(|error| (error.code().to_string(), error.message()))
        })
        .map_err(read_failed)?;
    Ok(observation)
}

/// `spend.multiplier.recompute`: computes a day over the interval
/// the daily stage used for it (the seven days ending at its 00:00Z) with the
/// current configuration and catalog, and appends one row per quota provider
/// that supersedes that day's head, or creates its original if none exists.
///
/// Every refusal writes nothing: an unknown provider, a missing day still
/// waiting for source reads, or a computation that fails (an unreadable catalog
/// is the same failure the daily stage retries). All fusiform calls finish
/// before the write transaction opens, and all providers are appended in that
/// one transaction or none is.
///
/// The request carries no operator identity, as for `spend.event.reprice`:
/// the caller is whoever the transport admitted, and the request type refuses
/// any extra field that would let a caller name themselves.
pub(crate) fn recompute_multiplier_day(
    state: &SharedState,
    plan_prices: Option<&dyn PlanPriceSource>,
    request: &multiplier::MultiplierRecomputeRequest,
    now_ms: i64,
) -> Result<serde_json::Value, (String, String)> {
    let config = state.desired_config();
    let config::MultiplierSetting::Recorded(multiplier_config) = &config.subscription_multiplier
    else {
        return Err((
            "multiplier_not_recorded".into(),
            "the committed desired generation does not record the subscription_multiplier member"
                .into(),
        ));
    };
    let day = &request.observation_day;
    let interval_end_ms = multiplier::utc_day_start_ms(day).ok_or_else(|| {
        (
            "malformed".to_string(),
            format!("observation_day {day:?} is not a YYYY-MM-DD date"),
        )
    })?;
    let quota_providers: Vec<String> = match &request.quota_provider {
        Some(quota_provider)
            if multiplier_config
                .quota_providers
                .contains_key(quota_provider) =>
        {
            vec![quota_provider.clone()]
        }
        Some(quota_provider) => {
            return Err((
                "unknown_quota_provider".into(),
                format!("quota provider '{quota_provider}' is not configured"),
            ))
        }
        None => multiplier_config.quota_providers.keys().cloned().collect(),
    };
    if quota_providers.is_empty() {
        return Err((
            "unknown_quota_provider".into(),
            "no quota provider is configured".into(),
        ));
    }
    // Preflight every requested scope before any catalog call: quota usage
    // includes excluded producers, but their tokens would be missing.
    for quota_provider in &quota_providers {
        let not_ingested = scoped_producers_not_ingested(&config, quota_provider);
        if !not_ingested.is_empty() {
            return Err((
                "scoped_producer_not_ingested".into(),
                format!(
                    "quota provider '{quota_provider}': {}",
                    not_ingested.join(", ")
                ),
            ));
        }
    }
    let catalog = state.catalog_client().ok_or_else(|| {
        (
            "catalog_unavailable".to_string(),
            "no fusiform catalog is connected, so no API rate can be read".to_string(),
        )
    })?;
    let mut observations = Vec::new();
    for quota_provider in &quota_providers {
        let exists = state.with_read_op(|conn| {
            multiplier::observation_exists(conn, quota_provider, day)
                .map_err(|error| ("internal".to_string(), error.to_string()))
        })?;
        let stale_sources = if exists {
            Vec::new()
        } else {
            multiplier_stale_sources(state, interval_end_ms, now_ms)
                .map_err(|error| (error.code, error.message))?
        };
        let mut observation = compute_multiplier_day(
            state,
            &catalog,
            plan_prices,
            multiplier_config,
            quota_provider,
            interval_end_ms,
            now_ms,
        )
        .map_err(|error| (error.code, error.message))?;
        disclose_stale_sources(&mut observation, &stale_sources);
        observations.push(observation);
    }
    state.ordered_op_write(|tx| {
        let mut recomputed = Vec::new();
        for observation in &observations {
            // Recheck inside the writer transaction: another writer may have
            // created this day while its catalog calls were in flight.
            let (observation_id, supersedes) = match multiplier::observation_head(
                tx,
                &observation.quota_provider,
                &observation.observation_day,
            )
            .map_err(|error| ("internal".to_string(), error.to_string()))?
            {
                Some(_) => {
                    let superseded =
                        multiplier::insert_superseding_observation_in_tx(tx, observation)
                            .map_err(|error| (error.code().to_string(), error.message()))?;
                    (superseded.observation_id, Some(superseded.supersedes))
                }
                None => {
                    multiplier::insert_observation_in_tx(tx, observation)
                        .map_err(|error| (error.code().to_string(), error.message()))?;
                    (tx.last_insert_rowid(), None)
                }
            };
            recomputed.push(serde_json::json!({
                "quota_provider": observation.quota_provider,
                "observation_day": observation.observation_day,
                "observation_id": observation_id,
                "supersedes": supersedes,
                "multiplier": observation.multiplier,
                "fee_multiplier": observation.fee_multiplier,
            }));
        }
        Ok((serde_json::json!({ "recomputed": recomputed }), true))
    })
}

// The backfill and timestamp fixtures in these tests stay within i64.
#[allow(clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {

    fn sequence_less_runtime(state: &SharedState, key: &str) -> (Option<i64>, i64) {
        state
            .with_conn(|conn| {
                conn.query_row(
                "SELECT last_success_ms,pending_unknown FROM source_runtime WHERE source_key=?1",
                [key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    #[test]
    fn harness_success_failure_success_persists_unknown_pending() {
        let state = test_state();
        let key = "gemini";
        harness_drain::ensure_runtime_row(&state, key).unwrap();
        harness_drain::record_requests(&state, key, &[], 101, &|| false).unwrap();
        assert_eq!(sequence_less_runtime(&state, key), (Some(101), 0));
        harness_drain::mark_drain_failed(&state, key, "read failed".into());
        assert_eq!(sequence_less_runtime(&state, key), (Some(101), 1));
        harness_drain::record_requests(&state, key, &[], 102, &|| false).unwrap();
        assert_eq!(sequence_less_runtime(&state, key), (Some(102), 0));
    }

    #[test]
    fn dedicated_success_failure_success_persists_unknown_pending() {
        let state = test_state();
        let temp = tempfile::TempDir::new().unwrap();
        let claude_root = temp.path().join("claude");
        let codex_root = temp.path().join("codex");
        let pi_root = temp.path().join("pi");
        for root in [&claude_root, &pi_root] {
            std::fs::create_dir(root).unwrap();
        }
        std::fs::create_dir_all(codex_root.join("sessions")).unwrap();
        let claude = claude_code_source::ClaudeCodeSource::at(&claude_root);
        let codex = codex_source::CodexSource::at(&codex_root);
        let pi = pi_source::PiSource::at(&pi_root);
        type DrainCase<'a> = (&'static str, &'a dyn Fn(i64), fn(&SharedState, String));
        let sources: [DrainCase<'_>; 3] = [
            (
                claude_code_source::CLAUDE_CODE_SOURCE_KEY,
                &|now| {
                    claude.drain(&state, now, &NEVER_STOP).unwrap();
                },
                mark_claude_code_drain_failed,
            ),
            (
                codex_source::CODEX_SOURCE_KEY,
                &|now| {
                    codex.drain(&state, now, &NEVER_STOP).unwrap();
                },
                mark_codex_drain_failed,
            ),
            (
                pi_source::PI_SOURCE_KEY,
                &|now| {
                    pi.drain(&state, now, &NEVER_STOP).unwrap();
                },
                mark_pi_drain_failed,
            ),
        ];
        for (key, success, fail) in sources {
            success(101);
            assert_eq!(sequence_less_runtime(&state, key), (Some(101), 0), "{key}");
            fail(&state, "read failed".into());
            assert_eq!(sequence_less_runtime(&state, key), (Some(101), 1), "{key}");
            success(102);
            assert_eq!(sequence_less_runtime(&state, key), (Some(102), 0), "{key}");
        }
    }

    #[test]
    fn failed_mark_store_error_is_visible_in_degraded_reason() {
        let state = test_state();
        harness_drain::ensure_runtime_row(&state, "gemini").unwrap();
        state.with_conn(|conn| {
            conn.execute_batch("CREATE TRIGGER refuse_mark BEFORE UPDATE ON source_runtime BEGIN SELECT RAISE(ABORT, 'mark refused'); END;")
                .map_err(|error| error.to_string())
        }).unwrap();
        harness_drain::mark_drain_failed(&state, "gemini", "read failed".into());
        let reason = state.producer_degraded_reason("gemini").unwrap();
        assert!(reason.contains("read failed"), "{reason}");
        assert!(reason.contains("mark refused"), "{reason}");
        harness_drain::mark_drain_failed(&state, "goose", "read failed".into());
        let reason = state.producer_degraded_reason("goose").unwrap();
        assert!(reason.contains("row missing"), "{reason}");
    }

    fn assert_path_origin(operator: bool, expected: &str) {
        let state = test_state();
        seed_unpriced_event(&state);
        let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        if operator {
            crate::ops::dispatch(
                &state,
                crate::ops::OP_EVENT_REPRICE,
                serde_json::json!({"event_id":"ev-late"}),
            )
            .unwrap();
        } else {
            assert_eq!(
                price_unpriced_events(&state, 1_800_000_000_001, &NEVER_STOP).unwrap(),
                1
            );
        }
        let row: (String, String, i64) = state.with_conn(|conn| {
            conn.query_row("SELECT origin,outcome,pricing_generation FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='ev-late')", [],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|e.to_string())
        }).unwrap();
        assert_eq!(row, (expected.into(), "priced".into(), 1));
    }

    #[test]
    fn refresher_pass_writes_pricing_pass_origin() {
        assert_path_origin(false, "pricing_pass");
    }

    #[test]
    fn operator_reprice_writes_operator_reprice_origin() {
        assert_path_origin(true, "operator_reprice");
    }

    /// The pricing pass checks the stop flag before each event. A pass that
    /// starts with the flag already set prices nothing, and reports that as
    /// success because a stop is not a pricing failure; the event stays
    /// unpriced and the next pass picks it up.
    #[test]
    fn a_stopped_pricing_pass_prices_no_further_event() {
        let state = test_state();
        seed_unpriced_event(&state);
        let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
        state.install_catalog(Some(catalog)).unwrap();

        assert_eq!(
            price_unpriced_events(&state, 1_800_000_000_001, &AtomicBool::new(true)).unwrap(),
            0
        );
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(
            price_unpriced_events(&state, 1_800_000_000_002, &NEVER_STOP).unwrap(),
            1,
            "the event skipped by the stopped pass is priced by the next one"
        );
    }

    use super::*;
    use crate::state::test_state;
    use astrocyte_core::catalog_client::CatalogCallError;
    use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
    use std::sync::atomic::AtomicUsize;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    /// A catalog peer that is not registered for the first `refusals` calls
    /// (the daemon-restart shape: every module booting at once, fusiform not
    /// yet live) and answers the discovery probe from then on.
    struct LateCatalog {
        refusals: usize,
        calls: AtomicUsize,
        /// Code served while refusing. `unavailable` is the warming peer;
        /// anything non-retryable models a producer this binary cannot talk to.
        refusal_code: &'static str,
    }

    impl LateCatalog {
        fn warming(refusals: usize) -> Self {
            Self {
                refusals,
                calls: AtomicUsize::new(0),
                refusal_code: "unavailable",
            }
        }
    }

    impl CatalogConnection for LateCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < self.refusals {
                return Err(CatalogCallError {
                    code: self.refusal_code.into(),
                    message: "route.open error: unknown_module 'fusiform' is not registered".into(),
                });
            }
            if request.at_ms == Some(0) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "no record at 0: fusiform's history begins at 1786529249396. An empty answer would claim the catalog was empty; it was unobserved.".into(),
                });
            }
            let rate = serde_json::json!({"state":"priced","units":3_000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
            Ok(serde_json::from_value(serde_json::json!({
                "source": "late-fake",
                "resolved_at_ms": request.at_ms,
                "catalog_version": 1,
                "models": {"anthropic/claude": {"rate.input": rate,
                    "rate.output": {"state":"priced","units":6000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}},
                "withheld": [], "uncertain": [], "overridden": []
            }))
            .unwrap())
        }
    }

    fn seed_unpriced_event(state: &SharedState) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO usage_event (event_id,producer,producer_ref,recorded_at_ms,occurred_at_ms,provider,model,charge_basis)
                     VALUES ('ev-late','broca','ev-late',1,1_800_000_000_000,'anthropic','claude','metered_api')",
                    [],
                )
                .map_err(|e| e.to_string())?;
                tx.execute(
                    "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                     VALUES ((SELECT event_key FROM usage_event WHERE event_id='ev-late'),'input','token','billed','producer',10,0)",
                    [],
                )
                .map_err(|e| e.to_string())?;
                // This fixture stands in for recording, which queues the
                // event in the recording transaction.
                astrocyte_core::pricing::enqueue_if_unpriced_in_tx(tx, "ev-late")
                    .map_err(|e| e.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn reasoning_billing_reaches_pricing_pass_reprice_and_basis_correction() {
        use serde_json::json;
        for path in ["pass", "reprice", "basis"] {
            for declared in [false, true] {
                let state = test_state();
                let mut desired = config::intentional_empty().config;
                if declared {
                    desired.reasoning_billed_as_output.push("anthropic".into());
                }
                state.publish_desired(desired, 1);
                let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
                state.install_catalog(Some(catalog)).unwrap();
                let event_id = crate::ops::dispatch(&state, crate::ops::OP_EVENT_INGEST, json!({
                    "producer":"producer", "producer_lineage":"lineage", "producer_ref":"thinking",
                    "provider":"anthropic", "model":"claude", "occurred_at_ms":1_800_000_000_000_i64,
                    "charge_basis": if path == "basis" { "unknown" } else { "metered_api" },
                    "measurements":[{"role":"reasoning","unit":"token","kind":"billed","source":"usage.reasoning","quantity_units":1000,"quantity_exponent":0}]
                })).unwrap()["event_id"].as_str().unwrap().to_owned();
                match path {
                    "pass" => {
                        assert_eq!(
                            price_unpriced_events(
                                &state,
                                1_800_000_000_001,
                                &AtomicBool::new(false)
                            )
                            .unwrap(),
                            1
                        );
                    }
                    "reprice" => {
                        crate::ops::dispatch(
                            &state,
                            crate::ops::OP_EVENT_REPRICE,
                            json!({"event_id":event_id}),
                        )
                        .unwrap();
                    }
                    _ => {
                        crate::ops::dispatch(&state, crate::ops::OP_BASIS_CORRECT, json!({"event_id":event_id,"corrected_basis":"metered_api","reason":"API-key evidence"})).unwrap();
                    }
                }
                let stored: (String, Option<i64>, Option<String>, String) = state.with_conn(|conn| {
                    conn.query_row("SELECT outcome,charge_nanos,unpriced_reason,json_extract(catalog_entry,'$.fact_key') FROM priced_charge", [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).map_err(|error| error.to_string())
                }).unwrap();
                let expected = if declared {
                    ("priced".into(), Some(6), None, "rate.output".into())
                } else {
                    (
                        "unpriced".into(),
                        None,
                        Some("no_rate".into()),
                        "rate.reasoning".into(),
                    )
                };
                assert_eq!(stored, expected, "{path}, declared={declared}");
            }
        }
    }

    fn priced_charge_rows(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |r| r.get(0))
                    .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    /// The pricing-pass hold must scope to the state it was taken on. Tests
    /// run as threads inside one process, so a hold that reached every state
    /// would let one test's held refresher silently skip the pricing pass of
    /// any test running beside it and fail that test's priced-row
    /// assertions. Here the held state's own iteration prices nothing while a
    /// second state's iteration prices its queued event.
    #[test]
    fn pricing_hold_on_one_state_does_not_hold_pricing_on_another() {
        let held = test_state();
        let _hold = hold_pricing_pass_for_test(&held);
        seed_unpriced_event(&held);
        held.install_catalog(Some(
            CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
        ))
        .unwrap();
        run_iteration(&held, &[], &NEVER_STOP);
        assert_eq!(priced_charge_rows(&held), 0);

        let other = test_state();
        seed_unpriced_event(&other);
        other
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
            ))
            .unwrap();
        run_iteration(&other, &[], &NEVER_STOP);
        assert_eq!(priced_charge_rows(&other), 1);
    }

    /// The production failure: a configured catalog that refuses its discovery
    /// probe at boot must not stop the daemon, must not price under a wrong
    /// reason while it waits, and must price on the first tick after the peer
    /// appears. Asserted by rows, not by which branch ran.
    #[test]
    fn catalog_unreachable_at_boot_defers_pricing_and_recovers_when_the_peer_appears() {
        let state = test_state();
        seed_unpriced_event(&state);
        let stop = AtomicBool::new(false);
        let late: Arc<dyn CatalogConnection> = Arc::new(LateCatalog::warming(2));
        let pending = PendingCatalog { connection: late };

        // Tick 1 and 2: peer refuses. Pricing must be SKIPPED, leaving no head
        // at all -- a `catalog_unconfigured` row here would be a lie that only a
        // manual reprice could correct.
        for _ in 0..2 {
            assert!(!pending.try_connect(&state).unwrap());
            assert!(state.catalog_client().is_none());
            run_iteration_full(&state, &[], None, true, &stop);
            assert_eq!(
                priced_charge_rows(&state),
                0,
                "no head while the catalog is pending"
            );
        }

        // Tick 3: peer is live. Discovery installs the catalog and the same
        // tick prices the waiting event.
        assert!(pending.try_connect(&state).unwrap());
        assert_eq!(
            state.catalog_client().unwrap().history_start_ms(),
            1_786_529_249_396
        );
        run_iteration_full(&state, &[], None, false, &stop);
        assert!(
            priced_charge_rows(&state) > 0,
            "first tick after the peer appears prices"
        );
        let outcomes: Vec<(String, Option<String>)> = state
            .with_conn(|conn| {
                let mut s = conn
                    .prepare("SELECT outcome,unpriced_reason FROM priced_charge")
                    .map_err(|e| e.to_string())?;
                let rows = s
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                Ok(rows)
            })
            .unwrap();
        assert!(
            outcomes.iter().any(|(o, _)| o == "priced"),
            "the waiting event resolves against the late catalog: {outcomes:?}"
        );
        assert!(
            !outcomes
                .iter()
                .any(|(_, r)| r.as_deref() == Some("catalog_unconfigured")),
            "a configured-but-late catalog must never be recorded as unconfigured: {outcomes:?}"
        );
    }

    #[test]
    fn history_status_transient_failure_defers_pricing_and_retries_each_tick() {
        use crate::fusiform_client::fixtures::{CatalogPeer, StatusMode};

        let peer = CatalogPeer::start_with_status(
            1_786_529_249_396,
            9,
            66_666,
            StatusMode::Transient { failures: 2 },
        );
        let state = test_state();
        seed_unpriced_event(&state);
        let mut config = config::intentional_empty().config;
        config.fusiform_connection_file = Some(peer.path.to_string_lossy().into_owned());
        let (mut pending, _) = configure_catalog(&state, &config).unwrap();
        assert!(
            pending.is_some(),
            "boot must stay pending after status fails"
        );
        assert!(state.catalog_client().is_none());
        let mut pricing = PricingWalk::new(PricingBudget::WHOLE_QUEUE);
        let mut reconciler =
            QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET);
        run_tick(
            &state,
            &[],
            &FileSources::default(),
            None,
            &mut pending,
            &mut pricing,
            &mut reconciler,
            &NEVER_STOP,
        );
        assert!(pending.is_some(), "the first tick's status still fails");
        assert!(state.catalog_client().is_none());
        assert_eq!(
            priced_charge_rows(&state),
            0,
            "pending is not catalog_unconfigured"
        );
        run_tick(
            &state,
            &[],
            &FileSources::default(),
            None,
            &mut pending,
            &mut pricing,
            &mut reconciler,
            &NEVER_STOP,
        );
        assert!(pending.is_none(), "the second tick must rediscover status");
        assert_eq!(
            state.catalog_client().unwrap().history_start_ms(),
            1_786_529_249_396
        );
        assert!(
            priced_charge_rows(&state) > 0,
            "the recovered tick must price"
        );
        let unconfigured: i64 = state.with_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM priced_charge WHERE unpriced_reason='catalog_unconfigured'",
                [], |row| row.get(0)).map_err(|error| error.to_string())
        }).unwrap();
        assert_eq!(unconfigured, 0);
        let requests = peer.finish();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["name"] == "catalog.status")
                .count(),
            3
        );
        assert!(
            !requests
                .iter()
                .any(|request| request["name"] == "catalog.get"
                    && request["arguments"]["at_ms"] == 0),
            "transient status errors must not trigger the text fallback: {requests:?}"
        );
    }

    #[test]
    fn history_status_error_stays_pending_without_a_text_probe() {
        use crate::fusiform_client::fixtures::{CatalogPeer, StatusMode};

        let peer =
            CatalogPeer::start_with_status(34_567, 9, 66_666, StatusMode::Refused("bad_request"));
        let state = test_state();
        seed_unpriced_event(&state);
        let mut config = config::intentional_empty().config;
        config.fusiform_connection_file = Some(peer.path.to_string_lossy().into_owned());
        let (mut pending, _) = configure_catalog(&state, &config).unwrap();
        assert!(pending.is_some());
        assert!(state.catalog_client().is_none());
        run_tick(
            &state,
            &[],
            &FileSources::default(),
            None,
            &mut pending,
            &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
            &mut QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET),
            &NEVER_STOP,
        );
        assert!(pending.is_some());
        assert!(state.catalog_client().is_none());
        assert_eq!(priced_charge_rows(&state), 0);
        let requests = peer.finish();
        assert_eq!(
            requests.len(),
            2,
            "startup and the next tick must try status"
        );
        assert!(
            requests
                .iter()
                .all(|request| request["name"] == "catalog.status"),
            "a status refusal is not evidence that the text fallback is safe: {requests:?}"
        );
    }

    /// The production crash loop of 2026-09-06: a producer one protocol version
    /// behind refused discovery with a code the classifier called non-retryable,
    /// try_connect returned Err, the refresher thread died, and the crash
    /// budget was spent in seconds. Pricing may wait; the thread may not die.
    /// Asserted on the effect: try_connect is Ok(false), nothing installed,
    /// and the next tick still runs ingest (the iteration returns normally).
    #[test]
    fn non_retryable_discovery_refusal_defers_pricing_and_does_not_kill_the_loop() {
        let state = test_state();
        let stop = AtomicBool::new(false);
        let peer: Arc<dyn CatalogConnection> = Arc::new(LateCatalog {
            refusals: usize::MAX,
            calls: AtomicUsize::new(0),
            refusal_code: "future_code",
        });
        let pending = PendingCatalog { connection: peer };
        assert_eq!(
            pending.try_connect(&state),
            Ok(false),
            "a non-retryable refusal defers; it is never an Err from try_connect"
        );
        assert!(state.catalog_client().is_none());
        run_iteration_full(&state, &[], None, true, &stop);
        assert_eq!(priced_charge_rows(&state), 0);
    }

    /// Startup with a configured catalog that is not yet registered returns a
    /// pending handle and no error: the daemon starts and the loop retries.
    #[test]
    fn configure_catalog_with_unreachable_peer_is_pending_not_fatal() {
        let state = test_state();
        // A connection file that no daemon serves: transport builds, discovery refuses.
        let dir =
            std::env::temp_dir().join(format!("astrocyte-late-catalog-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subc-connection.json");
        let mut config = crate::config::intentional_empty().config;
        config.fusiform_connection_file = Some(path.to_string_lossy().into_owned());
        // FusiformClient::new reads the file at construction; write an unroutable endpoint.
        subc_transport::connection_file::write_atomic(
            &path,
            &subc_transport::connection_file::ConnectionInfo {
                schema: subc_transport::connection_file::SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![subc_transport::connection_file::Endpoint {
                    host: "127.0.0.1".into(),
                    port: 1,
                }],
                key: vec![7; subc_transport::KEY_LEN],
                daemon_id: [9; 16],
                pid: std::process::id(),
                daemon_ver: "test".into(),
            },
        )
        .unwrap();
        let (pending, plan_prices) =
            configure_catalog(&state, &config).expect("unreachable peer is not a startup error");
        assert!(pending.is_some(), "discovery deferred, handle returned");
        assert!(
            plan_prices.is_some(),
            "the same fusiform client serves plan prices"
        );
        assert!(state.catalog_client().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The account and provider the plans in the tests below are declared for.
    /// An event naming this pair is inside the join, so only its instant or its
    /// head decides whether it is selected. Two tests deliberately record an
    /// event under a different provider and under a different account: coverage
    /// is keyed on both, and a fixture that never varies a column cannot hold
    /// the predicate that reads it.
    const ACCOUNT: &str = "account-a";
    const PROVIDER: &str = "anthropic";

    fn declare_plan(state: &SharedState, effective_from: i64) {
        state
            .ordered_write(|tx| {
                astrocyte_core::subscription::set_subscription_in_tx(
                    tx,
                    &astrocyte_core::subscription::SetSubscriptionRequest {
                        account_ref: ACCOUNT.into(),
                        provider: PROVIDER.into(),
                        plan_name: "team".into(),
                        recurring_cost_minor: 2_000,
                        currency: "USD".into(),
                        amount_exponent: 2,
                        period: astrocyte_core::subscription::SubscriptionPeriod::Month,
                        effective_from,
                    },
                    None,
                )
                .map_err(|error| error.message())?;
                Ok(((), true))
            })
            .unwrap();
    }

    /// Stands in for recording, so it queues the event in the same transaction
    /// exactly as recording does.
    fn insert_event(
        tx: &Transaction<'_>,
        event_id: &str,
        provider: &str,
        account_ref: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) -> Result<(), String> {
        insert_event_unqueued(
            tx,
            event_id,
            provider,
            account_ref,
            occurred_at_ms,
            charge_basis,
            model,
        )?;
        astrocyte_core::pricing::enqueue_if_unpriced_in_tx(tx, event_id)
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// An event and its measurement written WITHOUT the queue row: what a
    /// write path that forgot to enqueue would leave behind.
    fn insert_event_unqueued(
        tx: &Transaction<'_>,
        event_id: &str,
        provider: &str,
        account_ref: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) -> Result<(), String> {
        tx.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_ref,recorded_at_ms,occurred_at_ms,
                 provider,model,account_ref,charge_basis
             ) VALUES (?1,'broca',?1,1,?2,?3,?4,?5,?6)",
            params![
                event_id,
                occurred_at_ms,
                provider,
                model,
                account_ref,
                charge_basis
            ],
        )
        .map_err(|error| error.to_string())?;
        tx.execute(
            "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','producer',10,0)",
            [event_id],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// An event under the pair the plans here are declared for.
    fn insert_account_event(
        tx: &Transaction<'_>,
        event_id: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) -> Result<(), String> {
        insert_event(
            tx,
            event_id,
            PROVIDER,
            ACCOUNT,
            occurred_at_ms,
            charge_basis,
            model,
        )
    }

    fn seed_account_event(
        state: &SharedState,
        event_id: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) {
        state
            .ordered_write(|tx| {
                insert_account_event(tx, event_id, occurred_at_ms, charge_basis, model)?;
                Ok(((), true))
            })
            .unwrap();
    }

    /// A subscription-basis event under a provider and account the caller
    /// names, for the arms that must differ from the pair the plan is declared
    /// for. Both halves of that pair decide coverage, so both have to vary
    /// somewhere: a predicate on a column every fixture fills with one value is
    /// a predicate no test can hold.
    fn seed_event_under(
        state: &SharedState,
        event_id: &str,
        provider: &str,
        account_ref: &str,
        occurred_at_ms: i64,
    ) {
        state
            .ordered_write(|tx| {
                insert_event(
                    tx,
                    event_id,
                    provider,
                    account_ref,
                    occurred_at_ms,
                    "subscription_included",
                    "claude",
                )?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Head {
        charge_key: i64,
        outcome: String,
        reason: Option<String>,
        charge_nanos: Option<i64>,
        currency: Option<String>,
        /// Every charge row this event has, superseded ones included. A
        /// re-resolve that landed on the outcome already recorded would still
        /// leave a row behind, so this is what says nothing was re-resolved.
        rows: i64,
    }

    /// The current head of an event's single measurement.
    fn head(state: &SharedState, event_id: &str) -> Head {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT head.charge_key,head.outcome,head.unpriced_reason,head.charge_nanos,
                            head.currency,
                            (SELECT COUNT(*) FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1))
                     FROM priced_charge AS head
                     WHERE head.event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)
                       AND NOT EXISTS (
                           SELECT 1 FROM priced_charge AS child
                           WHERE child.supersedes=head.charge_key
                       )",
                    [event_id],
                    |row| {
                        Ok(Head {
                            charge_key: row.get(0)?,
                            outcome: row.get(1)?,
                            reason: row.get(2)?,
                            charge_nanos: row.get(3)?,
                            currency: row.get(4)?,
                            rows: row.get(5)?,
                        })
                    },
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn covered_heads(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM priced_charge AS head
                     WHERE head.outcome='covered'
                       AND NOT EXISTS (
                           SELECT 1 FROM priced_charge AS child
                           WHERE child.supersedes=head.charge_key
                       )",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn queued_event_ids(state: &SharedState) -> Vec<String> {
        state
            .with_conn(|conn| {
                conn.prepare("SELECT event_id FROM pricing_pending JOIN usage_event USING(event_key) ORDER BY event_id")
                    .and_then(|mut statement| {
                        statement
                            .query_map([], |row| row.get(0))?
                            .collect::<rusqlite::Result<Vec<String>>>()
                    })
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn backfill_due(state: &SharedState) -> Option<i64> {
        state
            .with_conn(|conn| {
                astrocyte_core::subscription::coverage_backfill_due(conn)
                    .map_err(|error| error.message())
            })
            .unwrap()
    }

    /// An installation that has declared no plan is the state production is in
    /// right now, with 34,378 events carrying an uncovered head. In that state
    /// the pass must select exactly what it selected before this existed: an
    /// uncovered head is not work until somebody declares the plan covering it.
    #[test]
    fn with_no_plan_declared_the_pass_still_selects_only_measurements_with_no_head() {
        let state = test_state();
        seed_account_event(
            &state,
            "ev-uncovered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        assert_eq!(
            head(&state, "ev-uncovered").reason.as_deref(),
            Some("no_subscription_in_force")
        );

        seed_account_event(
            &state,
            "ev-fresh",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        // The pass selects the queue, plus the backfill while a plan write is
        // undrained. No plan has been written, so the selection is the queue.
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-fresh".to_string()],
            "with no plan declared, an event that already has a head is not work"
        );
        assert_eq!(backfill_due(&state), None);
    }

    /// The operator flow this was written for: declare the plan, and the events
    /// it covers become covered on the next tick, at zero money and in the
    /// plan's currency.
    #[test]
    fn declaring_a_plan_reprices_the_event_it_covers_to_covered_at_zero() {
        let state = test_state();
        seed_account_event(
            &state,
            "ev-covered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let uncovered = head(&state, "ev-covered");
        assert_eq!(
            uncovered.reason.as_deref(),
            Some("no_subscription_in_force")
        );

        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 1);

        let covered = head(&state, "ev-covered");
        assert_eq!(
            (
                covered.outcome.as_str(),
                covered.reason.as_deref(),
                covered.charge_nanos,
                covered.currency.as_deref(),
                covered.rows,
            ),
            ("covered", None, Some(0), Some("USD"), 2)
        );
        assert_ne!(
            covered.charge_key, uncovered.charge_key,
            "the covered answer supersedes the uncovered one"
        );
    }

    /// Ruling 55: coverage is decided at the event's instant, never at now. An
    /// event recorded before the earliest plan an account has is outside every
    /// range, and must keep the head it has -- byte for byte, since a re-resolve
    /// would reach the same reason and still leave a second row behind.
    #[test]
    fn an_event_earlier_than_every_declared_plan_keeps_its_uncovered_head() {
        let state = test_state();
        seed_account_event(
            &state,
            "ev-early",
            1_700_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let before = head(&state, "ev-early");
        assert_eq!(before.reason.as_deref(), Some("no_subscription_in_force"));

        // Declared to start after the event happened.
        declare_plan(&state, 1_800_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a plan that was not in force at the event's instant covers nothing"
        );
        assert_eq!(head(&state, "ev-early"), before);
    }

    /// Coverage is keyed by provider as well as account: a plan is a contract
    /// with one vendor, and spend with a second vendor is not inside it however
    /// the dates line up. The event here is inside the plan's range and names
    /// the account the plan was declared for, so the provider alone decides.
    ///
    /// Selecting it would be worse than a wasted re-resolve. It would resolve
    /// to the reason it already carries -- coverage is looked up by provider,
    /// account and instant together -- so it would never leave the set, and
    /// every tick for the life of the event would append another superseding
    /// charge row. The set drains only because what it selects becomes covered.
    #[test]
    fn a_plan_under_a_different_provider_does_not_cover_this_account_s_event() {
        let state = test_state();
        seed_event_under(
            &state,
            "ev-other-vendor",
            "openai",
            ACCOUNT,
            1_800_000_000_000,
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let before = head(&state, "ev-other-vendor");
        assert_eq!(before.reason.as_deref(), Some("no_subscription_in_force"));

        // Declared for the same account under PROVIDER, with a range that
        // contains the event's instant.
        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a plan with another vendor covers nothing bought from this one"
        );
        assert_eq!(head(&state, "ev-other-vendor"), before);
    }

    /// The other half of the same key, and the same consequence: one account's
    /// plan says nothing about another account's spend, and an event selected
    /// on the provider alone would be rewritten with the reason it already
    /// carries on every tick, forever.
    #[test]
    fn a_plan_for_a_different_account_does_not_cover_this_providers_event() {
        let state = test_state();
        seed_event_under(
            &state,
            "ev-other-account",
            PROVIDER,
            "account-b",
            1_800_000_000_000,
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let before = head(&state, "ev-other-account");
        assert_eq!(before.reason.as_deref(), Some("no_subscription_in_force"));

        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a plan held by another account covers nothing this one spent"
        );
        assert_eq!(head(&state, "ev-other-account"), before);
    }

    /// Ruling 95: repricing preserves a head that already carries an answer.
    /// Both events here name the account the plan was declared for and sit
    /// inside its range, so the join alone does not keep them out -- the
    /// outcome of their heads does. The charge id is the assertion because a
    /// re-resolve landing on the same outcome would pass a weaker one.
    #[test]
    fn a_priced_or_covered_head_is_never_offered_to_the_pass_again() {
        let state = test_state();
        let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        declare_plan(&state, 1_700_000_000_000);
        seed_account_event(
            &state,
            "ev-metered",
            1_800_000_000_000,
            "metered_api",
            "claude",
        );
        seed_account_event(
            &state,
            "ev-covered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 2);
        let priced = head(&state, "ev-metered");
        let covered = head(&state, "ev-covered");
        assert_eq!(
            (priced.outcome.as_str(), covered.outcome.as_str()),
            ("priced", "covered")
        );

        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a pass with nothing to do reports nothing, rather than refusing an \
             event it should never have selected"
        );
        assert_eq!(head(&state, "ev-metered"), priced);
        assert_eq!(head(&state, "ev-covered"), covered);
    }

    /// A backlog of tens of thousands must not be repriced inside one tick, and
    /// must still drain: each pass takes a bounded slice and what it covers
    /// leaves the set, so the next pass takes the next slice.
    #[test]
    fn one_pass_takes_at_most_the_cap_and_the_next_pass_takes_the_rest() {
        let state = test_state();
        let surplus = 2;
        let total = COVERAGE_BACKFILL_PER_PASS + surplus;
        state
            .ordered_write(|tx| {
                for ordinal in 0..total {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:05}"),
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();

        // No plan yet, so all of these are priced through the no-head selection
        // the pass already had. That selection is uncapped and stays uncapped:
        // newly recorded facts keep their priority over the backfill.
        assert_eq!(
            price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(),
            total
        );
        assert_eq!(covered_heads(&state), 0);

        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            COVERAGE_BACKFILL_PER_PASS
        );
        assert_eq!(covered_heads(&state), COVERAGE_BACKFILL_PER_PASS as i64);

        assert_eq!(
            price_unpriced_events(&state, 3, &NEVER_STOP).unwrap(),
            surplus
        );
        assert_eq!(covered_heads(&state), total as i64);
        assert_eq!(price_unpriced_events(&state, 4, &NEVER_STOP).unwrap(), 0);
    }

    /// A catalog that answers discovery but refuses the next `refusals`
    /// pricing lookups, as a peer that goes away after the module connected.
    struct RefusesPricing {
        refusals: AtomicUsize,
        inner: LateCatalog,
    }

    struct BreaksLaterPricingRead {
        state: Arc<SharedState>,
        queue: bool,
    }

    impl CatalogConnection for BreaksLaterPricingRead {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            if request.at_ms == Some(0) {
                return LateCatalog::warming(0).catalog_get(request);
            }
            self.state
                .with_conn(|conn| {
                    conn.execute_batch(if self.queue {
                        "DROP TABLE pricing_pending"
                    } else {
                        "DROP TABLE pricing_selection_state"
                    })
                    .map_err(|error| error.to_string())
                })
                .unwrap();
            Err(CatalogCallError {
                code: "unavailable".into(),
                message: "injected event failure".into(),
            })
        }
    }

    fn assert_later_pricing_read_retains_failures(queue: bool) {
        let state = Arc::new(test_state());
        seed_unpriced_event(&state);
        state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(BreaksLaterPricingRead {
                    state: Arc::clone(&state),
                    queue,
                }))
                .unwrap(),
            ))
            .unwrap();
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 1,
            tick_events: 10,
            tick_time: Duration::from_secs(30),
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });
        let error = run_pricing_walk(&state, 1, &NEVER_STOP, &mut walk).unwrap_err();
        state.install_catalog(None).unwrap();
        assert_eq!(
            priced_charge_rows(&state),
            0,
            "failed event must stay headless"
        );
        assert!(
            error.contains("ev-late") && error.contains("injected event failure"),
            "lost event failure: {error}"
        );
        assert!(
            error.contains("no such table"),
            "lost later stage failure: {error}"
        );
    }

    #[test]
    fn later_queue_read_failure_keeps_prior_event_failures() {
        assert_later_pricing_read_retains_failures(true);
    }

    #[test]
    fn later_backfill_read_failure_keeps_prior_event_failures() {
        assert_later_pricing_read_retains_failures(false);
    }

    /// Takes one from `count` if it is above zero, and says whether it did.
    /// Written as a compare-exchange loop because `fetch_update` is deprecated
    /// from Rust 1.99 while its replacement `try_update` doesn't exist on 1.98,
    /// and this has to build on both.
    fn take_one(count: &AtomicUsize) -> bool {
        let mut current = count.load(Ordering::SeqCst);
        while let Some(next) = current.checked_sub(1) {
            match count.compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    impl CatalogConnection for RefusesPricing {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            let pricing_lookup = request.at_ms != Some(0);
            if pricing_lookup && take_one(&self.refusals) {
                return Err(CatalogCallError {
                    code: "unavailable".into(),
                    message: "fusiform is restarting".into(),
                });
            }
            self.inner.catalog_get(request)
        }
    }

    /// Deferred pricing writes no head, so the event must stay queued and be
    /// priced by a later pass. Two deferrals are exercised: the tick that
    /// skips pricing while the catalog is still pending, and a pass whose
    /// catalog lookup is refused. A queue that dropped an event when it was
    /// selected, rather than when it was headed, would lose it at the second.
    #[test]
    fn a_deferred_event_stays_queued_and_is_priced_on_a_later_pass() {
        let state = test_state();
        seed_unpriced_event(&state);
        let stop = AtomicBool::new(false);
        assert_eq!(queued_event_ids(&state), vec!["ev-late".to_string()]);

        run_iteration_full(&state, &[], None, true, &stop);
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-late".to_string()],
            "a tick that skips pricing leaves the event queued"
        );

        let catalog = CatalogClient::connect(Arc::new(RefusesPricing {
            refusals: AtomicUsize::new(1),
            inner: LateCatalog::warming(0),
        }))
        .unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        assert!(price_unpriced_events(&state, 1, &NEVER_STOP).is_err());
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-late".to_string()],
            "a refused catalog lookup writes no head, so the event stays queued"
        );

        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 1);
        assert!(priced_charge_rows(&state) > 0);
        assert!(queued_event_ids(&state).is_empty());
    }

    /// The reconciliation re-runs the old whole-ledger selection and queues
    /// what it finds unqueued, reporting how many. An event written without
    /// its queue row -- what a write path missing its enqueue leaves -- must be
    /// found; a queued event and a fully headed one must not be counted. The
    /// chunk size is shrunk so the missed events sit in different chunks and
    /// the walk across chunk boundaries is exercised, including the last,
    /// open-ended one.
    #[test]
    fn reconciliation_enqueues_and_counts_events_that_were_never_queued() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                insert_account_event(tx, "ev-a-headed", 1_800_000_000_000, "unknown", "claude")?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        state
            .ordered_write(|tx| {
                for event_id in ["ev-b-missed", "ev-d-missed", "ev-f-missed"] {
                    insert_event_unqueued(
                        tx,
                        event_id,
                        PROVIDER,
                        ACCOUNT,
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                for event_id in ["ev-c-queued", "ev-e-queued"] {
                    insert_account_event(tx, event_id, 1_800_000_000_000, "unknown", "claude")?;
                }
                Ok(((), true))
            })
            .unwrap();
        let never_stop = AtomicBool::new(false);

        assert_eq!(
            reconcile_pricing_queue_in_chunks(&state, &never_stop, NonZeroUsize::new(2).unwrap())
                .unwrap(),
            ReconcileOutcome::Completed { enqueued: 3 }
        );
        assert_eq!(
            queued_event_ids(&state),
            vec![
                "ev-b-missed",
                "ev-c-queued",
                "ev-d-missed",
                "ev-e-queued",
                "ev-f-missed"
            ]
        );
        assert_eq!(
            reconcile_pricing_queue(&state, &never_stop).unwrap(),
            ReconcileOutcome::Completed { enqueued: 0 },
            "a queue that agrees with the ledger reports nothing"
        );
        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 5);
        assert!(queued_event_ids(&state).is_empty());
    }

    /// A stop set before the reconciliation starts ends it before its first
    /// chunk, enqueuing nothing.
    #[test]
    fn a_stopped_reconciliation_enqueues_nothing_further() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                insert_event_unqueued(
                    tx,
                    "ev-missed",
                    PROVIDER,
                    ACCOUNT,
                    1_800_000_000_000,
                    "unknown",
                    "claude",
                )?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(
            reconcile_pricing_queue(&state, &AtomicBool::new(true)).unwrap(),
            ReconcileOutcome::Stopped { enqueued: 0 }
        );
        assert!(queued_event_ids(&state).is_empty());
    }

    /// A producer that is always caught up and counts how often it is pulled,
    /// so a test can see whether a tick drained.
    struct CountingSource {
        pulls: Arc<AtomicUsize>,
    }

    impl SourceClient for CountingSource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            let cursor = SourceCursor {
                lineage: "counting:1".into(),
                ordinal: 1,
            };
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: cursor.clone(),
                high_water: cursor,
                events: Vec::new(),
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    /// The queue check must not hold up ingest: with a budget that fits one
    /// chunk, a tick returns with the pass unfinished, and every tick of the
    /// pass drains the producer. The event recorded without its queue row is
    /// last in id order, so it is found only by the pass's final chunks,
    /// several ticks in.
    #[test]
    fn queue_check_spreads_over_ticks_and_every_tick_drains() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for index in 0..9 {
                    insert_account_event(
                        tx,
                        &format!("ev-{index:02}-queued"),
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                insert_event_unqueued(
                    tx,
                    "ev-99-missed",
                    PROVIDER,
                    ACCOUNT,
                    1_800_000_000_000,
                    "unknown",
                    "claude",
                )?;
                Ok(((), true))
            })
            .unwrap();
        let pulls = Arc::new(AtomicUsize::new(0));
        let producers = [Producer {
            // The test store seeds a runtime row only for this key.
            source_key: "broca".into(),
            client: Arc::new(CountingSource {
                pulls: Arc::clone(&pulls),
            }),
        }];
        // A zero budget fits exactly the one chunk every tick runs.
        let mut reconciler = QueueReconciler::new(NonZeroUsize::new(2).unwrap(), Duration::ZERO);
        let file_sources = FileSources::default();
        let is_queued =
            |state: &SharedState| queued_event_ids(state).contains(&"ev-99-missed".to_string());

        // Checked after every tick, because the tick after the one that
        // enqueues the event prices it and takes it off the queue.
        let mut queued_at_tick = None;
        let mut ticks = 0;
        loop {
            let pulls_before = pulls.load(Ordering::SeqCst);
            run_tick(
                &state,
                &producers,
                &file_sources,
                None,
                &mut None,
                &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
                &mut reconciler,
                &NEVER_STOP,
            );
            ticks += 1;
            assert!(
                pulls.load(Ordering::SeqCst) > pulls_before,
                "tick {ticks} did not drain the producer"
            );
            if ticks == 1 {
                assert!(
                    reconciler.in_progress(),
                    "the first tick must leave the pass unfinished"
                );
                assert!(
                    !is_queued(&state),
                    "the first chunk cannot reach the last event"
                );
            }
            if queued_at_tick.is_none() && is_queued(&state) {
                queued_at_tick = Some(ticks);
            }
            if !reconciler.in_progress() {
                break;
            }
            assert!(ticks < 50, "the pass never finished");
        }
        // Ten events in chunks of two: five bounded chunks and the empty
        // open-ended one, one per tick.
        assert_eq!(ticks, 6);
        // Its chunk, (ev-08..ev-99], is the fifth.
        assert_eq!(
            queued_at_tick,
            Some(5),
            "the unqueued event reaches pricing_pending on the tick whose chunk covers it"
        );
    }

    /// A catalog that runs `action` once, during the lookup for the event that
    /// occurred at `trigger_at_ms`. The pass resolves a whole group before it
    /// commits any of it, so the action lands after the events ahead of that
    /// one were resolved and before the group commits: the window in which an
    /// operator can move an event under a plan the pass already holds.
    struct ActsDuringLookup {
        inner: LateCatalog,
        trigger_at_ms: i64,
        action: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl ActsDuringLookup {
        fn new(trigger_at_ms: i64, action: impl FnOnce() + Send + 'static) -> Self {
            Self {
                inner: LateCatalog::warming(0),
                trigger_at_ms,
                action: std::sync::Mutex::new(Some(Box::new(action))),
            }
        }
    }

    impl CatalogConnection for ActsDuringLookup {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            if request.at_ms == Some(self.trigger_at_ms) {
                // Taken out before it runs, so an action that itself calls
                // the catalog does not wait on this lock.
                let action = self.action.lock().unwrap().take();
                if let Some(action) = action {
                    action();
                }
            }
            self.inner.catalog_get(request)
        }
    }

    fn charge_rows_by_origin(state: &SharedState, event_id: &str) -> Vec<(String, i64)> {
        state
            .with_conn(|conn| {
                conn.prepare(
                    "SELECT origin,pricing_generation FROM priced_charge
                     WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY pricing_generation",
                )
                .and_then(|mut statement| {
                    statement
                        .query_map([event_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// Five events are priced in one group. While the last one is being
    /// resolved, an operator declares the plan covering `ev-b` and reprices
    /// `ev-d` by hand, so the plans the pass holds for those two are stale:
    /// `ev-b`'s assumed no coverage, `ev-d`'s names a pricing generation that
    /// has moved on. Both are refused at commit, and the refusal takes neither
    /// the events before them nor the ones after them in the group with it.
    #[test]
    fn events_that_moved_before_their_group_commits_are_refused_and_the_rest_commit() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        seed_account_event(&state, "ev-a", instant, "metered_api", "claude");
        seed_account_event(
            &state,
            "ev-b",
            instant + 1_000,
            "subscription_included",
            "claude",
        );
        seed_account_event(&state, "ev-c", instant + 2_000, "metered_api", "claude");
        seed_account_event(&state, "ev-d", instant + 3_000, "metered_api", "claude");
        seed_account_event(&state, "ev-e", instant + 4_000, "metered_api", "claude");
        let operator = state.clone();
        let peer = ActsDuringLookup::new(instant + 4_000, move || {
            declare_plan(&operator, 1_700_000_000_000);
            crate::ops::dispatch(
                &operator,
                crate::ops::OP_EVENT_REPRICE,
                serde_json::json!({"event_id": "ev-d"}),
            )
            .unwrap();
        });
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();

        let error = price_unpriced_events(&state, 1, &NEVER_STOP).unwrap_err();
        assert!(error.contains("event ev-b:"), "{error}");
        assert!(error.contains("event ev-d:"), "{error}");
        for committed in ["ev-a", "ev-c", "ev-e"] {
            assert!(!error.contains(committed), "{error}");
            assert_eq!(
                charge_rows_by_origin(&state, committed),
                vec![("pricing_pass".to_string(), 1)],
                "{committed} commits in the same group as the refused events"
            );
        }
        assert!(
            charge_rows_by_origin(&state, "ev-b").is_empty(),
            "the event whose coverage moved is untouched"
        );
        assert_eq!(
            charge_rows_by_origin(&state, "ev-d"),
            vec![("operator_reprice".to_string(), 1)],
            "only the operator's reprice of ev-d is recorded; the pass's stale one was refused"
        );
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-b".to_string()],
            "the refused event stays queued"
        );

        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 1);
        assert_eq!(head(&state, "ev-b").outcome, "covered");
        assert!(queued_event_ids(&state).is_empty());
    }

    /// A stop seen while a group is being resolved drops the whole group:
    /// the events resolved before the stop are not committed either.
    #[test]
    fn a_stop_during_a_group_commits_nothing_from_it() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        for (ordinal, event_id) in ["ev-a", "ev-b", "ev-c"].into_iter().enumerate() {
            seed_account_event(
                &state,
                event_id,
                instant + 1_000 * ordinal as i64,
                "metered_api",
                "claude",
            );
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let peer = ActsDuringLookup::new(instant + 1_000, move || {
            stopping.store(true, Ordering::SeqCst);
        });
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();

        assert_eq!(price_unpriced_events(&state, 1, &stop).unwrap(), 0);
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(queued_event_ids(&state), vec!["ev-a", "ev-b", "ev-c"]);
    }

    #[test]
    fn pricing_time_budget_commits_the_resolved_prefix_and_resumes_remaining_events() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        for (ordinal, event_id) in ["ev-a", "ev-b", "ev-c"].into_iter().enumerate() {
            seed_account_event(
                &state,
                event_id,
                instant + ordinal as i64,
                "metered_api",
                "claude",
            );
        }
        let peer =
            ActsDuringLookup::new(instant, || std::thread::sleep(Duration::from_millis(300)));
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 100,
            tick_events: usize::MAX,
            tick_time: Duration::from_millis(200),
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });

        let pass = run_pricing_walk(&state, 1, &NEVER_STOP, &mut walk).unwrap();
        assert_eq!(
            pass.priced, 1,
            "no catalog call may start after the budget expires"
        );
        assert!(pass.paused);
        assert_eq!(queued_event_ids(&state), vec!["ev-b", "ev-c"]);
        assert_eq!(walk.after.as_deref(), Some("ev-a"));
        assert_eq!(
            charge_rows_by_origin(&state, "ev-a"),
            vec![("pricing_pass".to_string(), 1)]
        );

        walk.budget.tick_time = Duration::MAX;
        assert_eq!(
            run_pricing_walk(&state, 2, &NEVER_STOP, &mut walk)
                .unwrap()
                .priced,
            2
        );
        assert!(queued_event_ids(&state).is_empty());
        for event_id in ["ev-a", "ev-b", "ev-c"] {
            assert_eq!(
                charge_rows_by_origin(&state, event_id),
                vec![("pricing_pass".to_string(), 1)]
            );
        }
    }

    #[test]
    fn pricing_time_budget_keeps_backfill_due_after_queue_uses_budget() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        for (ordinal, event_id) in ["ev-a", "ev-b", "ev-c"].into_iter().enumerate() {
            seed_account_event(
                &state,
                event_id,
                instant + ordinal as i64,
                "subscription_included",
                "claude",
            );
        }
        state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
            ))
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 3);
        declare_plan(&state, 1_700_000_000_000);
        seed_account_event(&state, "ev-0", instant - 1, "metered_api", "claude");
        let peer = ActsDuringLookup::new(instant - 1, || {
            std::thread::sleep(Duration::from_millis(300))
        });
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 100,
            tick_events: usize::MAX,
            tick_time: Duration::from_millis(200),
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });
        let pass = run_pricing_walk(&state, 2, &NEVER_STOP, &mut walk).unwrap();
        assert_eq!(pass.priced, 1);
        assert!(!pass.backfill_queried);
        assert_eq!(charge_rows_by_origin(&state, "ev-a").len(), 1);
        assert_eq!(charge_rows_by_origin(&state, "ev-b").len(), 1);
        assert!(state
            .with_conn(
                |conn| astrocyte_core::subscription::coverage_backfill_due(conn)
                    .map_err(|error| error.message().to_string())
            )
            .unwrap()
            .is_some());
        walk.budget.tick_time = Duration::MAX;
        assert_eq!(
            run_pricing_walk(&state, 3, &NEVER_STOP, &mut walk)
                .unwrap()
                .priced,
            3
        );
        assert!(state
            .with_conn(
                |conn| astrocyte_core::subscription::coverage_backfill_due(conn)
                    .map_err(|error| error.message().to_string())
            )
            .unwrap()
            .is_none());
    }

    /// What a SQLite write-ahead log holds: every frame, and the commit
    /// frames among them, one per committed write transaction and so one per
    /// fsync of the log under `synchronous=FULL`. Read from the file itself
    /// rather than counted by the code under test. Frames are counted while
    /// they carry the header's salt, which is where a log that was restarted
    /// ends.
    #[derive(Debug, Clone, Copy, Default)]
    struct WalFrames {
        frames: usize,
        commits: usize,
        /// Bytes those frames take in the log, frame headers included.
        bytes: usize,
    }

    fn wal_frames(path: &std::path::Path) -> WalFrames {
        let bytes = std::fs::read(path).unwrap_or_default();
        if bytes.len() < 32 {
            return WalFrames::default();
        }
        let word = |at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
        let page_size = word(8) as usize;
        let salt = &bytes[16..24];
        let mut counted = WalFrames::default();
        let mut frame = 32;
        while frame + 24 + page_size <= bytes.len() && &bytes[frame + 8..frame + 16] == salt {
            counted.frames += 1;
            counted.bytes += 24 + page_size;
            // A frame's database size after commit is non-zero only on the
            // last frame of a transaction.
            if word(frame + 4) != 0 {
                counted.commits += 1;
            }
            frame += 24 + page_size;
        }
        counted
    }

    fn wal_commit_frames(path: &std::path::Path) -> usize {
        wal_frames(path).commits
    }

    /// Prices whatever is queued in a COPY of the live store
    /// (`PRICING_REHEARSAL_DB`) in commit groups of `PRICING_REHEARSAL_GROUP`
    /// events (default `PRICING_COMMIT_GROUP`; 1 is the old pass's one
    /// transaction per event), through the real fusiform client when
    /// `PRICING_REHEARSAL_SUBC` names a daemon connection file and with no
    /// catalog otherwise. Prints the events priced, the write-ahead-log
    /// commits and bytes, and the wall time. Checkpoints are turned off on the
    /// copy so the log keeps every frame the pass wrote. Ignored by default:
    /// it needs the copy, and it writes to it.
    #[test]
    #[ignore]
    fn pricing_rehearsal_on_a_store_copy() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let database = std::env::var("PRICING_REHEARSAL_DB").expect("PRICING_REHEARSAL_DB");
        let group = std::env::var("PRICING_REHEARSAL_GROUP")
            .map(|group| group.parse::<usize>().expect("PRICING_REHEARSAL_GROUP"))
            .unwrap_or(PRICING_COMMIT_GROUP);
        let store = open_sqlite(&StorageDescriptor {
            module_id: "ck-astrocyte-pricing-rehearsal".to_string(),
            storage_namespace: "astrocyte".to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: database.clone(),
            },
        })
        .unwrap();
        astrocyte_core::store::migrate_store(&store).unwrap();
        let state = SharedState::from_store(store).unwrap();
        let synchronous: i64 = state
            .with_conn(|conn| {
                conn.pragma_update(None, "wal_autocheckpoint", 0)
                    .and_then(|()| conn.pragma_query_value(None, "synchronous", |row| row.get(0)))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        if let Ok(subc) = std::env::var("PRICING_REHEARSAL_SUBC") {
            let client = Arc::new(crate::fusiform_client::FusiformClient::new(subc).unwrap());
            let connection: Arc<dyn CatalogConnection> = client;
            state
                .install_catalog(Some(CatalogClient::connect(connection).unwrap()))
                .unwrap();
        }
        let queued = queued_event_ids(&state).len();
        let wal = std::path::PathBuf::from(format!("{database}-wal"));
        let before = wal_frames(&wal);
        let started = std::time::Instant::now();
        let outcome = run_pricing_walk(
            &state,
            current_time_ms(),
            &NEVER_STOP,
            &mut PricingWalk::new(PricingBudget {
                group_events: group,
                tick_events: usize::MAX,
                tick_time: Duration::MAX,
                backfill_events: COVERAGE_BACKFILL_PER_PASS,
            }),
        );
        let elapsed = started.elapsed();
        let after = wal_frames(&wal);
        let (priced, failed) = match &outcome {
            Ok(pass) => (pass.priced, 0),
            Err(error) => (
                queued - queued_event_ids(&state).len(),
                error.matches("event ").count(),
            ),
        };
        println!(
            "group {group} synchronous {synchronous} queued {queued} priced {priced} failed {failed} \
             commits {} wal_frames {} wal_bytes {} wall_ms {}",
            after.commits - before.commits,
            after.frames - before.frames,
            after.bytes - before.bytes,
            elapsed.as_millis()
        );
    }

    /// Pricing N queued events commits ceil(N / group) transactions, counted
    /// as commit frames in the store's write-ahead log.
    #[test]
    fn pricing_commits_one_transaction_per_group() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let wal = directory.path().join("ledger.db-wal");
        // No checkpoint may restart the log while it is being counted.
        state
            .with_conn(|conn| {
                conn.pragma_update(None, "wal_autocheckpoint", 0)
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        // The first pass on a store also marks the coverage backfill drained,
        // which is one more commit; take it out of the count.
        run_pricing_pass(&state, 1, &NEVER_STOP).unwrap();
        let events = 2 * PRICING_COMMIT_GROUP + 1;
        state
            .ordered_write(|tx| {
                for ordinal in 0..events {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:05}"),
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();

        let before = wal_commit_frames(&wal);
        let pass = run_pricing_pass(&state, 2, &NEVER_STOP).unwrap();
        let commits = wal_commit_frames(&wal) - before;

        assert_eq!(pass.priced, events);
        assert!(queued_event_ids(&state).is_empty());
        assert_eq!(commits, events.div_ceil(PRICING_COMMIT_GROUP));
    }

    /// Seed `events` queued events, then run refresher ticks with `budget`
    /// until the queue is empty. Returns how many events were still queued
    /// after each tick, after checking that every tick drained the producer
    /// and that every event ended up priced exactly once.
    fn queue_left_after_each_tick(events: usize, budget: PricingBudget) -> Vec<usize> {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..events {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:02}"),
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        let pulls = Arc::new(AtomicUsize::new(0));
        let producers = [Producer {
            // The test store seeds a runtime row only for this key.
            source_key: "broca".into(),
            client: Arc::new(CountingSource {
                pulls: Arc::clone(&pulls),
            }),
        }];
        let mut pricing = PricingWalk::new(budget);
        let mut reconciler = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        let file_sources = FileSources::default();
        let mut left = Vec::new();
        while left.last() != Some(&0) {
            assert!(left.len() < 20, "the backlog never drained: {left:?}");
            let pulls_before = pulls.load(Ordering::SeqCst);
            run_tick(
                &state,
                &producers,
                &file_sources,
                None,
                &mut None,
                &mut pricing,
                &mut reconciler,
                &NEVER_STOP,
            );
            assert!(
                pulls.load(Ordering::SeqCst) > pulls_before,
                "tick {} did not drain the producer",
                left.len() + 1
            );
            left.push(queued_event_ids(&state).len());
        }
        for ordinal in 0..events {
            let event_id = format!("ev-{ordinal:02}");
            assert_eq!(
                charge_rows_by_origin(&state, &event_id),
                vec![("pricing_pass".to_string(), 1)],
                "{event_id} is priced exactly once"
            );
        }
        left
    }

    /// A queue larger than the event cap is spread over ticks, three events a
    /// tick in groups of two, while every tick still polls the fake BROCA source.
    #[test]
    fn a_backlog_over_the_event_cap_is_priced_across_ticks_and_every_tick_drains() {
        let capped = PricingBudget {
            group_events: 2,
            tick_events: 3,
            tick_time: Duration::MAX,
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        };
        assert_eq!(queue_left_after_each_tick(10, capped), vec![7, 4, 1, 0]);
    }

    #[test]
    fn a_zero_pricing_time_budget_leaves_every_event_queued() {
        let state = test_state();
        seed_account_event(&state, "ev-a", 1_800_000_000_000, "metered_api", "claude");
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 2,
            tick_events: usize::MAX,
            tick_time: Duration::ZERO,
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });
        let pass = run_pricing_walk(&state, 1, &NEVER_STOP, &mut walk).unwrap();
        assert_eq!(pass.priced, 0);
        assert!(pass.paused);
        assert_eq!(queued_event_ids(&state), vec!["ev-a"]);
        assert!(charge_rows_by_origin(&state, "ev-a").is_empty());
    }

    /// A pass paused and resumed at every chunk boundary examines each event
    /// exactly once: none skipped where a chunk ends in one tick and the next
    /// starts in another, none examined twice. The missed events sit on both
    /// sides of every boundary, and the last chunk is the open-ended one.
    #[test]
    fn resumed_queue_check_examines_each_event_once_per_pass() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                insert_account_event(tx, "ev-01-headed", 1_800_000_000_000, "unknown", "claude")?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        state
            .ordered_write(|tx| {
                for event_id in [
                    "ev-03-missed",
                    "ev-04-missed",
                    "ev-06-missed",
                    "ev-07-missed",
                ] {
                    insert_event_unqueued(
                        tx,
                        event_id,
                        PROVIDER,
                        ACCOUNT,
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                for event_id in ["ev-02-queued", "ev-05-queued", "ev-08-queued"] {
                    insert_account_event(tx, event_id, 1_800_000_000_000, "unknown", "claude")?;
                }
                Ok(((), true))
            })
            .unwrap();

        // Chunks of three over eight events: (..03], (03..06], (06..], one
        // per call, as three ticks would run them.
        let mut walk = ReconcileWalk::default();
        let mut progress = Vec::new();
        for _ in 0..10 {
            let step = walk
                .advance(
                    &state,
                    &NEVER_STOP,
                    NonZeroUsize::new(3).unwrap(),
                    Duration::ZERO,
                )
                .unwrap();
            progress.push((step, walk.after.clone()));
            if step == WalkProgress::Completed {
                break;
            }
        }
        assert_eq!(
            progress,
            vec![
                (WalkProgress::Paused, Some("ev-03-missed".to_string())),
                (WalkProgress::Paused, Some("ev-06-missed".to_string())),
                (WalkProgress::Completed, Some("ev-06-missed".to_string())),
            ]
        );
        assert_eq!(walk.examined, 8, "every event examined once");
        assert_eq!(walk.enqueued, 4, "every missed event found once");
        assert_eq!(walk.ticks, 3);
        assert_eq!(
            queued_event_ids(&state),
            vec![
                "ev-02-queued",
                "ev-03-missed",
                "ev-04-missed",
                "ev-05-queued",
                "ev-06-missed",
                "ev-07-missed",
                "ev-08-queued"
            ]
        );
    }

    /// The coverage backfill query runs only while a plan write is undrained.
    /// Asserted on whether the query ran, because a drained backfill returns
    /// nothing either way and its result cannot say whether it was skipped.
    #[test]
    fn coverage_backfill_runs_after_a_plan_write_and_not_on_a_quiet_tick() {
        let state = test_state();
        assert!(
            run_pricing_pass(&state, 1, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "the first pass after the upgrade drains whatever plan writes preceded it"
        );
        assert!(
            !run_pricing_pass(&state, 2, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "no plan changed since the backfill drained, so the query does not run"
        );

        declare_plan(&state, 1_700_000_000_000);
        assert!(
            run_pricing_pass(&state, 3, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
        assert!(
            !run_pricing_pass(&state, 4, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );

        state
            .ordered_write(|tx| {
                astrocyte_core::subscription::close_subscription_in_tx(
                    tx,
                    &astrocyte_core::subscription::CloseSubscriptionRequest {
                        account_ref: ACCOUNT.into(),
                        provider: PROVIDER.into(),
                        effective_to: 1_750_000_000_000,
                    },
                )
                .map_err(|error| error.message())?;
                Ok(((), true))
            })
            .unwrap();
        assert!(
            run_pricing_pass(&state, 5, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "closing a plan moves the generation too"
        );
        assert!(
            !run_pricing_pass(&state, 6, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
    }

    /// A stopped pass has not repriced what the backfill would find, so it
    /// must leave the plan write undrained for the next pass.
    #[test]
    fn a_stopped_pass_leaves_the_coverage_backfill_due() {
        let state = test_state();
        declare_plan(&state, 1_700_000_000_000);
        let due = backfill_due(&state);
        assert!(due.is_some());
        let stopped = run_pricing_pass(&state, 1, &AtomicBool::new(true)).unwrap();
        assert!(!stopped.backfill_queried);
        assert_eq!(backfill_due(&state), due);
        assert!(
            run_pricing_pass(&state, 2, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
        assert_eq!(backfill_due(&state), None);
    }

    /// A backfill batch that came back full has not proved the backlog is
    /// empty, so the generation stays undrained until a short batch.
    #[test]
    fn a_full_backfill_batch_does_not_mark_the_generation_drained() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..COVERAGE_BACKFILL_PER_PASS {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:05}"),
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        run_pricing_pass(&state, 1, &NEVER_STOP).unwrap();
        declare_plan(&state, 1_700_000_000_000);
        let full = run_pricing_pass(&state, 2, &NEVER_STOP).unwrap();
        assert!(full.backfill_queried);
        assert_eq!(full.priced, COVERAGE_BACKFILL_PER_PASS);
        assert!(
            run_pricing_pass(&state, 3, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "a full batch may have left work, so the next pass looks again"
        );
        assert!(
            !run_pricing_pass(&state, 4, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
    }

    /// An event that keeps failing to reprice must not hold the backfill on
    /// itself. The batch here is one event, the shape in which a set that
    /// always started from its beginning would return the failing event on
    /// every tick and never reach the events behind it. The failure is a
    /// store trigger refusing that one event's charge row, which stays in
    /// force until the test drops it, so it is a refusal on every attempt and
    /// not a transient one a retry would clear by itself.
    #[test]
    fn a_failing_backfill_event_is_retried_without_starving_the_rest() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..4 {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal}"),
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 4);
        declare_plan(&state, 1_700_000_000_000);
        state
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER refuse_ev_0 BEFORE INSERT ON priced_charge
                     WHEN NEW.event_key=(SELECT event_key FROM usage_event WHERE event_id='ev-0')
                     BEGIN SELECT RAISE(ABORT, 'injected refusal'); END;",
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        let before = head(&state, "ev-0");
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 1,
            tick_events: usize::MAX,
            tick_time: Duration::MAX,
            backfill_events: 1,
        });
        let mut tick = |at: i64| run_pricing_walk(&state, at, &NEVER_STOP, &mut walk);

        let refused = tick(2).unwrap_err();
        assert!(refused.contains("ev-0"), "{refused}");
        for at in 3..=5 {
            assert_eq!(
                tick(at).unwrap().priced,
                1,
                "tick {at} reprices the next event"
            );
        }
        for event_id in ["ev-1", "ev-2", "ev-3"] {
            assert_eq!(
                head(&state, event_id).outcome,
                "covered",
                "{event_id} is reached past the failing event"
            );
        }
        // This batch reaches the end of the set, so the sweep ends here; the
        // refused event is still in the set, so the plan write is not drained.
        assert_eq!(tick(6).unwrap().priced, 0);
        assert!(backfill_due(&state).is_some());
        let retried = tick(7).unwrap_err();
        assert!(
            retried.contains("ev-0"),
            "the next sweep retries it: {retried}"
        );
        assert_eq!(
            head(&state, "ev-0"),
            before,
            "a refused event keeps its head, unsuperseded"
        );
        assert_eq!(
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COUNT(*) FROM usage_event WHERE event_id='ev-0'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap(),
            1,
            "a refused event is never deleted"
        );

        state
            .with_conn(|conn| {
                conn.execute_batch("DROP TRIGGER refuse_ev_0")
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        // The sweep that retried it saw it fail, so its end does not drain.
        assert_eq!(tick(8).unwrap().priced, 0);
        assert!(backfill_due(&state).is_some());
        assert_eq!(tick(9).unwrap().priced, 1);
        assert_eq!(head(&state, "ev-0").outcome, "covered");
        assert!(
            backfill_due(&state).is_some(),
            "a full batch is not the end"
        );
        assert_eq!(tick(10).unwrap().priced, 0);
        assert_eq!(
            backfill_due(&state),
            None,
            "a sweep with no failure drains the plan write"
        );
    }

    /// Finding pricing work costs what is queued, not what the ledger holds.
    /// With nothing queued, the queue read over a ledger of 400 events takes
    /// exactly as many SQLite steps as over a ledger of 20. SQLite's own
    /// per-statement step counter is used rather than a timing, so the
    /// comparison is exact and cannot flake. A selection that scanned
    /// measurements, as the pass once did, would take steps in proportion to
    /// the ledger.
    #[test]
    fn with_nothing_queued_selection_cost_does_not_grow_with_the_ledger() {
        fn steps_over_ledger(events: usize) -> i64 {
            let state = test_state();
            state
                .ordered_write(|tx| {
                    for ordinal in 0..events {
                        insert_account_event(
                            tx,
                            &format!("ev-{ordinal:05}"),
                            1_800_000_000_000,
                            "unknown",
                            "claude",
                        )?;
                    }
                    Ok(((), true))
                })
                .unwrap();
            assert_eq!(
                run_pricing_pass(&state, 1, &NEVER_STOP).unwrap().priced,
                events
            );
            assert!(queued_event_ids(&state).is_empty());
            let pass = run_pricing_pass(&state, 2, &NEVER_STOP).unwrap();
            assert_eq!(pass.priced, 0);
            assert!(!pass.backfill_queried);
            pass.queue_selection_steps
        }

        let small = steps_over_ledger(20);
        let large = steps_over_ledger(400);
        assert!(small > 0, "the queue read ran and was counted");
        assert_eq!(small, large);
    }

    /// Ruling 86 is why this is affordable at all: coverage is answered from
    /// our own subscription table, so backfilling it must not put a catalog
    /// call in a tick. The decoy is the event left unpriced for a catalog
    /// reason -- it names the same covered account and instant, and only the
    /// reason on its head keeps it out of the backfill. Selecting it would
    /// reach the catalog, and the call count is what says it was not selected.
    #[test]
    fn backfilling_declared_coverage_makes_no_catalog_call() {
        let state = test_state();
        let peer = Arc::new(LateCatalog::warming(0));
        state
            .install_catalog(Some(CatalogClient::connect(peer.clone()).unwrap()))
            .unwrap();
        seed_account_event(
            &state,
            "ev-covered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        // A model this catalog serves no rate for: its head is unpriced for a
        // catalog reason, and resolving it again calls the catalog again.
        seed_account_event(
            &state,
            "ev-no-rate",
            1_800_000_000_000,
            "metered_api",
            "opus",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 2);
        assert_eq!(
            head(&state, "ev-no-rate").reason.as_deref(),
            Some("no_rate")
        );

        declare_plan(&state, 1_700_000_000_000);
        let calls_before = peer.calls.load(Ordering::SeqCst);
        let repriced = price_unpriced_events(&state, 2, &NEVER_STOP).unwrap();

        // Asserted before the count, so that this test fails on the claim it
        // exists for rather than on a symptom of the same defect.
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            calls_before,
            "a coverage backfill resolves from local tables only"
        );
        assert_eq!(repriced, 1);
        assert_eq!(head(&state, "ev-covered").outcome, "covered");
        assert_eq!(
            head(&state, "ev-no-rate").reason.as_deref(),
            Some("no_rate")
        );
    }

    struct OnePage;

    impl SourceClient for OnePage {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "instance-a:3".into(),
                    ordinal: 7,
                },
                high_water: SourceCursor {
                    lineage: "instance-a:3".into(),
                    ordinal: 8,
                },
                events: Vec::new(),
                pending_known: 1,
                pending_unknown: false,
            })
        }
    }

    #[test]
    fn malformed_hydration_refuses_a_fresh_store() {
        let state = Arc::new(test_state());
        let error =
            reject_startup_bytes_for_test(&state, br#"{"poll_interval_secs":60}"#).unwrap_err();
        assert!(error.contains("no committed generation"));
    }

    /// The production reload path, not the test helper: a store that already
    /// holds a committed generation must survive a malformed file at the next
    /// startup, keep serving that generation, and degrade health with the
    /// decode error. This arm existed and was unreachable -- reload_config
    /// returned Err before it, so a typo in the config file exited the daemon.
    #[test]
    fn malformed_file_retains_the_committed_generation_through_the_production_path() {
        let state = Arc::new(test_state());
        // pid + nanos keeps parallel test binaries and repeated runs from
        // sharing a path; the file is removed at the end.
        let path = std::env::temp_dir().join(format!(
            "astrocyte-reload-{}-{}.jsonc",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            br#"{"poll_interval_secs":45,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":[]}}"#,
        )
        .unwrap();
        reload_config(&state, Ok(path.clone())).unwrap();
        let committed = state.desired_generation();
        assert!(state.has_committed_desired_generation());
        assert_eq!(state.desired_config().poll_interval_secs, 45);

        std::fs::write(&path, br#"{"poll_interval_secs":60}"#).unwrap();
        reload_config(&state, Ok(path.clone()))
            .expect("a committed generation must be retained, not refused");
        assert_eq!(
            state.desired_generation(),
            committed,
            "the malformed file must not publish"
        );
        assert_eq!(
            state.desired_config().poll_interval_secs,
            45,
            "the retained generation must still be the one served"
        );
        let health = state.public_projection().health(0);
        assert_eq!(health.status, "degraded");
        assert!(
            health
                .degraded_reason
                .as_deref()
                .is_some_and(|r| r.contains("member")),
            "health must carry the decode error, got {:?}",
            health.degraded_reason
        );

        // The path-resolution arm routes the same way.
        reload_config(
            &state,
            Err("neither XDG_CONFIG_HOME nor HOME is set".into()),
        )
        .expect("an unresolvable path retains too");
        assert_eq!(state.desired_generation(), committed);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn successful_drain_writes_the_producer_relationship_state() {
        let state = test_state();
        let outcome = drain_once(&state, &OnePage, "broca", 20, 99).unwrap();
        assert_eq!(outcome.consumed_ordinal, Some(7));
        let values: (String, i64, i64, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,high_water_ordinal,last_success_ms
                     FROM source_runtime WHERE source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(values, ("instance-a:3".into(), 7, 8, 99));
    }

    #[test]
    fn failed_source_observation_keeps_the_cold_unavailable_marker() {
        let state = test_state();
        let result = drain_once(&state, &UnavailableSource, "broca", 20, 99);
        assert!(result.is_err());
        let values: (Option<String>, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,pending_unknown FROM source_runtime
                     WHERE source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(values, (None, 1));
    }

    /// A page from the SAME instance sitting behind the stored cursor: the
    /// shape a producer restore or clock rollback produces. The page carries a
    /// real event so the assertion is about non-application, not about an
    /// empty page having nothing to apply.
    struct RegressedPage {
        instance: &'static str,
        epoch: i64,
        seq: i64,
    }

    impl SourceClient for RegressedPage {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            let cursor = SourceCursor {
                lineage: format!("{}:{}", self.instance, self.epoch),
                ordinal: self.seq,
            };
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: cursor.clone(),
                high_water: cursor,
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }

        fn refuse_regression(
            &self,
            stored: Option<&SourceCursor>,
            page: &ExportPage,
        ) -> Result<(), PullError> {
            broca_source::refuse_broca_regression(stored, page)
        }
    }

    fn seed_cursor(state: &SharedState, instance: &str, epoch: i64, seq: i64) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "UPDATE source_runtime SET lineage=?1,
                     consumed_ordinal=?2,high_water_ordinal=?2,last_success_ms=50,
                     pending_unknown=0 WHERE source_key='broca'",
                    params![format!("{instance}:{epoch}"), seq],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
    }

    fn stored_state(state: &SharedState) -> (String, i64, i64, i64) {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,last_success_ms,
                            (SELECT COUNT(*) FROM usage_event)
                     FROM source_runtime WHERE source_key='broca'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    #[test]
    fn same_instance_lower_sequence_is_refused_and_nothing_is_written() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        let result = drain_once(
            &state,
            &RegressedPage {
                instance: "inst",
                epoch: 2,
                seq: 499,
            },
            "broca",
            20,
            99,
        );
        assert!(
            matches!(result, Err(PullError::LineageRegressed { .. })),
            "{result:?}"
        );
        assert_eq!(stored_state(&state), ("inst:2".into(), 500, 50, 0));
    }

    /// A page whose consumed position is past the high water it reports. Only
    /// a custom source can offer this: the BROCA client raises high water to
    /// at least what it consumed. The page carries a real event so the
    /// assertion is about non-application, not about an empty page.
    struct ConsumedPastHighWater {
        high_water_lineage: &'static str,
    }

    impl SourceClient for ConsumedPastHighWater {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "inst:2".into(),
                    ordinal: 507,
                },
                high_water: SourceCursor {
                    lineage: self.high_water_lineage.into(),
                    ordinal: 506,
                },
                events: vec![event],
                pending_known: 0,
                pending_unknown: true,
            })
        }
    }

    #[test]
    fn a_page_consumed_past_its_own_high_water_is_refused_and_nothing_is_written() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        let result = drain_once(
            &state,
            &ConsumedPastHighWater {
                high_water_lineage: "inst:2",
            },
            "broca",
            20,
            99,
        );
        assert!(
            matches!(&result, Err(PullError::InvalidResponse(message))
                if message.contains("consumed ordinal 507 is past high water 506")),
            "{result:?}"
        );
        assert_eq!(stored_state(&state), ("inst:2".into(), 500, 50, 0));
    }

    /// Ordinals of different lineages count different sequences, so a higher
    /// consumed ordinal than another lineage's high water is not an ordering
    /// fault, and an equal one in the same lineage is the caught-up page.
    #[test]
    fn consumed_and_high_water_are_compared_only_within_one_lineage() {
        let page = ConsumedPastHighWater {
            high_water_lineage: "inst:3",
        }
        .pull(None, 20)
        .unwrap();
        assert_eq!(validate_page(&page), Ok(()));
        let mut caught_up = page;
        caught_up.high_water = caught_up.cursor.clone();
        assert_eq!(validate_page(&caught_up), Ok(()));
    }

    #[test]
    fn same_instance_lower_epoch_is_refused_and_nothing_is_written() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        let result = drain_once(
            &state,
            &RegressedPage {
                instance: "inst",
                epoch: 1,
                seq: 9_000,
            },
            "broca",
            20,
            99,
        );
        assert!(
            matches!(result, Err(PullError::LineageRegressed { .. })),
            "{result:?}"
        );
        assert_eq!(stored_state(&state), ("inst:2".into(), 500, 50, 0));
    }

    /// The guard must not fire on the two legitimate backward-looking shapes:
    /// a higher epoch whose sequence restarted, and a different instance
    /// (the announced-identity-change path). Without this twin the
    /// regression tests could pass against a guard that refuses every page.
    #[test]
    fn higher_epoch_with_restarted_sequence_and_new_instance_both_apply() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        drain_once(
            &state,
            &RegressedPage {
                instance: "inst",
                epoch: 3,
                seq: 4,
            },
            "broca",
            20,
            99,
        )
        .unwrap();
        assert_eq!(stored_state(&state), ("inst:3".into(), 4, 99, 1));

        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        drain_once(
            &state,
            &RegressedPage {
                instance: "other",
                epoch: 0,
                seq: 1,
            },
            "broca",
            20,
            99,
        )
        .unwrap();
        assert_eq!(stored_state(&state), ("other:0".into(), 1, 99, 1));
    }
}

#[cfg(test)]
mod loop_tests {
    use super::*;
    use crate::state::test_state;

    struct ReadySource;
    impl SourceClient for ReadySource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "live:1".into(),
                    ordinal: 9,
                },
                high_water: SourceCursor {
                    lineage: "live:1".into(),
                    ordinal: 9,
                },
                events: Vec::new(),
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    struct PricingSource;

    #[test]
    fn hourly_summary_tick_publishes_real_drain_facts_without_changing_the_served_report() {
        let state = test_state();
        run_iteration_with_capacity(
            &state,
            &[Producer {
                source_key: "broca".into(),
                client: Arc::new(PricingSource),
            }],
            None,
            &AtomicBool::new(false),
        );
        let before =
            crate::ops::dispatch(&state, "spend.workclass.report", serde_json::json!({})).unwrap();
        assert_eq!(before["rows"][0]["events_total"], 1);
        let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
        let stop = AtomicBool::new(true);
        summary_tick(&state, &mut summaries, &stop);
        assert_eq!(
            state
                .with_conn(|conn| conn
                    .query_row("SELECT count(*) FROM summary_cell", [], |row| row
                        .get::<_, i64>(0))
                    .map_err(|error| error.to_string()))
                .unwrap(),
            0
        );
        stop.store(false, Ordering::Relaxed);
        summary_tick(&state, &mut summaries, &stop);
        assert_eq!(
            crate::ops::dispatch(&state, "spend.workclass.report", serde_json::json!({})).unwrap(),
            before
        );
        let (facts,cells,dirty):(i64,i64,i64)=state.with_conn(|conn|conn.query_row("SELECT (SELECT count(*) FROM usage_event),(SELECT count(*) FROM summary_cell),(SELECT count(*) FROM summary_dirty_hour)",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map_err(|error|error.to_string())).unwrap();
        assert_eq!(facts, 1);
        assert!(cells > 0);
        assert_eq!(dirty, 0);
    }

    #[test]
    fn hourly_report_snapshot_keeps_old_summary_money_across_a_write_and_publication() {
        use astrocyte_core::{ingest, verdict};
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let result=crate::ops::dispatch(&state,"spend.event.ingest",serde_json::json!({"producer":"fixture","producer_ref":"snapshot-one","provider":"provider","model":"model","charge_basis":"metered_api","occurred_at_ms":1000,
            "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":12,"quantity_exponent":0}]})).unwrap();
        let event_id = result["event_id"].as_str().unwrap().to_string();
        // Hand-valued setup isolates snapshot behavior from catalog/network availability.
        state.ordered_write(|tx| {
            tx.execute("UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_id=?1",[&event_id]).map_err(|error|error.to_string())?;
            tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token',1,'priced','metered_api',0,'USD',100,1,9,'none','fixture',1,'{}','pricing_pass',1)",[&event_id]).map_err(|error|error.to_string())?;
            Ok(((),true))
        }).unwrap();
        let mut summaries = astrocyte_core::report::SummaryMaintenance::default();
        let stop = AtomicBool::new(false);
        summary_tick(&state, &mut summaries, &stop);
        let reader = rusqlite::Connection::open_with_flags(
            directory.path().join("ledger.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        reader.busy_timeout(Duration::from_secs(5)).unwrap();
        reader.pragma_update(None, "foreign_keys", true).unwrap();
        let snapshot = reader.unchecked_transaction().unwrap();
        let request = verdict::VerdictRequest {
            project_root: None,
            provider: None,
            model: None,
            est_cost_nanos: None,
        };
        let before = verdict::evaluate(&snapshot, &request).unwrap();
        assert_eq!(before.consumed_nanos["USD"], 100);
        state
            .ordered_write(|tx| {
                ingest::record_adjustment_in_tx(
                    tx,
                    &ingest::AdjustmentRecordRequest {
                        event_id: event_id.clone(),
                        currency: "USD".into(),
                        amount_nanos: -25,
                        reason: ingest::AdjustmentReason::Refund,
                        occurred_at_ms: 2,
                        note: None,
                    },
                    &ingest::AdjustmentWriteContext::correction_ingest("snapshot-refund".into()),
                    3,
                )
                .map_err(|error| format!("{error:?}"))?;
                Ok(((), true))
            })
            .unwrap();
        summary_tick(&state, &mut summaries, &stop);
        assert_eq!(
            serde_json::to_vec(&verdict::evaluate(&snapshot, &request).unwrap()).unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
        snapshot.commit().unwrap();
        let after = crate::ops::dispatch(&state, "budget.verdict", serde_json::json!({})).unwrap();
        assert_eq!(after["consumed_nanos"]["USD"], 75);
        let (facts,adjustments,dirty):(i64,i64,i64)=state.with_conn(|conn|conn.query_row("SELECT (SELECT count(*) FROM usage_event),(SELECT count(*) FROM adjustment),(SELECT count(*) FROM summary_dirty_hour)",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map_err(|error|error.to_string())).unwrap();
        assert_eq!((facts, adjustments, dirty), (1, 1, 0));
    }

    impl SourceClient for PricingSource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "pricing:1".into(),
                    ordinal: 7,
                },
                high_water: SourceCursor {
                    lineage: "pricing:1".into(),
                    ordinal: 7,
                },
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    struct FailedCapacity;

    impl CapacitySource for FailedCapacity {
        fn poll(&self) -> Result<CapacitySnapshot, PullError> {
            Err(PullError::Unavailable("qta unavailable".into()))
        }
    }

    struct ReadyCapacity;

    impl CapacitySource for ReadyCapacity {
        fn poll(&self) -> Result<CapacitySnapshot, PullError> {
            Ok(CapacitySnapshot {
                windows: vec![astrocyte_core::capacity::CapacityWindow {
                    provider: "codex".into(),
                    api_provider: Some("openai".into()),
                    account_ref: "account-a".into(),
                    pool: "primary".into(),
                    pool_key: "primary".into(),
                    slot_kind: "primary".into(),
                    external_id: None,
                    title: None,
                    extra_ordinal: None,
                    legacy_pool_key: None,
                    used_percent: 25.0,
                    raw_used_percent: None,
                    resets_at: None,
                    window_minutes: Some(300),
                    used_count: None,
                    total_count: None,
                    source: Some("oauth".into()),
                    fetched_at: Some("2026-09-05T08:00:00Z".into()),
                    stale_since: None,
                    stale_class: None,
                    breakdown: None,
                }],
                observations: Vec::new(),
                provider_readings: Vec::new(),
                complete_providers: None,
                degraded_entries: 0,
            })
        }
    }

    /// Waits until `key`'s cursor has advanced to the source's page. Bounded
    /// only so that a producer that never advances fails by name instead of
    /// hanging; a healthy loop gets there on its first tick.
    fn wait_for_success(state: &SharedState, key: &str) {
        crate::test_support::wait_until(&format!("{key} advancing its cursor"), || {
            state.with_conn(|conn| conn.query_row("SELECT COUNT(*) FROM source_runtime WHERE source_key=?1 AND consumed_ordinal=9 AND last_success_ms>0", [key], |r| r.get::<_,i64>(0)).map_err(|e|e.to_string())).unwrap() == 1
        });
    }

    /// Runs `run_loop` over `producers` on its own thread, returning a guard
    /// that stops and joins the loop when the test ends, also when an
    /// assertion fails before the test stops it.
    fn spawn_run_loop(
        state: &Arc<SharedState>,
        producers: Vec<Producer>,
        stop: &Arc<AtomicBool>,
        finished: std::sync::mpsc::Sender<()>,
    ) -> crate::test_support::Background<()> {
        let state = Arc::clone(state);
        let loop_stop = Arc::clone(stop);
        let thread = std::thread::spawn(move || {
            run_loop(state, producers, 60, loop_stop);
            let _ = finished.send(());
        });
        let stop = Arc::clone(stop);
        crate::test_support::Background::new("the refresher loop", thread, move || {
            stop.store(true, Ordering::Relaxed)
        })
    }

    #[test]
    fn failing_capacity_poll_does_not_stop_spend_cursor_advancing() {
        let state = test_state();
        state.configure_capacity_runtime(true, 5_000, 10).unwrap();
        let producer = Producer {
            source_key: "broca".into(),
            client: Arc::new(ReadySource),
        };

        run_iteration_with_capacity(
            &state,
            &[producer],
            Some(&FailedCapacity),
            &AtomicBool::new(false),
        );

        let (cursor, last_attempt, runtime): (i64, Option<i64>, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT source_runtime.consumed_ordinal,
                            capacity_runtime.last_attempt_ms,
                            capacity_runtime.runtime_variant
                     FROM source_runtime CROSS JOIN capacity_runtime
                     WHERE source_runtime.source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(cursor, 9);
        assert!(
            last_attempt.is_some(),
            "the failed capacity attempt must be durable"
        );
        assert_eq!(runtime, "configured_unavailable");
    }

    #[test]
    fn failing_capacity_poll_does_not_stop_spend_pricing_pass() {
        let state = test_state();
        state.configure_capacity_runtime(true, 5_000, 10).unwrap();
        let producer = Producer {
            source_key: "broca".into(),
            client: Arc::new(PricingSource),
        };

        run_iteration_with_capacity(
            &state,
            &[producer],
            Some(&FailedCapacity),
            &AtomicBool::new(false),
        );

        let priced_rows: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert!(
            priced_rows > 0,
            "a failed capacity poll must not skip the spend pricing pass"
        );
    }

    #[test]
    fn failing_spend_drain_does_not_stop_capacity_applying() {
        let state = test_state();
        state.configure_capacity_runtime(true, 7_000, 10).unwrap();
        let producer = Producer {
            source_key: "broca".into(),
            client: Arc::new(UnavailableSource),
        };

        run_iteration_with_capacity(
            &state,
            &[producer],
            Some(&ReadyCapacity),
            &AtomicBool::new(false),
        );

        let (windows, pending_unknown, runtime): (i64, i64, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT (SELECT COUNT(*) FROM capacity_window),
                            source_runtime.pending_unknown,
                            capacity_runtime.runtime_variant
                     FROM source_runtime CROSS JOIN capacity_runtime
                     WHERE source_runtime.source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(windows, 1);
        assert_eq!(pending_unknown, 1);
        assert_eq!(runtime, "active");
    }

    #[test]
    fn unconfigured_capacity_stays_disabled_and_fresh() {
        let state = test_state();
        let config = config::AstrocyteConfig {
            poll_interval_secs: 11,
            qta_connection_file: None,
            fusiform_connection_file: None,
            reasoning_billed_as_output: Vec::new(),
            subscription_multiplier: config::intentional_empty().config.subscription_multiplier,
            harness_ingest: config::HarnessIngest::all_on(),
        };

        let capacity = configured_capacity(&state, &config).unwrap();
        run_iteration_with_capacity(&state, &[], capacity.as_deref(), &AtomicBool::new(false));

        assert!(capacity.is_none());

        let freshness = state
            .with_conn(|conn| {
                astrocyte_core::capacity::freshness(conn).map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(freshness.runtime_variant, "disabled");
        assert_eq!(freshness.effective_cadence_ms, 11_000);
        let runtime = state.public_projection().health(i64::MAX).capacity_runtime;
        assert_eq!(runtime["runtimeVariant"], "disabled");
        assert_eq!(runtime["effectiveHorizonMs"], 44_000);
        assert_eq!(runtime["stale"], false);
        assert!(!astrocyte_core::capacity::is_stale(
            &freshness,
            i64::MAX,
            freshness.effective_cadence_ms.saturating_mul(4),
        ));
    }

    fn add_second_producer_runtime(state: &SharedState) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO source_runtime (source_key,source_module,desired_generation,lineage,consumed_ordinal,high_water_ordinal,last_success_ms,pending_known,pending_unknown) VALUES ('second-producer','second-producer',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,NULL,0,1)",
                    [],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn two_producer_failures_are_isolated_in_both_directions() {
        let stop = AtomicBool::new(false);
        let state = test_state();
        add_second_producer_runtime(&state);
        run_iteration(
            &state,
            &[
                Producer {
                    source_key: "second-producer".into(),
                    client: Arc::new(UnavailableSource),
                },
                Producer {
                    source_key: "broca".into(),
                    client: Arc::new(ReadySource),
                },
            ],
            &stop,
        );
        let cursors: (Option<i64>, Option<i64>) = state
            .with_conn(|conn| {
                Ok((
                    conn.query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='broca'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?,
                    conn.query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='second-producer'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?,
                ))
            })
            .unwrap();
        assert_eq!(cursors, (Some(9), None));
        assert!(state.producer_degraded_reason("second-producer").is_some());
        assert!(state.producer_degraded_reason("broca").is_none());

        let state = test_state();
        add_second_producer_runtime(&state);
        run_iteration(
            &state,
            &[
                Producer {
                    source_key: "broca".into(),
                    client: Arc::new(UnavailableSource),
                },
                Producer {
                    source_key: "second-producer".into(),
                    client: Arc::new(ReadySource),
                },
            ],
            &stop,
        );
        let second: Option<i64> = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT consumed_ordinal FROM source_runtime WHERE source_key='second-producer'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(second, Some(9));
        assert!(state.producer_degraded_reason("broca").is_some());
        assert!(state.producer_degraded_reason("second-producer").is_none());
    }

    /// The sources a production tick drains are fixed: the only producer
    /// drained through `drain_once` is BROCA's store reader, and Pi and
    /// OpenCode are read from disk. Asserted on the lists themselves -- the
    /// producer keys the refresher builds and the cursor rows a publish plus
    /// one tick leave behind -- because a retired producer that is still
    /// drained would show up there even if its drain happened to succeed.
    #[test]
    fn a_fully_configured_tick_drains_exactly_broca_pi_and_opencode() {
        let temp = tempfile::TempDir::new().unwrap();
        let connection = temp.path().join("subc-connection.json");
        let connection = connection.to_string_lossy().into_owned();
        let desired = config::canonicalize(config::AstrocyteConfig {
            poll_interval_secs: 60,
            qta_connection_file: Some(connection.clone()),
            fusiform_connection_file: Some(connection),
            reasoning_billed_as_output: Vec::new(),
            subscription_multiplier: config::intentional_empty().config.subscription_multiplier,
            harness_ingest: config::HarnessIngest::all_on(),
        })
        .unwrap();

        let producers = refresher_producers(&Arc::new(AtomicBool::new(false)));
        let producer_keys: Vec<&str> = producers
            .iter()
            .map(|producer| producer.source_key.as_str())
            .collect();
        assert_eq!(producer_keys, ["broca"]);

        let state = test_state();
        state
            .ordered_write(|tx| publish_desired_transaction(tx, &desired, 1))
            .unwrap();
        // OpenCode is pointed at a directory where its database should be, so
        // its attempt is also visible as a degradation; Pi reads one real
        // session. Both leave a cursor row.
        let opencode_database = temp.path().join("opencode-database-is-a-directory");
        std::fs::create_dir(&opencode_database).unwrap();
        let opencode_source = opencode_source::OpenCodeSource::at(opencode_database);
        let pi_session = temp.path().join("pi").join("encoded");
        std::fs::create_dir_all(&pi_session).unwrap();
        std::fs::write(
            pi_session.join("session.jsonl"),
            concat!(
                r#"{"type":"session","id":"pi-session","timestamp":"2026-09-01T00:00:00.000Z","cwd":"/pi/project"}"#,
                "\n",
                r#"{"type":"message","id":"entry-1","timestamp":"2026-09-01T00:00:01.000Z","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"pi-response","usage":{"input":11,"output":7,"cacheRead":13,"cacheWrite":17,"reasoning":2,"totalTokens":50}}}"#,
                "\n"
            ),
        )
        .unwrap();
        let pi_source = pi_source::PiSource::at(temp.path().join("pi"));
        // The refresher's BROCA reader points at a store that does not exist
        // under test; the tick is driven with the same source keys over an
        // in-memory client instead, so each leaves a cursor row.
        let ticked: Vec<Producer> = producer_keys
            .iter()
            .map(|source_key| Producer {
                source_key: (*source_key).into(),
                client: Arc::new(ReadySource),
            })
            .collect();
        run_iteration_with_file_sources(
            &state,
            &ticked,
            &opencode_source,
            &pi_source,
            &AtomicBool::new(false),
        );

        let runtime_keys: Vec<String> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT source_key FROM source_runtime ORDER BY source_key")
                    .map_err(|error| error.to_string())?;
                let keys = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(|error| error.to_string());
                keys
            })
            .unwrap();
        assert_eq!(runtime_keys, ["broca", "opencode", "pi"]);
        assert!(state.producer_degraded_reason("opencode").is_some());
        assert!(state.producer_degraded_reason("broca").is_none());
        assert!(state.producer_degraded_reason("pi").is_none());
    }

    #[test]
    fn failed_producer_degrades_only_itself_and_second_producer_advances() {
        let state = Arc::new(test_state());
        state.ordered_write(|tx| {
            tx.execute("UPDATE source_runtime SET lineage='old:1',consumed_ordinal=1,high_water_ordinal=1,last_success_ms=1,pending_unknown=0 WHERE source_key='broca'", []).unwrap();
            Ok(((),true))
        }).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let (finished, _) = std::sync::mpsc::channel();
        let refresher = spawn_run_loop(
            &state,
            vec![
                Producer {
                    source_key: "broca".into(),
                    client: Arc::new(UnavailableSource),
                },
                Producer {
                    source_key: "second-producer".into(),
                    client: Arc::new(ReadySource),
                },
            ],
            &stop,
            finished,
        );
        wait_for_success(&state, "second-producer");
        refresher.stop_and_join();
        let stored: (i64,i64,i64) = state.with_conn(|conn| conn.query_row("SELECT consumed_ordinal,last_success_ms,pending_unknown FROM source_runtime WHERE source_key='broca'", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|e.to_string())).unwrap();
        assert_eq!(stored, (1, 1, 1));
    }

    #[test]
    fn shutdown_mid_interval_does_not_wait_sixty_seconds() {
        let state = Arc::new(test_state());
        let stop = Arc::new(AtomicBool::new(false));
        let (finished, done) = std::sync::mpsc::channel();
        let mut refresher = spawn_run_loop(
            &state,
            vec![Producer {
                source_key: "test".into(),
                client: Arc::new(ReadySource),
            }],
            &stop,
            finished,
        );
        wait_for_success(&state, "test");
        // Stop only once the loop has begun the wait after its first tick.
        // A stop that landed earlier, while the tick was still running, would
        // end the loop at its next check without the wait ever seeing it.
        crate::test_support::wait_until("the loop beginning its 60 s interval", || {
            state.interval_waits_for_test() >= 1
        });
        refresher.signal_stop();
        // The interval is 60 s, so a loop that slept through the stop could
        // not finish within half of it. Nothing healthy comes near this bound:
        // the wait checks the stop at least every 250 ms.
        assert!(
            done.recv_timeout(Duration::from_secs(30)).is_ok(),
            "stop must interrupt the poll interval"
        );
        refresher.stop_and_join();
    }
}

// cortexkit-log prefixes component targets with the supervised module id.
// Keep these emission boundaries callable so tests exercise the actual events.
pub fn log_config_retained(failure: &str) {
    tracing::warn!(target: "config", %failure, "desired configuration unavailable; retaining committed generation");
}

/// `degraded` is whether the failure was also recorded durably in the
/// producer's runtime row.
pub fn log_producer_drain_failed(producer: &str, reason: &str, degraded: &Result<(), String>) {
    tracing::warn!(target: "drain", %producer, %reason, ?degraded, "producer drain failed");
}

pub fn log_capacity_failed(error: &str) {
    tracing::error!(target: "capacity", %error, "capacity poll persistence failed");
}

pub fn log_pricing_failed(error: &str) {
    tracing::error!(target: "pricing", %error, "pricing pass failed");
}

#[cfg(test)]
mod multiplier_stage_tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use astrocyte_core::catalog_client::CatalogCallError;
    use astrocyte_core::store::MIGRATIONS;
    use fusiform_protocol::{
        CatalogGetRequest, CatalogGetResponse, PlanAmount, PlanPriceWire, PlanPricesRequest,
        PlanPricesResponse,
    };
    use serde_json::json;

    use super::*;

    /// 2024-10-04T00:00Z plus an hour: the first tick of that UTC day.
    const DAY_START: i64 = 20_000 * DAY_MS;
    const NOW: i64 = DAY_START + 3_600_000;

    /// Serves one $10-per-million rate for every model, the Pro 20x plan
    /// price, and fusiform's history-boundary refusal. Plan-price calls are
    /// counted and can be made to fail. The rate can be changed, a rate key
    /// this decoder does not know can be added beside it, and every model can
    /// be refused as outside the catalog's coverage.
    struct FakeFusiform {
        catalog_calls: AtomicUsize,
        plan_calls: AtomicUsize,
        plan_prices_fail: std::sync::atomic::AtomicBool,
        input_rate_units: std::sync::atomic::AtomicI64,
        unrecognized_key: std::sync::atomic::AtomicBool,
        uncovered: std::sync::atomic::AtomicBool,
    }

    impl FakeFusiform {
        fn new(plan_prices_fail: bool) -> Arc<Self> {
            Arc::new(Self {
                catalog_calls: AtomicUsize::new(0),
                plan_calls: AtomicUsize::new(0),
                plan_prices_fail: std::sync::atomic::AtomicBool::new(plan_prices_fail),
                input_rate_units: std::sync::atomic::AtomicI64::new(10_000_000_000),
                unrecognized_key: std::sync::atomic::AtomicBool::new(false),
                uncovered: std::sync::atomic::AtomicBool::new(false),
            })
        }
    }

    impl CatalogConnection for FakeFusiform {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            self.catalog_calls.fetch_add(1, Ordering::SeqCst);
            if request.at_ms == Some(0) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "fusiform's history begins at 1".into(),
                });
            }
            if self.uncovered.load(Ordering::SeqCst) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "the catalog has no row for this model".into(),
                });
            }
            let key = format!(
                "{}/{}",
                request.provider_id.unwrap(),
                request.model_id.unwrap()
            );
            let rate = json!({"state":"priced","units":self.input_rate_units.load(Ordering::SeqCst),"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
            let mut facts = BTreeMap::from([("rate.input".to_string(), rate.clone())]);
            facts.insert("rate.output".into(), json!({"state":"priced","units":20_000_000_000_i64,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}));
            if self.unrecognized_key.load(Ordering::SeqCst) {
                facts.insert("rate.input.above_context.272k".to_string(), rate);
            }
            Ok(CatalogGetResponse {
                source: "models.dev".into(),
                resolved_at_ms: request.at_ms.unwrap(),
                catalog_version: 1,
                models: BTreeMap::from([(key, facts)]),
                withheld: vec![],
                uncertain: vec![],
                overridden: vec![],
                retired: vec![],
                aliased: vec![],
                plane: None,
                billing_rules: vec![],
            })
        }
    }

    impl PlanPriceSource for FakeFusiform {
        fn plan_prices(
            &self,
            _request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            if self.plan_prices_fail.load(Ordering::SeqCst) {
                return Err(CatalogCallError {
                    code: "unavailable".into(),
                    message: "fusiform is restarting".into(),
                });
            }
            Ok(PlanPricesResponse {
                prices: vec![PlanPriceWire {
                    provider_id: "openai".into(),
                    tier: "pro_20x".into(),
                    price: Some(PlanAmount {
                        minor_units: 20_000,
                        exponent: 2,
                        currency: "USD".into(),
                        period: "month".into(),
                    }),
                    quota_value: None,
                    refusal_reason: None,
                    boundary_at_ms: 0,
                    established_by: "fusiform".into(),
                    established_at_ms: 0,
                    review_by_ms: i64::MAX,
                    source_ref: "https://example.invalid".into(),
                }],
                resolved_at_ms: NOW,
                tier_vocabulary: "vendor".into(),
                unit_policy: "list".into(),
            })
        }
    }

    const DESIRED: &str = r#"{
        "poll_interval_secs": 60,
        "qta_connection_file": null,
        "fusiform_connection_file": null,
        "reasoning_billed_as_output": ["openai"],
        "subscription_multiplier": {
            "api_twins": {},
            "plan_price_overrides": {},
            "quota_providers": {"codex": {
                "window": {"pool_key": "primary", "window_minutes": 10080},
                "scope": [{"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null}],
                "accounts": {"acct": {"plan": "openai/pro_20x", "price_override": null}}
            }}
        },
        "harness_ingest": {"enabled": true, "exclude": []}
    }"#;

    fn publish_desired(state: &Arc<SharedState>, directory: &tempfile::TempDir) {
        publish_document(state, directory, DESIRED);
    }

    fn publish_document(state: &Arc<SharedState>, directory: &tempfile::TempDir, document: &str) {
        let path = directory.path().join("astrocyte.jsonc");
        std::fs::write(&path, document).unwrap();
        reload_config(state, Ok(path)).unwrap();
    }

    /// `DESIRED` with its `harness_ingest` member replaced.
    fn desired_with_ingest(ingest: &str) -> String {
        DESIRED.replace(
            r#""harness_ingest": {"enabled": true, "exclude": []}"#,
            &format!(r#""harness_ingest": {ingest}"#),
        )
    }

    /// A reason the stage logged for the `codex` quota provider.
    fn codex_failure(stage: &MultiplierStage) -> Option<String> {
        stage
            .failure_logged
            .iter()
            .find(|(provider, _, _)| provider == "codex")
            .map(|(_, _, reason)| reason.clone())
    }

    /// A scope naming a producer the configuration excludes would count none
    /// of its tokens while the quota's used percent still counts its traffic.
    /// The day fails by name instead, writes nothing, and makes no fusiform
    /// call; it is recorded once the producer is ingested again.
    #[test]
    fn a_scope_naming_an_excluded_producer_fails_the_day_by_name() {
        for ingest in [
            r#"{"enabled": true, "exclude": ["opencode"]}"#,
            r#"{"enabled": false, "exclude": []}"#,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            publish_document(&state, &directory, &desired_with_ingest(ingest));
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            seed_usage(&state);
            let mut stage = MultiplierStage::new(Some(fusiform.clone()));
            stage.tick(&state, false, NOW);
            assert!(current_day_observations(&state).is_empty(), "{ingest}");
            assert_eq!(
                codex_failure(&stage).as_deref(),
                Some("scoped_producer_not_ingested: opencode"),
                "{ingest}"
            );
            assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0, "{ingest}");

            publish_desired(&state, &directory);
            stage.tick(&state, false, NOW + 60_000);
            assert_eq!(
                current_day_observations(&state),
                [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)],
                "{ingest}"
            );
        }
    }

    /// Excluding a producer the scope does not name changes nothing: the day
    /// is recorded as before.
    #[test]
    fn excluding_a_producer_outside_every_scope_still_records_the_day() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_document(
            &state,
            &directory,
            &desired_with_ingest(r#"{"enabled": true, "exclude": ["goose", "broca"]}"#),
        );
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
        assert_eq!(codex_failure(&stage), None);
    }

    /// BROCA is read from its store like every other reader and needs no
    /// configuration, so a scope naming it is refused only when BROCA is
    /// excluded. An absent store (BROCA not installed) is not a refusal:
    /// nothing on disk means no BROCA tokens exist to leave out.
    #[test]
    fn a_scope_naming_broca_is_refused_only_when_broca_is_excluded() {
        let with_broca_scope = |document: &str| {
            document.replace(
                r#""scope": [{"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null}]"#,
                r#""scope": [{"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null}, {"producer": "broca", "provider": "openai", "charge_basis": "subscription_included", "model_prefix": null}]"#,
            )
        };
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_document(
            &state,
            &directory,
            &with_broca_scope(&desired_with_ingest(
                r#"{"enabled": true, "exclude": ["broca"]}"#,
            )),
        );
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(current_day_observations(&state).is_empty());
        assert_eq!(
            codex_failure(&stage).as_deref(),
            Some("scoped_producer_not_ingested: broca")
        );

        publish_document(&state, &directory, &with_broca_scope(DESIRED));
        stage.tick(&state, false, NOW + 60_000);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
    }

    fn install_catalog(state: &SharedState, fusiform: &Arc<FakeFusiform>) {
        let connection: Arc<dyn CatalogConnection> = fusiform.clone();
        state
            .install_catalog(Some(CatalogClient::connect(connection).unwrap()))
            .unwrap();
    }

    /// Ten points of quota and 1,000 opencode/openai tokens inside the seven
    /// days before `DAY_START`, with no active reader runtime rows.
    fn seed_usage(state: &SharedState) {
        state
            .ordered_write(|tx| {
                // The leased fixture seeds BROCA for drain tests; this fixture
                // has no active store reader unless a test adds one explicitly.
                tx.execute("DELETE FROM source_runtime", [])
                    .map_err(|error| error.to_string())?;
                for (at_ms, used) in [(DAY_START - 8 * DAY_MS, 0.0), (DAY_START - DAY_MS, 10.0)] {
                    tx.execute(
                        "INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes)
                         VALUES ('codex','acct','primary',?1,?2,10080)",
                        params![at_ms * 1_000_000, used],
                    )
                    .map_err(|error| error.to_string())?;
                }
                tx.execute(
                    "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis)
                     VALUES ('e1','opencode','lineage','e1',?1,?1,'openai','m','unknown')",
                    [DAY_START - 2 * DAY_MS],
                )
                .map_err(|error| error.to_string())?;
                tx.execute(
                    "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                     VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'input','token','billed','usage.input',1000,0)",
                    [],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    fn current_day_observations(state: &SharedState) -> Vec<(String, String, i64)> {
        state
            .with_read_op(|conn| {
                let mut statement = conn
                    .prepare("SELECT quota_provider, observation_day, api_nanos FROM multiplier_observation WHERE observation_day=?1 ORDER BY observation_id")
                    .unwrap();
                let rows = statement
                    .query_map([DAY], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                Ok(rows)
            })
            .unwrap()
    }

    #[test]
    fn multiplier_uses_and_records_top_level_reasoning_billing() {
        for declared in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            let document = if declared {
                DESIRED.to_owned()
            } else {
                DESIRED.replace(
                    "\"reasoning_billed_as_output\": [\"openai\"]",
                    "\"reasoning_billed_as_output\": []",
                )
            };
            publish_document(&state, &directory, &document);
            seed_usage(&state);
            state.ordered_write(|tx| {
                tx.execute("INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                    VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'reasoning','token','billed','usage.reasoning',500,0)", [])
                    .map_err(|error| error.to_string())?;
                Ok(((), true))
            }).unwrap();
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            MultiplierStage::new(Some(fusiform)).tick(&state, false, NOW);
            assert_eq!(
                current_day_observations(&state),
                [(
                    "codex".into(),
                    DAY.into(),
                    if declared { 20_000_000 } else { 10_000_000 }
                )]
            );
            let recorded: String = state.with_conn(|conn| conn.query_row(
                "SELECT reasoning_billed_as_output FROM multiplier_observation WHERE observation_day=?1", [DAY], |row| row.get(0))
                .map_err(|error| error.to_string())).unwrap();
            assert_eq!(recorded, if declared { "[\"openai\"]" } else { "[]" });
        }
    }

    fn observation_days(state: &SharedState) -> Vec<String> {
        state
            .with_read_op(|conn| {
                let mut statement = conn
                    .prepare("SELECT observation_day FROM multiplier_observation ORDER BY observation_id")
                    .unwrap();
                Ok(statement
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap())
            })
            .unwrap()
    }

    fn source_success(state: &SharedState, source: &str, last_success_ms: Option<i64>) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO source_runtime
                     (source_key,source_module,desired_generation,last_success_ms,pending_known,pending_unknown)
                     VALUES (?1,?1,(SELECT generation FROM desired_config WHERE id = 1),?2,0,1)",
                    params![source, last_success_ms],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn a_day_that_failed_until_midnight_is_recorded_on_a_later_tick() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(true);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(observation_days(&state).is_empty());
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + DAY_MS);
        assert!(observation_days(&state).contains(&DAY.to_string()));
        let before = all_rows(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + DAY_MS + 60_000);
        assert_eq!(
            all_rows(&state),
            before,
            "restarts must not add another original"
        );
    }

    #[test]
    fn a_day_eight_days_back_is_never_attempted_automatically() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(true);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        fusiform.plan_calls.store(0, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 8 * DAY_MS);
        assert_eq!(
            observation_days(&state),
            (2..=8)
                .map(|offset| multiplier::utc_day(DAY_START + offset * DAY_MS).unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 7);
    }

    #[test]
    fn two_missed_days_are_recorded_oldest_first() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        let before = observation_days(&state).len();
        fusiform.plan_prices_fail.store(true, Ordering::SeqCst);
        stage.tick(&state, false, NOW + DAY_MS);
        stage.tick(&state, false, NOW + 2 * DAY_MS);
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 2 * DAY_MS + 60_000);
        assert_eq!(
            &observation_days(&state)[before..],
            ["2024-10-05", "2024-10-06"]
        );
    }

    #[test]
    fn the_day_waits_for_every_source_and_includes_its_late_usage() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", Some(DAY_START + 1));
        source_success(&state, "pi", Some(DAY_START - 1));
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(!observation_days(&state).contains(&DAY.to_string()));
        state.ordered_write(|tx| {
            tx.execute(
                "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis)
                 VALUES ('late','opencode','lineage','late',?1,?2,'openai','m','unknown')",
                params![DAY_START - DAY_MS, NOW],
            ).map_err(|error| error.to_string())?;
            tx.execute(
                "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES ((SELECT event_key FROM usage_event WHERE event_id='late'),'input','token','billed','usage.input',1000,0)", [],
            ).map_err(|error| error.to_string())?;
            Ok(((), true))
        }).unwrap();
        source_success(&state, "pi", Some(NOW + 60_000));
        stage.tick(&state, false, NOW + 60_000);
        let day_rows: Vec<_> = current_day_observations(&state)
            .into_iter()
            .filter(|row| row.1 == DAY)
            .collect();
        assert_eq!(day_rows, [("codex".into(), DAY.into(), 20_000_000)]);
        stage.tick(&state, false, NOW + 120_000);
        assert_eq!(
            observation_days(&state)
                .iter()
                .filter(|day| *day == DAY)
                .count(),
            1
        );
    }

    #[test]
    fn at_two_hours_the_day_records_and_names_all_stale_sources() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", None);
        source_success(&state, "pi", Some(DAY_START - 1));
        source_success(&state, "codex", Some(DAY_START + 1));
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, DAY_START + 7_200_000 - 1);
        assert!(!observation_days(&state).contains(&DAY.to_string()));
        stage.tick(&state, false, DAY_START + 7_200_000);
        let text: String = state
            .with_read_op(|conn| {
                conn.query_row(
                    "SELECT error_deflating FROM multiplier_observation WHERE observation_day=?1",
                    [DAY],
                    |row| row.get(0),
                )
                .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap();
        assert!(text.contains("stale_sources: opencode, pi"), "{text}");
        assert!(!text.contains("codex"), "{text}");
    }

    #[test]
    fn recompute_creates_a_missing_day_without_supersedes_and_refuses_unknown_provider() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let outcome = recompute(&state, &fusiform, DAY, Some("codex"), NOW).unwrap();
        assert!(outcome["recomputed"][0]["supersedes"].is_null());
        assert_eq!(
            current_day_observations(&state),
            [("codex".into(), DAY.into(), 10_000_000)]
        );
        let before = all_rows(&state);
        assert_eq!(
            recompute(&state, &fusiform, DAY, Some("unknown"), NOW)
                .unwrap_err()
                .0,
            "unknown_quota_provider"
        );
        assert_eq!(all_rows(&state), before);
        let again = recompute(&state, &fusiform, DAY, Some("codex"), NOW + 1).unwrap();
        assert_eq!(
            again["recomputed"][0]["supersedes"],
            outcome["recomputed"][0]["observation_id"]
        );
    }

    #[test]
    fn recompute_waits_only_for_a_missing_days_sources() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", None);
        assert_eq!(
            recompute(&state, &fusiform, DAY, Some("codex"), NOW)
                .unwrap_err()
                .0,
            "source_reads_pending"
        );
        assert!(all_rows(&state).is_empty());
        let created =
            recompute(&state, &fusiform, DAY, Some("codex"), DAY_START + 7_200_000).unwrap();
        assert!(created["recomputed"][0]["supersedes"].is_null());
        let text: String = state
            .with_read_op(|conn| {
                conn.query_row(
                    "SELECT error_deflating FROM multiplier_observation",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap();
        assert!(text.contains("stale_sources: opencode"), "{text}");
        let corrected = recompute(&state, &fusiform, DAY, Some("codex"), NOW).unwrap();
        assert_eq!(
            corrected["recomputed"][0]["supersedes"],
            created["recomputed"][0]["observation_id"]
        );
        assert_eq!(all_rows(&state).len(), 2);
    }

    /// A store whose committed generation predates the multiplier column
    /// boots with the member "not recorded" -- not as an empty declaration --
    /// and the daily stage writes nothing. The next reload from a valid file
    /// publishes a generation that records the member, and the stage then
    /// records the day exactly once, across repeated ticks and a restart.
    #[test]
    fn a_pre_multiplier_generation_records_nothing_until_a_reload_publishes_the_member() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("ledger.db");
        {
            let store = open_sqlite(&StorageDescriptor {
                module_id: "ck-astrocyte-leased-test".to_string(),
                storage_namespace: "astrocyte".to_string(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: database.to_string_lossy().into_owned(),
                },
            })
            .unwrap();
            let before_multiplier = MIGRATIONS
                .iter()
                .position(|migration| migration.version == 15)
                .unwrap();
            store
                .migrate("astrocyte", &MIGRATIONS[..before_multiplier])
                .unwrap();
        }
        {
            let conn = rusqlite::Connection::open(&database).unwrap();
            conn.execute(
                "UPDATE desired_config SET generation=3, poll_interval_secs=45,
                     content_hash='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                 WHERE id=1",
                [],
            )
            .unwrap();
            conn.execute("UPDATE source_runtime SET desired_generation=(SELECT generation FROM desired_config WHERE id=1)", [])
                .unwrap();
        }
        let state = Arc::new(state::leased_test_state(&directory));
        assert_eq!(state.desired_generation(), 3);
        assert_eq!(state.desired_config().poll_interval_secs, 45);
        assert_eq!(
            state.desired_config().subscription_multiplier,
            config::MultiplierSetting::NotRecorded
        );
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);

        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(current_day_observations(&state).is_empty());
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0);

        publish_desired(&state, &directory);
        assert_eq!(state.desired_generation(), 4);
        stage.tick(&state, false, NOW);
        stage.tick(&state, false, NOW + 60_000);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + 120_000);
        // 1,000 tokens at $10 per million.
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 7);
    }

    #[test]
    fn stored_generations_lift_reasoning_billing_and_new_generations_keep_it() {
        for nested in [None, Some(serde_json::json!(["openai", "anthropic"]))] {
            let directory = tempfile::tempdir().unwrap();
            let mut old: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
            old.as_object_mut()
                .unwrap()
                .remove("reasoning_billed_as_output");
            if let Some(providers) = &nested {
                old["subscription_multiplier"]["reasoning_billed_as_output"] = providers.clone();
            }
            {
                let state = state::leased_test_state(&directory);
                state.ordered_write(|tx| {
                    tx.execute("UPDATE desired_config SET generation=3,canonical_bytes=?1,subscription_multiplier=?2 WHERE id=1",
                        rusqlite::params![old.to_string().as_bytes(), old["subscription_multiplier"].to_string()])
                        .map_err(|error| error.to_string())?;
                    tx.execute("UPDATE source_runtime SET desired_generation=3", []).map_err(|error| error.to_string())?;
                    Ok(((), true))
                }).unwrap();
            }
            let state = Arc::new(state::leased_test_state(&directory));
            let expected = if nested.is_some() {
                vec!["openai", "anthropic"]
            } else {
                vec![]
            };
            assert_eq!(state.desired_config().reasoning_billed_as_output, expected);
            assert!(matches!(
                state.desired_config().subscription_multiplier,
                config::MultiplierSetting::Recorded(_)
            ));
            publish_desired(&state, &directory);
            drop(state);
            let state = state::leased_test_state(&directory);
            assert_eq!(
                state.desired_config().reasoning_billed_as_output,
                ["openai"]
            );
            state
                .with_conn(|conn| {
                    let stored: String = conn
                        .query_row(
                            "SELECT subscription_multiplier FROM desired_config",
                            [],
                            |row| row.get(0),
                        )
                        .map_err(|error| error.to_string())?;
                    assert!(!stored.contains("reasoning_billed_as_output"));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn stored_generations_without_model_prefix_hydrate_as_null() {
        let directory = tempfile::tempdir().unwrap();
        let mut old: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
        old["subscription_multiplier"]["quota_providers"]["codex"]["scope"][0]
            .as_object_mut()
            .unwrap()
            .remove("model_prefix");
        {
            let state = state::leased_test_state(&directory);
            state.ordered_write(|tx| {
                tx.execute("UPDATE desired_config SET generation=3,canonical_bytes=?1,subscription_multiplier=?2 WHERE id=1",
                    rusqlite::params![old.to_string().as_bytes(), old["subscription_multiplier"].to_string()])
                    .map_err(|error| error.to_string())?;
                tx.execute("UPDATE source_runtime SET desired_generation=3", []).map_err(|error| error.to_string())?;
                Ok(((), true))
            }).unwrap();
        }
        let state = Arc::new(state::leased_test_state(&directory));
        let config::MultiplierSetting::Recorded(multiplier) =
            state.desired_config().subscription_multiplier
        else {
            panic!("multiplier expected")
        };
        assert_eq!(
            multiplier.quota_providers["codex"].scope[0].model_prefix,
            None
        );
        assert_eq!(state.desired_generation(), 3);
        publish_desired(&state, &directory);
        state
            .with_conn(|conn| {
                let stored: String = conn
                    .query_row(
                        "SELECT subscription_multiplier FROM desired_config",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?;
                let stored: serde_json::Value = serde_json::from_str(&stored).unwrap();
                assert_eq!(
                    stored["quota_providers"]["codex"]["scope"][0].get("model_prefix"),
                    Some(&serde_json::Value::Null)
                );
                Ok(())
            })
            .unwrap();
    }

    /// A day whose inputs cannot be read writes nothing and is retried on
    /// every later tick: the first tick after 00:00Z fails, a later one
    /// succeeds, and exactly one observation exists for the day.
    #[test]
    fn a_failed_day_is_retried_until_it_is_recorded_once() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(true);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        stage.tick(&state, false, NOW + 60_000);
        assert!(current_day_observations(&state).is_empty());
        assert_eq!(
            fusiform.plan_calls.load(Ordering::SeqCst),
            14,
            "every tick retries each missing day"
        );
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 120_000);
        stage.tick(&state, false, NOW + 180_000);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
        assert_eq!(
            fusiform.plan_calls.load(Ordering::SeqCst),
            21,
            "recorded days are not recomputed"
        );
    }

    /// A failure is logged once per day and reason, and a failure only the
    /// configuration can cure once per reason whatever the day.
    #[test]
    fn a_failed_day_is_logged_once_per_day_and_reason() {
        let mut stage = MultiplierStage::new(None);
        let failure = |reason: &str, needs_config| DayFailure {
            reason: reason.into(),
            needs_config,
        };
        assert!(stage.log_failure("codex", "d1", failure("unavailable", false)));
        assert!(!stage.log_failure("codex", "d1", failure("unavailable", false)));
        assert!(stage.log_failure("codex", "d1", failure("refused", false)));
        assert!(stage.log_failure("codex", "d2", failure("refused", false)));
        assert!(!stage.log_failure("codex", "d1", failure("unavailable", false)));
        assert!(!stage.log_failure("codex", "d1", failure("refused", false)));
        assert!(stage.log_failure("codex", "d2", failure("no catalog", true)));
        assert!(
            !stage.log_failure("codex", "d3", failure("no catalog", true)),
            "a configuration-only failure is logged once per reason, not per day"
        );
        assert!(stage.log_failure("codex", "d3", failure("no plan prices", true)));
    }

    /// Records today's observation for every quota provider in the production
    /// config into a COPY of the live store (`MULTIPLIER_REHEARSAL_DB`, already
    /// migrated), through the real fusiform client on the daemon connection
    /// file `MULTIPLIER_REHEARSAL_SUBC`: the same read, resolve and append steps
    /// the daily stage runs. Both fusiform calls are reads. Ignored by default:
    /// it needs the copy and a live daemon.
    #[test]
    #[ignore]
    fn live_rehearsal_on_a_store_copy() {
        let database = std::env::var("MULTIPLIER_REHEARSAL_DB").expect("MULTIPLIER_REHEARSAL_DB");
        let subc = std::env::var("MULTIPLIER_REHEARSAL_SUBC").expect("MULTIPLIER_REHEARSAL_SUBC");
        let desired =
            config::decode(include_str!("../../../docs/prod-config/astrocyte-v2.jsonc")).unwrap();
        let config::MultiplierSetting::Recorded(config) = desired.config.subscription_multiplier
        else {
            unreachable!("a decoded document records the member");
        };
        let client = Arc::new(crate::fusiform_client::FusiformClient::new(subc).unwrap());
        let connection: Arc<dyn CatalogConnection> = client.clone();
        let catalog = CatalogClient::connect(connection).unwrap();
        let mut conn = rusqlite::Connection::open(database).unwrap();
        let now = current_time_ms();
        let end = now - now.rem_euclid(DAY_MS);
        for quota_provider in config.quota_providers.keys() {
            let plan = multiplier::plan_observation(
                &conn,
                &config,
                quota_provider,
                end - MULTIPLIER_INTERVAL_MS,
                end,
            )
            .unwrap();
            let rates = multiplier::resolve_rates(
                &catalog,
                &multiplier::catalog_models(&config, &plan),
                now,
            )
            .unwrap();
            let plans =
                multiplier::resolve_plans(Some(client.as_ref()), &config, &plan.provider, now)
                    .unwrap();
            let observation = multiplier::compute_observation(
                &conn,
                &config,
                &plan,
                &rates,
                &plans,
                now,
                &["openai".into()],
            )
            .unwrap();
            let tx = conn.transaction().unwrap();
            multiplier::insert_observation_in_tx(&tx, &observation).unwrap();
            tx.commit().unwrap();
            println!(
                "{quota_provider} {} delta% {} subscription ${:.2} fee ${:.2} api ${:.2} multiplier {:?} fee_multiplier {:?} excluded {} unpriced {} outside {} untested {} low_res {}",
                observation.observation_day,
                observation.delta_percent,
                observation.subscription_nanos as f64 / 1e9,
                observation.fee_paid_nanos as f64 / 1e9,
                observation.api_nanos as f64 / 1e9,
                observation.multiplier,
                observation.fee_multiplier,
                serde_json::to_string(&observation.accounts_excluded).unwrap(),
                serde_json::to_string(&observation.unpriced_tokens).unwrap(),
                observation.tokens_outside_quota_coverage,
                observation.base_rate_tier_untested_tokens,
                observation.low_resolution,
            );
            for price in &observation.plan_prices {
                println!(
                    "  plan {} {} {:?} {:?} overdue {}",
                    price.account_ref,
                    price.plan,
                    price.source,
                    price.monthly_nanos,
                    price.plan_price_overdue
                );
            }
        }
    }

    /// While fusiform's history boundary is still being discovered the day
    /// waits for it instead of failing, and is recorded once it is known.
    #[test]
    fn a_pending_catalog_defers_the_day_instead_of_failing_it() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        seed_usage(&state);
        let fusiform = FakeFusiform::new(false);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, true, NOW);
        assert!(current_day_observations(&state).is_empty());
        install_catalog(&state, &fusiform);
        stage.tick(&state, false, NOW + 60_000);
        assert_eq!(current_day_observations(&state).len(), 1);
    }

    const DAY: &str = "2024-10-04";

    /// A catalog serving a rate key this decoder does not know cannot be
    /// read, so the day fails as an outage does: no row, the reason logged
    /// once for the day, and the row written on a later tick once the
    /// catalog reads cleanly.
    #[test]
    fn an_unreadable_catalog_writes_no_row_until_it_reads_cleanly() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        fusiform.unrecognized_key.store(true, Ordering::SeqCst);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(
            current_day_observations(&state).is_empty(),
            "an unreadable day writes nothing"
        );
        let (_, logged_day, reason) = stage
            .failure_logged
            .iter()
            .find(|(provider, day, _)| provider == "codex" && day.as_deref() == Some(DAY))
            .unwrap()
            .clone();
        assert_eq!(logged_day.as_deref(), Some(DAY));
        assert!(
            reason.starts_with("rate_fact_unrecognized: ")
                && reason.contains("rate.input.above_context.272k"),
            "{reason}"
        );
        stage.tick(&state, false, NOW + 60_000);
        assert!(
            current_day_observations(&state).is_empty(),
            "a retry of the same catalog fails again"
        );
        assert!(
            !stage.log_failure(
                "codex",
                DAY,
                DayFailure {
                    reason: reason.clone(),
                    needs_config: false,
                }
            ),
            "the same day and reason is already logged"
        );
        fusiform.unrecognized_key.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 120_000);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), DAY.to_string(), 10_000_000)]
        );
    }

    /// A model the catalog has no row for is something the catalog says, not
    /// a failure to read it: the row is written, its tokens counted unpriced.
    #[test]
    fn no_catalog_coverage_still_writes_the_row_with_unpriced_tokens() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        fusiform.uncovered.store(true, Ordering::SeqCst);
        seed_usage(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), DAY.to_string(), 0)]
        );
        let unpriced: serde_json::Value = state
            .with_read_op(|conn| {
                conn.query_row(
                    "SELECT unpriced_tokens FROM multiplier_observation WHERE observation_day=?1",
                    [DAY],
                    |row| row.get::<_, String>(0),
                )
                .map(|text| serde_json::from_str(&text).unwrap())
                .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap();
        assert_eq!(
            unpriced,
            json!([{"reason":"no_catalog_coverage","model":"opencode/openai/m","role":"input","tokens":1000,"measurements":1}])
        );
        let report = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        let head = &report["latest"][0];
        assert!(head["subscription_nanos"].as_i64().unwrap() > 0);
        assert!(head["fee_paid_nanos"].as_i64().unwrap() > 0);
        assert_eq!(head["multiplier"], serde_json::Value::Null);
        assert_eq!(head["fee_multiplier"], serde_json::Value::Null);
        assert_eq!(head["unpriced_tokens"], unpriced);

        let originals = all_rows(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + 60_000);
        assert_eq!(
            all_rows(&state),
            originals,
            "an unmeasurable day is complete"
        );
        let outcome = recompute(&state, &fusiform, DAY, None, NOW + 120_000).unwrap();
        assert_eq!(
            outcome["recomputed"][0]["multiplier"],
            serde_json::Value::Null
        );
        assert_eq!(
            outcome["recomputed"][0]["fee_multiplier"],
            serde_json::Value::Null
        );
        let corrected = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        assert_eq!(corrected["series"].as_array().unwrap().len(), 1);
        assert_eq!(corrected["series"][0]["api_nanos"], 0);
        assert_eq!(
            corrected["series"][0]["multiplier"],
            serde_json::Value::Null
        );
        assert_eq!(
            corrected["series"][0]["fee_multiplier"],
            serde_json::Value::Null
        );
        assert_eq!(corrected["series"][0]["unpriced_tokens"], unpriced);
        let pace = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_PACE,
            json!({"quota_provider":"codex", "from_ms":DAY_START - DAY_MS, "to_ms":DAY_START}),
        )
        .unwrap();
        assert_eq!(pace["per_point_nanos"], serde_json::Value::Null);
    }

    fn recompute(
        state: &SharedState,
        fusiform: &Arc<FakeFusiform>,
        day: &str,
        quota_provider: Option<&str>,
        now_ms: i64,
    ) -> Result<serde_json::Value, (String, String)> {
        recompute_multiplier_day(
            state,
            Some(fusiform.as_ref()),
            &multiplier::MultiplierRecomputeRequest {
                observation_day: day.into(),
                quota_provider: quota_provider.map(str::to_string),
            },
            now_ms,
        )
    }

    /// Every multiplier row, all columns, to prove a refusal changed nothing.
    fn all_rows(state: &SharedState) -> Vec<Vec<rusqlite::types::Value>> {
        state
            .with_read_op(|conn| {
                let mut statement = conn
                    .prepare("SELECT * FROM multiplier_observation ORDER BY observation_id")
                    .unwrap();
                let count = statement.column_count();
                let rows = statement
                    .query_map([], |row| {
                        (0..count)
                            .map(|index| row.get::<_, rusqlite::types::Value>(index))
                            .collect()
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                Ok(rows)
            })
            .unwrap()
    }

    /// A recompute appends a row superseding the day's head, priced with the
    /// catalog as it is now. The report then serves that row in the day's
    /// slot, and the daily stage does not add another row for the day.
    #[test]
    fn a_recompute_supersedes_the_day_and_the_report_serves_it() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
        let original_id = all_rows(&state).last().unwrap()[0].clone();

        fusiform
            .input_rate_units
            .store(20_000_000_000, Ordering::SeqCst);
        let later = NOW + 3 * DAY_MS;
        let outcome = recompute(&state, &fusiform, DAY, None, later).unwrap();
        assert_eq!(outcome["recomputed"].as_array().unwrap().len(), 1);
        assert_eq!(
            current_day_observations(&state),
            [
                ("codex".to_string(), DAY.to_string(), 10_000_000),
                ("codex".to_string(), DAY.to_string(), 20_000_000),
            ]
        );
        let rusqlite::types::Value::Integer(original_id) = original_id else {
            panic!("observation_id is an integer");
        };
        assert_eq!(outcome["recomputed"][0]["supersedes"], original_id);

        let report = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        assert_eq!(report["latest"].as_array().unwrap().len(), 1);
        assert_eq!(report["latest"][0]["api_nanos"], 20_000_000);
        assert_eq!(report["latest"][0]["supersedes"], original_id);
        assert_eq!(report["latest"][0]["interval_end_ms"], DAY_START);
        assert_eq!(report["latest"][0]["snapshot_at_ms"], later);
        let series = report["series"].as_array().unwrap();
        assert_eq!(series.len(), 1, "one row per provider and day: {series:?}");
        assert_eq!(series[0]["api_nanos"], 20_000_000);

        // The daily stage sees the day as recorded, corrected or not.
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW + 60_000);
        assert_eq!(current_day_observations(&state).len(), 2);

        // A second recompute supersedes the correction, not the original.
        fusiform
            .input_rate_units
            .store(30_000_000_000, Ordering::SeqCst);
        let again = recompute(&state, &fusiform, DAY, Some("codex"), later + 1).unwrap();
        assert_eq!(
            again["recomputed"][0]["supersedes"],
            outcome["recomputed"][0]["observation_id"]
        );
    }

    /// A recompute must not undercount an excluded producer's tokens. Even
    /// an all-provider request checks every scope before calling the catalog.
    #[test]
    fn served_recompute_refuses_excluded_scope_without_calls_or_rows() {
        for quota_provider in [None, Some("z-excluded")] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            let mut document: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
            document["subscription_multiplier"]["plan_price_overrides"]["openai/pro_20x"] =
                json!({"minor_units": 20000, "exponent": 2, "currency": "USD", "period": "month"});
            let mut excluded =
                document["subscription_multiplier"]["quota_providers"]["codex"].clone();
            excluded["scope"][0]["producer"] = json!("broca");
            document["subscription_multiplier"]["quota_providers"]["z-excluded"] = excluded;
            document["harness_ingest"]["exclude"] = json!(["broca"]);
            publish_document(&state, &directory, &document.to_string());
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            seed_usage(&state);
            let calls_before = fusiform.catalog_calls.load(Ordering::SeqCst);
            assert!(all_rows(&state).is_empty());

            let mut params = json!({"observation_day": DAY});
            if let Some(provider) = quota_provider {
                params["quota_provider"] = json!(provider);
            }
            let (code, message) =
                crate::ops::dispatch(&state, crate::ops::OP_MULTIPLIER_RECOMPUTE, params)
                    .unwrap_err();
            assert_eq!(code, "scoped_producer_not_ingested", "{message}");
            assert!(
                message.contains("broca") && message.contains("z-excluded"),
                "{message}"
            );
            assert!(all_rows(&state).is_empty(), "no provider may write a row");
            assert_eq!(fusiform.catalog_calls.load(Ordering::SeqCst), calls_before);
            assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0);

            // The excluded scope must not block a request for an ingested one.
            let reply = crate::ops::dispatch(
                &state,
                crate::ops::OP_MULTIPLIER_RECOMPUTE,
                json!({"observation_day": DAY, "quota_provider": "codex"}),
            )
            .unwrap();
            assert_eq!(reply["recomputed"].as_array().unwrap().len(), 1);
            assert_eq!(reply["recomputed"][0]["quota_provider"], "codex");
            assert_eq!(all_rows(&state).len(), 1);
        }
    }

    /// Each refusal writes nothing: a malformed day, an unknown provider,
    /// and a computation that fails because the catalog still cannot be read.
    #[test]
    fn a_refused_recompute_leaves_the_store_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
        let before = all_rows(&state);
        assert_eq!(before.len(), 7);

        let code = |result: Result<serde_json::Value, (String, String)>| result.unwrap_err().0;
        assert_eq!(
            code(recompute(&state, &fusiform, "2024-10-4", None, NOW)),
            "malformed"
        );
        assert_eq!(
            code(recompute(&state, &fusiform, DAY, Some("claude"), NOW)),
            "unknown_quota_provider"
        );
        fusiform.unrecognized_key.store(true, Ordering::SeqCst);
        assert_eq!(
            code(recompute(&state, &fusiform, DAY, None, NOW)),
            "rate_fact_unrecognized"
        );
        assert_eq!(all_rows(&state), before);

        // Dispatch preserves unknown-provider refusal and rejects an operator
        // identity supplied by the caller.
        for (params, expected) in [
            (
                json!({"observation_day": "2024-10-05", "quota_provider": "unknown"}),
                "unknown_quota_provider",
            ),
            (
                json!({"observation_day": DAY, "operator": "someone"}),
                "malformed",
            ),
        ] {
            let (code, message) =
                crate::ops::dispatch(&state, crate::ops::OP_MULTIPLIER_RECOMPUTE, params.clone())
                    .unwrap_err();
            assert_eq!(code, expected, "{params}: {message}");
        }
        assert_eq!(all_rows(&state), before);
    }
}

#[cfg(test)]
mod harness_ingest_tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    use astrocyte_core::store::MIGRATIONS;

    use super::*;
    use crate::state::test_state;

    const INGEST_ON: &str = r#"{"enabled":true,"exclude":[]}"#;

    /// Every local reader's source key, in drain order.
    const READER_KEYS: [&str; 8] = [
        "opencode",
        "pi",
        "claude-code",
        "codex",
        "gemini",
        "goose",
        "omp",
        "kimi",
    ];

    /// Every local reader, each pointed at a path that fails if it is read: a
    /// regular file where a directory or a database is expected. A reader
    /// that is skipped leaves no trace; one that reads records the failure.
    struct Readers {
        opencode: opencode_source::OpenCodeSource,
        pi: pi_source::PiSource,
        claude_code: claude_code_source::ClaudeCodeSource,
        codex: codex_source::CodexSource,
        harnesses: Vec<Box<dyn harness_drain::HarnessSource>>,
    }

    fn readers_that_fail_if_read(directory: &tempfile::TempDir) -> Readers {
        let file = directory.path().join("not-a-store");
        std::fs::write(&file, b"not a store").unwrap();
        // Codex looks for `sessions` below its home. Below a regular file,
        // Unix answers "not a directory" but Windows answers "not found",
        // which reads as an uninstalled harness. A real home whose `sessions`
        // is a file fails the directory read on every OS.
        let codex_home = directory.path().join("codex-home");
        std::fs::create_dir(&codex_home).unwrap();
        std::fs::write(codex_home.join("sessions"), b"not a directory").unwrap();
        Readers {
            opencode: opencode_source::OpenCodeSource::at(&file),
            pi: pi_source::PiSource::at(&file),
            claude_code: claude_code_source::ClaudeCodeSource::at(&file),
            codex: codex_source::CodexSource::at(&codex_home),
            harnesses: vec![
                Box::new(gemini_source::GeminiSource::at(&file)),
                Box::new(goose_source::GooseSource::at(&file)),
                Box::new(omp_source::OmpSource::at(&file)),
                Box::new(kimi_source::KimiSource::at(&file)),
            ],
        }
    }

    /// One refresher tick over `readers` and `producers`.
    fn tick(state: &SharedState, readers: &Readers, producers: &[Producer]) {
        let harnesses: Vec<&dyn harness_drain::HarnessSource> = readers
            .harnesses
            .iter()
            .map(|source| source.as_ref())
            .collect();
        run_iteration_all(
            state,
            producers,
            Some(&readers.opencode),
            Some(&readers.pi),
            Some(&readers.claude_code),
            Some(&readers.codex),
            &harnesses,
            None,
            false,
            &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
            &AtomicBool::new(false),
        );
    }

    /// Publish a desired document with this `harness_ingest` member through
    /// the production reload path.
    fn publish(state: &Arc<SharedState>, directory: &tempfile::TempDir, ingest: &str) {
        let path = directory.path().join("astrocyte.jsonc");
        std::fs::write(
            &path,
            format!(
                r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{{"api_twins":{{}},"plan_price_overrides":{{}},"quota_providers":{{}}}},"harness_ingest":{ingest}}}"#
            ),
        )
        .unwrap();
        reload_config(state, Ok(path)).unwrap();
    }

    fn runtime_keys(state: &SharedState) -> Vec<String> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT source_key FROM source_runtime ORDER BY source_key")
                    .unwrap();
                let keys = statement
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<String>>>()
                    .unwrap();
                Ok(keys)
            })
            .unwrap()
    }

    fn count(state: &SharedState, table: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// A BROCA stand-in that serves the wire fixture's one fact from the
    /// start of its lineage and records every cursor it was asked from.
    #[derive(Default)]
    struct RecordingBroca {
        pulls: AtomicUsize,
        since: Mutex<Vec<Option<SourceCursor>>>,
    }

    impl SourceClient for RecordingBroca {
        fn pull(&self, since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            self.since.lock().unwrap().push(since);
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            let position = SourceCursor {
                lineage: "instance:1".into(),
                ordinal: 7,
            };
            Ok(ExportPage {
                model_steps: Vec::new(),
                set_aside: Vec::new(),
                cursor: position.clone(),
                high_water: position,
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    /// An excluded reader's store is never opened, its runtime row is
    /// removed, and health reads ok. Re-enabling it reads it again.
    #[test]
    fn an_excluded_reader_is_never_read_and_its_runtime_row_is_removed() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(test_state());
        let readers = readers_that_fail_if_read(&directory);
        let pi_only = |state: &SharedState| {
            run_iteration_with_pi(state, &[], &readers.pi, &AtomicBool::new(false))
        };
        // The migrations seed a BROCA runtime row, and this test drains only
        // Pi, so that row would stay cold and degrade health. Removing it
        // lets health speak only for Pi.
        harness_drain::remove_runtime_row(&state, "broca").unwrap();

        // Control: with Pi on, the drain reads the path and fails on it.
        publish(&state, &directory, INGEST_ON);
        pi_only(&state);
        let reason = state.producer_degraded_reason("pi").expect("Pi was read");
        assert!(reason.contains("read Pi session root"), "{reason}");
        assert!(runtime_keys(&state).contains(&"pi".to_string()));

        publish(&state, &directory, r#"{"enabled":true,"exclude":["pi"]}"#);
        pi_only(&state);
        assert_eq!(state.producer_degraded_reason("pi"), None);
        assert!(!runtime_keys(&state).contains(&"pi".to_string()));
        let health = state.public_projection().health(current_time_ms());
        assert_eq!(health.status, "ok", "{:?}", health.degraded_reason);

        // Re-enabled, it reads its path from scratch again.
        publish(&state, &directory, INGEST_ON);
        pi_only(&state);
        assert!(state.producer_degraded_reason("pi").is_some());
        assert!(runtime_keys(&state).contains(&"pi".to_string()));
    }

    /// With ingestion off no producer drains -- no reader opens its store,
    /// BROCA's included -- and every producer's runtime row is removed.
    #[test]
    fn ingestion_off_drains_no_producer_and_removes_every_runtime_row() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(test_state());
        let readers = readers_that_fail_if_read(&directory);
        let broca = Arc::new(RecordingBroca::default());
        let producers = [Producer {
            source_key: "broca".into(),
            client: broca.clone(),
        }];

        // Control: every reader reads its path and fails, BROCA is read
        // once, and each has a runtime row.
        publish(&state, &directory, INGEST_ON);
        tick(&state, &readers, &producers);
        for key in READER_KEYS {
            assert!(state.producer_degraded_reason(key).is_some(), "{key}");
        }
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        let mut every_key: Vec<String> = READER_KEYS
            .iter()
            .chain(["broca"].iter())
            .map(|key| key.to_string())
            .collect();
        every_key.sort();
        assert_eq!(runtime_keys(&state), every_key);

        publish(&state, &directory, r#"{"enabled":false,"exclude":[]}"#);
        tick(&state, &readers, &producers);
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        assert!(runtime_keys(&state).is_empty());
        for key in every_key {
            assert_eq!(state.producer_degraded_reason(&key), None, "{key}");
        }
        let health = state.public_projection().health(current_time_ms());
        assert_eq!(health.status, "ok", "{:?}", health.degraded_reason);
        // The event BROCA recorded before stays.
        assert_eq!(count(&state, "usage_event"), 1);
    }

    /// An excluded BROCA's store is never opened: the real reader, pointed at
    /// a file that fails if read, records no failure, and no BROCA runtime
    /// row is left. The control run shows the same reader does fail on it.
    #[test]
    fn an_excluded_broca_never_opens_its_store() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-store");
        std::fs::write(&file, b"not a store").unwrap();
        let producers = [Producer {
            source_key: "broca".into(),
            client: Arc::new(broca_source::BrocaSource::at(
                &file,
                Arc::new(AtomicBool::new(false)),
            )),
        }];
        let state = Arc::new(test_state());

        publish(&state, &directory, INGEST_ON);
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert!(state.producer_degraded_reason("broca").is_some());

        publish(
            &state,
            &directory,
            r#"{"enabled":true,"exclude":["broca"]}"#,
        );
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert!(!runtime_keys(&state).contains(&"broca".to_string()));
        assert_eq!(state.producer_degraded_reason("broca"), None);
    }

    /// Excluding BROCA removes its runtime row and with it the cursor.
    /// Re-enabled, it is read from the start of its store; the fact it holds
    /// carries the same content-derived event id, so nothing is recorded
    /// twice and nothing is quarantined.
    #[test]
    fn re_enabled_broca_repulls_from_the_start_without_duplicates_or_quarantine() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(test_state());
        let broca = Arc::new(RecordingBroca::default());
        let producers = [Producer {
            source_key: "broca".into(),
            client: broca.clone(),
        }];
        let consumed = |state: &SharedState| -> Option<i64> {
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='broca'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap()
        };

        publish(&state, &directory, INGEST_ON);
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert_eq!(consumed(&state), Some(7));
        let events = count(&state, "usage_event");
        let measurements = count(&state, "measurement");
        assert_eq!(events, 1);

        publish(
            &state,
            &directory,
            r#"{"enabled":true,"exclude":["broca"]}"#,
        );
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        assert!(!runtime_keys(&state).contains(&"broca".to_string()));

        publish(&state, &directory, INGEST_ON);
        run_iteration(&state, &producers, &AtomicBool::new(false));
        let since = broca.since.lock().unwrap().clone();
        assert_eq!(
            since,
            [None, None],
            "re-enabled BROCA is read from the start"
        );
        assert_eq!(consumed(&state), Some(7));
        assert_eq!(count(&state, "usage_event"), events);
        assert_eq!(count(&state, "measurement"), measurements);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
        assert_eq!(state.producer_degraded_reason("broca"), None);
    }

    /// A generation committed before the store could hold `harness_ingest`
    /// hydrates as all on, never as off: every reader is read, BROCA's
    /// included, exactly as before the member existed.
    #[test]
    fn a_generation_without_the_member_drains_every_producer() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("ledger.db");
        {
            let store = open_sqlite(&StorageDescriptor {
                module_id: "ck-astrocyte-leased-test".to_string(),
                storage_namespace: "astrocyte".to_string(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: database.to_string_lossy().into_owned(),
                },
            })
            .unwrap();
            let before_ingest = MIGRATIONS
                .iter()
                .position(|migration| migration.version == 17)
                .unwrap();
            store
                .migrate("astrocyte", &MIGRATIONS[..before_ingest])
                .unwrap();
        }
        {
            let conn = rusqlite::Connection::open(&database).unwrap();
            conn.execute(
                "UPDATE desired_config SET generation=3, poll_interval_secs=45,
                     subscription_multiplier='{\"api_twins\":{},\"plan_price_overrides\":{},\"quota_providers\":{},\"reasoning_billed_as_output\":[]}',
                     content_hash='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                 WHERE id=1",
                [],
            )
            .unwrap();
            conn.execute("UPDATE source_runtime SET desired_generation=(SELECT generation FROM desired_config WHERE id=1)", [])
                .unwrap();
        }
        let state = state::leased_test_state(&directory);
        assert_eq!(state.desired_generation(), 3);
        assert_eq!(
            state.desired_config().harness_ingest,
            config::HarnessIngest::all_on()
        );
        let readers = readers_that_fail_if_read(&directory);
        let broca = Arc::new(RecordingBroca::default());
        tick(
            &state,
            &readers,
            &[Producer {
                source_key: "broca".into(),
                client: broca.clone(),
            }],
        );
        for key in READER_KEYS {
            assert!(
                state.producer_degraded_reason(key).is_some(),
                "{key} was read"
            );
        }
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        assert_eq!(count(&state, "usage_event"), 1);
    }

    /// Runs this binary's `MIGRATIONS` over a COPY of the live store, whose
    /// path is in `INGEST_REHEARSAL_DB`, and hydrates it as startup does: the
    /// committed generation, written before `harness_ingest` existed, keeps
    /// its number and comes back all on, and every runtime row survives.
    /// Ignored by default: it needs the copy.
    #[test]
    #[ignore]
    fn ingest_rehearsal_on_a_store_copy() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let database = std::env::var("INGEST_REHEARSAL_DB").expect("INGEST_REHEARSAL_DB");
        let before: (i64, i64) = {
            let conn = rusqlite::Connection::open(&database).unwrap();
            conn.query_row(
                "SELECT (SELECT generation FROM desired_config WHERE id=1),
                        (SELECT COUNT(*) FROM source_runtime)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
        };
        let store = open_sqlite(&StorageDescriptor {
            module_id: "ck-astrocyte-rehearsal".to_string(),
            storage_namespace: "astrocyte".to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite { path: database },
        })
        .unwrap();
        astrocyte_core::store::migrate_store(&store).unwrap();
        let state = SharedState::from_store(store).unwrap();
        assert_eq!(state.desired_generation(), u64::try_from(before.0).unwrap());
        assert_eq!(
            state.desired_config().harness_ingest,
            config::HarnessIngest::all_on()
        );
        assert_eq!(
            runtime_keys(&state).len(),
            usize::try_from(before.1).unwrap()
        );
        println!(
            "generation {} runtime rows {} harness_ingest {:?}",
            before.0,
            before.1,
            state.desired_config().harness_ingest
        );
    }
}

#[cfg(test)]
mod discovery_logging_tests {
    use std::process::Command;

    const CHILD_TEST_ENV: &str = "ASTROCYTE_DISCOVERY_LOG_TEST";
    const TEST_NAME: &str = "store_presence_logs_only_when_it_changes";

    #[test]
    fn set_aside_evidence_is_atomic_and_warns_once() {
        const NAME: &str = "set_aside_evidence_is_atomic_and_warns_once";
        if std::env::var(CHILD_TEST_ENV).as_deref() == Ok(NAME) {
            let handle = cortexkit_log::init_from_env().unwrap();
            let state = crate::state::test_state();
            let evidence = [super::SetAsideRecord {
                locator: "fixture.jsonl:line:2".into(),
                reason: "missing identity".into(),
            }];
            let rollback: Result<(), String> = state.ordered_write(|tx| {
                super::harness_drain::record_set_aside_in_tx(tx, "pi", &evidence, 50)?;
                Err("forced rollback".into())
            });
            assert!(rollback.is_err());
            for now in [100, 200] {
                let newly_seen = state
                    .ordered_write(|tx| {
                        let newly_seen =
                            super::harness_drain::record_set_aside_in_tx(tx, "pi", &evidence, now)?;
                        Ok((newly_seen, true))
                    })
                    .unwrap();
                assert_eq!(newly_seen.len(), usize::from(now == 100));
                super::harness_drain::warn_set_aside("pi", &newly_seen);
            }
            super::harness_drain::assert_set_aside(&state, "pi", 1);
            let times: (i64, i64) = state.with_conn(|conn| conn.query_row("SELECT first_seen_ms,last_seen_ms FROM set_aside_record WHERE source_key='pi'", [], |r| Ok((r.get(0)?,r.get(1)?))).map_err(|e| e.to_string())).unwrap();
            assert_eq!(times, (100, 200));
            let contents = std::fs::read_to_string(handle.path()).unwrap();
            let lines: Vec<_> = contents.lines().collect();
            assert_eq!(lines.len(), 1, "{contents}");
            assert!(matches!(
                cortexkit_log::parse_line(lines[0]).unwrap().level,
                cortexkit_log::ParsedLevel::Warn
            ));
            for field in ["pi", "fixture.jsonl:line:2", "missing identity"] {
                assert!(lines[0].contains(field), "{}", lines[0]);
            }
            return;
        }
        let home = tempfile::tempdir().unwrap();
        let output = crate::test_support::output_within(
            Command::new(std::env::current_exe().unwrap()).args(["--exact", "refresher::discovery_logging_tests::set_aside_evidence_is_atomic_and_warns_once", "--nocapture"])
                .env(CHILD_TEST_ENV, NAME).env("SUBC_MODULE_ID", "astrocyte").env("CK_LOG", "trace").env("XDG_DATA_HOME", home.path()),
            "set-aside warning test child",
        );
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn store_presence_logs_only_when_it_changes() {
        if std::env::var(CHILD_TEST_ENV).as_deref() == Ok(TEST_NAME) {
            let handle = cortexkit_log::init_from_env().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().join("pi-sessions");
            let source = super::pi_source::PiSource::at(root.clone());
            let state = crate::state::test_state();
            let drain = || {
                source
                    .drain(&state, 100, &std::sync::atomic::AtomicBool::new(false))
                    .unwrap();
            };

            drain();
            drain();
            std::fs::create_dir_all(&root).unwrap();
            drain();
            drain();
            std::fs::remove_dir_all(&root).unwrap();
            drain();
            drain();

            let contents = std::fs::read_to_string(handle.path()).unwrap();
            let lines: Vec<_> = contents.lines().collect();
            assert_eq!(lines.len(), 3, "{contents}");
            let expected = ["absent", "present", "absent"];
            for (line, outcome) in lines.iter().zip(expected) {
                let parsed = cortexkit_log::parse_line(line).unwrap();
                assert_eq!(parsed.logger, "astrocyte.drain", "{line}");
                assert!(
                    matches!(parsed.level, cortexkit_log::ParsedLevel::Info),
                    "{line}"
                );
                assert!(
                    line.contains("source=pi") || line.contains("source=\"pi\""),
                    "{line}"
                );
                let path = root.display().to_string();
                assert!(
                    line.contains(&format!("path={path}"))
                        || line.contains(&format!("path=\"{path}\"")),
                    "{line}"
                );
                assert!(
                    line.contains(&format!("outcome={outcome}"))
                        || line.contains(&format!("outcome=\"{outcome}\"")),
                    "{line}"
                );
            }
            return;
        }

        // Logger configuration is process-global, so each capture runs alone.
        let data_home = std::env::temp_dir().join(format!(
            "astrocyte-discovery-log-{}-{TEST_NAME}",
            std::process::id()
        ));
        std::fs::create_dir_all(&data_home).unwrap();
        let output = crate::test_support::output_within(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "refresher::discovery_logging_tests::store_presence_logs_only_when_it_changes",
                    "--nocapture",
                ])
                .env(CHILD_TEST_ENV, TEST_NAME)
                .env("SUBC_MODULE_ID", "astrocyte")
                .env("CK_LOG", "trace")
                .env("XDG_DATA_HOME", &data_home),
            "store discovery log test child",
        );
        std::fs::remove_dir_all(data_home).unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success()
                && stdout.contains("running 1 test")
                && stdout.contains(
                    "test refresher::discovery_logging_tests::store_presence_logs_only_when_it_changes ... ok"
                ),
            "{stdout}\n{stderr}"
        );
    }
}
