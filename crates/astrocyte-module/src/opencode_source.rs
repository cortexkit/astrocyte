//! OpenCode's on-disk SQLite producer and ledger ingest mapping.

use super::harness_drain::DrainMode;
use std::collections::BTreeMap;
#[cfg(any(test, feature = "test-support"))]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::{EventRecordRequest, RecordDisposition, StepDetail};
use astrocyte_readers::file_identity::{file_identity, same_identity};
use astrocyte_readers::sessions::opencode::{
    read_opencode_page, OpenCodeCursor, OpenCodeMessage, OpenCodeReadError, OpenCodeScanCost,
    OpenCodeTailPage, OpenCodeTailReader, OpenCodeTailRequest, OpenCodeTotals,
};
use rusqlite::params;

use crate::refresher::SourceDrainError;
use crate::state::SharedState;

pub(crate) const OPENCODE_SOURCE_KEY: &str = "opencode";
const PAGE_LIMIT: usize = super::harness_drain::PAGE_LIMIT as usize;

/// Where a reader of one OpenCode store keeps its durable cursor and records
/// its set-aside evidence. Shared by the OpenCode 1 and OpenCode 2 readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CursorHome {
    /// The user's own store: the cursor lives in the source's own
    /// `source_runtime` row, which the reader creates and removes with the store.
    Runtime(&'static str),
    /// A store another module declared: the cursor is one
    /// `declared_store_cursor` row. The declaring source owns the single
    /// `source_runtime` row for all its stores and records its own success once
    /// its whole pass has committed, so a read never touches that row.
    Declared {
        source_key: &'static str,
        store: String,
        layout: &'static str,
    },
}

impl CursorHome {
    /// The key set-aside evidence is filed under.
    pub(crate) fn source_key(&self) -> &'static str {
        match self {
            Self::Runtime(key)
            | Self::Declared {
                source_key: key, ..
            } => key,
        }
    }

    /// The `source_runtime` row this reader creates and removes itself.
    pub(crate) fn runtime_key(&self) -> Option<&'static str> {
        match self {
            Self::Runtime(key) => Some(key),
            Self::Declared { .. } => None,
        }
    }

    /// Whether the reader may keep its read-only connection between reads.
    /// A declared store is erased by its owner soon after its session ends,
    /// so its connection is closed after every read on every OS.
    pub(crate) fn retains_connection(&self) -> bool {
        astrocyte_readers::read_io::retain_harness_connections() && matches!(self, Self::Runtime(_))
    }

    pub(crate) fn read_cursor(
        &self,
        state: &SharedState,
    ) -> Result<Option<OpenCodeCursor>, String> {
        use rusqlite::OptionalExtension;
        let row = state.with_conn(|connection| {
            match self {
                Self::Runtime(key) => connection.query_row(
                    "SELECT lineage,consumed_ordinal FROM source_runtime WHERE source_key=?1",
                    [key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ),
                Self::Declared {
                    source_key,
                    store,
                    layout,
                } => connection.query_row(
                    "SELECT lineage,consumed_ordinal FROM declared_store_cursor
                     WHERE source_key=?1 AND store_path=?2 AND layout=?3",
                    params![source_key, store, layout],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ),
            }
            .optional()
            .map_err(|error| format!("read OpenCode durable cursor: {error}"))
        })?;
        Ok(
            row.and_then(|(lineage, ordinal): (Option<String>, Option<i64>)| {
                Some(OpenCodeCursor {
                    lineage: lineage?,
                    ordinal: ordinal?,
                })
            }),
        )
    }

    /// Record a committed read in `tx`: the new durable cursor after a full
    /// page, or only the success time after a tail read. Returns the rows
    /// changed, which is 1 unless the source's runtime row has disappeared.
    pub(crate) fn record_read_in_tx(
        &self,
        tx: &rusqlite::Transaction<'_>,
        cursor: Option<&PageCursor<'_>>,
        recorded_at_ms: i64,
    ) -> rusqlite::Result<usize> {
        match (self, cursor) {
            (Self::Runtime(key), Some(cursor)) => tx.execute(
                "UPDATE source_runtime
                 SET lineage=?2,consumed_ordinal=?3,high_water_ordinal=?4,
                     last_success_ms=?5,pending_known=0,pending_unknown=?6
                 WHERE source_key=?1",
                params![
                    key,
                    cursor.lineage,
                    cursor.consumed_ordinal,
                    cursor.high_water_ordinal,
                    recorded_at_ms,
                    i64::from(cursor.consumed_ordinal < cursor.high_water_ordinal),
                ],
            ),
            (Self::Runtime(key), None) => tx.execute(
                "UPDATE source_runtime SET last_success_ms=?2 WHERE source_key=?1",
                params![key, recorded_at_ms],
            ),
            (
                Self::Declared {
                    source_key,
                    store,
                    layout,
                },
                Some(cursor),
            ) => tx.execute(
                "INSERT INTO declared_store_cursor
                 (source_key,store_path,layout,lineage,consumed_ordinal,high_water_ordinal)
                 VALUES (?1,?2,?3,?4,?5,?6)
                 ON CONFLICT(source_key,store_path,layout) DO UPDATE SET
                     lineage=excluded.lineage,consumed_ordinal=excluded.consumed_ordinal,
                     high_water_ordinal=excluded.high_water_ordinal",
                params![
                    source_key,
                    store,
                    layout,
                    cursor.lineage,
                    cursor.consumed_ordinal,
                    cursor.high_water_ordinal,
                ],
            ),
            (Self::Declared { .. }, None) => Ok(1),
        }
    }
}

/// How one drain reads a store. The user's own store reads with the default:
/// the producer's agent name as work class, and the normal wait for calls that
/// have tokens but no completion.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReadOptions<'a> {
    /// Stamped as `work_class` on every call read, instead of the agent name.
    pub work_class: Option<&'a str>,
    /// No further write can come to the store, so an OpenCode 2 call with
    /// tokens but no completion is final now rather than after the quiet
    /// window. OpenCode 1 rows carry their tokens only once `finish` or
    /// `time.completed` is written, so its reader has nothing to wait for.
    pub settled: bool,
}

/// When each kind of read runs. See `OpenCodeSource` for the design these
/// numbers tune.
#[derive(Debug, Clone, Copy)]
struct OpenCodeCadence {
    /// A full pass runs at least this often, on the drain's `recorded_at_ms`
    /// clock. It bounds how late the tail's blind spots are caught.
    full_pass_interval_ms: i64,
    /// An open row is re-read by rowid until it has gone this long without an
    /// update. OpenCode leaves rows open forever when its process dies
    /// mid-turn; after this the full pass alone is responsible for them.
    open_row_ttl_ms: i64,
    /// The first tail read in a process starts this many rowids below the
    /// largest one, so rows written just before the process started, and
    /// rows still open then, are watched too.
    seed_rows: i64,
    /// At most this many open rows are re-read per tick. The oldest rowids
    /// are dropped first and left to the full pass.
    open_row_cap: usize,
}

/// Measured in production on 2026-09-23 with a 60 s refresher tick, reading
/// the whole 25 GB, 1.28 million-message store every tick cost about 0.7 s of
/// CPU (release build) and 21.8 million SQLite VM steps, because
/// `time_updated` has no index. A full pass every hour keeps that cost to
/// about 0.02% of a core while bounding the tail's blind spots to an hour.
const PRODUCTION_CADENCE: OpenCodeCadence = OpenCodeCadence {
    full_pass_interval_ms: 60 * 60 * 1000,
    open_row_ttl_ms: 24 * 60 * 60 * 1000,
    seed_rows: 2_000,
    open_row_cap: 4_096,
};

/// How a drain read OpenCode's database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenCodePass {
    /// New rows and open rows were read by rowid. The durable cursor did not
    /// move.
    Tail,
    /// One page was read from the durable `time_updated` cursor.
    Full,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OpenCodeDrainOutcome {
    /// How the database was read; `None` when it was not read at all.
    pub pass: Option<OpenCodePass>,
    pub root_absent: bool,
    pub raw: OpenCodeTotals,
    pub raw_after_cursor: OpenCodeTotals,
    pub emitted: OpenCodeTotals,
    pub emitted_after_cursor: OpenCodeTotals,
    pub rows_recorded: usize,
    pub exact_redeliveries: usize,
    pub quarantined_redeliveries: usize,
    pub consumed_ordinal: Option<i64>,
    pub high_water_ordinal: Option<i64>,
    /// SQLite work this drain did against OpenCode's database.
    pub cost: OpenCodeScanCost,
}

#[derive(Debug, Clone)]
enum OpenCodeRoot {
    Path(PathBuf),
    Unresolved(String),
}

/// The database file's identity and size. This is only a hint that the store
/// was replaced: it can fire early, but it can also stay silent, because a
/// file system may give a recreated file the inode number just freed and the
/// same size. The front-row fingerprint read from the content is what a
/// replacement is actually detected by; this only adds a trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileSignature {
    identity: Option<(u64, u64)>,
    db_len: u64,
}

impl FileSignature {
    fn capture(database: &fs::File, metadata: &fs::Metadata) -> Self {
        Self {
            identity: file_identity(database, metadata),
            db_len: metadata.len(),
        }
    }

    /// A different file now sits at the path, or the file got smaller. Either
    /// can change which row is oldest and can renumber rowids.
    fn replaces(&self, previous: &Self) -> bool {
        !same_identity(self.identity, previous.identity) || self.db_len < previous.db_len
    }
}

/// Process-local read state. None of it is durable: a restart starts with a
/// full pass from the durable cursor, which does not depend on any of this.
#[derive(Debug, Default)]
struct OpenCodeCache {
    reader: Option<OpenCodeTailReader>,
    /// File identity and size at the last completed drain.
    signature: Option<FileSignature>,
    /// The front-row fingerprint read in the last full pass's snapshot.
    /// `Some(None)` means that store's `message` table was empty.
    front_row_id: Option<Option<String>>,
    /// When the last full pass that reached high water ran.
    last_full_pass_ms: Option<i64>,
    /// The last full pass stopped at its page limit below high water.
    full_backlog: bool,
    /// Every new row up to this rowid has been examined by a tail read.
    /// `None` until the first tail read in this process.
    tail_after_rowid: Option<i64>,
    /// Open rows to re-read: rowid to the `time_updated` last seen.
    open_rows: BTreeMap<i64, i64>,
}

impl OpenCodeCache {
    fn choose(
        &self,
        signature: &FileSignature,
        now_ms: i64,
        cadence: &OpenCodeCadence,
    ) -> OpenCodePass {
        let Some(last_full_pass_ms) = self.last_full_pass_ms else {
            return OpenCodePass::Full;
        };
        let replaced = self
            .signature
            .is_some_and(|previous| signature.replaces(&previous));
        // A clock that went backwards also makes a full pass due, rather
        // than postponing it until the clock passes the old time again.
        let due = now_ms < last_full_pass_ms
            || now_ms.saturating_sub(last_full_pass_ms) >= cadence.full_pass_interval_ms;
        if self.full_backlog || replaced || due {
            OpenCodePass::Full
        } else {
            OpenCodePass::Tail
        }
    }

    /// Whether a tail read shows its rowid position no longer describes the
    /// store, so the tail's rows must be dropped and a full pass run instead.
    fn tail_is_stale(&self, page: &OpenCodeTailPage) -> bool {
        // A different row at the smallest rowid: another store now sits at
        // the path, or rows were deleted from the front. Either can change
        // the lineage, which only a full pass works out.
        self.front_row_id.as_ref() != Some(&page.front_row_id)
            // Rowids went backwards: rows at the end were deleted, or the
            // table was renumbered (a VACUUM may renumber a table without an
            // INTEGER PRIMARY KEY). New rows may sit at rowids already passed.
            || page.max_rowid < page.start_rowid
    }

    fn forget_tail(&mut self) {
        self.tail_after_rowid = None;
        self.open_rows.clear();
    }

    fn after_full_pass(
        &mut self,
        signature: FileSignature,
        front_row_id: Option<String>,
        consumed_ordinal: i64,
        high_water_ordinal: i64,
        now_ms: i64,
    ) {
        let replaced = self
            .signature
            .is_some_and(|previous| signature.replaces(&previous));
        if replaced || self.front_row_id.as_ref() != Some(&front_row_id) {
            // The tail's rowids referred to the store that was there before.
            self.forget_tail();
            self.reader = None;
        }
        self.front_row_id = Some(front_row_id);
        self.full_backlog = consumed_ordinal < high_water_ordinal;
        if !self.full_backlog {
            self.last_full_pass_ms = Some(now_ms);
        }
        self.signature = Some(signature);
    }

    fn after_tail(
        &mut self,
        signature: FileSignature,
        page: &OpenCodeTailPage,
        reread: &[i64],
        now_ms: i64,
        cadence: &OpenCodeCadence,
    ) {
        self.signature = Some(signature);
        self.tail_after_rowid = Some(page.scanned_through_rowid);
        for rowid in reread {
            self.open_rows.remove(rowid);
        }
        for row in &page.open_rows {
            self.open_rows.insert(row.rowid, row.updated_at_ms);
        }
        self.open_rows.retain(|_, updated_at_ms| {
            now_ms.saturating_sub(*updated_at_ms) <= cadence.open_row_ttl_ms
        });
        while self.open_rows.len() > cadence.open_row_cap {
            self.open_rows.pop_first();
        }
    }
}

/// A durable incremental view over OpenCode's SQLite message store.
///
/// `message.time_updated` has no index, so every read keyed on it walks the
/// whole table. Each tick therefore does one of two things:
///
/// - Tail: read the id at the smallest rowid, rows above the last rowid
///   examined, and each row seen earlier whose tokens were not final yet,
///   all by rowid. The cost is O(log n) plus the rows returned, however
///   large the table is.
/// - Full: the page read from the durable `time_updated` cursor, unchanged.
///   It runs on the first drain in a process, while it has a backlog, when
///   the file's inode changed or it shrank, and at least every
///   `full_pass_interval_ms`. It also runs in place of a tail read, in the
///   same tick, when that read finds a different row at the smallest rowid
///   than the last full pass did (a replaced or front-pruned store) or finds
///   rowids going backwards; the tail's rows are then dropped uncommitted.
///
/// There is no tick that reads nothing. Deciding that nothing changed from
/// file length and modification time is unsound: coarse file timestamps let
/// two commits carry the same mtime, a WAL commit need not change the WAL's
/// length, and a tail read costs about a thousand SQLite steps anyway.
///
/// The two positions never mix. Only a full pass moves the durable cursor,
/// and it moves it exactly as it always has; the tail keeps its rowid
/// position in memory and ingests out of band. Whatever the tail did or did
/// not see, the next full pass reads everything from the durable cursor, so
/// it cannot skip a row. What the tail ingested first comes back from the full
/// pass as an exact redelivery, because event identity is content-derived.
///
/// What the tail cannot see, and so waits for the next full pass: a final
/// row rewritten later (the ledger quarantines that either way), a row that
/// becomes final after leaving the open set (open longer than
/// `open_row_ttl_ms`, evicted by `open_row_cap`, or already open and older
/// than the seed window when the process started), an assistant row whose
/// role was only written later, and a new row given a rowid at or below one
/// already examined (only after rows at the end are deleted).
///
/// `session.time_updated` is not used. In OpenCode v1.18.30 a message write
/// never touches its session row: the `MessageUpdated` projector in
/// packages/core/src/session/projector.ts upserts `message` alone, and the
/// usage roll-up there (`applyUsage`) explicitly keeps
/// `session.time_updated`. The session is bumped by prompt.ts's
/// `sessions.touch` when a user prompt arrives and by `setSummary`, which
/// processor.ts forks after each finish-step. The cleanup write that adds
/// `time.completed`, and anything else that follows the last session bump,
/// therefore leaves the message newer than its session, so filtering by
/// session would miss ordinary writes, not just rare ones. On 2026-09-23 the
/// live store had 1,995 messages newer than their session: 1,906 finished
/// assistant rows, 1,883 of them the last write in their session (up to
/// 8 h after the session's last bump), plus 38 in-flight assistant rows and
/// 51 user rows.
pub(crate) struct OpenCodeSource {
    root: OpenCodeRoot,
    home: CursorHome,
    cadence: OpenCodeCadence,
    cache: Mutex<OpenCodeCache>,
}

impl OpenCodeSource {
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
        let root = match crate::harness_paths::opencode(env) {
            Ok(path) => OpenCodeRoot::Path(path),
            Err(error) => OpenCodeRoot::Unresolved(error.to_string()),
        };
        Self::with_cadence(root, PRODUCTION_CADENCE)
    }

    /// HOME-only fixture layout keeps existing drain/health tests independent of
    /// process variables. Production discovery uses `from_env` instead.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_home(home: Option<OsString>) -> Self {
        let root = match home {
            Some(home) if Path::new(&home).is_absolute() => {
                OpenCodeRoot::Path(PathBuf::from(home).join(".local/share/opencode/opencode.db"))
            }
            Some(home) => OpenCodeRoot::Unresolved(format!(
                "HOME is relative ({home:?}); refusing to resolve the OpenCode database"
            )),
            None => OpenCodeRoot::Unresolved(
                "HOME is unset; cannot resolve the OpenCode database".to_string(),
            ),
        };
        Self::with_cadence(root, PRODUCTION_CADENCE)
    }

    fn with_cadence(root: OpenCodeRoot, cadence: OpenCodeCadence) -> Self {
        Self {
            root,
            home: CursorHome::Runtime(OPENCODE_SOURCE_KEY),
            cadence,
            cache: Mutex::new(OpenCodeCache::default()),
        }
    }

    /// A reader of a store another module declared, at `database`, keeping
    /// its cursor at `home`. It reads on the same cadence as the user's store.
    pub(crate) fn declared(database: PathBuf, home: CursorHome) -> Self {
        Self {
            home,
            ..Self::with_cadence(OpenCodeRoot::Path(database), PRODUCTION_CADENCE)
        }
    }

    /// Make the next full drain a full pass from the durable cursor, whatever
    /// the cadence says.
    pub(crate) fn require_full_pass(&self) {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_full_pass_ms = None;
    }

    /// Whether a full pass has reached the store's high water since the last
    /// `require_full_pass`.
    pub(crate) fn full_pass_reached_high_water(&self) -> bool {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_full_pass_ms
            .is_some()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn at(root: impl Into<PathBuf>) -> Self {
        Self::with_cadence(OpenCodeRoot::Path(root.into()), PRODUCTION_CADENCE)
    }

    /// A source whose every drain is a full pass, for tests about the full
    /// page read itself.
    #[cfg(test)]
    fn full_pass_every_tick(root: impl Into<PathBuf>) -> Self {
        Self::with_cadence(
            OpenCodeRoot::Path(root.into()),
            OpenCodeCadence {
                full_pass_interval_ms: 0,
                ..PRODUCTION_CADENCE
            },
        )
    }

    /// Read and commit one tick's worth: a tail read or one full page (see
    /// the type's documentation for which). Setting `stop` abandons the read
    /// wherever the drain is, including inside a running SQLite statement,
    /// and returns `SourceDrainError::Stopped` having committed nothing from
    /// it. The durable cursor is untouched; at most the in-memory tail
    /// position is dropped, which only means the next tail read reseeds.
    pub(crate) fn drain(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        self.drain_with_limit(state, recorded_at_ms, PAGE_LIMIT, stop)
    }

    fn drain_with_limit(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        page_limit: usize,
        stop: &AtomicBool,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        self.drain_in_lane(state, recorded_at_ms, page_limit, stop, DrainMode::Full)
    }

    pub(crate) fn drain_in_lane(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        page_limit: usize,
        stop: &AtomicBool,
        mode: DrainMode,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        self.drain_with_options(
            state,
            recorded_at_ms,
            page_limit,
            stop,
            mode,
            ReadOptions::default(),
        )
    }

    /// One full-tick drain of a declared store, read with `options`.
    pub(crate) fn drain_declared(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
        options: ReadOptions<'_>,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        self.drain_with_options(
            state,
            recorded_at_ms,
            PAGE_LIMIT,
            stop,
            DrainMode::Full,
            options,
        )
    }

    fn drain_with_options(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        page_limit: usize,
        stop: &AtomicBool,
        mode: DrainMode,
        options: ReadOptions<'_>,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        let result = self.drain_inner(state, recorded_at_ms, page_limit, stop, mode, options);
        if result.is_err() || !self.home.retains_connection() {
            self.cache
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .reader = None;
        }
        result
    }

    fn drain_inner(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        page_limit: usize,
        stop: &AtomicBool,
        mode: DrainMode,
        options: ReadOptions<'_>,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        let root = match &self.root {
            OpenCodeRoot::Path(root) => root,
            OpenCodeRoot::Unresolved(reason) => {
                mode.ensure_runtime(state, OPENCODE_SOURCE_KEY)?;
                return Err(reason.clone().into());
            }
        };
        let runtime_key = self.home.runtime_key();

        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let signature = if mode.is_full() {
            let database = match astrocyte_readers::read_io::open(root) {
                Ok(database) => {
                    if let Some(key) = runtime_key {
                        crate::refresher::harness_drain::record_source_discovery(
                            state,
                            key,
                            root.display(),
                            true,
                        );
                        mode.ensure_runtime(state, key)?;
                    }
                    database
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    cache.reader = None;
                    if let Some(key) = runtime_key {
                        crate::refresher::harness_drain::record_source_discovery(
                            state,
                            key,
                            root.display(),
                            false,
                        );
                        mode.remove_runtime(state, key)?;
                    }
                    return Ok(OpenCodeDrainOutcome {
                        root_absent: true,
                        ..OpenCodeDrainOutcome::default()
                    });
                }
                Err(error) => {
                    cache.reader = None;
                    if let Some(key) = runtime_key {
                        mode.ensure_runtime(state, key)?;
                    }
                    return Err(
                        format!("inspect OpenCode database {}: {error}", root.display()).into(),
                    );
                }
            };
            let metadata = database.metadata().map_err(|error| {
                SourceDrainError::from(format!(
                    "inspect OpenCode database {}: {error}",
                    root.display()
                ))
            })?;
            let signature = FileSignature::capture(&database, &metadata);
            if cache
                .signature
                .is_some_and(|previous| signature.replaces(&previous))
            {
                cache.reader = None;
            }
            signature
        } else if let Some(signature) = cache.signature.filter(|_| cache.front_row_id.is_some()) {
            signature
        } else {
            return Ok(OpenCodeDrainOutcome::default());
        };
        if mode.is_full()
            && cache.choose(&signature, recorded_at_ms, &self.cadence) == OpenCodePass::Full
        {
            return self.full_pass(
                &mut cache,
                signature,
                state,
                root,
                recorded_at_ms,
                page_limit,
                stop,
                options,
            );
        }

        let reread = cache.open_rows.keys().copied().collect::<Vec<_>>();
        let request = OpenCodeTailRequest {
            after_rowid: cache.tail_after_rowid,
            seed_rows: self.cadence.seed_rows,
            open_rowids: &reread,
            limit: page_limit,
        };
        if cache.reader.is_none() {
            cache.reader = Some(OpenCodeTailReader::open(root).map_err(read_error)?);
        }
        let read = cache
            .reader
            .as_ref()
            .expect("opened tail reader")
            .read(root, &request, stop);
        if !self.home.retains_connection() {
            cache.reader = None;
        }
        let mut page = match read {
            Ok(page) => page,
            Err(error) => {
                cache.reader = None;
                return Err(read_error(error));
            }
        };
        // A known-empty store can acquire its first row between full ticks. There
        // is no old rowid identity to contradict when the tail starts at zero.
        if !mode.is_full() && cache.front_row_id == Some(None) && page.start_rowid == 0 {
            cache.front_row_id = Some(page.front_row_id.clone());
        }
        if cache.tail_is_stale(&page) {
            // Nothing from the tail read is committed: its rowids may belong
            // to another store. The full pass in this same tick works out the
            // lineage again and reads from the durable cursor.
            cache.forget_tail();
            cache.reader = None;
            if !mode.is_full() {
                return Ok(OpenCodeDrainOutcome {
                    cost: page.cost,
                    ..OpenCodeDrainOutcome::default()
                });
            }
            let mut outcome = self.full_pass(
                &mut cache,
                signature,
                state,
                root,
                recorded_at_ms,
                page_limit,
                stop,
                options,
            )?;
            outcome.cost.add_assign_saturating(&page.cost);
            return Ok(outcome);
        }
        let mut set_aside: Vec<_> = std::mem::take(&mut page.set_aside)
            .into_iter()
            .map(|record| crate::refresher::SetAsideRecord {
                locator: format!("{root:?}:message:{}", record.id),
                reason: record.reason,
            })
            .collect();
        let requests = map_messages(
            root,
            std::mem::take(&mut page.messages),
            &mut set_aside,
            options.work_class,
        );
        let committed = commit_page_in_lane(
            state,
            &self.home,
            &requests,
            &set_aside,
            None,
            recorded_at_ms,
            &|| stop.load(Ordering::Relaxed),
            mode,
        )?;
        cache.after_tail(signature, &page, &reread, recorded_at_ms, &self.cadence);
        let mut outcome = OpenCodeDrainOutcome {
            pass: Some(OpenCodePass::Tail),
            cost: page.cost,
            ..OpenCodeDrainOutcome::default()
        };
        outcome.count(committed);
        Ok(outcome)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one call site pair; the arguments are the drain's own context"
    )]
    fn full_pass(
        &self,
        cache: &mut OpenCodeCache,
        signature: FileSignature,
        state: &SharedState,
        root: &Path,
        recorded_at_ms: i64,
        page_limit: usize,
        stop: &AtomicBool,
        options: ReadOptions<'_>,
    ) -> Result<OpenCodeDrainOutcome, SourceDrainError> {
        let stored = self.home.read_cursor(state)?;
        // A stop during the read abandons the page before anything is
        // written: no event, no cursor advance, no runtime failure. Every
        // OpenCode event has a content-derived identity, so the next start
        // reads the same page again and loses nothing.
        let page =
            read_opencode_page(root, stored.as_ref(), page_limit, stop).map_err(read_error)?;
        let front_row_id = page.front_row_id.clone();
        let mut set_aside: Vec<_> = page
            .set_aside
            .into_iter()
            .map(|record| crate::refresher::SetAsideRecord {
                locator: format!("{root:?}:message:{}", record.id),
                reason: record.reason,
            })
            .collect();
        let requests = map_messages(root, page.messages, &mut set_aside, options.work_class);
        let committed = commit_page_with_evidence(
            state,
            &self.home,
            &requests,
            &set_aside,
            Some(&PageCursor {
                lineage: &page.lineage,
                consumed_ordinal: page.consumed_ordinal,
                high_water_ordinal: page.high_water_ordinal,
            }),
            recorded_at_ms,
            &|| stop.load(Ordering::Relaxed),
        )?;

        cache.after_full_pass(
            signature,
            front_row_id,
            page.consumed_ordinal,
            page.high_water_ordinal,
            recorded_at_ms,
        );

        let mut outcome = OpenCodeDrainOutcome {
            pass: Some(OpenCodePass::Full),
            raw: page.raw,
            raw_after_cursor: page.raw_after_cursor,
            emitted: page.emitted,
            emitted_after_cursor: page.emitted_after_cursor,
            consumed_ordinal: Some(page.consumed_ordinal),
            high_water_ordinal: Some(page.high_water_ordinal),
            cost: page.cost,
            ..OpenCodeDrainOutcome::default()
        };
        outcome.count(committed);
        Ok(outcome)
    }
}

impl crate::refresher::harness_drain::HarnessSource for OpenCodeSource {
    fn drain_fast(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        let outcome = self.drain_in_lane(state, at, PAGE_LIMIT, stop, DrainMode::Fast)?;
        #[cfg(all(test, target_os = "linux"))]
        super::fast_lane_measure::note_vm_steps(outcome.cost.vm_steps);
        let _ = outcome;
        Ok(())
    }

    fn source_key(&self) -> &'static str {
        OPENCODE_SOURCE_KEY
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        recorded_at_ms: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let outcome = self.drain(state, recorded_at_ms, stop)?;
        #[cfg(all(test, target_os = "linux"))]
        super::fast_lane_measure::note_vm_steps(outcome.cost.vm_steps);
        Ok((outcome.quarantined_redeliveries > 0).then(|| {
            format!(
                "{} OpenCode deliveries were quarantined",
                outcome.quarantined_redeliveries
            )
        }))
    }
}

impl OpenCodeDrainOutcome {
    fn count(&mut self, committed: Vec<RecordDisposition>) {
        for disposition in committed {
            match disposition {
                RecordDisposition::Recorded => self.rows_recorded += 1,
                RecordDisposition::ExactRedelivery => self.exact_redeliveries += 1,
                RecordDisposition::QuarantinedRedelivery => {
                    self.quarantined_redeliveries += 1;
                }
            }
        }
    }
}

fn read_error(error: OpenCodeReadError) -> SourceDrainError {
    match error {
        OpenCodeReadError::Stopped => SourceDrainError::Stopped,
        OpenCodeReadError::Failed(reason) => SourceDrainError::Failed(reason),
    }
}

fn map_messages(
    path: &Path,
    messages: Vec<OpenCodeMessage>,
    set_aside: &mut Vec<crate::refresher::SetAsideRecord>,
    work_class: Option<&str>,
) -> Vec<EventRecordRequest> {
    let mut requests = Vec::new();
    for message in messages {
        let locator = format!("{path:?}:message:{}", message.id);
        match map_message(message) {
            Ok(request) => requests.push(stamp_work_class(request, work_class)),
            Err(reason) => set_aside.push(crate::refresher::SetAsideRecord { locator, reason }),
        }
    }
    requests
}

/// Replace the producer's agent name with a declared work class. Identity is
/// unchanged: it is derived from producer, lineage and reference only.
pub(crate) fn stamp_work_class(
    mut request: EventRecordRequest,
    work_class: Option<&str>,
) -> EventRecordRequest {
    if let Some(work_class) = work_class {
        request.work_class = Some(work_class.to_owned());
    }
    request
}

/// Where a fully ingested page moves the durable cursor.
pub(crate) struct PageCursor<'a> {
    pub lineage: &'a str,
    pub consumed_ordinal: i64,
    pub high_water_ordinal: i64,
}

/// Ingest one page and, for a full pass, advance the cursor, in a single
/// transaction. With no cursor (a tail read) only
/// `last_success_ms` is recorded; the durable cursor is left exactly as it
/// was, because only a full pass has seen everything below it.
///
/// `stop_requested` is asked before every event. When it answers true the
/// transaction is rolled back, so the page leaves no event and no cursor
/// advance behind. That is safe because OpenCode ingest is idempotent: event
/// identity is derived from content and a read never writes redelivery_log, so
/// the next start ingests the whole page again from the unchanged cursor.
#[cfg(test)]
fn commit_page(
    state: &SharedState,
    requests: &[EventRecordRequest],
    cursor: Option<&PageCursor<'_>>,
    recorded_at_ms: i64,
    stop_requested: &dyn Fn() -> bool,
) -> Result<Vec<RecordDisposition>, SourceDrainError> {
    commit_page_with_evidence(
        state,
        &CursorHome::Runtime(OPENCODE_SOURCE_KEY),
        requests,
        &[],
        cursor,
        recorded_at_ms,
        stop_requested,
    )
}

fn commit_page_with_evidence(
    state: &SharedState,
    home: &CursorHome,
    requests: &[EventRecordRequest],
    set_aside: &[crate::refresher::SetAsideRecord],
    cursor: Option<&PageCursor<'_>>,
    recorded_at_ms: i64,
    stop_requested: &dyn Fn() -> bool,
) -> Result<Vec<RecordDisposition>, SourceDrainError> {
    commit_page_in_lane(
        state,
        home,
        requests,
        set_aside,
        cursor,
        recorded_at_ms,
        stop_requested,
        DrainMode::Full,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "the arguments are the page and the drain's own context"
)]
fn commit_page_in_lane(
    state: &SharedState,
    home: &CursorHome,
    requests: &[EventRecordRequest],
    set_aside: &[crate::refresher::SetAsideRecord],
    cursor: Option<&PageCursor<'_>>,
    recorded_at_ms: i64,
    stop_requested: &dyn Fn() -> bool,
    mode: DrainMode,
) -> Result<Vec<RecordDisposition>, SourceDrainError> {
    if !mode.has_work(requests.len(), set_aside.len()) {
        return Ok(Vec::new());
    }
    let mut stopped = false;
    let committed = mode.write(state, |tx| {
        if requests.is_empty() && stop_requested() {
            stopped = true;
            return Err("OpenCode ingest stopped".into());
        }
        let newly_seen = crate::refresher::harness_drain::record_set_aside_in_tx(
            tx,
            home.source_key(),
            set_aside,
            recorded_at_ms,
        )?;
        let mut dispositions = Vec::with_capacity(requests.len());
        for request in requests {
            if stop_requested() {
                stopped = true;
                // Returning an error drops the transaction uncommitted.
                return Err("OpenCode ingest stopped".to_string());
            }
            let result = astrocyte_core::ingest::record_event_in_tx(
                tx,
                request,
                recorded_at_ms,
                astrocyte_core::ingest::DeliveryPath::Read,
            )
            .map_err(|error| format!("record OpenCode event: {error:?}"))?;
            dispositions.push(result.disposition);
        }
        let updated = if !mode.is_full() {
            Ok(1)
        } else {
            home.record_read_in_tx(tx, cursor, recorded_at_ms)
        }
        .map_err(|error| format!("record OpenCode source runtime: {error}"))?;
        if updated != 1 {
            return Err("OpenCode source runtime row disappeared before cursor commit".to_string());
        }
        Ok(((dispositions, newly_seen), true))
    });
    match committed {
        Ok((dispositions, newly_seen)) => {
            crate::refresher::harness_drain::warn_set_aside(home.source_key(), &newly_seen);
            Ok(dispositions)
        }
        Err(_) if stopped => Err(SourceDrainError::Stopped),
        Err(reason) => Err(SourceDrainError::Failed(reason)),
    }
}

fn map_message(message: OpenCodeMessage) -> Result<EventRecordRequest, String> {
    let measurements = [
        ("input", "usage.input", message.tokens.input),
        ("output", "usage.output", message.tokens.output),
        ("cache_read", "usage.cacheRead", message.tokens.cache_read),
        (
            "cache_write",
            "usage.cacheWrite",
            message.tokens.cache_write,
        ),
        // OpenCode's output excludes reasoning. Pi deliberately subtracts its
        // nested reasoning bucket; applying that convention here would corrupt
        // valid OpenCode turns where reasoning is greater than output.
        ("reasoning", "usage.reasoning", message.tokens.reasoning),
    ]
    .into_iter()
    .filter_map(|(role, source, quantity)| quantity.map(|quantity| (role, source, quantity)))
    .map(|(role, source, quantity)| {
        crate::refresher::harness_drain::measurement(role, source, Some(quantity))
            .map_err(|error| format!("OpenCode {error}"))
    })
    .collect::<Result<Vec<_>, String>>()?;

    Ok(EventRecordRequest {
        producer: OPENCODE_SOURCE_KEY.to_string(),
        // The live schema declares `message.id` as its table-wide PRIMARY KEY,
        // and corpus measurement found no duplicate ids. A session lineage
        // would therefore alter identity without disambiguating anything.
        producer_lineage: None,
        producer_ref: message.id,
        prior_event_ref: None,
        harness: None,
        project_root: message.project_root,
        session: Some(message.session_id),
        run_ref: None,
        flow_ref: None,
        // Preserve producer spelling byte-for-byte, including U+200B prefixes:
        // visually identical labels with different bytes are distinct work classes.
        work_class: message.agent,
        occurred_at_ms: Some(message.occurred_at_ms),
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
        // `data.finish` is kept beside the event for cache diagnostics; it is
        // not part of the event, so decoding it changes no ledger row.
        step_detail: StepDetail {
            finish: message.finish,
            context_limit: None,
        },
    })
}

// The test tokens and parsed duration fixtures fit their signed storage types.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::refresher::{
        run_iteration_with_file_sources, ExportPage, Producer, PullError, SourceClient,
        SourceCursor,
    };
    use crate::state::{test_state, SharedState};
    use astrocyte_core::ingest::EventRecordRequest;
    use rusqlite::Connection;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tempfile::TempDir;

    static NEVER_STOP: AtomicBool = AtomicBool::new(false);

    #[test]
    fn set_aside_opencode_bad_row_keeps_good_rows_and_cursor() {
        for kind in ["malformed", "blob", "overflow"] {
            let temp = TempDir::new().unwrap();
            let path = temp.path().join("opencode.db");
            let conn = create_database(&path);
            for (id, time) in [("before", 10), ("bad", 20), ("after", 30)] {
                insert_message(&conn, id, "session", time, time);
            }
            match kind {
                "malformed" => {
                    conn.execute("UPDATE message SET data='{bad' WHERE id='bad'", [])
                        .unwrap();
                }
                "blob" => {
                    conn.execute(
                        "UPDATE message SET data=CAST(data AS BLOB) WHERE id='bad'",
                        [],
                    )
                    .unwrap();
                }
                _ => {
                    conn.execute(
                        "UPDATE message SET data=?1 WHERE id='bad'",
                        [payload("build", "/root", 1_u64 << 63, 0, 20)],
                    )
                    .unwrap();
                }
            }
            let state = test_state();
            let source = OpenCodeSource::at(&path);
            for now in [100, 200] {
                let result = source.drain(&state, now, &NEVER_STOP);
                assert!(result.is_ok(), "{kind}, tick {now}: {result:?}");
                let rows = state
                    .with_conn(|conn| {
                        conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .map_err(|e| e.to_string())
                    })
                    .unwrap();
                assert_eq!(rows, 2);
                assert_eq!(runtime_cursor(&state).1, 30);
                crate::refresher::harness_drain::assert_set_aside(&state, "opencode", 1);
            }
        }
    }

    #[test]
    fn set_aside_good_opencode_rows_are_byte_identical() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let conn = create_database(&path);
        insert_message(&conn, "before", "session", 10, 10);
        insert_message(&conn, "after", "session", 30, 30);
        let state = test_state();
        OpenCodeSource::at(&path)
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();
        crate::refresher::harness_drain::assert_good_snapshot(
            &state,
            "opencode",
            "11ece20b59027050753832cf2355e9d7d5e955cd40e3984d49408cdfe84fc6e8",
        );
    }

    /// `data.finish` is kept in `step_detail`, and reading it changes no
    /// ledger row. The pinned fixture above carries `finish`; the same two
    /// messages without it (still final through `time.completed`) store
    /// exactly the same ledger bytes and no `step_detail` row.
    #[test]
    fn opencode_finish_is_stored_beside_a_byte_identical_ledger() {
        const PINNED: &str = "11ece20b59027050753832cf2355e9d7d5e955cd40e3984d49408cdfe84fc6e8";
        let drain = |finish: Option<serde_json::Value>| {
            let temp = TempDir::new().unwrap();
            let path = temp.path().join("opencode.db");
            let conn = create_database(&path);
            for (id, created) in [("before", 10), ("after", 30)] {
                let mut data: serde_json::Value =
                    serde_json::from_str(&payload("build", "/producer/root", 13, 17, created))
                        .unwrap();
                match &finish {
                    Some(finish) => data["finish"] = finish.clone(),
                    None => {
                        data.as_object_mut().unwrap().remove("finish");
                    }
                }
                conn.execute(
                    "INSERT INTO message (id,session_id,time_created,time_updated,data)
                     VALUES (?1,'session',?2,?2,?3)",
                    params![id, created, data.to_string()],
                )
                .unwrap();
            }
            let state = test_state();
            OpenCodeSource::at(&path)
                .drain(&state, 100, &NEVER_STOP)
                .unwrap();
            crate::refresher::harness_drain::assert_good_snapshot(&state, "opencode", PINNED);
            crate::refresher::harness_drain::step_details(&state)
        };
        let stop = Some("stop".to_string());
        assert_eq!(
            drain(Some(serde_json::json!("stop"))),
            [
                ("after".to_string(), stop.clone(), None),
                ("before".to_string(), stop, None)
            ]
        );
        assert!(drain(None).is_empty());
        // A finish that is not a string is not kept, and the message is read.
        assert!(drain(Some(serde_json::json!({"reason": "stop"}))).is_empty());
    }

    #[test]
    fn set_aside_opencode_tail_advances_past_bad_rows() {
        for kind in ["malformed", "blob", "overflow"] {
            let temp = TempDir::new().unwrap();
            let path = temp.path().join("opencode.db");
            let conn = create_database(&path);
            insert_message(&conn, "before", "session", 10, 10);
            let source = OpenCodeSource::at(&path);
            let state = test_state();
            source.drain(&state, 100, &NEVER_STOP).unwrap();
            insert_message(&conn, "bad", "session", 20, 20);
            insert_message(&conn, "after", "session", 30, 30);
            match kind {
                "malformed" => {
                    conn.execute("UPDATE message SET data='{bad' WHERE id='bad'", [])
                        .unwrap();
                }
                "blob" => {
                    conn.execute(
                        "UPDATE message SET data=CAST(data AS BLOB) WHERE id='bad'",
                        [],
                    )
                    .unwrap();
                }
                _ => {
                    conn.execute(
                        "UPDATE message SET data=?1 WHERE id='bad'",
                        [payload("build", "/root", 1_u64 << 63, 0, 20)],
                    )
                    .unwrap();
                }
            }
            for now in [200, 300] {
                let result = source.drain(&state, now, &NEVER_STOP).unwrap();
                assert_eq!(result.pass, Some(OpenCodePass::Tail));
                assert_eq!(source.cache.lock().unwrap().tail_after_rowid, Some(3));
                crate::refresher::harness_drain::assert_set_aside(&state, "opencode", 1);
            }
            OpenCodeSource::at(&path)
                .drain(&state, 400, &NEVER_STOP)
                .unwrap();
            assert_eq!(runtime_cursor(&state).1, 30);
            crate::refresher::harness_drain::assert_set_aside(&state, "opencode", 1);
        }
    }

    #[test]
    fn unknown_database_identity_requires_a_full_pass() {
        let signature = FileSignature {
            identity: None,
            db_len: 4096,
        };
        let cache = OpenCodeCache {
            signature: Some(signature),
            last_full_pass_ms: Some(0),
            ..OpenCodeCache::default()
        };
        assert_eq!(
            cache.choose(&signature, 1, &PRODUCTION_CADENCE),
            OpenCodePass::Full
        );
    }

    fn create_database(path: &Path) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE message (
                     id TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL,
                     time_created INTEGER NOT NULL,
                     time_updated INTEGER NOT NULL,
                     data TEXT NOT NULL
                 );
                 CREATE TABLE session (
                     id TEXT PRIMARY KEY,
                     directory TEXT NOT NULL,
                     tokens_output INTEGER NOT NULL
                 );",
            )
            .unwrap();
        connection
    }

    fn insert_message(
        connection: &Connection,
        id: &str,
        session: &str,
        created: i64,
        updated: i64,
    ) {
        connection
            .execute(
                "INSERT INTO message (id,session_id,time_created,time_updated,data)
                 VALUES (?1,?2,?3,?4,?5)",
                params![
                    id,
                    session,
                    created,
                    updated,
                    payload("build", "/producer/root", 13, 17, created)
                ],
            )
            .unwrap();
    }

    fn drain_one(data: &str, session_output: i64) -> (TempDir, SharedState) {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        connection
            .execute(
                "INSERT INTO session (id,directory,tokens_output)
                 VALUES ('session-1','/wrong/session/directory',?1)",
                [session_output],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO message (id,session_id,time_created,time_updated,data)
                 VALUES ('msg-1','session-1',10,20,?1)",
                [data],
            )
            .unwrap();
        drop(connection);
        let state = test_state();
        OpenCodeSource::at(&path)
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();
        (temp, state)
    }

    fn runtime_cursor(state: &SharedState) -> (String, i64, i64) {
        state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT lineage,consumed_ordinal,high_water_ordinal
                         FROM source_runtime WHERE source_key='opencode'",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    #[test]
    fn reasoning_measurement_is_stored_as_given_not_subtracted() {
        let (_temp, state) = drain_one(
            &payload("build", "/producer/root", 4, 9, 1_700_000_000_001),
            999_999,
        );
        let quantities: (i64, i64) = state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT
                           MAX(CASE WHEN role='output' THEN quantity_units END),
                           MAX(CASE WHEN role='reasoning' THEN quantity_units END)
                         FROM measurement",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(quantities, (4, 9));
    }

    #[test]
    fn zero_width_space_work_class_is_stored_byte_identical() {
        let agent = "\u{200b}Alfonso - CTO";
        let (_temp, state) = drain_one(
            &payload(agent, "/producer/root", 4, 9, 1_700_000_000_001),
            999_999,
        );
        let stored: String = state
            .with_conn(|connection| {
                connection
                    .query_row("SELECT work_class FROM usage_event", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored.as_bytes(), agent.as_bytes());
    }

    #[test]
    fn stored_project_root_comes_from_message_path_not_session_directory() {
        let (_temp, state) = drain_one(
            &payload("build", "/producer/declared/root", 4, 9, 1_700_000_000_001),
            999_999,
        );
        let stored: String = state
            .with_conn(|connection| {
                connection
                    .query_row("SELECT project_root FROM usage_event", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored, "/producer/declared/root");
    }

    #[test]
    fn stored_output_ignores_session_token_rollup() {
        let (_temp, state) = drain_one(
            &payload("build", "/producer/root", 7, 11, 1_700_000_000_001),
            999_999,
        );
        let stored: i64 = state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT quantity_units FROM measurement WHERE role='output'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored, 7);
    }

    #[test]
    fn durable_cursor_advances_and_second_drain_reads_strictly_fewer_rows() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        insert_message(&connection, "msg-oldest", "session-1", 10, 20);
        insert_message(&connection, "msg-newest", "session-1", 30, 40);
        drop(connection);
        let state = test_state();
        // The second drain must be a full pass for its cursor to be under
        // test; under the default cadence it would be a tail read.
        let source = OpenCodeSource::full_pass_every_tick(&path);

        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();

        assert_eq!(first.raw.records, 2);
        assert_eq!(first.rows_recorded, 2);
        assert_eq!(second.raw.records, 1);
        assert!(second.raw.records < first.raw.records);
        assert_eq!(second.exact_redeliveries, 1);
        assert_eq!(runtime_cursor(&state), ("msg-oldest".to_string(), 40, 40));
    }

    /// The inclusive cursor makes every drain read its boundary message again.
    /// That re-read is ours, not OpenCode sending the message twice, so it is
    /// an exact redelivery that leaves no redelivery_log row.
    #[test]
    fn boundary_reread_is_an_exact_redelivery_without_a_redelivery_log_row() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        insert_message(&connection, "msg-only", "session-1", 10, 20);
        drop(connection);
        let state = test_state();
        // The boundary re-read happens only in a full pass.
        let source = OpenCodeSource::full_pass_every_tick(&path);

        source.drain(&state, 100, &NEVER_STOP).unwrap();
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(second.exact_redeliveries, 1);
        let logged: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM redelivery_log", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(logged, 0);
    }

    #[test]
    fn tied_timestamp_boundary_is_inclusive_so_no_message_is_skipped() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        insert_message(&connection, "msg-oldest", "session-1", 10, 10);
        insert_message(&connection, "msg-tied-a", "session-1", 20, 20);
        insert_message(&connection, "msg-tied-b", "session-1", 21, 20);
        insert_message(&connection, "msg-later", "session-1", 30, 30);
        drop(connection);
        let state = test_state();
        let source = OpenCodeSource::at(&path);

        let first = source
            .drain_with_limit(&state, 100, 2, &NEVER_STOP)
            .unwrap();
        assert_eq!(first.consumed_ordinal, Some(20));
        assert_eq!(first.rows_recorded, 3);

        let second = source
            .drain_with_limit(&state, 200, 2, &NEVER_STOP)
            .unwrap();
        assert_eq!(second.consumed_ordinal, Some(20));
        assert_eq!(second.raw.records, 2);
        assert_eq!(second.exact_redeliveries, 2);
        assert_eq!(second.rows_recorded, 0);

        let (tied_b, later): (i64, i64) = state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT
                           (SELECT COUNT(*) FROM usage_event WHERE producer_ref='msg-tied-b'),
                           (SELECT COUNT(*) FROM usage_event WHERE producer_ref='msg-later')",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(tied_b, 1, "the second tied message must be ingested");
        assert_eq!(later, 0, "the tied boundary must not advance early");
    }

    #[test]
    fn different_oldest_message_lineage_resets_replaced_store_cursor() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let first_store = create_database(&path);
        insert_message(&first_store, "old-store-first", "session-old", 10, 20);
        insert_message(&first_store, "old-store-last", "session-old", 30, 40);
        drop(first_store);
        let state = test_state();
        let source = OpenCodeSource::at(&path);
        source.drain(&state, 100, &NEVER_STOP).unwrap();

        fs::remove_file(&path).unwrap();
        let replacement = create_database(&path);
        insert_message(&replacement, "new-store-first", "session-new", 1, 5);
        drop(replacement);

        let outcome = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(outcome.rows_recorded, 1);
        assert_eq!(
            runtime_cursor(&state),
            ("new-store-first".to_string(), 5, 5)
        );
        let replacement_rows: i64 = state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM usage_event
                         WHERE producer='opencode' AND producer_ref='new-store-first'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(replacement_rows, 1, "replacement rows must not be skipped");
    }

    /// Lifecycle states of one assistant message, following OpenCode's
    /// processor: created with zero tokens, then `finish` and the final tokens
    /// at finish-step, then `time.completed` at cleanup. A process that dies
    /// between the last two leaves the row in the `Finished` state forever.
    #[derive(Clone, Copy)]
    enum Lifecycle {
        InFlight,
        Finished,
        Completed,
    }

    const FINAL_TOKENS: (u64, u64, u64, u64, u64) = (2, 380, 11, 251_076, 443);

    fn lifecycle_payload(state: Lifecycle) -> String {
        let (input, output, reasoning, cache_read, cache_write) = match state {
            Lifecycle::InFlight => (0, 0, 0, 0, 0),
            Lifecycle::Finished | Lifecycle::Completed => FINAL_TOKENS,
        };
        let mut value = serde_json::json!({
            "role": "assistant",
            "agent": "build",
            "modelID": "claude-sonnet-4",
            "providerID": "anthropic",
            "path": {"root": "/producer/root"},
            "time": {"created": 1_700_000_000_000_i64},
            "tokens": {
                "input": input,
                "output": output,
                "reasoning": reasoning,
                "cache": {"read": cache_read, "write": cache_write}
            }
        });
        if matches!(state, Lifecycle::Finished | Lifecycle::Completed) {
            value["finish"] = serde_json::json!("tool-calls");
        }
        if matches!(state, Lifecycle::Completed) {
            value["time"]["completed"] = serde_json::json!(1_700_000_009_000_i64);
        }
        value.to_string()
    }

    fn write_lifecycle(connection: &Connection, id: &str, updated: i64, state: Lifecycle) {
        connection
            .execute(
                "INSERT INTO message (id,session_id,time_created,time_updated,data)
                 VALUES (?1,'session-1',10,?2,?3)
                 ON CONFLICT(id) DO UPDATE SET time_updated=?2,data=?3",
                params![id, updated, lifecycle_payload(state)],
            )
            .unwrap();
    }

    fn ledger_count(state: &SharedState, sql: &str) -> i64 {
        state
            .with_conn(|connection| {
                connection
                    .query_row(sql, [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn stored_measurements(state: &SharedState, producer_ref: &str) -> Vec<(String, i64)> {
        state
            .with_conn(|connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT m.role,m.quantity_units FROM measurement m
                         JOIN usage_event e ON e.event_key=m.event_key
                         WHERE e.producer='opencode' AND e.producer_ref=?1
                         ORDER BY m.role",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([producer_ref], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(|error| error.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string());
                rows
            })
            .unwrap()
    }

    fn final_measurements() -> Vec<(String, i64)> {
        let (input, output, reasoning, cache_read, cache_write) = FINAL_TOKENS;
        let mut expected = vec![
            ("cache_read".to_string(), cache_read as i64),
            ("cache_write".to_string(), cache_write as i64),
            ("input".to_string(), input as i64),
            ("output".to_string(), output as i64),
            ("reasoning".to_string(), reasoning as i64),
        ];
        expected.sort();
        expected
    }

    #[test]
    fn in_flight_message_is_not_ingested() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-streaming", 20, Lifecycle::InFlight);
        drop(connection);
        let state = test_state();

        let outcome = OpenCodeSource::at(&path)
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();

        assert_eq!(outcome.rows_recorded, 0);
        assert_eq!(
            ledger_count(&state, "SELECT COUNT(*) FROM usage_event"),
            0,
            "a streaming message's zero tokens must not reach the ledger"
        );
    }

    /// Skipping an in-flight row is only correct if a later drain picks it up
    /// once final. This drives the cursor past the in-flight row (a later
    /// completed row sets the page boundary), then completes the row at a
    /// later `time_updated`, as OpenCode's completion write does.
    #[test]
    fn in_flight_message_completed_later_is_ingested_with_final_figures_and_no_quarantine() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-streaming", 20, Lifecycle::InFlight);
        write_lifecycle(&connection, "msg-other", 30, Lifecycle::Completed);
        let state = test_state();
        // Full passes only: this is about the durable cursor moving past an
        // in-flight row. The tail picks the same row up by rowid, which
        // would pass this test without exercising the cursor at all.
        let source = OpenCodeSource::full_pass_every_tick(&path);

        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(first.consumed_ordinal, Some(30), "cursor passed the row");

        write_lifecycle(&connection, "msg-streaming", 40, Lifecycle::Completed);
        drop(connection);
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();

        // The quarantine is asserted first: it is what an ingested zero
        // followed by the completed figure produces in a write-once ledger.
        assert_eq!(
            ledger_count(&state, "SELECT COUNT(*) FROM rejected_delivery"),
            0,
            "the completed figure must not be rejected"
        );
        assert_eq!(
            first.quarantined_redeliveries + second.quarantined_redeliveries,
            0
        );
        assert_eq!(
            stored_measurements(&state, "msg-streaming"),
            final_measurements()
        );
    }

    /// OpenCode stamps `time_updated` with the wall clock in milliseconds, so
    /// a completion can share the cursor's exact timestamp. The inclusive
    /// lower bound must still select it.
    #[test]
    fn completion_at_the_cursor_timestamp_is_still_ingested() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-streaming", 20, Lifecycle::InFlight);
        write_lifecycle(&connection, "msg-other", 30, Lifecycle::Completed);
        let state = test_state();
        // Full passes only: the inclusive lower bound is what is under test,
        // and the tail would ingest the completion without consulting it.
        let source = OpenCodeSource::full_pass_every_tick(&path);

        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(first.consumed_ordinal, Some(30));

        write_lifecycle(&connection, "msg-streaming", 30, Lifecycle::Completed);
        drop(connection);
        source.drain(&state, 200, &NEVER_STOP).unwrap();

        assert_eq!(
            stored_measurements(&state, "msg-streaming"),
            final_measurements()
        );
    }

    /// A process that dies between finish-step and cleanup leaves `finish`
    /// and the final tokens but never `time.completed`. Those tokens were
    /// consumed and must be ingested.
    #[test]
    fn aborted_message_with_finish_but_no_completion_is_ingested() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-aborted", 20, Lifecycle::Finished);
        drop(connection);
        let state = test_state();

        OpenCodeSource::at(&path)
            .drain(&state, 100, &NEVER_STOP)
            .unwrap();

        assert_eq!(
            stored_measurements(&state, "msg-aborted"),
            final_measurements()
        );
    }

    /// Ingesting at `finish` is only safe if the later cleanup write, which
    /// adds `time.completed`, changes nothing the reader maps. This compares
    /// the mapped requests for both states directly, then checks the ledger
    /// sees the completed row as an exact redelivery.
    #[test]
    fn finished_and_completed_states_map_to_identical_requests() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-turn", 20, Lifecycle::Finished);
        let state = test_state();
        let source = OpenCodeSource::at(&path);

        let finished_page = read_opencode_page(&path, None, 10, &NEVER_STOP).unwrap();
        source.drain(&state, 100, &NEVER_STOP).unwrap();
        write_lifecycle(&connection, "msg-turn", 40, Lifecycle::Completed);
        drop(connection);
        let completed_page = read_opencode_page(&path, None, 10, &NEVER_STOP).unwrap();
        let second = source.drain(&state, 200, &NEVER_STOP).unwrap();

        let finished = finished_page
            .messages
            .into_iter()
            .map(|message| map_message(message).unwrap())
            .collect::<Vec<_>>();
        let completed = completed_page
            .messages
            .into_iter()
            .map(|message| map_message(message).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished, completed);
        assert_eq!(second.exact_redeliveries, 1);
        assert_eq!(
            ledger_count(&state, "SELECT COUNT(*) FROM rejected_delivery"),
            0
        );
    }

    /// The tail's blind spot, shaped like the messages measured newer than
    /// their session: the session row's `time_updated` is never bumped, and a
    /// row turns final only after it left the open set (here because it had
    /// been open longer than the open-row TTL, as a turn abandoned by a dead
    /// process is). Only the periodic full pass can ingest it.
    #[test]
    fn a_change_the_tail_cannot_see_is_ingested_by_the_periodic_full_pass() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        connection
            .execute_batch(
                "ALTER TABLE session ADD COLUMN time_updated INTEGER NOT NULL DEFAULT 0;
                 INSERT INTO session (id,directory,tokens_output,time_updated)
                 VALUES ('session-1','/project',0,5);",
            )
            .unwrap();
        write_lifecycle(&connection, "msg-abandoned", 20, Lifecycle::InFlight);
        let state = test_state();
        let source = OpenCodeSource::with_cadence(
            OpenCodeRoot::Path(path.clone()),
            OpenCodeCadence {
                full_pass_interval_ms: 1_000,
                open_row_ttl_ms: 100,
                ..PRODUCTION_CADENCE
            },
        );

        let first = source.drain(&state, 1_000, &NEVER_STOP).unwrap();
        assert_eq!(first.pass, Some(OpenCodePass::Full));
        write_lifecycle(&connection, "msg-other", 30, Lifecycle::Completed);
        let seeded = source.drain(&state, 1_100, &NEVER_STOP).unwrap();
        assert_eq!(seeded.pass, Some(OpenCodePass::Tail));
        assert_eq!(seeded.rows_recorded, 1, "the tail ingests the new row");

        write_lifecycle(&connection, "msg-abandoned", 40, Lifecycle::Completed);
        let blind = source.drain(&state, 1_200, &NEVER_STOP).unwrap();
        assert_eq!(blind.pass, Some(OpenCodePass::Tail));
        assert!(
            stored_measurements(&state, "msg-abandoned").is_empty(),
            "the tail must not see this row, or this test does not exercise the full pass"
        );

        let full = source.drain(&state, 2_000, &NEVER_STOP).unwrap();
        assert_eq!(full.pass, Some(OpenCodePass::Full));
        assert_eq!(
            stored_measurements(&state, "msg-abandoned"),
            final_measurements()
        );
        assert_eq!(
            ledger_count(&state, "SELECT COUNT(*) FROM rejected_delivery"),
            0
        );
    }

    /// An ordinary new turn reaches the ledger on the very next tick through
    /// the tail, long before a full pass is due, and the tail leaves the
    /// durable cursor where the last full pass put it.
    #[test]
    fn a_new_message_is_ingested_by_the_tail_on_the_next_tick_without_a_full_pass() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-first", 20, Lifecycle::Completed);
        let state = test_state();
        let source = OpenCodeSource::at(&path);

        let first = source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(first.pass, Some(OpenCodePass::Full));
        write_lifecycle(&connection, "msg-new", 30, Lifecycle::Completed);
        let next = source.drain(&state, 200, &NEVER_STOP).unwrap();

        assert_eq!(next.pass, Some(OpenCodePass::Tail));
        assert_eq!(next.rows_recorded, 1);
        assert_eq!(stored_measurements(&state, "msg-new"), final_measurements());
        assert_eq!(
            runtime_cursor(&state),
            ("msg-first".to_string(), 20, 20),
            "only a full pass may move the durable cursor"
        );
    }

    /// Tokens-final through the tail: a row the tail saw in flight is looked
    /// up again by rowid and ingested once final, with its final figures.
    #[test]
    fn an_open_row_the_tail_saw_is_ingested_by_the_tail_once_final() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        write_lifecycle(&connection, "msg-first", 20, Lifecycle::Completed);
        let state = test_state();
        let source = OpenCodeSource::at(&path);
        source.drain(&state, 100, &NEVER_STOP).unwrap();

        write_lifecycle(&connection, "msg-streaming", 30, Lifecycle::InFlight);
        let streaming = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(streaming.pass, Some(OpenCodePass::Tail));
        assert!(stored_measurements(&state, "msg-streaming").is_empty());

        write_lifecycle(&connection, "msg-streaming", 40, Lifecycle::Completed);
        let completed = source.drain(&state, 300, &NEVER_STOP).unwrap();
        assert_eq!(completed.pass, Some(OpenCodePass::Tail));
        assert_eq!(completed.quarantined_redeliveries, 0);
        assert_eq!(
            stored_measurements(&state, "msg-streaming"),
            final_measurements()
        );
    }

    /// Drives a store of `rows` finished messages to a steady state and
    /// returns the SQLite cost of a tick that finds nothing new.
    fn steady_state_tick_cost(rows: i64) -> OpenCodeScanCost {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let mut connection = create_database(&path);
        let tx = connection.transaction().unwrap();
        for index in 0..rows {
            insert_message(&tx, &format!("msg-{index}"), "session-1", index, index + 1);
        }
        tx.commit().unwrap();
        drop(connection);
        let state = test_state();
        let source = OpenCodeSource::at(&path);

        assert_eq!(
            source.drain(&state, 100, &NEVER_STOP).unwrap().pass,
            Some(OpenCodePass::Full)
        );
        // The first tail read in a process reads the seed window.
        assert_eq!(
            source.drain(&state, 200, &NEVER_STOP).unwrap().pass,
            Some(OpenCodePass::Tail)
        );
        let steady = source.drain(&state, 300, &NEVER_STOP).unwrap();
        assert_eq!(steady.pass, Some(OpenCodePass::Tail));
        assert_eq!(steady.rows_recorded + steady.exact_redeliveries, 0);
        steady.cost
    }

    /// With nothing new, a tick's work does not grow with the table. Asserted
    /// on SQLite's own step counters, not on time.
    #[test]
    fn a_steady_state_tick_does_not_scan_the_message_table() {
        let small = steady_state_tick_cost(10);
        let large = steady_state_tick_cost(5_000);

        assert_eq!(large.fullscan_steps, 0, "no full scan of message");
        assert!(large.vm_steps > 0, "the tail read did run");
        assert_eq!(
            large, small,
            "the same tick must cost the same at 10 and at 5,000 rows"
        );
    }

    /// A store replaced by overwriting the file in place keeps its inode, is
    /// no smaller and has more rows, so the inode, shrink and rowid
    /// regression checks are all blind to it, as they are on a file system
    /// that hands a recreated file the inode number just freed. Only the
    /// front-row fingerprint read from the content can notice, and it must
    /// turn the next tick into a full pass that resets the lineage.
    #[test]
    fn a_replaced_store_resets_lineage_on_the_next_tick_by_its_content() {
        use std::io::Write;

        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let original = create_database(&path);
        insert_message(&original, "old-store-first", "session-old", 10, 20);
        drop(original);
        let state = test_state();
        let source = OpenCodeSource::at(&path);
        source.drain(&state, 100, &NEVER_STOP).unwrap();
        assert_eq!(
            source.drain(&state, 150, &NEVER_STOP).unwrap().pass,
            Some(OpenCodePass::Tail)
        );
        assert_eq!(runtime_cursor(&state).0, "old-store-first");

        let staged = temp.path().join("replacement.db");
        let replacement = create_database(&staged);
        insert_message(&replacement, "new-store-first", "session-new", 1, 5);
        insert_message(&replacement, "new-store-second", "session-new", 2, 6);
        insert_message(&replacement, "new-store-third", "session-new", 3, 7);
        drop(replacement);
        let database = fs::File::open(&path).unwrap();
        let before = FileSignature::capture(&database, &database.metadata().unwrap());
        let bytes = fs::read(&staged).unwrap();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&bytes).unwrap();
        drop(file);
        let after = FileSignature::capture(&database, &database.metadata().unwrap());
        assert!(
            !after.replaces(&before),
            "the file metadata must not reveal the replacement, or this test proves nothing"
        );

        let outcome = source.drain(&state, 200, &NEVER_STOP).unwrap();
        assert_eq!(outcome.pass, Some(OpenCodePass::Full));
        assert_eq!(
            runtime_cursor(&state),
            ("new-store-first".to_string(), 7, 7)
        );
        assert_eq!(
            stored_measurements(&state, "new-store-third").len(),
            5,
            "the replacement's rows are ingested"
        );
    }
    struct BrocaEventSource;

    impl SourceClient for BrocaEventSource {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event: EventRecordRequest = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            let cursor = SourceCursor {
                lineage: "broca-test:1".to_string(),
                ordinal: 7,
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
    }

    #[test]
    fn opencode_read_failure_does_not_prevent_broca_or_pi_rows_committing_same_tick() {
        let temp = TempDir::new().unwrap();
        let unreadable_database = temp.path().join("database-is-a-directory");
        fs::create_dir(&unreadable_database).unwrap();
        let opencode = OpenCodeSource::at(unreadable_database);

        let pi_root = temp.path().join("pi");
        let pi_session = pi_root.join("encoded");
        fs::create_dir_all(&pi_session).unwrap();
        fs::write(
            pi_session.join("session.jsonl"),
            concat!(
                r#"{"type":"session","id":"pi-session","timestamp":"2026-09-01T00:00:00.000Z","cwd":"/pi/project"}"#,
                "\n",
                r#"{"type":"message","id":"entry-1","timestamp":"2026-09-01T00:00:01.000Z","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4","responseId":"pi-response","usage":{"input":11,"output":7,"cacheRead":13,"cacheWrite":17,"reasoning":2,"totalTokens":50}}}"#,
                "\n"
            ),
        )
        .unwrap();
        let pi = super::super::pi_source::PiSource::at(pi_root);
        let state = test_state();
        let broca = Producer {
            source_key: "broca".to_string(),
            client: Arc::new(BrocaEventSource),
        };

        run_iteration_with_file_sources(&state, &[broca], &opencode, &pi, &AtomicBool::new(false));

        let (broca_rows, pi_rows): (i64, i64) = state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT
                           (SELECT COUNT(*) FROM usage_event WHERE producer='broca'),
                           (SELECT COUNT(*) FROM usage_event WHERE producer='pi')",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(
            broca_rows, 1,
            "BROCA facts, not only its cursor, must commit"
        );
        assert_eq!(pi_rows, 1, "Pi facts must commit after OpenCode fails");
        assert!(state.producer_degraded_reason("opencode").is_some());
        assert!(state.producer_degraded_reason("broca").is_none());
        assert!(state.producer_degraded_reason("pi").is_none());
    }
    /// Runs `work` on its own thread, so that work which ignores the stop
    /// fails the test's bounded wait instead of hanging the suite, and
    /// returns a guard that sets `stop` and joins the thread when the test
    /// ends, also when an assertion fails before the test sets it.
    fn spawn_stoppable(
        stop: &Arc<AtomicBool>,
        work: impl FnOnce() + Send + 'static,
    ) -> crate::test_support::Background<()> {
        let stop = Arc::clone(stop);
        crate::test_support::Background::new(
            "the OpenCode drain",
            std::thread::spawn(work),
            move || stop.store(true, Ordering::Relaxed),
        )
    }

    /// A drain inside a statement that would run for hours must return
    /// `Stopped` once the stop is set. This proves only that the drain
    /// returns. It cannot show that nothing was committed; the next test
    /// covers that.
    #[test]
    fn stop_interrupts_a_drain_blocked_inside_the_page_statement() {
        let temp = TempDir::new().unwrap();
        let path = slow_page_database(temp.path());
        let state = Arc::new(test_state());
        let stop = Arc::new(AtomicBool::new(false));
        let (done, returned) = mpsc::channel();
        let mut drain = {
            let state = Arc::clone(&state);
            let stop_flag = Arc::clone(&stop);
            let path = path.clone();
            spawn_stoppable(&stop, move || {
                let result = OpenCodeSource::at(path).drain(&state, 100, &stop_flag);
                let _ = done.send(result);
            })
        };
        wait_until_drain_reaches_the_page_statement(&path);
        assert!(
            returned.try_recv().is_err(),
            "the drain finished before the stop, so the fixture was not slow and nothing was interrupted"
        );

        drain.signal_stop();
        // The fixture's statement runs for hours, so a drain that returns
        // within this bound was interrupted; the bound itself only stops a
        // drain that ignores the stop from hanging the suite.
        let result = returned.recv_timeout(STOP_BOUND).unwrap_or_else(|_| {
            panic!("the drain did not return within {STOP_BOUND:?} of the stop")
        });
        drain.stop_and_join();

        assert_eq!(result.unwrap_err(), SourceDrainError::Stopped);
    }

    /// Nothing leaks from an interrupted page: the cursor row is unchanged,
    /// no event or rejected delivery is written, and the stop does not
    /// register as a failure in health or in `source_runtime`. This runs the
    /// refresher's own iteration because that is where a drain result becomes
    /// health. A drain that returned on the stop but committed its page would
    /// pass the test above and fail this one.
    #[test]
    fn an_interrupted_page_commits_nothing_and_does_not_degrade_health() {
        let temp = TempDir::new().unwrap();
        let path = slow_page_database(temp.path());
        let state = Arc::new(test_state());
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO source_runtime
                     (source_key,source_module,desired_generation,lineage,consumed_ordinal,
                      high_water_ordinal,last_success_ms,pending_known,pending_unknown)
                     SELECT 'opencode','opencode',generation,'msg-slow',5,5,42,0,0
                     FROM desired_config WHERE id=1",
                    [],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
        let before = opencode_runtime_row(&state);
        let stop = Arc::new(AtomicBool::new(false));
        let (done, returned) = mpsc::channel();
        let mut iteration = {
            let state = Arc::clone(&state);
            let stop_flag = Arc::clone(&stop);
            let pi = super::super::pi_source::PiSource::at(temp.path().join("no-pi"));
            let path = path.clone();
            spawn_stoppable(&stop, move || {
                run_iteration_with_file_sources(
                    &state,
                    &[],
                    &OpenCodeSource::at(path),
                    &pi,
                    &stop_flag,
                );
                let _ = done.send(());
            })
        };
        wait_until_drain_reaches_the_page_statement(&path);
        assert!(
            returned.try_recv().is_err(),
            "the iteration finished before the stop, so nothing was interrupted"
        );
        iteration.signal_stop();
        returned.recv_timeout(STOP_BOUND).unwrap_or_else(|_| {
            panic!("the iteration did not return within {STOP_BOUND:?} of the stop")
        });
        iteration.stop_and_join();

        assert_eq!(opencode_runtime_row(&state), before);
        assert_eq!(ledger_count(&state, "SELECT COUNT(*) FROM usage_event"), 0);
        assert_eq!(
            ledger_count(&state, "SELECT COUNT(*) FROM rejected_delivery"),
            0
        );
        assert_eq!(state.producer_degraded_reason("opencode"), None);
    }

    /// A stop that arrives partway through ingesting a page rolls the whole
    /// page back: no event from it, and no cursor advance. The stop signal is
    /// driven by a counter rather than a clock so the stop lands exactly after
    /// two events have been written inside the transaction.
    #[test]
    fn stop_during_page_ingest_rolls_back_every_event_of_the_page() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = create_database(&path);
        for index in 0..5 {
            insert_message(
                &connection,
                &format!("msg-{index}"),
                "session-1",
                10 + index,
                20 + index,
            );
        }
        drop(connection);
        let state = test_state();
        crate::refresher::harness_drain::ensure_runtime_row(&state, OPENCODE_SOURCE_KEY).unwrap();
        let before = opencode_runtime_row(&state);
        let page = read_opencode_page(&path, None, 100, &NEVER_STOP).unwrap();
        let cursor = PageCursor {
            lineage: &page.lineage,
            consumed_ordinal: page.consumed_ordinal,
            high_water_ordinal: page.high_water_ordinal,
        };
        let requests = page
            .messages
            .clone()
            .into_iter()
            .map(map_message)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(requests.len(), 5);
        let asked = std::cell::Cell::new(0);

        let result = commit_page(&state, &requests, Some(&cursor), 100, &|| {
            asked.set(asked.get() + 1);
            asked.get() == 3
        });

        assert_eq!(result.unwrap_err(), SourceDrainError::Stopped);
        assert_eq!(
            asked.get(),
            3,
            "the stop must land after two events were written, not before the first"
        );
        assert_eq!(ledger_count(&state, "SELECT COUNT(*) FROM usage_event"), 0);
        assert_eq!(ledger_count(&state, "SELECT COUNT(*) FROM measurement"), 0);
        assert_eq!(opencode_runtime_row(&state), before);
    }

    fn ledger_totals(state: &SharedState) -> OpenCodeTotals {
        state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT
                           (SELECT COUNT(*) FROM usage_event WHERE producer='opencode'),
                           COALESCE(SUM(CASE WHEN role='input' THEN quantity_units ELSE 0 END),0),
                           COALESCE(SUM(CASE WHEN role='output' THEN quantity_units ELSE 0 END),0),
                           COALESCE(SUM(CASE WHEN role='cache_read' THEN quantity_units ELSE 0 END),0),
                           COALESCE(SUM(CASE WHEN role='cache_write' THEN quantity_units ELSE 0 END),0),
                           COALESCE(SUM(CASE WHEN role='reasoning' THEN quantity_units ELSE 0 END),0)
                         FROM measurement
                         WHERE event_id IN (
                           SELECT event_id FROM usage_event WHERE producer='opencode'
                         )",
                        [],
                        |row| {
                            Ok(OpenCodeTotals {
                                records: row.get(0)?,
                                input: row.get(1)?,
                                output: row.get(2)?,
                                cache_read: row.get(3)?,
                                cache_write: row.get(4)?,
                                reasoning: row.get(5)?,
                            })
                        },
                    )
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    #[test]
    #[ignore = "reads and temporarily ingests the mutable ~/.local/share/opencode/opencode.db corpus"]
    fn live_opencode_raw_reader_and_ledger_populations_match_without_quarantine() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};

        let root = std::env::var_os("OPENCODE_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap())
                    .join(".local/share/opencode/opencode.db")
            });
        let temporary = TempDir::new().unwrap();
        let ledger_path = temporary.path().join("ledger.db");
        let descriptor = StorageDescriptor {
            module_id: "ck-astrocyte-opencode-live-test".to_string(),
            storage_namespace: "astrocyte".to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: ledger_path.to_string_lossy().into_owned(),
            },
        };
        let store = open_sqlite(&descriptor).unwrap();
        astrocyte_core::store::migrate_store(&store).unwrap();
        let state = SharedState::from_store(store).unwrap();
        let root_for_report = root.clone();
        let source = OpenCodeSource::at(root);
        let mut raw = OpenCodeTotals::default();
        let mut emitted = OpenCodeTotals::default();
        let mut quarantined = 0usize;
        let mut previous_cursor = None;

        for tick in 0..10_000_i64 {
            let outcome = source
                .drain(&state, 1_800_000_000_000 + tick, &NEVER_STOP)
                .unwrap();
            raw.add_assign_saturating(&outcome.raw_after_cursor);
            emitted.add_assign_saturating(&outcome.emitted_after_cursor);
            quarantined = quarantined.saturating_add(outcome.quarantined_redeliveries);
            let consumed = outcome.consumed_ordinal.unwrap();
            let high_water = outcome.high_water_ordinal.unwrap();
            if consumed == high_water {
                break;
            }
            assert!(
                previous_cursor.is_none_or(|previous| consumed > previous),
                "the live drain must make durable progress"
            );
            previous_cursor = Some(consumed);
            assert!(tick < 9_999, "live drain did not converge");
        }

        let ledger = ledger_totals(&state);
        let corpus = Connection::open_with_flags(
            format!("file:{}?mode=ro", root_for_report.display()),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();
        let mut by_kind = corpus
            .prepare(
                "SELECT
                   CASE
                     WHEN json_extract(data,'$.time.completed') IS NOT NULL THEN 'completed'
                     WHEN json_extract(data,'$.finish') IS NOT NULL THEN 'finish-only (included)'
                     ELSE 'neither (excluded)'
                   END,
                   COUNT(*),
                   SUM(COALESCE(json_extract(data,'$.tokens.input'),0)
                       + COALESCE(json_extract(data,'$.tokens.output'),0)
                       + COALESCE(json_extract(data,'$.tokens.reasoning'),0)
                       + COALESCE(json_extract(data,'$.tokens.cache.read'),0)
                       + COALESCE(json_extract(data,'$.tokens.cache.write'),0) > 0)
                 FROM message
                 WHERE json_extract(data,'$.role')='assistant'
                   AND json_extract(data,'$.tokens') IS NOT NULL
                 GROUP BY 1 ORDER BY 1",
            )
            .unwrap();
        let kinds = by_kind
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (kind, rows, nonzero) in kinds {
            eprintln!("OpenCode assistant rows {kind}: {rows} ({nonzero} with nonzero tokens)");
        }
        eprintln!("OpenCode RAW population: {raw:#?}");
        eprintln!("OpenCode READER-EMITTED population: {emitted:#?}");
        eprintln!("OpenCode LEDGER population: {ledger:#?}");
        eprintln!("OpenCode quarantines: {quarantined}");

        // These populations fence different boundaries and cannot substitute
        // for one another: RAW fences the SQL predicate, READER-EMITTED fences
        // decoding and mapping, and LEDGER fences event identity and ingest.
        assert_eq!(raw, emitted);
        assert_eq!(emitted, ledger);
        assert_eq!(quarantined, 0);
    }

    /// This process's total CPU time in milliseconds, as `ps` reports it
    /// (centisecond resolution on macOS).
    fn process_cpu_ms() -> i64 {
        let output = std::process::Command::new("ps")
            .args(["-o", "cputime=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        let text = String::from_utf8(output.stdout).unwrap();
        // Formats seen: "0:01.23", "1:02:03.45", "1-02:03:04".
        let text = text.trim();
        let (days, clock) = text.split_once('-').unwrap_or(("0", text));
        let mut seconds = days.parse::<f64>().unwrap() * 86_400.0;
        for field in clock.split(':') {
            seconds = seconds * 60.0 + field.parse::<f64>().unwrap();
        }
        (seconds * 1_000.0).round() as i64
    }

    fn wall_clock_ms() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    }

    /// Measures what a caught-up drain costs per tick against the live
    /// OpenCode database, opened read-only, into a temporary ledger. The
    /// temporary ledger's cursor is seeded at the live lineage and high water
    /// so it starts caught up without replaying the whole corpus; the SQLite
    /// work of a tick does not depend on how many events the ledger already
    /// holds. Knobs: OPENCODE_DB, OPENCODE_TICKS (default 10) and
    /// OPENCODE_TICK_MS (pause between ticks, default 5000).
    #[test]
    #[ignore = "reads the live ~/.local/share/opencode/opencode.db read-only and prints per-tick cost"]
    fn live_steady_state_tick_cost() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};

        let root = std::env::var_os("OPENCODE_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap())
                    .join(".local/share/opencode/opencode.db")
            });
        let ticks: usize = std::env::var("OPENCODE_TICKS")
            .map(|value| value.parse().unwrap())
            .unwrap_or(10);
        let pause_ms: u64 = std::env::var("OPENCODE_TICK_MS")
            .map(|value| value.parse().unwrap())
            .unwrap_or(5_000);
        let temporary = TempDir::new().unwrap();
        let descriptor = StorageDescriptor {
            module_id: "ck-astrocyte-opencode-cost-test".to_string(),
            storage_namespace: "astrocyte".to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: temporary
                    .path()
                    .join("ledger.db")
                    .to_string_lossy()
                    .into_owned(),
            },
        };
        let store = open_sqlite(&descriptor).unwrap();
        astrocyte_core::store::migrate_store(&store).unwrap();
        let state = SharedState::from_store(store).unwrap();

        let probe = read_opencode_page(&root, None, 1, &NEVER_STOP).unwrap();
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO source_runtime
                     (source_key,source_module,desired_generation,lineage,consumed_ordinal,
                      high_water_ordinal,last_success_ms,pending_known,pending_unknown)
                     SELECT 'opencode','opencode',generation,?1,?2,?2,0,0,0
                     FROM desired_config WHERE id=1",
                    params![probe.lineage, probe.high_water_ordinal],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
        eprintln!(
            "seeded cursor lineage={} ordinal={}",
            probe.lineage, probe.high_water_ordinal
        );

        let source = OpenCodeSource::at(&root);
        let mut quarantined = 0usize;
        for tick in 0..ticks {
            let cpu_before = process_cpu_ms();
            let outcome = source.drain(&state, wall_clock_ms(), &NEVER_STOP).unwrap();
            let cpu_ms = process_cpu_ms() - cpu_before;
            quarantined += outcome.quarantined_redeliveries;
            eprintln!(
                "tick {tick}: vm_steps={} fullscan_steps={} recorded={} exact={} quarantined={} cpu_ms={cpu_ms}",
                outcome.cost.vm_steps,
                outcome.cost.fullscan_steps,
                outcome.rows_recorded,
                outcome.exact_redeliveries,
                outcome.quarantined_redeliveries,
            );
            std::thread::sleep(Duration::from_millis(pause_ms));
        }
        assert_eq!(quarantined, 0);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod fixtures {
    use super::*;
    use rusqlite::Connection;
    use std::time::Duration;
    /// A finished turn as OpenCode stores it: `finish` and `time.completed`
    /// are both set, so the reader treats its tokens as final.
    pub(super) fn payload(
        agent: &str,
        root: &str,
        output: u64,
        reasoning: u64,
        occurred: i64,
    ) -> String {
        serde_json::json!({
            "role": "assistant",
            "agent": agent,
            "modelID": "claude-sonnet-4",
            "providerID": "anthropic",
            "path": {"root": root},
            "time": {"created": occurred, "completed": occurred + 1},
            "finish": "stop",
            "tokens": {
                "input": 3,
                "output": output,
                "reasoning": reasoning,
                "cache": {"read": 5, "write": 7}
            }
        })
        .to_string()
    }

    /// Iterations of the counting loop inside the slow fixture's page
    /// statement. Run by hand with the sqlite3 shell, the fixture's three
    /// preliminary queries (lineage, high water, page boundary) finished in
    /// 10 ms together, and its page statement took about 8 s at 5,000,000
    /// iterations. At this count it runs for hours, so a drain that returns
    /// within seconds did so because the statement was interrupted, not
    /// because it finished.
    const SLOW_PAGE_SPIN: i64 = 5_000_000_000;

    /// Hang guard on a stop ending a drain, counted from the stop. It never
    /// decides a healthy run: interrupting costs one watcher poll (20 ms) plus
    /// one SQLite step, two orders of magnitude below this even on a slow CI
    /// runner. It is also far below the fixture's natural running time, so a
    /// drain that returns within it was interrupted rather than finished.
    pub const STOP_BOUND: Duration = Duration::from_secs(10);

    /// An OpenCode database whose page statement runs for hours and whose
    /// other queries are instant. `message` is a view over one real finished
    /// turn. Its `data` column hides a scalar subquery that counts to
    /// `SLOW_PAGE_SPIN`. Only the page statement reads `data` (through
    /// json_extract), so only that statement pays for the count. This keeps
    /// the slow part inside the one statement a stop has to interrupt.
    pub fn slow_page_database(dir: &Path) -> PathBuf {
        let path = dir.join("opencode.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TABLE message_rows (
                     id TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL,
                     time_created INTEGER NOT NULL,
                     time_updated INTEGER NOT NULL,
                     data TEXT NOT NULL
                 );
                 CREATE VIEW message AS
                 SELECT id,session_id,time_created,time_updated,
                        CASE WHEN (
                            WITH RECURSIVE spin(i) AS (
                                SELECT 1 UNION ALL
                                SELECT i+1 FROM spin WHERE i<{SLOW_PAGE_SPIN}
                            )
                            SELECT count(*) FROM spin
                        )>0 THEN data END AS data
                 FROM message_rows;"
            ))
            .unwrap();
        connection
            .execute(
                "INSERT INTO message_rows (id,session_id,time_created,time_updated,data)
                 VALUES ('msg-slow','session-1',10,20,?1)",
                [payload("build", "/producer/root", 13, 17, 10)],
            )
            .unwrap();
        path
    }

    /// Block until a drain holds its read snapshot of `database`, the slow
    /// fixture.
    ///
    /// The drain checks its stop flag once before it opens the OpenCode
    /// database and returns `Stopped` there without running a statement, so a
    /// stop that landed that early would pass the stop tests without ever
    /// exercising the interrupt. Once the drain's read transaction has read
    /// from the file it holds SQLite's shared lock until the snapshot ends.
    /// From then on the only way out is an interrupt of a running statement:
    /// the preliminary queries finish in milliseconds, and the page statement
    /// runs for hours. Waiting on a fixed sleep instead let a loaded runner
    /// set the stop before the drain got this far.
    ///
    /// The lock is seen from a second connection: `BEGIN EXCLUSIVE` with no
    /// busy wait fails with SQLITE_BUSY exactly while another connection holds
    /// a shared lock. When it succeeds the probe rolls back at once, and the
    /// drain's connection waits out that instant with its own busy timeout.
    pub fn wait_until_drain_reaches_the_page_statement(database: &Path) {
        let probe = Connection::open(database).unwrap();
        probe.busy_timeout(Duration::ZERO).unwrap();
        crate::test_support::wait_until("the OpenCode drain opening its read snapshot", || {
            match probe.execute_batch("BEGIN EXCLUSIVE") {
                Ok(()) => {
                    probe.execute_batch("ROLLBACK").unwrap();
                    false
                }
                Err(error)
                    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) =>
                {
                    true
                }
                Err(error) => panic!("probing the OpenCode fixture's lock: {error}"),
            }
        });
    }

    type RuntimeRow = (
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        i64,
        i64,
    );

    pub fn opencode_runtime_row(state: &SharedState) -> RuntimeRow {
        state
            .with_conn(|connection| {
                connection
                    .query_row(
                        "SELECT lineage,consumed_ordinal,high_water_ordinal,last_success_ms,
                                pending_known,pending_unknown
                         FROM source_runtime WHERE source_key='opencode'",
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
            .unwrap()
    }
}
