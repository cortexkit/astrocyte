//! OpenCode sessions that thalamus-harness runs in private or shared servers,
//! and the read watermark that tells it when a finished session's folder or
//! rows have been recorded and may be erased. The binding contract is
//! `docs/thalamus-opencode-sessions-contract.md`.
//!
//! Each declared folder's `opencode.db` is read with the same OpenCode 1 and
//! OpenCode 2 readers as the user's own store, each with its own durable
//! cursor per folder. The folders are read on full refresher ticks only; the
//! fast lane only rechecks queued payer joins. Calls keep producer `opencode` and OpenCode's message id,
//! and carry the listed session's declaring module as `work_class`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_readers::sessions::opencode::OpenCodeReadError;
use astrocyte_readers::sessions::opencode_v2;
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;

use super::declared_roots::{self, Contained, RootsFile};
use super::harness_drain::{self, DrainMode, HarnessSource};
use super::opencode_source::{CursorHome, OpenCodeSource, ReadOptions, SessionOptions};
use super::opencode_v2_source::OpenCodeV2Source;
use super::SourceDrainError;
use crate::state::SharedState;

pub(crate) const SOURCE_KEY: &str = "opencode-thalamus";

/// The watermark trails the start of the complete pass it reports by this
/// margin, as the contract's section 4 fixes.
const WATERMARK_MARGIN_MS: i64 = 60_000;

/// `declared_store_cursor.layout` of each reader.
const LAYOUT_V1: &str = "opencode-1";
const LAYOUT_V2: &str = "opencode-2";

#[derive(Clone, Deserialize)]
struct Entry {
    path: PathBuf,
    #[serde(flatten)]
    store: Store,
}

#[derive(Clone)]
enum Store {
    Folder(Session),
    Shared { sessions: Vec<SharedSession> },
    MissingSharedKind,
}

impl<'de> Deserialize<'de> for Store {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value.get("kind") {
            None => match serde_json::from_value(value.clone()) {
                Ok(session) => Ok(Self::Folder(session)),
                // A shared declaration without its discriminator is refused
                // locally, rather than preventing other stores from reading.
                Err(_) if value.get("sessions").is_some() => Ok(Self::MissingSharedKind),
                Err(error) => Err(serde::de::Error::custom(error)),
            },
            Some(kind) if kind == "shared" => {
                #[derive(Deserialize)]
                struct Shared {
                    sessions: Vec<SharedSession>,
                }
                serde_json::from_value::<Shared>(value)
                    .map(|shared| Self::Shared {
                        sessions: shared.sessions,
                    })
                    .map_err(serde::de::Error::custom)
            }
            _ => Err(serde::de::Error::custom("unknown OpenCode store kind")),
        }
    }
}

#[derive(Clone, Deserialize)]
struct Session {
    session_ref: String,
    owner_principal: String,
    state: SessionState,
}

#[derive(Clone, Deserialize)]
struct SharedSession {
    session: String,
    #[serde(default, deserialize_with = "imported_through_seq")]
    imported_through_seq: Option<u64>,
    #[serde(flatten)]
    metadata: Session,
}

fn imported_through_seq<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    // Only absence means no boundary; an explicit null is not an integer.
    u64::deserialize(deserializer).map(Some)
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum SessionState {
    /// The session may still write, including while its server is stopped
    /// for idleness.
    Active,
    /// The session has ended for good; no further write can come.
    Done,
}

/// The two readers of one declared folder's `opencode.db`.
struct Folder {
    v1: OpenCodeSource,
    v2: OpenCodeV2Source,
    /// Since the folder or its latest done session was seen done, a full pass
    /// of each layout has reached its high water and recorded its final calls.
    settled: bool,
    /// Sessions already seen done. A newly done session requires a full pass
    /// even while the shared server continues writing other sessions.
    done_sessions: HashSet<String>,
}

impl Folder {
    fn payer_waiting(&self) -> usize {
        self.v1.payer_waiting() + self.v2.payer_waiting()
    }

    fn new(folder: &Path) -> Self {
        let database = folder.join("opencode.db");
        let store = folder.to_string_lossy().into_owned();
        let home = |layout| CursorHome::Declared {
            source_key: SOURCE_KEY,
            store: store.clone(),
            layout,
        };
        Self {
            v1: OpenCodeSource::declared(database.clone(), home(LAYOUT_V1)),
            v2: OpenCodeV2Source::declared(database, home(LAYOUT_V2)),
            settled: false,
            done_sessions: HashSet::new(),
        }
    }
}

/// What reading one declared folder found.
enum FolderRead {
    /// Its `opencode.db` is gone: erased, or not created yet.
    Gone,
    /// Read and committed.
    Read {
        /// Whether the store lets the watermark move: a done folder or newly
        /// done shared session must finish its settling pass first.
        recorded_all: bool,
        quarantined: usize,
    },
}

#[derive(Default)]
struct Cache {
    /// The last good entry list, kept while the roots file is unusable.
    entries: Vec<Entry>,
    /// Readers by canonical folder path.
    folders: HashMap<PathBuf, Folder>,
    warned: HashSet<String>,
}

impl Cache {
    fn payer_waiting(&self) -> usize {
        self.folders.values().map(Folder::payer_waiting).sum()
    }

    fn drain_payer_waits(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
        mode: DrainMode,
    ) -> Result<(), SourceDrainError> {
        let mut total = self.payer_waiting();
        for folder in self.folders.values() {
            let before = folder.v1.payer_waiting();
            folder.v1.drain_payer_waits(state, at, stop, mode)?;
            total = total.saturating_sub(before) + folder.v1.payer_waiting();
            state.set_payer_waiting(SOURCE_KEY, total);
            let before = folder.v2.payer_waiting();
            folder.v2.drain_payer_waits(state, at, stop, mode)?;
            total = total.saturating_sub(before) + folder.v2.payer_waiting();
            state.set_payer_waiting(SOURCE_KEY, total);
        }
        if self.folders.is_empty() {
            state.set_payer_waiting(SOURCE_KEY, 0);
        }
        Ok(())
    }
}

pub(crate) struct OpenCodeThalamusSource {
    roots: Result<PathBuf, String>,
    cache: Mutex<Cache>,
}

impl OpenCodeThalamusSource {
    pub(crate) fn from_data_home(home: PathBuf) -> Self {
        Self {
            roots: crate::harness_paths::opencode_sessions_roots(home),
            cache: Mutex::new(Cache::default()),
        }
    }

    /// One full pass over every declared folder. The watermark moves only when
    /// the roots file was usable and every declared folder that passes the
    /// containment rule and still has its `opencode.db` was read, and only in
    /// the transaction that records the pass, after every folder's own
    /// transaction has committed.
    fn drain(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        let roots = self
            .roots
            .as_ref()
            .map_err(|reason| SourceDrainError::Failed(reason.clone()))?;
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        // Queued payloads survive a missing store or declaration until they
        // resolve. Neither an idle tail nor a missing file may drop a call.
        cache.drain_payer_waits(state, at, stop, DrainMode::Full)?;
        let mut problems = Vec::new();
        let declarations_known =
            match declared_roots::read::<Entry>(roots, "OpenCode session roots") {
                RootsFile::Absent => {
                    if cache.payer_waiting() == 0 {
                        *cache = Cache::default();
                        forget_source(state)?;
                        return Ok(None);
                    }
                    false
                }
                RootsFile::UnknownVersion => {
                    harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
                    return Err("unknown OpenCode session roots version".to_string().into());
                }
                RootsFile::Unusable(reason) => {
                    problems.push(reason);
                    false
                }
                RootsFile::Declared(entries) => match check_paths_unique(&entries) {
                    Ok(()) => {
                        cache.entries = entries;
                        true
                    }
                    Err(reason) => {
                        problems.push(reason);
                        false
                    }
                },
            };
        harness_drain::ensure_runtime_row(state, SOURCE_KEY)?;
        let folder = std::fs::canonicalize(roots.parent().expect("roots parent"))
            .map_err(|e| format!("resolve OpenCode sessions folder: {e}"))?;

        // An unusable roots file may declare a folder the last good list
        // lacks, so its pass cannot release anything.
        let mut complete = declarations_known;
        let mut quarantined = 0;
        let mut seen = HashSet::new();
        let entries = cache.entries.clone();
        // Borrow the cache's fields apart, through the guard, so a folder's
        // reader and the warned set can both be held in the loop.
        let cache = &mut *cache;
        for entry in &entries {
            // A stop leaves every folder reader in the cache, so the next
            // pass resumes from the same in-memory positions.
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            let contained = match &entry.store {
                Store::Folder(_) => declared_roots::contain_opencode_session(&folder, &entry.path),
                Store::Shared { .. } => {
                    declared_roots::contain_opencode_shared(&folder, &entry.path)
                }
                Store::MissingSharedKind => {
                    warn_once(
                        &mut cache.warned,
                        format!(
                            "refuse shared OpenCode store without kind: {}",
                            entry.path.display()
                        ),
                    );
                    continue;
                }
            };
            let canonical = match contained {
                Ok(Contained::Inside(path)) => path,
                Ok(Contained::Refused(reason)) => {
                    warn_once(&mut cache.warned, reason);
                    continue;
                }
                Ok(Contained::Gone) => continue,
                Err(reason) => {
                    complete = false;
                    problems.push(reason);
                    continue;
                }
            };
            // Two entries can name different in-root paths that resolve to one
            // folder, for example through a symlink. The first is read; the
            // second is skipped, because reading it again could record the
            // same calls under another owner. The owner is then ambiguous, so
            // the pass cannot release the watermark.
            if !seen.insert(canonical.clone()) {
                complete = false;
                problems.push(format!(
                    "two OpenCode session entries resolve to one folder: {}",
                    canonical.display()
                ));
                continue;
            }
            let other_waiting = cache.payer_waiting().saturating_sub(
                cache
                    .folders
                    .get(&canonical)
                    .map_or(0, Folder::payer_waiting),
            );
            let reader = cache
                .folders
                .entry(canonical.clone())
                .or_insert_with(|| Folder::new(&canonical));
            let database = canonical.join("opencode.db");
            let read = read_folder(reader, entry, &database, state, at, stop, other_waiting);
            match read {
                Ok(FolderRead::Gone) => {}
                Ok(FolderRead::Read {
                    recorded_all,
                    quarantined: count,
                }) => {
                    complete &= recorded_all;
                    quarantined += count;
                }
                Err(error @ (SourceDrainError::Stopped | SourceDrainError::Deferred)) => {
                    return Err(error)
                }
                // The database vanished while it was read: the harness module
                // erased it, which neither holds the watermark nor degrades.
                Err(SourceDrainError::Failed(_)) if gone(&database) => {}
                Err(SourceDrainError::Failed(reason)) => {
                    complete = false;
                    let reason = format!(
                        "read OpenCode session {} at {}: {reason}",
                        match &entry.store {
                            Store::Folder(session) => session.session_ref.as_str(),
                            Store::Shared { .. } | Store::MissingSharedKind => "shared store",
                        },
                        database.display()
                    );
                    warn_once(&mut cache.warned, reason.clone());
                    problems.push(reason);
                }
            }
        }
        cache.folders.retain(|path, folder| {
            seen.contains(path) || folder.v1.has_payer_waits() || folder.v2.has_payer_waits()
        });
        complete &= cache.payer_waiting() == 0;
        let stores = declarations_known.then(|| {
            seen.iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<HashSet<_>>()
        });

        let watermark = complete.then(|| at.saturating_sub(WATERMARK_MARGIN_MS));
        state.ordered_write(|tx| {
            record_pass_in_tx(tx, at, complete, stores.as_ref())?;
            store_watermark_in_tx(tx, watermark, at)?;
            Ok(((), true))
        })?;
        if quarantined > 0 {
            problems.push(format!(
                "{quarantined} OpenCode session deliveries were quarantined"
            ));
        }
        Ok((!problems.is_empty()).then(|| problems.join("; ")))
    }
}

impl HarnessSource for OpenCodeThalamusSource {
    fn source_key(&self) -> &'static str {
        SOURCE_KEY
    }

    // Fast ticks resolve only cached payloads, without reading roots or stores.
    fn drain_fast(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<(), SourceDrainError> {
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain_payer_waits(state, at, stop, DrainMode::Fast)
    }

    fn drain_for_health(
        &self,
        state: &SharedState,
        at: i64,
        stop: &AtomicBool,
    ) -> Result<Option<String>, SourceDrainError> {
        self.drain(state, at, stop)
    }
}

/// Read one declared folder's `opencode.db`, both layouts, and commit what
/// was read.
fn read_folder(
    folder: &mut Folder,
    entry: &Entry,
    database: &Path,
    state: &SharedState,
    at: i64,
    stop: &AtomicBool,
    other_waiting: usize,
) -> Result<FolderRead, SourceDrainError> {
    match astrocyte_readers::read_io::metadata(database) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(FolderRead::Gone),
        Err(error) => return Err(format!("inspect {}: {error}", database.display()).into()),
    }
    let mut sessions = HashMap::new();
    let mut imported = HashMap::new();
    if let Store::Shared { sessions: listed } = &entry.store {
        for session in listed {
            imported
                .entry(session.session.as_str())
                .or_insert(session.imported_through_seq);
            sessions
                .entry(session.session.clone())
                .or_insert(SessionOptions {
                    work_class: &session.metadata.owner_principal,
                    settled: session.metadata.state == SessionState::Done,
                });
        }
    }
    let done_sessions = sessions
        .iter()
        .filter(|(_, options)| options.settled)
        .map(|(session, _)| session.clone())
        .collect::<HashSet<_>>();
    let done =
        matches!(&entry.store, Store::Folder(session) if session.state == SessionState::Done);
    if matches!(&entry.store, Store::Folder(_)) && !done
        || !done_sessions.is_subset(&folder.done_sessions)
    {
        folder.settled = false;
    }
    folder.done_sessions = done_sessions;
    // A final full pass finds calls the tail could have missed, including
    // incomplete rows below the cursor in a newly done shared session.
    let settling = (done || !folder.done_sessions.is_empty()) && !folder.settled;
    if settling {
        folder.v1.require_full_pass();
        folder.v2.require_full_pass();
    }
    let options = ReadOptions {
        work_class: match &entry.store {
            Store::Folder(session) => Some(session.owner_principal.as_str()),
            Store::Shared { .. } | Store::MissingSharedKind => None,
        },
        settled: done,
        sessions: matches!(&entry.store, Store::Shared { .. }).then_some(&sessions),
    };
    let has_v1 = opencode_v2::has_v1_message_table(database, stop).map_err(read_error)?;
    let mut quarantined = 0;
    if has_v1 {
        let outcome = folder.v1.drain_declared(state, at, stop, options)?;
        if outcome.root_absent {
            return Ok(FolderRead::Gone);
        }
        quarantined = outcome.quarantined_redeliveries;
        // A v2 read can fail after the v1 recording transaction commits.
        // Publish the v1 reader's updated in-memory queue count now, including
        // waits in other layouts and folders.
        state.set_payer_waiting(SOURCE_KEY, other_waiting + folder.payer_waiting());
    }
    let outcome = folder
        .v2
        .drain_declared(state, at, stop, options, &|session| {
            imported.get(session).copied().flatten()
        })?;
    tracing::debug!(target: "drain", recorded = outcome.recorded,
        skipped_imported = outcome.skipped_imported, skipped_recorded = outcome.skipped_recorded,
        "declared OpenCode 2 drain");
    state.set_payer_waiting(SOURCE_KEY, other_waiting + folder.payer_waiting());
    if settling {
        folder.settled = (!has_v1 || folder.v1.full_pass_reached_high_water())
            && folder.v2.full_pass_reached_high_water();
    }
    Ok(FolderRead::Read {
        recorded_all: (!settling || folder.settled)
            && !folder.v1.has_payer_waits()
            && !folder.v2.has_payer_waits(),
        quarantined,
    })
}

fn read_error(error: OpenCodeReadError) -> SourceDrainError {
    match error {
        OpenCodeReadError::Stopped => SourceDrainError::Stopped,
        OpenCodeReadError::Failed(reason) => SourceDrainError::Failed(reason),
    }
}

fn gone(path: &Path) -> bool {
    matches!(
        astrocyte_readers::read_io::metadata(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    )
}

/// Entries are keyed by path, so two entries with the same declared path
/// leave the folder's owner ambiguous, and the file is then treated as
/// malformed. This compares the declared text only. Different paths that
/// resolve to one folder are caught after containment, in `drain`.
fn check_paths_unique(entries: &[Entry]) -> Result<(), String> {
    let mut paths = HashSet::new();
    if entries.iter().all(|entry| paths.insert(&entry.path)) {
        Ok(())
    } else {
        Err("OpenCode session roots name one folder twice".into())
    }
}

fn warn_once(warned: &mut HashSet<String>, reason: String) {
    if warned.insert(reason.clone()) {
        tracing::warn!(target: "drain", producer = SOURCE_KEY, %reason, "skip declared OpenCode session folder");
    }
}

/// No roots file means no sessions: no runtime row, and no folder cursors.
/// The watermark stays as it was.
fn forget_source(state: &SharedState) -> Result<(), String> {
    state.ordered_write(|tx| {
        let changed = tx
            .execute(
                "DELETE FROM source_runtime WHERE source_key=?1",
                [SOURCE_KEY],
            )
            .map_err(|e| format!("remove absent {SOURCE_KEY} source runtime: {e}"))?
            + tx.execute(
                "DELETE FROM declared_store_cursor WHERE source_key=?1",
                [SOURCE_KEY],
            )
            .map_err(|e| format!("remove {SOURCE_KEY} folder cursors: {e}"))?;
        Ok(((), changed > 0))
    })
}

/// Record the pass's success, and with a usable roots file drop the cursors of
/// folders no longer declared. A new session always gets a new folder, so a
/// removed folder's cursor is never needed again.
fn record_pass_in_tx(
    tx: &rusqlite::Transaction<'_>,
    at: i64,
    complete: bool,
    stores: Option<&HashSet<String>>,
) -> Result<(), String> {
    let changed = tx
        .execute(
            "UPDATE source_runtime SET last_success_ms=?2,pending_known=0,pending_unknown=?3
             WHERE source_key=?1",
            params![SOURCE_KEY, at, i64::from(!complete)],
        )
        .map_err(|e| format!("record {SOURCE_KEY} pass: {e}"))?;
    if changed != 1 {
        return Err(format!(
            "{SOURCE_KEY} source runtime row disappeared before the pass committed"
        ));
    }
    let Some(stores) = stores else {
        return Ok(());
    };
    let recorded = {
        let mut statement = tx
            .prepare("SELECT DISTINCT store_path FROM declared_store_cursor WHERE source_key=?1")
            .map_err(|e| e.to_string())?;
        let rows = statement
            .query_map([SOURCE_KEY], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        rows
    };
    for store in recorded.iter().filter(|store| !stores.contains(*store)) {
        tx.execute(
            "DELETE FROM declared_store_cursor WHERE source_key=?1 AND store_path=?2",
            params![SOURCE_KEY, store],
        )
        .map_err(|e| format!("remove undeclared folder cursor: {e}"))?;
    }
    Ok(())
}

/// Move the stored watermark forward to `watermark`, never back: a clock
/// that stepped backwards cannot lower it.
fn store_watermark_in_tx(
    tx: &rusqlite::Transaction<'_>,
    watermark: Option<i64>,
    at: i64,
) -> Result<(), String> {
    let Some(watermark) = watermark else {
        return Ok(());
    };
    tx.execute(
        "INSERT INTO read_watermark (source_key,watermark_ms,updated_at_ms) VALUES (?1,?2,?3)
         ON CONFLICT(source_key) DO UPDATE SET
             watermark_ms=MAX(watermark_ms,excluded.watermark_ms),
             updated_at_ms=excluded.updated_at_ms",
        params![SOURCE_KEY, watermark, at],
    )
    .map_err(|e| format!("store {SOURCE_KEY} read watermark: {e}"))?;
    Ok(())
}

/// The read watermark in Unix milliseconds: every call in a declared folder
/// whose `done_at_ms` is at or before it has been recorded. `None` until the
/// first complete pass. `watermark.get` reads it on the store's read-only
/// connection (see `ops::watermark_get`).
pub(crate) fn watermark_ms(connection: &rusqlite::Connection) -> Result<Option<i64>, String> {
    connection
        .query_row(
            "SELECT watermark_ms FROM read_watermark WHERE source_key=?1",
            [SOURCE_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| format!("read {SOURCE_KEY} read watermark: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::opencode_source::OpenCodeSource;
    use crate::refresher::run_iteration_with_harness;
    use crate::state::test_state;
    use astrocyte_core::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
    use astrocyte_readers::sessions::opencode_v2::QUIET_WINDOW;
    use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse, StatusRequest, StatusResponse};
    use rusqlite::Connection;
    use serde_json::{json, Value};
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    const ONE: &str = "s-0001";
    const TWO: &str = "s-0002";
    const OWNER: &str = "prefrontal-core";
    /// When the fixture calls were made, and when the first tick reads them.
    const CREATED: i64 = 1_700_000_000_000;
    const T0: i64 = 1_800_000_000_000;
    const TICK: i64 = 30_000;

    /// A fixed dollar per million tokens makes every billed token worth 1,000
    /// nanodollars, independently of the provider's live catalog.
    struct DollarCatalog;

    impl CatalogConnection for DollarCatalog {
        fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(json!({"source":"fixed", "catalog_version":1,
                "model_count":1, "era_count":1, "recent_polls":[]}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            if request.at_ms == Some(0) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: format!("no record at 0: fusiform's history begins at {CREATED}. An empty answer would claim the catalog was empty; it was unobserved."),
                });
            }
            let rate = json!({"state":"priced", "units":1_000_000_000_i64,
                "exponent":9, "currency":"USD", "unit_provenance":{"kind":"stated"}});
            Ok(
                serde_json::from_value(json!({"source":"fixed", "catalog_version":1,
                "resolved_at_ms": request.at_ms,
                "models":{"anthropic/claude-sonnet-4":{
                    "rate.input":rate, "rate.output":rate, "rate.reasoning":rate,
                    "rate.cache_read":rate, "rate.cache_write":rate}},
                "withheld":[], "uncertain":[], "overridden":[]}))
                .unwrap(),
            )
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Layout {
        /// OpenCode 1's `message` table.
        V1,
        /// OpenCode 2's `session_message` table, without OpenCode 1's table.
        V2,
    }
    const LAYOUTS: [Layout; 2] = [Layout::V1, Layout::V2];

    struct Fixture {
        temp: TempDir,
        folder: PathBuf,
        roots: PathBuf,
        source: OpenCodeThalamusSource,
        state: Arc<SharedState>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_temp(TempDir::new().unwrap())
        }

        fn new_in(directory: &Path) -> Self {
            Self::with_temp(tempfile::tempdir_in(directory).unwrap())
        }

        fn with_temp(temp: TempDir) -> Self {
            let home = fs::canonicalize(temp.path()).unwrap();
            let folder = home.join("cortexkit/thalamus-harness/opencode-sessions");
            fs::create_dir_all(&folder).unwrap();
            Self {
                roots: folder.join("roots.json"),
                source: OpenCodeThalamusSource::from_data_home(home),
                temp,
                folder,
                state: Arc::new(test_state()),
            }
        }

        fn restart(&mut self) {
            self.source =
                OpenCodeThalamusSource::from_data_home(fs::canonicalize(self.temp.path()).unwrap());
        }

        fn entry(&self, session: &str, done: bool) -> Value {
            json!({"path": self.folder.join(session), "session_ref": session,
                "owner_principal": OWNER, "state": if done {"done"} else {"active"},
                "done_at_ms": if done {Some(T0)} else {None}, "future": true})
        }

        fn shared_path(&self) -> PathBuf {
            self.folder
                .parent()
                .unwrap()
                .join("opencode2-server/data/opencode")
        }

        fn shared_entry(&self, sessions: Vec<Value>) -> Value {
            json!({"path": self.shared_path(), "kind": "shared", "sessions": sessions})
        }

        fn shared_session(session: &str, done: bool) -> Value {
            json!({"session": session, "session_ref": format!("ref-{session}"),
                "owner_principal": OWNER, "state": if done {"done"} else {"active"},
                "done_at_ms": if done {Some(T0)} else {None}, "work_class": "ignored"})
        }

        fn shared_call(&self, session: &str, id: &str, complete: bool, updated: i64) {
            let database = self.shared_path().join("opencode.db");
            insert(
                &database,
                Layout::V2,
                id,
                "assistant",
                updated,
                &payload(Layout::V2, complete),
            );
            Connection::open(database)
                .unwrap()
                .execute(
                    "UPDATE session_message SET session_id=?1 WHERE id=?2",
                    params![session, id],
                )
                .unwrap();
        }

        fn declare(&self, entries: Vec<Value>) {
            fs::write(
                &self.roots,
                json!({"version":1,"roots":entries,"future":true}).to_string(),
            )
            .unwrap();
        }

        fn database(&self, session: &str) -> PathBuf {
            self.folder.join(session).join("opencode.db")
        }

        fn call(&self, session: &str, layout: Layout, id: &str, complete: bool, updated: i64) {
            insert(
                &self.database(session),
                layout,
                id,
                "assistant",
                updated,
                &payload(layout, complete),
            );
        }

        fn tick(&self, at: i64) {
            struct Clocked<'a>(&'a OpenCodeThalamusSource, i64);
            impl HarnessSource for Clocked<'_> {
                fn source_key(&self) -> &'static str {
                    SOURCE_KEY
                }
                fn drain_for_health(
                    &self,
                    state: &SharedState,
                    _: i64,
                    stop: &AtomicBool,
                ) -> Result<Option<String>, SourceDrainError> {
                    self.0.drain_for_health(state, self.1, stop)
                }
            }
            run_iteration_with_harness(
                &self.state,
                &[],
                &Clocked(&self.source, at),
                &AtomicBool::new(false),
            );
        }

        fn runtime(&self) -> Option<(Option<i64>, bool)> {
            self.state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT last_success_ms,pending_unknown FROM source_runtime
                         WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(|e| e.to_string())
                })
                .unwrap()
        }

        /// `(producer_ref, producer, work_class)` of every event.
        fn events(&self) -> Vec<(String, String, Option<String>)> {
            self.state
                .with_conn(|conn| {
                    let mut stmt = conn
                        .prepare(
                            "SELECT producer_ref,producer,work_class FROM usage_event
                             ORDER BY producer_ref",
                        )
                        .unwrap();
                    let rows = stmt
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                        .unwrap()
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .unwrap();
                    Ok(rows)
                })
                .unwrap()
        }

        fn refs(&self) -> Vec<String> {
            self.events().into_iter().map(|event| event.0).collect()
        }

        fn watermark(&self) -> Option<i64> {
            self.state.with_conn(watermark_ms).unwrap()
        }

        fn degraded(&self) -> Option<String> {
            self.state.producer_degraded_reason(SOURCE_KEY)
        }

        fn cursor_rows(&self) -> i64 {
            self.state
                .with_conn(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM declared_store_cursor", [], |row| {
                        row.get(0)
                    })
                    .map_err(|e| e.to_string())
                })
                .unwrap()
        }

        fn v2_floor(&self) -> Option<i64> {
            self.state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT open_row_floor FROM declared_store_cursor WHERE layout=?1",
                        [LAYOUT_V2],
                        |row| row.get(0),
                    )
                    .map_err(|e| e.to_string())
                })
                .unwrap()
        }

        fn sql(&self, sql: &str) {
            self.state
                .with_conn(|conn| conn.execute_batch(sql).map_err(|e| e.to_string()))
                .unwrap();
        }
    }

    fn store(path: &Path, layout: Layout) -> Connection {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(match layout {
            Layout::V1 => {
                "CREATE TABLE IF NOT EXISTS message(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                    time_created INTEGER NOT NULL,time_updated INTEGER NOT NULL,data TEXT NOT NULL);"
            }
            Layout::V2 => {
                "CREATE TABLE IF NOT EXISTS session_v2(id TEXT PRIMARY KEY,directory TEXT,path TEXT);
                CREATE TABLE IF NOT EXISTS session_message(id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,type TEXT NOT NULL,seq INTEGER NOT NULL,
                    time_created INTEGER NOT NULL,time_updated INTEGER NOT NULL,data TEXT NOT NULL);"
            }
        })
        .unwrap();
        conn
    }

    /// An assistant call with tokens; a complete one also has its completion
    /// and finish.
    fn payload(layout: Layout, complete: bool) -> Value {
        let mut value = match layout {
            Layout::V1 => json!({"role":"assistant","agent":"build","modelID":"claude-sonnet-4",
                "providerID":"anthropic","path":{"root":"/work"}}),
            Layout::V2 => json!({"type":"assistant","agent":"build",
                "model":{"providerID":"anthropic","id":"claude-sonnet-4"}}),
        };
        value["time"] = json!({"created": CREATED});
        value["tokens"] = json!({"input":3,"output":5,"reasoning":2,"cache":{"read":7,"write":11}});
        if complete {
            value["time"]["completed"] = json!(CREATED + 1);
            value["finish"] = json!("stop");
        }
        value
    }

    fn insert(path: &Path, layout: Layout, id: &str, kind: &str, updated: i64, data: &Value) {
        let conn = store(path, layout);
        let data = data.to_string();
        match layout {
            Layout::V1 => conn.execute(
                "INSERT INTO message VALUES (?1,'session',?2,?2,?3)",
                params![id, updated, data],
            ),
            Layout::V2 => conn.execute(
                "INSERT INTO session_message VALUES (?1,'session',?4,
                    (SELECT COALESCE(MAX(seq),0)+1 FROM session_message),?2,?2,?3)",
                params![id, updated, data, kind],
            ),
        }
        .unwrap();
    }

    /// The most rows one full pass of `layout` reads in one drain.
    fn page_limit(layout: Layout) -> usize {
        match layout {
            Layout::V1 => crate::refresher::opencode_source::PAGE_LIMIT,
            Layout::V2 => crate::refresher::opencode_v2_source::PAGE_LIMIT,
        }
    }

    /// Insert `count` user messages updated one millisecond apart, from
    /// `first_updated` on. A full pass counts every row towards its page but
    /// records none of these, so a page limit of them makes the pass need a
    /// second drain.
    fn fill(path: &Path, layout: Layout, count: usize, first_updated: i64) {
        let mut conn = store(path, layout);
        let tx = conn.transaction().unwrap();
        let seq: i64 = match layout {
            Layout::V1 => 0,
            Layout::V2 => tx
                .query_row(
                    "SELECT COALESCE(MAX(seq),0) FROM session_message",
                    [],
                    |row| row.get(0),
                )
                .unwrap(),
        };
        for index in 0..i64::try_from(count).unwrap() {
            let id = format!("fill-{index}");
            let updated = first_updated + index;
            match layout {
                Layout::V1 => tx.execute(
                    r#"INSERT INTO message VALUES (?1,'session',?2,?2,'{"role":"user"}')"#,
                    params![id, updated],
                ),
                Layout::V2 => tx.execute(
                    r#"INSERT INTO session_message
                       VALUES (?1,'session','user',?3,?2,?2,'{"type":"user"}')"#,
                    params![id, updated, seq + 1 + index],
                ),
            }
            .unwrap();
        }
        tx.commit().unwrap();
    }

    /// Rewrite a row in place, keeping its rowid.
    fn rewrite(path: &Path, layout: Layout, id: &str, kind: &str, updated: i64, data: &Value) {
        let conn = store(path, layout);
        let data = data.to_string();
        match layout {
            Layout::V1 => conn.execute(
                "UPDATE message SET time_updated=?2,data=?3 WHERE id=?1",
                params![id, updated, data],
            ),
            Layout::V2 => conn.execute(
                "UPDATE session_message SET type=?4,time_updated=?2,data=?3 WHERE id=?1",
                params![id, updated, data, kind],
            ),
        }
        .unwrap();
    }

    #[test]
    fn shared_calls_record_listed_owners_and_unlisted_usage() {
        let f = Fixture::new();
        f.state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(DollarCatalog)).unwrap(),
            ))
            .unwrap();
        f.shared_call("listed", "listed-call", true, CREATED);
        f.shared_call("unlisted", "unlisted-call", true, CREATED);
        f.declare(vec![
            f.shared_entry(vec![Fixture::shared_session("listed", false)])
        ]);
        f.tick(T0);
        assert_eq!(
            f.events(),
            vec![
                ("listed-call".into(), "opencode".into(), Some(OWNER.into())),
                ("unlisted-call".into(), "opencode".into(), None),
            ],
            "every shared-store call must be recorded, even without a session item"
        );
        assert_eq!(f.runtime(), Some((Some(T0), false)));
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
        assert_eq!(f.degraded(), None);

        for reference in ["listed-call", "unlisted-call"] {
            f.state.with_conn(|conn| {
                let mut stmt = conn.prepare("SELECT m.role,m.source,m.quantity_units,m.quantity_exponent,m.unit
                    FROM measurement m JOIN usage_event e USING(event_key)
                    WHERE e.producer_ref=?1 ORDER BY m.role").unwrap();
                let measurements = stmt.query_map([reference], |r| Ok((
                    r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,i64>(2)?,
                    r.get::<_,i64>(3)?, r.get::<_,String>(4)?
                ))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
                assert_eq!(measurements, vec![
                    ("cache_read".into(), "usage.cacheRead".into(), 7, 0, "token".into()),
                    ("cache_write".into(), "usage.cacheWrite".into(), 11, 0, "token".into()),
                    ("input".into(), "usage.input".into(), 3, 0, "token".into()),
                    ("output".into(), "usage.output".into(), 5, 0, "token".into()),
                    ("reasoning".into(), "usage.reasoning".into(), 2, 0, "token".into()),
                ]);
                let unpriced: i64 = conn.query_row("SELECT COUNT(*) FROM priced_charge p
                    JOIN usage_event e USING(event_key) WHERE e.producer_ref=?1
                    AND p.outcome='unpriced' AND p.unpriced_reason='charge_basis_unknown'
                    AND NOT EXISTS(SELECT 1 FROM priced_charge child WHERE child.supersedes=p.charge_key)",
                    [reference], |r| r.get(0)).unwrap();
                assert_eq!(unpriced, 5);
                Ok(())
            }).unwrap();

            // OpenCode does not attest who paid. Explicit API-key evidence is
            // required to price these otherwise unpriced, unknown-basis calls.
            let id = astrocyte_core::fact::event_id("opencode", None, reference).unwrap();
            crate::ops::dispatch(
                &f.state,
                crate::ops::OP_BASIS_CORRECT,
                json!({
                    "event_id":id, "corrected_basis":"metered_api", "reason":"API-key evidence"
                }),
            )
            .unwrap();
            f.state.with_conn(|conn| {
                let mut stmt = conn.prepare("SELECT p.role,p.outcome,p.charge_nanos FROM priced_charge p
                    JOIN usage_event e USING(event_key) WHERE e.producer_ref=?1
                    AND NOT EXISTS(SELECT 1 FROM priced_charge child WHERE child.supersedes=p.charge_key)
                    ORDER BY p.role").unwrap();
                let money = stmt.query_map([reference], |r| Ok((
                    r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,i64>(2)?
                ))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
                assert_eq!(money, vec![
                    ("cache_read".into(), "priced".into(), 7_000),
                    ("cache_write".into(), "priced".into(), 11_000),
                    ("input".into(), "priced".into(), 3_000),
                    ("output".into(), "priced".into(), 5_000),
                    ("reasoning".into(), "priced".into(), 2_000),
                ]);
                Ok(())
            }).unwrap();
        }
    }

    #[test]
    fn shared_paths_require_the_shared_kind_and_exact_resolved_location() {
        for relative in [
            "opencode2-server/data/opencode",
            "opencode2-server/data",
            "opencode2-server/data/opencode/x",
            "other/opencode2-server/data/opencode",
            "launch-head-runner/data/opencode",
            "opencode-sessions/private",
        ] {
            for shared in [false, true] {
                let accepted = if shared {
                    relative == "opencode2-server/data/opencode"
                } else {
                    relative.starts_with("launch-head-")
                        || relative.starts_with("opencode-sessions/")
                };
                let f = Fixture::new();
                f.call(ONE, Layout::V2, "good", true, CREATED);
                let path = f.folder.parent().unwrap().join(relative);
                insert(
                    &path.join("opencode.db"),
                    Layout::V2,
                    "candidate",
                    "assistant",
                    CREATED,
                    &payload(Layout::V2, true),
                );
                let mut entry = if shared {
                    f.shared_entry(vec![Fixture::shared_session("session", false)])
                } else {
                    f.entry(TWO, false)
                };
                entry["path"] = json!(path);
                f.declare(vec![entry, f.entry(ONE, false)]);
                f.tick(T0);
                let expected = if accepted {
                    vec!["candidate", "good"]
                } else {
                    vec!["good"]
                };
                assert_eq!(f.refs(), expected, "{relative}, shared={shared}");
                assert_eq!(f.degraded(), None);
                assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
                assert_eq!(
                    f.source.cache.lock().unwrap().warned.len(),
                    usize::from(!accepted)
                );
                f.tick(T0 + TICK);
                assert_eq!(
                    f.source.cache.lock().unwrap().warned.len(),
                    usize::from(!accepted)
                );
            }
        }
    }

    #[test]
    fn shared_declaration_without_kind_is_skipped_not_a_malformed_roots_file() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "good", true, CREATED);
        f.shared_call("session", "unmarked", true, CREATED);
        let mut shared = f.shared_entry(vec![]);
        shared.as_object_mut().unwrap().remove("kind");
        f.declare(vec![shared, f.entry(ONE, false)]);
        f.tick(T0);
        assert_eq!(f.refs(), vec!["good"]);
        assert_eq!(f.source.cache.lock().unwrap().warned.len(), 1);
        f.tick(T0 + TICK);
        assert_eq!(f.source.cache.lock().unwrap().warned.len(), 1);
        assert_eq!(f.degraded(), None);
        assert_eq!(f.watermark(), Some(T0 + TICK - WATERMARK_MARGIN_MS));
    }

    #[cfg(unix)]
    #[test]
    fn shared_path_uses_the_resolved_location_even_for_an_in_sessions_alias() {
        let f = Fixture::new();
        f.shared_call("session", "shared", true, CREATED);
        let alias = f.folder.join("alias");
        std::os::unix::fs::symlink(f.shared_path(), &alias).unwrap();
        let mut shared = f.shared_entry(vec![]);
        shared["path"] = json!(alias);
        f.declare(vec![shared]);
        f.tick(T0);
        assert_eq!(f.refs(), vec!["shared"]);
        assert_eq!(f.degraded(), None);
        let mut folder = f.entry(ONE, false);
        folder["path"] = json!(alias);
        f.declare(vec![folder]);
        f.tick(T0 + TICK);
        assert_eq!(f.source.cache.lock().unwrap().warned.len(), 1);
        assert_eq!(f.degraded(), None);
    }

    #[cfg(unix)]
    #[test]
    fn shared_path_symlink_cannot_escape_the_harness_parent() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "good", true, CREATED);
        f.shared_call("session", "escaped", true, CREATED);
        let path = f.shared_path();
        let outside = f.temp.path().join("outside");
        fs::rename(&path, &outside).unwrap();
        std::os::unix::fs::symlink(outside, &path).unwrap();
        f.declare(vec![f.shared_entry(vec![]), f.entry(ONE, false)]);
        f.tick(T0);
        assert_eq!(f.refs(), vec!["good"]);
        assert_eq!(f.source.cache.lock().unwrap().warned.len(), 1);
        assert_eq!(f.degraded(), None);
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
    }

    #[test]
    fn shared_interrupted_calls_settle_per_session_and_unlisted_calls_wait() {
        let f = Fixture::new();
        for session in ["active", "done", "unlisted"] {
            f.shared_call(session, session, false, T0);
        }
        f.declare(vec![f.shared_entry(vec![
            Fixture::shared_session("active", false),
            Fixture::shared_session("done", false),
            Fixture::shared_session("deleted", true),
        ])]);
        f.tick(T0);
        assert!(f.refs().is_empty());
        f.declare(vec![f.shared_entry(vec![
            Fixture::shared_session("active", false),
            Fixture::shared_session("done", true),
            Fixture::shared_session("deleted", true),
        ])]);
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["done"]);
        f.tick(T0 + QUIET_WINDOW - 1);
        assert_eq!(f.refs(), vec!["done"]);
        f.tick(T0 + QUIET_WINDOW);
        assert_eq!(f.refs(), vec!["active", "done", "unlisted"]);
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn unreadable_shared_store_holds_watermark_but_missing_store_does_not() {
        let f = Fixture::new();
        f.shared_call("session", "shared", true, CREATED);
        f.declare(vec![f.shared_entry(vec![]), f.entry(ONE, false)]);
        f.tick(T0);
        let released = f.watermark();
        assert_eq!(released, Some(T0 - WATERMARK_MARGIN_MS));
        fs::write(f.shared_path().join("opencode.db"), vec![7_u8; 4096]).unwrap();
        f.call(ONE, Layout::V2, "folder", true, CREATED);
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["folder", "shared"]);
        assert_eq!(f.watermark(), released);
        assert!(f.degraded().unwrap().contains("shared store"));
        assert_eq!(f.runtime(), Some((Some(T0 + TICK), true)));
        fs::remove_file(f.shared_path().join("opencode.db")).unwrap();
        f.tick(T0 + 2 * TICK);
        assert_eq!(f.watermark(), Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS));
        assert_eq!(f.degraded(), None);
        fs::remove_dir_all(f.shared_path()).unwrap();
        f.tick(T0 + 3 * TICK);
        assert_eq!(f.watermark(), Some(T0 + 3 * TICK - WATERMARK_MARGIN_MS));
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn shared_done_sessions_hold_watermark_until_their_final_full_pass_finishes() {
        let f = Fixture::new();
        f.shared_call("done", "first", true, CREATED);
        f.declare(vec![
            f.shared_entry(vec![Fixture::shared_session("done", false)])
        ]);
        f.tick(T0);
        let released = f.watermark();
        let count = page_limit(Layout::V2) + 2;
        let database = f.shared_path().join("opencode.db");
        fill(&database, Layout::V2, count, T0 + 1);
        f.shared_call(
            "done",
            "final",
            false,
            T0 + i64::try_from(count).unwrap() + 1,
        );
        f.declare(vec![
            f.shared_entry(vec![Fixture::shared_session("done", true)])
        ]);
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["first"]);
        assert_eq!(f.watermark(), released);
        assert_eq!(f.degraded(), None);
        f.tick(T0 + 2 * TICK);
        assert_eq!(f.refs(), vec!["final", "first"]);
        assert_eq!(f.watermark(), Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS));
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn shared_session_erasure_below_the_cursor_does_not_block_later_calls() {
        let mut f = Fixture::new();
        f.shared_call("keeper", "keeper", true, CREATED);
        f.shared_call("interrupted", "interrupted", false, T0 - 1);
        f.shared_call("done", "erased", true, T0);
        f.declare(vec![
            f.shared_entry(vec![Fixture::shared_session("done", true)])
        ]);
        f.tick(T0);
        assert_eq!(f.v2_floor(), Some(2));
        Connection::open(f.shared_path().join("opencode.db"))
            .unwrap()
            .execute("DELETE FROM session_message WHERE session_id='done'", [])
            .unwrap();
        f.declare(vec![
            f.shared_entry(vec![Fixture::shared_session("interrupted", true)])
        ]);
        f.restart();
        f.tick(T0 + TICK);
        assert_eq!(f.watermark(), Some(T0 + TICK - WATERMARK_MARGIN_MS));
        assert_eq!(f.degraded(), None);
        f.shared_call("keeper", "later", true, T0 + 2 * TICK);
        f.tick(T0 + 3 * TICK);
        assert_eq!(f.refs(), vec!["erased", "interrupted", "keeper", "later"]);
        assert_eq!(f.v2_floor(), Some(2));
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn shared_v2_floor_survives_restart_and_advances_after_completion() {
        let mut f = Fixture::new();
        f.shared_call("front", "front", true, CREATED);
        f.shared_call("pending", "pending", false, T0);
        f.declare(vec![
            f.shared_entry(vec![Fixture::shared_session("pending", false)])
        ]);
        f.tick(T0);
        assert_eq!(f.refs(), vec!["front"]);
        assert_eq!(f.v2_floor(), Some(2));
        f.shared_call("later", "later", true, T0 + TICK);
        f.restart();
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["front", "later"]);
        assert_eq!(f.v2_floor(), Some(2));

        f.sql("UPDATE declared_store_cursor SET open_row_floor=NULL");
        f.restart();
        f.tick(T0 + 2 * TICK);
        assert_eq!(f.v2_floor(), Some(2));
        let conn = Connection::open(f.shared_path().join("opencode.db")).unwrap();
        conn.execute(
            "UPDATE session_message SET data=?1,time_updated=?2 WHERE id='pending'",
            params![payload(Layout::V2, true).to_string(), T0 + 3 * TICK],
        )
        .unwrap();
        drop(conn);
        f.restart();
        f.tick(T0 + 3 * TICK);
        assert_eq!(f.refs(), vec!["front", "later", "pending"]);
        assert_eq!(f.v2_floor(), Some(3));
        let output: i64 = f
            .state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT SUM(quantity_units) FROM measurement WHERE role='output'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap();
        assert_eq!(output, 15);
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn shared_copy_cutoff_rejects_bad_values_as_unreadable_roots() {
        for bad in [
            json!(-1),
            json!(1.5),
            json!(1.0),
            json!("11"),
            json!(null),
            json!(true),
            json!([]),
            json!({}),
        ] {
            let f = Fixture::new();
            f.shared_call("copy", "history", true, T0);
            let mut session = Fixture::shared_session("copy", false);
            session["imported_through_seq"] = bad.clone();
            f.declare(vec![f.shared_entry(vec![session])]);
            f.tick(T0);
            assert!(f.events().is_empty(), "{bad}");
            assert!(
                f.degraded()
                    .unwrap()
                    .contains("decode OpenCode session roots"),
                "{bad}"
            );
            assert_eq!(f.watermark(), None, "{bad}");
        }
    }

    #[test]
    fn shared_copy_cutoff_accepts_zero_and_unsigned_maximum() {
        for through in [0, u64::MAX] {
            let mut value = Fixture::shared_session("copy", false);
            value["imported_through_seq"] = json!(through);
            let session: SharedSession = serde_json::from_value(value).unwrap();
            assert_eq!(session.imported_through_seq, Some(through));
        }
    }

    #[test]
    fn folder_entries_ignore_the_shared_copy_cutoff() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "folder-call", true, T0);
        let mut entry = f.entry(ONE, false);
        entry["imported_through_seq"] = json!("not a shared-session field");
        f.declare(vec![entry]);
        f.tick(T0);
        assert_eq!(
            f.events(),
            vec![("folder-call".into(), "opencode".into(), Some(OWNER.into()))]
        );
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn shared_copy_cutoff_skips_history_without_holding_progress() {
        let mut f = Fixture::new();
        for (session, id, seq, complete) in [
            ("copy", "copied-old", 2, true),
            ("copy", "copied-open", 8, false),
            ("copy", "copied-boundary", 11, true),
            ("copy", "live-first", 19, true),
            ("ordinary", "ordinary-call", 3, true),
            ("unlisted", "unlisted-call", 4, true),
        ] {
            f.shared_call(session, id, complete, T0);
            Connection::open(f.shared_path().join("opencode.db"))
                .unwrap()
                .execute(
                    "UPDATE session_message SET seq=?1 WHERE id=?2",
                    params![seq, id],
                )
                .unwrap();
        }
        let mut copy = Fixture::shared_session("copy", false);
        copy["owner_principal"] = json!("copy-owner");
        copy["imported_through_seq"] = json!(11);
        f.declare(vec![f.shared_entry(vec![
            copy,
            Fixture::shared_session("ordinary", false),
        ])]);
        f.tick(T0);
        assert_eq!(
            f.events(),
            vec![
                (
                    "live-first".into(),
                    "opencode".into(),
                    Some("copy-owner".into())
                ),
                (
                    "ordinary-call".into(),
                    "opencode".into(),
                    Some(OWNER.into())
                ),
                ("unlisted-call".into(), "opencode".into(), None),
            ]
        );
        assert_eq!(f.v2_floor(), Some(6));
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));

        f.shared_call("copy", "live-next", true, T0 + TICK);
        Connection::open(f.shared_path().join("opencode.db"))
            .unwrap()
            .execute("UPDATE session_message SET seq=25 WHERE id='live-next'", [])
            .unwrap();
        f.tick(T0 + TICK);
        f.restart();
        f.tick(T0 + QUIET_WINDOW);
        assert_eq!(
            f.refs(),
            vec!["live-first", "live-next", "ordinary-call", "unlisted-call"]
        );
        assert_eq!(f.v2_floor(), Some(7));
        assert_eq!(f.watermark(), Some(T0 + QUIET_WINDOW - WATERMARK_MARGIN_MS));
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn shared_imported_history_keeps_the_first_record_without_quarantine() {
        let mut f = Fixture::new();
        f.call(ONE, Layout::V2, "history", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0);
        f.shared_call("imported", "history", true, CREATED);
        f.shared_call("imported", "new", true, CREATED + 1);
        let mut session = Fixture::shared_session("imported", false);
        session["owner_principal"] = json!("other-owner");
        f.declare(vec![f.shared_entry(vec![session])]);
        f.tick(T0 + TICK);
        f.restart();
        f.tick(T0 + 2 * TICK);
        assert_eq!(
            f.events(),
            vec![
                ("history".into(), "opencode".into(), Some(OWNER.into())),
                ("new".into(), "opencode".into(), Some("other-owner".into())),
            ]
        );
        f.state
            .with_conn(|conn| {
                let rejected: i64 = conn
                    .query_row("SELECT COUNT(*) FROM rejected_delivery", [], |r| r.get(0))
                    .unwrap();
                let evidence: i64 = conn
                    .query_row("SELECT COUNT(*) FROM set_aside_record", [], |r| r.get(0))
                    .unwrap();
                let measurements: i64 = conn
                    .query_row("SELECT COUNT(*) FROM measurement", [], |r| r.get(0))
                    .unwrap();
                let original_session: String = conn
                    .query_row(
                        "SELECT session FROM usage_event WHERE producer_ref='history'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(rejected, 0);
                assert_eq!(evidence, 0);
                assert_eq!(measurements, 10);
                assert_eq!(original_session, "session");
                Ok(())
            })
            .unwrap();
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn declared_calls_keep_the_user_store_identity_and_carry_the_owner() {
        type Row = Vec<rusqlite::types::Value>;
        fn ledger(state: &SharedState) -> (Vec<Row>, Vec<Option<String>>, Vec<Row>) {
            state
                .with_conn(|conn| {
                    let rows = |sql: &str| {
                        let mut stmt = conn.prepare(sql).unwrap();
                        let columns = stmt.column_count();
                        let rows = stmt
                            .query_map([], |row| {
                                (0..columns)
                                    .map(|i| row.get(i))
                                    .collect::<rusqlite::Result<Row>>()
                            })
                            .unwrap()
                            .collect::<rusqlite::Result<Vec<_>>>()
                            .unwrap();
                        rows
                    };
                    let identity = rows(
                        "SELECT event_id,producer,producer_lineage,producer_ref,session,
                            project_root,occurred_at_ms,provider,model FROM usage_event
                         ORDER BY event_id",
                    );
                    let classes = rows("SELECT work_class FROM usage_event ORDER BY event_id")
                        .into_iter()
                        .map(|row| match &row[0] {
                            rusqlite::types::Value::Text(text) => Some(text.clone()),
                            _ => None,
                        })
                        .collect();
                    let measurements = rows(
                        "SELECT e.event_id,m.role,m.source,m.quantity_units FROM measurement m
                         JOIN usage_event e USING(event_key) ORDER BY 1,2",
                    );
                    Ok((identity, classes, measurements))
                })
                .unwrap()
        }
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "msg-one", true, CREATED);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);

            // The same database read as the user's own store.
            let user = test_state();
            let stop = AtomicBool::new(false);
            match layout {
                Layout::V1 => {
                    OpenCodeSource::at(f.database(ONE))
                        .drain(&user, T0, &stop)
                        .unwrap();
                }
                Layout::V2 => {
                    OpenCodeV2Source::at(f.database(ONE))
                        .drain(&user, T0, &stop)
                        .unwrap();
                }
            }
            let (declared, declared_classes, declared_measurements) = ledger(&f.state);
            let (own, own_classes, own_measurements) = ledger(&user);
            assert_eq!(declared.len(), 1, "{layout:?}");
            assert_eq!(declared, own, "{layout:?}: identity must not change");
            assert_eq!(declared_measurements, own_measurements, "{layout:?}");
            assert_eq!(own_classes, vec![Some("build".to_string())], "{layout:?}");
            assert_eq!(
                declared_classes,
                vec![Some(OWNER.to_string())],
                "{layout:?}"
            );
            assert_eq!(f.runtime(), Some((Some(T0), false)), "{layout:?}");
            assert_eq!(f.degraded(), None, "{layout:?}");
        }
    }

    #[test]
    fn runner_head_calls_are_recorded_with_the_declared_owner() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            let head = f
                .folder
                .parent()
                .unwrap()
                .join("launch-head-runner/data/opencode");
            insert(
                &head.join("opencode.db"),
                layout,
                "head-call",
                "assistant",
                CREATED,
                &payload(layout, true),
            );
            let mut entry = f.entry(ONE, false);
            entry["path"] = json!(head);
            f.declare(vec![entry]);
            f.tick(T0);
            assert_eq!(
                f.events(),
                vec![("head-call".into(), "opencode".into(), Some(OWNER.into()))],
                "{layout:?}"
            );
            assert_eq!(f.runtime(), Some((Some(T0), false)), "{layout:?}");
            assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS), "{layout:?}");
            assert_eq!(f.degraded(), None, "{layout:?}");

            insert(
                &head.join("opencode.db"),
                layout,
                "head-next",
                "assistant",
                CREATED + TICK,
                &payload(layout, true),
            );
            f.tick(T0 + TICK);
            assert_eq!(
                f.events(),
                vec![
                    ("head-call".into(), "opencode".into(), Some(OWNER.into())),
                    ("head-next".into(), "opencode".into(), Some(OWNER.into())),
                ],
                "{layout:?}"
            );
            assert_eq!(
                f.watermark(),
                Some(T0 + TICK - WATERMARK_MARGIN_MS),
                "{layout:?}"
            );
        }
    }

    #[test]
    fn a_moved_sessions_recorded_history_is_skipped_not_quarantined() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            let heads = f.folder.parent().unwrap().to_path_buf();
            let first = heads.join("launch-head-first/data/opencode");
            let moved = heads.join("launch-head-moved/data/opencode");
            insert(
                &first.join("opencode.db"),
                layout,
                "history",
                "assistant",
                CREATED,
                &payload(layout, true),
            );
            let mut entry = f.entry(ONE, false);
            entry["path"] = json!(first);
            f.declare(vec![entry.clone()]);
            f.tick(T0);

            // The session moves to another head under another owner, and its
            // new store carries the old call as well as a new one.
            fs::create_dir_all(&moved).unwrap();
            fs::copy(first.join("opencode.db"), moved.join("opencode.db")).unwrap();
            insert(
                &moved.join("opencode.db"),
                layout,
                "after-move",
                "assistant",
                CREATED + TICK,
                &payload(layout, true),
            );
            let mut moved_entry = f.entry(TWO, false);
            moved_entry["path"] = json!(moved);
            moved_entry["owner_principal"] = json!("other-owner");
            f.declare(vec![entry, moved_entry]);
            f.tick(T0 + TICK);

            assert_eq!(
                f.events(),
                vec![
                    (
                        "after-move".into(),
                        "opencode".into(),
                        Some("other-owner".into())
                    ),
                    ("history".into(), "opencode".into(), Some(OWNER.into())),
                ],
                "{layout:?}"
            );
            assert_eq!(f.degraded(), None, "{layout:?}");
            let rejected: i64 = f
                .state
                .with_conn(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |row| {
                        row.get(0)
                    })
                    .map_err(|e| e.to_string())
                })
                .unwrap();
            assert_eq!(rejected, 0, "{layout:?}");
        }
    }

    fn refuses_path(f: Fixture, path: impl FnOnce(&Fixture) -> PathBuf) {
        f.call(ONE, Layout::V2, "good", true, CREATED);
        f.call(TWO, Layout::V2, "bad", true, CREATED);
        let mut bad = f.entry(TWO, true);
        bad["path"] = json!(path(&f));
        f.declare(vec![bad, f.entry(ONE, false)]);
        f.tick(T0);
        assert_eq!(
            f.events(),
            vec![("good".into(), "opencode".into(), Some(OWNER.into()))]
        );
        assert_eq!(f.runtime(), Some((Some(T0), false)));
        assert_eq!(f.degraded(), None);
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
    }

    #[test]
    fn relative_entry_is_refused_without_blocking_other_entries() {
        // Windows may put the checkout and system temp directory on different
        // drives. A fixture under the canonical cwd has a real relative path.
        let cwd = fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        refuses_path(Fixture::new_in(&cwd), |f| {
            let target = f.folder.join(TWO);
            let relative = target.strip_prefix(&cwd).unwrap().to_path_buf();
            assert!(relative.is_relative());
            assert!(relative.exists());
            relative
        });
    }

    #[test]
    fn outside_entry_is_refused_without_blocking_other_entries() {
        refuses_path(Fixture::new(), |f| {
            let outside = f.temp.path().join("outside");
            fs::rename(f.folder.join(TWO), &outside).unwrap();
            outside
        });
    }

    #[test]
    fn the_sessions_folder_itself_is_refused() {
        refuses_path(Fixture::new(), |f| {
            fs::copy(f.database(TWO), f.folder.join("opencode.db")).unwrap();
            f.folder.clone()
        });
    }

    #[test]
    fn non_head_harness_paths_are_refused_without_blocking_other_entries() {
        for relative in [
            "",
            "launch-worker/data/opencode",
            "launch-head-x",
            "launch-head-x/data",
            "launch-head-x/opencode",
            "launch-head-x/data/opencode/deeper",
            "launch-head-/data/opencode",
            "other/launch-head-x/data/opencode",
            "launch-head-x/extra/data/opencode",
            "launch-head-x/data/other",
        ] {
            refuses_path(Fixture::new(), |f| {
                let path = f.folder.parent().unwrap().join(relative);
                fs::create_dir_all(&path).unwrap();
                fs::copy(f.database(TWO), path.join("opencode.db")).unwrap();
                path
            });
        }
    }

    #[cfg(unix)]
    #[test]
    fn escaping_symlink_is_refused_without_blocking_other_entries() {
        refuses_path(Fixture::new(), |f| {
            let outside = f.temp.path().join("outside");
            fs::rename(f.folder.join(TWO), &outside).unwrap();
            let link = f.folder.join("escape");
            std::os::unix::fs::symlink(outside, &link).unwrap();
            link
        });
    }

    #[cfg(unix)]
    #[test]
    fn escaping_head_symlink_is_refused_without_blocking_other_entries() {
        refuses_path(Fixture::new(), |f| {
            let outside = f.temp.path().join("outside/launch-head-runner");
            fs::create_dir_all(outside.join("data")).unwrap();
            fs::rename(f.folder.join(TWO), outside.join("data/opencode")).unwrap();
            let link = f.folder.parent().unwrap().join("launch-head-runner");
            std::os::unix::fs::symlink(outside, &link).unwrap();
            link.join("data/opencode")
        });
    }

    #[test]
    fn unknown_version_skips_entries_and_degrades_source() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "first", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0);
        f.call(ONE, Layout::V2, "new", true, CREATED);
        fs::write(
            &f.roots,
            json!({"version":2,"roots":[f.entry(ONE, false)]}).to_string(),
        )
        .unwrap();
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["first".to_string()]);
        assert_eq!(f.runtime(), Some((Some(T0), true)));
        assert!(f.degraded().is_some());
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
    }

    #[test]
    fn malformed_roots_read_the_last_good_entries_and_degrade() {
        for malformed in [
            b"{".to_vec(),
            json!({"roots":[]}).to_string().into_bytes(),
            json!({"version":1,"roots":[{"path":"/x"}]})
                .to_string()
                .into_bytes(),
        ] {
            let f = Fixture::new();
            f.call(ONE, Layout::V2, "first", true, CREATED);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            f.call(ONE, Layout::V2, "second", true, CREATED);
            fs::write(&f.roots, malformed).unwrap();
            f.tick(T0 + TICK);
            assert_eq!(f.refs(), vec!["first".to_string(), "second".to_string()]);
            assert_eq!(f.runtime(), Some((Some(T0 + TICK), true)));
            assert!(f.degraded().is_some());
            // A declaration the last good list lacks may be done already.
            assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
        }
    }

    #[test]
    fn two_entries_resolving_to_one_folder_read_it_once_and_hold_the_watermark() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.call(ONE, layout, "two", true, CREATED);
            // A second, different path to the same folder, under another owner.
            // It is built from the temporary directory's own path rather than
            // the canonical one: on Windows the canonical path is a verbatim
            // `\\?\` path, onto which a joined `..` is resolved as text.
            fs::create_dir_all(f.folder.join(TWO)).unwrap();
            let mut alias = f.entry(ONE, true);
            alias["path"] = json!(f
                .temp
                .path()
                .join("cortexkit/thalamus-harness/opencode-sessions")
                .join(TWO)
                .join("..")
                .join(ONE));
            alias["owner_principal"] = json!("magic-context");
            f.declare(vec![f.entry(ONE, true), alias]);
            f.tick(T0);
            f.tick(T0 + TICK);
            assert_eq!(
                f.events(),
                vec![
                    ("one".into(), "opencode".into(), Some(OWNER.into())),
                    ("two".into(), "opencode".into(), Some(OWNER.into())),
                ],
                "{layout:?}: {:?}",
                f.degraded()
            );
            let rejected: i64 = f
                .state
                .with_conn(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |row| {
                        row.get(0)
                    })
                    .map_err(|e| e.to_string())
                })
                .unwrap();
            assert_eq!(rejected, 0, "{layout:?}: the folder was read a second time");
            assert!(
                f.degraded()
                    .is_some_and(|reason| reason.contains("resolve to one folder")),
                "{layout:?}"
            );
            assert_eq!(f.watermark(), None, "{layout:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_head_and_its_in_sessions_symlink_read_once_and_hold_the_watermark() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "head-call", true, CREATED);
            let head = f
                .folder
                .parent()
                .unwrap()
                .join("launch-head-x/data/opencode");
            fs::create_dir_all(head.parent().unwrap()).unwrap();
            fs::rename(f.folder.join(ONE), &head).unwrap();
            std::os::unix::fs::symlink(&head, f.folder.join(ONE)).unwrap();
            let mut entry = f.entry(ONE, false);
            entry["path"] = json!(head);
            let mut alias = f.entry(ONE, false);
            alias["owner_principal"] = json!("magic-context");
            f.declare(vec![entry, alias]);
            f.tick(T0);
            f.tick(T0 + TICK);
            assert_eq!(
                f.events(),
                vec![("head-call".into(), "opencode".into(), Some(OWNER.into()))],
                "{layout:?}"
            );
            let rejected: i64 = f
                .state
                .with_conn(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |row| {
                        row.get(0)
                    })
                    .map_err(|e| e.to_string())
                })
                .unwrap();
            assert_eq!(rejected, 0, "{layout:?}: the head was read a second time");
            assert!(
                f.degraded()
                    .is_some_and(|reason| reason.contains("resolve to one folder")),
                "{layout:?}"
            );
            assert_eq!(f.runtime(), Some((Some(T0 + TICK), true)), "{layout:?}");
            assert_eq!(f.watermark(), None, "{layout:?}");
        }
    }

    #[test]
    fn duplicate_folder_entries_are_malformed() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "first", true, CREATED);
        let mut other_owner = f.entry(ONE, false);
        other_owner["owner_principal"] = json!("magic-context");
        f.declare(vec![f.entry(ONE, false), other_owner]);
        f.tick(T0);
        assert!(f.events().is_empty());
        assert!(f.degraded().is_some());
        assert_eq!(f.watermark(), None);
    }

    #[test]
    fn unreadable_roots_read_the_last_good_entries_and_degrade() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "first", true, CREATED);
        f.declare(vec![f.entry(ONE, false)]);
        f.tick(T0);
        f.call(ONE, Layout::V2, "second", true, CREATED);
        fs::remove_file(&f.roots).unwrap();
        fs::create_dir(&f.roots).unwrap();
        f.tick(T0 + TICK);
        assert_eq!(f.events().len(), 2);
        assert_eq!(f.runtime(), Some((Some(T0 + TICK), true)));
        assert!(f.degraded().is_some());
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
    }

    #[test]
    fn absent_roots_remove_runtime_and_cursors_but_keep_the_watermark() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "first", true, CREATED);
        f.declare(vec![f.entry(ONE, true)]);
        f.tick(T0);
        assert!(f.cursor_rows() > 0);
        fs::remove_file(&f.roots).unwrap();
        f.tick(T0 + TICK);
        assert_eq!(f.runtime(), None);
        assert_eq!(f.cursor_rows(), 0);
        assert_eq!(f.degraded(), None);
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
    }

    #[test]
    fn an_unfinished_call_is_recorded_at_once_in_a_done_folder_and_waits_in_an_active_one() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "active", false, T0);
        f.call(TWO, Layout::V2, "done", false, T0);
        f.declare(vec![f.entry(ONE, false), f.entry(TWO, true)]);
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["done".to_string()]);
        f.tick(T0 + QUIET_WINDOW - 1);
        assert_eq!(f.refs(), vec!["done".to_string()]);
        f.tick(T0 + QUIET_WINDOW);
        assert_eq!(f.refs(), vec!["active".to_string(), "done".to_string()]);
        assert_eq!(f.degraded(), None);
    }

    #[test]
    fn a_folder_seen_done_gets_a_full_pass_before_it_releases_the_watermark() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "first", true, T0);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            // A row the tail sees as a user message, and passes, is turned
            // into a finished call in place. Only a full pass from the durable
            // cursor finds it again; the hourly one is not due yet.
            let mut user = payload(layout, true);
            user["role"] = json!("user");
            insert(&f.database(ONE), layout, "late", "user", T0 + 1, &user);
            f.tick(T0 + TICK);
            rewrite(
                &f.database(ONE),
                layout,
                "late",
                "assistant",
                T0 + TICK + 1,
                &payload(layout, true),
            );
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.refs(), vec!["first".to_string()], "{layout:?}");
            assert_eq!(f.watermark(), Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS));
            f.declare(vec![f.entry(ONE, true)]);
            f.tick(T0 + 3 * TICK);
            assert_eq!(
                f.refs(),
                vec!["first".to_string(), "late".to_string()],
                "{layout:?}"
            );
            assert_eq!(f.watermark(), Some(T0 + 3 * TICK - WATERMARK_MARGIN_MS));
        }
    }

    #[test]
    fn a_done_folder_holds_the_watermark_until_its_full_pass_reaches_high_water() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "first", true, T0);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            let held = Some(T0 - WATERMARK_MARGIN_MS);
            assert_eq!(f.watermark(), held, "{layout:?}");
            // More rows above the durable cursor than one page holds.
            fill(&f.database(ONE), layout, page_limit(layout), T0 + 1);
            f.declare(vec![f.entry(ONE, true)]);
            f.tick(T0 + TICK);
            assert_eq!(
                f.watermark(),
                held,
                "{layout:?}: one page is not high water"
            );
            assert_eq!(f.degraded(), None, "{layout:?}");
            assert_eq!(f.runtime(), Some((Some(T0 + TICK), true)), "{layout:?}");
            f.tick(T0 + 2 * TICK);
            assert_eq!(
                f.watermark(),
                Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS),
                "{layout:?}"
            );
            assert_eq!(f.degraded(), None, "{layout:?}");
            assert_eq!(f.runtime(), Some((Some(T0 + 2 * TICK), false)));
            assert_eq!(f.refs(), vec!["first"], "{layout:?}");
        }
    }

    #[test]
    fn a_row_only_the_full_pass_finds_holds_its_done_folder_until_it_is_recorded() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            let database = f.database(ONE);
            f.call(ONE, layout, "first", true, T0);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            // The tail reads this row as a user message and passes its rowid.
            let mut user = payload(layout, true);
            user["role"] = json!("user");
            insert(&database, layout, "late", "user", T0 + 1, &user);
            f.tick(T0 + TICK);
            let held = Some(T0 + TICK - WATERMARK_MARGIN_MS);
            assert_eq!(f.watermark(), held, "{layout:?}");
            // It becomes a finished call in place, updated after a full page
            // of other rows, so the settling full pass reaches it only on its
            // second drain and no tail read ever does.
            let limit = page_limit(layout);
            fill(&database, layout, limit, T0 + TICK + 1);
            let after_page = T0 + TICK + 1 + i64::try_from(limit).unwrap();
            rewrite(
                &database,
                layout,
                "late",
                "assistant",
                after_page,
                &payload(layout, true),
            );
            f.declare(vec![f.entry(ONE, true)]);
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.refs(), vec!["first"], "{layout:?}");
            assert_eq!(f.watermark(), held, "{layout:?}: `late` is not recorded");
            assert_eq!(f.degraded(), None, "{layout:?}");
            f.tick(T0 + 3 * TICK);
            assert_eq!(f.refs(), vec!["first", "late"], "{layout:?}");
            assert_eq!(
                f.watermark(),
                Some(T0 + 3 * TICK - WATERMARK_MARGIN_MS),
                "{layout:?}"
            );
        }
    }

    #[test]
    fn the_watermark_is_none_before_the_first_complete_pass() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            assert_eq!(f.watermark(), None);
            f.tick(T0);
            assert_eq!(f.watermark(), None, "{layout:?}: absent roots read nothing");
            f.call(ONE, layout, "one", true, CREATED);
            fs::create_dir_all(f.folder.join(TWO)).unwrap();
            fs::write(f.database(TWO), b"not a database, just bytes long enough").unwrap();
            f.declare(vec![f.entry(ONE, true), f.entry(TWO, true)]);
            f.tick(T0 + TICK);
            assert_eq!(f.refs(), vec!["one".to_string()], "{layout:?}");
            assert_eq!(f.watermark(), None, "{layout:?}");
            fs::remove_file(f.database(TWO)).unwrap();
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.watermark(), Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS));
        }
    }

    #[test]
    fn the_watermark_is_set_only_after_the_pass_commits() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.declare(vec![f.entry(ONE, true)]);
            // The folder commits, but the transaction recording the pass fails.
            f.sql(
                "CREATE TRIGGER refuse_pass BEFORE UPDATE OF last_success_ms ON source_runtime
                 BEGIN SELECT RAISE(ABORT,'refuse pass'); END;",
            );
            f.tick(T0);
            assert_eq!(f.refs(), vec!["one".to_string()], "{layout:?}");
            assert_eq!(f.watermark(), None, "{layout:?}");
            f.sql("DROP TRIGGER refuse_pass");
            // The folder's own transaction fails.
            f.call(ONE, layout, "two", true, CREATED);
            f.sql(
                "CREATE TRIGGER refuse_event BEFORE INSERT ON usage_event
                 BEGIN SELECT RAISE(ABORT,'refuse event'); END;",
            );
            f.tick(T0 + TICK);
            assert_eq!(f.refs(), vec!["one".to_string()], "{layout:?}");
            assert_eq!(f.watermark(), None, "{layout:?}");
            assert!(f.degraded().is_some(), "{layout:?}");
            f.sql("DROP TRIGGER refuse_event");
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.refs(), vec!["one".to_string(), "two".to_string()]);
            assert_eq!(f.watermark(), Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS));
            assert_eq!(f.degraded(), None, "{layout:?}");
        }
    }

    #[test]
    fn the_watermark_never_goes_backwards_across_a_restart() {
        for layout in LAYOUTS {
            let mut f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.declare(vec![f.entry(ONE, true)]);
            f.tick(T0);
            assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
            f.restart();
            // The clock stepped back across the restart.
            f.tick(T0 - 10 * TICK);
            assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS), "{layout:?}");
            assert_eq!(f.runtime(), Some((Some(T0 - 10 * TICK), false)));
            f.tick(T0 + TICK);
            assert_eq!(f.watermark(), Some(T0 + TICK - WATERMARK_MARGIN_MS));
            assert_eq!(f.refs(), vec!["one".to_string()], "{layout:?}");
        }
    }

    #[test]
    fn an_unreadable_folder_holds_the_watermark_and_degrades_but_others_read() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.call(TWO, layout, "two", true, CREATED);
            f.declare(vec![f.entry(ONE, true), f.entry(TWO, true)]);
            f.tick(T0);
            let released = Some(T0 - WATERMARK_MARGIN_MS);
            assert_eq!(f.watermark(), released, "{layout:?}");
            // A corrupt store: it exists, but SQLite cannot read it.
            fs::write(f.database(TWO), vec![7_u8; 4096]).unwrap();
            f.call(ONE, layout, "three", true, CREATED);
            f.tick(T0 + TICK);
            assert_eq!(
                f.refs(),
                vec!["one".to_string(), "three".to_string(), "two".to_string()],
                "{layout:?}"
            );
            assert_eq!(f.watermark(), released, "{layout:?}");
            assert!(f.degraded().unwrap().contains(TWO), "{layout:?}");
            assert_eq!(f.runtime(), Some((Some(T0 + TICK), true)), "{layout:?}");
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.watermark(), released, "{layout:?}");
            // Once it is erased, it holds nothing back.
            fs::remove_dir_all(f.folder.join(TWO)).unwrap();
            f.tick(T0 + 3 * TICK);
            assert_eq!(f.watermark(), Some(T0 + 3 * TICK - WATERMARK_MARGIN_MS));
            assert_eq!(f.degraded(), None, "{layout:?}");
        }
    }

    #[test]
    fn erasing_a_folder_between_ticks_neither_blocks_nor_degrades() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.call(TWO, layout, "two", true, CREATED);
            f.declare(vec![f.entry(ONE, true), f.entry(TWO, false)]);
            f.tick(T0);
            f.tick(T0 + TICK);
            assert_eq!(f.cursor_rows(), 2, "{layout:?}");
            fs::remove_dir_all(f.folder.join(ONE)).unwrap();
            f.call(TWO, layout, "three", true, CREATED);
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.degraded(), None, "{layout:?}");
            assert_eq!(f.runtime(), Some((Some(T0 + 2 * TICK), false)));
            assert_eq!(f.watermark(), Some(T0 + 2 * TICK - WATERMARK_MARGIN_MS));
            assert_eq!(f.refs(), vec!["one", "three", "two"], "{layout:?}");
            // An erased folder's cursor is never needed again.
            assert_eq!(f.cursor_rows(), 1, "{layout:?}");
            // A folder without its database yet is skipped the same way.
            fs::create_dir_all(f.folder.join(ONE)).unwrap();
            f.tick(T0 + 3 * TICK);
            assert_eq!(f.degraded(), None, "{layout:?}");
            assert_eq!(f.watermark(), Some(T0 + 3 * TICK - WATERMARK_MARGIN_MS));
            // Once an entry is removed, its cursor goes too.
            f.declare(vec![]);
            f.tick(T0 + 4 * TICK);
            assert_eq!(f.cursor_rows(), 0, "{layout:?}");
        }
    }

    #[test]
    fn a_set_aside_record_does_not_hold_the_watermark() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "good", true, CREATED);
            let mut bad = payload(layout, true);
            bad["time"]["created"] = json!("not a time");
            insert(&f.database(ONE), layout, "bad", "assistant", CREATED, &bad);
            f.declare(vec![f.entry(ONE, true)]);
            f.tick(T0);
            assert_eq!(f.refs(), vec!["good".to_string()], "{layout:?}");
            super::super::harness_drain::assert_set_aside(&f.state, SOURCE_KEY, 1);
            assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS), "{layout:?}");
            assert_eq!(f.degraded(), None, "{layout:?}");
        }
    }

    #[test]
    fn excluded_sessions_are_not_read_and_runtime_is_removed() {
        let f = Fixture::new();
        f.call(ONE, Layout::V2, "one", true, CREATED);
        f.declare(vec![f.entry(ONE, true)]);
        f.tick(T0);
        let config = f.temp.path().join("astrocyte.jsonc");
        fs::write(&config,r#"{"poll_interval_secs":30,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":["opencode-thalamus"]}}"#).unwrap();
        crate::refresher::reload_config(&f.state, Ok(config)).unwrap();
        f.call(ONE, Layout::V2, "two", true, CREATED);
        f.tick(T0 + TICK);
        assert_eq!(f.refs(), vec!["one".to_string()]);
        assert_eq!(f.runtime(), None);
        assert_eq!(f.watermark(), Some(T0 - WATERMARK_MARGIN_MS));
    }

    #[test]
    fn a_declared_store_never_keeps_its_connection() {
        let home = CursorHome::Declared {
            source_key: SOURCE_KEY,
            store: "/sessions/s-0001".into(),
            layout: LAYOUT_V1,
        };
        assert!(!home.retains_connection());
    }

    include!("opencode_thalamus_source_tests.rs");

    #[cfg(windows)]
    #[test]
    fn windows_declared_stores_can_be_deleted_between_reads() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            // A tail read on the second tick. The declared reader must close
            // its connection after it, or the owner could not delete the
            // database below.
            f.call(ONE, layout, "two", true, CREATED);
            f.tick(T0 + TICK);
            let database = f.database(ONE);
            fs::remove_file(&database).unwrap_or_else(|error| {
                panic!(
                    "reader blocked owner deletion of {}: {error}",
                    database.display()
                )
            });
            f.tick(T0 + 2 * TICK);
            assert_eq!(f.refs(), vec!["one".to_string(), "two".to_string()]);
            assert_eq!(f.degraded(), None, "{layout:?}");
        }
    }
}
