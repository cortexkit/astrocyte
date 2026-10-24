//! OpenCode sessions that thalamus-harness runs in private OpenCode servers,
//! and the read watermark that tells it when a finished session's folder has
//! been recorded and may be erased. The binding contract is
//! `docs/thalamus-opencode-sessions-contract.md`.
//!
//! Each declared folder's `opencode.db` is read with the same OpenCode 1 and
//! OpenCode 2 readers as the user's own store, on the same cadence, each with
//! its own durable cursor per folder. Calls keep producer `opencode` and
//! OpenCode's message id, and carry the declaring module as `work_class`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use astrocyte_readers::sessions::opencode::OpenCodeReadError;
use astrocyte_readers::sessions::opencode_v2;
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;

use super::declared_roots::{self, Contained, RootsFile};
use super::harness_drain::{self, HarnessSource};
use super::opencode_source::{CursorHome, OpenCodeSource, ReadOptions};
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
    session_ref: String,
    owner_principal: String,
    state: SessionState,
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
    /// The folder is `done` and, since it was seen done, a full pass of each
    /// layout has reached its high water, so every call in it is recorded.
    settled: bool,
}

impl Folder {
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
        }
    }
}

/// What reading one declared folder found.
enum FolderRead {
    /// Its `opencode.db` is gone: erased, or not created yet.
    Gone,
    /// Read and committed.
    Read {
        /// Every call the folder will ever hold is recorded: it is `active`
        /// (whose later calls the watermark does not promise), or `done` and
        /// settled.
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
    /// the roots file was usable and every folder that still exists was read,
    /// and only in the transaction that records the pass, after every
    /// folder's own transaction has committed.
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
        let mut problems = Vec::new();
        let declarations_known =
            match declared_roots::read::<Entry>(roots, "OpenCode session roots") {
                RootsFile::Absent => {
                    *cache = Cache::default();
                    forget_source(state)?;
                    return Ok(None);
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
        let mut kept = HashMap::new();
        let entries = cache.entries.clone();
        for entry in &entries {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceDrainError::Stopped);
            }
            let canonical = match declared_roots::contain(
                Some(&folder),
                &entry.path,
                "OpenCode session folder",
            ) {
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
            let mut reader = cache
                .folders
                .remove(&canonical)
                .unwrap_or_else(|| Folder::new(&canonical));
            let database = canonical.join("opencode.db");
            match read_folder(&mut reader, entry, &database, state, at, stop) {
                Ok(FolderRead::Gone) => {}
                Ok(FolderRead::Read {
                    recorded_all,
                    quarantined: count,
                }) => {
                    complete &= recorded_all;
                    quarantined += count;
                }
                Err(SourceDrainError::Stopped) => return Err(SourceDrainError::Stopped),
                // The database vanished while it was read: the harness module
                // erased it, which neither holds the watermark nor degrades.
                Err(SourceDrainError::Failed(_)) if gone(&database) => {}
                Err(SourceDrainError::Failed(reason)) => {
                    complete = false;
                    let reason = format!(
                        "read OpenCode session {} at {}: {reason}",
                        entry.session_ref,
                        database.display()
                    );
                    warn_once(&mut cache.warned, reason.clone());
                    problems.push(reason);
                }
            }
            kept.insert(canonical, reader);
        }
        let stores = declarations_known.then(|| {
            kept.keys()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<HashSet<_>>()
        });
        cache.folders = kept;

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

    // Full ticks only: the fast lane's default reads nothing from here.
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
) -> Result<FolderRead, SourceDrainError> {
    match astrocyte_readers::read_io::metadata(database) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(FolderRead::Gone),
        Err(error) => return Err(format!("inspect {}: {error}", database.display()).into()),
    }
    let done = entry.state == SessionState::Done;
    if !done {
        folder.settled = false;
    }
    // Once a folder is done, one full pass of each layout from its durable
    // cursor finds every call the tail reads could have missed.
    let settling = done && !folder.settled;
    if settling {
        folder.v1.require_full_pass();
        folder.v2.require_full_pass();
    }
    let options = ReadOptions {
        work_class: Some(entry.owner_principal.as_str()),
        settled: done,
    };
    let has_v1 = opencode_v2::has_v1_message_table(database, stop).map_err(read_error)?;
    let mut quarantined = 0;
    if has_v1 {
        let outcome = folder.v1.drain_declared(state, at, stop, options)?;
        if outcome.root_absent {
            return Ok(FolderRead::Gone);
        }
        quarantined = outcome.quarantined_redeliveries;
    }
    folder.v2.drain_declared(state, at, stop, options)?;
    if settling {
        folder.settled = (!has_v1 || folder.v1.full_pass_reached_high_water())
            && folder.v2.full_pass_reached_high_water();
    }
    Ok(FolderRead::Read {
        recorded_all: !done || folder.settled,
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

/// Entries are keyed by path, so two entries naming one folder leave its
/// owner ambiguous; the file is then treated as malformed.
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
/// first complete pass.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the served watermark.get operation reads it once it is added"
    )
)]
pub(crate) fn watermark_ms(state: &SharedState) -> Result<Option<i64>, String> {
    state.with_conn(|connection| {
        connection
            .query_row(
                "SELECT watermark_ms FROM read_watermark WHERE source_key=?1",
                [SOURCE_KEY],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| format!("read {SOURCE_KEY} read watermark: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::opencode_source::OpenCodeSource;
    use crate::refresher::run_iteration_with_harness;
    use crate::state::test_state;
    use astrocyte_readers::sessions::opencode_v2::QUIET_WINDOW;
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
            watermark_ms(&self.state).unwrap()
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

    #[cfg(windows)]
    #[test]
    fn windows_declared_stores_can_be_deleted_between_reads() {
        for layout in LAYOUTS {
            let f = Fixture::new();
            f.call(ONE, layout, "one", true, CREATED);
            f.declare(vec![f.entry(ONE, false)]);
            f.tick(T0);
            // A tail read: the reader that a user's store would keep open.
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
