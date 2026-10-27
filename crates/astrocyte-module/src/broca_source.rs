//! BROCA's usage facts, read from BROCA's own store.
//!
//! BROCA writes every spend segment it bills into `export_facts` in
//! `<data home>/cortexkit/broca/run-index.db`: one immutable JSON row per
//! fact, numbered by `export_seq`. The single `export_meta` row names the
//! database (`source_instance_id`, minted when it is created), its
//! `source_epoch`, and `high_water`, which BROCA sets to `MAX(export_seq)` in
//! the same transaction as each insert. This reader tails that table from the
//! durable cursor and decodes each row with the decoder the `usage.export`
//! pull used, so an event keeps the identity and payload the pull gave it.
//! A row whose segment carries `"kind": "system_one"` is a fact from one of
//! BROCA's batch models and has its own decoder. Other kinds are set aside
//! (see [`decode_store_row`]). BROCA steps will arrive from its step log.
//!
//! When `BROCA_STATE_ROOT` is unset, BROCA writes to the same path, resolving
//! the data home in the same order as this reader
//! (`$XDG_DATA_HOME/cortexkit/broca`, then `~/.local/share/cortexkit/broca`).
//! A `BROCA_STATE_ROOT` naming any other directory puts the store where this
//! reader never looks, and a missing store reads exactly like a machine
//! without BROCA: no runtime row, no warning.
//!
//! The store is opened read-only (`mode=ro`) and nothing here writes to it.
//! Every read is one snapshot, so `export_meta` and the rows read with it
//! agree.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use astrocyte_core::ingest::EventRecordRequest;
use astrocyte_readers::{
    file_identity::{file_identity, same_identity},
    read_io,
};
use rusqlite::{Connection, ErrorCode};
use serde_json::Value;

use super::{interrupt_on_stop, ExportPage, PullError, SourceClient, SourceCursor};
use crate::wire_map::{
    map_fact, map_system_one_fact, WireSpendSegmentFact, WireSystemOneFact, SYSTEM_ONE_KIND,
};

pub(crate) const BROCA_SOURCE_KEY: &str = "broca";

/// Rows read and committed per tick, the same bound as the OpenCode reader's
/// page. A backlog larger than this drains over later ticks.
pub(crate) const PAGE_LIMIT: u32 = super::harness_drain::PAGE_LIMIT;

#[derive(Debug, Clone)]
enum BrocaRoot {
    Path(PathBuf),
    Unresolved(String),
}

/// BROCA's `run-index.db`, read once per refresher tick.
///
/// The cursor is stored in `source_runtime` exactly as the `usage.export`
/// pull stored it: `lineage` is `<source_instance_id>:<source_epoch>` and
/// `consumed_ordinal` is the last `export_seq` read (or `high_water` once a
/// read finds nothing newer). A cursor the pull left behind is therefore
/// continued where it stopped rather than read again.
///
/// A stored lineage that differs from the store's own names another
/// database (BROCA rebuilt `run-index.db`, which mints a new instance id) or
/// a new epoch, and the read restarts at `export_seq` 0. The facts read again
/// carry the same content-derived event ids, so they are recorded as exact
/// redeliveries. A store that answers under the stored identity from behind
/// the cursor is refused by [`refuse_broca_regression`].
pub(crate) struct BrocaSource {
    root: BrocaRoot,
    cache: Mutex<BrocaCache>,
    /// Set on shutdown. A read in progress is interrupted inside SQLite.
    stop: Arc<AtomicBool>,
}

#[derive(Default)]
struct BrocaCache {
    connection: Option<Connection>,
    signature: Option<(Option<(u64, u64)>, u64)>,
}

impl BrocaSource {
    #[cfg_attr(
        any(test, feature = "test-support"),
        expect(
            dead_code,
            reason = "tests inject the data home directly; only the environment ladder remains unexercised"
        )
    )]
    pub(crate) fn from_environment(stop: Arc<AtomicBool>) -> Self {
        Self::from_data_home(
            PathBuf::from(cortexkit_store_types::resolve_data_home()),
            stop,
        )
    }

    /// The store under `data_home`, refusing a relative home: the shared
    /// ladder ends in a relative `.local/share` when no home variable is set,
    /// and a path resolved against the working directory would read as absent
    /// while BROCA's real store went unread.
    pub(crate) fn from_data_home(data_home: PathBuf, stop: Arc<AtomicBool>) -> Self {
        let root = if data_home.is_absolute() {
            BrocaRoot::Path(data_home.join("cortexkit/broca/run-index.db"))
        } else {
            BrocaRoot::Unresolved(format!(
                "the data home resolved to the relative {data_home:?}; refusing to resolve BROCA's run-index.db"
            ))
        };
        Self {
            root,
            stop,
            cache: Mutex::new(BrocaCache::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn at(path: impl Into<PathBuf>, stop: Arc<AtomicBool>) -> Self {
        Self {
            root: BrocaRoot::Path(path.into()),
            stop,
            cache: Mutex::new(BrocaCache::default()),
        }
    }
    fn pull_in_lane(
        &self,
        since: Option<SourceCursor>,
        limit: u32,
        full: bool,
    ) -> Result<ExportPage, PullError> {
        let path = match &self.root {
            BrocaRoot::Path(path) => path,
            BrocaRoot::Unresolved(reason) => return Err(PullError::Unavailable(reason.clone())),
        };
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        if full {
            let file = match read_io::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    cache.connection = None;
                    return Err(PullError::SourceAbsent);
                }
                Err(error) => {
                    cache.connection = None;
                    return Err(PullError::Unavailable(format!(
                        "inspect BROCA store {}: {error}",
                        path.display()
                    )));
                }
            };
            let metadata = astrocyte_readers::read_io::file_metadata(&file).map_err(|error| {
                cache.connection = None;
                PullError::Unavailable(error.to_string())
            })?;
            let signature = (file_identity(&file, &metadata), metadata.len());
            if cache.signature.is_some_and(|previous| {
                !same_identity(signature.0, previous.0) || signature.1 < previous.1
            }) {
                cache.connection = None;
            }
            cache.signature = Some(signature);
        }
        if self.stop.load(Ordering::Relaxed) {
            cache.connection = None;
            return Err(PullError::Stopped);
        }
        let result = (|| {
            if cache.connection.is_none() {
                let connection = read_io::connect(&readonly_uri(path)?)
                    .map_err(|error| sqlite_error(&self.stop, "open BROCA store", error))?;
                read_io::execute(&connection, "PRAGMA query_only=ON")
                    .map_err(|error| sqlite_error(&self.stop, "configure BROCA reader", error))?;
                cache.connection = Some(connection);
            }
            let snapshot = read_snapshot(
                cache.connection.as_ref().expect("opened BROCA reader"),
                path,
                since.as_ref(),
                limit,
                &self.stop,
            )?;
            if !read_io::retain_harness_connections() {
                cache.connection = None;
            }
            page_from_snapshot(snapshot, path)
        })();
        if result.is_err() || !read_io::retain_harness_connections() {
            cache.connection = None;
        }
        result
    }
}

impl SourceClient for BrocaSource {
    fn pull(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError> {
        self.pull_in_lane(since, limit, true)
    }

    fn pull_fast(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError> {
        self.pull_in_lane(since, limit, false)
    }

    fn resolved_path(&self) -> Option<PathBuf> {
        match &self.root {
            BrocaRoot::Path(path) => Some(path.clone()),
            BrocaRoot::Unresolved(_) => None,
        }
    }

    fn refuse_regression(
        &self,
        stored: Option<&SourceCursor>,
        page: &ExportPage,
    ) -> Result<(), PullError> {
        refuse_broca_regression(stored, page)
    }
}

/// One row of `export_facts`.
struct StoreRow {
    export_seq: i64,
    fact_id: String,
    segment_json: String,
}

/// What one read snapshot saw.
struct Snapshot {
    lineage: String,
    high_water: i64,
    rows: Vec<StoreRow>,
}

fn encode_lineage(source_instance_id: &str, source_epoch: i64) -> String {
    format!("{source_instance_id}:{source_epoch}")
}

/// Split a stored lineage at its LAST colon, as the pull did, so an instance
/// id that itself contains a colon still round-trips.
fn decode_lineage(lineage: &str) -> Result<(&str, i64), PullError> {
    let (source_instance_id, epoch) = lineage
        .rsplit_once(':')
        .ok_or_else(|| PullError::InvalidResponse("broca lineage is not instance:epoch".into()))?;
    if source_instance_id.is_empty() {
        return Err(PullError::InvalidResponse(
            "broca lineage has an empty instance".into(),
        ));
    }
    let source_epoch = epoch.parse::<i64>().map_err(|_| {
        PullError::InvalidResponse("broca lineage epoch is not a stored integer".into())
    })?;
    if source_epoch < 0 {
        return Err(PullError::InvalidResponse(
            "broca lineage epoch is negative".into(),
        ));
    }
    Ok((source_instance_id, source_epoch))
}

/// Refuse a store that answers under the stored instance from behind the
/// cursor: a lower epoch, or the same epoch with a high water below the
/// consumed ordinal. That is an older copy of the database put back (or its
/// tail lost), and advancing would either skip facts or read them twice. A
/// different instance is a rebuilt store and is read from the start instead.
pub(crate) fn refuse_broca_regression(
    stored: Option<&SourceCursor>,
    page: &ExportPage,
) -> Result<(), PullError> {
    let Some(stored) = stored else {
        return Ok(());
    };
    let offered = &page.high_water;
    let (stored_instance, stored_epoch) = decode_lineage(&stored.lineage)?;
    let (offered_instance, offered_epoch) = decode_lineage(&offered.lineage)?;
    if stored_instance == offered_instance
        && (offered_epoch < stored_epoch
            || (offered_epoch == stored_epoch && offered.ordinal < stored.ordinal))
    {
        return Err(PullError::LineageRegressed {
            stored: stored.clone(),
            offered: offered.clone(),
        });
    }
    Ok(())
}

/// Percent-encode `path` into a read-only SQLite URI.
fn readonly_uri(path: &Path) -> Result<String, PullError> {
    let raw = path.to_str().ok_or_else(|| {
        PullError::Unavailable(format!("BROCA store path is not UTF-8: {}", path.display()))
    })?;
    Ok(astrocyte_readers::sqlite_uri::sqlite_readonly_uri(raw))
}

/// An interrupt is a stop only when our own stop flag asked for it; any other
/// SQLite failure means the store could not be read.
fn sqlite_error(stop: &AtomicBool, context: &str, error: rusqlite::Error) -> PullError {
    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted)
        && stop.load(Ordering::Relaxed)
    {
        PullError::Stopped
    } else {
        PullError::Unavailable(format!("{context}: {error}"))
    }
}

fn read_snapshot(
    connection: &Connection,
    path: &Path,
    since: Option<&SourceCursor>,
    limit: u32,
    stop: &AtomicBool,
) -> Result<Snapshot, PullError> {
    let context = format!("read BROCA store {}", path.display());
    read_io::execute(connection, "BEGIN DEFERRED")
        .map_err(|error| sqlite_error(stop, &context, error))?;
    let read = interrupt_on_stop(connection, stop, || {
        let mut meta = read_io::prepare_cached(
            connection,
            "SELECT source_instance_id,source_epoch,high_water FROM export_meta WHERE id=1",
        )?;
        let (source_instance_id, source_epoch, high_water) = meta.query_row([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let lineage = encode_lineage(&source_instance_id, source_epoch);
        let start = since
            .filter(|cursor| cursor.lineage == lineage)
            .map_or(0, |cursor| cursor.ordinal);
        let mut statement = read_io::prepare_cached(
            connection,
            "SELECT export_seq,fact_id,segment_json FROM export_facts
             WHERE export_seq > ?1 ORDER BY export_seq LIMIT ?2",
        )?;
        let rows = statement
            .query_map(rusqlite::params![start, i64::from(limit)], |row| {
                Ok(StoreRow {
                    export_seq: row.get(0)?,
                    fact_id: row.get(1)?,
                    segment_json: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        #[cfg(all(test, target_os = "linux"))]
        super::fast_lane_measure::note_vm_steps(
            u64::try_from(
                meta.reset_status(rusqlite::StatementStatus::VmStep)
                    + statement.reset_status(rusqlite::StatementStatus::VmStep),
            )
            .unwrap(),
        );
        Ok((source_instance_id, source_epoch, high_water, rows))
    });
    let finish = read_io::execute(connection, if read.is_ok() { "COMMIT" } else { "ROLLBACK" });
    let (source_instance_id, source_epoch, high_water, rows) =
        read.map_err(|error| sqlite_error(stop, &context, error))?;
    finish.map_err(|error| sqlite_error(stop, &context, error))?;
    if source_instance_id.is_empty() || source_epoch < 0 || high_water < 0 {
        return Err(PullError::InvalidResponse(format!(
            "BROCA export_meta holds an unusable identity ({source_instance_id:?}, epoch \
             {source_epoch}, high water {high_water})"
        )));
    }
    Ok(Snapshot {
        lineage: encode_lineage(&source_instance_id, source_epoch),
        high_water,
        rows,
    })
}

/// Decode one row by the `kind` its segment carries.
///
/// A segment with no `kind` is a session fact and is decoded exactly as the
/// `usage.export` pull served it. A `system_one` segment is a batch fact. Any
/// other `kind`, including a `kind` that is not a string, is refused with the
/// kind named, so the row is set aside as evidence: decoding it as a session
/// fact could record a charge under a shape BROCA did not mean.
fn decode_store_row(
    export_seq: i64,
    fact_id: &str,
    segment_json: &str,
) -> Result<EventRecordRequest, String> {
    let export_seq = u64::try_from(export_seq)
        .map_err(|_| format!("export_seq {export_seq} of fact {fact_id} is negative"))?;
    let segment: Value = serde_json::from_str(segment_json)
        .map_err(|error| format!("segment_json of fact {fact_id} is not JSON: {error}"))?;
    if !segment.is_object() {
        return Err(format!("segment_json of fact {fact_id} is not an object"));
    }
    let kind = segment.get("kind").cloned();
    match kind {
        None => decode_session_segment(export_seq, fact_id, segment),
        Some(Value::String(kind)) if kind == SYSTEM_ONE_KIND => {
            decode_system_one_segment(export_seq, fact_id, segment)
        }
        Some(kind) => Err(format!("segment of fact {fact_id} has unknown kind {kind}")),
    }
}

/// Decode a session fact the way `usage.export` served it. `segment_json`
/// carries no `export_seq`; the pull's wire type has it at the top level, so
/// the row's column is put there before decoding. The rest of the row is
/// decoded as stored: `usage.export` re-encoded the same fields without
/// adding any.
fn decode_session_segment(
    export_seq: u64,
    fact_id: &str,
    mut segment: Value,
) -> Result<EventRecordRequest, String> {
    segment
        .as_object_mut()
        .ok_or_else(|| format!("segment_json of fact {fact_id} is not an object"))?
        .insert("export_seq".to_string(), Value::from(export_seq));
    let wire: WireSpendSegmentFact = serde_json::from_value(segment)
        .map_err(|error| format!("decode segment of fact {fact_id}: {error}"))?;
    refuse_fact_id_mismatch(export_seq, fact_id, &wire.fact_id)?;
    map_fact(wire)
}

fn decode_system_one_segment(
    export_seq: u64,
    fact_id: &str,
    segment: Value,
) -> Result<EventRecordRequest, String> {
    let wire: WireSystemOneFact = serde_json::from_value(segment)
        .map_err(|error| format!("decode {SYSTEM_ONE_KIND} segment of fact {fact_id}: {error}"))?;
    refuse_fact_id_mismatch(export_seq, fact_id, &wire.fact_id)?;
    map_system_one_fact(wire)
}

/// The column and the segment name the fact the same way whenever BROCA
/// wrote the row; a disagreement means the row is damaged, and ingesting it
/// under either id could record a charge under the wrong identity.
fn refuse_fact_id_mismatch(
    export_seq: u64,
    column_fact_id: &str,
    segment_fact_id: &str,
) -> Result<(), String> {
    if segment_fact_id != column_fact_id {
        return Err(format!(
            "export_facts row {export_seq} names fact {column_fact_id} but its segment names \
             {segment_fact_id}"
        ));
    }
    Ok(())
}

fn page_from_snapshot(snapshot: Snapshot, path: &Path) -> Result<ExportPage, PullError> {
    let mut events = Vec::new();
    let mut set_aside = Vec::new();
    for row in &snapshot.rows {
        match decode_store_row(row.export_seq, &row.fact_id, &row.segment_json) {
            Ok(event) => events.push(event),
            Err(reason) => set_aside.push(crate::refresher::SetAsideRecord {
                locator: format!(
                    "{path:?}:{}:export_seq:{}",
                    snapshot.lineage, row.export_seq
                ),
                reason,
            }),
        }
    }
    // With rows, the cursor is the last one read. With none, the snapshot
    // holds nothing past the position read from, so the store is read up to
    // its high water; committing that high water is what the pull did for an
    // empty page. A high water below a stored cursor is left for
    // `refuse_broca_regression` to refuse.
    let consumed = snapshot
        .rows
        .last()
        .map_or(snapshot.high_water, |row| row.export_seq);
    let high_water = snapshot.high_water.max(consumed);
    Ok(ExportPage {
        events,
        set_aside,
        cursor: SourceCursor {
            lineage: snapshot.lineage.clone(),
            ordinal: consumed,
        },
        high_water: SourceCursor {
            lineage: snapshot.lineage,
            ordinal: high_water,
        },
        // `export_seq` has gaps, so the number of facts left is not the
        // difference of the two positions; the lag is reported as unknown.
        pending_known: 0,
        pending_unknown: consumed != high_water,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::{drain_once, run_iteration, Producer};
    use crate::state::{test_state, SharedState};
    use crate::wire_map::wire_fixture;
    use tempfile::TempDir;

    /// `export_meta` and `export_facts` exactly as BROCA declares them.
    fn create_store(path: &Path, instance: &str, epoch: i64) {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE export_meta (
                     id                  INTEGER PRIMARY KEY CHECK (id = 1),
                     source_epoch       INTEGER NOT NULL DEFAULT 1,
                     source_instance_id TEXT NOT NULL,
                     high_water         INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE TABLE export_facts (
                     export_seq  INTEGER PRIMARY KEY AUTOINCREMENT,
                     fact_id     TEXT NOT NULL UNIQUE,
                     run_id      TEXT NOT NULL,
                     segment_json TEXT NOT NULL
                 );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO export_meta (id,source_epoch,source_instance_id,high_water)
                 VALUES (1,?1,?2,0)",
                rusqlite::params![epoch, instance],
            )
            .unwrap();
    }

    /// The wire fixture as BROCA stores it: no `export_seq`, its own fact id.
    fn stored_segment(fact_id: &str) -> String {
        let mut segment = wire_fixture();
        let object = segment.as_object_mut().unwrap();
        object.remove("export_seq");
        object.insert("fact_id".into(), Value::from(fact_id));
        segment.to_string()
    }

    /// Insert one fact at `export_seq` and move the high water with it in the
    /// same transaction, as BROCA does.
    fn add_fact(path: &Path, export_seq: i64, fact_id: &str) {
        add_segment(path, export_seq, fact_id, &stored_segment(fact_id));
    }

    /// Insert one row with the given segment JSON, as [`add_fact`] does.
    fn add_segment(path: &Path, export_seq: i64, fact_id: &str, segment_json: &str) {
        let mut connection = Connection::open(path).unwrap();
        let tx = connection.transaction().unwrap();
        tx.execute(
            "INSERT INTO export_facts (export_seq,fact_id,run_id,segment_json)
             VALUES (?1,?2,'run-1',?3)",
            rusqlite::params![export_seq, fact_id, segment_json],
        )
        .unwrap();
        tx.execute(
            "UPDATE export_meta SET high_water=(SELECT COALESCE(MAX(export_seq),0) FROM export_facts)",
            [],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    fn source(path: &Path) -> BrocaSource {
        BrocaSource::at(path, Arc::new(AtomicBool::new(false)))
    }

    #[test]
    fn set_aside_broca_bad_row_keeps_good_rows_and_progress() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 1, "good-before");
        add_fact(&path, 2, "bad");
        add_fact(&path, 3, "good-after");
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE export_facts SET segment_json='{}' WHERE export_seq=2",
                [],
            )
            .unwrap();
        let state = test_state();
        let broca = source(&path);
        for now in [100, 200] {
            let result = drain_once(&state, &broca, BROCA_SOURCE_KEY, PAGE_LIMIT, now);
            assert!(
                result.is_ok(),
                "tick {now}: {result:?}; runtime={:?}",
                runtime(&state)
            );
            assert_eq!(count(&state, "usage_event"), 2);
            assert_eq!(runtime(&state).unwrap().1, Some(3));
            crate::refresher::harness_drain::assert_set_aside(&state, "broca", 1);
        }
    }

    /// A session-only store stores exactly the bytes it stored before batch
    /// facts existed: the hash was pinned before the `flow_ref` column was
    /// added, and flow_ref is NULL on every row.
    #[test]
    fn set_aside_good_broca_rows_are_byte_identical() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 1, "good-before");
        add_fact(&path, 3, "good-after");
        let state = test_state();
        drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();
        crate::refresher::harness_drain::assert_good_snapshot(
            &state,
            "broca",
            "f699ba842c11f2f7a694b4aa9acdd7e8fc439bcca4cf7fd65ff9ebbe0eeacda4",
        );
        assert_eq!(count(&state, "usage_event"), 2);
        assert_eq!(count(&state, "usage_event WHERE flow_ref IS NOT NULL"), 0);
    }

    /// `(lineage, consumed_ordinal, last_success_ms, pending_unknown)` of the
    /// BROCA runtime row, or `None` when there is no row.
    type Runtime = Option<(Option<String>, Option<i64>, Option<i64>, i64)>;

    fn runtime(state: &SharedState) -> Runtime {
        state
            .with_conn(|conn| {
                use rusqlite::OptionalExtension;
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,last_success_ms,pending_unknown
                     FROM source_runtime WHERE source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(|error| error.to_string())
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

    fn tick(state: &SharedState, source: BrocaSource) {
        run_iteration(
            state,
            &[Producer {
                source_key: BROCA_SOURCE_KEY.into(),
                client: Arc::new(source),
            }],
            &AtomicBool::new(false),
        );
    }

    /// The event read from a stored row is the one the pull's decoder made
    /// from the same segment served with `export_seq` flattened in.
    #[test]
    fn a_store_row_decodes_to_the_pull_payload() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 7, "broca-content-hash");

        let page = source(&path).pull(None, PAGE_LIMIT).unwrap();

        let pulled = map_fact(serde_json::from_value(wire_fixture()).unwrap()).unwrap();
        assert_eq!(page.events, vec![pulled]);
        assert_eq!(
            page.cursor,
            SourceCursor {
                lineage: "instance-a:1".into(),
                ordinal: 7
            }
        );
    }

    /// A page stops at the limit and the cursor moves with it; the next drain
    /// starts after it, and a drain past the end reads nothing.
    #[test]
    fn the_tail_is_read_across_pages_and_the_cursor_advances() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        // Gaps in export_seq are normal: sequence numbers are never reused.
        for (seq, fact) in [(1, "f1"), (2, "f2"), (4, "f4"), (5, "f5"), (9, "f9")] {
            add_fact(&path, seq, fact);
        }
        let state = test_state();
        let broca = source(&path);

        let mut seen = Vec::new();
        for now in [10, 20, 30] {
            let outcome = drain_once(&state, &broca, BROCA_SOURCE_KEY, 2, now).unwrap();
            seen.push((outcome.events_recorded, outcome.consumed_ordinal));
        }
        assert_eq!(seen, [(2, Some(2)), (2, Some(5)), (1, Some(9))]);
        assert_eq!(count(&state, "usage_event"), 5);
        assert_eq!(
            runtime(&state),
            Some((Some("instance-a:1".into()), Some(9), Some(30), 0))
        );

        let outcome = drain_once(&state, &broca, BROCA_SOURCE_KEY, 2, 40).unwrap();
        assert_eq!(outcome.events_recorded, 0);
        assert_eq!(count(&state, "usage_event"), 5);
    }

    /// A drain that finds nothing new still commits, so the source reads as
    /// fresh rather than going stale while BROCA is idle.
    #[test]
    fn a_drain_with_nothing_new_still_advances_last_success() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 3, "f3");
        let state = test_state();
        let broca = source(&path);

        drain_once(&state, &broca, BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();
        assert_eq!(
            runtime(&state),
            Some((Some("instance-a:1".into()), Some(3), Some(100), 0))
        );
        drain_once(&state, &broca, BROCA_SOURCE_KEY, PAGE_LIMIT, 200).unwrap();
        assert_eq!(
            runtime(&state),
            Some((Some("instance-a:1".into()), Some(3), Some(200), 0))
        );
    }

    /// A rebuilt store has a new instance id and numbers its facts from 1
    /// again. The stored cursor belongs to the old database, so the read
    /// restarts at 0: the facts come back with their old event ids and are
    /// recorded as exact redeliveries, with no duplicate, no quarantine, no
    /// rejection and no redelivery_log row (a read is not a producer resend).
    #[test]
    fn a_new_source_instance_restarts_at_zero_and_rereads_without_duplicates() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        for (seq, fact) in [(10, "f1"), (11, "f2"), (12, "f3")] {
            add_fact(&path, seq, fact);
        }
        let state = test_state();
        let reader = source(&path);
        drain_once(&state, &reader, BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();
        let measurements = count(&state, "measurement");
        assert_eq!(count(&state, "usage_event"), 3);

        std::fs::remove_file(&path).unwrap();
        create_store(&path, "instance-b", 1);
        for (seq, fact) in [(1, "f1"), (2, "f2"), (3, "f3")] {
            add_fact(&path, seq, fact);
        }
        let outcome = drain_once(&state, &reader, BROCA_SOURCE_KEY, PAGE_LIMIT, 200).unwrap();

        assert_eq!(outcome.events_recorded, 3, "every fact is read again");
        assert_eq!(outcome.quarantined_redeliveries, 0);
        assert_eq!(
            runtime(&state),
            Some((Some("instance-b:1".into()), Some(3), Some(200), 0))
        );
        assert_eq!(count(&state, "usage_event"), 3);
        assert_eq!(count(&state, "measurement"), measurements);
        assert_eq!(count(&state, "rejected_delivery"), 0);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
        assert_eq!(count(&state, "redelivery_log"), 0);
    }

    /// An older copy of the same database put back: same instance, high water
    /// below the cursor. The drain is refused and the cursor holds.
    #[test]
    fn a_restored_older_copy_of_the_same_store_is_refused() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        for (seq, fact) in [(1, "f1"), (2, "f2"), (3, "f3")] {
            add_fact(&path, seq, fact);
        }
        let state = test_state();
        drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();

        std::fs::remove_file(&path).unwrap();
        create_store(&path, "instance-a", 1);
        add_fact(&path, 1, "f1");
        let result = drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 200);

        assert!(
            matches!(result, Err(PullError::LineageRegressed { .. })),
            "{result:?}"
        );
        assert_eq!(
            runtime(&state),
            Some((Some("instance-a:1".into()), Some(3), Some(100), 0))
        );
    }

    /// No store means BROCA is not installed: nothing to read and nothing
    /// stale. The runtime row the migrations seed is removed, and health
    /// carries no BROCA reason.
    #[test]
    fn an_absent_store_writes_no_runtime_row() {
        let directory = TempDir::new().unwrap();
        let state = test_state();
        assert!(runtime(&state).is_some(), "the migrations seed a BROCA row");

        tick(&state, source(&directory.path().join("run-index.db")));

        assert_eq!(runtime(&state), None);
        assert_eq!(state.producer_degraded_reason(BROCA_SOURCE_KEY), None);
    }

    /// A file that exists and is not a readable store is a failed drain: the
    /// row is kept with unknown pending work, and health names the failure.
    #[test]
    fn a_present_but_unreadable_store_is_a_failed_drain() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        std::fs::write(&path, b"not a sqlite database").unwrap();
        let state = test_state();

        tick(&state, source(&path));

        assert_eq!(runtime(&state), Some((None, None, None, 1)));
        let reason = state
            .producer_degraded_reason(BROCA_SOURCE_KEY)
            .expect("an unreadable store degrades health");
        assert!(reason.contains("read BROCA store"), "{reason}");
    }

    /// A relative data home is refused rather than resolved against the
    /// working directory.
    #[test]
    fn a_relative_data_home_is_refused() {
        let broca = BrocaSource::from_data_home(
            PathBuf::from(".local/share"),
            Arc::new(AtomicBool::new(false)),
        );
        let result = broca.pull(None, PAGE_LIMIT);
        assert!(
            matches!(&result, Err(PullError::Unavailable(reason)) if reason.contains("relative")),
            "{result:?}"
        );
    }

    /// A stop requested before the read abandons it without touching the
    /// store, and the refresher treats it as neither success nor failure.
    #[test]
    fn a_stopped_read_commits_nothing_and_does_not_degrade() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 1, "f1");
        let stop = Arc::new(AtomicBool::new(true));
        let broca = BrocaSource::at(&path, Arc::clone(&stop));
        assert_eq!(
            broca.pull(None, PAGE_LIMIT).unwrap_err(),
            PullError::Stopped
        );

        let state = test_state();
        let before = runtime(&state);
        let result = drain_once(&state, &broca, BROCA_SOURCE_KEY, PAGE_LIMIT, 100);
        assert_eq!(result.unwrap_err(), PullError::Stopped);
        assert_eq!(runtime(&state), before);
        assert_eq!(count(&state, "usage_event"), 0);
    }

    /// SQLite's interrupt is a stop only when this reader's stop flag asked
    /// for it; an interrupt nobody here requested is a failed read.
    #[test]
    fn an_interrupt_is_a_stop_only_when_the_flag_is_set() {
        let interrupted = || {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
                None,
            )
        };
        assert_eq!(
            sqlite_error(&AtomicBool::new(true), "read", interrupted()),
            PullError::Stopped
        );
        assert!(matches!(
            sqlite_error(&AtomicBool::new(false), "read", interrupted()),
            PullError::Unavailable(_)
        ));
    }

    /// A row whose column and segment disagree about the fact id refuses the
    /// page instead of guessing which identity is right.
    #[test]
    fn a_row_whose_fact_id_disagrees_with_its_segment_is_refused() {
        let error = decode_store_row(1, "column-id", &stored_segment("segment-id")).unwrap_err();
        assert!(error.contains("column-id"), "{error}");
        let error = decode_store_row(1, "column-id", &system_one_segment("segment-id", |_| {}))
            .unwrap_err();
        assert!(error.contains("column-id"), "{error}");
    }

    /// A BROCA batch fact as stored: the agreed fixture under `fact_id`,
    /// changed by `edit`.
    fn system_one_segment(
        fact_id: &str,
        edit: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) -> String {
        let mut segment = crate::wire_map::system_one_fixture();
        let object = segment.as_object_mut().unwrap();
        object.insert("fact_id".into(), Value::from(fact_id));
        edit(object);
        segment.to_string()
    }

    /// A batch fact made in a flow: `flow_id` instead of `run_id`, both
    /// counts reported, a work class and no account.
    fn flow_segment(fact_id: &str, flow_id: &str) -> String {
        system_one_segment(fact_id, |object| {
            object.remove("run_id");
            object.remove("account_ref");
            object.insert("flow_id".into(), Value::from(flow_id));
            object.insert("work_class".into(), Value::from("extract"));
            object.insert(
                "usage".into(),
                serde_json::json!({"input_tokens": 40, "output_tokens": 7}),
            );
        })
    }

    /// The stored attribution of every BROCA event, by producer_ref. The
    /// lineage is asserted separately: a tuple holds at most twelve fields.
    type StoredEvent = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<i64>,
        Option<String>,
    );

    fn stored_events(state: &SharedState) -> Vec<StoredEvent> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT producer_ref,harness,project_root,session,
                                run_ref,flow_ref,provider,model,account_ref,charge_basis,
                                occurred_at_ms,work_class
                         FROM usage_event WHERE producer='broca' ORDER BY producer_ref",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                            row.get(8)?,
                            row.get(9)?,
                            row.get(10)?,
                            row.get(11)?,
                        ))
                    })
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// `(producer_ref, role, source, quantity)` of every stored measurement.
    fn stored_measurements(state: &SharedState) -> Vec<(String, String, String, Option<i64>)> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT e.producer_ref,m.role,m.source,m.quantity_units
                         FROM measurement m JOIN usage_event e USING(event_key)
                         ORDER BY e.producer_ref,m.role",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// `(locator, reason)` of every BROCA set-aside row, in locator order.
    fn set_aside_rows(state: &SharedState) -> Vec<(String, String)> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT locator,reason FROM set_aside_record
                         WHERE source_key='broca' ORDER BY locator",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// Session facts and batch facts share `export_facts`. Both are recorded
    /// by one drain: a batch fact under its `fact_id`, with its run or flow
    /// and no session, harness or project, and a measurement for each count
    /// it reported and none for a count it did not.
    #[test]
    fn session_and_system_one_facts_in_one_store_are_both_recorded() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 1, "session-a");
        add_segment(
            &path,
            2,
            "batch-run",
            &system_one_segment("batch-run", |_| {}),
        );
        add_segment(
            &path,
            3,
            "batch-flow",
            &flow_segment("batch-flow", "flow-1"),
        );
        add_fact(&path, 5, "session-b");
        let state = test_state();

        let outcome =
            drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();

        assert_eq!(outcome.events_recorded, 4);
        assert_eq!(
            runtime(&state),
            Some((Some("instance-a:1".into()), Some(5), Some(100), 0))
        );
        assert!(set_aside_rows(&state).is_empty());
        let some = |value: &str| Some(value.to_string());
        let session = |fact: &str| -> StoredEvent {
            (
                fact.into(),
                some("test"),
                some("/project"),
                some("session-1"),
                some("run-1"),
                None,
                some("anthropic"),
                some("claude"),
                None,
                "metered_api".into(),
                Some(1000),
                None,
            )
        };
        assert_eq!(
            stored_events(&state),
            vec![
                (
                    "batch-flow".into(),
                    None,
                    None,
                    None,
                    None,
                    some("flow-1"),
                    some("typesafe"),
                    some("jev-1.13.0"),
                    None,
                    "metered_api".into(),
                    Some(1_792_772_000_000),
                    some("extract"),
                ),
                (
                    "batch-run".into(),
                    None,
                    None,
                    None,
                    some("run-1"),
                    None,
                    some("typesafe"),
                    some("jev-1.13.0"),
                    some("primary"),
                    "metered_api".into(),
                    Some(1_792_772_000_000),
                    None,
                ),
                session("session-a"),
                session("session-b"),
            ]
        );
        assert_eq!(
            count(&state, "usage_event WHERE producer_lineage IS NOT NULL"),
            0
        );
        let auth: Vec<Option<String>> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT auth_selection FROM usage_event
                         WHERE producer_ref LIKE 'batch-%' ORDER BY producer_ref",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(auth, vec![some("apikey:typesafe"), some("apikey:typesafe")]);
        let batch_measurements: Vec<_> = stored_measurements(&state)
            .into_iter()
            .filter(|(fact, ..)| fact.starts_with("batch-"))
            .collect();
        let row = |fact: &str, role: &str, source: &str, quantity: i64| {
            (
                fact.to_string(),
                role.to_string(),
                source.to_string(),
                Some(quantity),
            )
        };
        assert_eq!(
            batch_measurements,
            vec![
                row("batch-flow", "input", "usage.input_tokens", 40),
                row("batch-flow", "output", "usage.output_tokens", 7),
                row("batch-run", "input", "usage.input_tokens", 100),
            ]
        );
    }

    /// The two batch facts BROCA captured from its real producer path, as the
    /// exact `segment_json` bytes it stores (broca
    /// crates/broca-module-serve/tests/golden/, source 32a91294). They pin the
    /// shape BROCA actually writes, not the shape this decoder was written
    /// from: if BROCA renames or nests a member, this drain sets the row aside
    /// and the test fails.
    #[test]
    fn broca_captured_system_one_facts_are_recorded_with_their_provenance() {
        let vector = |name: &str| {
            std::fs::read_to_string(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/vectors/broca-system-one")
                    .join(name),
            )
            .unwrap()
        };
        let run_fact = "2e8d91c64676534cfd6e30783b2b95d7ab90905c8bd8bff6ebf80e91437f3f00";
        let flow_fact = "fc713f0ed72c3b03d4e734619bc163705fe49eb4797815f0aa84e7a5e588c8c9";
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_segment(
            &path,
            1,
            run_fact,
            vector("system_one_run_fact.json").trim_end(),
        );
        add_segment(
            &path,
            2,
            flow_fact,
            vector("system_one_flow_fact.json").trim_end(),
        );
        let state = test_state();

        let outcome =
            drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();

        assert_eq!(outcome.events_recorded, 2);
        assert!(set_aside_rows(&state).is_empty());
        let some = |value: &str| Some(value.to_string());
        let stored = |run: Option<String>, flow: Option<String>, fact: &str| -> StoredEvent {
            (
                fact.into(),
                None,
                None,
                None,
                run,
                flow,
                some("typesafe"),
                some("jev-1.13.0"),
                some("primary"),
                "metered_api".into(),
                Some(1_792_772_000_000),
                None,
            )
        };
        assert_eq!(
            stored_events(&state),
            vec![
                stored(Some("run".into()), None, run_fact),
                stored(None, Some("flow-1".into()), flow_fact),
            ]
        );
        let provenance: Vec<(Option<String>, Option<i64>, Option<String>)> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT auth_selection,charge_basis_mapping_version,charge_basis_overrides
                         FROM usage_event WHERE producer='broca' ORDER BY producer_ref",
                    )
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        let expected = (some("apikey:typesafe:primary"), Some(5), some("none"));
        assert_eq!(provenance, vec![expected.clone(), expected]);
        assert_eq!(
            stored_measurements(&state),
            vec![
                (
                    run_fact.into(),
                    "input".into(),
                    "usage.input_tokens".into(),
                    Some(100)
                ),
                (
                    flow_fact.into(),
                    "input".into(),
                    "usage.input_tokens".into(),
                    Some(100)
                ),
            ]
        );
    }

    /// A row this reader cannot record is set aside with a reason naming the
    /// kind or member, the good rows around it are recorded, and the cursor
    /// passes it. An unknown kind is never decoded as a session fact, even
    /// when the rest of its segment would decode as one.
    #[test]
    fn set_aside_unknown_kind_and_malformed_system_one_rows_keep_good_rows_and_progress() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("run-index.db");
        create_store(&path, "instance-a", 1);
        add_fact(&path, 1, "good-session");
        let mut unknown: Value = serde_json::from_str(&stored_segment("unknown-kind")).unwrap();
        unknown["kind"] = Value::from("system_two");
        add_segment(&path, 2, "unknown-kind", &unknown.to_string());
        add_segment(
            &path,
            3,
            "no-provider",
            &system_one_segment("no-provider", |object| {
                object.remove("provider");
            }),
        );
        add_segment(
            &path,
            4,
            "run-and-flow",
            &system_one_segment("run-and-flow", |object| {
                object.insert("flow_id".into(), Value::from("flow-1"));
            }),
        );
        add_segment(
            &path,
            5,
            "no-run-or-flow",
            &system_one_segment("no-run-or-flow", |object| {
                object.remove("run_id");
            }),
        );
        add_segment(
            &path,
            6,
            "good-batch",
            &system_one_segment("good-batch", |_| {}),
        );
        let mut model_step: Value = serde_json::from_str(&stored_segment("unused-step")).unwrap();
        model_step["kind"] = Value::from("model_step");
        add_segment(&path, 7, "unused-step", &model_step.to_string());
        let state = test_state();
        let broca = source(&path);
        for now in [100, 200] {
            drain_once(&state, &broca, BROCA_SOURCE_KEY, PAGE_LIMIT, now).unwrap();
            assert_eq!(
                stored_events(&state)
                    .into_iter()
                    .map(|event| event.0)
                    .collect::<Vec<_>>(),
                vec!["good-batch".to_string(), "good-session".to_string()]
            );
            assert_eq!(runtime(&state).unwrap().1, Some(7));
            let set_aside = set_aside_rows(&state);
            assert_eq!(set_aside.len(), 5, "{set_aside:?}");
            for ((locator, reason), (seq, expected)) in set_aside.iter().zip([
                (2, "unknown kind \"system_two\""),
                (3, "missing field `provider`"),
                (4, "both run_id and flow_id"),
                (5, "neither run_id nor flow_id"),
                (7, "unknown kind \"model_step\""),
            ]) {
                assert!(locator.ends_with(&format!("export_seq:{seq}")), "{locator}");
                assert!(reason.contains(expected), "{seq}: {reason}");
            }
            crate::refresher::harness_drain::assert_set_aside(&state, "broca", 5);
        }
        assert_eq!(count(&state, "measurement"), 5 + 1);
    }

    /// Batch facts re-read from a rebuilt store are exact redeliveries, and a
    /// re-read batch fact whose flow changed is a contradiction: quarantined,
    /// with the stored event left as first written.
    #[test]
    fn a_reread_system_one_fact_is_an_exact_redelivery_and_a_changed_flow_quarantines() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run-index.db");
        let write_store = |instance: &str, flow_id: &str| {
            if path.exists() {
                std::fs::remove_file(&path).unwrap();
            }
            create_store(&path, instance, 1);
            add_fact(&path, 1, "session-a");
            add_segment(
                &path,
                2,
                "batch-run",
                &system_one_segment("batch-run", |_| {}),
            );
            add_segment(&path, 3, "batch-flow", &flow_segment("batch-flow", flow_id));
        };
        write_store("instance-a", "flow-1");
        let state = test_state();
        drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();
        let events = stored_events(&state);
        let measurements = stored_measurements(&state);
        assert_eq!(events.len(), 3);

        write_store("instance-b", "flow-1");
        let outcome =
            drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 200).unwrap();
        assert_eq!(outcome.quarantined_redeliveries, 0);
        assert_eq!(stored_events(&state), events);
        assert_eq!(stored_measurements(&state), measurements);
        assert_eq!(count(&state, "rejected_delivery"), 0);
        assert_eq!(count(&state, "redelivery_log"), 0);

        write_store("instance-c", "flow-2");
        let outcome =
            drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 300).unwrap();
        assert_eq!(outcome.quarantined_redeliveries, 1);
        assert_eq!(stored_events(&state), events, "the stored flow is kept");
        assert_eq!(stored_measurements(&state), measurements);
        let rejected: Vec<(String, String)> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT producer_ref,incoming_payload FROM rejected_delivery")
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(rejected.len(), 1, "{rejected:?}");
        assert_eq!(rejected[0].0, "batch-flow");
        assert!(
            rejected[0].1.contains(r#""flow_ref":"flow-2""#),
            "{}",
            rejected[0].1
        );
        assert_eq!(
            runtime(&state),
            Some((Some("instance-c:1".into()), Some(3), Some(300), 0))
        );
    }

    /// Reconciles this reader against a ledger the `usage.export` pull filled.
    /// Needs backup copies of BOTH live stores: BROCA's `run-index.db` in
    /// `BROCA_RECONCILE_STORE` and astrocyte's `store.db` in
    /// `ASTROCYTE_RECONCILE_STORE`. Every fact in `export_facts` is read with
    /// this reader and recorded into the astrocyte copy inside a transaction
    /// that is rolled back, so the copy is left as it was.
    #[test]
    #[ignore]
    fn reconcile_store_copies() {
        use std::collections::{BTreeMap, BTreeSet};

        use astrocyte_core::ingest::{record_event_in_tx, DeliveryPath, RecordDisposition};

        let broca_path = std::env::var("BROCA_RECONCILE_STORE").expect("BROCA_RECONCILE_STORE");
        let ledger_path =
            std::env::var("ASTROCYTE_RECONCILE_STORE").expect("ASTROCYTE_RECONCILE_STORE");
        let broca = source(Path::new(&broca_path));

        // Every fact, page by page from the start.
        let mut requests = Vec::new();
        let mut cursor = None;
        loop {
            let page = broca.pull(cursor.clone(), PAGE_LIMIT).unwrap();
            let done = page.events.is_empty();
            requests.extend(page.events);
            cursor = Some(page.cursor);
            if done {
                break;
            }
        }
        let full_cursor = cursor.unwrap();

        let mut ledger = Connection::open(&ledger_path).unwrap();
        let stored_cursor: (String, i64, i64) = ledger
            .query_row(
                "SELECT lineage,consumed_ordinal,high_water_ordinal FROM source_runtime
                 WHERE source_key='broca'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let stored = SourceCursor {
            lineage: stored_cursor.0.clone(),
            ordinal: stored_cursor.1,
        };
        let first_tick = broca.pull(Some(stored.clone()), PAGE_LIMIT).unwrap();
        println!(
            "cursor: pull stored {stored_cursor:?}; reader reading all of export_facts ends at \
             ({}, {}); first tick from the stored cursor reads {} facts and would store ({}, {}) \
             high water {}",
            full_cursor.lineage,
            full_cursor.ordinal,
            first_tick.events.len(),
            first_tick.cursor.lineage,
            first_tick.cursor.ordinal,
            first_tick.high_water.ordinal
        );

        let reader_ids: BTreeMap<String, String> = requests
            .iter()
            .map(|request| {
                (
                    astrocyte_core::fact::event_id(
                        &request.producer,
                        request.producer_lineage.as_deref(),
                        &request.producer_ref,
                    )
                    .unwrap(),
                    request.producer_ref.clone(),
                )
            })
            .collect();
        let ledger_ids: BTreeMap<String, String> = {
            let mut statement = ledger
                .prepare("SELECT event_id,producer_ref FROM usage_event WHERE producer='broca'")
                .unwrap();
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            rows
        };
        let reader_set: BTreeSet<&String> = reader_ids.keys().collect();
        let ledger_set: BTreeSet<&String> = ledger_ids.keys().collect();
        println!(
            "facts read {}; distinct reader event ids {}; ledger broca events {}",
            requests.len(),
            reader_set.len(),
            ledger_set.len()
        );
        for id in reader_set.difference(&ledger_set) {
            println!(
                "only in BROCA's export_facts: event {id} fact {}",
                reader_ids[*id]
            );
        }
        for id in ledger_set.difference(&reader_set) {
            println!(
                "only in astrocyte's ledger: event {id} fact {}",
                ledger_ids[*id]
            );
        }

        let tx = ledger.transaction().unwrap();
        let counts = |tx: &rusqlite::Transaction<'_>| -> (i64, i64, i64) {
            tx.query_row(
                "SELECT (SELECT COUNT(*) FROM rejected_delivery),
                        (SELECT COUNT(*) FROM quarantine_evidence),
                        (SELECT COUNT(*) FROM redelivery_log)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
        };
        let before = counts(&tx);
        let (mut recorded, mut exact, mut refused) = (0, 0, Vec::new());
        let mut quarantined = Vec::new();
        for request in &requests {
            match record_event_in_tx(&tx, request, 1, DeliveryPath::Read) {
                Ok(outcome) => match outcome.disposition {
                    RecordDisposition::Recorded => recorded += 1,
                    RecordDisposition::ExactRedelivery => exact += 1,
                    RecordDisposition::QuarantinedRedelivery => quarantined.push(request),
                },
                Err(error) => refused.push((request.producer_ref.clone(), format!("{error:?}"))),
            }
        }
        let after = counts(&tx);
        println!(
            "recorded into the ledger copy: already recorded {exact}, newly recorded {recorded}, \
             quarantined {}, refused {}; rejected_delivery/quarantine_evidence/\
             redelivery_log rows {before:?} -> {after:?}",
            quarantined.len(),
            refused.len()
        );
        for (fact, error) in &refused {
            println!("refused: fact {fact}: {error}");
        }

        // Which stored columns the quarantined facts disagree on. Each
        // quarantined request is recorded again with the disagreeing
        // top-level columns replaced by the stored values: one that then
        // restates the stored event differed in those columns only.
        type Stored = (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<String>,
        );
        let mut shapes: BTreeMap<String, usize> = BTreeMap::new();
        let mut explained = 0;
        for request in &quarantined {
            let stored: Stored = tx
                .query_row(
                    "SELECT auth_selection,account_ref,work_class,charge_basis_mapping_version,
                            charge_basis_overrides
                     FROM usage_event WHERE producer='broca' AND producer_ref=?1",
                    [&request.producer_ref],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .unwrap();
            let mut differing = Vec::new();
            let mut adjusted = (*request).clone();
            if stored.0 != request.auth_selection {
                differing.push(format!(
                    "auth_selection stored {:?} read {:?}",
                    stored.0.as_deref().map(|_| "value"),
                    request.auth_selection.as_deref().map(|_| "value")
                ));
                adjusted.auth_selection = stored.0.clone();
            }
            if stored.1 != request.account_ref {
                differing.push("account_ref".into());
                adjusted.account_ref = stored.1.clone();
            }
            if stored.2 != request.work_class {
                differing.push("work_class".into());
                adjusted.work_class = stored.2.clone();
            }
            let restated = matches!(
                record_event_in_tx(&tx, &adjusted, 1, DeliveryPath::Read).map(|o| o.disposition),
                Ok(RecordDisposition::ExactRedelivery)
            );
            if restated {
                explained += 1;
            } else {
                differing.push("something else".into());
            }
            *shapes.entry(differing.join("; ")).or_default() += 1;
        }
        tx.rollback().unwrap();
        println!(
            "quarantined facts explained by the columns below alone: {explained} of {}",
            quarantined.len()
        );
        for (shape, count) in &shapes {
            println!("  {count} x {shape}");
        }
        let refs: BTreeSet<&str> = quarantined
            .iter()
            .map(|r| r.producer_ref.as_str())
            .collect();
        println!(
            "first and last quarantined fact: {:?} {:?}",
            refs.first(),
            refs.last()
        );
    }
}
