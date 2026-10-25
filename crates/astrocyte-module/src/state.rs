//! Shared serving state, desired configuration, and public projections.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use astrocyte_core::{
    capacity::CapacityFreshness, catalog_client::CatalogClient, store::StoreInitError,
};
use cortexkit_store::SqliteStore;
use rusqlite::{Connection, OpenFlags, Transaction};
use serde_json::{json, Value};

use crate::config::{self, AstrocyteConfig};

#[cfg(any(test, feature = "test-support"))]
use astrocyte_core::store::MIGRATIONS;

enum StateStore {
    /// The production store: the leased writer connection, and a second,
    /// read-only connection to the same file that served reports run on, so a
    /// long report never holds the connection the writer commits through.
    /// The store is boxed because it is much larger than the test-only
    /// variant; the state is built once per process, so the indirection is free.
    Leased {
        store: Box<SqliteStore>,
        reader: Mutex<Connection>,
    },
    /// A single in-memory connection. An in-memory database cannot be opened
    /// by a second connection, so reads and writes share this one; tests that
    /// need the separate reader build a `Leased` state over a temporary file.
    #[cfg(any(test, feature = "test-support"))]
    Direct(Mutex<Connection>),
}

#[derive(Clone)]
struct SourceRuntime {
    last_success_ms: Option<i64>,
}

#[derive(Clone)]
struct ProjectionSnapshot {
    source_runtime: Vec<SourceRuntime>,
    capacity_runtime: Option<CapacityFreshness>,
}

struct DesiredSnapshot {
    config: AstrocyteConfig,
    generation: u64,
    degraded: Option<String>,
}

struct Inner {
    store: StateStore,
    writer: Mutex<()>,
    desired: RwLock<DesiredSnapshot>,
    catalog: RwLock<Option<CatalogClient>>,
    producer_degraded: RwLock<BTreeMap<String, String>>,
    projection: RwLock<ProjectionSnapshot>,
    projection_degraded: RwLock<Option<String>>,
    crash: tokio::sync::watch::Sender<Option<String>>,
    /// Last observed store presence per source, kept after an absent store deletes
    /// its runtime row so the change is logged only once.
    source_discovery: Mutex<HashMap<String, bool>>,
    quota_bus: crate::quota_events::Control,
    /// Recent harness steps kept for `usage.cache.steps`. Lock order: this
    /// mutex before either store connection, never the other way round.
    recent_steps: Mutex<crate::recent_steps::RecentSteps>,
    /// The live step feed's subscriptions (`usage.steps.subscribe`).
    step_feed: crate::step_feed::FeedHub,
    /// Test-only clock for the recent-steps memory, whose 24-hour window is
    /// otherwise measured from the wall clock. See `set_recent_steps_now_for_test`.
    #[cfg(test)]
    recent_steps_now: Mutex<Option<i64>>,
    /// Test-only hold on the refresher loop's pricing pass, scoped to this
    /// state so a held test never silences the pass of a test running beside
    /// it. See `hold_pricing_pass_for_test` in refresher.rs.
    #[cfg(any(test, feature = "test-support"))]
    pricing_pass_hold: std::sync::atomic::AtomicBool,
    /// Test-only pause for the next served read, whichever connection it runs
    /// on. See `pause_next_read_for_test`.
    #[cfg(any(test, feature = "test-support"))]
    read_pause: Mutex<Option<ReadPause>>,
    /// Test-only count of the times this state's refresher loop has begun
    /// its wait between ticks. See `interval_waits_for_test`.
    #[cfg(test)]
    interval_waits: std::sync::atomic::AtomicU64,
}

#[cfg(any(test, feature = "test-support"))]
type ReadPause = Box<dyn FnOnce(&Connection) + Send>;

#[derive(Clone)]
pub struct SharedState(Arc<Inner>);

struct WriteCrashGuard<'a>(&'a SharedState);

impl Drop for WriteCrashGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Transaction and writer guards unwind first, rolling back the
            // transaction before the serving owner is told to stop.
            self.0.report_crash("store write panicked".into());
        }
    }
}

/// Why an ordered write, or a read on the reader connection, did not
/// complete. The two causes are kept apart so the caller's own error `E`
/// reaches it unchanged (for a served operation that is its `(code, message)`
/// refusal, whose code clients branch on), while a failure raised by SQLite at
/// BEGIN or COMMIT, by the store wrapper, or by a poisoned lock is carried as
/// its rendered message. Projection maintenance cannot undo a committed write.
/// Nothing is replaced by a stand-in error that names a condition which never
/// happened, and nothing is recovered by re-parsing a rendered message.
enum StoreFailure<E> {
    Operation(E),
    Store(String),
}

fn sqlite_write_failure<E>(error: rusqlite::Error) -> StoreFailure<E> {
    StoreFailure::Store(format!("store write: {}", describe_sqlite_error(&error)))
}

fn sqlite_read_failure<E>(error: rusqlite::Error) -> StoreFailure<E> {
    StoreFailure::Store(format!("store read: {}", describe_sqlite_error(&error)))
}

/// Run one ordered write transaction on `conn`: BEGIN, the caller's
/// operation, COMMIT. Tail reads may roll back an unchanged transaction instead.
/// Any error rolls the transaction back when it drops.
fn run_ordered_transaction<T, E>(
    conn: &Connection,
    operation: impl FnOnce(&Transaction<'_>) -> Result<(T, bool), E>,
    skip_unchanged: bool,
) -> Result<(T, bool), StoreFailure<E>> {
    let tx = conn.unchecked_transaction().map_err(sqlite_write_failure)?;
    let before = tx.total_changes();
    let output = operation(&tx).map_err(StoreFailure::Operation)?;
    if skip_unchanged && tx.total_changes() == before {
        tx.rollback().map_err(sqlite_write_failure)?;
        return Ok((output.0, false));
    }
    tx.commit().map_err(sqlite_write_failure)?;
    #[cfg(test)]
    TRANSACTION_COMMITS.with(|count| count.set(count.get() + 1));
    Ok(output)
}

#[cfg(test)]
thread_local! {
    static TRANSACTION_COMMITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn transaction_commits_for_test() -> usize {
    TRANSACTION_COMMITS.with(std::cell::Cell::get)
}

/// Render a SQLite error with its text and, when SQLite supplied one, its
/// extended result code, so a log line can tell a full disk
/// (`database or disk is full`, code 13) from a busy timeout or a constraint
/// failure.
fn describe_sqlite_error(error: &rusqlite::Error) -> String {
    match error.sqlite_error() {
        Some(sqlite) => format!(
            "{error} (sqlite code {}, {:?})",
            sqlite.extended_code, sqlite.code
        ),
        None => error.to_string(),
    }
}

/// Prepare a freshly opened connection and read the committed desired
/// configuration from it.
fn initialize_and_hydrate(conn: &Connection) -> Result<(AstrocyteConfig, u64), String> {
    astrocyte_core::store::initialize_connection(conn).map_err(|error| match error {
        StoreInitError::Sql(error) => describe_sqlite_error(&error),
        StoreInitError::ForeignKeysDisabled => {
            "foreign key enforcement did not turn on".to_string()
        }
    })?;
    hydrate_config(conn)
}

/// Open the read-only connection that served reports run on, beside the
/// store's writer connection.
///
/// Called after the store is opened and migrated, so the file exists and is
/// already in WAL mode; WAL is what lets a reader hold a snapshot while the
/// writer commits, so any other journal mode is refused rather than served
/// with reports that would block ingest again. The reader opens the file
/// directly and takes no lease: the lease excludes a second WRITER, and this
/// connection cannot write. It gets the writer's busy timeout, read back from
/// the writer connection so the two cannot drift apart, then applies report-only
/// mapping and page-cache settings. It does not run
/// `initialize_connection`, whose capability probe creates tables and would
/// fail on a read-only connection; foreign-key enforcement is moot for a
/// connection that cannot write.
fn open_reader(store: &SqliteStore) -> Result<Connection, String> {
    let mut settings = None;
    store
        .with_conn(|conn| {
            settings = Some(read_writer_settings(conn));
            Ok(())
        })
        .map_err(|error| format!("open store reader: {error}"))?;
    let (path, busy_timeout_ms) = settings
        .unwrap_or_else(|| Err("the store did not run the settings read".to_string()))
        .map_err(|error| format!("open store reader: {error}"))?;
    // The writer is opened with rusqlite's default flags (read-write, create,
    // URI, no-mutex). The reader keeps URI and NO_MUTEX (its own Rust mutex
    // serializes it) and swaps read-write and create for READ_ONLY.
    let reader = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("open store reader: {}", describe_sqlite_error(&error)))?;
    reader
        .busy_timeout(Duration::from_millis(busy_timeout_ms))
        .map_err(|error| format!("open store reader: {}", describe_sqlite_error(&error)))?;
    astrocyte_core::report::configure_report_reader(&reader)
        .map_err(|error| format!("open store reader: {}", describe_sqlite_error(&error)))?;
    Ok(reader)
}

/// The writer connection's file path and busy timeout, after checking it is
/// in WAL mode.
fn read_writer_settings(conn: &Connection) -> Result<(String, u64), String> {
    let path = conn
        .path()
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "the store has no file path to open a reader on".to_string())?
        .to_string();
    let journal_mode: String = conn
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(|error| describe_sqlite_error(&error))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(format!(
            "the store is in journal mode '{journal_mode}', not WAL; a separate reader \
             would block the writer"
        ));
    }
    let busy_timeout_ms: i64 = conn
        .pragma_query_value(None, "busy_timeout", |row| row.get(0))
        .map_err(|error| describe_sqlite_error(&error))?;
    let busy_timeout_ms = u64::try_from(busy_timeout_ms.max(0))
        .map_err(|_| "busy timeout does not fit in u64".to_string())?;
    Ok((path, busy_timeout_ms))
}

impl SharedState {
    pub fn from_store(store: SqliteStore) -> Result<Self, String> {
        // The closure always returns Ok so that `SqliteStore::with_conn` only
        // fails for reasons of its own; the hydration result travels out
        // beside it with its message intact.
        let mut hydrated = None;
        store
            .with_conn(|conn| {
                hydrated = Some(initialize_and_hydrate(conn));
                Ok(())
            })
            .map_err(|error| format!("hydrate state: {error}"))?;
        let (config, generation) = hydrated
            .unwrap_or_else(|| Err("the store did not run the hydration".to_string()))
            .map_err(|error| format!("hydrate state: {error}"))?;
        let reader = open_reader(&store)?;
        let state = Self::from_parts(
            StateStore::Leased {
                store: Box::new(store),
                reader: Mutex::new(reader),
            },
            config,
            generation,
        );
        state.refresh_projection()?;
        Ok(state)
    }

    fn from_parts(store: StateStore, config: AstrocyteConfig, generation: u64) -> Self {
        Self(Arc::new(Inner {
            store,
            writer: Mutex::new(()),
            crash: tokio::sync::watch::channel(None).0,
            desired: RwLock::new(DesiredSnapshot {
                config,
                generation,
                degraded: None,
            }),
            catalog: RwLock::new(None),
            producer_degraded: RwLock::new(BTreeMap::new()),
            projection_degraded: RwLock::new(None),
            projection: RwLock::new(ProjectionSnapshot {
                source_runtime: Vec::new(),
                capacity_runtime: None,
            }),
            source_discovery: Mutex::new(HashMap::new()),
            quota_bus: crate::quota_events::Control::default(),
            recent_steps: Mutex::new(crate::recent_steps::RecentSteps::default()),
            step_feed: crate::step_feed::FeedHub::default(),
            #[cfg(test)]
            recent_steps_now: Mutex::new(None),
            #[cfg(any(test, feature = "test-support"))]
            pricing_pass_hold: std::sync::atomic::AtomicBool::new(false),
            #[cfg(any(test, feature = "test-support"))]
            read_pause: Mutex::new(None),
            #[cfg(test)]
            interval_waits: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// A served read, run inside ONE read transaction on the reader connection.
    ///
    /// The reader is a separate read-only connection to the same WAL-mode
    /// file, guarded by its own mutex. It takes neither the writer fence nor
    /// the writer's connection mutex, so a slow report does not hold up an
    /// ingest commit, and a commit does not wait for the report. The
    /// transaction holds one snapshot from the first SELECT until `COMMIT`, so
    /// every SELECT in the operation sees the same committed state, and a
    /// commit made meanwhile is seen by the next call. SQLite refuses a write
    /// on this connection.
    ///
    /// The operation's own `(code, message)` refusal is returned unchanged;
    /// only a failure the operation did not produce (SQLite at BEGIN or COMMIT
    /// on the reader, a poisoned lock) is reported under the code `internal`.
    pub(crate) fn with_read_op<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, (String, String)>,
    ) -> Result<T, (String, String)> {
        self.with_read_conn_split(operation)
            .map_err(|failure| match failure {
                StoreFailure::Operation(refusal) => refusal,
                StoreFailure::Store(error) => ("internal".into(), error),
            })
    }

    /// The body of `with_read_op`.
    fn with_read_conn_split<T, E>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, E>,
    ) -> Result<T, StoreFailure<E>> {
        match &self.0.store {
            // A poisoned reader mutex is read through: the reader holds no
            // state between calls, and a transaction a panicking call left
            // open was rolled back when it unwound.
            StateStore::Leased { reader, .. } => {
                let guard = reader
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                self.run_read_transaction(&guard, operation)
            }
            #[cfg(any(test, feature = "test-support"))]
            StateStore::Direct(connection) => {
                let guard = connection
                    .lock()
                    .map_err(|_| StoreFailure::Store("store mutex poisoned".into()))?;
                self.run_read_transaction(&guard, operation)
            }
        }
    }

    fn run_read_transaction<T, E>(
        &self,
        conn: &Connection,
        operation: impl FnOnce(&Connection) -> Result<T, E>,
    ) -> Result<T, StoreFailure<E>> {
        let tx = conn.unchecked_transaction().map_err(sqlite_read_failure)?;
        #[cfg(any(test, feature = "test-support"))]
        self.run_read_pause(&tx);
        let output = operation(&tx).map_err(StoreFailure::Operation)?;
        tx.commit().map_err(sqlite_read_failure)?;
        Ok(output)
    }

    /// Make the next served read, on either connection, run `pause` first.
    /// A test uses this to hold a report mid-flight at a point of its choosing.
    #[cfg(test)]
    pub(crate) fn pause_next_read_for_test(
        &self,
        pause: impl FnOnce(&Connection) + Send + 'static,
    ) {
        *self.0.read_pause.lock().unwrap() = Some(Box::new(pause));
    }

    #[cfg(any(test, feature = "test-support"))]
    fn run_read_pause(&self, conn: &Connection) {
        let pause = self.0.read_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause(conn);
        }
    }

    pub fn with_conn<T>(
        &self,
        operation: impl FnOnce(&Connection) -> Result<T, String>,
    ) -> Result<T, String> {
        match &self.0.store {
            StateStore::Leased { store, .. } => {
                // The closure always returns Ok so that `SqliteStore::with_conn`
                // only fails for reasons of its own; the operation's result
                // travels out beside it untouched.
                let mut outcome = None;
                store
                    .with_conn(|conn| {
                        #[cfg(any(test, feature = "test-support"))]
                        self.run_read_pause(conn);
                        outcome = Some(operation(conn));
                        Ok(())
                    })
                    .map_err(|error| format!("store read: {error}"))?;
                outcome.unwrap_or_else(|| Err("store read: the store did not run the read".into()))
            }
            #[cfg(any(test, feature = "test-support"))]
            StateStore::Direct(connection) => connection
                .lock()
                .map_err(|_| "store mutex poisoned".to_string())
                .and_then(|guard| {
                    self.run_read_pause(&guard);
                    operation(&guard)
                }),
        }
    }

    pub(crate) fn ordered_write<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<(T, bool), String>,
    ) -> Result<T, String> {
        self.ordered_write_split(operation, false)
            .map_err(|failure| match failure {
                StoreFailure::Operation(error) | StoreFailure::Store(error) => error,
            })
    }

    /// Tail reads can re-emit already recorded events while seeding their in-memory cursor.
    /// Roll back a transaction with no SQLite changes rather than committing an empty drain.
    pub(crate) fn ordered_tail_write<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<(T, bool), String>,
    ) -> Result<T, String> {
        self.ordered_write_split(operation, true)
            .map_err(|failure| match failure {
                StoreFailure::Operation(error) | StoreFailure::Store(error) => error,
            })
    }

    /// Panic after inserting into a scratch table inside the production write
    /// boundary, so serving tests can observe rollback and process shutdown.
    #[cfg(any(test, feature = "test-support"))]
    pub fn panic_write_for_test(&self) {
        let _: Result<(), String> = self.ordered_write(|tx| {
            tx.execute("INSERT INTO crash_probe VALUES (1)", [])
                .unwrap();
            panic!("injected writer panic");
        });
    }

    /// Panic in a served read to exercise the real route-handler crash boundary.
    #[cfg(any(test, feature = "test-support"))]
    pub fn panic_next_read_for_test(&self) {
        *self.0.read_pause.lock().unwrap() = Some(Box::new(|_| panic!("injected handler panic")));
    }

    /// A served operation's write. The operation's own `(code, message)`
    /// refusal is returned unchanged; only a failure the operation did not
    /// produce (SQLite at BEGIN or COMMIT or the store wrapper) is reported
    /// under the code `internal`. A failed projection refresh is logged,
    /// but the operation's committed result is still returned.
    pub(crate) fn ordered_op_write<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<(T, bool), (String, String)>,
    ) -> Result<T, (String, String)> {
        self.ordered_write_split(operation, false)
            .map_err(|failure| match failure {
                StoreFailure::Operation(refusal) => refusal,
                StoreFailure::Store(error) => ("internal".into(), error),
            })
    }

    /// The shared body of `ordered_write` and `ordered_op_write`: one
    /// transaction under the writer fence, then a projection refresh when the
    /// operation reports a change. The error keeps the operation's own error
    /// apart from every other failure so each wrapper can report both honestly.
    fn ordered_write_split<T, E>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<(T, bool), E>,
        skip_unchanged: bool,
    ) -> Result<T, StoreFailure<E>> {
        let _crash_guard = WriteCrashGuard(self);
        let _writer = self.0.writer.lock().map_err(|_| {
            self.report_crash("store writer fence poisoned".into());
            StoreFailure::Store("store writer fence poisoned".into())
        })?;
        let (value, changed) = match &self.0.store {
            StateStore::Leased { store, .. } => {
                // As in `with_conn`: the store wrapper's own error and the
                // transaction's error are reported separately, and neither is
                // swapped for a placeholder.
                let mut outcome = None;
                store
                    .with_conn(|conn| {
                        outcome = Some(run_ordered_transaction(conn, operation, skip_unchanged));
                        Ok(())
                    })
                    .map_err(|error| StoreFailure::Store(format!("store write: {error}")))?;
                outcome.unwrap_or_else(|| {
                    Err(StoreFailure::Store(
                        "store write: the store did not run the transaction".into(),
                    ))
                })?
            }
            #[cfg(any(test, feature = "test-support"))]
            StateStore::Direct(connection) => {
                let guard = connection
                    .lock()
                    .map_err(|_| StoreFailure::Store("store mutex poisoned".into()))?;
                run_ordered_transaction(&guard, operation, skip_unchanged)?
            }
        };
        if changed {
            if let Err(error) = self.refresh_projection() {
                tracing::error!(target: "astrocyte", %error,
                    "write committed but projection refresh failed");
                *self
                    .0
                    .projection_degraded
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(format!("projection stale: {error}"));
            }
        }
        Ok(value)
    }

    /// Notify the serving owner of a broken internal invariant. Keep the
    /// first cause so simultaneous failures do not obscure the original crash.
    pub fn report_crash(&self, cause: String) {
        self.0.crash.send_if_modified(|current| {
            if current.is_some() {
                false
            } else {
                *current = Some(cause);
                true
            }
        });
    }

    /// The first crash cause, also available while draining an intended stop.
    pub fn crash_cause(&self) -> Option<String> {
        self.0.crash.borrow().clone()
    }

    /// Wait for a crash, including one that happened before serving began.
    pub async fn crashed(&self) -> String {
        let mut crash = self.0.crash.subscribe();
        loop {
            if let Some(cause) = crash.borrow_and_update().clone() {
                return cause;
            }
            crash
                .changed()
                .await
                .expect("shared state owns the crash sender");
        }
    }

    pub fn public_projection(&self) -> PublicProjection {
        PublicProjection(self.clone())
    }
    pub fn desired_config(&self) -> AstrocyteConfig {
        self.0
            .desired
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .config
            .clone()
    }

    /// Share the one launch-nonce handoff with the attested ckbus client, not other peers.
    pub fn install_quota_bus_access(
        &self,
        path: std::path::PathBuf,
        module_id: String,
        nonce: String,
    ) {
        self.0.quota_bus.install_access(path, module_id, nonce);
    }

    pub(crate) fn quota_bus(&self) -> &crate::quota_events::Control {
        &self.0.quota_bus
    }

    pub(crate) fn capacity_poll_ticket(
        &self,
        now_ms: i64,
    ) -> Option<crate::quota_events::PollTicket> {
        let projection = self.0.projection.read().ok();
        self.quota_bus().poll_ticket(
            now_ms,
            projection
                .as_ref()
                .and_then(|p| p.capacity_runtime.as_ref()),
        )
    }

    pub fn catalog_client(&self) -> Option<CatalogClient> {
        self.0
            .catalog
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn configure_capacity_runtime(
        &self,
        enabled: bool,
        cadence_ms: i64,
        now_ms: i64,
    ) -> Result<(), String> {
        let desired_generation = i64::try_from(self.desired_generation())
            .map_err(|_| "desired generation exceeds SQLite range".to_string())?;
        self.ordered_write(|tx| {
            if enabled {
                tx.execute(
                    "UPDATE capacity_runtime SET
                       desired_generation=?1,
                       effective_cadence_ms=?2,
                       runtime_variant=CASE WHEN runtime_variant='disabled'
                                            THEN 'configured_unavailable'
                                            ELSE runtime_variant END,
                       changed_at_ms=NULL,
                       enabled_at_ms=CASE WHEN runtime_variant='disabled' THEN ?3
                                          ELSE enabled_at_ms END,
                       detail=CASE WHEN runtime_variant='disabled'
                                   THEN 'no usable poll completed' ELSE detail END
                     WHERE id=1",
                    rusqlite::params![desired_generation, cadence_ms, now_ms],
                )
                .map_err(|error| error.to_string())?;
            } else {
                tx.execute(
                    "UPDATE capacity_runtime SET
                       desired_generation=?1,
                       effective_cadence_ms=?2,
                       runtime_variant='disabled',
                       changed_at_ms=CASE
                         WHEN runtime_variant='disabled' AND effective_cadence_ms=?2
                         THEN changed_at_ms ELSE ?3 END,
                       enabled_at_ms=NULL,
                       last_attempt_ms=NULL,
                       last_success_ms=NULL,
                       degraded_entries=0,
                       detail=NULL
                     WHERE id=1",
                    rusqlite::params![desired_generation, cadence_ms, now_ms],
                )
                .map_err(|error| error.to_string())?;
            }
            Ok(((), true))
        })
    }

    pub(crate) fn install_catalog(&self, catalog: Option<CatalogClient>) -> Result<(), String> {
        *self
            .0
            .catalog
            .write()
            .map_err(|_| "catalog lock poisoned".to_string())? = catalog;
        Ok(())
    }

    /// Whether this state's refresher loop is holding its pricing pass.
    /// Test-only; see `hold_pricing_pass_for_test` in refresher.rs.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn pricing_pass_held(&self) -> bool {
        self.0
            .pricing_pass_hold
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn set_pricing_pass_hold(&self, held: bool) {
        self.0
            .pricing_pass_hold
            .store(held, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many times this state's refresher loop has begun its wait between
    /// ticks. Test-only: a test that must stop the loop during that wait
    /// waits for this to move instead of guessing with a sleep.
    #[cfg(test)]
    pub(crate) fn interval_waits_for_test(&self) -> u64 {
        self.0
            .interval_waits
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn note_interval_wait_for_test(&self) {
        self.0
            .interval_waits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn desired_generation(&self) -> u64 {
        self.0
            .desired
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
    }
    pub(crate) fn has_committed_desired_generation(&self) -> bool {
        self.desired_generation() > 0
    }
    pub fn publish_desired(&self, config: AstrocyteConfig, generation: u64) {
        // Replace the complete startup publication at once. Even after a
        // panic, readers see a coherent config, generation and degraded state.
        *self
            .0
            .desired
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = DesiredSnapshot {
            config,
            generation,
            degraded: None,
        };
        let _ = self.refresh_projection();
    }
    pub(crate) fn mark_config_degraded(&self, message: String) {
        self.0
            .desired
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .degraded = Some(message);
        let _ = self.refresh_projection();
    }

    pub(crate) fn mark_producer_degraded(&self, source_key: &str, reason: String) {
        self.0
            .producer_degraded
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(source_key.to_owned(), reason);
    }

    pub(crate) fn clear_producer_degraded(&self, source_key: &str) {
        self.0
            .producer_degraded
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(source_key);
    }

    /// Returns true when this source's store is first seen or changes presence.
    pub(crate) fn source_discovery_changed(&self, source_key: &str, present: bool) -> bool {
        let mut outcomes = self
            .0
            .source_discovery
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        outcomes.insert(source_key.to_owned(), present) != Some(present)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn producer_degraded_reason(&self, source_key: &str) -> Option<String> {
        self.0
            .producer_degraded
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(source_key)
            .cloned()
    }

    fn refresh_projection(&self) -> Result<(), String> {
        let (source_runtime, capacity_runtime) = self.with_conn(|conn| {
            let mut statement = conn
                .prepare("SELECT last_success_ms FROM source_runtime ORDER BY source_key")
                .map_err(|error| error.to_string())?;
            let source_runtime = statement
                .query_map([], |row| {
                    Ok(SourceRuntime {
                        last_success_ms: row.get(0)?,
                    })
                })
                .map_err(|error| error.to_string())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| error.to_string())?;
            let capacity_runtime =
                astrocyte_core::capacity::freshness(conn).map_err(|error| error.to_string())?;
            Ok((source_runtime, Some(capacity_runtime)))
        })?;
        *self
            .0
            .projection
            .write()
            .map_err(|_| "projection lock poisoned".to_string())? = ProjectionSnapshot {
            source_runtime,
            capacity_runtime,
        };
        *self
            .0
            .projection_degraded
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.advance_recent_steps();
        Ok(())
    }

    /// Fold the steps committed since the last call into the recent-steps
    /// memory. Part of the post-commit projection refresh, so it runs under
    /// the writer fence on the writer's connection and reads only the events
    /// recorded since the previous refresh. A failure is logged and leaves
    /// memory off; `usage.cache.steps` then reads the store.
    fn advance_recent_steps(&self) {
        let now_ms = self.recent_steps_now_ms();
        let mut memory = self
            .0
            .recent_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let advanced = self.with_conn(|conn| {
            let late = astrocyte_core::cache_diagnostics::take_late_step_details(conn)
                .map_err(|error| describe_sqlite_error(&error))?;
            let advanced = memory
                .advance(conn, &late, now_ms)
                .map_err(|error| describe_sqlite_error(&error));
            if let Err(error) = advanced {
                tracing::warn!(target: "astrocyte", %error,
                    "recent cache steps not advanced; usage.cache.steps reads the store");
            }
            // After memory, so a subscriber told of a step finds it there.
            self.publish_step_feed(conn, &late);
            Ok(())
        });
        if let Err(error) = advanced {
            tracing::warn!(target: "astrocyte", %error,
                "late step details not read after a commit; cache memory and feed not advanced");
        }
    }

    /// Append the steps committed since the last call to the live step feed,
    /// then tell `usage.steps.subscribe` subscribers. Must run only after the
    /// commit that recorded the steps: a notice for a step that then rolled
    /// back would announce a row that never existed.
    fn publish_step_feed(&self, conn: &Connection, late: &[i64]) {
        if let Err(error) = self.0.step_feed.publish(conn, now_ms(), late) {
            tracing::warn!(target: "astrocyte", error = %describe_sqlite_error(&error),
                "live step feed not advanced");
        }
    }

    /// The live step feed's subscriptions.
    pub fn step_feed(&self) -> &crate::step_feed::FeedHub {
        &self.0.step_feed
    }

    /// Fill the recent-steps memory from the store: the sessions active in
    /// the last 24 hours, their newest steps each. Bounded by the memory's
    /// own limits, and run on the reader connection in one snapshot. Called
    /// once at startup, off the async executor. A failure is logged; the
    /// store then serves every request.
    pub(crate) fn rebuild_recent_steps(&self) {
        let now_ms = self.recent_steps_now_ms();
        let started = std::time::Instant::now();
        let mut memory = self
            .0
            .recent_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let rebuilt = self.with_read_conn_split(|conn| memory.rebuild(conn, now_ms));
        match rebuilt {
            Ok(()) => tracing::info!(target: "astrocyte",
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "recent cache steps loaded"),
            Err(failure) => {
                *memory = crate::recent_steps::RecentSteps::default();
                let error = match failure {
                    StoreFailure::Operation(error) => describe_sqlite_error(&error),
                    StoreFailure::Store(error) => error,
                };
                tracing::warn!(target: "astrocyte", %error,
                    "recent cache steps not loaded; usage.cache.steps reads the store");
            }
        }
    }

    /// `usage.cache.steps` from memory, or `None` when the store must answer.
    pub(crate) fn recent_steps_answer(
        &self,
        query: &astrocyte_core::cache_diagnostics::StepsQuery,
    ) -> Option<astrocyte_core::cache_diagnostics::StepsReply> {
        self.0
            .recent_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .answer(query)
    }

    #[cfg(test)]
    pub(crate) fn with_recent_steps_for_test<T>(
        &self,
        read: impl FnOnce(&crate::recent_steps::RecentSteps) -> T,
    ) -> T {
        read(&self.0.recent_steps.lock().unwrap())
    }

    /// Measure the recent-steps window from `now_ms` instead of the wall
    /// clock, so a test can use fixed step times.
    #[cfg(test)]
    pub(crate) fn set_recent_steps_now_for_test(&self, now_ms: i64) {
        *self.0.recent_steps_now.lock().unwrap() = Some(now_ms);
    }

    fn recent_steps_now_ms(&self) -> i64 {
        #[cfg(test)]
        if let Some(now_ms) = *self.0.recent_steps_now.lock().unwrap() {
            return now_ms;
        }
        now_ms()
    }
}

pub struct PublicProjection(SharedState);

#[derive(Debug)]
pub struct Health {
    pub status: SpendHealth,
    pub degraded_reason: Option<String>,
    pub capacity_runtime: Value,
    pub spend_last_attempt_ms: Option<i64>,
}

/// PublicProjection::health distinguishes healthy spend ingestion from
/// degraded configuration, projection or source freshness. Fatal failures stop
/// serving rather than adding a third health state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpendHealth {
    Ok,
    Degraded,
}

impl SpendHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl PartialEq<&str> for SpendHealth {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PublicProjection {
    pub(crate) fn report(
        &self,
        request: &astrocyte_core::report::ReportRequest,
        now_ms: i64,
    ) -> Result<Value, (String, String)> {
        // A refusal (an unknown grouping or grain) keeps its own code, as the
        // other served reports do; only a store failure is `internal`.
        self.0.with_read_op(|conn| {
            astrocyte_core::report::spend_report(conn, request, now_ms)
                .map(|value| json!(value))
                .map_err(crate::ops::map_report_error)
        })
    }
    pub fn health(&self, now_ms: i64) -> Health {
        // Both locks are read THROUGH poison. A poisoned lock means a writer
        // panicked; the projection is an immutable snapshot swapped whole, so
        // the value under a poisoned lock is still the last coherent one.
        // Reading it as `None` was fail-open: no projection meant "not cold",
        // no degraded value meant "not degraded", and the health of a daemon
        // whose writer had just panicked read `ok`.
        let projection = self
            .0
             .0
            .projection
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Ingest freshness is a claim about RECENCY: did a drain succeed within
        // the staleness horizon. It is not a claim about how much remains.
        // `pending_unknown` is the producer-count disclosure (broca supplies no
        // pending count, so it is set whenever the cursor trails high water) and
        // reading it here would mark a daemon degraded for the entire duration
        // of a backfill it is performing correctly, then flip it back every
        // time the producer writes a fact between two ticks. Use the last
        // successful drain time to distinguish a source that has never been
        // read from one that has become stale. File readers track neither a
        // producer identity nor event ordering, so freshness cannot require
        // those coordinates.
        let desired = self
            .0
             .0
            .desired
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let horizon_ms = i64::try_from(desired.config.poll_interval_secs)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000)
            .saturating_mul(4);
        let cold = projection.source_runtime.iter().any(|source| {
            source
                .last_success_ms
                .is_none_or(|last_success_ms| now_ms.saturating_sub(last_success_ms) > horizon_ms)
        });
        let degraded_reason = desired
            .degraded
            .clone()
            .or_else(|| {
                self.0
                     .0
                    .projection_degraded
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
            })
            .or_else(|| {
                self.0
                     .0
                    .producer_degraded
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .next()
                    .map(|(source, reason)| format!("producer {source} degraded: {reason}"))
            })
            .or_else(|| cold.then_some("ingest_freshness_unavailable".into()));
        Health {
            status: if degraded_reason.is_some() {
                SpendHealth::Degraded
            } else {
                SpendHealth::Ok
            },
            degraded_reason,
            capacity_runtime: projection
                .capacity_runtime
                .as_ref()
                .map(|freshness| {
                    let horizon_ms = freshness.horizon_ms();
                    json!({
                        "runtimeVariant": freshness.runtime_variant,
                        "changedAtMs": freshness.changed_at_ms,
                        "enabledAtMs": freshness.enabled_at_ms,
                        "lastAttemptMs": freshness.last_attempt_ms,
                        "lastSuccessMs": freshness.last_success_ms,
                        "degradedEntries": freshness.degraded_entries,
                        "detail": freshness.detail,
                        "effectiveHorizonMs": horizon_ms,
                        "stale": astrocyte_core::capacity::is_stale(freshness, now_ms, horizon_ms),
                    })
                })
                .unwrap_or_else(|| json!({})),
            spend_last_attempt_ms: projection
                .source_runtime
                .iter()
                .filter_map(|source| source.last_success_ms)
                .max(),
        }
    }
}

/// System-clock milliseconds, saturating at the store's range and retaining
/// epoch zero for a clock before the Unix epoch.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn hydrate_config(conn: &Connection) -> Result<(AstrocyteConfig, u64), String> {
    #[allow(clippy::type_complexity)]
    let (generation, mut config, multiplier_column, ingest_column, canonical_bytes): (
        i64,
        AstrocyteConfig,
        Option<String>,
        Option<String>,
        Vec<u8>,
    ) = conn
        .query_row(
            "SELECT generation,poll_interval_secs,qta_connection_file,
                    fusiform_connection_file,subscription_multiplier,harness_ingest,canonical_bytes
             FROM desired_config WHERE id=1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    AstrocyteConfig {
                        poll_interval_secs: row.get(1)?,
                        qta_connection_file: row.get(2)?,
                        fusiform_connection_file: row.get(3)?,
                        reasoning_billed_as_output: Vec::new(),
                        subscription_multiplier: config::MultiplierSetting::NotRecorded,
                        harness_ingest: config::HarnessIngest::all_on(),
                    },
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .map_err(|error| error.to_string())?;
    if generation == 0 {
        return Ok((config::intentional_empty().config, 0));
    }
    config.reasoning_billed_as_output =
        config::decode_reasoning_generation(&canonical_bytes, multiplier_column.as_deref())
            .map_err(|error| error.to_string())?;
    // NULL here is a generation committed before the store had the column.
    // It boots as "not recorded", never as an empty declaration, and the
    // next reload from the desired file publishes a generation that has it.
    config.subscription_multiplier = config::decode_multiplier_column(multiplier_column.as_deref())
        .map_err(|error| error.to_string())?;
    if config.subscription_multiplier == config::MultiplierSetting::NotRecorded {
        tracing::info!(
            target: "multiplier",
            generation,
            "committed desired generation predates the subscription_multiplier member; \
             the daily multiplier is skipped until a reload publishes one that records it"
        );
    }
    // NULL here is a generation committed before the store had this column
    // either. That generation drained every producer, so it hydrates as all
    // on and ingests exactly as before; the next reload records the member.
    config.harness_ingest = config::decode_harness_ingest_column(ingest_column.as_deref())
        .map_err(|error| error.to_string())?;
    let validated = config::canonicalize(config).map_err(|error| error.to_string())?;
    Ok((
        validated.config,
        u64::try_from(generation).map_err(|_| "negative desired generation".to_string())?,
    ))
}

/// Where the operator edits this module's desired configuration.
///
/// The config-home ladder is the fleet's, not a copy: `resolve_config_home`
/// in cortexkit-store-types is pinned to the daemon's golden rows, so the
/// rungs cannot drift from what `ck` itself resolves. What is ours is the
/// last rung. The shared ladder ends in a RELATIVE `.config` when no home
/// variable is set; here that arm is refused, never joined -- under a
/// supervisor that exports no HOME it resolves against the cwd, reads as
/// Missing, and Missing is a legitimate operator state: `reload_config` would
/// publish intentional-empty as a NEW generation and durably demote the real
/// config to "no producers" while every status field read healthy. An
/// unresolvable path is the Unreadable arm: retain the committed generation,
/// degrade.
pub(crate) fn user_config_path() -> Result<std::path::PathBuf, String> {
    config_path_under_home(std::path::PathBuf::from(
        cortexkit_store_types::resolve_config_home(),
    ))
}

/// Join this module's file under a resolved config home, refusing a home the
/// ladder could only produce by falling through every variable. Split from
/// `user_config_path` so the refusal is testable without touching process
/// environment; the ladder itself is tested where it lives.
pub(crate) fn config_path_under_home(
    home: std::path::PathBuf,
) -> Result<std::path::PathBuf, String> {
    if !home.is_absolute() {
        return Err(format!(
            "cannot resolve the configuration path: the config home resolved to the relative \
             {home:?}, which means neither XDG_CONFIG_HOME nor HOME is set (a relative \
             path would read the wrong file as missing)"
        ));
    }
    Ok(home.join("cortexkit/astrocyte.jsonc"))
}

#[cfg(any(test, feature = "test-support"))]
pub fn test_state() -> SharedState {
    let conn = Connection::open_in_memory().unwrap();
    astrocyte_core::store::initialize_connection(&conn).unwrap();
    for migration in MIGRATIONS {
        conn.execute_batch(migration.statements).unwrap();
    }
    let state = SharedState::from_parts(
        StateStore::Direct(Mutex::new(conn)),
        config::intentional_empty().config,
        0,
    );
    state.refresh_projection().unwrap();
    state
}

/// A state over a real leased, migrated store in `directory`: the arm
/// production runs, with its separate reader connection. Tests that need two
/// connections to one database use this, since `test_state`'s in-memory
/// database cannot be opened twice.
#[cfg(any(test, feature = "test-support"))]
pub fn leased_test_state(directory: &tempfile::TempDir) -> SharedState {
    use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
    let descriptor = StorageDescriptor {
        module_id: "ck-astrocyte-leased-test".to_string(),
        storage_namespace: "astrocyte".to_string(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: directory
                .path()
                .join("ledger.db")
                .to_string_lossy()
                .into_owned(),
        },
    };
    let store = open_sqlite(&descriptor).unwrap();
    astrocyte_core::store::migrate_store(&store).unwrap();
    let state = SharedState::from_store(store).unwrap();
    assert!(matches!(state.0.store, StateStore::Leased { .. }));
    state
}

#[cfg(test)]
mod report_reader_tests {
    use super::*;

    #[test]
    fn leased_report_reader_has_effective_mapping_and_cache_without_tuning_writer() {
        let directory = tempfile::tempdir().unwrap();
        let state = leased_test_state(&directory);
        let StateStore::Leased { store, reader } = &state.0.store else {
            panic!("leased fixture must use the production reader");
        };
        let reader = reader.lock().unwrap();
        let mmap: i64 = reader
            .pragma_query_value(None, "mmap_size", |row| row.get(0))
            .unwrap();
        let cache: i64 = reader
            .pragma_query_value(None, "cache_size", |row| row.get(0))
            .unwrap();
        assert_eq!(mmap, astrocyte_core::report::REPORT_MMAP_BYTES);
        assert_eq!(cache, astrocyte_core::report::REPORT_CACHE_KIB);
        assert!(reader
            .execute("CREATE TABLE must_be_read_only (id INTEGER)", [])
            .is_err());
        store
            .with_conn(|writer| {
                let writer_cache: i64 =
                    writer.pragma_query_value(None, "cache_size", |row| row.get(0))?;
                assert_ne!(writer_cache, cache);
                let writer_mmap: i64 =
                    writer.pragma_query_value(None, "mmap_size", |row| row.get(0))?;
                assert_eq!(writer_mmap, 0);
                Ok(())
            })
            .unwrap();
    }
}

#[cfg(test)]
mod config_path_tests {
    use super::*;

    /// The ladder's rungs are pinned by cortexkit-store-types' golden rows and
    /// are not re-tested here. What this module owns is the last rung: the
    /// shared ladder ends in a relative `.config` when no home variable is
    /// set, and here that must refuse, never join. This used to be a fallback
    /// that read as Missing, and Missing publishes intentional-empty as a new
    /// generation.
    #[test]
    fn relative_config_home_refuses_rather_than_joining() {
        let error = config_path_under_home(std::path::PathBuf::from(".config")).unwrap_err();
        assert!(
            error.contains("XDG_CONFIG_HOME") && error.contains("HOME"),
            "the refusal must name both variables: {error}"
        );
    }

    #[test]
    fn absolute_config_home_joins_this_modules_file() {
        // `/h` is not absolute on Windows, which needs a drive or UNC root, so
        // build the home under the temp dir, which is absolute on every OS.
        let home = std::env::temp_dir().join("h").join(".config");
        let path = config_path_under_home(home.clone()).unwrap();
        assert_eq!(path, home.join("cortexkit/astrocyte.jsonc"));
        assert!(path.is_absolute());
    }
}

#[cfg(test)]
mod health_tests {
    use super::*;

    #[test]
    fn poisoned_desired_lock_can_publish_a_complete_generation() {
        let state = test_state();
        state.mark_config_degraded("invalid startup config".into());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = state.0.desired.write().unwrap();
            panic!("injected publication panic");
        }));
        let mut config = config::intentional_empty().config;
        config.poll_interval_secs = 3;
        state.publish_desired(config, 7);

        assert_eq!(state.desired_config().poll_interval_secs, 3);
        assert_eq!(state.desired_generation(), 7);
        assert_ne!(
            state
                .public_projection()
                .health(0)
                .degraded_reason
                .as_deref(),
            Some("invalid startup config")
        );
    }

    /// Drives a sequence-bearing `source_runtime` row through the states the
    /// health arm must tell apart. The fixture writes the complete sequence axis
    /// because the CHECK rejects partial lineage and ordinal coordinates.
    fn drain_broca(state: &SharedState, last_success_ms: i64, consumed: i64, high_water: i64) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "UPDATE source_runtime SET lineage='inst:1',
                     consumed_ordinal=?1, high_water_ordinal=?2, last_success_ms=?3,
                     pending_known=0, pending_unknown=?4 WHERE source_key='broca'",
                    rusqlite::params![
                        consumed,
                        high_water,
                        last_success_ms,
                        (consumed != high_water) as i64
                    ],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn never_drained_source_is_degraded() {
        let state = test_state();
        let health = state.public_projection().health(10_000);
        assert_eq!(health.status, "degraded");
        assert_eq!(
            health.degraded_reason.as_deref(),
            Some("ingest_freshness_unavailable")
        );
    }

    /// A writer that panicked while holding the projection lock leaves it
    /// poisoned. Reading the poison as "no projection" made `cold` false and
    /// health `ok` -- the daemon read healthiest at the exact moment its
    /// writer had died. The value under the poison is the last coherent
    /// snapshot, so health reads through it and still reports the cold source.
    #[test]
    fn poisoned_projection_lock_does_not_read_as_healthy() {
        let state = test_state();
        let lock = std::sync::Arc::clone(&state.0);
        let _ = std::thread::spawn(move || {
            let _guard = lock.projection.write().unwrap();
            panic!("writer dies holding the lock");
        })
        .join();
        assert!(
            state.0.projection.is_poisoned(),
            "fixture must poison the lock"
        );
        let health = state.public_projection().health(10_000);
        assert_eq!(health.status, "degraded");
        assert_eq!(
            health.degraded_reason.as_deref(),
            Some("ingest_freshness_unavailable")
        );
    }

    /// The production defect: a daemon mid-backfill has a recent success and a
    /// cursor far below high water. That is a healthy daemon doing its job, and
    /// the count it cannot state belongs on the report, not on health.
    #[test]
    fn recent_success_trailing_high_water_is_ok_not_degraded() {
        let state = test_state();
        drain_broca(&state, 9_000, 4_352, 112_645);
        let health = state.public_projection().health(10_000);
        assert_eq!(health.status, "ok", "{:?}", health.degraded_reason);
        assert_eq!(health.degraded_reason, None);
    }

    #[test]
    fn recent_no_sequence_success_is_ok_not_degraded() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                tx.execute(
                    "UPDATE source_runtime SET lineage=NULL,consumed_ordinal=NULL,
                     high_water_ordinal=NULL,last_success_ms=9000,
                     pending_known=0,pending_unknown=0 WHERE source_key='broca'",
                    [],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();

        let health = state.public_projection().health(10_000);
        assert_eq!(health.status, "ok", "{:?}", health.degraded_reason);
        assert_eq!(health.degraded_reason, None);
    }

    /// The horizon is four poll intervals (default 60s → 240s). A success one
    /// millisecond past it is stale; one millisecond inside it is not.
    #[test]
    fn success_older_than_the_horizon_is_degraded_and_inside_is_not() {
        let state = test_state();
        let horizon_ms = 60_000 * 4;
        drain_broca(&state, 0, 5, 5);
        assert_eq!(state.public_projection().health(horizon_ms).status, "ok");
        assert_eq!(
            state.public_projection().health(horizon_ms + 1).status,
            "degraded"
        );
    }
}

#[cfg(test)]
mod ordered_write_error_tests {
    use super::*;

    #[test]
    fn committed_write_survives_projection_refresh_failure() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_state(&directory);
        create_fk_tables(&state);
        let poisoned = state.clone();
        let _ = std::thread::spawn(move || {
            let _projection = poisoned.0.projection.write().unwrap();
            panic!("injected projection poison");
        })
        .join();

        let result = state.ordered_op_write(|tx| {
            tx.execute("INSERT INTO fk_parent VALUES (1)", [])
                .map_err(|error| ("internal".into(), error.to_string()))?;
            Ok((json!({"committed": true}), true))
        });
        let rows: i64 = state
            .with_read_op(|conn| {
                Ok(conn
                    .query_row("SELECT COUNT(*) FROM fk_parent", [], |row| row.get(0))
                    .unwrap())
            })
            .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(result.unwrap(), json!({"committed": true}));
        let health = state.public_projection().health(0);
        assert_eq!(health.status, "degraded");
        assert!(health.degraded_reason.unwrap().contains("projection"));
    }

    fn leased_state(directory: &tempfile::TempDir) -> SharedState {
        leased_test_state(directory)
    }

    /// Scratch parent/child tables private to these tests, so the failure is
    /// forced without depending on the ledger schema.
    fn create_fk_tables(state: &SharedState) {
        state
            .with_conn(|conn| {
                conn.execute_batch(
                    "PRAGMA foreign_keys=ON;
                     CREATE TABLE fk_parent(id INTEGER PRIMARY KEY);
                     CREATE TABLE fk_child(parent_id INTEGER REFERENCES fk_parent(id));",
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
    }

    /// Inserts an orphan child with foreign-key checks deferred, so the
    /// operation itself succeeds and SQLite refuses the COMMIT.
    fn write_orphan_child(state: &SharedState) -> Result<(), String> {
        state.ordered_write(|tx| {
            tx.execute_batch(
                "PRAGMA defer_foreign_keys=ON;
                 INSERT INTO fk_child(parent_id) VALUES (42);",
            )
            .map_err(|error| error.to_string())?;
            Ok(((), false))
        })
    }

    fn child_rows(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM fk_child", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn assert_names_the_commit_failure(error: &str) {
        assert!(
            error.contains("FOREIGN KEY constraint failed"),
            "the error must carry SQLite's own message: {error}"
        );
        // 787 is SQLITE_CONSTRAINT_FOREIGNKEY, the extended result code.
        assert!(
            error.contains("sqlite code 787"),
            "the error must carry SQLite's result code: {error}"
        );
        assert!(
            !error.contains("Query is not read-only"),
            "the error must not be replaced by a stand-in: {error}"
        );
    }

    /// Production shape: a COMMIT that SQLite refuses must be reported as what
    /// SQLite said, not as rusqlite's unrelated `InvalidQuery` text. The write
    /// must also have rolled back and left the connection usable.
    #[test]
    fn leased_commit_failure_reports_the_sqlite_error() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_state(&directory);
        create_fk_tables(&state);

        let error = write_orphan_child(&state).unwrap_err();

        assert_names_the_commit_failure(&error);
        assert_eq!(child_rows(&state), 0, "the refused write must roll back");
        state
            .ordered_write(|tx| {
                tx.execute("INSERT INTO fk_parent(id) VALUES (1)", [])
                    .map_err(|error| error.to_string())?;
                Ok(((), false))
            })
            .expect("the next write must still commit");
    }

    /// The operation's own error is still returned exactly as it produced it.
    #[test]
    fn leased_operation_error_is_returned_verbatim() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_state(&directory);
        let error = state
            .ordered_write(|_| Err::<((), bool), _>("caller refused".to_string()))
            .unwrap_err();
        assert_eq!(error, "caller refused");
    }

    /// A served operation's refusal keeps its own code: closing a plan that
    /// was never opened is `no_open_subscription`, which clients branch on,
    /// and must not be flattened to `internal`.
    #[test]
    fn leased_served_refusal_keeps_its_code() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_state(&directory);

        let (code, message) = crate::ops::dispatch(
            &state,
            crate::ops::OP_SUBSCRIPTION_CLOSE,
            json!({"account_ref":"acct","provider":"provider","effective_to":10}),
        )
        .unwrap_err();

        assert_eq!(code, "no_open_subscription", "message: {message}");
    }

    /// A served operation whose own work succeeds but whose COMMIT SQLite
    /// refuses is reported as `internal`, carrying SQLite's message. The
    /// refusal is forced by a temporary trigger that adds an orphan row under a
    /// deferred foreign key whenever the operation inserts a plan.
    #[test]
    fn leased_served_commit_failure_is_internal_with_the_sqlite_error() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_state(&directory);
        state
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TABLE fk_parent(id INTEGER PRIMARY KEY);
                     CREATE TABLE fk_child(parent_id INTEGER
                       REFERENCES fk_parent(id) DEFERRABLE INITIALLY DEFERRED);
                     CREATE TEMP TRIGGER orphan_on_plan AFTER INSERT ON main.account_subscription
                     BEGIN INSERT INTO fk_child(parent_id) VALUES (42); END;",
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();

        let (code, message) = crate::ops::dispatch(
            &state,
            crate::ops::OP_SUBSCRIPTION_SET,
            json!({"account_ref":"acct","provider":"provider","plan_name":"plan",
                   "recurring_cost_minor":100,"currency":"USD","amount_exponent":2,
                   "period":"month","effective_from":1}),
        )
        .unwrap_err();

        assert_eq!(code, "internal", "message: {message}");
        assert_names_the_commit_failure(&message);
        assert_eq!(child_rows(&state), 0, "the refused write must roll back");
    }

    #[test]
    fn direct_commit_failure_reports_the_sqlite_error() {
        let state = test_state();
        create_fk_tables(&state);

        let error = write_orphan_child(&state).unwrap_err();

        assert_names_the_commit_failure(&error);
        assert_eq!(child_rows(&state), 0, "the refused write must roll back");
    }
}

#[cfg(test)]
mod read_connection_tests {
    use super::*;

    fn ingest_event(state: &SharedState, producer_ref: &str) {
        crate::ops::dispatch(
            state,
            crate::ops::OP_EVENT_INGEST,
            json!({
                "producer":"producer", "producer_lineage":"lineage", "producer_ref":producer_ref,
                "charge_basis":"unknown", "occurred_at_ms":10,
                "measurements":[{"role":"input","unit":"token","kind":"raw","source":"usage",
                                 "quantity_units":1,"quantity_exponent":0}]
            }),
        )
        .unwrap();
    }

    fn event_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap()
    }

    /// One `with_read_op` call sees one snapshot: an event committed by the
    /// writer between two of its reads is invisible to the second read, and
    /// visible to the next call. Without the read transaction each SELECT
    /// would see the latest commit and the two counts would differ.
    #[test]
    fn one_read_call_sees_one_snapshot_across_a_writer_commit() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);
        ingest_event(&state, "before");

        let (first, second) = state
            .with_read_op(|conn| {
                let first = event_count(conn);
                // The writer path takes its own fence and connection, neither
                // of which this read holds, so it commits from inside the read.
                ingest_event(&state, "during");
                let second = event_count(conn);
                Ok((first, second))
            })
            .unwrap();
        assert_eq!(first, 1);
        assert_eq!(
            second, first,
            "a read call must not see a commit made during it"
        );

        let after = state.with_read_op(|conn| Ok(event_count(conn))).unwrap();
        assert_eq!(after, 2, "the next read call must see the committed event");
    }

    #[test]
    fn regression_long_verdict_allows_ingest_and_keeps_one_snapshot() {
        use std::sync::mpsc;
        use std::time::Duration;

        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);
        ingest_event(&state, "before-verdict");
        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        state.pause_next_read_for_test(move |conn| {
            // Take the snapshot before pausing, as a long verdict's first SELECT does.
            assert_eq!(event_count(conn), 1);
            held_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(30));
        });
        let verdict_state = state.clone();
        let verdict = std::thread::spawn(move || {
            crate::ops::dispatch(&verdict_state, crate::ops::OP_VERDICT, json!({}))
        });
        let started = held_rx.recv_timeout(Duration::from_secs(10));
        if started.is_err() {
            let _ = release_tx.send(());
            crate::test_support::join_within(verdict, "the verdict thread").unwrap();
            panic!("budget.verdict never started its snapshot read");
        }

        let (done_tx, done_rx) = mpsc::channel();
        let writer_state = state.clone();
        let writer = std::thread::spawn(move || {
            ingest_event(&writer_state, "during-verdict");
            let _ = done_tx.send(());
        });
        let committed = done_rx.recv_timeout(Duration::from_secs(10));
        let _ = release_tx.send(());
        let verdict = crate::test_support::join_within(verdict, "the verdict thread").unwrap();
        crate::test_support::join_within(writer, "the ingest thread");
        committed.expect("ingest waited behind the open verdict read");
        assert_eq!(verdict["uncertainty"]["events_total"], 1);
        assert_eq!(verdict["decision"], "allow");
        assert_eq!(verdict["uncertainty_scope"], "machine");
        let next = crate::ops::dispatch(&state, crate::ops::OP_VERDICT, json!({})).unwrap();
        assert_eq!(next["uncertainty"]["events_total"], 2);
        assert_eq!(state.with_conn(|conn| Ok(event_count(conn))).unwrap(), 2);
    }

    /// A served read whose operation refuses returns the operation's own code
    /// and message, not `internal`.
    #[test]
    fn served_read_refusal_keeps_its_code() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);

        let refusal = crate::ops::dispatch(
            &state,
            crate::ops::OP_CHARGE_EXPLAIN,
            json!({"event_id":"no-such-event"}),
        )
        .unwrap_err();

        assert_eq!(
            refusal,
            (
                "not_found".to_string(),
                "event 'no-such-event' was not found".to_string()
            )
        );
    }

    /// A failure the operation did not produce is reported as `internal`
    /// carrying SQLite's own message. The failure is SQLite's real refusal of
    /// the reader's BEGIN: the test leaves a transaction open on the reader
    /// connection, so the next BEGIN is refused with "cannot start a
    /// transaction within a transaction". No hook fakes the error.
    #[test]
    fn served_read_store_failure_is_internal_with_the_sqlite_error() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);
        let StateStore::Leased { reader, .. } = &state.0.store else {
            unreachable!("leased_test_state builds a leased store")
        };
        reader.lock().unwrap().execute_batch("BEGIN").unwrap();

        let (code, message) =
            crate::ops::dispatch(&state, crate::ops::OP_LEDGER_COVERAGE, json!({})).unwrap_err();

        assert_eq!(code, "internal", "message: {message}");
        assert!(
            message.starts_with("store read: ")
                && message.contains("cannot start a transaction within a transaction"),
            "the message must carry SQLite's own error: {message}"
        );
        // 1 is SQLITE_ERROR, the result code SQLite gives a nested BEGIN.
        assert!(
            message.contains("sqlite code 1,"),
            "the message must carry SQLite's result code: {message}"
        );
    }

    /// The reader is opened read-only: SQLite refuses a write on it with its
    /// read-only error, and nothing is written.
    #[test]
    fn the_reader_refuses_writes() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = leased_test_state(&directory);

        let errors = state
            .with_read_op(|conn| {
                Ok([
                    conn.execute("UPDATE source_runtime SET last_success_ms=1", [])
                        .unwrap_err(),
                    conn.execute("CREATE TABLE reader_scratch(id INTEGER)", [])
                        .unwrap_err(),
                ])
            })
            .unwrap();
        for error in errors {
            assert_eq!(
                error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::ReadOnly),
                "{error}"
            );
        }
        let touched: i64 = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM source_runtime WHERE last_success_ms=1",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(touched, 0);
    }
}
