//! Astrocyte v2 storage schema and connection prerequisites.
//!
//! The v2 migration chain intentionally starts at version 1. The replaced store
//! contains disposable data, so no v1 schema or data migration is retained.

mod migrations;

use std::time::Duration;

use cortexkit_store::{Migration, MigrationOutcome, SqliteStore, StoreError};
use rusqlite::Connection;

const STORE_BUSY_TIMEOUT_MS: u64 = 5_000;
/// The size the write-ahead log is cut back to after a checkpoint.
const STORE_WAL_SIZE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum StoreInitError {
    Sql(rusqlite::Error),
    ForeignKeysDisabled,
}

impl From<rusqlite::Error> for StoreInitError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

/// Enables and reads back foreign-key enforcement, sets the write timeout, and
/// probes JSON functions, partial indexes, foreign keys and triggers. This is
/// not a complete preflight of every SQL feature used by the migrations.
///
/// `synchronous` is deliberately not set, so SQLite's default FULL applies on
/// top of the WAL journal the store opens with: every commit is fsynced before
/// it returns. Do not lower it to NORMAL. Under NORMAL a power loss can drop
/// transactions that had already committed, including an operator's write
/// that the operation had already reported back as done. What FULL costs is
/// one fsync per commit, and the answer to that cost is fewer commits: bulk
/// writers such as the refresher's pricing pass commit their work in groups
/// rather than one row or one event per transaction.
///
/// `temp_store` is MEMORY. A statement that can fail partway inside a larger
/// transaction (an insert whose triggers write other tables, or a savepoint
/// per priced event) keeps a statement journal, and SQLite otherwise writes
/// that journal to a file in its temporary directory (`SQLITE_TMPDIR`,
/// `TMPDIR` or `/var/tmp`), not beside the store. Measured on a production
/// copy, one catch-up tick wrote 659 MB through those files against 168 MB to
/// the store and WAL, and MEMORY raised peak memory by 4.4 MiB. A statement
/// journal holds only the pages one statement or savepoint changes, so its
/// size follows a single event's writes, not the transaction's.
///
/// `journal_size_limit` cuts the write-ahead log back to 64 MiB after a
/// checkpoint. SQLite otherwise keeps the log at the largest size it ever
/// reached and only reuses it from the start, so one catch-up tick or summary
/// rebuild could hold hundreds of megabytes of disk until the store closes
/// (measured: 823 MB after a full summary rebuild). Steady ticks write far
/// less than the limit, so the cut costs nothing there.
pub fn initialize_connection(conn: &Connection) -> Result<(), StoreInitError> {
    conn.busy_timeout(Duration::from_millis(STORE_BUSY_TIMEOUT_MS))?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "journal_size_limit", STORE_WAL_SIZE_LIMIT_BYTES)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    let foreign_keys: i64 = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    if foreign_keys != 1 {
        return Err(StoreInitError::ForeignKeysDisabled);
    }
    probe_sqlite_capabilities(conn)?;
    Ok(())
}

fn probe_sqlite_capabilities(conn: &Connection) -> rusqlite::Result<()> {
    let capabilities: (i64, String, i64) = conn.query_row(
        "SELECT json_valid('{\"k\":1}'), json_type('{\"k\":1}'), json_extract('{\"k\":1}','$.k')",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if capabilities != (1, "object".to_string(), 1) {
        return Err(rusqlite::Error::InvalidQuery);
    }

    // Rollback keeps the probe out of durable schema state while exercising the
    // foreign-key, partial-index, and trigger features used below.
    conn.execute_batch("SAVEPOINT astrocyte_sqlite_capability_probe;")?;
    let probe = conn.execute_batch(
        "CREATE TABLE astrocyte_capability_parent (id INTEGER PRIMARY KEY);
         CREATE TABLE astrocyte_capability_child (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES astrocyte_capability_parent(id)
                 DEFERRABLE INITIALLY DEFERRED
         );
         CREATE UNIQUE INDEX astrocyte_capability_partial
             ON astrocyte_capability_child(parent_id) WHERE parent_id IS NOT NULL;
         CREATE TRIGGER astrocyte_capability_raise
             BEFORE UPDATE ON astrocyte_capability_child
              BEGIN SELECT RAISE(ABORT,'astrocyte_capability_raise'); END;",
    );
    let rollback = conn.execute_batch("ROLLBACK TO astrocyte_sqlite_capability_probe;");
    let release = conn.execute_batch("RELEASE astrocyte_sqlite_capability_probe;");
    // Attempt cleanup even when a feature fails, but keep that original error.
    probe?;
    rollback?;
    release?;
    Ok(())
}

/// Migrates the leased writer before it is shared with readers or writers.
/// SQLite requires foreign keys off outside the rebuild transaction: otherwise
/// dropping the old self-referencing charge table deletes every text index row
/// individually. Migrations 23 and 33 check rebuilt foreign keys before commit.
pub fn migrate_store(store: &SqliteStore) -> Result<MigrationOutcome, StoreError> {
    store.with_conn(|conn| set_foreign_keys(conn, true))?;
    let mut applied = 0;
    for version in [23, 33] {
        let before = MIGRATIONS.partition_point(|migration| migration.version < version);
        let earlier = store.migrate("astrocyte", &MIGRATIONS[..before])?;
        applied += earlier.applied;
        if earlier.recorded >= version {
            continue;
        }
        let mut restore = ForeignKeyRestore {
            store,
            active: true,
        };
        store.with_conn(|conn| set_foreign_keys(conn, false))?;
        let rebuilt = store.migrate("astrocyte", &MIGRATIONS[..=before]);
        // Restore enforcement even when the transaction failed. Drop also attempts
        // restoration during unwinding, before startup can hand the store to callers.
        restore.restore()?;
        applied += rebuilt?.applied;
    }
    let before = MIGRATIONS.partition_point(|migration| migration.version < SESSION_REF_VERSION);
    let earlier = store.migrate("astrocyte", &MIGRATIONS[..before])?;
    applied += earlier.applied;
    if earlier.recorded < SESSION_REF_VERSION {
        store
            .with_conn(|conn| Ok(migrate_session_refs(conn, &MIGRATIONS[before])))?
            .map_err(|error| {
                StoreError::Migration(format!(
                    "namespace 'astrocyte' migration {SESSION_REF_VERSION}: {error}"
                ))
            })?;
        applied += 1;
    }
    let mut outcome = store.migrate("astrocyte", MIGRATIONS)?;
    outcome.applied += applied;
    Ok(outcome)
}

/// The migration that adds `attribution_session_ref`. Its rows are SHA-256
/// references SQLite can't compute, so the store runs the migration itself:
/// the table, the backfill from every stored session fact and the version
/// record commit together, exactly as a SQL-only migration would.
const SESSION_REF_VERSION: u32 = 59;
fn migrate_session_refs(conn: &Connection, migration: &Migration) -> Result<(), String> {
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    tx.execute_batch(migration.statements)
        .map_err(|e| e.to_string())?;
    crate::attribution::backfill_session_refs(&tx)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    tx.execute(
        "INSERT INTO cortexkit_schema_version(namespace,version,applied_at_unix) VALUES(?1,?2,?3)",
        rusqlite::params![
            "astrocyte",
            migration.version,
            i64::try_from(now).unwrap_or(i64::MAX)
        ],
    )
    .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())
}

fn set_foreign_keys(conn: &Connection, enabled: bool) -> rusqlite::Result<()> {
    conn.pragma_update(None, "foreign_keys", enabled)?;
    let actual: i64 = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    if actual != i64::from(enabled) {
        // rusqlite's InvalidQuery displays as "Query is not read-only", which
        // would misreport this, so carry the real reason as text.
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some(format!(
                "PRAGMA foreign_keys={enabled} did not take effect: read back {actual}"
            )),
        ));
    }
    Ok(())
}

struct ForeignKeyRestore<'a> {
    store: &'a SqliteStore,
    active: bool,
}

impl ForeignKeyRestore<'_> {
    fn restore(&mut self) -> Result<(), StoreError> {
        self.store.with_conn(|conn| {
            set_foreign_keys(conn, true)?;
            conn.pragma_update(None, "cache_size", -2000)
        })?;
        self.active = false;
        Ok(())
    }
}

impl Drop for ForeignKeyRestore<'_> {
    fn drop(&mut self) {
        if self.active {
            if let Err(error) = self.restore() {
                tracing::error!(%error, "failed to restore foreign keys after migration");
            }
        }
    }
}

/// The fresh Astrocyte v2 schema. Historical Astrocyte schemas are deliberately
/// absent because the pre-release store is disposable.
// Put new migration SQL in the newest range file, or a new range file when it nears the limit.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        statements: migrations::v01_v20::V1,
    },
    Migration {
        version: 2,
        statements: migrations::v01_v20::V2,
    },
    Migration {
        version: 3,
        statements: migrations::v01_v20::V3,
    },
    Migration {
        version: 4,
        statements: migrations::v01_v20::V4,
    },
    Migration {
        version: 5,
        statements: migrations::v01_v20::V5,
    },
    Migration {
        version: 6,
        statements: migrations::v01_v20::V6,
    },
    Migration {
        version: 7,
        statements: migrations::v01_v20::V7,
    },
    Migration {
        version: 8,
        statements: migrations::v01_v20::V8,
    },
    Migration {
        version: 9,
        statements: migrations::v01_v20::V9,
    },
    Migration {
        version: 10,
        statements: migrations::v01_v20::V10,
    },
    Migration {
        version: 11,
        statements: migrations::v01_v20::V11,
    },
    Migration {
        version: 12,
        statements: migrations::v01_v20::V12,
    },
    // READING THESE COLUMNS ON OLDER ROWS: the SQL comment below says a NULL
    // `charge_basis_overrides` means the producer did not say. That holds only
    // for rows written after this migration. On a row that already existed,
    // ADD COLUMN wrote NULL whether or not BROCA had sent the field, so NULL
    // means astrocyte did not record it. Nothing fills those NULLs later: the
    // update guard makes both columns write-once, and a re-sent fact is only
    // spared a false contradiction (`EventPayload::restated_by` in ingest.rs).
    // A BROCA row tells the two cases apart itself: BROCA has stamped
    // `charge_basis_mapping_version` on every fact since v0.3.0, so on a
    // producer='broca' row a NULL version means "stored before this migration,
    // provenance not recorded", and a set version with NULL overrides means a
    // fact BROCA wrote before it emitted overrides (v0.3.117). The SQL text
    // stays as applied; a migration already run in production is never edited.
    Migration {
        version: 13,
        statements: migrations::v01_v20::V13,
    },
    Migration {
        version: 14,
        statements: migrations::v01_v20::V14,
    },
    Migration {
        version: 15,
        statements: migrations::v01_v20::V15,
    },
    Migration {
        version: 16,
        statements: migrations::v01_v20::V16,
    },
    Migration {
        version: 17,
        statements: migrations::v01_v20::V17,
    },
    Migration {
        version: 18,
        statements: migrations::v01_v20::V18,
    },
    Migration {
        version: 19,
        statements: migrations::v01_v20::V19,
    },
    Migration {
        version: 20,
        statements: migrations::v01_v20::V20,
    },
    Migration {
        version: 21,
        statements: migrations::v21_v40::V21,
    },
    Migration {
        version: 22,
        statements: migrations::v21_v40::V22,
    },
    Migration {
        version: 23,
        statements: migrations::v21_v40::V23,
    },
    Migration {
        version: 24,
        statements: migrations::v21_v40::V24,
    },
    Migration {
        version: 25,
        statements: migrations::v21_v40::V25,
    },
    Migration {
        version: 26,
        statements: migrations::v21_v40::V26,
    },
    Migration {
        version: 27,
        statements: migrations::v21_v40::V27,
    },
    Migration {
        version: 28,
        statements: migrations::v21_v40::V28,
    },
    Migration {
        version: 29,
        statements: migrations::v21_v40::V29,
    },
    Migration {
        version: 30,
        statements: migrations::v21_v40::V30,
    },
    Migration {
        version: 31,
        statements: migrations::v21_v40::V31,
    },
    Migration {
        version: 32,
        statements: migrations::v21_v40::V32,
    },
    Migration {
        version: 33,
        statements: migrations::v21_v40::V33,
    },
    Migration {
        version: 34,
        statements: migrations::v21_v40::V34,
    },
    Migration {
        version: 35,
        statements: migrations::v21_v40::V35,
    },
    Migration {
        version: 36,
        statements: migrations::v21_v40::V36,
    },
    Migration {
        version: 37,
        statements: migrations::v21_v40::V37,
    },
    Migration {
        version: 38,
        statements: migrations::v21_v40::V38,
    },
    Migration {
        version: 39,
        statements: migrations::v21_v40::V39,
    },
    Migration {
        version: 40,
        statements: migrations::v21_v40::V40,
    },
    Migration {
        version: 41,
        statements: migrations::v41_v58::V41,
    },
    Migration {
        version: 42,
        statements: migrations::v41_v58::V42,
    },
    Migration {
        version: 43,
        statements: migrations::v41_v58::V43,
    },
    Migration {
        version: 44,
        statements: migrations::v41_v58::V44,
    },
    Migration {
        version: 45,
        statements: migrations::v41_v58::V45,
    },
    Migration {
        version: 46,
        statements: migrations::v41_v58::V46,
    },
    Migration {
        version: 47,
        statements: migrations::v41_v58::V47,
    },
    Migration {
        version: 48,
        statements: migrations::v41_v58::V48,
    },
    Migration {
        version: 49,
        statements: migrations::v41_v58::V49,
    },
    Migration {
        version: 50,
        statements: migrations::v41_v58::V50,
    },
    Migration {
        version: 51,
        statements: migrations::v41_v58::V51,
    },
    Migration {
        version: 52,
        statements: migrations::v41_v58::V52,
    },
    Migration {
        version: 53,
        statements: migrations::v41_v58::V53,
    },
    Migration {
        version: 54,
        statements: migrations::v41_v58::V54,
    },
    Migration {
        version: 55,
        statements: migrations::v41_v58::V55,
    },
    Migration {
        version: 56,
        statements: migrations::v41_v58::V56,
    },
    Migration {
        version: 57,
        statements: migrations::v41_v58::V57,
    },
    Migration {
        version: 58,
        statements: migrations::v41_v58::V58,
    },
    // Run by `migrate_session_refs`, which also fills the table in Rust.
    Migration {
        version: 59,
        statements: migrations::v41_v58::V59,
    },
    Migration {
        version: 60,
        statements: migrations::v60_v79::V60,
    },
    Migration {
        version: 61,
        statements: migrations::v60_v79::V61,
    },    Migration {
        version: 62,
        statements: migrations::v60_v79::V62,
    },
];

/// Counts the SQLite virtual-machine instructions a connection runs between
/// `start` and `steps`. Tests compare the count for the same work at two
/// history sizes: unlike elapsed time it is deterministic, so work that grows
/// with unrelated history shows up as a difference.
#[cfg(test)]
pub(crate) struct VmCounter {
    handle: *mut rusqlite::ffi::sqlite3,
    steps: Box<u64>,
}
#[cfg(test)]
unsafe extern "C" fn count_vm_step(pointer: *mut std::ffi::c_void) -> std::ffi::c_int {
    unsafe {
        *pointer.cast::<u64>() += 1;
    }
    0
}
#[cfg(test)]
impl VmCounter {
    /// The counter holds the raw handle rather than a borrow, so the caller
    /// can keep using the connection mutably (for example to open a
    /// transaction) while it counts. Drop or `steps` removes the handler.
    pub(crate) fn start(conn: &Connection) -> Self {
        let mut counter = Self {
            handle: unsafe { conn.handle() },
            steps: Box::new(0),
        };
        unsafe {
            rusqlite::ffi::sqlite3_progress_handler(
                counter.handle,
                1,
                Some(count_vm_step),
                std::ptr::from_mut(counter.steps.as_mut()).cast(),
            );
        }
        counter
    }
    pub(crate) fn steps(self) -> u64 {
        *self.steps
    }
}
#[cfg(test)]
impl Drop for VmCounter {
    fn drop(&mut self) {
        unsafe {
            rusqlite::ffi::sqlite3_progress_handler(self.handle, 0, None, std::ptr::null_mut());
        }
    }
}

// The migration table and configured SQLite timeout are small fixed values.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {
    include!("store_v58_tests.rs");
    include!("store_v59_tests.rs");
    include!("store_v33_tests.rs");

    #[test]
    fn lookup_index_migration_adds_two_indexes_and_changes_no_rows() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let position = MIGRATIONS
            .iter()
            .position(|migration| {
                migration
                    .statements
                    .contains("CREATE INDEX multiplier_observation_provider_day_plan")
            })
            .unwrap();
        for migration in &MIGRATIONS[..position] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,
                 source,effective_from_ms,observed_at_ms)
             VALUES('cred','anthropic','acct','max','anthropic/max','detected',1,2),
                   ('cred','anthropic','acct',NULL,NULL,'detected',3,4);",
        )
        .unwrap();
        let schema = |conn: &Connection| -> Vec<(String, String)> {
            conn.prepare("SELECT type,name FROM sqlite_schema ORDER BY type,name")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let before = all_rows(&conn);
        let mut expected_schema = schema(&conn);
        conn.execute_batch(MIGRATIONS[position].statements).unwrap();
        assert_eq!(all_rows(&conn), before, "no stored row changes");
        for name in [
            "multiplier_observation_provider_day_plan",
            "vault_plan_credential_latest",
        ] {
            expected_schema.push(("index".into(), name.into()));
        }
        expected_schema.sort();
        assert_eq!(
            schema(&conn),
            expected_schema,
            "only the two indexes are added"
        );
        for (index, columns) in [
            (
                "multiplier_observation_provider_day_plan",
                vec!["quota_provider", "observation_day", "plan"],
            ),
            (
                "vault_plan_credential_latest",
                vec!["credential_id", "history_id"],
            ),
        ] {
            let found: Vec<String> = conn
                .prepare(&format!(
                    "SELECT name FROM pragma_index_info('{index}') ORDER BY seqno"
                ))
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert_eq!(found, columns, "{index}");
            let partial: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE name=?1",
                    [index],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(!partial.contains("WHERE"), "{index} covers every row");
        }
    }

    #[test]
    fn step_message_migration_preserves_old_rows_without_backfill() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let position = MIGRATIONS
            .iter()
            .position(|migration| migration.statements.contains("CREATE TABLE step_message"))
            .unwrap();
        for migration in &MIGRATIONS[..position] {
            conn.execute_batch(migration.statements).unwrap();
        }
        seed_integer_chains(&conn);
        conn.execute_batch(
            "INSERT INTO step_detail(event_key,finish) VALUES(10,'stop');
             INSERT INTO step_feed VALUES(1,'broca','step','runner','session',1,'{}');",
        )
        .unwrap();
        let before = all_rows(&conn);
        let detail_schema: String = conn
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name='step_detail'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        conn.execute_batch(MIGRATIONS[position].statements).unwrap();
        let after = all_rows(&conn);
        assert!(after
            .iter()
            .find(|(table, _)| table == "step_message")
            .unwrap()
            .1
            .is_empty());
        assert_eq!(
            after
                .into_iter()
                .filter(|(table, _)| table != "step_message")
                .collect::<Vec<_>>(),
            before,
            "existing ledger, detail and feed rows are unchanged"
        );
        assert_eq!(
            conn.query_row(
                "SELECT sql FROM sqlite_schema WHERE name='step_detail'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            detail_schema,
            "the existing detail constraint is unchanged"
        );
    }

    #[test]
    fn schema_v40_adds_null_opencode_floors_without_changing_cursor_state() {
        let conn = Connection::open_in_memory().unwrap();
        for migration in MIGRATIONS
            .iter()
            .take_while(|migration| migration.version < 40)
        {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO source_runtime
             (source_key,source_module,desired_generation,lineage,consumed_ordinal,
              high_water_ordinal,last_success_ms,pending_known,pending_unknown)
             VALUES ('opencode-v2','fixture',0,'front',100,120,200,0,1);
             INSERT INTO declared_store_cursor
             VALUES ('opencode-thalamus','/fixture','opencode-2','front',100,120);",
        )
        .unwrap();
        let before = all_rows(&conn);
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 40)
                .unwrap()
                .statements,
        )
        .unwrap();
        let expected: Vec<_> = before
            .into_iter()
            .map(|(table, rows)| {
                let rows = if matches!(table.as_str(), "source_runtime" | "declared_store_cursor") {
                    rows.into_iter()
                        .map(|mut row| {
                            row.push(rusqlite::types::Value::Null);
                            row
                        })
                        .collect()
                } else {
                    rows
                };
                (table, rows)
            })
            .collect();
        assert_eq!(all_rows(&conn), expected);
        for table in ["source_runtime", "declared_store_cursor"] {
            let floor: Option<i64> = conn
                .query_row(&format!("SELECT open_row_floor FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(floor, None);
            conn.execute(&format!("UPDATE {table} SET open_row_floor=17"), [])
                .unwrap();
            let floor: i64 = conn
                .query_row(&format!("SELECT open_row_floor FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(floor, 17);
        }
    }

    fn migration_store() -> (TempStoreDirectory, SqliteStore) {
        let directory = TempStoreDirectory::new();
        let store = open_sqlite(&StorageDescriptor {
            module_id: "astrocyte-v23-test".into(),
            storage_namespace: "astrocyte".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: directory
                    .path()
                    .join("store.sqlite")
                    .to_string_lossy()
                    .into_owned(),
            },
        })
        .unwrap();
        (directory, store)
    }

    #[test]
    fn schema_v23_runner_checks_rebuilt_fks_and_rolls_back_to_22_with_enforcement_restored() {
        let (_directory, store) = migration_store();
        store.migrate("astrocyte", &MIGRATIONS[..22]).unwrap();
        store
            .with_conn(|conn| {
                seed_pricing_target(conn, "money", &["input"]);
                conn.pragma_update(None, "foreign_keys", false)?;
                insert_priced_charge(conn, "orphan", "money", "absent", 1, "USD", None)?;
                conn.pragma_update(None, "foreign_keys", true)
            })
            .unwrap();
        let before = store.with_conn(|conn| Ok(all_rows(conn))).unwrap();
        let schema = store
            .with_conn(|conn| Ok(table_rows(conn, "sqlite_schema")))
            .unwrap();
        let error = migrate_store(&store).unwrap_err().to_string();
        assert!(
            error.contains("post_foreign_key_check_priced_charge"),
            "{error}"
        );
        store.with_conn(|conn| {
            assert_eq!(all_rows(conn),before);
            assert_eq!(table_rows(conn,"sqlite_schema"),schema);
            assert_eq!(conn.query_row("SELECT max(version) FROM cortexkit_schema_version WHERE namespace='astrocyte'",[],|r|r.get::<_,i64>(0))?,22);
            assert_eq!(conn.pragma_query_value(None,"foreign_keys",|r|r.get::<_,i64>(0))?,1);
            assert_eq!(conn.pragma_query_value(None,"cache_size",|r|r.get::<_,i64>(0))?,-2000);
            Ok(())
        }).unwrap();
    }

    #[test]
    fn schema_v23_runner_applies_once_and_returns_the_final_store_ahead_outcome() {
        let (_directory, store) = migration_store();
        // Versions run 1..=latest without gaps, so a fresh store applies all of them.
        // Reading the latest from the chain keeps this test valid for new migrations.
        let latest = MIGRATIONS.last().unwrap().version;
        let first = migrate_store(&store).unwrap();
        // The rebuild runner must return the outcome after later migrations too.
        assert_eq!(
            (first.applied, first.recorded, first.chain_max),
            (latest, latest, latest)
        );
        assert_eq!(
            store
                .with_conn(
                    |conn| conn.pragma_query_value(None, "foreign_keys", |r| r.get::<_, i64>(0))
                )
                .unwrap(),
            1
        );
        let second = migrate_store(&store).unwrap();
        assert_eq!(
            (second.applied, second.recorded, second.chain_max),
            (0, latest, latest)
        );
        assert!(!second.store_ahead());
        store
            .with_conn(|conn| {
                assert_eq!(
                    conn.pragma_query_value(None, "foreign_keys", |r| r.get::<_, i64>(0))?,
                    1
                );
                conn.execute(
                    "INSERT INTO cortexkit_schema_version VALUES ('astrocyte',?1,1)",
                    [latest + 1],
                )?;
                Ok(())
            })
            .unwrap();
        let ahead = migrate_store(&store).unwrap();
        assert_eq!(
            (ahead.applied, ahead.recorded, ahead.chain_max),
            (0, latest + 1, latest)
        );
        assert!(ahead.store_ahead());
    }

    #[test]
    fn schema_v25_keeps_lineage_absent_keys_unique_through_the_derived_event_id() {
        let (_directory, store) = migration_store();
        migrate_store(&store).unwrap();
        store
            .with_conn(|conn| {
                let dropped: i64 = conn.query_row(
                    "SELECT count(*) FROM sqlite_schema
                     WHERE name='usage_event_natural_key_lineage_absent'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(dropped, 0);
                // Ingest stores only the hash of the natural key, so a second
                // lineage-absent event with the same key carries the same id and
                // UNIQUE(event_id) refuses it, now that the partial index is gone.
                let id = event_id("producer", None, "ref-a").unwrap();
                let insert = "INSERT INTO usage_event
                     (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,
                      charge_basis,pricing_generation)
                     VALUES (?1,'producer',NULL,'ref-a',1000,'metered_api',0)";
                conn.execute(insert, [&id])?;
                assert!(conn.execute(insert, [&id]).is_err());
                let count: i64 =
                    conn.query_row("SELECT count(*) FROM usage_event", [], |row| row.get(0))?;
                assert_eq!(count, 1);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn schema_v26_adds_a_null_flow_ref_keeps_every_row_and_guards_it() {
        let conn = hourly_store();
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 25)
                .unwrap()
                .statements,
        )
        .unwrap();
        seed_integer_chains(&conn);
        conn.execute_batch(
            "INSERT INTO usage_event (event_id,producer,producer_ref,run_ref,charge_basis,
                                      recorded_at_ms,occurred_at_ms)
             VALUES ('legacy-run','fixture','legacy-run','run-1','metered_api',1,1);",
        )
        .unwrap();
        let before = all_rows(&conn);
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 26)
                .unwrap()
                .statements,
        )
        .unwrap();
        // Every table keeps its rows; usage_event rows gain one trailing
        // NULL, the new flow_ref.
        let after = all_rows(&conn);
        let expected: Vec<_> = before
            .into_iter()
            .map(|(table, rows)| {
                let rows = if table == "usage_event" {
                    rows.into_iter()
                        .map(|mut row| {
                            row.push(rusqlite::types::Value::Null);
                            row
                        })
                        .collect()
                } else {
                    rows
                };
                (table, rows)
            })
            .collect();
        assert_eq!(after, expected);
        let last_column: String = conn
            .query_row(
                "SELECT name FROM pragma_table_info('usage_event') ORDER BY cid DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_column, "flow_ref");
        for sql in [
            "UPDATE usage_event SET flow_ref='flow-1' WHERE event_key=10",
            "UPDATE usage_event SET run_ref='changed' WHERE event_key=10",
            "UPDATE usage_event SET auth_selection='changed' WHERE event_key=10",
            "UPDATE usage_event SET terminal_status='changed' WHERE event_key=10",
        ] {
            refuse_without_effect(&conn, "usage_event", sql, "usage_event_update_guard");
        }
        // A set flow_ref is as fixed as a NULL one.
        conn.execute(
            "INSERT INTO usage_event (event_id,producer,producer_ref,flow_ref,charge_basis,
                                      recorded_at_ms)
             VALUES ('flowed','fixture','flowed','flow-1','metered_api',1)",
            [],
        )
        .unwrap();
        for sql in [
            "UPDATE usage_event SET flow_ref='flow-2' WHERE event_id='flowed'",
            "UPDATE usage_event SET flow_ref=NULL WHERE event_id='flowed'",
        ] {
            refuse_without_effect(&conn, "usage_event", sql, "usage_event_update_guard");
        }
        // The guard still allows a pricing_generation step.
        conn.execute_batch(
            "UPDATE usage_event SET pricing_generation=pricing_generation+1 WHERE event_key=10",
        )
        .unwrap();
    }

    #[test]
    fn schema_v46_adds_a_null_resolved_credential_and_guards_it() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|migration| migration.version < 46) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO usage_event (event_id,producer,producer_ref,auth_selection,
                                      charge_basis,recorded_at_ms)
             VALUES ('legacy','broca','legacy','apikey:anthropic-platform','metered_api',1);",
        )
        .unwrap();
        let before = all_rows(&conn);
        conn.execute_batch(MIGRATIONS[45].statements).unwrap();
        let expected: Vec<_> = before
            .into_iter()
            .map(|(table, rows)| {
                let rows = if table == "usage_event" {
                    rows.into_iter()
                        .map(|mut row| {
                            row.push(rusqlite::types::Value::Null);
                            row
                        })
                        .collect()
                } else {
                    rows
                };
                (table, rows)
            })
            .collect();
        assert_eq!(all_rows(&conn), expected);
        let column: (String, i64, Option<String>) = conn
            .query_row(
                "SELECT type,\"notnull\",dflt_value FROM pragma_table_info('usage_event')
                 WHERE name='resolved_credential'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(column, ("TEXT".into(), 0, None));
        conn.execute_batch(
            "INSERT INTO usage_event (event_id,producer,producer_ref,resolved_credential,
                                      charge_basis,recorded_at_ms)
             VALUES ('paid','broca','paid','apikey:anthropic:acct1','metered_api',1);",
        )
        .unwrap();
        for sql in [
            "UPDATE usage_event SET resolved_credential='key' WHERE event_id='legacy'",
            "UPDATE usage_event SET resolved_credential='other' WHERE event_id='paid'",
            "UPDATE usage_event SET resolved_credential=NULL WHERE event_id='paid'",
            "UPDATE usage_event SET auth_selection='other' WHERE event_id='legacy'",
            "UPDATE usage_event SET flow_ref='other' WHERE event_id='legacy'",
            "UPDATE usage_event SET media_facts='{}' WHERE event_id='legacy'",
        ] {
            let guard = if sql.contains("media_facts") {
                "usage_event_media_facts_update_guard"
            } else {
                "usage_event_update_guard"
            };
            refuse_without_effect(&conn, "usage_event", sql, guard);
        }
        conn.execute_batch(
            "UPDATE usage_event SET pricing_generation=pricing_generation+1,
                charge_basis='subscription_included',currency='USD' WHERE event_id='paid'",
        )
        .unwrap();
    }

    #[test]
    fn schema_v48_discards_only_old_cells_and_preserves_stale_hours_and_dirty_triggers() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|migration| migration.version < 48) {
            conn.execute_batch(migration.statements).unwrap();
        }
        // Clean key-only Ollama and routed Kimi calls, with both plan ledgers
        // and populated format-3 cache children, mirror an upgraded live store.
        conn.execute_batch(
            "INSERT INTO account_subscription VALUES
                 ('acct','openai','openai/pro',20000,'USD',2,'month',10,NULL,'vault');
             INSERT INTO credential_subscription VALUES
                 ('apikey:ollama-cloud','ollama-cloud','ollama-cloud/free',0,'USD',0,'month',0,NULL,'vault'),
                 ('apikey:kimi-for-coding','kimi-code-plan-global','kimi/allegro',2000,'USD',2,'month',0,NULL,'vault');
             INSERT INTO usage_event (event_id,producer,producer_ref,provider,auth_selection,
                 resolved_credential,occurred_at_ms,charge_basis,recorded_at_ms)
             VALUES ('ollama','broca','ollama','ollama-cloud','apikey:ollama-cloud',NULL,20,'subscription_included',21),
                    ('kimi','broca','kimi','kimi-code-plan-global','apikey:kimi-code-plan-global',
                     'apikey:kimi-for-coding',3600030,'subscription_included',3600031);
             INSERT INTO summary_value VALUES (1,'broca'),(2,'subscription_included'),(3,'USD'),(4,'token'),(5,'input');
             UPDATE summary_hour SET built_revision=revision,format_version=3,events_total=1;
             DELETE FROM summary_dirty_hour;
             INSERT INTO summary_cell VALUES (1,0,0,1,0,0,0,0,2,0,0,3,'{}'),(2,0,1,1,0,0,0,0,2,0,0,3,'{}');
             INSERT INTO summary_quantity VALUES (1,4,5,0,'12',1,'12',1),(2,4,5,0,'34',1,'34',1);
             INSERT INTO summary_money VALUES (1,0,3,'100',1),(2,0,3,'200',1);",
        ).unwrap();
        let before = all_rows(&conn);
        let schema_objects = || {
            conn.prepare(
                "SELECT type,name,sql FROM sqlite_schema WHERE type IN ('index','trigger')
                AND sql IS NOT NULL AND name<>'summary_hour_stale' ORDER BY type,name",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };
        let objects_before = schema_objects();
        conn.execute_batch(MIGRATIONS[47].statements).unwrap();
        assert_eq!(MIGRATIONS[47].version, 48);
        let expected = before
            .into_iter()
            .map(|(name, rows)| {
                let rows = if ["summary_cell", "summary_quantity", "summary_money"]
                    .contains(&name.as_str())
                {
                    Vec::new()
                } else {
                    rows
                };
                (name, rows)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            all_rows(&conn),
            expected,
            "ledger rows and stale hour metadata are untouched"
        );
        assert_eq!(
            schema_objects(),
            objects_before,
            "all existing ledger dirty triggers and other indexes survive"
        );
        let stale_index: String = conn
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name='summary_hour_stale'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(stale_index.contains("built_revision IS NOT revision OR format_version IS NOT 4"));
        assert_eq!(conn.query_row(
            "SELECT count(*) FROM summary_hour INDEXED BY summary_hour_stale WHERE built_revision IS NOT revision OR format_version IS NOT 4",
            [], |row| row.get::<_, i64>(0)).unwrap(), 2);

        // Distinct payers may split a visible cell; an identical payer cannot.
        let insert = "INSERT INTO summary_cell VALUES (1,0,0,1,0,0,0,0,2,0,0,3,'{}',1),
                                                        (2,0,0,1,0,0,0,0,2,0,0,3,'{}',2)";
        conn.execute_batch(insert).unwrap();
        assert!(conn.execute_batch("INSERT INTO summary_cell SELECT 3,kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,payload,paying_credential FROM summary_cell WHERE cell_id=1").is_err());
        conn.execute_batch(
            "INSERT INTO summary_quantity VALUES (1,4,5,0,'12',1,'12',1);
                            INSERT INTO summary_money VALUES (1,0,3,'100',1);
                            DELETE FROM summary_cell WHERE cell_id=1;",
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM summary_quantity", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM summary_money", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            0
        );
        conn.execute_batch("INSERT INTO usage_event (event_id,producer,producer_ref,occurred_at_ms,charge_basis,recorded_at_ms)
                            VALUES ('later','broca','later',40,'subscription_included',41);").unwrap();
        assert_eq!(
            conn.query_row("SELECT hour_number FROM summary_dirty_hour", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
        conn.execute_batch("DELETE FROM summary_dirty_hour;
                            UPDATE usage_event SET pricing_generation=pricing_generation+1 WHERE event_id='kimi';").unwrap();
        assert_eq!(
            conn.query_row("SELECT hour_number FROM summary_dirty_hour", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            1
        );
    }

    #[test]
    fn schema_v47_adds_an_empty_guarded_credential_ledger_and_changes_no_rows() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|migration| migration.version < 47) {
            conn.execute_batch(migration.statements).unwrap();
        }
        // Shaped like the live store: an account plan, account-less calls that
        // name a vault credential, and key-only vault history with operator
        // and config sources.
        conn.execute_batch(
            "INSERT INTO account_subscription VALUES
                 ('acct','openai','openai/pro',20000,'USD',2,'month',10,NULL,'vault');
             INSERT INTO usage_event (event_id,producer,producer_ref,provider,auth_selection,
                                      resolved_credential,occurred_at_ms,charge_basis,recorded_at_ms)
             VALUES ('ollama','broca','ollama','ollama-cloud','apikey:ollama-cloud',NULL,20,
                     'subscription_included',21),
                    ('kimi','broca','kimi','kimi-code-plan-global','apikey:kimi-code-plan-global',
                     'apikey:kimi-for-coding',30,'subscription_included',31);
             INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,
                                            source,effective_from_ms,observed_at_ms)
             VALUES ('apikey:ollama-cloud','ollama-cloud',NULL,'free','ollama-cloud/free','operator',5,6),
                    ('apikey:kimi-for-coding','kimi-code-plan-global',NULL,'allegro',
                     'kimi-code-plan-global/allegro','config',0,1);",
        )
        .unwrap();
        let before = all_rows(&conn);
        conn.execute_batch(MIGRATIONS[46].statements).unwrap();
        assert_eq!(MIGRATIONS[46].version, 47);
        let mut expected = before;
        // all_rows lists tables by name.
        let position =
            expected.partition_point(|(name, _)| name.as_str() < "credential_subscription");
        expected.insert(position, ("credential_subscription".into(), Vec::new()));
        assert_eq!(
            all_rows(&conn),
            expected,
            "only the new empty table appears"
        );

        let insert = |from: i64, to: &str| {
            format!(
                "INSERT INTO credential_subscription VALUES
                     ('apikey:ollama-cloud','ollama-cloud','ollama-cloud/free',0,'USD',0,'month',{from},{to},'vault')"
            )
        };
        conn.execute_batch(&insert(20, "NULL")).unwrap();
        for (sql, rule) in [
            (insert(30, "NULL"), "credential_subscription_no_overlap"),
            (insert(5, "25"), "credential_subscription_no_overlap"),
            (
                "UPDATE credential_subscription SET plan_name='changed'".into(),
                "credential_subscription_update_guard",
            ),
            (
                "UPDATE credential_subscription SET effective_to_ms=20".into(),
                "credential_subscription_update_guard",
            ),
            (
                "DELETE FROM credential_subscription".into(),
                "credential_subscription_no_delete",
            ),
            (
                "INSERT INTO credential_subscription VALUES ('','p','plan',0,'USD',0,'month',1,NULL,NULL)"
                    .into(),
                "CHECK constraint failed",
            ),
        ] {
            refuse_without_effect(&conn, "credential_subscription", &sql, rule);
        }
        // Closing the open interval once is the only allowed update, after
        // which a successor may start at the close.
        conn.execute_batch("UPDATE credential_subscription SET effective_to_ms=40")
            .unwrap();
        refuse_without_effect(
            &conn,
            "credential_subscription",
            "UPDATE credential_subscription SET effective_to_ms=50",
            "credential_subscription_update_guard",
        );
        conn.execute_batch(&insert(40, "NULL")).unwrap();
        // The overlap trigger is removed so it cannot hide a missing
        // insert-once guard, as the other REPLACE tests do.
        conn.execute_batch(
            "PRAGMA recursive_triggers=OFF; DROP TRIGGER credential_subscription_no_overlap;",
        )
        .unwrap();
        refuse_replacements(
            &conn,
            "credential_subscription",
            "",
            &[("operator", "'changed'")],
        );
        refuse_replacements(
            &conn,
            "credential_subscription",
            "",
            &[("rowid", "rowid"), ("provider", "'other'")],
        );
        // account_subscription keeps its own guards untouched.
        refuse_without_effect(
            &conn,
            "account_subscription",
            "DELETE FROM account_subscription",
            "account_subscription_no_delete",
        );
    }

    #[test]
    fn schema_v23_runner_restores_foreign_keys_during_unwinding() {
        let (_directory, store) = migration_store();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _restore = ForeignKeyRestore {
                store: &store,
                active: true,
            };
            store
                .with_conn(|conn| set_foreign_keys(conn, false))
                .unwrap();
            panic!("forced migration unwind");
        }));
        assert!(result.is_err());
        store
            .with_conn(|conn| {
                assert_eq!(
                    conn.pragma_query_value(None, "foreign_keys", |r| r.get::<_, i64>(0))?,
                    1
                );
                Ok(())
            })
            .unwrap();
    }

    fn migrate23(conn: &mut Connection) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        tx.execute_batch(
            MIGRATIONS
                .iter()
                .find(|m| m.version == 23)
                .unwrap()
                .statements,
        )?;
        tx.commit()
    }

    fn replace_guard_store() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 31) {
            conn.execute_batch(migration.statements).unwrap();
        }
        seed_integer_chains(&conn);
        conn.execute_batch("INSERT INTO usage_event (event_key,event_id,producer,producer_lineage,producer_ref,charge_basis,recorded_at_ms)
                VALUES (30,'unpriced','fixture','lineage','unpriced','metered_api',1);
            INSERT INTO chargeability_transition VALUES (10,'unresolved','billed','later','later_record',2);
            INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,correction_id)
                VALUES ('adj',10,'USD',-5,'refund',2,2,'operator','retry');
            INSERT INTO statement VALUES ('s','provider','account',0,10,'USD',2);
            INSERT INTO statement_line VALUES ('l','s','line',2,'line',1,'token',1,'USD',NULL,'settled',10);
            INSERT INTO match_attempt VALUES ('attempt','l','settled',10,3000,3000,2);
            INSERT INTO statement_import VALUES ('import','s',printf('%064d',0),'fixture.csv',2,NULL,'created',1,0,0,1,0,0,0,0,0);
            INSERT INTO account_subscription VALUES ('account','provider','plan',100,'USD',2,'month',0,NULL,NULL);
            INSERT INTO step_detail(event_key,finish) VALUES (10,'stop');
            INSERT INTO step_feed VALUES (1,'broca','step','runner','session',1,'{}');").unwrap();
        insert_multiplier_row(&conn, "provider", "2026-01-02", Some(None)).unwrap();
        let before = all_rows(&conn);
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 31)
                .unwrap()
                .statements,
        )
        .unwrap();
        assert_eq!(all_rows(&conn), before, "migration must not rewrite rows");
        // An external writer may leave foreign_keys and recursive_triggers off.
        // Clear cache_step_sequence so its unique event/step indexes cannot hide
        // a missing insert-once guard by rejecting REPLACE for a different reason.
        conn.execute_batch(
            "PRAGMA foreign_keys=OFF; PRAGMA recursive_triggers=OFF;
            DELETE FROM cache_step_sequence;
            DROP TRIGGER account_subscription_no_overlap;",
        )
        .unwrap();
        conn
    }

    fn refuse_replacements(conn: &Connection, table: &str, filter: &str, changes: &[(&str, &str)]) {
        let insert =
            clone_insert(conn, table, changes).replace("LIMIT 1", &format!("{filter} LIMIT 1"));
        let insert = if let Some((_, rowid)) = changes.iter().find(|(name, _)| *name == "rowid") {
            insert.replacen(" (", " (rowid,", 1).replacen(
                " SELECT ",
                &format!(" SELECT {rowid},"),
                1,
            )
        } else {
            insert
        };
        for verb in ["INSERT OR REPLACE", "REPLACE"] {
            let sql = insert.replacen("INSERT", verb, 1);
            let before = all_rows(conn);
            let result = conn.execute(&sql, []);
            assert_eq!(all_rows(conn), before, "{sql}: returned {result:?}");
            let error = result.expect_err("REPLACE must be refused");
            assert!(
                error.to_string().contains(&format!("{table}_insert_once")),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn usage_event_refuses_replace() {
        let conn = replace_guard_store();
        for keys in [
            vec![],
            vec![("event_key", "40")],
            vec![("event_key", "40"), ("event_id", "'other'")],
        ] {
            let mut changes = vec![("recorded_at_ms", "9")];
            changes.extend(keys);
            refuse_replacements(&conn, "usage_event", "WHERE event_key=30", &changes);
        }
    }

    #[test]
    fn priced_charge_refuses_replace() {
        let conn = replace_guard_store();
        refuse_replacements(&conn, "priced_charge", "", &[("recorded_at_ms", "9")]);
        refuse_replacements(
            &conn,
            "priced_charge",
            "",
            &[("charge_key", "101"), ("recorded_at_ms", "9")],
        );
        conn.execute_batch("UPDATE usage_event SET pricing_generation=2 WHERE event_key=10")
            .unwrap();
        refuse_replacements(
            &conn,
            "priced_charge",
            "",
            &[
                ("charge_key", "101"),
                ("pricing_generation", "2"),
                ("recorded_at_ms", "9"),
            ],
        );
        conn.execute_batch(&integer_charge(101, 10, "input", "token", 2, Some(100)))
            .unwrap();
        conn.execute_batch("UPDATE usage_event SET pricing_generation=3 WHERE event_key=10")
            .unwrap();
        refuse_replacements(
            &conn,
            "priced_charge",
            "WHERE charge_key=101",
            &[
                ("charge_key", "102"),
                ("pricing_generation", "3"),
                ("recorded_at_ms", "9"),
            ],
        );
    }

    #[test]
    fn multiplier_observation_refuses_replace() {
        let conn = replace_guard_store();
        refuse_replacements(
            &conn,
            "multiplier_observation",
            "",
            &[("snapshot_at_ms", "9")],
        );
        refuse_replacements(
            &conn,
            "multiplier_observation",
            "",
            &[("observation_id", "2"), ("snapshot_at_ms", "9")],
        );
        insert_multiplier_row(&conn, "provider", "2026-01-02", Some(Some(1))).unwrap();
        refuse_replacements(
            &conn,
            "multiplier_observation",
            "WHERE observation_id=2",
            &[("observation_id", "3"), ("snapshot_at_ms", "9")],
        );
    }

    macro_rules! replace_guard_test {
        ($name:ident, $table:literal, $($changes:expr),+ $(,)?) => {
            #[test]
            fn $name() {
                let conn = replace_guard_store();
                $(refuse_replacements(&conn, $table, "", $changes);)+
            }
        };
    }

    replace_guard_test!(
        measurement_refuses_replace,
        "measurement",
        &[("quantity_units", "9")],
        &[
            ("rowid", "rowid"),
            ("role", "'changed'"),
            ("quantity_units", "9")
        ]
    );
    replace_guard_test!(
        chargeability_transition_refuses_replace,
        "chargeability_transition",
        &[("reason", "'changed'")]
    );
    replace_guard_test!(
        adjustment_refuses_replace,
        "adjustment",
        &[("amount_nanos", "9")],
        &[("adjustment_id", "'other'"), ("amount_nanos", "9")],
        &[
            ("rowid", "rowid"),
            ("adjustment_id", "'other'"),
            ("correction_id", "NULL"),
            ("amount_nanos", "9")
        ]
    );
    replace_guard_test!(
        statement_refuses_replace,
        "statement",
        &[("amount_exponent", "3")],
        &[("statement_id", "'other'"), ("amount_exponent", "3")],
        &[
            ("rowid", "rowid"),
            ("statement_id", "'other'"),
            ("provider", "'other'"),
            ("amount_exponent", "3")
        ]
    );
    replace_guard_test!(
        statement_line_refuses_replace,
        "statement_line",
        &[("description", "'changed'")],
        &[
            ("statement_line_id", "'other'"),
            ("description", "'changed'")
        ],
        &[
            ("rowid", "rowid"),
            ("statement_line_id", "'other'"),
            ("line_id", "'other'"),
            ("description", "'changed'")
        ]
    );
    replace_guard_test!(
        match_attempt_refuses_replace,
        "match_attempt",
        &[("vendor_amount_nanos", "9")],
        &[
            ("rowid", "rowid"),
            ("attempt_id", "'other'"),
            ("vendor_amount_nanos", "9")
        ]
    );
    replace_guard_test!(
        statement_import_refuses_replace,
        "statement_import",
        &[("source_path", "'changed.csv'")],
        &[
            ("rowid", "rowid"),
            ("import_id", "'other'"),
            ("source_path", "'changed.csv'")
        ]
    );
    // The overlap validator is removed in the fixture so it cannot hide a
    // missing insert-once guard; production retains both independent guards.
    replace_guard_test!(
        account_subscription_refuses_replace,
        "account_subscription",
        &[("operator", "'changed'")],
        &[("effective_from_ms", "1"), ("operator", "'changed'")],
        &[
            ("rowid", "rowid"),
            ("provider", "'other'"),
            ("operator", "'changed'")
        ]
    );
    replace_guard_test!(
        step_detail_refuses_replace,
        "step_detail",
        &[("finish", "'changed'")]
    );
    replace_guard_test!(
        step_feed_refuses_replace,
        "step_feed",
        &[("row_json", "'{\"changed\":true}'")]
    );

    fn integer_store() -> Connection {
        let mut conn = open_memory();
        migrate23(&mut conn).unwrap();
        conn
    }

    fn hourly_store() -> Connection {
        let conn = integer_store();
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 24)
                .unwrap()
                .statements,
        )
        .unwrap();
        conn
    }

    /// Tests using current ingest need the latest event schema, even when they
    /// exercise a constraint introduced in an earlier migration.
    fn latest_hourly_store() -> Connection {
        let conn = hourly_store();
        for migration in MIGRATIONS.iter().filter(|migration| migration.version > 24) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn hourly_request() -> crate::ingest::EventRecordRequest {
        serde_json::from_value(serde_json::json!({
            "producer":"hourly-test", "producer_ref":"call", "charge_basis":"unknown",
            "occurred_at_ms":3_600_001,
            "measurements":(["input","output","reasoning","cache_read","cache_write"].map(|role|
                serde_json::json!({"role":role,"unit":"token","kind":"billed","source":"test",
                    "quantity_units":12,"quantity_exponent":0})))
        }))
        .unwrap()
    }

    #[test]
    fn hourly_schema_seeds_every_hour_without_changing_per_call_facts() {
        let conn = integer_store();
        for (index, occurred) in [
            Some(-1_i64),
            None,
            Some(0),
            Some(3_599_999),
            Some(3_600_000),
        ]
        .into_iter()
        .enumerate()
        {
            conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,charge_basis)
                VALUES(?1,'hourly-test',?1,?2,1,'unknown')",params![format!("e{index}"),occurred]).unwrap();
        }
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 24)
                .unwrap()
                .statements,
        )
        .unwrap();
        let dirty = conn
            .prepare("SELECT kind,hour_number FROM summary_dirty_hour ORDER BY kind,hour_number")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(dirty, [(0, -1), (0, 0), (0, 1), (1, 0)]);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM usage_event", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            5
        );
        conn.execute("INSERT INTO summary_value(value) VALUES('')", [])
            .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM summary_value", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert!(conn
            .execute("INSERT INTO summary_value(value) VALUES(NULL)", [])
            .is_err());
    }

    #[test]
    fn hourly_recording_marks_once_and_rolls_back_with_its_measurements() {
        use crate::ingest::{record_event_in_tx, DeliveryPath};
        let mut conn = latest_hourly_store();
        let tx = conn.transaction().unwrap();
        record_event_in_tx(&tx, &hourly_request(), 1, DeliveryPath::Read).unwrap();
        assert_eq!(
            tx.query_row("SELECT revision FROM summary_hour", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            tx.query_row("SELECT count(*) FROM measurement", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            5
        );
        assert_eq!(
            tx.query_row("SELECT count(*) FROM summary_dirty_hour", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        tx.rollback().unwrap();
        for table in [
            "usage_event",
            "measurement",
            "summary_hour",
            "summary_dirty_hour",
        ] {
            assert_eq!(
                conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                0,
                "{table}"
            );
        }
        conn.execute_batch("CREATE TRIGGER refuse_dirty BEFORE INSERT ON summary_dirty_hour BEGIN SELECT RAISE(ABORT,'refuse dirty'); END;").unwrap();
        let tx = conn.transaction().unwrap();
        assert!(record_event_in_tx(&tx, &hourly_request(), 1, DeliveryPath::Read).is_err());
        tx.rollback().unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM usage_event", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn hourly_pricing_bumps_header_before_guarded_heads_and_marks_once() {
        use crate::{ingest, pricing};
        let mut conn = latest_hourly_store();
        let request = hourly_request();
        let event = ingest::record_event(&mut conn, &request, 1).unwrap();
        let tx = conn.transaction().unwrap();
        let plan = pricing::read_reprice_in_tx(&tx, &event.event_id).unwrap();
        tx.commit().unwrap();
        let resolved = pricing::resolve_reprice_without_catalog(&plan).unwrap();
        let tx = conn.transaction().unwrap();
        pricing::commit_reprice_in_tx(
            &tx,
            &plan,
            &resolved,
            2,
            pricing::PricingOrigin::PricingPass,
        )
        .unwrap();
        assert_eq!(
            tx.query_row("SELECT revision FROM summary_hour", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            tx.query_row(
                "SELECT count(*) FROM priced_charge WHERE pricing_generation=1",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            5
        );
        assert_eq!(
            tx.query_row("SELECT pricing_generation FROM usage_event", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            1
        );
        tx.commit().unwrap();
    }

    fn logical_rows(
        conn: &Connection,
        table: &str,
        integer: bool,
    ) -> Vec<Vec<rusqlite::types::Value>> {
        let columns: Vec<String> = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let select = columns.iter().filter_map(|column| {
            if (table == "usage_event" && column == "event_key")
                || (table == "priced_charge" && matches!(column.as_str(), "charge_id" | "charge_key")) {
                return None;
            }
            Some(if integer && column.ends_with("event_key") {
                format!("(SELECT event_id FROM usage_event e WHERE e.event_key=t.{column})")
            } else if table == "priced_charge" && column == "supersedes" {
                let key = if integer { "charge_key" } else { "charge_id" };
                format!("(SELECT pricing_generation FROM priced_charge p WHERE p.{key}=t.supersedes)")
            } else { format!("t.{column}") })
        }).collect::<Vec<_>>().join(",");
        let mut query = conn
            .prepare(&format!("SELECT {select} FROM {table} t ORDER BY {select}"))
            .unwrap();
        let count = query.column_count();
        query
            .query_map([], |row| (0..count).map(|i| row.get(i)).collect())
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn schema_v23_preserves_populated_ledger_and_all_references() {
        let mut conn = open_memory();
        seed_pricing_target(&conn, "a", &["input", "output"]);
        seed_pricing_target(&conn, "b", &["input"]);
        insert_priced_charge(&conn, "root", "a", "input", 1, "USD", None).unwrap();
        conn.execute_batch("UPDATE usage_event SET pricing_generation=2 WHERE event_id='a';
            INSERT INTO pricing_pending VALUES ('b');
            INSERT INTO chargeability_transition VALUES ('a','unresolved','billed','later','later_record',2);
            INSERT INTO adjustment (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,correction_id)
                VALUES ('adj','a','USD',-5,'refund',2,2,'operator','retry');
            INSERT INTO quarantine_evidence VALUES ('b','a','input','token','{}','{}',2);
            INSERT INTO redelivery_log VALUES ('fixture',NULL,'a','a',2);
            INSERT INTO rejected_delivery VALUES ('fixture',NULL,'a','{}','a',2);
            INSERT INTO statement VALUES ('s','provider','account',0,10,'USD',2);
            INSERT INTO statement_line VALUES ('l','s','line',2,'line',1,'token',1,'USD',NULL,'settled','a');
            INSERT INTO match_attempt VALUES ('attempt','l','settled','a',3000,3000,2);").unwrap();
        insert_priced_charge(&conn, "child", "a", "input", 2, "USD", Some("root")).unwrap();
        conn.execute_batch("UPDATE usage_event SET pricing_generation=3 WHERE event_id='a'")
            .unwrap();
        insert_priced_charge(&conn, "head", "a", "input", 3, "USD", Some("child")).unwrap();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let before: Vec<_> = tables
            .iter()
            .map(|t| logical_rows(&conn, t, false))
            .collect();
        let heads_before: (i64,i64) = conn.query_row("SELECT count(*),sum(charge_nanos) FROM priced_charge c WHERE NOT EXISTS (SELECT 1 FROM priced_charge p WHERE p.supersedes=c.charge_id)", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
        migrate23(&mut conn).unwrap();
        for (table, rows) in tables.iter().zip(before) {
            assert_eq!(logical_rows(&conn, table, true), rows, "{table}");
        }
        let heads_after = conn.query_row("SELECT count(*),sum(charge_nanos) FROM priced_charge c WHERE NOT EXISTS (SELECT 1 FROM priced_charge p WHERE p.supersedes=c.charge_key)", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(heads_before, heads_after);
        assert_eq!(heads_after, (1, 3000));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_eq!(
            conn.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        refuse_without_effect(
            &conn,
            "adjustment",
            "UPDATE adjustment SET amount_nanos=99",
            "adjustment_no_update",
        );
        refuse_without_effect(
            &conn,
            "adjustment",
            "DELETE FROM adjustment",
            "adjustment_no_delete",
        );
    }

    #[test]
    fn schema_v23_preflight_refuses_orphans_without_changing_schema_or_rows() {
        let mut conn = open_memory();
        conn.execute_batch("PRAGMA foreign_keys=OFF; INSERT INTO measurement VALUES ('absent','input','token','count','reported',1,0); PRAGMA foreign_keys=ON;").unwrap();
        let before = all_rows(&conn);
        let schema = table_rows(&conn, "sqlite_schema");
        let error = migrate23(&mut conn).unwrap_err();
        assert!(
            error.to_string().contains("pre_measurement_event"),
            "{error}"
        );
        assert_eq!(all_rows(&conn), before);
        assert_eq!(table_rows(&conn, "sqlite_schema"), schema);
        assert_eq!(
            conn.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    fn seed_integer_chains(conn: &Connection) {
        conn.execute_batch("INSERT INTO usage_event (event_key,event_id,producer,producer_ref,charge_basis,recorded_at_ms) VALUES
            (10,'a','fixture','a','subscription_included',1), (20,'b','fixture','b','subscription_included',1);
            UPDATE usage_event SET pricing_generation=1,currency='USD';
            INSERT INTO measurement VALUES (10,'input','token','count','reported',1,0),(20,'input','token','count','reported',1,0),(10,'output','token','count','reported',1,0);
            INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms)
                VALUES (100,10,'input','token',1,'covered','subscription_included',1,'USD',0,'pricing_pass',1);").unwrap();
    }

    fn integer_charge(
        key: i64,
        event: i64,
        role: &str,
        unit: &str,
        generation: i64,
        predecessor: Option<i64>,
    ) -> String {
        let predecessor = predecessor.map_or_else(|| "NULL".into(), |key| key.to_string());
        format!("INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,supersedes,origin,recorded_at_ms)
            VALUES ({key},{event},'{role}','{unit}',{generation},'covered','subscription_included',1,'USD',0,{predecessor},'pricing_pass',1)")
    }

    #[test]
    fn schema_v23_predecessor_must_exist_on_the_same_measurement_even_with_deferred_fks() {
        let mut conn = integer_store();
        seed_integer_chains(&conn);
        let tx = conn.transaction().unwrap();
        tx.execute_batch("PRAGMA defer_foreign_keys=ON; UPDATE usage_event SET pricing_generation=2 WHERE event_key=10;").unwrap();
        for (event, role, unit, generation, predecessor) in [
            (20, "input", "token", 1, 100),
            (10, "output", "token", 2, 100),
            (10, "input", "request", 2, 100),
            (20, "input", "token", 1, 999),
            (20, "input", "token", 1, 101),
        ] {
            refuse_without_effect(
                &tx,
                "priced_charge",
                &integer_charge(101, event, role, unit, generation, Some(predecessor)),
                "priced_charge_supersedes_same_measurement",
            );
        }
        tx.execute_batch(&integer_charge(101, 10, "input", "token", 2, Some(100)))
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT supersedes FROM priced_charge WHERE charge_key=101",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            100
        );
    }

    #[test]
    fn schema_v23_measurement_event_key_foreign_key_rejects_orphans() {
        let conn = integer_store();
        refuse_without_effect(
            &conn,
            "measurement",
            "INSERT INTO measurement VALUES (999,'input','token','count','reported',1,0)",
            "FOREIGN KEY",
        );
        seed_integer_chains(&conn);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM measurement", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            3
        );
    }

    #[test]
    fn schema_v23_child_event_keys_cannot_be_implicitly_allocated() {
        let conn = integer_store();
        conn.execute_batch("INSERT INTO usage_event (event_key,event_id,producer,producer_ref,charge_basis,recorded_at_ms) VALUES (1,'a','fixture','a','unknown',1)").unwrap();
        refuse_without_effect(&conn, "chargeability_transition", "INSERT INTO chargeability_transition VALUES (NULL,'unresolved','unbillable','operator','operator',1)", "NOT NULL");
        refuse_without_effect(
            &conn,
            "pricing_pending",
            "INSERT INTO pricing_pending VALUES (NULL)",
            "NOT NULL",
        );
    }

    #[test]
    fn schema_v23_explain_serves_generations_not_local_keys() {
        let conn = integer_store();
        seed_integer_chains(&conn);
        conn.execute_batch("UPDATE usage_event SET pricing_generation=2 WHERE event_key=10")
            .unwrap();
        conn.execute_batch(&integer_charge(9876, 10, "input", "token", 2, Some(100)))
            .unwrap();
        let explanation = crate::report::explain_charge(
            &conn,
            &crate::report::ChargeExplainRequest {
                event_id: "a".into(),
            },
        )
        .unwrap();
        let value = serde_json::to_value(explanation).unwrap();
        assert_eq!(value["event"]["event_id"], "a");
        let charges = value["measurements"][0]["charges"].as_array().unwrap();
        assert_eq!(charges.len(), 2);
        assert_eq!(charges[0]["pricing_generation"], 1);
        assert!(charges[0]["supersedes"].is_null());
        assert_eq!(charges[0]["current"], false);
        assert_eq!(charges[1]["pricing_generation"], 2);
        assert_eq!(charges[1]["supersedes"], 1);
        assert_eq!(charges[1]["current"], true);
        let encoded = value.to_string();
        for private in ["charge_id", "charge_key", "event_key", "9876"] {
            assert!(
                !encoded.contains(private),
                "explanation leaked {private}: {encoded}"
            );
        }
    }

    #[test]
    fn schema_v23_integer_constraints_preserve_chain_shape_currency_and_immutability() {
        let conn = integer_store();
        seed_integer_chains(&conn);
        conn.execute_batch("UPDATE usage_event SET pricing_generation=2 WHERE event_key=10")
            .unwrap();
        refuse_without_effect(
            &conn,
            "priced_charge",
            &integer_charge(101, 10, "input", "token", 2, None),
            "UNIQUE",
        );
        refuse_without_effect(
            &conn,
            "priced_charge",
            &integer_charge(101, 10, "input", "token", 2, Some(100)).replace("'USD'", "'EUR'"),
            "FOREIGN KEY",
        );
        refuse_without_effect(
            &conn,
            "priced_charge",
            &integer_charge(101, 10, "input", "token", 1, Some(100)),
            "priced_charge_matches_event_generation_and_basis",
        );
        conn.execute_batch(&integer_charge(101, 10, "input", "token", 2, Some(100)))
            .unwrap();
        conn.execute_batch("UPDATE usage_event SET pricing_generation=3 WHERE event_key=10")
            .unwrap();
        refuse_without_effect(
            &conn,
            "priced_charge",
            &integer_charge(102, 10, "input", "token", 3, Some(100)),
            "UNIQUE",
        );
        refuse_without_effect(
            &conn,
            "priced_charge",
            &integer_charge(102, 10, "input", "token", 3, Some(101))
                .replace("'USD',0,", "'USD',-1,"),
            "priced_charge_nonnegative",
        );
        for sql in [
            "UPDATE usage_event SET event_key=11 WHERE event_key=10",
            "UPDATE usage_event SET event_id='changed' WHERE event_key=10",
            "UPDATE usage_event SET auth_selection='changed' WHERE event_key=10",
            "UPDATE usage_event SET terminal_status='changed' WHERE event_key=10",
            "UPDATE usage_event SET charge_basis='metered_api' WHERE event_key=10",
            "UPDATE usage_event SET currency='EUR' WHERE event_key=10",
        ] {
            refuse_without_effect(&conn, "usage_event", sql, "usage_event_update_guard");
        }
        refuse_without_effect(
            &conn,
            "usage_event",
            "DELETE FROM usage_event",
            "usage_event_no_delete",
        );
        refuse_without_effect(
            &conn,
            "measurement",
            "UPDATE measurement SET quantity_units=2",
            "measurement_no_update",
        );
        refuse_without_effect(
            &conn,
            "measurement",
            "DELETE FROM measurement",
            "measurement_no_delete",
        );
        refuse_without_effect(
            &conn,
            "priced_charge",
            "UPDATE priced_charge SET recorded_at_ms=2",
            "priced_charge_no_update",
        );
        refuse_without_effect(
            &conn,
            "priced_charge",
            "DELETE FROM priced_charge",
            "priced_charge_no_delete",
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM priced_charge", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    fn pre19() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 19) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn pre22() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 22) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    #[test]
    fn schema_v22_adjustment_update_and_delete_are_refused_without_effect() {
        let mut conn = pre22();
        seed_pricing_target(&conn, "money", &["input"]);
        conn.execute_batch("INSERT INTO adjustment
            (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
            VALUES ('refund','money','USD',-5,'refund',1,1,'operator')").unwrap();
        let before = table_rows(&conn, "adjustment");
        let tx = conn.transaction().unwrap();
        tx.execute_batch(
            MIGRATIONS
                .iter()
                .find(|m| m.version == 22)
                .unwrap()
                .statements,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(table_rows(&conn, "adjustment"), before);
        refuse_without_effect(
            &conn,
            "adjustment",
            "UPDATE adjustment SET amount_nanos=99 WHERE adjustment_id='refund'",
            "adjustment_no_update",
        );
        refuse_without_effect(
            &conn,
            "adjustment",
            "DELETE FROM adjustment WHERE adjustment_id='refund'",
            "adjustment_no_delete",
        );
        refuse_without_effect(&conn, "adjustment", "INSERT INTO adjustment
            (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
            VALUES ('bad','money','EUR',2,'our_repricing',2,2,'operator')", "adjustment_currency_matches_event");
        conn.execute_batch("INSERT INTO adjustment
            (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
            VALUES ('correction','money','USD',2,'our_repricing',2,2,'operator')").unwrap();
        let amounts = conn
            .prepare("SELECT adjustment_id,amount_nanos FROM adjustment ORDER BY adjustment_id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(amounts, [("correction".into(), 2), ("refund".into(), -5)]);
    }

    fn migrate19(conn: &mut Connection) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        if let Some(migration) = MIGRATIONS.iter().find(|m| m.version == 19) {
            tx.execute_batch(migration.statements)?;
        }
        tx.commit()
    }

    fn refuse_without_effect(conn: &Connection, table: &str, sql: &str, rule: &str) {
        let before = table_rows(conn, table);
        let error = conn.execute_batch(sql).unwrap_err();
        assert!(error.to_string().contains(rule), "{rule}: {error}");
        assert_eq!(table_rows(conn, table), before, "{rule} changed rows");
    }

    macro_rules! source_rule_test {
        ($name:ident, $change:literal, $rule:literal) => {
            #[test]
            fn $name() {
                let conn = open_memory();
                refuse_without_effect(
                    &conn,
                    "source_runtime",
                    concat!(
                        "UPDATE source_runtime SET ",
                        $change,
                        " WHERE source_key='broca'"
                    ),
                    $rule,
                );
            }
        };
    }

    source_rule_test!(
        schema_v19_source_key_not_null,
        "source_key=NULL",
        "source_runtime.source_key"
    );
    source_rule_test!(
        schema_v19_source_key_nonempty,
        "source_key=''",
        "source_runtime_key_nonempty"
    );
    source_rule_test!(
        schema_v19_source_module_nonempty,
        "source_module=''",
        "source_runtime_module_nonempty"
    );
    source_rule_test!(
        schema_v19_source_ordinals_nonnegative,
        "lineage='x',consumed_ordinal=-1,high_water_ordinal=0,last_success_ms=1",
        "source_runtime_ordinals_nonnegative"
    );
    source_rule_test!(
        schema_v19_source_high_water_not_behind,
        "lineage='x',consumed_ordinal=2,high_water_ordinal=1,last_success_ms=1",
        "source_runtime_high_water"
    );
    source_rule_test!(
        schema_v19_source_success_nonnegative,
        "last_success_ms=-1",
        "source_runtime_success_nonnegative"
    );

    #[test]
    fn schema_v19_source_generation_deferred_fk() {
        let mut conn = open_memory();
        let before = table_rows(&conn, "source_runtime");
        let tx = conn.transaction().unwrap();
        tx.execute("UPDATE source_runtime SET desired_generation=999", [])
            .unwrap();
        assert!(tx.commit().unwrap_err().to_string().contains("FOREIGN KEY"));
        assert_eq!(table_rows(&conn, "source_runtime"), before);
    }

    fn clone_insert(conn: &Connection, table: &str, changes: &[(&str, &str)]) -> String {
        let columns: Vec<String> = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let expressions: Vec<&str> = columns
            .iter()
            .map(|column| {
                changes
                    .iter()
                    .find(|(name, _)| *name == column)
                    .map_or(column.as_str(), |(_, value)| *value)
            })
            .collect();
        format!(
            "INSERT INTO {table} ({}) SELECT {} FROM {table} LIMIT 1",
            columns.join(","),
            expressions.join(",")
        )
    }

    fn negative_charge(column: &str) {
        let conn = open_memory();
        seed_pricing_target(&conn, "money", &["input"]);
        insert_priced_charge(&conn, "root", "money", "input", 1, "USD", None).unwrap();
        conn.execute(
            "UPDATE usage_event SET pricing_generation=2 WHERE event_id='money'",
            [],
        )
        .unwrap();
        let mut changes = vec![
            ("charge_id", "'negative'"),
            ("supersedes", "'root'"),
            ("pricing_generation", "2"),
            (column, "-1"),
        ];
        if column == "floor_units" {
            changes.push(("floor_state", "'minimum'"));
        }
        let sql = clone_insert(&conn, "priced_charge", &changes);
        refuse_without_effect(&conn, "priced_charge", &sql, "priced_charge_nonnegative");
    }

    #[test]
    fn schema_v19_charge_nanos_nonnegative() {
        negative_charge("charge_nanos");
    }
    #[test]
    fn schema_v19_rate_units_nonnegative() {
        negative_charge("rate_units");
    }
    #[test]
    fn schema_v19_floor_units_nonnegative() {
        negative_charge("floor_units");
    }

    #[test]
    fn schema_v19_adjustment_currency_matches_event() {
        // Exercise the currency UPDATE guard before v22 makes every UPDATE
        // immutable, so the later trigger cannot mask this guard's failure.
        let conn = pre22();
        seed_pricing_target(&conn, "money", &["input"]);
        refuse_without_effect(&conn, "adjustment", "INSERT INTO adjustment
            (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
            VALUES ('bad','money','EUR',-5,'refund',1,1,'operator')", "adjustment_currency_matches_event");
        conn.execute_batch("INSERT INTO adjustment
            (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
            VALUES ('good','money','USD',-5,'refund',1,1,'operator')").unwrap();
        refuse_without_effect(
            &conn,
            "adjustment",
            "UPDATE adjustment SET currency='EUR'",
            "adjustment_currency_matches_event",
        );
    }

    fn invalid_multiplier(changes: &[(&str, &str)], rule: &str) {
        let conn = open_memory();
        insert_multiplier_row(&conn, "p", "1970-01-01", Some(None)).unwrap();
        let mut edits = vec![("observation_id", "NULL"), ("quota_provider", "'bad'")];
        edits.extend_from_slice(changes);
        let sql = clone_insert(&conn, "multiplier_observation", &edits);
        refuse_without_effect(&conn, "multiplier_observation", &sql, rule);
    }

    #[test]
    fn schema_v19_multiplier_counts_nonnegative() {
        for column in [
            "window_minutes",
            "input_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "output_tokens",
            "reasoning_tokens",
            "tokens_outside_quota_coverage",
            "base_rate_tier_untested_tokens",
            "accounts_included_count",
        ] {
            invalid_multiplier(
                &[(column, "-1")],
                "multiplier_observation_counts_nonnegative",
            );
        }
    }
    #[test]
    fn schema_v19_multiplier_interval_ordered() {
        for end in ["10", "9"] {
            invalid_multiplier(
                &[("interval_start_ms", "10"), ("interval_end_ms", end)],
                "multiplier_observation_interval_ordered",
            );
        }
    }
    #[test]
    fn schema_v19_multiplier_day_matches_utc_end() {
        invalid_multiplier(
            &[("observation_day", "'1970-01-02'")],
            "multiplier_observation_day_matches_end",
        );
        invalid_multiplier(
            &[("interval_end_ms", "9223372036854775807")],
            "multiplier_observation_day_matches_end",
        );
    }

    #[test]
    fn schema_v19_new_rules_also_guard_updates() {
        let mut conn = seeded_pre19();
        migrate19(&mut conn).unwrap();
        // Isolate the value rules from the independent append-only protection.
        conn.execute_batch(
            "DROP TRIGGER priced_charge_no_update; DROP TRIGGER multiplier_observation_no_update",
        )
        .unwrap();
        for (table, change, rule) in [
            (
                "priced_charge",
                "charge_nanos=-1",
                "priced_charge_nonnegative",
            ),
            (
                "multiplier_observation",
                "input_tokens=-1",
                "multiplier_observation_counts_nonnegative",
            ),
            (
                "multiplier_observation",
                "interval_start_ms=interval_end_ms",
                "multiplier_observation_interval_ordered",
            ),
            (
                "multiplier_observation",
                "observation_day='2000-01-01'",
                "multiplier_observation_day_matches_end",
            ),
        ] {
            refuse_without_effect(&conn, table, &format!("UPDATE {table} SET {change}"), rule);
        }
    }

    #[test]
    fn schema_v19_multiplier_day_uses_utc_millisecond_boundary() {
        let conn = open_memory();
        insert_multiplier_row(&conn, "p", "1970-01-01", Some(None)).unwrap();
        let sql = clone_insert(
            &conn,
            "multiplier_observation",
            &[
                ("observation_id", "NULL"),
                ("quota_provider", "'before_epoch'"),
                ("observation_day", "'1969-12-31'"),
                ("interval_start_ms", "-2"),
                ("interval_end_ms", "-1"),
            ],
        );
        conn.execute_batch(&sql).unwrap();
        let sql = clone_insert(
            &conn,
            "multiplier_observation",
            &[
                ("observation_id", "NULL"),
                ("quota_provider", "'before_midnight'"),
                ("interval_start_ms", "0"),
                ("interval_end_ms", "86399999"),
            ],
        );
        conn.execute_batch(&sql).unwrap();
        invalid_multiplier(
            &[("interval_start_ms", "0"), ("interval_end_ms", "86400000")],
            "multiplier_observation_day_matches_end",
        );
    }

    #[test]
    fn schema_v19_attempt_ordinal_overflow_is_integrity_error() {
        let mut conn = latest_hourly_store();
        let mut event = sample_event("overflow");
        event.attempt_ordinal = None;
        insert_event(&conn, &event).unwrap();
        conn.execute_batch("DROP TRIGGER usage_event_update_guard")
            .unwrap();
        conn.execute("UPDATE usage_event SET attempt_ordinal=4294967296", [])
            .unwrap();
        let before = table_rows(&conn, "usage_event");
        let request = crate::ingest::EventRecordRequest {
            producer: event.producer,
            producer_lineage: event.producer_lineage,
            producer_ref: event.producer_ref,
            prior_event_ref: event.prior_event_ref,
            harness: event.harness,
            project_root: event.project_root,
            session: event.session,
            run_ref: event.run_ref,
            flow_ref: None,
            runner: None,
            work_class: event.work_class,
            occurred_at_ms: event.occurred_at_ms,
            emitted_at_ms: event.emitted_at_ms,
            terminal_status: event.terminal_status,
            attempt_ordinal: Some(0),
            vendor_request_id: event.vendor_request_id,
            provider: event.provider,
            model: event.model,
            account_ref: event.account_ref,
            auth_selection: event.auth_selection,
            resolved_credential: None,
            charge_basis: event.charge_basis,
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements: vec![],
            step_detail: Default::default(),
        };
        let error = crate::ingest::record_event(&mut conn, &request, 100).unwrap_err();
        assert!(
            matches!(
                error,
                crate::ingest::IngestError::Store(rusqlite::Error::IntegralValueOutOfRange(
                    9, 4294967296
                ))
            ),
            "{error:?}"
        );
        assert_eq!(table_rows(&conn, "usage_event"), before);
        assert!(table_rows(&conn, "rejected_delivery").is_empty());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read_event(
                &conn,
                &event.event_id
            )))
            .is_err()
        );
    }

    #[test]
    fn capability_probe_failure_releases_savepoint_without_schema_effect() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE astrocyte_capability_child (sentinel TEXT); INSERT INTO astrocyte_capability_child VALUES ('keep')").unwrap();
        let before = table_rows(&conn, "sqlite_schema");
        probe_sqlite_capabilities(&conn).unwrap_err();
        assert!(conn.is_autocommit(), "failed probe leaked a transaction");
        assert_eq!(table_rows(&conn, "sqlite_schema"), before);
        assert_eq!(
            table_rows(&conn, "astrocyte_capability_child"),
            vec![vec![rusqlite::types::Value::Text("keep".into())]]
        );
    }

    fn seeded_pre19() -> Connection {
        let conn = pre19();
        seed_pricing_target(&conn, "seed", &["input"]);
        insert_priced_charge(&conn, "seed", "seed", "input", 1, "USD", None).unwrap();
        conn.execute_batch("INSERT INTO source_runtime VALUES ('file','file',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,42,0,0);
            INSERT INTO source_runtime VALUES ('failed','file',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,41,0,1);
            INSERT INTO source_runtime VALUES ('seq','broca',(SELECT generation FROM desired_config WHERE id=1),'instance:7',40,43,99,3,0);
            INSERT INTO adjustment (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
                VALUES ('seed','seed','USD',-5,'refund',1,1,'operator')").unwrap();
        insert_multiplier_row(&conn, "p", "1970-01-01", Some(None)).unwrap();
        conn
    }

    fn all_rows(conn: &Connection) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        tables
            .into_iter()
            .map(|table| {
                let rows = table_rows(conn, &table);
                (table, rows)
            })
            .collect()
    }

    #[test]
    fn schema_v19_preserves_every_row_and_large_table_roots() {
        let mut conn = seeded_pre19();
        let before = all_rows(&conn);
        let roots = |conn: &Connection| -> Vec<(String, i64)> {
            conn.prepare("SELECT name,rootpage FROM sqlite_schema WHERE name IN ('usage_event','measurement','priced_charge') ORDER BY name").unwrap()
                .query_map([], |row| Ok((row.get(0)?,row.get(1)?))).unwrap().map(Result::unwrap).collect()
        };
        let before_roots = roots(&conn);
        migrate19(&mut conn).unwrap();
        assert_eq!(all_rows(&conn), before);
        assert_eq!(roots(&conn), before_roots);
    }

    #[test]
    fn schema_v19_preflight_counts_violations_and_rolls_back_everything() {
        for (sql, rule) in [
            ("UPDATE source_runtime SET source_key='' WHERE source_key='broca'", "source_runtime_key_nonempty"),
            ("UPDATE source_runtime SET source_module=''", "source_runtime_module_nonempty"),
            ("UPDATE source_runtime SET consumed_ordinal=-1 WHERE source_key='seq'", "source_runtime_ordinals_nonnegative"),
            ("UPDATE source_runtime SET high_water_ordinal=39 WHERE source_key='seq'", "source_runtime_high_water"),
            ("UPDATE source_runtime SET last_success_ms=-1 WHERE source_key='file'", "source_runtime_success_nonnegative"),
            ("UPDATE source_runtime SET desired_generation=999", "source_runtime_generation_fk"),
            ("DROP TRIGGER priced_charge_no_update; UPDATE priced_charge SET charge_nanos=-1", "priced_charge_nonnegative"),
            ("UPDATE adjustment SET currency='EUR'", "adjustment_currency_matches_event"),
            ("DROP TRIGGER multiplier_observation_no_update; UPDATE multiplier_observation SET input_tokens=-1", "multiplier_observation_counts_nonnegative"),
            ("DROP TRIGGER multiplier_observation_no_update; UPDATE multiplier_observation SET interval_end_ms=interval_start_ms", "multiplier_observation_interval_ordered"),
            ("DROP TRIGGER multiplier_observation_no_update; UPDATE multiplier_observation SET observation_day='2000-01-01'", "multiplier_observation_day_matches_end"),
        ] {
            let mut conn = seeded_pre19();
            conn.execute_batch(sql).unwrap();
            let rows = all_rows(&conn);
            let schema = table_rows(&conn, "sqlite_schema");
            let count = if rule == "source_runtime_module_nonempty" || rule == "source_runtime_generation_fk" { 4 } else { 1 };
            let error = migrate19(&mut conn).unwrap_err().to_string();
            assert!(error.contains(&format!("{rule}: {count} violating rows")), "{error}");
            assert_eq!(all_rows(&conn), rows, "{rule}");
            assert_eq!(table_rows(&conn, "sqlite_schema"), schema, "{rule}");
            assert!(conn.is_autocommit());
        }
    }

    #[test]
    fn schema_v19_runner_refusal_preserves_schema_rows_and_version() {
        let directory = TempStoreDirectory::new();
        let path = directory.path().join("store.sqlite");
        let store = open_sqlite(&StorageDescriptor {
            module_id: "astrocyte-v19-test".into(),
            storage_namespace: "astrocyte".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: path.to_string_lossy().into_owned(),
            },
        })
        .unwrap();
        store.migrate("astrocyte", &MIGRATIONS[..18]).unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.execute("UPDATE source_runtime SET source_key=''", [])
            .unwrap();
        let before = all_rows(&conn);
        let schema = table_rows(&conn, "sqlite_schema");
        let error = store
            .migrate("astrocyte", MIGRATIONS)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("source_runtime_key_nonempty: 1 violating rows"),
            "{error}"
        );
        assert_eq!(all_rows(&conn), before);
        assert_eq!(table_rows(&conn, "sqlite_schema"), schema);
        assert_eq!(
            conn.query_row(
                "SELECT MAX(version) FROM cortexkit_schema_version",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            18
        );
    }

    fn seed_query_history(conn: &Connection, events: i64, charges: i64) {
        conn.execute_batch("BEGIN").unwrap();
        conn.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1)
            INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,
                recorded_at_ms,provider,model,account_ref,charge_basis,pricing_generation)
            SELECT printf('e%07d',i),'codex','l',CAST(i AS TEXT),i,1,'openai','model','a','subscription_included',0 FROM n",
            [events]).unwrap();
        conn.execute_batch("UPDATE usage_event SET pricing_generation=1")
            .unwrap();
        conn.execute("WITH roles(role,offset) AS (VALUES('input',0),('output',1),('cache_read',2),('cache_write',3),('reasoning',4))
            INSERT INTO measurement (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
            SELECT event_id,role,'token','billed','usage',1,0 FROM usage_event CROSS JOIN roles
            WHERE (CAST(producer_ref AS INTEGER)-1)*5+offset<?1", [charges]).unwrap();
        conn.execute("INSERT INTO priced_charge (charge_id,event_id,role,unit,pricing_generation,outcome,
                charge_basis,basis_covered,unpriced_reason,origin,recorded_at_ms)
            SELECT event_id||role,event_id,role,unit,1,'unpriced','subscription_included',0,
                CASE WHEN event_id>=printf('e%07d',min(?1,(?2+4)/5)-9) THEN 'no_subscription_in_force' ELSE 'no_rate' END,
                'pricing_pass',1 FROM measurement", params![events, charges]).unwrap();
        conn.execute_batch("INSERT INTO account_subscription (account_ref,provider,plan_name,recurring_cost_minor,
            currency,amount_exponent,period,effective_from_ms) VALUES ('a','openai','plan',20,'USD',2,'month',0); COMMIT").unwrap();
    }

    fn real_query(source: &str, start: &str) -> String {
        let tail = &source[source.find(start).expect("production query moved")..];
        tail[..tail.find('"').unwrap()].to_string()
    }

    fn query_metrics(
        conn: &Connection,
        sql: &str,
        values: &[rusqlite::types::Value],
    ) -> (Vec<String>, i32, Vec<Vec<rusqlite::types::Value>>) {
        let plan = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(rusqlite::params_from_iter(values), |row| row.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let mut stmt = conn.prepare(sql).unwrap();
        let columns = stmt.column_count();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(values), |row| {
                (0..columns).map(|i| row.get(i)).collect()
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let steps = stmt.get_status(rusqlite::StatementStatus::VmStep);
        (plan, steps, rows)
    }

    #[test]
    fn schema_v23_query_plans_bound_backfill_and_multiplier_history() {
        use rusqlite::types::Value::{Integer, Text};
        let mut conn = pre19();
        seed_query_history(&conn, 3_000, 15_000);
        conn.execute_batch("UPDATE usage_event SET currency='USD';
            INSERT INTO adjustment (adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin)
                SELECT event_id,event_id,'USD',-1,'refund',1,1,'operator' FROM usage_event").unwrap();
        migrate19(&mut conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version > 19) {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(migration.statements).unwrap();
            tx.commit().unwrap();
        }
        let indexes: Vec<(String, String)> = conn.prepare("SELECT name,sql FROM sqlite_schema WHERE name IN ('priced_charge_subscription_backfill','usage_event_statement_match','adjustment_event_lookup','usage_event_multiplier_scope_time')")
            .unwrap().query_map([], |row| Ok((row.get(0)?,row.get(1)?))).unwrap().map(Result::unwrap).collect();
        assert_eq!(indexes.len(), 4);
        for (name, _) in &indexes {
            conn.execute_batch(&format!("DROP INDEX {name}")).unwrap();
        }
        // The first candidate page skips past the large prefix of other charge reasons.
        let subscription = real_query(
            include_str!("subscription.rs"),
            "WITH candidates AS MATERIALIZED",
        );
        let tokens = real_query(
            include_str!("multiplier.rs"),
            "SELECT e.event_key, e.producer, e.provider, e.model, m.role, m.unit, m.source,",
        );
        let mut cases = vec![(
            "backfill",
            subscription,
            vec![Integer(0), Integer(0), Integer(100)],
            "priced_charge_subscription_backfill",
        )];
        cases.push((
            "statement_match",
            real_query(
                include_str!("statement.rs"),
                "SELECT event.event_id,event.currency,",
            ),
            vec![Text("openai".into()), Text("a".into()), Text("3000".into())],
            "usage_event_statement_match",
        ));
        cases.push((
            "adjustment_lookup",
            real_query(
                include_str!("statement.rs"),
                "SELECT amount_nanos FROM adjustment WHERE event_key=",
            ),
            vec![Text("e0003000".into())],
            "adjustment_event_lookup",
        ));
        cases.push((
            "time_tokens",
            tokens.replace("{predicate}", "((e.producer = ? AND e.provider = ?))"),
            vec![
                Text("codex".into()),
                Text("openai".into()),
                Text("2991".into()),
                Text("3001".into()),
            ],
            "usage_event_multiplier_scope_time",
        ));
        let multi_scope = tokens
            .replace("{predicate}", "((e.producer = ? AND e.provider = ? AND e.charge_basis = ?) OR (e.producer = ? AND e.provider = ?))");
        cases.push((
            "multi_scope_tokens",
            multi_scope,
            vec![
                Text("codex".into()),
                Text("openai".into()),
                Text("subscription_included".into()),
                Text("pi".into()),
                Text("openai".into()),
                Text("2991".into()),
                Text("3001".into()),
            ],
            "usage_event_multiplier_scope_time",
        ));
        let before: Vec<_> = cases
            .iter()
            .map(|(_, sql, values, _)| {
                let before = sql.replace(" INDEXED BY priced_charge_subscription_backfill", "");
                query_metrics(&conn, &before, values)
            })
            .collect();
        for (_, sql) in indexes {
            conn.execute_batch(&sql).unwrap();
        }
        for ((name, sql, values, index), (old_plan, old_steps, old_rows)) in
            cases.iter().zip(before)
        {
            let (plan, steps, rows) = query_metrics(&conn, sql, values);
            println!("{name}: before={old_plan:?}, VM steps={old_steps}; after={plan:?}, VM steps={steps}");
            assert!(!rows.is_empty(), "{name} fixture must reach the real query");
            assert_eq!(rows, old_rows);
            assert!(
                plan.iter().any(|line| line.contains(index)),
                "{name}: {plan:?}"
            );
            assert!(steps * 10 < old_steps, "{name}: {old_steps} -> {steps}");
        }
        assert_eq!(
            crate::subscription::coverage_backfill_page(&conn, None, 10_000)
                .unwrap()
                .events
                .len(),
            10
        );
    }

    #[test]
    #[ignore = "production-size migration benchmark; creates millions of rows"]
    fn schema_v19_production_size_migration_timing() {
        let directory = TempStoreDirectory::new();
        let mut conn = Connection::open(directory.path().join("benchmark.sqlite")).unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 19) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        let seed_start = std::time::Instant::now();
        seed_query_history(&conn, 1_900_000, 9_300_000);
        println!("seed elapsed={:?}", seed_start.elapsed());
        for (table, count) in [
            ("usage_event", 1_900_000),
            ("measurement", 9_300_000),
            ("priced_charge", 9_300_000),
        ] {
            assert_eq!(
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                count
            );
        }
        let start = std::time::Instant::now();
        migrate19(&mut conn).unwrap();
        println!("migration 19: 1,900,000 events / 9,300,000 measurements / 9,300,000 charges, elapsed={:?}", start.elapsed());
        assert!(conn.is_autocommit());
    }

    #[test]
    fn set_aside_migration_preserves_ledger_and_keys_evidence_by_source_and_locator() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let index = MIGRATIONS
            .iter()
            .position(|m| m.statements.contains("CREATE TABLE set_aside_record"))
            .unwrap();
        for m in &MIGRATIONS[..index] {
            conn.execute_batch(m.statements).unwrap();
        }
        insert_minimal_event(&conn, "before", "lineage", "before", None, None).unwrap();
        let before = table_rows(&conn, "usage_event");
        conn.execute_batch(MIGRATIONS[index].statements).unwrap();
        assert_eq!(table_rows(&conn, "usage_event"), before);
        assert!(table_rows(&conn, "set_aside_record").is_empty());
        conn.execute(
            "INSERT INTO set_aside_record VALUES ('pi','file:line:2','no id',10,10)",
            [],
        )
        .unwrap();
        assert!(conn
            .execute(
                "INSERT INTO set_aside_record VALUES ('pi','file:line:2','no id',20,20)",
                []
            )
            .is_err());
        conn.execute(
            "INSERT INTO set_aside_record VALUES ('codex','file:line:2','no id',10,10)",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE set_aside_record SET last_seen_ms=20 WHERE source_key='pi'",
            [],
        )
        .unwrap();
        assert_eq!(table_rows(&conn, "set_aside_record").len(), 2);
        assert_eq!(
            conn.query_row(
                "SELECT first_seen_ms,last_seen_ms FROM set_aside_record WHERE source_key='pi'",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            )
            .unwrap(),
            (10, 20)
        );
    }

    #[test]
    fn schema_v18_preserves_existing_window_sample_rows_with_null_breakdown() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let v18 = MIGRATIONS
            .iter()
            .position(|migration| migration.version == 18)
            .unwrap();
        for migration in &MIGRATIONS[..v18] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant, used_percent, raw_used_percent, resets_at, window_minutes, source)
             VALUES ('claude', 'account-1', 'secondary', 1700000000000000000, 42.5, 43.0, '2026-09-28T00:00:00Z', 10080, 'oauth')",
            [],
        )
        .unwrap();

        let count_before = table_rows(&conn, "window_sample");
        assert_eq!(count_before.len(), 1);

        conn.execute_batch(MIGRATIONS[v18].statements).unwrap();

        let count_after = table_rows(&conn, "window_sample");
        assert_eq!(count_after.len(), 1);
        let breakdown: Option<String> = conn
            .query_row(
                "SELECT breakdown FROM window_sample WHERE account_ref = 'account-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(breakdown, None);

        // Valid JSON can be inserted into breakdown
        conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant, used_percent, breakdown)
             VALUES ('claude', 'account-2', 'secondary', 1700000001000000000, 10.0, '{\"asOf\":null,\"rows\":[]}')",
            [],
        )
        .unwrap();

        // Invalid JSON is rejected by CHECK constraint
        let bad = conn.execute(
            "INSERT INTO window_sample (provider, account_ref, pool_key, fetched_at_instant, used_percent, breakdown)
             VALUES ('claude', 'account-3', 'secondary', 1700000002000000000, 10.0, 'not json')",
            [],
        );
        assert!(
            bad.is_err(),
            "invalid JSON in breakdown must violate CHECK constraint"
        );
    }

    #[test]
    fn schema_v8_preserves_v7_rows_counts_and_money() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..7] {
            conn.execute_batch(migration.statements).unwrap();
        }
        seed_pricing_target(&conn, "historical", &["input", "output"]);
        for (id, role, generation, prior, origin) in [
            ("root", "input", 1, None, "unattributed"),
            ("child", "input", 2, Some("root"), "operator_reprice"),
            ("output", "output", 2, None, "pricing_pass"),
        ] {
            if generation == 2 {
                conn.execute(
                    "UPDATE usage_event SET pricing_generation=2 WHERE event_id='historical'",
                    [],
                )
                .unwrap();
            }
            conn.execute("INSERT INTO priced_charge (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,supersedes,origin,recorded_at_ms) VALUES (?1,'historical',?2,'token',?3,'priced','metered_api',0,'USD',?3,3,9,'unknown','test.catalog',?3,'{}',?4,?5,3)",params![id,role,generation,prior,origin]).unwrap();
        }
        let before = table_rows(&conn, "priced_charge");
        let money = |conn: &Connection| {
            conn.query_row(
                "SELECT COUNT(*),SUM(charge_nanos) FROM priced_charge",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .unwrap()
        };
        assert_eq!(money(&conn), (3, 5));
        let tx = conn.transaction().unwrap();
        tx.execute_batch(MIGRATIONS[7].statements).unwrap();
        tx.commit().unwrap();
        assert_eq!(table_rows(&conn, "priced_charge"), before);
        assert_eq!(money(&conn), (3, 5));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    #[test]
    fn schema_v8_origin_accepts_correction_rejects_retired_and_invented() {
        let conn = open_memory();
        seed_pricing_target(&conn, "origin", &["input"]);
        let sql = "INSERT INTO priced_charge (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,basis_covered,unpriced_reason,origin,recorded_at_ms) VALUES ('origin','origin','input','token',1,'unpriced','metered_api',0,'no_rate',?1,1)";
        for origin in ["reprice", "ingest", "invented"] {
            let error = conn.execute(sql, [origin]).unwrap_err();
            assert!(error
                .to_string()
                .contains("CHECK constraint failed: origin IN"));
            assert_eq!(count_for_event(&conn, "priced_charge", "origin"), 0);
        }
        conn.execute(sql, ["basis_correction"]).unwrap();
        assert_eq!(count_for_event(&conn, "priced_charge", "origin"), 1);
    }

    #[test]
    fn schema_v7_preserves_rows_with_unattributed_origin() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..6] {
            conn.execute_batch(migration.statements).unwrap();
        }
        seed_pricing_target(&conn, "historical", &["input", "output"]);
        for (id, role, generation, prior) in [
            ("root", "input", 1, None),
            ("child", "input", 2, Some("root")),
            ("output", "output", 2, None),
        ] {
            if id == "child" {
                conn.execute(
                    "UPDATE usage_event SET pricing_generation=2 WHERE event_id='historical'",
                    [],
                )
                .unwrap();
            }
            conn.execute("INSERT INTO priced_charge (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,supersedes,origin,recorded_at_ms) VALUES (?1,'historical',?2,'token',?3,'priced','metered_api',0,'USD',?3,3,9,'unknown','test.catalog',?3,'{}',?4,'reprice',3)", params![id,role,generation,prior]).unwrap();
        }
        let before = table_rows(&conn, "priced_charge");
        assert_eq!(before.len(), 3);
        let tx = conn.transaction().unwrap();
        tx.execute_batch(MIGRATIONS[6].statements).unwrap();
        tx.commit().unwrap();
        let after = table_rows(&conn, "priced_charge");
        assert_eq!(after.len(), 3);
        let origins: Vec<String> = conn
            .prepare("SELECT origin FROM priced_charge")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            origins,
            vec!["unattributed"; 3],
            "pre-v7 origin cannot establish its writer"
        );
        // Compare every column, including IDs, currency, money, and resolution time.
        let expected: Vec<_> = before
            .into_iter()
            .map(|mut row| {
                row[19] = rusqlite::types::Value::Text("unattributed".into());
                row
            })
            .collect();
        assert_eq!(after, expected);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    #[test]
    fn schema_v7_origin_check_rejects_retired_values() {
        for origin in ["reprice", "ingest", "basis_correction"] {
            let conn = Connection::open_in_memory().unwrap();
            initialize_connection(&conn).unwrap();
            for migration in &MIGRATIONS[..7] {
                conn.execute_batch(migration.statements).unwrap();
            }
            seed_pricing_target(&conn, "origin", &["input"]);
            let sql = "INSERT INTO priced_charge (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,basis_covered,unpriced_reason,origin,recorded_at_ms) VALUES ('origin','origin','input','token',1,'unpriced','metered_api',0,'no_rate',?1,1)";
            let error = conn.execute(sql, [origin]).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("CHECK constraint failed: origin IN"),
                "{origin}: {error}"
            );
            assert_eq!(count_for_event(&conn, "priced_charge", "origin"), 0);
            conn.execute(sql, ["pricing_pass"]).unwrap();
        }
    }

    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
    use rusqlite::params;

    use super::*;
    use crate::fact::{event_id, ChargeBasis, Measurement, UsageEvent};

    fn open_memory() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        // Historical string-key constraint fixtures stay on the last schema
        // that used those columns. Integer-key constraints have separate v23 tests.
        for migration in MIGRATIONS
            .iter()
            .filter(|migration| migration.version <= 22)
        {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    #[test]
    fn drained_no_sequence_runtime_is_accepted_by_constraint() {
        let conn = open_memory();
        conn.execute(
            "INSERT INTO source_runtime
             (source_key,source_module,desired_generation,lineage,consumed_ordinal,
              high_water_ordinal,last_success_ms,pending_known,pending_unknown)
             VALUES ('reader','reader',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,42,0,0)",
            [],
        )
        .unwrap();

        let state: (
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            i64,
            i64,
        ) = conn
            .query_row(
                "SELECT lineage,consumed_ordinal,high_water_ordinal,last_success_ms,
                        pending_known,pending_unknown
                 FROM source_runtime WHERE source_key='reader'",
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
            .unwrap();
        assert_eq!(state, (None, None, None, Some(42), 0, 0));
    }

    #[test]
    fn half_drained_sequence_states_remain_unrepresentable_without_effect() {
        let conn = open_memory();
        let malformed = [
            ("ordinals-without-success", Some(5_i64), Some(5_i64), None),
            (
                "success-with-partial-ordinals",
                Some(5_i64),
                None,
                Some(42_i64),
            ),
        ];

        for (source_key, consumed, high_water, last_success_ms) in malformed {
            let error = conn
                .execute(
                    "INSERT INTO source_runtime
                     (source_key,source_module,desired_generation,lineage,consumed_ordinal,
                      high_water_ordinal,last_success_ms,pending_known,pending_unknown)
                     VALUES (?1,?1,(SELECT generation FROM desired_config WHERE id=1),'instance:1',?2,?3,?4,0,0)",
                    params![source_key, consumed, high_water, last_success_ms],
                )
                .unwrap_err();
            assert!(
                error.to_string().contains("source_runtime_state_complete"),
                "{source_key}: {error}"
            );
        }

        let landed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_runtime
                 WHERE source_key IN ('ordinals-without-success','success-with-partial-ordinals')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(landed, 0);
    }

    /// The no-sequence arm is distinguished from the cold arm ONLY by
    /// `last_success_ms`, because both carry all-NULL coordinates. Without this
    /// test that clause is unfenced: weakening it admits a row claiming a
    /// completed drain with nothing pending and no success instant, which is a
    /// drain that never happened reported as one that did.
    ///
    /// Found by mutating the shipped v9 arm and watching every suite stay green.
    #[test]
    fn no_sequence_drain_without_a_success_instant_is_unrepresentable_without_effect() {
        let conn = open_memory();

        let error = conn
            .execute(
                "INSERT INTO source_runtime
                 (source_key,source_module,desired_generation,lineage,consumed_ordinal,
                  high_water_ordinal,last_success_ms,pending_known,pending_unknown)
                 VALUES ('no-seq-no-success','reader',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,NULL,0,0)",
                [],
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("source_runtime_state_complete"),
            "{error}"
        );

        // The same row WITH a success instant is the legal no-sequence state, so
        // the rejection above is about the missing instant and not about the
        // all-NULL coordinates it shares with the cold arm.
        conn.execute(
            "INSERT INTO source_runtime
             (source_key,source_module,desired_generation,lineage,consumed_ordinal,
              high_water_ordinal,last_success_ms,pending_known,pending_unknown)
             VALUES ('no-seq-drained','reader',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,42,0,0)",
            [],
        )
        .unwrap();

        let landed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='no-seq-no-success'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(landed, 0);
    }

    #[test]
    fn schema_v14_preserves_sequenced_and_sequence_less_rows_verbatim() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let migration = MIGRATIONS
            .iter()
            .find(|migration| migration.version == 14)
            .unwrap();
        for previous in MIGRATIONS
            .iter()
            .filter(|previous| previous.version < migration.version)
        {
            conn.execute_batch(previous.statements).unwrap();
        }
        conn.execute(
            "UPDATE source_runtime SET lineage='instance:7',consumed_ordinal=41,
             high_water_ordinal=43,last_success_ms=99,pending_known=2,pending_unknown=1
             WHERE source_key='broca'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO source_runtime
             (source_key,source_module,desired_generation,last_success_ms,pending_known,pending_unknown)
             VALUES ('reader','reader',(SELECT generation FROM desired_config WHERE id=1),101,0,0)",
            [],
        )
        .unwrap();
        let before = table_rows(&conn, "source_runtime");
        assert_eq!(before.len(), 2);
        conn.execute_batch(migration.statements).unwrap();
        assert_eq!(table_rows(&conn, "source_runtime"), before);
        conn.execute(
            "UPDATE source_runtime SET pending_unknown=1 WHERE source_key='reader'",
            [],
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT last_success_ms,pending_unknown FROM source_runtime WHERE source_key='reader'", [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).unwrap(),
            (101, 1)
        );
    }

    /// Inserts one multiplier row with every column given, so a test controls
    /// each value. `supersedes` is only a column from version 16 on.
    fn insert_multiplier_row(
        conn: &Connection,
        provider: &str,
        day: &str,
        supersedes: Option<Option<i64>>,
    ) -> rusqlite::Result<i64> {
        let columns = "quota_provider, observation_day, snapshot_at_ms, interval_start_ms,
             interval_end_ms, token_interval_start_ms, token_interval_end_ms, window_pool_key,
             window_minutes, input_tokens, cache_read_tokens, cache_write_tokens, output_tokens,
             reasoning_tokens, api_nanos, api_nanos_by_model, unpriced_tokens,
             tokens_outside_quota_coverage, base_rate_tier_untested_tokens, subscription_nanos,
             fee_paid_nanos, delta_percent, accounts_included_count, accounts_included,
             accounts_excluded, excluded_pairs, low_resolution, scope_rules, plan_prices,
             catalog_resolved_at_ms, api_twins, reasoning_billed_as_output, error_deflating,
             error_inflating, multiplier, fee_multiplier";
        let values = "?1, ?2, 1000, unixepoch(?2)*1000-10, unixepoch(?2)*1000, 11, 19, 'primary', 10080, 1, 2, 3, 4, 5, 6,
             '[{\"model\":\"m\"}]', '[]', 7, 8, 9, 10, 12.5, 1, '[1]', '[]', '{}', 1, '[]', '[]',
             13, '{}', '[\"openai\"]', 'low', 'high', 0.25, NULL";
        match supersedes {
            None => conn.execute(
                &format!("INSERT INTO multiplier_observation ({columns}) VALUES ({values})"),
                params![provider, day],
            )?,
            Some(supersedes) => conn.execute(
                &format!(
                    "INSERT INTO multiplier_observation ({columns}, supersedes)
                     VALUES ({values}, ?3)"
                ),
                params![provider, day, supersedes],
            )?,
        };
        Ok(conn.last_insert_rowid())
    }

    /// Version 16 rebuilds multiplier_observation with a `supersedes` column.
    /// Every row written under version 15 survives it with every column
    /// identical, as an original (supersedes NULL), and keeps its id.
    #[test]
    fn schema_v16_copies_multiplier_rows_verbatim() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let v16 = MIGRATIONS
            .iter()
            .position(|migration| migration.version == 16)
            .unwrap();
        for migration in &MIGRATIONS[..v16] {
            conn.execute_batch(migration.statements).unwrap();
        }
        insert_multiplier_row(&conn, "codex", "2026-09-25", None).unwrap();
        insert_multiplier_row(&conn, "claude", "2026-09-25", None).unwrap();
        insert_multiplier_row(&conn, "codex", "2026-09-26", None).unwrap();
        let before = table_rows(&conn, "multiplier_observation");
        assert_eq!(before.len(), 3);

        conn.execute_batch(MIGRATIONS[v16].statements).unwrap();

        let after = table_rows(&conn, "multiplier_observation");
        assert_eq!(after.len(), before.len());
        for (old, new) in before.iter().zip(&after) {
            assert_eq!(new.len(), old.len() + 1, "one column is added");
            assert_eq!(&new[..old.len()], &old[..], "every old column is copied");
            assert_eq!(new[old.len()], rusqlite::types::Value::Null, "an original");
        }
        let leftovers: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE '%_v15'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leftovers, 0, "the renamed-aside table is dropped");
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    /// The schema, not only the writer, holds the supersession rules: one
    /// original per provider and day, a correction only within its own
    /// provider and day and at most once per row, and no update or delete.
    #[test]
    fn schema_v16_multiplier_supersession_rules() {
        let conn = open_memory();
        let original = insert_multiplier_row(&conn, "codex", "2026-09-26", None).unwrap();
        insert_multiplier_row(&conn, "claude", "2026-09-26", None).unwrap();
        insert_multiplier_row(&conn, "codex", "2026-09-25", None).unwrap();

        let second_original = insert_multiplier_row(&conn, "codex", "2026-09-26", None)
            .unwrap_err()
            .to_string();
        assert!(
            second_original.contains("UNIQUE"),
            "a second original for a provider and day: {second_original}"
        );
        for (provider, day) in [("claude", "2026-09-26"), ("codex", "2026-09-25")] {
            let error = insert_multiplier_row(&conn, provider, day, Some(Some(original)))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("FOREIGN KEY"),
                "{provider} {day} may not supersede codex 2026-09-26: {error}"
            );
        }
        let missing = insert_multiplier_row(&conn, "codex", "2026-09-26", Some(Some(999)))
            .unwrap_err()
            .to_string();
        assert!(missing.contains("FOREIGN KEY"), "{missing}");

        let correction =
            insert_multiplier_row(&conn, "codex", "2026-09-26", Some(Some(original))).unwrap();
        let fork = insert_multiplier_row(&conn, "codex", "2026-09-26", Some(Some(original)))
            .unwrap_err()
            .to_string();
        assert!(fork.contains("UNIQUE"), "a row is superseded once: {fork}");
        insert_multiplier_row(&conn, "codex", "2026-09-26", Some(Some(correction))).unwrap();

        let update = conn
            .execute("UPDATE multiplier_observation SET multiplier = 1", [])
            .unwrap_err()
            .to_string();
        assert!(
            update.contains("multiplier_observation_no_update"),
            "{update}"
        );
        let delete = conn
            .execute("DELETE FROM multiplier_observation", [])
            .unwrap_err()
            .to_string();
        assert!(
            delete.contains("multiplier_observation_no_delete"),
            "{delete}"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 5);
    }

    #[test]
    fn schema_v14_never_drained_still_requires_unknown_pending() {
        let conn = open_memory();
        let error = conn.execute(
            "INSERT INTO source_runtime (source_key,source_module,desired_generation,pending_known,pending_unknown)
             VALUES ('cold','reader',(SELECT generation FROM desired_config WHERE id=1),0,0)",
            [],
        ).unwrap_err();
        assert!(
            error.to_string().contains("source_runtime_state_complete"),
            "{error}"
        );
    }

    #[test]
    fn schema_v9_converts_only_pi_filesystem_sentinel_and_preserves_every_column() {
        use rusqlite::types::Value::{Integer, Null, Text};

        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..8] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute(
            "UPDATE source_runtime SET lineage='broca-instance:7',consumed_ordinal=41,
             high_water_ordinal=43,last_success_ms=99,pending_known=2,pending_unknown=0
             WHERE source_key='broca'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO source_runtime
             (source_key,source_module,desired_generation,lineage,consumed_ordinal,
              high_water_ordinal,last_success_ms,pending_known,pending_unknown)
             VALUES ('pi','pi',(SELECT generation FROM desired_config WHERE id=1),'filesystem',0,0,101,0,0)",
            [],
        )
        .unwrap();

        let before = table_rows(&conn, "source_runtime");
        assert_eq!(
            before[1],
            vec![
                Text("pi".into()),
                Text("pi".into()),
                Integer(0),
                Text("filesystem".into()),
                Integer(0),
                Integer(0),
                Integer(101),
                Integer(0),
                Integer(0),
            ]
        );

        conn.execute_batch(MIGRATIONS[8].statements).unwrap();
        let after = table_rows(&conn, "source_runtime");
        assert_eq!(after.len(), 2);
        assert_eq!(after[0], before[0], "every broca column must be unchanged");
        assert_eq!(
            after[1],
            vec![
                Text("pi".into()),
                Text("pi".into()),
                Integer(0),
                Null,
                Null,
                Null,
                Integer(101),
                Integer(0),
                Integer(0),
            ]
        );
    }

    /// The deletion must be scoped to the file readers. A bare
    /// `DELETE FROM redelivery_log` would also pass a test that only checked
    /// the reader rows vanish, so a pushing producer's row is asserted to
    /// survive with every column unchanged.
    #[test]
    fn schema_v10_deletes_reader_redeliveries_and_keeps_pushed_rows_byte_identical() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..9] {
            conn.execute_batch(migration.statements).unwrap();
        }
        insert_minimal_event(&conn, "redelivered", "lineage", "ref", None, None).unwrap();
        for (producer, lineage, detected_at_ms) in [
            ("pi", None, 11),
            ("pi", None, 12),
            ("opencode", Some("opencode-lineage"), 13),
            ("broca", Some("broca-instance:7"), 14),
        ] {
            conn.execute(
                "INSERT INTO redelivery_log
                 (producer,producer_lineage,producer_ref,event_id,detected_at_ms)
                 VALUES (?1,?2,'ref','redelivered',?3)",
                params![producer, lineage, detected_at_ms],
            )
            .unwrap();
        }
        let before = table_rows(&conn, "redelivery_log");
        assert_eq!(before.len(), 4);
        let broca_before: Vec<_> = before
            .iter()
            .filter(|row| row[0] == rusqlite::types::Value::Text("broca".into()))
            .cloned()
            .collect();
        assert_eq!(broca_before.len(), 1);

        let tx = conn.unchecked_transaction().unwrap();
        tx.execute_batch(MIGRATIONS[9].statements).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            table_rows(&conn, "redelivery_log"),
            broca_before,
            "pi and opencode rows are deleted; the broca row survives unchanged"
        );
    }

    /// Retiring the THALAMUS lane removes its cursor row and its config column
    /// and nothing else. The other producers' cursor rows are compared whole,
    /// and the facts it delivered -- 471 events in the live store, each with
    /// its measurements and charges -- must survive byte for byte, because
    /// deleting a producer's relationship is not deleting what it reported.
    #[test]
    fn schema_v11_retires_thalamus_runtime_and_column_but_keeps_its_facts() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..10] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute(
            "UPDATE desired_config SET thalamus_connection_file='/run/subc-connection.json' WHERE id=1",
            [],
        )
        .unwrap();
        for (source_key, lineage, ordinal, last_success_ms, pending_unknown) in [
            ("broca", Some("broca-instance:7"), Some(40), 1_001, 0),
            ("opencode", Some("opencode-lineage"), Some(12), 1_002, 0),
            ("pi", None, None, 1_003, 0),
            ("thalamus", Some("1789303948000"), Some(471), 1_004, 0),
        ] {
            conn.execute(
                // Earlier migrations seed a cold broca row; overwrite it so
                // every producer carries distinguishable state.
                "INSERT OR REPLACE INTO source_runtime (source_key,source_module,desired_generation,lineage,consumed_ordinal,high_water_ordinal,last_success_ms,pending_known,pending_unknown)
                 VALUES (?1,?1,(SELECT generation FROM desired_config WHERE id=1),?2,?3,?3,?4,0,?5)",
                params![source_key, lineage, ordinal, last_success_ms, pending_unknown],
            )
            .unwrap();
        }
        const THALAMUS_EVENTS: i64 = 471;
        for index in 0..THALAMUS_EVENTS {
            let event_id = format!("thalamus-{index:04}");
            conn.execute(
                "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,provider,model,charge_basis,pricing_generation,currency)
                 VALUES (?1,'thalamus','1789303948000',?1,1789303948000,'anthropic','claude-opus','metered_api',0,NULL)",
                [&event_id],
            )
            .unwrap();
            for (role, quantity) in [
                ("input", 10 + index),
                ("cache_write_5m", 20 + index),
                ("cache_write_1h", 30 + index),
            ] {
                conn.execute(
                    "INSERT INTO measurement (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
                     VALUES (?1,?2,'token','billed','usage.tokens',?3,0)",
                    params![event_id, role, quantity],
                )
                .unwrap();
            }
            conn.execute(
                "UPDATE usage_event SET currency='USD',pricing_generation=1 WHERE event_id=?1",
                [&event_id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO priced_charge (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
                 VALUES (?1,?1,'input','token',1,'priced','metered_api',0,'USD',?2,3,9,'unknown','test.catalog',1,'{}','pricing_pass',3)",
                params![event_id, 30 + 3 * index],
            )
            .unwrap();
        }
        insert_minimal_event(&conn, "broca-fact", "broca-instance:7", "ref", None, None).unwrap();

        let thalamus_count = |conn: &Connection| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM usage_event WHERE producer='thalamus'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        let sample = |conn: &Connection| -> Vec<rusqlite::types::Value> {
            let mut statement = conn
                .prepare(
                    "SELECT usage_event.*, measurement.*
                     FROM usage_event JOIN measurement USING (event_id)
                     WHERE usage_event.event_id='thalamus-0237' AND measurement.role='cache_write_1h'",
                )
                .unwrap();
            let count = statement.column_count();
            statement
                .query_row([], |row| {
                    (0..count)
                        .map(|index| row.get::<_, rusqlite::types::Value>(index))
                        .collect()
                })
                .unwrap()
        };
        let other_runtime = |conn: &Connection| {
            table_rows(conn, "source_runtime")
                .into_iter()
                .filter(|row| row[0] != rusqlite::types::Value::Text("thalamus".into()))
                .collect::<Vec<_>>()
        };
        let desired_columns = |conn: &Connection| {
            conn.prepare("PRAGMA table_info(desired_config)")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        let events_before = table_rows(&conn, "usage_event");
        let measurements_before = table_rows(&conn, "measurement");
        let charges_before = table_rows(&conn, "priced_charge");
        let sample_before = sample(&conn);
        let runtime_before = other_runtime(&conn);
        assert_eq!(thalamus_count(&conn), THALAMUS_EVENTS);
        assert_eq!(runtime_before.len(), 3, "broca, opencode and pi rows");
        assert!(desired_columns(&conn).contains(&"thalamus_connection_file".to_string()));
        assert_eq!(
            sample_before[1],
            rusqlite::types::Value::Text("thalamus".into()),
            "the sample is a thalamus event"
        );

        let tx = conn.unchecked_transaction().unwrap();
        tx.execute_batch(MIGRATIONS[10].statements).unwrap();
        tx.commit().unwrap();

        let thalamus_runtime: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='thalamus'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(thalamus_runtime, 0, "the thalamus cursor row is removed");
        assert_eq!(other_runtime(&conn), runtime_before);
        assert_eq!(
            desired_columns(&conn),
            [
                "id",
                "generation",
                "canonical_bytes",
                "content_hash",
                "poll_interval_secs",
                "qta_connection_file",
                "broca_connection_file",
                "fusiform_connection_file",
                "updated_at_ms",
            ]
        );
        assert_eq!(thalamus_count(&conn), THALAMUS_EVENTS);
        assert_eq!(sample(&conn), sample_before);
        assert_eq!(table_rows(&conn, "usage_event"), events_before);
        assert_eq!(table_rows(&conn, "measurement"), measurements_before);
        assert_eq!(table_rows(&conn, "priced_charge"), charges_before);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    /// Version 12 seeds the pricing queue with a cheaper predicate than the
    /// pricing pass used ("has any charge row" instead of "has a charge row
    /// nothing supersedes"). The fixture is built to tell them apart if they
    /// ever differed: a superseded chain, a partly headed event, an event with
    /// no measurements, and a plainly headless one. The seeded queue must be
    /// exactly what the old selection returns on the same store.
    #[test]
    fn schema_v12_seeds_the_pricing_queue_with_exactly_the_old_selection() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..11] {
            conn.execute_batch(migration.statements).unwrap();
        }
        seed_pricing_target(&conn, "ev-headless", &["input"]);
        seed_pricing_target(&conn, "ev-priced", &["input"]);
        insert_priced_charge(&conn, "priced-root", "ev-priced", "input", 1, "USD", None).unwrap();
        seed_pricing_target(&conn, "ev-chain", &["input"]);
        insert_priced_charge(&conn, "chain-root", "ev-chain", "input", 1, "USD", None).unwrap();
        conn.execute(
            "UPDATE usage_event SET pricing_generation=2 WHERE event_id='ev-chain'",
            [],
        )
        .unwrap();
        insert_priced_charge(
            &conn,
            "chain-child",
            "ev-chain",
            "input",
            2,
            "USD",
            Some("chain-root"),
        )
        .unwrap();
        seed_pricing_target(&conn, "ev-partial", &["input", "output"]);
        insert_priced_charge(&conn, "partial-root", "ev-partial", "input", 1, "USD", None).unwrap();
        seed_pricing_target(&conn, "ev-empty", &[]);

        let old_selection = conn
            .prepare(
                "SELECT DISTINCT measurement.event_id
                 FROM measurement
                 WHERE NOT EXISTS (
                     SELECT 1 FROM priced_charge
                     WHERE priced_charge.event_id=measurement.event_id
                       AND priced_charge.role=measurement.role
                       AND priced_charge.unit=measurement.unit
                       AND NOT EXISTS (
                           SELECT 1 FROM priced_charge AS child
                           WHERE child.supersedes=priced_charge.charge_id
                       )
                 )
                 ORDER BY measurement.event_id",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(old_selection, vec!["ev-headless", "ev-partial"]);

        let tx = conn.unchecked_transaction().unwrap();
        tx.execute_batch(MIGRATIONS[11].statements).unwrap();
        tx.commit().unwrap();

        let queued = conn
            .prepare("SELECT event_id FROM pricing_pending ORDER BY event_id")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(queued, old_selection);
        let selection_state: (i64, i64) = conn
            .query_row(
                "SELECT plan_generation,backfill_drained_generation
                 FROM pricing_selection_state WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            selection_state,
            (1, 0),
            "the first pass after the upgrade runs the coverage backfill once"
        );
    }

    fn sample_event(producer_ref: &str) -> UsageEvent {
        UsageEvent {
            event_id: event_id("plex", Some("lineage-a"), producer_ref).unwrap(),
            producer: "plex".into(),
            producer_lineage: Some("lineage-a".into()),
            producer_ref: producer_ref.into(),
            prior_event_ref: Some("prior-1".into()),
            harness: Some("connector".into()),
            project_root: Some("/work/project".into()),
            session: Some("session-1".into()),
            run_ref: Some("run-1".into()),
            work_class: Some("image_generation".into()),
            occurred_at_ms: Some(1_725_000_000_123),
            emitted_at_ms: Some(1_725_000_000_456),
            recorded_at_ms: 1_725_000_000_789,
            terminal_status: Some("completed".into()),
            attempt_ordinal: Some(0),
            vendor_request_id: Some("vendor-request-1".into()),
            provider: Some("runway".into()),
            model: Some("gen-3".into()),
            account_ref: Some("account-1".into()),
            auth_selection: None,
            charge_basis: ChargeBasis::MeteredApi,
            pricing_generation: 0,
            currency: None,
        }
    }

    fn insert_event(conn: &Connection, event: &UsageEvent) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO usage_event (
                event_id,producer,producer_lineage,producer_ref,prior_event_ref,
                harness,project_root,session,run_ref,work_class,
                occurred_at_ms,emitted_at_ms,recorded_at_ms,terminal_status,attempt_ordinal,
                vendor_request_id,provider,model,account_ref,
                charge_basis,pricing_generation,currency,auth_selection
             ) VALUES (
                ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,
                ?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23
             )",
            params![
                event.event_id.as_str(),
                event.producer.as_str(),
                event.producer_lineage.as_deref(),
                event.producer_ref.as_str(),
                event.prior_event_ref.as_deref(),
                event.harness.as_deref(),
                event.project_root.as_deref(),
                event.session.as_deref(),
                event.run_ref.as_deref(),
                event.work_class.as_deref(),
                event.occurred_at_ms,
                event.emitted_at_ms,
                event.recorded_at_ms,
                event.terminal_status.as_deref(),
                event.attempt_ordinal.map(i64::from),
                event.vendor_request_id.as_deref(),
                event.provider.as_deref(),
                event.model.as_deref(),
                event.account_ref.as_deref(),
                event.charge_basis.as_str(),
                i64::try_from(event.pricing_generation).unwrap(),
                event.currency.as_deref(),
                event.auth_selection.as_deref(),
            ],
        )
    }

    fn read_event(conn: &Connection, id: &str) -> UsageEvent {
        conn.query_row(
            "SELECT event_id,producer,producer_lineage,producer_ref,prior_event_ref,
                    harness,project_root,session,run_ref,work_class,
                    occurred_at_ms,emitted_at_ms,recorded_at_ms,terminal_status,attempt_ordinal,
                    vendor_request_id,provider,model,account_ref,
                    charge_basis,pricing_generation,currency,auth_selection
             FROM usage_event WHERE event_id=?1",
            [id],
            |row| {
                let attempt_ordinal: Option<i64> = row.get(14)?;
                Ok(UsageEvent {
                    event_id: row.get(0)?,
                    producer: row.get(1)?,
                    producer_lineage: row.get(2)?,
                    producer_ref: row.get(3)?,
                    prior_event_ref: row.get(4)?,
                    harness: row.get(5)?,
                    project_root: row.get(6)?,
                    session: row.get(7)?,
                    run_ref: row.get(8)?,
                    work_class: row.get(9)?,
                    occurred_at_ms: row.get(10)?,
                    emitted_at_ms: row.get(11)?,
                    recorded_at_ms: row.get(12)?,
                    terminal_status: row.get(13)?,
                    attempt_ordinal: attempt_ordinal
                        .map(|value| {
                            u32::try_from(value)
                                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(14, value))
                        })
                        .transpose()?,
                    vendor_request_id: row.get(15)?,
                    provider: row.get(16)?,
                    model: row.get(17)?,
                    account_ref: row.get(18)?,
                    auth_selection: row.get(22)?,
                    charge_basis: ChargeBasis::parse(&row.get::<_, String>(19)?)
                        .ok_or(rusqlite::Error::InvalidQuery)?,
                    pricing_generation: u64::try_from(row.get::<_, i64>(20)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    currency: row.get(21)?,
                })
            },
        )
        .unwrap()
    }

    fn insert_minimal_event(
        conn: &Connection,
        id: &str,
        lineage: &str,
        producer_ref: &str,
        attempt_ordinal: Option<i64>,
        currency: Option<&str>,
    ) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO usage_event (
                event_id,producer,producer_lineage,producer_ref,recorded_at_ms,
                attempt_ordinal,charge_basis,pricing_generation,currency
             ) VALUES (?1,'test-producer',?2,?3,1000,?4,'metered_api',0,?5)",
            params![id, lineage, producer_ref, attempt_ordinal, currency],
        )
    }

    fn insert_measurement(conn: &Connection, measurement: &Measurement) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO measurement (
                event_id,role,unit,kind,source,quantity_units,quantity_exponent
             ) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                measurement.event_id.as_str(),
                measurement.role.as_str(),
                measurement.unit.as_str(),
                measurement.kind.as_str(),
                measurement.source.as_str(),
                measurement.quantity_units,
                measurement.quantity_exponent,
            ],
        )
    }

    fn insert_transition(
        conn: &Connection,
        event_id: &str,
        from_state: &str,
        to_state: &str,
        resolved_by: &str,
    ) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO chargeability_transition (
                event_id,from_state,to_state,reason,resolved_by,occurred_at_ms
             ) VALUES (?1,?2,?3,'test settlement',?4,2000)",
            params![event_id, from_state, to_state, resolved_by],
        )
    }

    fn count_for_event(conn: &Connection, table: &str, event_id: &str) -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM {table} WHERE event_id=?1"),
            [event_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    struct TempStoreDirectory(PathBuf);

    impl TempStoreDirectory {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "astrocyte-v2-store-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempStoreDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn schema_v49_discards_ambiguous_quantity_cache_not_ledger_or_hour_metadata() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 49) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO usage_event(event_id,producer,producer_ref,occurred_at_ms,recorded_at_ms,charge_basis)
             VALUES('unknown','fixture','unknown',1,1,'metered_api');
             INSERT INTO measurement(event_key,role,unit,kind,source)
             VALUES((SELECT event_key FROM usage_event WHERE event_id='unknown'),'input','token','billed','fixture');
             INSERT INTO summary_value VALUES(1,'fixture'),(2,'metered_api'),(3,'token'),(4,'input');
             UPDATE summary_hour SET built_revision=revision,format_version=4,events_total=1;
             DELETE FROM summary_dirty_hour;
             INSERT INTO summary_cell VALUES(1,0,0,1,0,0,0,0,2,0,0,0,'{}',0);
             INSERT INTO summary_quantity VALUES(1,3,4,1,'0',1,'0',0);",
        ).unwrap();
        let before = all_rows(&conn);
        conn.execute_batch(MIGRATIONS[48].statements).unwrap();
        assert_eq!(MIGRATIONS[48].version, 49);
        let expected = before
            .into_iter()
            .map(|(table, rows)| {
                let rows = if ["summary_cell", "summary_quantity", "summary_money"]
                    .contains(&table.as_str())
                {
                    Vec::new()
                } else {
                    rows
                };
                (table, rows)
            })
            .collect::<Vec<_>>();
        assert_eq!(all_rows(&conn), expected);
        assert_eq!(conn.query_row("SELECT count(*) FROM summary_hour INDEXED BY summary_hour_stale WHERE built_revision IS NOT revision OR format_version IS NOT 5",[],|r|r.get::<_,i64>(0)).unwrap(),1);
        conn.execute_batch("INSERT INTO summary_cell VALUES(1,0,0,1,0,0,0,0,2,0,0,0,'{}',0);
                            INSERT INTO summary_quantity VALUES(1,3,4,0,'0',1,'0',0,0);
                            INSERT INTO summary_quantity VALUES(1,3,4,1,'0',1,'0',0,1);
                            DELETE FROM summary_cell WHERE cell_id=1;
                            UPDATE usage_event SET pricing_generation=pricing_generation+1 WHERE event_id='unknown';").unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM summary_quantity", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM summary_dirty_hour", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    #[test]
    fn schema_v52_rewrite_order_backfills_evidence_and_feed_only_steps() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 52) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(r#"INSERT INTO step_evidence(harness,step_ref,runner,first_feed_cursor,recorded_at_ms,facts_json)
            VALUES ('pi','known','broca',2,100,'{"started_at_ms":900}'),
                   ('pi','expired','broca',1,50,'{"started_at_ms":1}');
            INSERT INTO step_feed(feed_cursor,harness,step_ref,source,session,committed_at_ms,row_json)
            VALUES (10,'pi','known','runner','old-session',100,'{"occurred_at_ms":1000,"cache_read":20}'),
                   (11,'pi','file-only','file','session',110,'{"occurred_at_ms":1100}'),
                   (12,'pi','known','file','new-session',120,'{"occurred_at_ms":1200,"cache_read":0}');"#).unwrap();
        let feed_before = table_rows(&conn, "step_feed");
        let evidence_before = table_rows(&conn, "step_evidence");
        conn.execute_batch(MIGRATIONS[51].statements).unwrap();
        assert_eq!(table_rows(&conn, "step_feed"), feed_before);
        for (before, after) in evidence_before
            .iter()
            .zip(table_rows(&conn, "step_evidence"))
        {
            assert_eq!(before, &after[..before.len()]);
        }
        let projected: Vec<String> = conn
            .prepare(
                "SELECT json_array(step_ref,session,runner,first_feed_cursor,order_ms,
            feed_source,occurred_at_ms,reports_cache,positive_read,latest_feed_cursor)
            FROM step_evidence ORDER BY step_ref",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            projected,
            [
                r#"["expired",null,"broca",1,1,null,null,0,0,null]"#,
                r#"["file-only","session",null,11,1100,"file",1100,0,0,11]"#,
                r#"["known","new-session","broca",2,900,"file",1200,1,0,12]"#,
            ]
        );
        crate::rewrite_evidence::save(
            &conn,
            200,
            "pi",
            "known",
            "thalamus",
            &crate::rewrite_evidence::Evidence {
                started_at_ms: Some(800),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT runner,order_ms FROM step_evidence WHERE step_ref='known'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            )
            .unwrap(),
            ("thalamus".into(), 800)
        );
        conn.execute_batch(r#"INSERT INTO step_feed(harness,step_ref,source,session,committed_at_ms,row_json)
            VALUES ('pi','known','runner','new-session',200,'{"occurred_at_ms":1300,"cache_read":3}');
            DELETE FROM step_feed WHERE feed_cursor IN (10,12);"#).unwrap();
        assert_eq!(conn.query_row("SELECT latest_feed_cursor,order_ms,positive_read FROM step_evidence WHERE step_ref='known'",[],
            |row|Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,i64>(2)?))).unwrap(),(13,800,1));
        conn.execute("DELETE FROM step_feed WHERE feed_cursor=13", [])
            .unwrap();
        assert_eq!(conn.query_row("SELECT count(*) FROM step_evidence WHERE step_ref='known' AND latest_feed_cursor IS NULL",[],|row|row.get::<_,i64>(0)).unwrap(),1);
    }

    #[test]
    fn schema_v52_rewrite_order_backfill_timing() {
        for steps in [1_000, 10_000] {
            let conn = Connection::open_in_memory().unwrap();
            initialize_connection(&conn).unwrap();
            for migration in MIGRATIONS.iter().filter(|m| m.version < 52) {
                conn.execute_batch(migration.statements).unwrap();
            }
            conn.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1)
                INSERT INTO step_evidence(harness,step_ref,runner,recorded_at_ms,facts_json,first_feed_cursor)
                SELECT 'pi','step-'||i,'broca',1,json_object('started_at_ms',i),i FROM n",[steps*3]).unwrap();
            for source in ["runner", "file"] {
                conn.execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1)
                    INSERT INTO step_feed(harness,step_ref,source,session,committed_at_ms,row_json)
                    SELECT 'pi','step-'||i,?2,'head-session',1,json_object('occurred_at_ms',i,'cache_read',i%10) FROM n",
                    params![steps,source]).unwrap();
            }
            let started = std::time::Instant::now();
            conn.execute_batch(MIGRATIONS[51].statements).unwrap();
            eprintln!("migration 52: {steps} session steps, {} feed rows, {} evidence rows, in-memory SQLite: {:?}",steps*2,steps*3,started.elapsed());
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM step_evidence WHERE latest_feed_cursor IS NOT NULL",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                steps
            );
        }
    }

    #[test]
    fn schema_v61_backfills_and_maintains_evidence_provider_and_model() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 61) {
            conn.execute_batch(migration.statements).unwrap();
        }
        for step_ref in ["live", "expired"] {
            crate::rewrite_evidence::save(
                &conn,
                100,
                "pi",
                step_ref,
                "broca",
                &crate::rewrite_evidence::Evidence {
                    started_at_ms: Some(100),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        conn.execute_batch(
            r#"INSERT INTO step_feed(feed_cursor,harness,step_ref,source,session,committed_at_ms,row_json)
            VALUES (10,'pi','live','runner','session',100,'{"occurred_at_ms":100,"provider":"anthropic","model":"model-a"}'),
                   (11,'pi','expired','runner','session',100,'{"occurred_at_ms":100,"provider":"openai","model":"model-b"}');
            DELETE FROM step_feed WHERE feed_cursor=11;"#,
        )
        .unwrap();
        let migration = MIGRATIONS
            .iter()
            .find(|migration| migration.version == 61)
            .expect("migration 61");
        conn.execute_batch(migration.statements).unwrap();
        let stored: Vec<(String, Option<String>, Option<String>)> = conn
            .prepare("SELECT step_ref,provider,model FROM step_evidence ORDER BY step_ref")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            stored,
            [
                ("expired".into(), None, None),
                (
                    "live".into(),
                    Some("anthropic".into()),
                    Some("model-a".into()),
                ),
            ]
        );
        crate::rewrite_evidence::save(
            &conn,
            200,
            "pi",
            "new",
            "broca",
            &crate::rewrite_evidence::Evidence::default(),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO step_feed(harness,step_ref,source,session,committed_at_ms,row_json)
             VALUES ('pi','new','runner','session',200,'{\"occurred_at_ms\":200,\"provider\":\"google\",\"model\":\"model-c\"}')",
            [],
        )
        .unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT provider,model FROM step_evidence WHERE step_ref='new'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
            ("google".into(), "model-c".into())
        );
        conn.execute("DELETE FROM step_evidence WHERE step_ref='live'", [])
            .unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT runner,facts_json,provider,model FROM step_evidence WHERE step_ref='live'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap(),
            (None, "{}".into(), "anthropic".into(), "model-a".into(),)
        );
    }

    #[test]
    fn schema_v50_runner_backfill_guards_and_cache_preserve_history() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 50) {
            conn.execute_batch(migration.statements).unwrap();
        }
        for (id, producer, session) in [
            ("broca", "broca", "no-evidence"),
            ("claude-step", "claude-code", "owner-session"),
            ("cross-harness", "pi", "declared"),
            ("payer", "thalamus-harness", "no-evidence"),
            ("media", "colliculus", "declared"),
            ("declared", "opencode", "declared"),
            ("record", "pi", "record-only"),
            ("local", "opencode", "local"),
        ] {
            conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,session,occurred_at_ms,recorded_at_ms,charge_basis) VALUES(?1,?2,?1,?3,1,1,'unknown')", params![id,producer,session]).unwrap();
        }
        conn.execute_batch("INSERT INTO thalamus_payer_session VALUES('opencode','declared',NULL,0);
            INSERT INTO thalamus_payer_record(attempt_id,seq,incarnation,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,auth_selection) VALUES('claude-attempt',2,'one','claude-code','native-session','main','claude-step',1,'p','m','auth');
            INSERT INTO thalamus_payer_record(attempt_id,seq,incarnation,harness,session,request_kind,recorded_at_ms,provider,model,auth_selection) VALUES('a',1,'one','pi','record-only','main',1,'p','m','auth');
            INSERT INTO summary_value VALUES(1,'opencode'),(2,'unknown');
            UPDATE summary_hour SET built_revision=revision,format_version=5,events_total=8;
            DELETE FROM summary_dirty_hour;
            INSERT INTO summary_cell VALUES(1,0,0,1,0,0,0,0,2,0,0,0,'{}',0);").unwrap();
        let before = all_rows(&conn);
        let started = std::time::Instant::now();
        conn.execute_batch(MIGRATIONS[49].statements).unwrap();
        eprintln!(
            "migration 50 eight-row backfill and cache reset: {:?}",
            started.elapsed()
        );
        let rows: Vec<(String, Option<String>)> = conn
            .prepare("SELECT event_id,runner FROM usage_event ORDER BY event_id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("broca".into(), Some("broca".into())),
                ("claude-step".into(), Some("thalamus".into())),
                ("cross-harness".into(), None),
                ("declared".into(), Some("thalamus".into())),
                ("local".into(), None),
                ("media".into(), Some("colliculus".into())),
                ("payer".into(), Some("thalamus".into())),
                ("record".into(), Some("thalamus".into()))
            ]
        );
        let after = all_rows(&conn);
        for ((table, previous), (after_table, current)) in before.iter().zip(&after) {
            assert_eq!(table, after_table);
            if table == "usage_event" {
                for (old, new) in previous.iter().zip(current) {
                    assert_eq!(old, &new[..new.len() - 1]);
                }
            } else if ["summary_cell", "summary_quantity", "summary_money"]
                .contains(&table.as_str())
            {
                assert!(current.is_empty());
            } else {
                assert_eq!(previous, current, "{table}");
            }
        }
        for sql in [
            "UPDATE usage_event SET runner='thalamus' WHERE event_id='local'",
            "UPDATE usage_event SET runner=NULL WHERE event_id='broca'",
            "UPDATE usage_event SET runner='thalamus' WHERE event_id='broca'",
        ] {
            refuse_without_effect(&conn, "usage_event", sql, "usage_event_update_guard");
        }
        for verb in ["REPLACE", "INSERT OR REPLACE"] {
            refuse_without_effect(&conn, "usage_event", &format!("{verb} INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,charge_basis,runner) VALUES('local','opencode','local',1,'unknown','thalamus')"), "usage_event_insert_once");
        }
        assert!(conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,charge_basis,runner) VALUES('invalid','fixture','invalid',1,'unknown','other')", []).is_err());
        assert_eq!(conn.query_row("SELECT count(*) FROM summary_hour INDEXED BY summary_hour_stale WHERE built_revision IS NOT revision OR format_version IS NOT 6",[],|r|r.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }

    #[test]
    #[ignore = "synthetic migration timing; live stores must be rehearsed on a copy"]
    fn schema_v50_runner_backfill_timing() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|m| m.version < 50) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100000)
            INSERT INTO usage_event(event_id,producer,producer_ref,session,recorded_at_ms,charge_basis)
            SELECT 'event-'||i,CASE i%4 WHEN 0 THEN 'broca' WHEN 1 THEN 'thalamus-harness' WHEN 2 THEN 'colliculus' ELSE 'opencode' END,'ref-'||i,'session-'||(i%20000),1,'unknown' FROM n;
            WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
            INSERT INTO thalamus_payer_session SELECT 'opencode','session-'||i,NULL,0 FROM n;").unwrap();
        let started = std::time::Instant::now();
        conn.execute_batch(MIGRATIONS[49].statements).unwrap();
        eprintln!(
            "migration 50: 100000 events, 10000 declared sessions, in-memory SQLite: {:?}",
            started.elapsed()
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM usage_event WHERE runner IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            87500
        );
    }

    /// The chain restarts at v1 and is contiguous; the runner records every
    /// version exactly once even when run twice. The expected chain is derived
    /// from the number of migrations, so adding one needs no edit here, while
    /// a gap, a duplicate or a misnumbered migration still fails.
    #[test]
    fn migration_chain_restarts_at_v1_and_runner_records_it_once() {
        assert!(!MIGRATIONS.is_empty());
        let count = u32::try_from(MIGRATIONS.len()).unwrap();
        let expected: Vec<u32> = (1..=count).collect();
        assert_eq!(
            MIGRATIONS.iter().map(|m| m.version).collect::<Vec<_>>(),
            expected,
            "migration versions must be contiguous from 1"
        );

        let directory = TempStoreDirectory::new();
        let path = directory.path().join("store.sqlite");
        let descriptor = StorageDescriptor {
            module_id: "astrocyte-v2-test".into(),
            storage_namespace: "astrocyte".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: path.to_string_lossy().into_owned(),
            },
        };
        let store = open_sqlite(&descriptor).unwrap();
        migrate_store(&store).unwrap();
        migrate_store(&store).unwrap();
        drop(store);

        let conn = Connection::open(path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM pragma_table_info('valuation_cell') WHERE name='plan_record'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        for table in [
            "rate_era",
            "rate_model",
            "rate_cache_state",
            "rate_dirty_hour",
            "valuation_state",
            "valuation_hour",
            "valuation_cell",
        ] {
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                1,
                "missing cache table {table}"
            );
        }
        let stale_index: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='summary_hour_stale'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            stale_index.contains(&format!("format_version IS NOT {}", crate::runs::FORMAT)),
            "stale-hour index must match the current summary format: {stale_index}"
        );
        let versions = conn
            .prepare(
                "SELECT namespace,version FROM cortexkit_schema_version
                 ORDER BY namespace,version",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            versions,
            expected
                .iter()
                .map(|v| ("astrocyte".to_string(), i64::from(*v)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn fee_record_migration_discards_only_rebuildable_cells_and_dirties_hours() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..55] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch("INSERT INTO valuation_hour(kind,hour_number,state) VALUES(0,0,'built');
            INSERT INTO valuation_cell VALUES(0,0,'[]','[]','pool','same','{}','42','{}');
            INSERT INTO account_subscription(account_ref,provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms)
                VALUES('same','provider','plan',17,'USD',9,'month',0);
            UPDATE valuation_hour SET state='built';").unwrap();
        conn.execute_batch(MIGRATIONS[55].statements).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM valuation_cell", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT state FROM valuation_hour", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "dirty"
        );
        assert_eq!(
            conn.query_row(
                "SELECT recurring_cost_minor FROM account_subscription",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            17
        );
    }

    #[test]
    fn schema_v4_adds_auth_selection_nullable() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        conn.execute_batch(MIGRATIONS[0].statements).unwrap();

        // Before v4 the column does not exist.
        let columns_before: Vec<String> = conn
            .prepare("PRAGMA table_info(usage_event)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(!columns_before.contains(&"auth_selection".to_string()));

        // Seed a row before the migration so we can prove existing rows read NULL.
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('pre-v4','producer','lineage','pre-v4',1,'metered_api');",
        )
        .unwrap();
        let rows_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();

        conn.execute_batch(MIGRATIONS[3].statements).unwrap();

        // After v4 the column exists, is nullable, and has no default.
        let columns_after: Vec<(String, String, i64, Option<String>, i64)> = conn
            .prepare("PRAGMA table_info(usage_event)")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let auth = columns_after
            .iter()
            .find(|(name, _, _, _, _)| name == "auth_selection")
            .expect("auth_selection column present after v4");
        assert_eq!(auth.1, "TEXT");
        assert_eq!(auth.2, 0, "nullable (no NOT NULL)");
        assert_eq!(auth.3, None, "no default");
        assert_eq!(auth.4, 0, "not a primary key");

        // Existing rows read NULL; row count unchanged.
        let value: Option<String> = conn
            .query_row(
                "SELECT auth_selection FROM usage_event WHERE event_id='pre-v4'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, None);
        let rows_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows_before, rows_after);
    }

    /// Version 13 adds the two charge-basis provenance columns as NULL on
    /// existing rows and brings them, and the previously unguarded
    /// auth_selection, under the update guard: once written, even a NULL cannot
    /// be filled in later. The guard's other rules are unchanged, so the
    /// write-once currency fill still works and a guarded column is still
    /// refused.
    #[test]
    fn schema_v13_provenance_columns_start_null_and_are_immutable() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let v13 = MIGRATIONS
            .iter()
            .position(|migration| migration.version == 13)
            .unwrap();
        for migration in &MIGRATIONS[..v13] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('pre-v13','broca',NULL,'pre-v13',1,'subscription_included');",
        )
        .unwrap();
        conn.execute_batch(MIGRATIONS[v13].statements).unwrap();

        let stored: (Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT charge_basis_mapping_version,charge_basis_overrides
                 FROM usage_event WHERE event_id='pre-v13'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (None, None));

        for update in [
            "UPDATE usage_event SET charge_basis_mapping_version=4 WHERE event_id='pre-v13'",
            "UPDATE usage_event SET charge_basis_overrides='none' WHERE event_id='pre-v13'",
            "UPDATE usage_event SET work_class='x' WHERE event_id='pre-v13'",
            "UPDATE usage_event SET auth_selection='apikey:x' WHERE event_id='pre-v13'",
        ] {
            let error = conn.execute(update, []).unwrap_err();
            assert!(
                error.to_string().contains("usage_event_update_guard"),
                "{update}: {error}"
            );
        }
        conn.execute(
            "UPDATE usage_event SET currency='USD' WHERE event_id='pre-v13'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis,
                  charge_basis_mapping_version,charge_basis_overrides)
             VALUES ('post-v13','broca',NULL,'post-v13',1,'metered_api',4,'none')",
            [],
        )
        .unwrap();
        for update in [
            "UPDATE usage_event SET charge_basis_mapping_version=5 WHERE event_id='post-v13'",
            "UPDATE usage_event SET charge_basis_overrides=NULL WHERE event_id='post-v13'",
        ] {
            let error = conn.execute(update, []).unwrap_err();
            assert!(
                error.to_string().contains("usage_event_update_guard"),
                "{update}: {error}"
            );
        }
    }

    #[test]
    fn schema_v5_rebuild_preserves_drained_and_never_drained_producers() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in &MIGRATIONS[..4] {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute(
            "UPDATE source_runtime SET source_instance_id='550e8400-e29b-41d4-a716-446655440000',source_epoch=7,consumed_export_seq=41,high_water_export_seq=43,last_success_ms=99,pending_known=2,pending_unknown=0 WHERE source_key='broca'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO source_runtime (source_key,source_module,desired_generation,source_instance_id,source_epoch,consumed_export_seq,high_water_export_seq,last_success_ms,pending_known,pending_unknown) VALUES ('never','never',(SELECT generation FROM desired_config WHERE id=1),NULL,NULL,NULL,NULL,NULL,0,1)",
            [],
        )
        .unwrap();

        conn.execute_batch(MIGRATIONS[4].statements).unwrap();
        type RuntimeRow = (
            String,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        );
        let rows: Vec<RuntimeRow> = conn
            .prepare("SELECT source_key,lineage,consumed_ordinal,high_water_ordinal,last_success_ms FROM source_runtime ORDER BY source_key")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "broca".into(),
                    Some("550e8400-e29b-41d4-a716-446655440000:7".into()),
                    Some(41),
                    Some(43),
                    Some(99),
                ),
                ("never".into(), None, None, None, None),
            ]
        );

        let error = conn
            .execute(
                "INSERT INTO source_runtime (source_key,source_module,desired_generation,lineage,consumed_ordinal,high_water_ordinal,last_success_ms,pending_known,pending_unknown) VALUES ('half','half',(SELECT generation FROM desired_config WHERE id=1),'9',NULL,1,1,0,0)",
                [],
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("source_runtime_state_complete"),
            "constraint name missing from {error}"
        );
        let landed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_runtime WHERE source_key='half'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(landed, 0);
    }

    #[test]
    fn v2_rebuild_preserves_every_priced_charge_row_and_maps_floor_state() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        conn.execute_batch(MIGRATIONS[0].statements).unwrap();
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('priced','producer','lineage','priced',1,'metered_api');
             INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('unpriced','producer','lineage','unpriced',1,'metered_api');
             INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ('priced','input','token','billed','test',1,0);
             INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ('unpriced','input','token','billed','test',1,0);
             UPDATE usage_event SET currency='USD',pricing_generation=1 WHERE event_id='priced';
             UPDATE usage_event SET pricing_generation=1 WHERE event_id='unpriced';
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                  catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
             VALUES
                 ('priced-charge','priced','input','token',1,'priced','metered_api',
                  0,'USD',3,3,9,'test.catalog',1,'{}','reprice',1);
             UPDATE usage_event SET pricing_generation=2 WHERE event_id='priced';
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                  catalog_source,resolved_at_ms,catalog_entry,supersedes,origin,recorded_at_ms)
             VALUES
                 ('priced-child','priced','input','token',2,'priced','metered_api',
                  0,'USD',4,4,9,'test.catalog',2,'{}','priced-charge','reprice',2);
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,unpriced_reason,origin,recorded_at_ms)
             VALUES
                 ('unpriced-charge','unpriced','input','token',1,'unpriced','metered_api',
                  0,'no_rate','reprice',1);",
        )
        .unwrap();

        let schema_objects = |conn: &Connection| {
            conn.prepare(
                "SELECT type,name FROM sqlite_master
                 WHERE tbl_name='priced_charge' AND type IN ('index','trigger')
                 ORDER BY type,name",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };
        let before_objects = schema_objects(&conn);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            3
        );

        conn.execute_batch(MIGRATIONS[1].statements).unwrap();

        let rows = conn
            .prepare("SELECT outcome,floor_state,floor_units FROM priced_charge ORDER BY outcome")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("priced".into(), Some("unknown".into()), None),
                ("priced".into(), Some("unknown".into()), None),
                ("unpriced".into(), None, None),
            ]
        );
        assert_eq!(schema_objects(&conn), before_objects);
        let columns = conn
            .prepare("PRAGMA table_info(priced_charge)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(columns.contains(&"floor_state".to_string()));
        assert!(columns.contains(&"floor_units".to_string()));
        assert!(!columns.contains(&"floor_exponent".to_string()));
    }

    /// Every row of a table, all columns, in a form `assert_eq!` can compare,
    /// used to prove a rebuild leaves a table byte-for-byte intact.
    fn table_rows(conn: &Connection, table: &str) -> Vec<Vec<rusqlite::types::Value>> {
        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
            .unwrap();
        let count = stmt.column_count();
        stmt.query_map([], |row| {
            (0..count)
                .map(|index| row.get::<_, rusqlite::types::Value>(index))
                .collect()
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
    }

    /// v6 widens the unpriced_reason vocabulary with `rate_inherited`. Proved
    /// non-vacuously: the same insert is REFUSED by the v5 CHECK (named), then
    /// accepted after v6, and a row written before the rebuild survives it
    /// byte-for-byte alongside its index and triggers.
    #[test]
    fn schema_v6_widens_unpriced_reason_with_rate_inherited_and_preserves_rows() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS.iter().filter(|migration| migration.version <= 5) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('ev','producer','lineage','ev',1,'metered_api');
             INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ('ev','input','token','billed','test',1,0),
                    ('ev','output','token','billed','test',1,0);
             UPDATE usage_event SET pricing_generation=1 WHERE event_id='ev';
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,unpriced_reason,origin,recorded_at_ms)
             VALUES ('kept','ev','input','token',1,'unpriced','metered_api',0,'no_rate','reprice',1);",
        )
        .unwrap();
        let insert_inherited = "INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,unpriced_reason,origin,recorded_at_ms)
             VALUES ('new','ev','output','token',1,'unpriced','metered_api',0,'rate_inherited','reprice',2)";
        let refused = conn.execute(insert_inherited, []).unwrap_err();
        assert!(
            refused.to_string().contains("CHECK"),
            "v5 must refuse the new reason by its CHECK: {refused}"
        );

        let before = table_rows(&conn, "priced_charge");
        let v6 = MIGRATIONS.iter().find(|m| m.version == 6).unwrap();
        conn.execute_batch(v6.statements).unwrap();
        assert_eq!(
            table_rows(&conn, "priced_charge"),
            before,
            "rebuild must carry rows byte-for-byte"
        );
        conn.execute(insert_inherited, [])
            .expect("v6 accepts rate_inherited");
        for guard in [
            "priced_charge_no_update",
            "priced_charge_no_delete",
            "priced_charge_one_chain_root",
        ] {
            let present: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name=?1",
                    [guard],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(present, 1, "{guard} must exist on the rebuilt table");
        }
    }

    #[test]
    fn v2_rebuild_refuses_a_populated_legacy_floor() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        conn.execute_batch(MIGRATIONS[0].statements).unwrap();
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('legacy-floor','producer','lineage','legacy-floor',1,'metered_api');
             INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ('legacy-floor','input','token','billed','test',1,0);
             UPDATE usage_event SET currency='USD',pricing_generation=1
             WHERE event_id='legacy-floor';
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                  floor_units,floor_exponent,catalog_source,resolved_at_ms,catalog_entry,
                  origin,recorded_at_ms)
             VALUES
                 ('legacy-floor-charge','legacy-floor','input','token',1,'priced','metered_api',
                  0,'USD',64,3,9,64,9,'test.catalog',1,'{}','reprice',1);",
        )
        .unwrap();

        // The rebuilt table's own CHECK would also refuse this row mid-copy, so
        // `is_err()` alone cannot tell the guard from the constraint. The guard
        // exists to name the cause before the copy starts; assert the name.
        let error = conn
            .execute_batch(MIGRATIONS[1].statements)
            .expect_err("a populated legacy floor must refuse the rebuild");
        assert!(
            error.to_string().contains("legacy_floor_rows"),
            "refusal must come from the named guard, not the copy: {error}"
        );
        assert_eq!(
            conn.query_row(
                "SELECT floor_units FROM priced_charge WHERE charge_id='legacy-floor-charge'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            64
        );
    }

    #[test]
    fn schema_v3_adds_window_sample_without_rewriting_v2_state() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        conn.execute_batch(MIGRATIONS[0].statements).unwrap();
        conn.execute_batch(
            "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('priced','producer','lineage','priced',1,'metered_api');
             INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis)
             VALUES ('unpriced','producer','lineage','unpriced',1,'metered_api');
             INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ('priced','input','token','billed','test',1,0);
             INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ('unpriced','input','token','billed','test',1,0);
             UPDATE usage_event SET currency='USD',pricing_generation=1 WHERE event_id='priced';
             UPDATE usage_event SET pricing_generation=1 WHERE event_id='unpriced';
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                  catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms)
             VALUES
                 ('priced-charge','priced','input','token',1,'priced','metered_api',
                  0,'USD',3,3,9,'test.catalog',1,'{}','reprice',1);
             UPDATE usage_event SET pricing_generation=2 WHERE event_id='priced';
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                  catalog_source,resolved_at_ms,catalog_entry,supersedes,origin,recorded_at_ms)
             VALUES
                 ('priced-child','priced','input','token',2,'priced','metered_api',
                  0,'USD',4,4,9,'test.catalog',2,'{}','priced-charge','reprice',2);
             INSERT INTO priced_charge
                 (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                  basis_covered,unpriced_reason,origin,recorded_at_ms)
             VALUES
                 ('unpriced-charge','unpriced','input','token',1,'unpriced','metered_api',
                  0,'no_rate','reprice',1);",
        )
        .unwrap();
        conn.execute_batch(MIGRATIONS[1].statements).unwrap();

        conn.execute(
            "INSERT INTO capacity_window
             (provider,api_provider,account_ref,pool_key,slot_kind,used_percent,
              fetched_at,stale_since,stale_class,observed_at_ms)
             VALUES ('claude','anthropic','acct-a','primary','primary',42.5,
                     '2026-08-13T16:03:50Z','2026-08-13T16:05:00Z','upstream_failed',1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO capacity_observation
             (provider,api_provider,account_ref,observation_state,error_class,error_detail,
              source,fetched_at,observed_at_ms)
             VALUES ('gemini','google','acct-b','degraded','auth_expired','token invalid',
                     'env','2026-08-13T16:03:50Z',1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO capacity_provider_reading
             (provider,api_provider,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('codex','openai','primary','primary',20.0,1000)",
            [],
        )
        .unwrap();

        let table_exists = |conn: &Connection, table: &str| -> bool {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                params![table],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
                > 0
        };

        assert!(
            !table_exists(&conn, "window_sample"),
            "window_sample must be absent before v3"
        );

        let before_priced_charges: i64 = conn
            .query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before_priced_charges, 3);

        let migration = MIGRATIONS
            .iter()
            .find(|migration| migration.version == 3)
            .unwrap();
        conn.execute_batch(migration.statements).unwrap();

        assert!(
            table_exists(&conn, "window_sample"),
            "window_sample must be present after v3"
        );

        let sample_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM window_sample", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sample_rows, 0, "v3 creates an empty history table");

        let after_priced_charges: i64 = conn
            .query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            after_priced_charges, before_priced_charges,
            "priced_charge row count must be unchanged across v3 migration"
        );

        let create_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='window_sample'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            create_sql.contains("append-only"),
            "table sql in sqlite_master must contain append-only comment: {create_sql}"
        );
        assert!(
            create_sql
                .contains("PRIMARY KEY (provider, account_ref, pool_key, fetched_at_instant)"),
            "window_sample primary key must key on identity triple + parsed instant: {create_sql}"
        );
    }

    #[test]
    fn fresh_schema_contains_only_v2_and_surviving_capacity_tables() {
        let conn = open_memory();
        let tables = conn
            .prepare(
                "SELECT name FROM sqlite_schema
                 WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();
        assert_eq!(
            tables,
            vec![
                "account_subscription".to_string(),
                "adjustment".to_string(),
                "capacity_observation".to_string(),
                "capacity_provider_reading".to_string(),
                "capacity_runtime".to_string(),
                "capacity_window".to_string(),
                "chargeability_transition".to_string(),
                "desired_config".to_string(),
                "match_attempt".to_string(),
                "measurement".to_string(),
                "multiplier_observation".to_string(),
                "priced_charge".to_string(),
                "pricing_pending".to_string(),
                "pricing_selection_state".to_string(),
                "quarantine_evidence".to_string(),
                "redelivery_log".to_string(),
                "rejected_delivery".to_string(),
                "set_aside_record".to_string(),
                "source_runtime".to_string(),
                "statement".to_string(),
                "statement_import".to_string(),
                "statement_line".to_string(),
                "usage_event".to_string(),
                "window_sample".to_string(),
            ]
        );
    }

    #[test]
    fn sqlite_build_disables_per_allocation_memory_statistics() {
        let conn = Connection::open_in_memory().unwrap();
        let enabled: i64 = conn
            .query_row(
                "SELECT sqlite_compileoption_used('DEFAULT_MEMSTATUS=0')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            enabled, 1,
            "SQLite must not take a global memory-statistics mutex on every allocation"
        );
    }

    #[test]
    fn connection_initialization_keeps_statement_journals_in_memory() {
        // SQLite unlinks a temporary journal as soon as it opens it, so a test
        // cannot observe the file; the setting it reads back is the guard.
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let temp_store: i64 = conn
            .pragma_query_value(None, "temp_store", |row| row.get(0))
            .unwrap();
        assert_eq!(temp_store, 2, "temp_store must be MEMORY");
    }

    #[test]
    fn the_write_ahead_log_shrinks_back_after_a_large_write() {
        // A catch-up tick or a summary rebuild can grow the WAL to hundreds
        // of megabytes. Without a size limit SQLite reuses that file from its
        // start after a checkpoint but never shortens it, so the disk stays
        // used until the store is closed.
        let dir = std::env::temp_dir().join(format!(
            "astrocyte-wal-limit-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db");
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        initialize_connection(&conn).unwrap();
        conn.execute_batch("CREATE TABLE filler(body BLOB NOT NULL)")
            .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for _ in 0..4_000 {
            tx.execute("INSERT INTO filler(body) VALUES (zeroblob(65536))", [])
                .unwrap();
        }
        tx.commit().unwrap();
        let wal = path.with_extension("db-wal");
        let grown = std::fs::metadata(&wal).unwrap().len();
        assert!(
            grown > 2 * STORE_WAL_SIZE_LIMIT_BYTES,
            "the large write grew the WAL to {grown}"
        );
        conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
            .unwrap();
        conn.execute("INSERT INTO filler(body) VALUES (x'00')", [])
            .unwrap();
        conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
            .unwrap();
        let after = std::fs::metadata(&wal).unwrap().len();
        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(
            after <= STORE_WAL_SIZE_LIMIT_BYTES,
            "the WAL stayed at {after} bytes after a checkpoint, above the {STORE_WAL_SIZE_LIMIT_BYTES}-byte limit"
        );
    }

    #[test]
    fn connection_initialization_reads_back_foreign_keys_and_exact_timeout() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        assert_eq!(
            conn.pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.pragma_query_value(None, "busy_timeout", |row| row.get::<_, i64>(0))
                .unwrap(),
            STORE_BUSY_TIMEOUT_MS as i64
        );
        let probe_objects: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'astrocyte_capability_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(probe_objects, 0);
    }

    #[test]
    fn capacity_window_constraints_reject_invalid_rows_without_effect() {
        let conn = open_memory();
        for statement in [
            "INSERT INTO capacity_window
             (provider,account_ref,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('p','a','unknown','unknown',1.0,1)",
            "INSERT INTO capacity_window
             (provider,account_ref,pool_key,slot_kind,extra_ordinal,used_percent,observed_at_ms)
             VALUES ('p','a','extra:ordinal:-1','extra',-1,1.0,1)",
            "INSERT INTO capacity_window
             (provider,account_ref,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('p','a','secondary','primary',1.0,1)",
        ] {
            assert!(conn.execute(statement, []).is_err());
            let count: i64 = conn
                .query_row("SELECT count(*) FROM capacity_window", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn capacity_provider_reading_constraints_reject_invalid_rows_without_effect() {
        let conn = open_memory();
        for statement in [
            "INSERT INTO capacity_provider_reading
             (provider,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('p','unknown','unknown',1.0,1)",
            "INSERT INTO capacity_provider_reading
             (provider,pool_key,slot_kind,extra_ordinal,used_percent,observed_at_ms)
             VALUES ('p','extra:ordinal:-1','extra',-1,1.0,1)",
            "INSERT INTO capacity_provider_reading
             (provider,pool_key,slot_kind,used_percent,observed_at_ms)
             VALUES ('p','secondary','primary',1.0,1)",
        ] {
            assert!(conn.execute(statement, []).is_err());
            let count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM capacity_provider_reading",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn capacity_observation_constraints_reject_invalid_rows_without_effect() {
        let conn = open_memory();
        for statement in [
            "INSERT INTO capacity_observation
             (provider,account_ref,observation_state,observed_at_ms)
             VALUES ('p','unknown','unknown',1)",
            "INSERT INTO capacity_observation
             (provider,account_ref,observation_state,error_class,observed_at_ms)
             VALUES ('p','usable-with-error','usable','error',1)",
            "INSERT INTO capacity_observation
             (provider,account_ref,observation_state,observed_at_ms)
             VALUES ('p','degraded-without-error','degraded',1)",
        ] {
            assert!(conn.execute(statement, []).is_err());
            let count: i64 = conn
                .query_row("SELECT count(*) FROM capacity_observation", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn capacity_runtime_constraints_reject_invalid_rows_without_effect() {
        let conn = open_memory();
        let original: (i64, String, i64, i64) = conn
            .query_row(
                "SELECT desired_generation,runtime_variant,effective_cadence_ms,degraded_entries
                 FROM capacity_runtime WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        for statement in [
            "INSERT INTO capacity_runtime
             (id,desired_generation,runtime_variant,effective_cadence_ms,changed_at_ms)
             VALUES (2,0,'disabled',60000,1)",
            "UPDATE capacity_runtime SET runtime_variant='unknown' WHERE id=1",
            "UPDATE capacity_runtime SET effective_cadence_ms=0 WHERE id=1",
            "UPDATE capacity_runtime SET degraded_entries=-1 WHERE id=1",
            "UPDATE capacity_runtime SET runtime_variant='configured_unavailable',
             changed_at_ms=NULL,enabled_at_ms=1,detail=NULL WHERE id=1",
        ] {
            assert!(conn.execute(statement, []).is_err());
            let stored: (i64, String, i64, i64) = conn
                .query_row(
                    "SELECT desired_generation,runtime_variant,effective_cadence_ms,degraded_entries
                     FROM capacity_runtime WHERE id=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(stored, original);
            let count: i64 = conn
                .query_row("SELECT count(*) FROM capacity_runtime", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 1);
        }
    }

    #[test]
    fn usage_event_round_trips_typed_fields_with_zero_measurements() {
        let conn = open_memory();
        let event = sample_event("event-round-trip");
        insert_event(&conn, &event).unwrap();

        assert_eq!(read_event(&conn, &event.event_id), event);
        assert_eq!(count_for_event(&conn, "measurement", &event.event_id), 0);
    }

    #[test]
    fn usage_event_required_fields_reject_null_without_effect() {
        let conn = open_memory();
        // producer_lineage is nullable: a producer that could
        // not observe its own generation stores no lineage rather than a
        // stand-in value, so that field no longer sits in the required list.
        // Acceptance is covered by usage_event_accepts_absent_lineage below.
        for required_field in [
            "event_id",
            "producer",
            "producer_ref",
            "recorded_at_ms",
            "charge_basis",
            "pricing_generation",
        ] {
            let id = format!("required-{required_field}");
            let event_id = (required_field != "event_id").then_some(id.as_str());
            let producer = (required_field != "producer").then_some("producer");
            let lineage = Some("lineage");
            let producer_ref = (required_field != "producer_ref").then_some(required_field);
            let recorded_at_ms = (required_field != "recorded_at_ms").then_some(1000_i64);
            let charge_basis = (required_field != "charge_basis").then_some("metered_api");
            let pricing_generation = (required_field != "pricing_generation").then_some(0_i64);
            let result = conn.execute(
                "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,
                  charge_basis,pricing_generation)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    event_id,
                    producer,
                    lineage,
                    producer_ref,
                    recorded_at_ms,
                    charge_basis,
                    pricing_generation
                ],
            );
            assert!(result.is_err(), "{required_field}");
            let count: i64 = conn
                .query_row("SELECT count(*) FROM usage_event", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0, "{required_field}");
        }
    }

    #[test]
    fn usage_event_accepts_absent_lineage_and_keeps_the_natural_key_unique() {
        let conn = open_memory();
        // A lineage-absent event is storable, and its natural key
        // stays unique even though the table-level UNIQUE constraint treats
        // NULLs as distinct — the partial index is the backstop.
        conn.execute(
            "INSERT INTO usage_event
             (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,
              charge_basis,pricing_generation)
             VALUES ('absent-1','producer',NULL,'ref-a',1000,'metered_api',0)",
            [],
        )
        .unwrap();
        let duplicate = conn.execute(
            "INSERT INTO usage_event
             (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,
              charge_basis,pricing_generation)
             VALUES ('absent-2','producer',NULL,'ref-a',1000,'metered_api',0)",
            [],
        );
        assert!(duplicate.is_err());
        let count: i64 = conn
            .query_row("SELECT count(*) FROM usage_event", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn usage_event_primary_key_rejects_duplicate_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "same-id", "lineage-a", "ref-a", None, None).unwrap();
        let result = insert_minimal_event(&conn, "same-id", "lineage-b", "ref-b", None, None);
        assert!(result.is_err());
        assert_eq!(count_for_event(&conn, "usage_event", "same-id"), 1);
    }

    #[test]
    fn usage_event_natural_key_rejects_duplicate_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event-a", "lineage-a", "same-ref", None, None).unwrap();
        let result = insert_minimal_event(&conn, "event-b", "lineage-a", "same-ref", None, None);
        assert!(result.is_err());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM usage_event
                 WHERE producer='test-producer' AND producer_lineage='lineage-a'
                   AND producer_ref='same-ref'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn usage_event_attempt_ordinal_rejects_negative_without_effect() {
        let conn = open_memory();
        let result = insert_minimal_event(
            &conn,
            "negative-attempt",
            "lineage-a",
            "negative-attempt",
            Some(-1),
            None,
        );
        assert!(result.is_err());
        assert_eq!(count_for_event(&conn, "usage_event", "negative-attempt"), 0);
    }

    #[test]
    fn usage_event_currency_starts_null_without_effect() {
        let conn = open_memory();
        let result = insert_minimal_event(
            &conn,
            "prepriced-event",
            "lineage-a",
            "prepriced-event",
            None,
            Some("USD"),
        );
        assert!(result.is_err());
        assert_eq!(count_for_event(&conn, "usage_event", "prepriced-event"), 0);
    }

    #[test]
    fn usage_event_currency_is_write_once_without_effect() {
        let conn = open_memory();
        insert_minimal_event(
            &conn,
            "currency-event",
            "lineage-a",
            "currency-event",
            None,
            None,
        )
        .unwrap();
        conn.execute(
            "UPDATE usage_event SET currency='JPY' WHERE event_id='currency-event'",
            [],
        )
        .unwrap();

        let result = conn.execute(
            "UPDATE usage_event SET currency='USD' WHERE event_id='currency-event'",
            [],
        );
        assert!(result.is_err());
        let currency: String = conn
            .query_row(
                "SELECT currency FROM usage_event WHERE event_id='currency-event'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(currency, "JPY");
    }

    #[test]
    fn usage_event_payload_update_is_refused_without_effect() {
        let conn = open_memory();
        insert_minimal_event(
            &conn,
            "immutable-event",
            "lineage-a",
            "immutable-event",
            None,
            None,
        )
        .unwrap();
        let result = conn.execute(
            "UPDATE usage_event SET terminal_status='completed'
             WHERE event_id='immutable-event'",
            [],
        );
        assert!(result.is_err());
        let status: Option<String> = conn
            .query_row(
                "SELECT terminal_status FROM usage_event WHERE event_id='immutable-event'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, None);
    }

    #[test]
    fn usage_event_delete_is_refused_without_effect() {
        let conn = open_memory();
        insert_minimal_event(
            &conn,
            "retained-event",
            "lineage-a",
            "retained-event",
            None,
            None,
        )
        .unwrap();
        let result = conn.execute(
            "DELETE FROM usage_event WHERE event_id='retained-event'",
            [],
        );
        assert!(result.is_err());
        assert_eq!(count_for_event(&conn, "usage_event", "retained-event"), 1);
    }

    #[test]
    fn measurement_round_trips_null_zero_and_positive_as_distinct_states() {
        let conn = open_memory();
        let event = sample_event("measurement-states");
        insert_event(&conn, &event).unwrap();
        let measurements = vec![
            Measurement {
                event_id: event.event_id.clone(),
                role: "absent".into(),
                unit: "token".into(),
                kind: "billed".into(),
                source: "usage.absent".into(),
                quantity_units: None,
                quantity_exponent: None,
            },
            Measurement {
                event_id: event.event_id.clone(),
                role: "positive".into(),
                unit: "second".into(),
                kind: "billed".into(),
                source: "usage.duration".into(),
                quantity_units: Some(15),
                quantity_exponent: Some(-1),
            },
            Measurement {
                event_id: event.event_id.clone(),
                role: "zero".into(),
                unit: "credit".into(),
                kind: "billed".into(),
                source: "usage.credits".into(),
                quantity_units: Some(0),
                quantity_exponent: Some(0),
            },
        ];
        for measurement in &measurements {
            insert_measurement(&conn, measurement).unwrap();
        }

        let stored = conn
            .prepare(
                "SELECT event_id,role,unit,kind,source,quantity_units,quantity_exponent
                 FROM measurement WHERE event_id=?1 ORDER BY role,unit",
            )
            .unwrap()
            .query_map([event.event_id.as_str()], |row| {
                Ok(Measurement {
                    event_id: row.get(0)?,
                    role: row.get(1)?,
                    unit: row.get(2)?,
                    kind: row.get(3)?,
                    source: row.get(4)?,
                    quantity_units: row.get(5)?,
                    quantity_exponent: row.get(6)?,
                })
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(stored, measurements);
    }

    #[test]
    fn measurement_required_fields_reject_null_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        for required_field in ["event_id", "role", "unit", "kind", "source"] {
            let event_id = (required_field != "event_id").then_some("event");
            let role = (required_field != "role").then_some(required_field);
            let unit = (required_field != "unit").then_some("token");
            let kind = (required_field != "kind").then_some("billed");
            let source = (required_field != "source").then_some("test");
            let result = conn.execute(
                "INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES (?1,?2,?3,?4,?5,NULL,NULL)",
                params![event_id, role, unit, kind, source],
            );
            assert!(result.is_err(), "{required_field}");
            let count: i64 = conn
                .query_row("SELECT count(*) FROM measurement", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0, "{required_field}");
        }
    }

    #[test]
    fn measurement_quantity_presence_rejects_half_present_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        for (role, units, exponent) in [
            ("units-only", Some(1_i64), None),
            ("exponent-only", None, Some(0_i64)),
        ] {
            let result = conn.execute(
                "INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES ('event',?1,'token','billed','test',?2,?3)",
                params![role, units, exponent],
            );
            assert!(result.is_err());
            let count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM measurement WHERE event_id='event' AND role=?1",
                    [role],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn measurement_quantity_units_rejects_negative_and_real_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        let negative = conn.execute(
            "INSERT INTO measurement VALUES ('event','negative','token','billed','test',-1,0)",
            [],
        );
        let real = conn.execute(
            "INSERT INTO measurement VALUES ('event','real','token','billed','test',1.5,0)",
            [],
        );
        assert!(negative.is_err());
        assert!(real.is_err());
        let count: i64 = conn
            .query_row("SELECT count(*) FROM measurement", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn measurement_quantity_exponent_rejects_noncanonical_values_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        for (role, exponent) in [("too-small", -10.0), ("positive", 1.0), ("real", -1.5)] {
            let result = conn.execute(
                "INSERT INTO measurement VALUES
                 ('event',?1,'token','billed','test',1,?2)",
                params![role, exponent],
            );
            assert!(result.is_err());
            let count: i64 = conn
                .query_row(
                    "SELECT count(*) FROM measurement WHERE event_id='event' AND role=?1",
                    [role],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn measurement_foreign_key_rejects_orphan_without_effect() {
        let conn = open_memory();
        let result = conn.execute(
            "INSERT INTO measurement VALUES
             ('missing-event','input','token','billed','test',1,0)",
            [],
        );
        assert!(result.is_err());
        assert_eq!(count_for_event(&conn, "measurement", "missing-event"), 0);
    }

    #[test]
    fn measurement_identity_rejects_duplicates_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        conn.execute(
            "INSERT INTO measurement VALUES
             ('event','input','token','billed','first',1,0)",
            [],
        )
        .unwrap();
        let result = conn.execute(
            "INSERT INTO measurement VALUES
             ('event','input','token','raw','second',2,0)",
            [],
        );
        assert!(result.is_err());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM measurement
                 WHERE event_id='event' AND role='input' AND unit='token'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn measurement_update_is_refused_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        conn.execute(
            "INSERT INTO measurement VALUES
             ('event','input','token','billed','test',1,0)",
            [],
        )
        .unwrap();
        let result = conn.execute(
            "UPDATE measurement SET quantity_units=2
             WHERE event_id='event' AND role='input' AND unit='token'",
            [],
        );
        assert!(result.is_err());
        let units: i64 = conn
            .query_row(
                "SELECT quantity_units FROM measurement
                 WHERE event_id='event' AND role='input' AND unit='token'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(units, 1);
    }

    #[test]
    fn measurement_delete_is_refused_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "event", "lineage", "event", None, None).unwrap();
        conn.execute(
            "INSERT INTO measurement VALUES
             ('event','input','token','billed','test',1,0)",
            [],
        )
        .unwrap();
        let result = conn.execute(
            "DELETE FROM measurement
             WHERE event_id='event' AND role='input' AND unit='token'",
            [],
        );
        assert!(result.is_err());
        assert_eq!(count_for_event(&conn, "measurement", "event"), 1);
    }

    #[test]
    fn chargeability_transition_required_fields_reject_null_without_effect() {
        let conn = open_memory();
        for required_field in [
            "event_id",
            "from_state",
            "to_state",
            "reason",
            "resolved_by",
            "occurred_at_ms",
        ] {
            let id = format!("required-{required_field}");
            insert_minimal_event(&conn, &id, "lineage", &id, None, None).unwrap();
            let event_id = (required_field != "event_id").then_some(id.as_str());
            let from_state = (required_field != "from_state").then_some("unresolved");
            let to_state = (required_field != "to_state").then_some("billed");
            let reason = (required_field != "reason").then_some("test settlement");
            let resolved_by = (required_field != "resolved_by").then_some("statement");
            let occurred_at_ms = (required_field != "occurred_at_ms").then_some(2000_i64);
            let result = conn.execute(
                "INSERT INTO chargeability_transition
                 (event_id,from_state,to_state,reason,resolved_by,occurred_at_ms)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    event_id,
                    from_state,
                    to_state,
                    reason,
                    resolved_by,
                    occurred_at_ms,
                ],
            );
            assert!(result.is_err(), "{required_field}");
            let count: i64 = conn
                .query_row("SELECT count(*) FROM chargeability_transition", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{required_field}");
        }
    }

    #[test]
    fn chargeability_state_matrix_accepts_only_three_terminal_transitions() {
        let conn = open_memory();
        let states = ["unresolved", "billed", "written_off", "unbillable"];
        let mut accepted = 0;
        for (from_index, from_state) in states.iter().enumerate() {
            for (to_index, to_state) in states.iter().enumerate() {
                let id = format!("state-{from_index}-{to_index}");
                insert_minimal_event(&conn, &id, "lineage", &id, None, None).unwrap();
                let authority = match *to_state {
                    "billed" => "statement",
                    "written_off" | "unbillable" => "operator",
                    _ => "operator",
                };
                let result = insert_transition(&conn, &id, from_state, to_state, authority);
                let should_accept = *from_state == "unresolved" && *to_state != "unresolved";
                assert_eq!(result.is_ok(), should_accept, "{from_state} -> {to_state}");
                let count = count_for_event(&conn, "chargeability_transition", &id);
                assert_eq!(
                    count,
                    i64::from(should_accept),
                    "{from_state} -> {to_state}"
                );
                accepted += usize::from(result.is_ok());
            }
        }
        assert_eq!(accepted, 3);
    }

    #[test]
    fn chargeability_authority_matrix_accepts_only_five_legal_pairs() {
        let conn = open_memory();
        let authorities = ["correction", "later_record", "statement", "operator"];
        let targets = ["unresolved", "billed", "written_off", "unbillable"];
        let mut accepted = 0;
        for (authority_index, authority) in authorities.iter().enumerate() {
            for (target_index, target) in targets.iter().enumerate() {
                let id = format!("authority-{authority_index}-{target_index}");
                insert_minimal_event(&conn, &id, "lineage", &id, None, None).unwrap();
                let result = insert_transition(&conn, &id, "unresolved", target, authority);
                let should_accept = matches!(
                    (*authority, *target),
                    ("correction", "written_off")
                        | ("later_record", "billed")
                        | ("statement", "billed")
                        | ("operator", "written_off")
                        | ("operator", "unbillable")
                );
                assert_eq!(result.is_ok(), should_accept, "{authority} -> {target}");
                let count = count_for_event(&conn, "chargeability_transition", &id);
                assert_eq!(count, i64::from(should_accept), "{authority} -> {target}");
                accepted += usize::from(result.is_ok());
            }
        }
        assert_eq!(accepted, 5);
    }

    #[test]
    fn chargeability_state_vocabulary_rejects_unknown_without_effect() {
        let conn = open_memory();
        for (id, from_state, to_state) in [
            ("unknown-from", "pending", "billed"),
            ("unknown-to", "unresolved", "refunded"),
        ] {
            insert_minimal_event(&conn, id, "lineage", id, None, None).unwrap();
            let result = insert_transition(&conn, id, from_state, to_state, "statement");
            assert!(result.is_err());
            assert_eq!(count_for_event(&conn, "chargeability_transition", id), 0);
        }
    }

    #[test]
    fn chargeability_authority_vocabulary_rejects_unknown_without_effect() {
        let conn = open_memory();
        insert_minimal_event(
            &conn,
            "unknown-authority",
            "lineage",
            "authority",
            None,
            None,
        )
        .unwrap();
        let result = insert_transition(
            &conn,
            "unknown-authority",
            "unresolved",
            "billed",
            "redelivery",
        );
        assert!(result.is_err());
        assert_eq!(
            count_for_event(&conn, "chargeability_transition", "unknown-authority"),
            0
        );
    }

    #[test]
    fn chargeability_transition_foreign_key_rejects_orphan_without_effect() {
        let conn = open_memory();
        let result = insert_transition(&conn, "missing-event", "unresolved", "billed", "statement");
        assert!(result.is_err());
        assert_eq!(
            count_for_event(&conn, "chargeability_transition", "missing-event"),
            0
        );
    }

    #[test]
    fn settlement_is_terminal_per_event_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "settled-event", "lineage", "settled", None, None).unwrap();
        insert_transition(&conn, "settled-event", "unresolved", "billed", "statement").unwrap();
        let result = insert_transition(
            &conn,
            "settled-event",
            "unresolved",
            "written_off",
            "operator",
        );
        assert!(result.is_err());
        assert_eq!(
            count_for_event(&conn, "chargeability_transition", "settled-event"),
            1
        );
    }

    #[test]
    fn chargeability_transition_update_is_refused_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "transition", "lineage", "transition", None, None).unwrap();
        insert_transition(&conn, "transition", "unresolved", "billed", "statement").unwrap();
        let result = conn.execute(
            "UPDATE chargeability_transition SET reason='rewritten'
             WHERE event_id='transition'",
            [],
        );
        assert!(result.is_err());
        let reason: String = conn
            .query_row(
                "SELECT reason FROM chargeability_transition WHERE event_id='transition'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reason, "test settlement");
    }

    #[test]
    fn chargeability_transition_delete_is_refused_without_effect() {
        let conn = open_memory();
        insert_minimal_event(&conn, "transition", "lineage", "transition", None, None).unwrap();
        insert_transition(&conn, "transition", "unresolved", "billed", "statement").unwrap();
        let result = conn.execute(
            "DELETE FROM chargeability_transition WHERE event_id='transition'",
            [],
        );
        assert!(result.is_err());
        assert_eq!(
            count_for_event(&conn, "chargeability_transition", "transition"),
            1
        );
    }

    fn seed_pricing_target(conn: &Connection, event_id: &str, roles: &[&str]) {
        insert_minimal_event(conn, event_id, "pricing-lineage", event_id, None, None).unwrap();
        for role in roles {
            conn.execute(
                "INSERT INTO measurement
                 (event_id,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES (?1,?2,'token','billed','test',1000000,-6)",
                params![event_id, role],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE usage_event SET currency='USD',pricing_generation=1 WHERE event_id=?1",
            [event_id],
        )
        .unwrap();
    }

    fn insert_priced_charge(
        conn: &Connection,
        charge_id: &str,
        event_id: &str,
        role: &str,
        generation: i64,
        currency: &str,
        supersedes: Option<&str>,
    ) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO priced_charge (
                 charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
                 basis_covered,currency,charge_nanos,rate_units,rate_exponent,
                 floor_state,catalog_source,resolved_at_ms,catalog_entry,
                 supersedes,origin,recorded_at_ms
             ) VALUES (
                 ?1,?2,?3,'token',?4,'priced','metered_api',0,?5,3000,3000,9,
                 'unknown','test.catalog',1000,
                 '{\"units\":3000,\"exponent\":9,\"currency\":\"USD\"}',
                 ?6,'pricing_pass',2000
             )",
            params![charge_id, event_id, role, generation, currency, supersedes],
        )
    }

    #[test]
    fn usage_event_pricing_projection_guards_reject_invalid_steps_without_effect() {
        let conn = open_memory();
        for (id, basis, generation) in [
            ("unknown-basis", "guess", 0_i64),
            ("prepriced-generation", "metered_api", 1_i64),
        ] {
            let result = conn.execute(
                "INSERT INTO usage_event
                 (event_id,producer,producer_lineage,producer_ref,recorded_at_ms,
                  charge_basis,pricing_generation)
                 VALUES (?1,'producer','lineage',?1,1000,?2,?3)",
                params![id, basis, generation],
            );
            assert!(result.is_err(), "{id}");
            assert_eq!(count_for_event(&conn, "usage_event", id), 0, "{id}");
        }

        insert_minimal_event(&conn, "generation", "lineage", "generation", None, None).unwrap();
        for next in [2_i64, -1] {
            let result = conn.execute(
                "UPDATE usage_event SET pricing_generation=?1 WHERE event_id='generation'",
                [next],
            );
            assert!(result.is_err(), "generation {next}");
            let stored: i64 = conn
                .query_row(
                    "SELECT pricing_generation FROM usage_event WHERE event_id='generation'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored, 0);
        }
        conn.execute(
            "UPDATE usage_event
             SET charge_basis='subscription_included',pricing_generation=1
             WHERE event_id='generation'",
            [],
        )
        .unwrap();
        let stored: (String, i64) = conn
            .query_row(
                "SELECT charge_basis,pricing_generation FROM usage_event
                 WHERE event_id='generation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("subscription_included".into(), 1));
    }

    #[test]
    fn priced_charge_outcome_matrix_rejects_malformed_rows_without_effect() {
        let conn = open_memory();
        seed_pricing_target(&conn, "outcomes", &["input"]);
        for statement in [
            "INSERT INTO priced_charge
              (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
               basis_covered,currency,charge_nanos,origin,recorded_at_ms)
              VALUES ('priced-without-rate','outcomes','input','token',1,'priced',
                      'metered_api',0,'USD',1,'pricing_pass',1)",
            "INSERT INTO priced_charge
              (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
               basis_covered,currency,charge_nanos,rate_units,rate_exponent,
               resolved_at_ms,catalog_entry,origin,recorded_at_ms)
              VALUES ('priced-without-source','outcomes','input','token',1,'priced',
                      'metered_api',0,'USD',1,1,9,1,'{}','pricing_pass',1)",
            "INSERT INTO priced_charge
             (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
              basis_covered,currency,charge_nanos,origin,recorded_at_ms)
             VALUES ('bad-covered','outcomes','input','token',1,'covered',
                     'subscription_included',1,'USD',1,'pricing_pass',1)",
            "INSERT INTO priced_charge
             (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
              basis_covered,currency,charge_nanos,unpriced_reason,origin,recorded_at_ms)
             VALUES ('bad-unpriced','outcomes','input','token',1,'unpriced',
                     'metered_api',0,NULL,0,'no_rate','pricing_pass',1)",
            "INSERT INTO priced_charge
             (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
              basis_covered,currency,charge_nanos,unpriced_reason,origin,recorded_at_ms)
             VALUES ('bad-unknown-variant','outcomes','input','token',1,'unpriced',
                     'metered_api',0,NULL,NULL,'rate_variant_unknown','pricing_pass',1)",
            "INSERT INTO priced_charge
             (charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
              basis_covered,currency,charge_nanos,rate_units,rate_exponent,
              resolved_at_ms,catalog_entry,origin,recorded_at_ms)
             VALUES ('bad-exponent','outcomes','input','token',1,'priced',
                     'metered_api',0,'USD',1,1,2,1,'{}','pricing_pass',1)",
        ] {
            assert!(conn.execute(statement, []).is_err());
            assert_eq!(count_for_event(&conn, "priced_charge", "outcomes"), 0);
        }
    }

    #[test]
    fn priced_charge_generation_must_match_event_without_effect() {
        let conn = open_memory();
        seed_pricing_target(&conn, "generation-mismatch", &["input"]);
        assert!(insert_priced_charge(
            &conn,
            "future-charge",
            "generation-mismatch",
            "input",
            2,
            "USD",
            None,
        )
        .is_err());
        assert_eq!(
            count_for_event(&conn, "priced_charge", "generation-mismatch"),
            0
        );
    }

    #[test]
    fn priced_charge_foreign_keys_reject_orphan_and_mismatched_currency_without_effect() {
        let conn = open_memory();
        seed_pricing_target(&conn, "currency", &["input"]);
        assert!(
            insert_priced_charge(&conn, "wrong-currency", "currency", "input", 1, "EUR", None,)
                .is_err()
        );
        assert!(insert_priced_charge(
            &conn,
            "missing-measurement",
            "currency",
            "output",
            1,
            "USD",
            None,
        )
        .is_err());
        assert_eq!(count_for_event(&conn, "priced_charge", "currency"), 0);
    }

    #[test]
    fn priced_charge_chain_rejects_second_root_cross_measurement_and_fork_without_effect() {
        let conn = open_memory();
        seed_pricing_target(&conn, "chain", &["input", "output"]);
        insert_priced_charge(&conn, "root", "chain", "input", 1, "USD", None).unwrap();
        conn.execute(
            "UPDATE usage_event SET pricing_generation=2 WHERE event_id='chain'",
            [],
        )
        .unwrap();

        assert!(
            insert_priced_charge(&conn, "second-root", "chain", "input", 2, "USD", None).is_err()
        );
        assert!(insert_priced_charge(
            &conn,
            "cross-measurement",
            "chain",
            "output",
            2,
            "USD",
            Some("root"),
        )
        .is_err());
        insert_priced_charge(&conn, "child", "chain", "input", 2, "USD", Some("root")).unwrap();
        conn.execute(
            "UPDATE usage_event SET pricing_generation=3 WHERE event_id='chain'",
            [],
        )
        .unwrap();
        assert!(
            insert_priced_charge(&conn, "fork", "chain", "input", 3, "USD", Some("root"),).is_err()
        );
        assert_eq!(count_for_event(&conn, "priced_charge", "chain"), 2);
    }

    #[test]
    fn priced_charge_update_and_delete_are_refused_without_effect() {
        let conn = open_memory();
        seed_pricing_target(&conn, "append-only", &["input"]);
        insert_priced_charge(
            &conn,
            "retained-charge",
            "append-only",
            "input",
            1,
            "USD",
            None,
        )
        .unwrap();
        assert!(conn
            .execute(
                "UPDATE priced_charge SET charge_nanos=7 WHERE charge_id='retained-charge'",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "DELETE FROM priced_charge WHERE charge_id='retained-charge'",
                [],
            )
            .is_err());
        let amount: i64 = conn
            .query_row(
                "SELECT charge_nanos FROM priced_charge WHERE charge_id='retained-charge'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(amount, 3000);
    }
}
