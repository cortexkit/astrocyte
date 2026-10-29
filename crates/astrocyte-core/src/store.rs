//! Astrocyte v2 storage schema and connection prerequisites.
//!
//! The v2 migration chain intentionally starts at version 1. The replaced store
//! contains disposable data, so no v1 schema or data migration is retained.

use std::time::Duration;

use cortexkit_store::{Migration, MigrationOutcome, SqliteStore, StoreError};
use rusqlite::Connection;

const STORE_BUSY_TIMEOUT_MS: u64 = 5_000;

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
pub fn initialize_connection(conn: &Connection) -> Result<(), StoreInitError> {
    conn.busy_timeout(Duration::from_millis(STORE_BUSY_TIMEOUT_MS))?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
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
    let mut outcome = store.migrate("astrocyte", MIGRATIONS)?;
    outcome.applied += applied;
    Ok(outcome)
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
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        statements: r#"
CREATE TABLE usage_event (
    event_id           TEXT NOT NULL PRIMARY KEY,
    producer           TEXT NOT NULL,
    -- NULL when the producer could not observe its own generation. The
    -- absence is stored, not smoothed over, so a reader can see the identity
    -- is weaker rather than having to infer it.
    producer_lineage   TEXT,
    producer_ref       TEXT NOT NULL,
    prior_event_ref    TEXT,

    harness            TEXT,
    project_root       TEXT,
    session            TEXT,
    run_ref            TEXT,
    work_class         TEXT,

    occurred_at_ms     INTEGER,
    emitted_at_ms      INTEGER,
    recorded_at_ms     INTEGER NOT NULL,
    terminal_status    TEXT,
    attempt_ordinal    INTEGER,

    vendor_request_id  TEXT,
    provider           TEXT,
    model              TEXT,
    account_ref        TEXT,

    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    pricing_generation INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation >= 0),
    currency           TEXT,

    CONSTRAINT usage_event_natural_key
        UNIQUE (producer, producer_lineage, producer_ref),
    CONSTRAINT usage_event_attempt_ordinal_nonnegative
        CHECK (
            attempt_ordinal IS NULL
            OR (typeof(attempt_ordinal) = 'integer' AND attempt_ordinal >= 0)
        )
);

-- The table UNIQUE constraint cannot cover a lineage-absent key: SQLite
-- treats NULLs as distinct. This index keeps the natural key unique when the
-- lineage is absent, which is the case the ingest dedup lookup matches with
-- null-safe `IS` equality.
CREATE UNIQUE INDEX usage_event_natural_key_lineage_absent
    ON usage_event (producer, producer_ref)
    WHERE producer_lineage IS NULL;

CREATE INDEX usage_event_prior_event_lookup
    ON usage_event (producer, producer_lineage, prior_event_ref)
    WHERE prior_event_ref IS NOT NULL;

-- Composite charge foreign keys make an event's write-once currency structural.
CREATE UNIQUE INDEX usage_event_currency_identity
    ON usage_event (event_id, currency);

CREATE TABLE measurement (
    event_id           TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    kind               TEXT NOT NULL,
    source             TEXT NOT NULL,
    quantity_units     INTEGER,
    quantity_exponent  INTEGER,

    PRIMARY KEY (event_id, role, unit),
    CONSTRAINT measurement_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT,
    CONSTRAINT measurement_quantity_presence
        CHECK (
            (quantity_units IS NULL AND quantity_exponent IS NULL)
            OR (quantity_units IS NOT NULL AND quantity_exponent IS NOT NULL)
        ),
    CONSTRAINT measurement_quantity_units_nonnegative_integer
        CHECK (
            quantity_units IS NULL
            OR (typeof(quantity_units) = 'integer' AND quantity_units >= 0)
        ),
    CONSTRAINT measurement_quantity_exponent_canonical
        CHECK (
            quantity_exponent IS NULL
            OR (
                typeof(quantity_exponent) = 'integer'
                AND quantity_exponent BETWEEN -9 AND 0
            )
        )
);

CREATE TABLE chargeability_transition (
    event_id        TEXT NOT NULL PRIMARY KEY,
    from_state      TEXT NOT NULL,
    to_state        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    resolved_by     TEXT NOT NULL,
    occurred_at_ms  INTEGER NOT NULL,

    CONSTRAINT chargeability_transition_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT,
    CONSTRAINT chargeability_transition_starts_unresolved
        CHECK (from_state = 'unresolved'),
    CONSTRAINT chargeability_transition_terminal_target
        CHECK (to_state IN ('billed', 'written_off', 'unbillable')),
    CONSTRAINT chargeability_transition_authority_vocabulary
        CHECK (resolved_by IN ('correction', 'later_record', 'statement', 'operator')),
    CONSTRAINT chargeability_transition_authority_target
        CHECK (
            (to_state = 'billed' AND resolved_by IN ('later_record', 'statement'))
            OR (to_state = 'written_off' AND resolved_by IN ('correction', 'operator'))
            OR (to_state = 'unbillable' AND resolved_by = 'operator')
        )
);

-- These four tables preserve the capacity axis at its v9 end state. The
-- configured capacity runtime treats its generation as opaque durable chronology;
-- it is not tied to a desired-configuration table.
CREATE TABLE capacity_window (
    provider          TEXT NOT NULL,
    api_provider      TEXT,
    account_ref       TEXT NOT NULL,
    pool_key          TEXT NOT NULL,
    slot_kind         TEXT NOT NULL CHECK (slot_kind IN ('primary','secondary','tertiary','extra','legacy_extra')),
    external_id       TEXT,
    title             TEXT,
    extra_ordinal     INTEGER CHECK (extra_ordinal IS NULL OR extra_ordinal>=0),
    legacy_pool_key   TEXT,
    used_percent      REAL NOT NULL,
    raw_used_percent  REAL,
    resets_at         TEXT,
    window_minutes    INTEGER,
    used_count        INTEGER,
    total_count       INTEGER,
    source            TEXT,
    fetched_at        TEXT,
    observed_at_ms    INTEGER NOT NULL,
    stale_since       TEXT,
    stale_class       TEXT,
    PRIMARY KEY (provider,account_ref,pool_key),
    CHECK (
        (slot_kind IN ('primary','secondary','tertiary')
         AND pool_key=slot_kind AND external_id IS NULL AND title IS NULL
         AND extra_ordinal IS NULL AND legacy_pool_key IS NULL)
        OR
        (slot_kind='extra' AND legacy_pool_key IS NULL AND (
            (external_id IS NOT NULL AND extra_ordinal IS NULL
             AND pool_key=('extra:' || CAST(length(CAST(external_id AS BLOB)) AS TEXT) || ':' || external_id))
            OR
            (external_id IS NULL AND extra_ordinal IS NOT NULL
             AND pool_key=('extra:ordinal:' || CAST(extra_ordinal AS TEXT)))
        ))
        OR
        (slot_kind='legacy_extra' AND external_id IS NULL AND title IS NULL
         AND extra_ordinal IS NULL AND legacy_pool_key IS NOT NULL
         AND pool_key=('legacy:' || CAST(length(CAST(legacy_pool_key AS BLOB)) AS TEXT) || ':' || legacy_pool_key))
    )
);

CREATE TABLE capacity_provider_reading (
    provider          TEXT NOT NULL,
    api_provider      TEXT,
    pool_key          TEXT NOT NULL,
    slot_kind         TEXT NOT NULL CHECK (slot_kind IN ('primary','secondary','tertiary','extra')),
    external_id       TEXT,
    title             TEXT,
    extra_ordinal     INTEGER CHECK (extra_ordinal IS NULL OR extra_ordinal>=0),
    used_percent      REAL NOT NULL,
    raw_used_percent  REAL,
    resets_at         TEXT,
    window_minutes    INTEGER,
    used_count        INTEGER,
    total_count       INTEGER,
    source            TEXT,
    fetched_at        TEXT,
    observed_at_ms    INTEGER NOT NULL,
    PRIMARY KEY (provider,pool_key),
    CHECK (
        (slot_kind IN ('primary','secondary','tertiary')
         AND pool_key=slot_kind AND external_id IS NULL AND title IS NULL AND extra_ordinal IS NULL)
        OR
        (slot_kind='extra' AND (
            (external_id IS NOT NULL AND extra_ordinal IS NULL
             AND pool_key=('extra:' || CAST(length(CAST(external_id AS BLOB)) AS TEXT) || ':' || external_id))
            OR
            (external_id IS NULL AND extra_ordinal IS NOT NULL
             AND pool_key=('extra:ordinal:' || CAST(extra_ordinal AS TEXT)))
        ))
    )
);

CREATE TABLE capacity_observation (
    provider          TEXT NOT NULL,
    api_provider      TEXT,
    account_ref       TEXT NOT NULL,
    observation_state TEXT NOT NULL CHECK (observation_state IN ('usable','degraded')),
    error_class       TEXT,
    error_detail      TEXT,
    source            TEXT,
    fetched_at        TEXT,
    observed_at_ms    INTEGER NOT NULL,
    PRIMARY KEY(provider,account_ref),
    CHECK (
        (observation_state='usable' AND error_class IS NULL AND error_detail IS NULL)
        OR
        (observation_state='degraded' AND error_class IS NOT NULL AND typeof(error_class)='text')
    )
);

CREATE TABLE capacity_runtime (
    id                    INTEGER PRIMARY KEY CHECK (id=1),
    desired_generation    INTEGER NOT NULL,
    runtime_variant       TEXT NOT NULL CHECK (runtime_variant IN ('disabled','configured_unavailable','active')),
    effective_cadence_ms  INTEGER NOT NULL CHECK (effective_cadence_ms>0),
    changed_at_ms         INTEGER,
    enabled_at_ms         INTEGER,
    last_attempt_ms       INTEGER,
    last_success_ms       INTEGER,
    degraded_entries      INTEGER NOT NULL DEFAULT 0 CHECK (degraded_entries>=0),
    detail                TEXT,
    CHECK (
        (runtime_variant='disabled' AND changed_at_ms IS NOT NULL
         AND enabled_at_ms IS NULL AND last_attempt_ms IS NULL AND last_success_ms IS NULL
         AND degraded_entries=0 AND detail IS NULL)
        OR
        (runtime_variant='configured_unavailable' AND changed_at_ms IS NULL
         AND enabled_at_ms IS NOT NULL AND last_success_ms IS NULL AND detail IS NOT NULL)
        OR
        (runtime_variant='active' AND changed_at_ms IS NULL AND enabled_at_ms IS NOT NULL
         AND last_success_ms IS NOT NULL)
    )
);
INSERT INTO capacity_runtime (
    id, desired_generation, runtime_variant, effective_cadence_ms, changed_at_ms,
    enabled_at_ms, last_attempt_ms, last_success_ms, degraded_entries, detail
) VALUES (
    1, 0, 'disabled', 60000,
    CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER),
    NULL, NULL, NULL, 0, NULL
);

-- An event begins without a currency. Pricing establishes the projection by
-- updating it when the first priced or subscription-covered measurement lands.
CREATE TRIGGER usage_event_currency_starts_null
BEFORE INSERT ON usage_event
WHEN NEW.currency IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'usage_event_currency_starts_null');
END;

CREATE TRIGGER usage_event_pricing_generation_starts_zero
BEFORE INSERT ON usage_event
WHEN NEW.pricing_generation <> 0
BEGIN
    SELECT RAISE(ABORT, 'usage_event_pricing_generation_starts_zero');
END;

-- Producer facts are immutable. Pricing generation advances one act at a time;
-- basis corrections advance it with the charge-basis projection. Currency may
-- move from NULL to one value exactly once.
CREATE TRIGGER usage_event_update_guard
BEFORE UPDATE ON usage_event
WHEN NOT (
    NEW.event_id IS OLD.event_id
    AND NEW.producer IS OLD.producer
    AND NEW.producer_lineage IS OLD.producer_lineage
    AND NEW.producer_ref IS OLD.producer_ref
    AND NEW.prior_event_ref IS OLD.prior_event_ref
    AND NEW.harness IS OLD.harness
    AND NEW.project_root IS OLD.project_root
    AND NEW.session IS OLD.session
    AND NEW.run_ref IS OLD.run_ref
    AND NEW.work_class IS OLD.work_class
    AND NEW.occurred_at_ms IS OLD.occurred_at_ms
    AND NEW.emitted_at_ms IS OLD.emitted_at_ms
    AND NEW.recorded_at_ms IS OLD.recorded_at_ms
    AND NEW.terminal_status IS OLD.terminal_status
    AND NEW.attempt_ordinal IS OLD.attempt_ordinal
    AND NEW.vendor_request_id IS OLD.vendor_request_id
    AND NEW.provider IS OLD.provider
    AND NEW.model IS OLD.model
    AND NEW.account_ref IS OLD.account_ref
    AND (
        NEW.charge_basis IS OLD.charge_basis
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.pricing_generation = OLD.pricing_generation
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.currency IS OLD.currency
        OR (OLD.currency IS NULL AND NEW.currency IS NOT NULL)
    )
)
BEGIN
    SELECT RAISE(ABORT, 'usage_event_update_guard');
END;

CREATE TRIGGER usage_event_no_delete
BEFORE DELETE ON usage_event
BEGIN
    SELECT RAISE(ABORT, 'usage_event_no_delete');
END;

CREATE TRIGGER measurement_no_update
BEFORE UPDATE ON measurement
BEGIN
    SELECT RAISE(ABORT, 'measurement_no_update');
END;

CREATE TRIGGER measurement_no_delete
BEFORE DELETE ON measurement
BEGIN
    SELECT RAISE(ABORT, 'measurement_no_delete');
END;

CREATE TRIGGER chargeability_transition_no_update
BEFORE UPDATE ON chargeability_transition
BEGIN
    SELECT RAISE(ABORT, 'chargeability_transition_no_update');
END;

CREATE TRIGGER chargeability_transition_no_delete
BEFORE DELETE ON chargeability_transition
BEGIN
    SELECT RAISE(ABORT, 'chargeability_transition_no_delete');
END;

CREATE TABLE quarantine_evidence (
    event_id           TEXT NOT NULL,
    target_event_id    TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    stored_quantity    TEXT NOT NULL
        CHECK (json_valid(stored_quantity) AND json_type(stored_quantity) = 'object'),
    incoming_quantity  TEXT NOT NULL
        CHECK (json_valid(incoming_quantity) AND json_type(incoming_quantity) = 'object'),
    detected_at_ms     INTEGER NOT NULL
        CHECK (typeof(detected_at_ms) = 'integer' AND detected_at_ms >= 0),

    PRIMARY KEY (event_id, target_event_id, role, unit),
    CONSTRAINT quarantine_evidence_distinct_events CHECK (event_id <> target_event_id),
    CONSTRAINT quarantine_evidence_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT,
    CONSTRAINT quarantine_evidence_target_fk
        FOREIGN KEY (target_event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT
);

CREATE TABLE rejected_delivery (
    producer           TEXT NOT NULL,
    producer_lineage   TEXT,
    producer_ref       TEXT NOT NULL,
    incoming_payload   TEXT NOT NULL
        CHECK (json_valid(incoming_payload) AND json_type(incoming_payload) = 'object'),
    stored_event_id    TEXT NOT NULL,
    detected_at_ms     INTEGER NOT NULL
        CHECK (typeof(detected_at_ms) = 'integer' AND detected_at_ms >= 0),

    CONSTRAINT rejected_delivery_stored_event_fk
        FOREIGN KEY (stored_event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT
);

CREATE TABLE redelivery_log (
    producer           TEXT NOT NULL,
    producer_lineage   TEXT,
    producer_ref       TEXT NOT NULL,
    event_id           TEXT NOT NULL,
    detected_at_ms     INTEGER NOT NULL
        CHECK (typeof(detected_at_ms) = 'integer' AND detected_at_ms >= 0),

    CONSTRAINT redelivery_log_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT
);

CREATE TABLE adjustment (
    adjustment_id  TEXT NOT NULL PRIMARY KEY,
    event_id       TEXT NOT NULL,
    currency       TEXT NOT NULL,
    amount_nanos   INTEGER NOT NULL
        CHECK (typeof(amount_nanos) = 'integer'),
    reason         TEXT NOT NULL
        CHECK (reason IN ('write_off', 'vendor_restatement', 'our_repricing', 'refund', 'goodwill')),
    occurred_at_ms INTEGER NOT NULL
        CHECK (typeof(occurred_at_ms) = 'integer' AND occurred_at_ms >= 0),
    recorded_at_ms INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),
    origin         TEXT NOT NULL
        CHECK (origin IN ('operator', 'statement', 'correction_ingest')),
    operator       TEXT,
    note           TEXT,

    CONSTRAINT adjustment_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT
);

CREATE TABLE priced_charge (
    charge_id          TEXT NOT NULL PRIMARY KEY,
    event_id           TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    pricing_generation INTEGER NOT NULL
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation > 0),
    outcome            TEXT NOT NULL
        CHECK (outcome IN ('priced', 'covered', 'unpriced')),
    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    basis_covered      INTEGER NOT NULL
        CHECK (basis_covered IN (0, 1)),
    currency           TEXT,
    charge_nanos       INTEGER,
    unpriced_reason    TEXT
        CHECK (
            unpriced_reason IS NULL
            OR unpriced_reason IN (
                'undated_event', 'charge_basis_unknown', 'no_subscription_in_force',
                'missing_catalog_identity', 'catalog_pre_history', 'no_rate',
                'no_catalog_coverage', 'catalog_unconfigured', 'catalog_unknown_charge_basis',
                'missing_quantity', 'rate_tiered', 'rate_time_banded',
                'rate_expired', 'rate_withheld',
                'rate_uncertain', 'stated_zero_requires_policy',
                'arithmetic_out_of_range'
            )
        ),
    rate_units         INTEGER,
    rate_exponent      INTEGER,
    floor_units        INTEGER,
    floor_exponent     INTEGER,
    -- NULL means this charge was not priced from a fusiform response; pre-history
    -- charges are priced from our own preserved rows instead.
    catalog_source     TEXT,
    resolved_at_ms     INTEGER
        CHECK (
            resolved_at_ms IS NULL
            OR (typeof(resolved_at_ms) = 'integer' AND resolved_at_ms >= 0)
        ),
    catalog_entry      TEXT
        CHECK (
            catalog_entry IS NULL
            OR (json_valid(catalog_entry) AND json_type(catalog_entry) = 'object')
        ),
    supersedes         TEXT UNIQUE,
    origin             TEXT NOT NULL
        CHECK (origin IN ('ingest', 'reprice', 'basis_correction')),
    recorded_at_ms     INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),

    UNIQUE (charge_id, event_id, role, unit),
    UNIQUE (event_id, role, unit, pricing_generation),
    CONSTRAINT priced_charge_measurement_fk
        FOREIGN KEY (event_id, role, unit)
        REFERENCES measurement(event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_currency_fk
        FOREIGN KEY (event_id, currency)
        REFERENCES usage_event(event_id, currency) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_supersedes_same_measurement_fk
        FOREIGN KEY (supersedes, event_id, role, unit)
        REFERENCES priced_charge(charge_id, event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> charge_id),
    CONSTRAINT priced_charge_floor_pair
        CHECK (
            (floor_units IS NULL AND floor_exponent IS NULL)
            OR (floor_units IS NOT NULL AND floor_exponent = 9)
        ),
    CONSTRAINT priced_charge_outcome_shape
        CHECK (
            (
                outcome = 'priced'
                AND charge_basis = 'metered_api'
                AND basis_covered = 0
                AND currency IS NOT NULL
                AND typeof(charge_nanos) = 'integer'
                AND unpriced_reason IS NULL
                AND typeof(rate_units) = 'integer'
                AND rate_exponent = 9
                AND catalog_source IS NOT NULL
                AND resolved_at_ms IS NOT NULL
                AND catalog_entry IS NOT NULL
            )
            OR (
                outcome = 'covered'
                AND charge_basis = 'subscription_included'
                AND basis_covered = 1
                AND currency IS NOT NULL
                AND charge_nanos = 0
                AND unpriced_reason IS NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_units IS NULL
                AND floor_exponent IS NULL
                AND catalog_source IS NULL
                AND resolved_at_ms IS NULL
                AND catalog_entry IS NULL
            )
            OR (
                outcome = 'unpriced'
                AND basis_covered = 0
                AND currency IS NULL
                AND charge_nanos IS NULL
                AND unpriced_reason IS NOT NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_units IS NULL
                AND floor_exponent IS NULL
            )
        )
);

CREATE UNIQUE INDEX priced_charge_one_chain_root
    ON priced_charge (event_id, role, unit)
    WHERE supersedes IS NULL;

CREATE TRIGGER priced_charge_no_update
BEFORE UPDATE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_update');
END;

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

CREATE TRIGGER priced_charge_matches_event_generation_and_basis
BEFORE INSERT ON priced_charge
WHEN NOT EXISTS (
    SELECT 1 FROM usage_event event
    WHERE event.event_id = NEW.event_id
      AND event.pricing_generation = NEW.pricing_generation
      AND event.charge_basis = NEW.charge_basis
)
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_matches_event_generation_and_basis');
END;

CREATE TABLE statement (
    statement_id     TEXT NOT NULL PRIMARY KEY,
    provider         TEXT NOT NULL CHECK (length(provider) > 0),
    account_ref      TEXT NOT NULL CHECK (length(account_ref) > 0),
    period_start_ms  INTEGER NOT NULL
        CHECK (typeof(period_start_ms) = 'integer' AND period_start_ms >= 0),
    period_end_ms    INTEGER NOT NULL
        CHECK (typeof(period_end_ms) = 'integer' AND period_end_ms > period_start_ms),
    currency         TEXT NOT NULL CHECK (length(currency) > 0),
    amount_exponent  INTEGER NOT NULL
        CHECK (typeof(amount_exponent) = 'integer' AND amount_exponent BETWEEN 0 AND 9),

    CONSTRAINT statement_identity
        UNIQUE (provider, account_ref, period_start_ms, period_end_ms)
);

CREATE TABLE statement_line (
    statement_line_id  TEXT NOT NULL PRIMARY KEY,
    statement_id       TEXT NOT NULL,
    line_id            TEXT NOT NULL CHECK (length(line_id) > 0),
    occurred_at_ms     INTEGER NOT NULL
        CHECK (typeof(occurred_at_ms) = 'integer' AND occurred_at_ms >= 0),
    description        TEXT NOT NULL,
    quantity           INTEGER NOT NULL
        CHECK (typeof(quantity) = 'integer' AND quantity >= 0),
    unit               TEXT NOT NULL CHECK (length(unit) > 0),
    amount_minor       INTEGER NOT NULL CHECK (typeof(amount_minor) = 'integer'),
    currency           TEXT NOT NULL CHECK (length(currency) > 0),
    correlation_ref    TEXT,
    match_outcome      TEXT NOT NULL
        CHECK (match_outcome IN (
            'unmatched', 'settled', 'ambiguous', 'already-settled',
            'unpriced_target', 'currency_mismatch'
        )),
    matched_event_id   TEXT,

    CONSTRAINT statement_line_identity UNIQUE (statement_id, line_id),
    CONSTRAINT statement_line_statement_fk
        FOREIGN KEY (statement_id) REFERENCES statement(statement_id) ON DELETE RESTRICT,
    CONSTRAINT statement_line_event_fk
        FOREIGN KEY (matched_event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT,
    CONSTRAINT statement_line_match_shape CHECK (
        (match_outcome IN ('settled', 'unpriced_target', 'currency_mismatch')
            AND matched_event_id IS NOT NULL)
        OR (match_outcome IN ('unmatched', 'ambiguous') AND matched_event_id IS NULL)
        OR match_outcome = 'already-settled'
    )
);

CREATE TABLE match_attempt (
    attempt_id          TEXT NOT NULL PRIMARY KEY,
    statement_line_id   TEXT NOT NULL,
    outcome             TEXT NOT NULL
        CHECK (outcome IN (
            'unmatched', 'settled', 'ambiguous', 'already-settled',
            'unpriced_target', 'currency_mismatch'
        )),
    event_id            TEXT,
    ledger_amount_nanos INTEGER CHECK (
        ledger_amount_nanos IS NULL OR typeof(ledger_amount_nanos) = 'integer'
    ),
    vendor_amount_nanos INTEGER NOT NULL CHECK (typeof(vendor_amount_nanos) = 'integer'),
    attempted_at_ms     INTEGER NOT NULL
        CHECK (typeof(attempted_at_ms) = 'integer' AND attempted_at_ms >= 0),

    CONSTRAINT match_attempt_line_fk
        FOREIGN KEY (statement_line_id)
        REFERENCES statement_line(statement_line_id) ON DELETE RESTRICT,
    CONSTRAINT match_attempt_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT,
    CONSTRAINT match_attempt_identified_outcome CHECK (
        (outcome IN ('settled', 'unpriced_target', 'currency_mismatch')
            AND event_id IS NOT NULL AND ledger_amount_nanos IS NOT NULL)
        OR (outcome IN ('unmatched', 'ambiguous')
            AND event_id IS NULL AND ledger_amount_nanos IS NULL)
        OR (outcome = 'already-settled'
            AND ((event_id IS NULL AND ledger_amount_nanos IS NULL)
                OR (event_id IS NOT NULL AND ledger_amount_nanos IS NOT NULL)))
    )
);

CREATE INDEX match_attempt_line_history
    ON match_attempt (statement_line_id, attempted_at_ms, attempt_id);

CREATE TABLE statement_import (
    import_id                TEXT NOT NULL PRIMARY KEY,
    statement_id             TEXT NOT NULL,
    file_sha256              TEXT NOT NULL
        CHECK (length(file_sha256) = 64 AND file_sha256 NOT GLOB '*[^0-9a-f]*'),
    source_path              TEXT NOT NULL CHECK (length(source_path) > 0),
    imported_at_ms           INTEGER NOT NULL
        CHECK (typeof(imported_at_ms) = 'integer' AND imported_at_ms >= 0),
    operator                 TEXT,
    outcome                  TEXT NOT NULL CHECK (outcome IN ('created', 'no_op')),
    lines_created            INTEGER NOT NULL
        CHECK (typeof(lines_created) = 'integer' AND lines_created >= 0),
    lines_skipped            INTEGER NOT NULL
        CHECK (typeof(lines_skipped) = 'integer' AND lines_skipped >= 0),
    unmatched_lines          INTEGER NOT NULL
        CHECK (typeof(unmatched_lines) = 'integer' AND unmatched_lines >= 0),
    settled_lines            INTEGER NOT NULL
        CHECK (typeof(settled_lines) = 'integer' AND settled_lines >= 0),
    ambiguous_lines          INTEGER NOT NULL
        CHECK (typeof(ambiguous_lines) = 'integer' AND ambiguous_lines >= 0),
    already_settled_lines    INTEGER NOT NULL
        CHECK (typeof(already_settled_lines) = 'integer' AND already_settled_lines >= 0),
    unpriced_target_lines    INTEGER NOT NULL
        CHECK (typeof(unpriced_target_lines) = 'integer' AND unpriced_target_lines >= 0),
    currency_mismatch_lines  INTEGER NOT NULL
        CHECK (typeof(currency_mismatch_lines) = 'integer' AND currency_mismatch_lines >= 0),
    changed_outcomes         INTEGER NOT NULL
        CHECK (typeof(changed_outcomes) = 'integer' AND changed_outcomes >= 0),

    CONSTRAINT statement_import_statement_fk
        FOREIGN KEY (statement_id) REFERENCES statement(statement_id) ON DELETE RESTRICT,
    CONSTRAINT statement_import_line_partition CHECK (
        (outcome = 'created' AND lines_created > 0 AND lines_skipped = 0)
        OR (outcome = 'no_op' AND lines_created = 0 AND lines_skipped > 0)
    ),
    CONSTRAINT statement_import_match_partition CHECK (
        unmatched_lines + settled_lines + ambiguous_lines + already_settled_lines
        + unpriced_target_lines + currency_mismatch_lines
        = lines_created + lines_skipped
    )
);

CREATE INDEX statement_import_digest_lookup ON statement_import (file_sha256);

CREATE TRIGGER statement_line_matches_statement
BEFORE INSERT ON statement_line
WHEN NOT EXISTS (
    SELECT 1 FROM statement
    WHERE statement.statement_id = NEW.statement_id
      AND NEW.occurred_at_ms >= statement.period_start_ms
      AND NEW.occurred_at_ms < statement.period_end_ms
      AND NEW.currency = statement.currency
)
BEGIN
    SELECT RAISE(ABORT, 'statement_line_matches_statement');
END;

CREATE TRIGGER statement_no_update
BEFORE UPDATE ON statement
BEGIN
    SELECT RAISE(ABORT, 'statement_no_update');
END;

CREATE TRIGGER statement_no_delete
BEFORE DELETE ON statement
BEGIN
    SELECT RAISE(ABORT, 'statement_no_delete');
END;

-- Content is immutable. Only the latest reconciliation projection may change;
-- every prior value remains in match_attempt.
CREATE TRIGGER statement_line_update_guard
BEFORE UPDATE ON statement_line
WHEN NOT (
    NEW.statement_line_id IS OLD.statement_line_id
    AND NEW.statement_id IS OLD.statement_id
    AND NEW.line_id IS OLD.line_id
    AND NEW.occurred_at_ms IS OLD.occurred_at_ms
    AND NEW.description IS OLD.description
    AND NEW.quantity IS OLD.quantity
    AND NEW.unit IS OLD.unit
    AND NEW.amount_minor IS OLD.amount_minor
    AND NEW.currency IS OLD.currency
    AND NEW.correlation_ref IS OLD.correlation_ref
)
BEGIN
    SELECT RAISE(ABORT, 'statement_line_update_guard');
END;

CREATE TRIGGER statement_line_no_delete
BEFORE DELETE ON statement_line
BEGIN
    SELECT RAISE(ABORT, 'statement_line_no_delete');
END;

CREATE TRIGGER match_attempt_no_update
BEFORE UPDATE ON match_attempt
BEGIN
    SELECT RAISE(ABORT, 'match_attempt_no_update');
END;

CREATE TRIGGER match_attempt_no_delete
BEFORE DELETE ON match_attempt
BEGIN
    SELECT RAISE(ABORT, 'match_attempt_no_delete');
END;

CREATE TRIGGER statement_import_no_update
BEFORE UPDATE ON statement_import
BEGIN
    SELECT RAISE(ABORT, 'statement_import_no_update');
END;

CREATE TRIGGER statement_import_no_delete
BEFORE DELETE ON statement_import
BEGIN
    SELECT RAISE(ABORT, 'statement_import_no_delete');
END;

-- Desired configuration is durable before it is published to serving memory.
CREATE TABLE desired_config (
    id                    INTEGER PRIMARY KEY CHECK (id=1),
    generation            INTEGER NOT NULL UNIQUE CHECK (generation>=0),
    canonical_bytes       BLOB NOT NULL CHECK (length(canonical_bytes)>0),
    content_hash          TEXT NOT NULL CHECK (length(content_hash)=64 AND content_hash NOT GLOB '*[^0-9a-f]*'),
    poll_interval_secs    INTEGER NOT NULL CHECK (poll_interval_secs>0),
    qta_connection_file   TEXT,
    broca_connection_file TEXT,
    fusiform_connection_file TEXT,
    updated_at_ms         INTEGER NOT NULL CHECK (updated_at_ms>=0)
);
INSERT INTO desired_config (id,generation,canonical_bytes,content_hash,poll_interval_secs,qta_connection_file,broca_connection_file,fusiform_connection_file,updated_at_ms)
VALUES (1,0,CAST('{}' AS BLOB),'0000000000000000000000000000000000000000000000000000000000000000',60,NULL,NULL,NULL,0);

-- This row describes the relationship with the producer, not any one fact. A
-- cold axis is explicit: it is not allowed to look current merely because the
-- table has no observation yet.
CREATE TABLE source_runtime (
    source_key             TEXT NOT NULL PRIMARY KEY CHECK(length(source_key)>0),
    source_module          TEXT NOT NULL CHECK(length(source_module)>0),
    desired_generation     INTEGER NOT NULL REFERENCES desired_config(generation) DEFERRABLE INITIALLY DEFERRED,
    source_instance_id     TEXT,
    source_epoch           INTEGER CHECK(source_epoch IS NULL OR source_epoch>=0),
    consumed_export_seq    INTEGER CHECK(consumed_export_seq IS NULL OR consumed_export_seq>=0),
    high_water_export_seq  INTEGER CHECK(high_water_export_seq IS NULL OR high_water_export_seq>=0),
    last_success_ms        INTEGER CHECK(last_success_ms IS NULL OR last_success_ms>=0),
    pending_known          INTEGER NOT NULL CHECK(pending_known>=0),
    pending_unknown        INTEGER NOT NULL CHECK(pending_unknown IN (0,1)),
    CONSTRAINT source_runtime_lineage_pair CHECK (
        (source_instance_id IS NULL AND source_epoch IS NULL)
        OR
        (source_instance_id IS NOT NULL AND length(source_instance_id)>0
         AND source_epoch IS NOT NULL)
    ),
    CONSTRAINT source_runtime_state_complete CHECK (
        (source_instance_id IS NULL
         AND consumed_export_seq IS NULL
         AND high_water_export_seq IS NULL
         AND last_success_ms IS NULL
         AND pending_known=0
         AND pending_unknown=1)
        OR
        (source_instance_id IS NOT NULL
         AND consumed_export_seq IS NOT NULL
         AND high_water_export_seq IS NOT NULL
         AND high_water_export_seq>=consumed_export_seq
         AND last_success_ms IS NOT NULL)
    )
);
INSERT INTO source_runtime (
    source_key,source_module,desired_generation,source_instance_id,source_epoch,
    consumed_export_seq,high_water_export_seq,last_success_ms,pending_known,pending_unknown
) VALUES ('broca','broca',0,NULL,NULL,NULL,NULL,NULL,0,1);

CREATE TABLE account_subscription (
    account_ref          TEXT NOT NULL CHECK (length(account_ref) > 0),
    provider             TEXT NOT NULL CHECK (length(provider) > 0),
    plan_name            TEXT NOT NULL CHECK (length(plan_name) > 0),
    recurring_cost_minor INTEGER NOT NULL
        CHECK (typeof(recurring_cost_minor) = 'integer' AND recurring_cost_minor >= 0),
    currency             TEXT NOT NULL
        CHECK (length(currency) = 3 AND currency NOT GLOB '*[^A-Z]*'),
    amount_exponent      INTEGER NOT NULL
        CHECK (typeof(amount_exponent) = 'integer' AND amount_exponent BETWEEN 0 AND 9),
    period               TEXT NOT NULL CHECK (period IN ('month', 'year')),
    effective_from_ms    INTEGER NOT NULL
        CHECK (typeof(effective_from_ms) = 'integer' AND effective_from_ms >= 0),
    effective_to_ms      INTEGER CHECK (
        effective_to_ms IS NULL
        OR (typeof(effective_to_ms) = 'integer' AND effective_to_ms > effective_from_ms)
    ),
    operator             TEXT CHECK (operator IS NULL OR length(operator) > 0),

    PRIMARY KEY (provider, account_ref, effective_from_ms)
);

CREATE UNIQUE INDEX account_subscription_one_open_interval
    ON account_subscription (provider, account_ref)
    WHERE effective_to_ms IS NULL;

CREATE TRIGGER account_subscription_no_overlap
BEFORE INSERT ON account_subscription
WHEN EXISTS (
    SELECT 1 FROM account_subscription AS existing
    WHERE existing.provider = NEW.provider
      AND existing.account_ref = NEW.account_ref
      AND (existing.effective_to_ms IS NULL
           OR NEW.effective_from_ms < existing.effective_to_ms)
      AND (NEW.effective_to_ms IS NULL
           OR existing.effective_from_ms < NEW.effective_to_ms)
)
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_no_overlap');
END;

-- A successor may close an open interval once. Plan content and closed history
-- remain immutable so the plan used for an event's instant stays auditable.
CREATE TRIGGER account_subscription_update_guard
BEFORE UPDATE ON account_subscription
WHEN NOT (
    NEW.account_ref IS OLD.account_ref
    AND NEW.provider IS OLD.provider
    AND NEW.plan_name IS OLD.plan_name
    AND NEW.recurring_cost_minor IS OLD.recurring_cost_minor
    AND NEW.currency IS OLD.currency
    AND NEW.amount_exponent IS OLD.amount_exponent
    AND NEW.period IS OLD.period
    AND NEW.effective_from_ms IS OLD.effective_from_ms
    AND NEW.operator IS OLD.operator
    AND OLD.effective_to_ms IS NULL
    AND NEW.effective_to_ms IS NOT NULL
    AND NEW.effective_to_ms > OLD.effective_from_ms
)
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_update_guard');
END;

CREATE TRIGGER account_subscription_no_delete
BEFORE DELETE ON account_subscription
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_no_delete');
END;
"#,
    },
    Migration {
        version: 2,
        statements: r#"
-- The legacy optional floor pair collapsed "no minimum" with "nobody knows".
-- Refuse populated legacy floors because no stored state can recover which one
-- they meant; all historical priced rows written by Astrocyte have NULL here.
CREATE TEMP TABLE priced_charge_v2_legacy_floor_guard (
    legacy_floor_rows INTEGER NOT NULL CHECK (legacy_floor_rows = 0)
);
INSERT INTO priced_charge_v2_legacy_floor_guard
SELECT COUNT(*) FROM priced_charge WHERE floor_units IS NOT NULL;
DROP TABLE priced_charge_v2_legacy_floor_guard;

CREATE TABLE priced_charge_v2 (
    charge_id          TEXT NOT NULL PRIMARY KEY,
    event_id           TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    pricing_generation INTEGER NOT NULL
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation > 0),
    outcome            TEXT NOT NULL
        CHECK (outcome IN ('priced', 'covered', 'unpriced')),
    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    basis_covered      INTEGER NOT NULL
        CHECK (basis_covered IN (0, 1)),
    currency           TEXT,
    charge_nanos       INTEGER,
    unpriced_reason    TEXT
        CHECK (
            unpriced_reason IS NULL
            OR unpriced_reason IN (
                'undated_event', 'charge_basis_unknown', 'no_subscription_in_force',
                'missing_catalog_identity', 'catalog_pre_history', 'no_rate',
                'no_catalog_coverage', 'catalog_unconfigured', 'catalog_unknown_charge_basis',
                'missing_quantity', 'rate_tiered', 'rate_time_banded',
                'rate_expired', 'rate_withheld',
                'rate_uncertain', 'stated_zero_requires_policy',
                'arithmetic_out_of_range'
            )
        ),
    rate_units         INTEGER,
    rate_exponent      INTEGER,
    floor_state        TEXT CHECK (floor_state IN ('unknown', 'none', 'minimum')),
    floor_units        INTEGER,
    catalog_source     TEXT,
    resolved_at_ms     INTEGER
        CHECK (
            resolved_at_ms IS NULL
            OR (typeof(resolved_at_ms) = 'integer' AND resolved_at_ms >= 0)
        ),
    catalog_entry      TEXT
        CHECK (
            catalog_entry IS NULL
            OR (json_valid(catalog_entry) AND json_type(catalog_entry) = 'object')
        ),
    supersedes         TEXT UNIQUE,
    origin             TEXT NOT NULL
        CHECK (origin IN ('ingest', 'reprice', 'basis_correction')),
    recorded_at_ms     INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),

    UNIQUE (charge_id, event_id, role, unit),
    UNIQUE (event_id, role, unit, pricing_generation),
    CONSTRAINT priced_charge_measurement_fk
        FOREIGN KEY (event_id, role, unit)
        REFERENCES measurement(event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_currency_fk
        FOREIGN KEY (event_id, currency)
        REFERENCES usage_event(event_id, currency) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_supersedes_same_measurement_fk
        FOREIGN KEY (supersedes, event_id, role, unit)
        REFERENCES priced_charge_v2(charge_id, event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> charge_id),
    CONSTRAINT priced_charge_floor_shape
        CHECK ((floor_state = 'minimum') = (floor_units IS NOT NULL)),
    CONSTRAINT priced_charge_outcome_shape
        CHECK (
            (
                outcome = 'priced'
                AND charge_basis = 'metered_api'
                AND basis_covered = 0
                AND currency IS NOT NULL
                AND typeof(charge_nanos) = 'integer'
                AND unpriced_reason IS NULL
                AND typeof(rate_units) = 'integer'
                AND rate_exponent = 9
                AND floor_state IS NOT NULL
                AND catalog_source IS NOT NULL
                AND resolved_at_ms IS NOT NULL
                AND catalog_entry IS NOT NULL
            )
            OR (
                outcome = 'covered'
                AND charge_basis = 'subscription_included'
                AND basis_covered = 1
                AND currency IS NOT NULL
                AND charge_nanos = 0
                AND unpriced_reason IS NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
                AND catalog_source IS NULL
                AND resolved_at_ms IS NULL
                AND catalog_entry IS NULL
            )
            OR (
                outcome = 'unpriced'
                AND basis_covered = 0
                AND currency IS NULL
                AND charge_nanos IS NULL
                AND unpriced_reason IS NOT NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
            )
        )
);

INSERT INTO priced_charge_v2 (
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
)
SELECT
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    CASE WHEN outcome = 'priced' THEN 'unknown' ELSE NULL END,
    floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
FROM priced_charge
ORDER BY event_id,role,unit,pricing_generation;

-- Dropping a self-referencing table performs an implicit DELETE. Defer that
-- obsolete self-reference until the replacement has taken its name.
PRAGMA defer_foreign_keys = ON;
DROP TABLE priced_charge;
ALTER TABLE priced_charge_v2 RENAME TO priced_charge;

CREATE UNIQUE INDEX priced_charge_one_chain_root
    ON priced_charge (event_id, role, unit)
    WHERE supersedes IS NULL;

CREATE TRIGGER priced_charge_no_update
BEFORE UPDATE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_update');
END;

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

CREATE TRIGGER priced_charge_matches_event_generation_and_basis
BEFORE INSERT ON priced_charge
WHEN NOT EXISTS (
    SELECT 1 FROM usage_event event
    WHERE event.event_id = NEW.event_id
      AND event.pricing_generation = NEW.pricing_generation
      AND event.charge_basis = NEW.charge_basis
)
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_matches_event_generation_and_basis');
END;
"#,
    },
    Migration {
        version: 3,
        statements: r#"-- window_sample: append-only capacity history (issue #1).
--
-- An append-only SERIES beside the current-state caches. capacity_window,
-- capacity_observation, and capacity_provider_reading all hold the LATEST
-- observation per identity (each poll deletes/reinserts or upserts); a
-- window_sample row is never overwritten — one row per distinct provider
-- observation, so a per-account drain curve can distinguish an account flat
-- for six days from one burning fast toward the same remaining percent.
--
-- Identity is the landed capacity identity (provider, account_ref, pool_key)
-- plus the fetchedAt instant: the dedupe key is the PARSED integer instant
-- (nanoseconds since the Unix epoch), never the RFC3339 string. chrono
-- serialises a whole second with no fractional part and a nanosecond value
-- with nine digits, and six-digit forms exist in the wild, so string equality
-- is not instant equality. UNIQUE + INSERT OR IGNORE makes a re-poll of the
-- same reading idempotent.
--
-- The table is deliberately dumb — it appends what was genuinely observed and
-- lets readers decide what is interesting. No derived/interpretive columns,
-- and no observed_at_ms: with INSERT OR IGNORE the discarded duplicates ARE
-- the polling evidence, so a poll-clock column would silently mean "when we
-- first happened to see it" rather than "when we polled it".
--
-- Accepted rather than filtered: a module restart in insula discards every
-- slot and re-fetches, so the same underlying window mints a NEW fetchedAt and
-- the curve gains a true-but-uninformative point where utilization has not
-- moved. A flat segment that is genuinely flat is the observation a stranding
-- decision most depends on, so it is kept.
--
-- A preserved reading (insula's stale:{since,class}) clones the previous
-- fetchedAt, so the integer-instant key drops it without this table modelling
-- the disclosure. The current-state capacity_window carries stale_since and
-- stale_class (v9) for the "as of now" question; this series answers "what was
-- the utilization".
--
-- Deferred:
--   * Retention (downsampling + age cap) — deliberately out of scope. The two
--     sizes would be config members, and this repo's config format is closed
--     (deny_unknown_fields, no defaults), so adding them here would make the
--     daemon refuse to start on a config that lacks them. The table is useless
--     until it has data anyway.
CREATE TABLE window_sample (
    -- window_sample: append-only capacity history (issue #1)
    provider           TEXT NOT NULL,
    api_provider       TEXT,
    account_ref        TEXT NOT NULL,
    pool_key           TEXT NOT NULL,
    fetched_at_instant INTEGER NOT NULL,
    used_percent       REAL NOT NULL,
    raw_used_percent   REAL,
    resets_at          TEXT,
    window_minutes     INTEGER,
    source             TEXT,
    PRIMARY KEY (provider, account_ref, pool_key, fetched_at_instant)
);
"#,
    },
    Migration {
        version: 4,
        statements: r#"-- auth_selection: the producer's credential id, carried verbatim.
--
-- BROCA stamps auth_selection on every export segment (the credential id the
-- run was admitted under, frozen at admission). For api-key credentials
-- account_ref is absent BY CONSTRUCTION (the vault has no account id for a
-- key), so auth_selection is the only per-account discriminator a PAYG fact
-- carries. This is a PRODUCER DECLARATION carried verbatim, never derived: we
-- store the string BROCA sent and infer nothing from its shape.
--
-- Nullable with no default and no CHECK: an older producer that predates the
-- field sends no value (NULL), and the value is an opaque credential id whose
-- spelling we do not validate.
ALTER TABLE usage_event ADD COLUMN auth_selection TEXT;
"#,
    },
    Migration {
        version: 5,
        statements: r#"-- Generalise producer runtime coordinates. BROCA's pair is encoded as
-- source_instance_id || ':' || source_epoch. UUIDs and decimal integers cannot
-- contain ':', so splitting at the final separator is a bijection.
ALTER TABLE desired_config ADD COLUMN thalamus_connection_file TEXT;

CREATE TABLE source_runtime_v5 (
    source_key            TEXT PRIMARY KEY,
    source_module         TEXT NOT NULL,
    desired_generation    INTEGER NOT NULL,
    lineage               TEXT,
    consumed_ordinal      INTEGER,
    high_water_ordinal    INTEGER,
    last_success_ms       INTEGER,
    pending_known         INTEGER NOT NULL CHECK (pending_known >= 0),
    pending_unknown       INTEGER NOT NULL CHECK (pending_unknown IN (0,1)),
    CONSTRAINT source_runtime_state_complete CHECK (
        (lineage IS NULL AND consumed_ordinal IS NULL AND high_water_ordinal IS NULL AND last_success_ms IS NULL)
        OR (lineage IS NOT NULL AND consumed_ordinal IS NOT NULL AND high_water_ordinal IS NOT NULL AND last_success_ms IS NOT NULL)
    )
);
INSERT INTO source_runtime_v5 (
    source_key,source_module,desired_generation,lineage,consumed_ordinal,
    high_water_ordinal,last_success_ms,pending_known,pending_unknown
)
SELECT source_key,source_module,desired_generation,
       CASE WHEN source_instance_id IS NULL THEN NULL
            ELSE source_instance_id || ':' || CAST(source_epoch AS TEXT) END,
       consumed_export_seq,high_water_export_seq,last_success_ms,
       pending_known,pending_unknown
FROM source_runtime;
DROP TABLE source_runtime;
ALTER TABLE source_runtime_v5 RENAME TO source_runtime;
"#,
    },
    Migration {
        version: 6,
        statements: r#"-- Widen priced_charge.unpriced_reason with 'rate_inherited': a catalog rate the
-- serving provider did not publish, borrowed from the model's originator and
-- marked as such by fusiform (protocol 0.19). Reference data, never a charge.
-- An inline CHECK can only be widened by rebuilding the table; every row,
-- index, and trigger is carried unchanged.
CREATE TABLE priced_charge_v6 (
    charge_id          TEXT NOT NULL PRIMARY KEY,
    event_id           TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    pricing_generation INTEGER NOT NULL
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation > 0),
    outcome            TEXT NOT NULL
        CHECK (outcome IN ('priced', 'covered', 'unpriced')),
    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    basis_covered      INTEGER NOT NULL
        CHECK (basis_covered IN (0, 1)),
    currency           TEXT,
    charge_nanos       INTEGER,
    unpriced_reason    TEXT
        CHECK (
            unpriced_reason IS NULL
            OR unpriced_reason IN (
                'undated_event', 'charge_basis_unknown', 'no_subscription_in_force',
                'missing_catalog_identity', 'catalog_pre_history', 'no_rate',
                'no_catalog_coverage', 'catalog_unconfigured', 'catalog_unknown_charge_basis',
                'missing_quantity', 'rate_tiered', 'rate_time_banded',
                'rate_expired', 'rate_withheld',
                'rate_uncertain', 'stated_zero_requires_policy',
                'arithmetic_out_of_range', 'rate_inherited'
            )
        ),
    rate_units         INTEGER,
    rate_exponent      INTEGER,
    floor_state        TEXT CHECK (floor_state IN ('unknown', 'none', 'minimum')),
    floor_units        INTEGER,
    catalog_source     TEXT,
    resolved_at_ms     INTEGER
        CHECK (
            resolved_at_ms IS NULL
            OR (typeof(resolved_at_ms) = 'integer' AND resolved_at_ms >= 0)
        ),
    catalog_entry      TEXT
        CHECK (
            catalog_entry IS NULL
            OR (json_valid(catalog_entry) AND json_type(catalog_entry) = 'object')
        ),
    supersedes         TEXT UNIQUE,
    origin             TEXT NOT NULL
        CHECK (origin IN ('ingest', 'reprice', 'basis_correction')),
    recorded_at_ms     INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),

    UNIQUE (charge_id, event_id, role, unit),
    UNIQUE (event_id, role, unit, pricing_generation),
    CONSTRAINT priced_charge_measurement_fk
        FOREIGN KEY (event_id, role, unit)
        REFERENCES measurement(event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_currency_fk
        FOREIGN KEY (event_id, currency)
        REFERENCES usage_event(event_id, currency) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_supersedes_same_measurement_fk
        FOREIGN KEY (supersedes, event_id, role, unit)
        REFERENCES priced_charge_v6(charge_id, event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> charge_id),
    CONSTRAINT priced_charge_floor_shape
        CHECK ((floor_state = 'minimum') = (floor_units IS NOT NULL)),
    CONSTRAINT priced_charge_outcome_shape
        CHECK (
            (
                outcome = 'priced'
                AND charge_basis = 'metered_api'
                AND basis_covered = 0
                AND currency IS NOT NULL
                AND typeof(charge_nanos) = 'integer'
                AND unpriced_reason IS NULL
                AND typeof(rate_units) = 'integer'
                AND rate_exponent = 9
                AND floor_state IS NOT NULL
                AND catalog_source IS NOT NULL
                AND resolved_at_ms IS NOT NULL
                AND catalog_entry IS NOT NULL
            )
            OR (
                outcome = 'covered'
                AND charge_basis = 'subscription_included'
                AND basis_covered = 1
                AND currency IS NOT NULL
                AND charge_nanos = 0
                AND unpriced_reason IS NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
                AND catalog_source IS NULL
                AND resolved_at_ms IS NULL
                AND catalog_entry IS NULL
            )
            OR (
                outcome = 'unpriced'
                AND basis_covered = 0
                AND currency IS NULL
                AND charge_nanos IS NULL
                AND unpriced_reason IS NOT NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
            )
        )
);

INSERT INTO priced_charge_v6 (
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
)
SELECT
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,
    floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
FROM priced_charge
ORDER BY event_id,role,unit,pricing_generation;

-- Dropping a self-referencing table performs an implicit DELETE. Defer that
-- obsolete self-reference until the replacement has taken its name.
PRAGMA defer_foreign_keys = ON;
DROP TABLE priced_charge;
ALTER TABLE priced_charge_v6 RENAME TO priced_charge;

CREATE UNIQUE INDEX priced_charge_one_chain_root
    ON priced_charge (event_id, role, unit)
    WHERE supersedes IS NULL;

CREATE TRIGGER priced_charge_no_update
BEFORE UPDATE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_update');
END;

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

CREATE TRIGGER priced_charge_matches_event_generation_and_basis
BEFORE INSERT ON priced_charge
WHEN NOT EXISTS (
    SELECT 1 FROM usage_event event
    WHERE event.event_id = NEW.event_id
      AND event.pricing_generation = NEW.pricing_generation
      AND event.charge_basis = NEW.charge_basis
)
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_matches_event_generation_and_basis');
END;
"#,
    },
    Migration {
        version: 7,
        statements: r#"-- The store cannot establish which path wrote a pre-v7 row, and a migration
-- that stamps a value it cannot establish destroys the evidence that the
-- distinction was ever missing. Preserve historical IDs and money unchanged.
-- As of THIS version neither ingest nor basis_correction had a priced_charge
-- writer: recording defers pricing, and basis correction refused every call.
-- (Version 8 restores basis_correction to the vocabulary, with a writer.)
CREATE TEMP TABLE priced_charge_origin_stage AS SELECT * FROM priced_charge;
UPDATE priced_charge_origin_stage SET origin = 'unattributed';
CREATE TABLE priced_charge_v7 (
    charge_id          TEXT NOT NULL PRIMARY KEY,
    event_id           TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    pricing_generation INTEGER NOT NULL
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation > 0),
    outcome            TEXT NOT NULL
        CHECK (outcome IN ('priced', 'covered', 'unpriced')),
    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    basis_covered      INTEGER NOT NULL
        CHECK (basis_covered IN (0, 1)),
    currency           TEXT,
    charge_nanos       INTEGER,
    unpriced_reason    TEXT
        CHECK (
            unpriced_reason IS NULL
            OR unpriced_reason IN (
                'undated_event', 'charge_basis_unknown', 'no_subscription_in_force',
                'missing_catalog_identity', 'catalog_pre_history', 'no_rate',
                'no_catalog_coverage', 'catalog_unconfigured', 'catalog_unknown_charge_basis',
                'missing_quantity', 'rate_tiered', 'rate_time_banded',
                'rate_expired', 'rate_withheld',
                'rate_uncertain', 'stated_zero_requires_policy',
                'arithmetic_out_of_range', 'rate_inherited'
            )
        ),
    rate_units         INTEGER,
    rate_exponent      INTEGER,
    floor_state        TEXT CHECK (floor_state IN ('unknown', 'none', 'minimum')),
    floor_units        INTEGER,
    catalog_source     TEXT,
    resolved_at_ms     INTEGER
        CHECK (
            resolved_at_ms IS NULL
            OR (typeof(resolved_at_ms) = 'integer' AND resolved_at_ms >= 0)
        ),
    catalog_entry      TEXT
        CHECK (
            catalog_entry IS NULL
            OR (json_valid(catalog_entry) AND json_type(catalog_entry) = 'object')
        ),
    supersedes         TEXT UNIQUE,
    origin             TEXT NOT NULL
        CHECK (origin IN ('pricing_pass', 'operator_reprice', 'unattributed')),
    recorded_at_ms     INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),

    UNIQUE (charge_id, event_id, role, unit),
    UNIQUE (event_id, role, unit, pricing_generation),
    CONSTRAINT priced_charge_measurement_fk
        FOREIGN KEY (event_id, role, unit)
        REFERENCES measurement(event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_currency_fk
        FOREIGN KEY (event_id, currency)
        REFERENCES usage_event(event_id, currency) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_supersedes_same_measurement_fk
        FOREIGN KEY (supersedes, event_id, role, unit)
        REFERENCES priced_charge_v7(charge_id, event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> charge_id),
    CONSTRAINT priced_charge_floor_shape
        CHECK ((floor_state = 'minimum') = (floor_units IS NOT NULL)),
    CONSTRAINT priced_charge_outcome_shape
        CHECK (
            (
                outcome = 'priced'
                AND charge_basis = 'metered_api'
                AND basis_covered = 0
                AND currency IS NOT NULL
                AND typeof(charge_nanos) = 'integer'
                AND unpriced_reason IS NULL
                AND typeof(rate_units) = 'integer'
                AND rate_exponent = 9
                AND floor_state IS NOT NULL
                AND catalog_source IS NOT NULL
                AND resolved_at_ms IS NOT NULL
                AND catalog_entry IS NOT NULL
            )
            OR (
                outcome = 'covered'
                AND charge_basis = 'subscription_included'
                AND basis_covered = 1
                AND currency IS NOT NULL
                AND charge_nanos = 0
                AND unpriced_reason IS NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
                AND catalog_source IS NULL
                AND resolved_at_ms IS NULL
                AND catalog_entry IS NULL
            )
            OR (
                outcome = 'unpriced'
                AND basis_covered = 0
                AND currency IS NULL
                AND charge_nanos IS NULL
                AND unpriced_reason IS NOT NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
            )
        )
);

INSERT INTO priced_charge_v7 (
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
)
SELECT
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,
    floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
FROM priced_charge_origin_stage
ORDER BY event_id,role,unit,pricing_generation;

-- Dropping a self-referencing table performs an implicit DELETE. Defer that
-- obsolete self-reference until the replacement has taken its name.
PRAGMA defer_foreign_keys = ON;
DROP TABLE priced_charge;
ALTER TABLE priced_charge_v7 RENAME TO priced_charge;

CREATE UNIQUE INDEX priced_charge_one_chain_root
    ON priced_charge (event_id, role, unit)
    WHERE supersedes IS NULL;

CREATE TRIGGER priced_charge_no_update
BEFORE UPDATE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_update');
END;

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

CREATE TRIGGER priced_charge_matches_event_generation_and_basis
BEFORE INSERT ON priced_charge
WHEN NOT EXISTS (
    SELECT 1 FROM usage_event event
    WHERE event.event_id = NEW.event_id
      AND event.pricing_generation = NEW.pricing_generation
      AND event.charge_basis = NEW.charge_basis
)
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_matches_event_generation_and_basis');
END;

DROP TABLE priced_charge_origin_stage;
"#,
    },
    Migration {
        version: 8,
        statements: r#"-- Basis corrections now append their own priced-charge heads, so
-- 'basis_correction' returns to the origin vocabulary. Version 7 removed it
-- because nothing could write it, and an origin a writer cannot produce is a
-- value the column claims to hold and never will; it comes back here WITH its
-- writer. The origin must name the act because a correction and an ordinary
-- pricing pass leave rows that are otherwise indistinguishable, and only the
-- origin says which authority produced the money.
--
-- The whole table is rebuilt because SQLite cannot alter a CHECK constraint.
-- Every other column, constraint, index and trigger is carried over from
-- version 7 unchanged; the sole difference is the added origin value.
CREATE TABLE priced_charge_v8 (
    charge_id          TEXT NOT NULL PRIMARY KEY,
    event_id           TEXT NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    pricing_generation INTEGER NOT NULL
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation > 0),
    outcome            TEXT NOT NULL
        CHECK (outcome IN ('priced', 'covered', 'unpriced')),
    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    basis_covered      INTEGER NOT NULL
        CHECK (basis_covered IN (0, 1)),
    currency           TEXT,
    charge_nanos       INTEGER,
    unpriced_reason    TEXT
        CHECK (
            unpriced_reason IS NULL
            OR unpriced_reason IN (
                'undated_event', 'charge_basis_unknown', 'no_subscription_in_force',
                'missing_catalog_identity', 'catalog_pre_history', 'no_rate',
                'no_catalog_coverage', 'catalog_unconfigured', 'catalog_unknown_charge_basis',
                'missing_quantity', 'rate_tiered', 'rate_time_banded',
                'rate_expired', 'rate_withheld',
                'rate_uncertain', 'stated_zero_requires_policy',
                'arithmetic_out_of_range', 'rate_inherited'
            )
        ),
    rate_units         INTEGER,
    rate_exponent      INTEGER,
    floor_state        TEXT CHECK (floor_state IN ('unknown', 'none', 'minimum')),
    floor_units        INTEGER,
    catalog_source     TEXT,
    resolved_at_ms     INTEGER
        CHECK (
            resolved_at_ms IS NULL
            OR (typeof(resolved_at_ms) = 'integer' AND resolved_at_ms >= 0)
        ),
    catalog_entry      TEXT
        CHECK (
            catalog_entry IS NULL
            OR (json_valid(catalog_entry) AND json_type(catalog_entry) = 'object')
        ),
    supersedes         TEXT UNIQUE,
    origin             TEXT NOT NULL
        CHECK (origin IN ('pricing_pass', 'operator_reprice', 'unattributed', 'basis_correction')),
    recorded_at_ms     INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),

    UNIQUE (charge_id, event_id, role, unit),
    UNIQUE (event_id, role, unit, pricing_generation),
    CONSTRAINT priced_charge_measurement_fk
        FOREIGN KEY (event_id, role, unit)
        REFERENCES measurement(event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_currency_fk
        FOREIGN KEY (event_id, currency)
        REFERENCES usage_event(event_id, currency) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_supersedes_same_measurement_fk
        FOREIGN KEY (supersedes, event_id, role, unit)
        REFERENCES priced_charge_v8(charge_id, event_id, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> charge_id),
    CONSTRAINT priced_charge_floor_shape
        CHECK ((floor_state = 'minimum') = (floor_units IS NOT NULL)),
    CONSTRAINT priced_charge_outcome_shape
        CHECK (
            (
                outcome = 'priced'
                AND charge_basis = 'metered_api'
                AND basis_covered = 0
                AND currency IS NOT NULL
                AND typeof(charge_nanos) = 'integer'
                AND unpriced_reason IS NULL
                AND typeof(rate_units) = 'integer'
                AND rate_exponent = 9
                AND floor_state IS NOT NULL
                AND catalog_source IS NOT NULL
                AND resolved_at_ms IS NOT NULL
                AND catalog_entry IS NOT NULL
            )
            OR (
                outcome = 'covered'
                AND charge_basis = 'subscription_included'
                AND basis_covered = 1
                AND currency IS NOT NULL
                AND charge_nanos = 0
                AND unpriced_reason IS NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
                AND catalog_source IS NULL
                AND resolved_at_ms IS NULL
                AND catalog_entry IS NULL
            )
            OR (
                outcome = 'unpriced'
                AND basis_covered = 0
                AND currency IS NULL
                AND charge_nanos IS NULL
                AND unpriced_reason IS NOT NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
            )
        )
);

INSERT INTO priced_charge_v8 (
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
)
SELECT
    charge_id,event_id,role,unit,pricing_generation,outcome,charge_basis,
    basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
    floor_state,
    floor_units,catalog_source,resolved_at_ms,catalog_entry,
    supersedes,origin,recorded_at_ms
FROM priced_charge
ORDER BY event_id,role,unit,pricing_generation;

-- Dropping a self-referencing table performs an implicit DELETE. Defer that
-- obsolete self-reference until the replacement has taken its name.
PRAGMA defer_foreign_keys = ON;
DROP TABLE priced_charge;
ALTER TABLE priced_charge_v8 RENAME TO priced_charge;

CREATE UNIQUE INDEX priced_charge_one_chain_root
    ON priced_charge (event_id, role, unit)
    WHERE supersedes IS NULL;

CREATE TRIGGER priced_charge_no_update
BEFORE UPDATE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_update');
END;

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

CREATE TRIGGER priced_charge_matches_event_generation_and_basis
BEFORE INSERT ON priced_charge
WHEN NOT EXISTS (
    SELECT 1 FROM usage_event event
    WHERE event.event_id = NEW.event_id
      AND event.pricing_generation = NEW.pricing_generation
      AND event.charge_basis = NEW.charge_basis
)
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_matches_event_generation_and_basis');
END;

"#,
    },
    Migration {
        version: 9,
        statements: r#"-- File-backed readers can complete a drain without a producer lineage or
-- ordinal axis. Their successful-drain state is distinguished from the cold state by
-- last_success_ms; pending is known empty because the complete available file set was read.
--
-- SQLite cannot alter the named CHECK, so rebuild the table. The Pi sentinel is
-- retired only when both its source key and explicit historical lineage match;
-- every other row is copied verbatim.
CREATE TABLE source_runtime_v9 (
    source_key            TEXT PRIMARY KEY,
    source_module         TEXT NOT NULL,
    desired_generation    INTEGER NOT NULL,
    lineage               TEXT,
    consumed_ordinal      INTEGER,
    high_water_ordinal    INTEGER,
    last_success_ms       INTEGER,
    pending_known         INTEGER NOT NULL CHECK (pending_known >= 0),
    pending_unknown       INTEGER NOT NULL CHECK (pending_unknown IN (0,1)),
    CONSTRAINT source_runtime_state_complete CHECK (
        (lineage IS NULL
         AND consumed_ordinal IS NULL
         AND high_water_ordinal IS NULL
         AND last_success_ms IS NULL
         AND pending_known=0
         AND pending_unknown=1)
        OR
        (lineage IS NULL
         AND consumed_ordinal IS NULL
         AND high_water_ordinal IS NULL
         AND last_success_ms IS NOT NULL
         AND pending_known=0
         AND pending_unknown=0)
        OR
        (lineage IS NOT NULL
         AND consumed_ordinal IS NOT NULL
         AND high_water_ordinal IS NOT NULL
         AND last_success_ms IS NOT NULL)
    )
);
INSERT INTO source_runtime_v9 (
    source_key,source_module,desired_generation,lineage,consumed_ordinal,
    high_water_ordinal,last_success_ms,pending_known,pending_unknown
)
SELECT source_key,source_module,desired_generation,
       CASE WHEN source_key='pi' AND lineage='filesystem' THEN NULL ELSE lineage END,
       CASE WHEN source_key='pi' AND lineage='filesystem' THEN NULL ELSE consumed_ordinal END,
       CASE WHEN source_key='pi' AND lineage='filesystem' THEN NULL ELSE high_water_ordinal END,
       last_success_ms,
       CASE WHEN source_key='pi' AND lineage='filesystem' THEN 0 ELSE pending_known END,
       CASE WHEN source_key='pi' AND lineage='filesystem' THEN 0 ELSE pending_unknown END
FROM source_runtime
ORDER BY source_key;
DROP TABLE source_runtime;
ALTER TABLE source_runtime_v9 RENAME TO source_runtime;
"#,
    },
    Migration {
        version: 10,
        statements: r#"-- redelivery_log counts a producer sending the same record again. The Pi and
-- OpenCode file readers wrote a row every time they re-read a session file,
-- which records our own re-read as the producer's behaviour. Those rows are
-- false, nothing reads the table yet, and ingest no longer writes them for
-- read-path arrivals, so they are deleted. Rows from pushing producers are
-- genuine and stay untouched. Space is reclaimed out of band; VACUUM cannot
-- run inside the migrator's transaction.
DELETE FROM redelivery_log WHERE producer IN ('pi','opencode');
"#,
    },
    Migration {
        version: 11,
        statements: r#"-- The THALAMUS usage.export producer lane is retired. A producer reports only
-- what it observed while running, for sessions someone pointed at it, so it
-- captured a small slice of the Claude Code usage that actually happened. Its
-- cursor row and its connection setting go; nothing drains it any more, and a
-- stale cursor row would read as a producer that stopped reporting.
--
-- The usage_event rows it delivered (producer 'thalamus') and their
-- measurements and charges are recorded facts and are deliberately untouched.
--
-- DROP COLUMN is clean here: no index, trigger, view, foreign key or CHECK
-- names desired_config.thalamus_connection_file, so no table rebuild is needed.
DELETE FROM source_runtime WHERE source_key='thalamus';
ALTER TABLE desired_config DROP COLUMN thalamus_connection_file;
"#,
    },
    Migration {
        version: 12,
        statements: r#"-- The pricing pass used to find its work by scanning every measurement in
-- the ledger for one with no current charge head, on every tick. That costs
-- what the ledger costs rather than what is pending, and on a million-event
-- ledger it ran for most of each tick while usually finding nothing.
--
-- pricing_pending holds exactly the events that have at least one measurement
-- with no current charge head. Recording an event enqueues it in the
-- recording transaction; the pricing commit that heads its last headless
-- measurement dequeues it in that same transaction. Nothing else writes it
-- except the periodic reconciliation, which re-runs the old full scan to
-- catch a write path that forgot to enqueue.
CREATE TABLE pricing_pending (
    event_id TEXT PRIMARY KEY,
    CONSTRAINT pricing_pending_event_fk
        FOREIGN KEY (event_id) REFERENCES usage_event(event_id) ON DELETE RESTRICT
) WITHOUT ROWID;

-- Coverage backfill re-prices history when a subscription plan is declared or
-- closed. It only has work after such a write, so it is gated on a generation
-- that every plan write moves (in the plan write's own transaction) and that
-- the backfill records once it has drained a generation to completion. It
-- starts one ahead of the drained mark so the first pass after this migration
-- runs the backfill once: a plan written before the upgrade may not have been
-- drained yet.
CREATE TABLE pricing_selection_state (
    id                          INTEGER PRIMARY KEY CHECK (id = 1),
    plan_generation             INTEGER NOT NULL
        CHECK (typeof(plan_generation) = 'integer' AND plan_generation >= 0),
    backfill_drained_generation INTEGER NOT NULL
        CHECK (typeof(backfill_drained_generation) = 'integer'
               AND backfill_drained_generation >= 0),
    CONSTRAINT pricing_selection_drained_not_ahead
        CHECK (backfill_drained_generation <= plan_generation)
);
INSERT INTO pricing_selection_state (id,plan_generation,backfill_drained_generation)
VALUES (1,1,0);

-- Seed the queue with everything already pending, so nothing is lost at the
-- cutover. This asks "does the measurement have ANY charge row" rather than
-- "does it have a charge row nothing supersedes", which is the same set and
-- several times cheaper on a large ledger: a measurement's charge rows form
-- one chain (one root per measurement, and `supersedes` is UNIQUE, so the
-- chain cannot fork), and a non-empty finite chain always has exactly one
-- row nothing supersedes. The first reconciliation after startup re-checks
-- the result against the exact head definition.
INSERT INTO pricing_pending (event_id)
SELECT DISTINCT measurement.event_id
FROM measurement
WHERE NOT EXISTS (
    SELECT 1 FROM priced_charge
    WHERE priced_charge.event_id = measurement.event_id
      AND priced_charge.role = measurement.role
      AND priced_charge.unit = measurement.unit
);
"#,
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
        statements: r#"-- BROCA's charge-basis provenance, carried verbatim on every fact it exports.
--
-- charge_basis_mapping_version: the version of BROCA's SHIPPED charge-basis
-- table that decided charge_basis. charge_basis_overrides: which local override
-- rows BROCA applied on top of that table -- 'none' means none did, a hex digest
-- identifies the rows that did, and NULL means the producer did not say (a fact
-- written before BROCA emitted the field). NULL must never be read as 'none'.
--
-- Nullable with no default and no CHECK. Other producers leave both NULL. A
-- CHECK refusing a malformed digest would refuse the whole BROCA export page
-- and stall ingest, so the value is stored as the producer sent it.
ALTER TABLE usage_event ADD COLUMN charge_basis_mapping_version INTEGER;
ALTER TABLE usage_event ADD COLUMN charge_basis_overrides TEXT;

-- The update guard lists every column that must stay unchanged, so a new
-- column is mutable until it is listed. The guard is recreated with the two
-- provenance columns added, and with auth_selection (added in version 4),
-- which the guard never listed: it is producer provenance like the others and
-- was left mutable by omission. Every other condition is carried unchanged.
DROP TRIGGER usage_event_update_guard;
CREATE TRIGGER usage_event_update_guard
BEFORE UPDATE ON usage_event
WHEN NOT (
    NEW.event_id IS OLD.event_id
    AND NEW.producer IS OLD.producer
    AND NEW.producer_lineage IS OLD.producer_lineage
    AND NEW.producer_ref IS OLD.producer_ref
    AND NEW.prior_event_ref IS OLD.prior_event_ref
    AND NEW.harness IS OLD.harness
    AND NEW.project_root IS OLD.project_root
    AND NEW.session IS OLD.session
    AND NEW.run_ref IS OLD.run_ref
    AND NEW.work_class IS OLD.work_class
    AND NEW.occurred_at_ms IS OLD.occurred_at_ms
    AND NEW.emitted_at_ms IS OLD.emitted_at_ms
    AND NEW.recorded_at_ms IS OLD.recorded_at_ms
    AND NEW.terminal_status IS OLD.terminal_status
    AND NEW.attempt_ordinal IS OLD.attempt_ordinal
    AND NEW.vendor_request_id IS OLD.vendor_request_id
    AND NEW.provider IS OLD.provider
    AND NEW.model IS OLD.model
    AND NEW.account_ref IS OLD.account_ref
    AND NEW.auth_selection IS OLD.auth_selection
    AND NEW.charge_basis_mapping_version IS OLD.charge_basis_mapping_version
    AND NEW.charge_basis_overrides IS OLD.charge_basis_overrides
    AND (
        NEW.charge_basis IS OLD.charge_basis
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.pricing_generation = OLD.pricing_generation
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.currency IS OLD.currency
        OR (OLD.currency IS NULL AND NEW.currency IS NOT NULL)
    )
)
BEGIN
    SELECT RAISE(ABORT, 'usage_event_update_guard');
END;
"#,
    },
    Migration {
        version: 14,
        statements: r#"-- A file reader that succeeded before can fail its latest drain. Its
-- pending work is then unknown, but last_success_ms still records the prior success.
-- SQLite cannot alter the named CHECK, so rebuild and copy every row verbatim.
CREATE TABLE source_runtime_v14 (
    source_key            TEXT PRIMARY KEY,
    source_module         TEXT NOT NULL,
    desired_generation    INTEGER NOT NULL,
    lineage               TEXT,
    consumed_ordinal      INTEGER,
    high_water_ordinal    INTEGER,
    last_success_ms       INTEGER,
    pending_known         INTEGER NOT NULL CHECK (pending_known >= 0),
    pending_unknown       INTEGER NOT NULL CHECK (pending_unknown IN (0,1)),
    CONSTRAINT source_runtime_state_complete CHECK (
        (lineage IS NULL
         AND consumed_ordinal IS NULL
         AND high_water_ordinal IS NULL
         AND last_success_ms IS NULL
         AND pending_known=0
         AND pending_unknown=1)
        OR
        (lineage IS NULL
         AND consumed_ordinal IS NULL
         AND high_water_ordinal IS NULL
         AND last_success_ms IS NOT NULL
         AND pending_known=0
         AND pending_unknown IN (0,1))
        OR
        (lineage IS NOT NULL
         AND consumed_ordinal IS NOT NULL
         AND high_water_ordinal IS NOT NULL
         AND last_success_ms IS NOT NULL)
    )
);
INSERT INTO source_runtime_v14 (
    source_key,source_module,desired_generation,lineage,consumed_ordinal,
    high_water_ordinal,last_success_ms,pending_known,pending_unknown
)
SELECT source_key,source_module,desired_generation,lineage,consumed_ordinal,
       high_water_ordinal,last_success_ms,pending_known,pending_unknown
FROM source_runtime
ORDER BY source_key;
DROP TABLE source_runtime;
ALTER TABLE source_runtime_v14 RENAME TO source_runtime;
"#,
    },
    Migration {
        version: 15,
        statements: r#"-- The derived subscription multiplier: its desired configuration, and the
-- daily series it records.
--
-- desired_config.subscription_multiplier holds the member's canonical JSON.
-- NULL on a generation committed before this migration means that generation
-- did not record the member; it is never read as "configured with nothing".
ALTER TABLE desired_config ADD COLUMN subscription_multiplier TEXT
    CHECK (subscription_multiplier IS NULL OR json_valid(subscription_multiplier));

-- One row per quota provider per UTC day: what that provider's subscription
-- quota was worth at API list prices over the trailing seven days. Append-only.
-- Every input a multiplier depends on is stored beside it, so any row can be
-- recomputed and audited: tokens by role, API nanos and their split per model
-- (the multiplier moves with model mix), unpriced tokens by reason, the
-- denominators, the accounts and plan prices used, the scope rules, the API
-- twin map and the catalog resolution instant. observation_day is the UTC date
-- of interval_end_ms, and (quota_provider, observation_day) is unique, so a
-- day can never be recorded twice.
CREATE TABLE multiplier_observation (
    observation_id                 INTEGER PRIMARY KEY,
    quota_provider                 TEXT NOT NULL,
    observation_day                TEXT NOT NULL,
    snapshot_at_ms                 INTEGER NOT NULL,
    interval_start_ms              INTEGER NOT NULL,
    interval_end_ms                INTEGER NOT NULL,
    -- The interval clipped to the span the quota data covers; NULL when no
    -- configured account has a reading in the interval.
    token_interval_start_ms        INTEGER,
    token_interval_end_ms          INTEGER,
    window_pool_key                TEXT NOT NULL,
    window_minutes                 INTEGER NOT NULL,
    input_tokens                   INTEGER NOT NULL,
    cache_read_tokens              INTEGER NOT NULL,
    cache_write_tokens             INTEGER NOT NULL,
    output_tokens                  INTEGER NOT NULL,
    reasoning_tokens               INTEGER NOT NULL,
    api_nanos                      INTEGER NOT NULL,
    api_nanos_by_model             TEXT NOT NULL CHECK (json_valid(api_nanos_by_model)),
    unpriced_tokens                TEXT NOT NULL CHECK (json_valid(unpriced_tokens)),
    tokens_outside_quota_coverage  INTEGER NOT NULL,
    base_rate_tier_untested_tokens INTEGER NOT NULL,
    subscription_nanos             INTEGER NOT NULL,
    fee_paid_nanos                 INTEGER NOT NULL,
    delta_percent                  REAL NOT NULL,
    accounts_included_count        INTEGER NOT NULL,
    accounts_included              TEXT NOT NULL CHECK (json_valid(accounts_included)),
    accounts_excluded              TEXT NOT NULL CHECK (json_valid(accounts_excluded)),
    excluded_pairs                 TEXT NOT NULL CHECK (json_valid(excluded_pairs)),
    low_resolution                 INTEGER NOT NULL CHECK (low_resolution IN (0, 1)),
    scope_rules                    TEXT NOT NULL CHECK (json_valid(scope_rules)),
    plan_prices                    TEXT NOT NULL CHECK (json_valid(plan_prices)),
    catalog_resolved_at_ms         INTEGER NOT NULL,
    api_twins                      TEXT NOT NULL CHECK (json_valid(api_twins)),
    reasoning_billed_as_output     TEXT NOT NULL CHECK (json_valid(reasoning_billed_as_output)),
    error_deflating                TEXT NOT NULL,
    error_inflating                TEXT NOT NULL,
    -- NULL when the denominator is zero: no multiplier exists, and none is
    -- invented.
    multiplier                     REAL,
    fee_multiplier                 REAL,
    CONSTRAINT multiplier_observation_one_per_day UNIQUE (quota_provider, observation_day)
);

CREATE TRIGGER multiplier_observation_no_update
BEFORE UPDATE ON multiplier_observation
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_no_update');
END;

CREATE TRIGGER multiplier_observation_no_delete
BEFORE DELETE ON multiplier_observation
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_no_delete');
END;
"#,
    },
    Migration {
        version: 16,
        statements: r#"-- A recorded multiplier day can be corrected. Rows stay append-only: a
-- correction is a new row whose `supersedes` names the row it replaces, as a
-- priced_charge chain does. A day's current row (its head) is the one no row
-- supersedes. At most one ORIGINAL row (supersedes IS NULL) exists per
-- provider and day, and a superseding row must name a row of the same
-- provider and day, enforced by the composite foreign key.
--
-- SQLite cannot drop a table constraint, so the table is rebuilt: the old one
-- is renamed aside (its triggers go with it), the new one is created under the
-- final name so its self-reference names it, every row is copied verbatim with
-- supersedes NULL, and the old table and its triggers are dropped.
ALTER TABLE multiplier_observation RENAME TO multiplier_observation_v15;

CREATE TABLE multiplier_observation (
    observation_id                 INTEGER PRIMARY KEY,
    quota_provider                 TEXT NOT NULL,
    observation_day                TEXT NOT NULL,
    snapshot_at_ms                 INTEGER NOT NULL,
    interval_start_ms              INTEGER NOT NULL,
    interval_end_ms                INTEGER NOT NULL,
    -- The interval clipped to the span the quota data covers; NULL when no
    -- configured account has a reading in the interval.
    token_interval_start_ms        INTEGER,
    token_interval_end_ms          INTEGER,
    window_pool_key                TEXT NOT NULL,
    window_minutes                 INTEGER NOT NULL,
    input_tokens                   INTEGER NOT NULL,
    cache_read_tokens              INTEGER NOT NULL,
    cache_write_tokens             INTEGER NOT NULL,
    output_tokens                  INTEGER NOT NULL,
    reasoning_tokens               INTEGER NOT NULL,
    api_nanos                      INTEGER NOT NULL,
    api_nanos_by_model             TEXT NOT NULL CHECK (json_valid(api_nanos_by_model)),
    unpriced_tokens                TEXT NOT NULL CHECK (json_valid(unpriced_tokens)),
    tokens_outside_quota_coverage  INTEGER NOT NULL,
    base_rate_tier_untested_tokens INTEGER NOT NULL,
    subscription_nanos             INTEGER NOT NULL,
    fee_paid_nanos                 INTEGER NOT NULL,
    delta_percent                  REAL NOT NULL,
    accounts_included_count        INTEGER NOT NULL,
    accounts_included              TEXT NOT NULL CHECK (json_valid(accounts_included)),
    accounts_excluded              TEXT NOT NULL CHECK (json_valid(accounts_excluded)),
    excluded_pairs                 TEXT NOT NULL CHECK (json_valid(excluded_pairs)),
    low_resolution                 INTEGER NOT NULL CHECK (low_resolution IN (0, 1)),
    scope_rules                    TEXT NOT NULL CHECK (json_valid(scope_rules)),
    plan_prices                    TEXT NOT NULL CHECK (json_valid(plan_prices)),
    catalog_resolved_at_ms         INTEGER NOT NULL,
    api_twins                      TEXT NOT NULL CHECK (json_valid(api_twins)),
    reasoning_billed_as_output     TEXT NOT NULL CHECK (json_valid(reasoning_billed_as_output)),
    error_deflating                TEXT NOT NULL,
    error_inflating                TEXT NOT NULL,
    -- NULL when the denominator is zero: no multiplier exists, and none is
    -- invented.
    multiplier                     REAL,
    fee_multiplier                 REAL,
    -- NULL on a day's original row; otherwise the row this one replaced.
    -- UNIQUE: a row is superseded at most once, so a day's chain never forks.
    supersedes                     INTEGER UNIQUE
        REFERENCES multiplier_observation(observation_id) ON DELETE RESTRICT,
    CONSTRAINT multiplier_observation_chain_identity
        UNIQUE (observation_id, quota_provider, observation_day),
    CONSTRAINT multiplier_observation_supersedes_same_day_fk
        FOREIGN KEY (supersedes, quota_provider, observation_day)
        REFERENCES multiplier_observation(observation_id, quota_provider, observation_day)
        ON DELETE RESTRICT,
    CONSTRAINT multiplier_observation_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> observation_id)
);

CREATE UNIQUE INDEX multiplier_observation_one_original_per_day
    ON multiplier_observation (quota_provider, observation_day)
    WHERE supersedes IS NULL;

INSERT INTO multiplier_observation (
    observation_id, quota_provider, observation_day, snapshot_at_ms, interval_start_ms,
    interval_end_ms, token_interval_start_ms, token_interval_end_ms, window_pool_key,
    window_minutes, input_tokens, cache_read_tokens, cache_write_tokens, output_tokens,
    reasoning_tokens, api_nanos, api_nanos_by_model, unpriced_tokens,
    tokens_outside_quota_coverage, base_rate_tier_untested_tokens, subscription_nanos,
    fee_paid_nanos, delta_percent, accounts_included_count, accounts_included,
    accounts_excluded, excluded_pairs, low_resolution, scope_rules, plan_prices,
    catalog_resolved_at_ms, api_twins, reasoning_billed_as_output, error_deflating,
    error_inflating, multiplier, fee_multiplier, supersedes
)
SELECT
    observation_id, quota_provider, observation_day, snapshot_at_ms, interval_start_ms,
    interval_end_ms, token_interval_start_ms, token_interval_end_ms, window_pool_key,
    window_minutes, input_tokens, cache_read_tokens, cache_write_tokens, output_tokens,
    reasoning_tokens, api_nanos, api_nanos_by_model, unpriced_tokens,
    tokens_outside_quota_coverage, base_rate_tier_untested_tokens, subscription_nanos,
    fee_paid_nanos, delta_percent, accounts_included_count, accounts_included,
    accounts_excluded, excluded_pairs, low_resolution, scope_rules, plan_prices,
    catalog_resolved_at_ms, api_twins, reasoning_billed_as_output, error_deflating,
    error_inflating, multiplier, fee_multiplier, NULL
FROM multiplier_observation_v15
ORDER BY observation_id;

DROP TABLE multiplier_observation_v15;

CREATE TRIGGER multiplier_observation_no_update
BEFORE UPDATE ON multiplier_observation
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_no_update');
END;

CREATE TRIGGER multiplier_observation_no_delete
BEFORE DELETE ON multiplier_observation
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_no_delete');
END;
"#,
    },
    Migration {
        version: 17,
        statements: r#"-- desired_config.harness_ingest holds the member's canonical JSON: which
-- producers the refresher drains. NULL on a generation committed before this
-- migration means that generation did not record the member; it drained every
-- producer, so it is read as "all on", never as "off".
ALTER TABLE desired_config ADD COLUMN harness_ingest TEXT
    CHECK (harness_ingest IS NULL OR json_valid(harness_ingest));
"#,
    },
    Migration {
        version: 18,
        statements: r#"-- window_sample.breakdown holds the window's per-surface consumption breakdown
-- serialized as JSON exactly as decoded (asOf, windowStartedAt, rows with key
-- and sharePercent, in the order received), or NULL when the window carried none.
-- NULL means insula published no breakdown for that reading, never "no other surface".
ALTER TABLE window_sample ADD COLUMN breakdown TEXT
    CHECK (breakdown IS NULL OR json_valid(breakdown));
"#,
    },
    Migration {
        version: 19,
        statements: r#"-- Validate before changing any durable schema or row. The migration runner
-- executes this entire migration in one transaction. Older SQLite versions
-- require literal RAISE messages; an invalid JSON path carries the computed
-- rule name and violation count in the error instead.
CREATE TEMP TABLE astrocyte_v19_preflight (rule TEXT, violations INTEGER);
CREATE TEMP TRIGGER astrocyte_v19_preflight_refuse
BEFORE INSERT ON astrocyte_v19_preflight
WHEN NEW.violations > 0
BEGIN
    SELECT json_extract('{}', 'migration 19: ' || NEW.rule || ': ' || NEW.violations || ' violating rows');
END;
INSERT INTO astrocyte_v19_preflight
SELECT 'source_runtime_key_nonempty', COUNT(*) FROM source_runtime WHERE source_key IS NULL OR length(source_key)=0
UNION ALL SELECT 'source_runtime_module_nonempty', COUNT(*) FROM source_runtime WHERE length(source_module)=0
UNION ALL SELECT 'source_runtime_ordinals_nonnegative', COUNT(*) FROM source_runtime WHERE consumed_ordinal<0 OR high_water_ordinal<0
UNION ALL SELECT 'source_runtime_high_water', COUNT(*) FROM source_runtime WHERE high_water_ordinal<consumed_ordinal
UNION ALL SELECT 'source_runtime_success_nonnegative', COUNT(*) FROM source_runtime WHERE last_success_ms<0
UNION ALL SELECT 'source_runtime_generation_fk', COUNT(*) FROM source_runtime s WHERE NOT EXISTS (SELECT 1 FROM desired_config d WHERE d.generation=s.desired_generation)
UNION ALL SELECT 'priced_charge_nonnegative', COUNT(*) FROM priced_charge WHERE charge_nanos<0 OR rate_units<0 OR floor_units<0
UNION ALL SELECT 'adjustment_currency_matches_event', COUNT(*) FROM adjustment a WHERE NOT EXISTS (SELECT 1 FROM usage_event e WHERE e.event_id=a.event_id AND e.currency=a.currency)
UNION ALL SELECT 'multiplier_observation_counts_nonnegative', COUNT(*) FROM multiplier_observation WHERE window_minutes<0 OR input_tokens<0 OR cache_read_tokens<0 OR cache_write_tokens<0 OR output_tokens<0 OR reasoning_tokens<0 OR tokens_outside_quota_coverage<0 OR base_rate_tier_untested_tokens<0 OR accounts_included_count<0
UNION ALL SELECT 'multiplier_observation_interval_ordered', COUNT(*) FROM multiplier_observation WHERE interval_start_ms>=interval_end_ms
UNION ALL SELECT 'multiplier_observation_day_matches_end', COUNT(*) FROM multiplier_observation WHERE observation_day IS NOT strftime('%Y-%m-%d', interval_end_ms / 1000.0, 'unixepoch');
DROP TABLE astrocyte_v19_preflight;

-- Only the small runtime table is rebuilt; ledger tables retain their roots.
CREATE TABLE source_runtime_v19 (
    source_key TEXT NOT NULL PRIMARY KEY CONSTRAINT source_runtime_key_nonempty CHECK (length(source_key)>0),
    source_module TEXT NOT NULL CONSTRAINT source_runtime_module_nonempty CHECK (length(source_module)>0),
    desired_generation INTEGER NOT NULL REFERENCES desired_config(generation) DEFERRABLE INITIALLY DEFERRED,
    lineage TEXT,
    consumed_ordinal INTEGER,
    high_water_ordinal INTEGER,
    last_success_ms INTEGER CONSTRAINT source_runtime_success_nonnegative CHECK (last_success_ms IS NULL OR last_success_ms>=0),
    pending_known INTEGER NOT NULL CHECK (pending_known>=0),
    pending_unknown INTEGER NOT NULL CHECK (pending_unknown IN (0,1)),
    CONSTRAINT source_runtime_ordinals_nonnegative CHECK (
        (consumed_ordinal IS NULL OR consumed_ordinal>=0) AND (high_water_ordinal IS NULL OR high_water_ordinal>=0)),
    CONSTRAINT source_runtime_high_water CHECK (
        high_water_ordinal IS NULL OR consumed_ordinal IS NULL OR high_water_ordinal>=consumed_ordinal),
    CONSTRAINT source_runtime_state_complete CHECK (
        (lineage IS NULL AND consumed_ordinal IS NULL AND high_water_ordinal IS NULL
         AND last_success_ms IS NULL AND pending_known=0 AND pending_unknown=1)
        OR
        (lineage IS NULL AND consumed_ordinal IS NULL AND high_water_ordinal IS NULL
         AND last_success_ms IS NOT NULL AND pending_known=0 AND pending_unknown IN (0,1))
        OR
        (lineage IS NOT NULL AND consumed_ordinal IS NOT NULL AND high_water_ordinal IS NOT NULL
         AND last_success_ms IS NOT NULL)
    )
);
INSERT INTO source_runtime_v19 SELECT * FROM source_runtime ORDER BY source_key;
DROP TABLE source_runtime;
ALTER TABLE source_runtime_v19 RENAME TO source_runtime;

CREATE TRIGGER priced_charge_nonnegative_insert
BEFORE INSERT ON priced_charge
WHEN NEW.charge_nanos<0 OR NEW.rate_units<0 OR NEW.floor_units<0
BEGIN SELECT RAISE(ABORT, 'priced_charge_nonnegative'); END;
CREATE TRIGGER priced_charge_nonnegative_update
BEFORE UPDATE ON priced_charge
WHEN NEW.charge_nanos<0 OR NEW.rate_units<0 OR NEW.floor_units<0
BEGIN SELECT RAISE(ABORT, 'priced_charge_nonnegative'); END;

CREATE TRIGGER adjustment_currency_matches_event_insert
BEFORE INSERT ON adjustment
WHEN NOT EXISTS (SELECT 1 FROM usage_event e WHERE e.event_id=NEW.event_id AND e.currency=NEW.currency)
BEGIN SELECT RAISE(ABORT, 'adjustment_currency_matches_event'); END;
CREATE TRIGGER adjustment_currency_matches_event_update
BEFORE UPDATE ON adjustment
WHEN NOT EXISTS (SELECT 1 FROM usage_event e WHERE e.event_id=NEW.event_id AND e.currency=NEW.currency)
BEGIN SELECT RAISE(ABORT, 'adjustment_currency_matches_event'); END;

CREATE TRIGGER multiplier_observation_counts_nonnegative_insert
BEFORE INSERT ON multiplier_observation
WHEN NEW.window_minutes<0 OR NEW.input_tokens<0 OR NEW.cache_read_tokens<0 OR NEW.cache_write_tokens<0
 OR NEW.output_tokens<0 OR NEW.reasoning_tokens<0 OR NEW.tokens_outside_quota_coverage<0
 OR NEW.base_rate_tier_untested_tokens<0 OR NEW.accounts_included_count<0
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_counts_nonnegative'); END;
CREATE TRIGGER multiplier_observation_counts_nonnegative_update
BEFORE UPDATE ON multiplier_observation
WHEN NEW.window_minutes<0 OR NEW.input_tokens<0 OR NEW.cache_read_tokens<0 OR NEW.cache_write_tokens<0
 OR NEW.output_tokens<0 OR NEW.reasoning_tokens<0 OR NEW.tokens_outside_quota_coverage<0
 OR NEW.base_rate_tier_untested_tokens<0 OR NEW.accounts_included_count<0
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_counts_nonnegative'); END;

CREATE TRIGGER multiplier_observation_interval_ordered_insert
BEFORE INSERT ON multiplier_observation
WHEN NEW.interval_start_ms>=NEW.interval_end_ms
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_interval_ordered'); END;
CREATE TRIGGER multiplier_observation_interval_ordered_update
BEFORE UPDATE ON multiplier_observation
WHEN NEW.interval_start_ms>=NEW.interval_end_ms
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_interval_ordered'); END;

CREATE TRIGGER multiplier_observation_day_matches_end_insert
BEFORE INSERT ON multiplier_observation
WHEN NEW.observation_day IS NOT strftime('%Y-%m-%d', NEW.interval_end_ms / 1000.0, 'unixepoch')
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_day_matches_end'); END;
CREATE TRIGGER multiplier_observation_day_matches_end_update
BEFORE UPDATE ON multiplier_observation
WHEN NEW.observation_day IS NOT strftime('%Y-%m-%d', NEW.interval_end_ms / 1000.0, 'unixepoch')
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_day_matches_end'); END;

CREATE INDEX usage_event_statement_match ON usage_event (provider, account_ref, producer_ref);
CREATE INDEX adjustment_event_lookup ON adjustment (event_id);
-- The backfill reads a tiny unpriced subset in event order and probes children
-- using the existing supersedes index. Cover that selector, not all charges.
CREATE INDEX priced_charge_subscription_backfill ON priced_charge (event_id, charge_id)
    WHERE outcome='unpriced' AND unpriced_reason='no_subscription_in_force';
-- Equality scope columns precede the time range; model discovery is covered.
CREATE INDEX usage_event_multiplier_scope_time ON usage_event (producer, provider, occurred_at_ms, model);
"#,
    },
    Migration {
        version: 20,
        statements: r#"-- Caller keys distinguish correction retries from separate equal adjustments.
-- Internal statement settlements and historical adjustments have no caller key.
ALTER TABLE adjustment ADD COLUMN correction_id TEXT CHECK (
    correction_id IS NULL OR (
        typeof(correction_id) = 'text'
        AND length(CAST(correction_id AS BLOB)) BETWEEN 1 AND 128
    )
);
CREATE UNIQUE INDEX adjustment_correction_id
    ON adjustment (correction_id) WHERE correction_id IS NOT NULL;
"#,
    },
    Migration {
        version: 21,
        statements: r#"CREATE TABLE set_aside_record (
    source_key TEXT NOT NULL,
    locator TEXT NOT NULL,
    reason TEXT NOT NULL,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL CHECK (last_seen_ms >= first_seen_ms),
    PRIMARY KEY (source_key, locator)
);
"#,
    },
    Migration {
        version: 22,
        statements: r#"CREATE TRIGGER adjustment_no_update
BEFORE UPDATE ON adjustment
BEGIN
    SELECT RAISE(ABORT, 'adjustment_no_update');
END;

CREATE TRIGGER adjustment_no_delete
BEFORE DELETE ON adjustment
BEGIN
    SELECT RAISE(ABORT, 'adjustment_no_delete');
END;
"#,
    },
    Migration {
        version: 23,
        statements: r#"-- Offline rebuild: stop readers and writers and keep a rollback backup first.
-- Integer keys are local locators, never served identities. Event digest bytes stay
-- unchanged; charge digests are intentionally retired. Run VACUUM offline after
-- this transaction commits, then start a new full backup baseline.
-- The runner owns the transaction. migrate_store suspends foreign keys outside
-- that transaction; triggers are absent only during copying. Validate every
-- rebuilt FK before returning. Physical integrity is checked offline by operators.
-- The larger cache avoids repeatedly evicting pages while deleting the old
-- hash indexes and checking the replacement. Restore the normal 2 MiB limit below.
PRAGMA cache_size=-262144;
CREATE TEMP TABLE astrocyte_v23_check (rule TEXT, violations INTEGER);
CREATE TEMP TRIGGER astrocyte_v23_refuse
BEFORE INSERT ON astrocyte_v23_check
WHEN NEW.violations > 0
BEGIN
    SELECT json_extract('{}', 'migration 23: ' || NEW.rule || ': ' || NEW.violations || ' violating rows');
END;
INSERT INTO astrocyte_v23_check SELECT 'pre_measurement_event', count(*)
FROM measurement c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_charge_event', count(*)
FROM priced_charge c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_pending_event', count(*)
FROM pricing_pending c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_transition_event', count(*)
FROM chargeability_transition c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_adjustment_event', count(*)
FROM adjustment c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_quarantine_event', count(*)
FROM quarantine_evidence c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_quarantine_target', count(*)
FROM quarantine_evidence c LEFT JOIN usage_event e ON e.event_id=c.target_event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_rejected_event', count(*)
FROM rejected_delivery c LEFT JOIN usage_event e ON e.event_id=c.stored_event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_redelivery_event', count(*)
FROM redelivery_log c LEFT JOIN usage_event e ON e.event_id=c.event_id WHERE e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_statement_event', count(*)
FROM statement_line c LEFT JOIN usage_event e ON e.event_id=c.matched_event_id
WHERE c.matched_event_id IS NOT NULL AND e.event_id IS NULL;
INSERT INTO astrocyte_v23_check SELECT 'pre_match_event', count(*)
FROM match_attempt c LEFT JOIN usage_event e ON e.event_id=c.event_id
WHERE c.event_id IS NOT NULL AND e.event_id IS NULL;
INSERT INTO astrocyte_v23_check
SELECT 'pre_same_measurement_predecessor', count(*) FROM priced_charge c
WHERE c.supersedes IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM priced_charge p WHERE p.charge_id=c.supersedes
    AND p.event_id=c.event_id AND p.role=c.role AND p.unit=c.unit
);
PRAGMA defer_foreign_keys=ON;
CREATE TEMP TABLE astrocyte_v23_counts AS
SELECT 'usage_event' AS name, count(*) AS rows FROM usage_event
UNION ALL
SELECT 'measurement' AS name, count(*) AS rows FROM measurement
UNION ALL
SELECT 'priced_charge' AS name, count(*) AS rows FROM priced_charge
UNION ALL
SELECT 'pricing_pending' AS name, count(*) AS rows FROM pricing_pending
UNION ALL
SELECT 'chargeability_transition' AS name, count(*) AS rows FROM chargeability_transition
UNION ALL
SELECT 'adjustment' AS name, count(*) AS rows FROM adjustment
UNION ALL
SELECT 'quarantine_evidence' AS name, count(*) AS rows FROM quarantine_evidence
UNION ALL
SELECT 'rejected_delivery' AS name, count(*) AS rows FROM rejected_delivery
UNION ALL
SELECT 'redelivery_log' AS name, count(*) AS rows FROM redelivery_log
UNION ALL
SELECT 'statement_line' AS name, count(*) AS rows FROM statement_line
UNION ALL
SELECT 'match_attempt' AS name, count(*) AS rows FROM match_attempt;
DROP TRIGGER account_subscription_no_delete;
DROP TRIGGER account_subscription_no_overlap;
DROP TRIGGER account_subscription_update_guard;
DROP TRIGGER adjustment_currency_matches_event_insert;
DROP TRIGGER adjustment_currency_matches_event_update;
DROP TRIGGER chargeability_transition_no_delete;
DROP TRIGGER chargeability_transition_no_update;
DROP TRIGGER match_attempt_no_delete;
DROP TRIGGER match_attempt_no_update;
DROP TRIGGER measurement_no_delete;
DROP TRIGGER measurement_no_update;
DROP TRIGGER multiplier_observation_counts_nonnegative_insert;
DROP TRIGGER multiplier_observation_counts_nonnegative_update;
DROP TRIGGER multiplier_observation_day_matches_end_insert;
DROP TRIGGER multiplier_observation_day_matches_end_update;
DROP TRIGGER multiplier_observation_interval_ordered_insert;
DROP TRIGGER multiplier_observation_interval_ordered_update;
DROP TRIGGER multiplier_observation_no_delete;
DROP TRIGGER multiplier_observation_no_update;
DROP TRIGGER priced_charge_matches_event_generation_and_basis;
DROP TRIGGER priced_charge_no_delete;
DROP TRIGGER priced_charge_no_update;
DROP TRIGGER priced_charge_nonnegative_insert;
DROP TRIGGER priced_charge_nonnegative_update;
DROP TRIGGER statement_import_no_delete;
DROP TRIGGER statement_import_no_update;
DROP TRIGGER statement_line_matches_statement;
DROP TRIGGER statement_line_no_delete;
DROP TRIGGER statement_line_update_guard;
DROP TRIGGER statement_no_delete;
DROP TRIGGER statement_no_update;
DROP TRIGGER usage_event_currency_starts_null;
DROP TRIGGER usage_event_no_delete;
DROP TRIGGER usage_event_pricing_generation_starts_zero;
DROP TRIGGER usage_event_update_guard;
DROP TRIGGER adjustment_no_update;
DROP TRIGGER adjustment_no_delete;

CREATE TABLE "__compact_usage_event" (
    event_key INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    producer           TEXT NOT NULL,
    -- NULL when the producer could not observe its own generation. The
    -- absence is stored, not smoothed over, so a reader can see the identity
    -- is weaker rather than having to infer it.
    producer_lineage   TEXT,
    producer_ref       TEXT NOT NULL,
    prior_event_ref    TEXT,

    harness            TEXT,
    project_root       TEXT,
    session            TEXT,
    run_ref            TEXT,
    work_class         TEXT,

    occurred_at_ms     INTEGER,
    emitted_at_ms      INTEGER,
    recorded_at_ms     INTEGER NOT NULL,
    terminal_status    TEXT,
    attempt_ordinal    INTEGER,

    vendor_request_id  TEXT,
    provider           TEXT,
    model              TEXT,
    account_ref        TEXT,

    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    pricing_generation INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation >= 0),
    currency           TEXT, auth_selection TEXT, charge_basis_mapping_version INTEGER, charge_basis_overrides TEXT,

    CONSTRAINT usage_event_natural_key
        UNIQUE (producer, producer_lineage, producer_ref),
    CONSTRAINT usage_event_attempt_ordinal_nonnegative
        CHECK (
            attempt_ordinal IS NULL
            OR (typeof(attempt_ordinal) = 'integer' AND attempt_ordinal >= 0)
        )
);
CREATE UNIQUE INDEX astrocyte_v23_currency ON __compact_usage_event(event_key,currency);
INSERT INTO "__compact_usage_event" ("event_key","event_id","producer","producer_lineage","producer_ref","prior_event_ref","harness","project_root","session","run_ref","work_class","occurred_at_ms","emitted_at_ms","recorded_at_ms","terminal_status","attempt_ordinal","vendor_request_id","provider","model","account_ref","charge_basis","pricing_generation","currency","auth_selection","charge_basis_mapping_version","charge_basis_overrides")
SELECT s.rowid,s."event_id",s."producer",s."producer_lineage",s."producer_ref",s."prior_event_ref",s."harness",s."project_root",s."session",s."run_ref",s."work_class",s."occurred_at_ms",s."emitted_at_ms",s."recorded_at_ms",s."terminal_status",s."attempt_ordinal",s."vendor_request_id",s."provider",s."model",s."account_ref",s."charge_basis",s."pricing_generation",s."currency",s."auth_selection",s."charge_basis_mapping_version",s."charge_basis_overrides" FROM "usage_event" s  ORDER BY s.rowid;

CREATE TABLE "__compact_measurement" (
    event_key           INTEGER NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    kind               TEXT NOT NULL,
    source             TEXT NOT NULL,
    quantity_units     INTEGER,
    quantity_exponent  INTEGER,

    PRIMARY KEY (event_key, role, unit),
    CONSTRAINT measurement_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT,
    CONSTRAINT measurement_quantity_presence
        CHECK (
            (quantity_units IS NULL AND quantity_exponent IS NULL)
            OR (quantity_units IS NOT NULL AND quantity_exponent IS NOT NULL)
        ),
    CONSTRAINT measurement_quantity_units_nonnegative_integer
        CHECK (
            quantity_units IS NULL
            OR (typeof(quantity_units) = 'integer' AND quantity_units >= 0)
        ),
    CONSTRAINT measurement_quantity_exponent_canonical
        CHECK (
            quantity_exponent IS NULL
            OR (
                typeof(quantity_exponent) = 'integer'
                AND quantity_exponent BETWEEN -9 AND 0
            )
        )
);
INSERT INTO "__compact_measurement" ("event_key","role","unit","kind","source","quantity_units","quantity_exponent")
SELECT map_event_id.rowid,s."role",s."unit",s."kind",s."source",s."quantity_units",s."quantity_exponent" FROM "measurement" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_priced_charge" (
    charge_key INTEGER PRIMARY KEY,
    event_key           INTEGER NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    pricing_generation INTEGER NOT NULL
        CHECK (typeof(pricing_generation) = 'integer' AND pricing_generation > 0),
    outcome            TEXT NOT NULL
        CHECK (outcome IN ('priced', 'covered', 'unpriced')),
    charge_basis       TEXT NOT NULL
        CHECK (charge_basis IN ('metered_api', 'subscription_included', 'unknown')),
    basis_covered      INTEGER NOT NULL
        CHECK (basis_covered IN (0, 1)),
    currency           TEXT,
    charge_nanos       INTEGER,
    unpriced_reason    TEXT
        CHECK (
            unpriced_reason IS NULL
            OR unpriced_reason IN (
                'undated_event', 'charge_basis_unknown', 'no_subscription_in_force',
                'missing_catalog_identity', 'catalog_pre_history', 'no_rate',
                'no_catalog_coverage', 'catalog_unconfigured', 'catalog_unknown_charge_basis',
                'missing_quantity', 'rate_tiered', 'rate_time_banded',
                'rate_expired', 'rate_withheld',
                'rate_uncertain', 'stated_zero_requires_policy',
                'arithmetic_out_of_range', 'rate_inherited'
            )
        ),
    rate_units         INTEGER,
    rate_exponent      INTEGER,
    floor_state        TEXT CHECK (floor_state IN ('unknown', 'none', 'minimum')),
    floor_units        INTEGER,
    catalog_source     TEXT,
    resolved_at_ms     INTEGER
        CHECK (
            resolved_at_ms IS NULL
            OR (typeof(resolved_at_ms) = 'integer' AND resolved_at_ms >= 0)
        ),
    catalog_entry      TEXT
        CHECK (
            catalog_entry IS NULL
            OR (json_valid(catalog_entry) AND json_type(catalog_entry) = 'object')
        ),
    supersedes         INTEGER,
    origin             TEXT NOT NULL
        CHECK (origin IN ('pricing_pass', 'operator_reprice', 'unattributed', 'basis_correction')),
    recorded_at_ms     INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),
    CONSTRAINT priced_charge_measurement_fk
        FOREIGN KEY (event_key, role, unit)
        REFERENCES "__compact_measurement"(event_key, role, unit) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_currency_fk
        FOREIGN KEY (event_key, currency)
        REFERENCES "__compact_usage_event"(event_key, currency) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_supersedes_same_measurement_fk
        FOREIGN KEY (supersedes)
        REFERENCES "__compact_priced_charge"(charge_key) ON DELETE RESTRICT,
    CONSTRAINT priced_charge_not_self_superseding
        CHECK (supersedes IS NULL OR supersedes <> charge_key),
    CONSTRAINT priced_charge_floor_shape
        CHECK ((floor_state = 'minimum') = (floor_units IS NOT NULL)),
    CONSTRAINT priced_charge_outcome_shape
        CHECK (
            (
                outcome = 'priced'
                AND charge_basis = 'metered_api'
                AND basis_covered = 0
                AND currency IS NOT NULL
                AND typeof(charge_nanos) = 'integer'
                AND unpriced_reason IS NULL
                AND typeof(rate_units) = 'integer'
                AND rate_exponent = 9
                AND floor_state IS NOT NULL
                AND catalog_source IS NOT NULL
                AND resolved_at_ms IS NOT NULL
                AND catalog_entry IS NOT NULL
            )
            OR (
                outcome = 'covered'
                AND charge_basis = 'subscription_included'
                AND basis_covered = 1
                AND currency IS NOT NULL
                AND charge_nanos = 0
                AND unpriced_reason IS NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
                AND catalog_source IS NULL
                AND resolved_at_ms IS NULL
                AND catalog_entry IS NULL
            )
            OR (
                outcome = 'unpriced'
                AND basis_covered = 0
                AND currency IS NULL
                AND charge_nanos IS NULL
                AND unpriced_reason IS NOT NULL
                AND rate_units IS NULL
                AND rate_exponent IS NULL
                AND floor_state IS NULL
                AND floor_units IS NULL
            )
        )
);
INSERT INTO "__compact_priced_charge" ("charge_key","event_key","role","unit","pricing_generation","outcome","charge_basis","basis_covered","currency","charge_nanos","unpriced_reason","rate_units","rate_exponent","floor_state","floor_units","catalog_source","resolved_at_ms","catalog_entry","supersedes","origin","recorded_at_ms")
SELECT s.rowid,map_event_id.rowid,s."role",s."unit",s."pricing_generation",s."outcome",s."charge_basis",s."basis_covered",s."currency",s."charge_nanos",s."unpriced_reason",s."rate_units",s."rate_exponent",s."floor_state",s."floor_units",s."catalog_source",s."resolved_at_ms",s."catalog_entry",map_charge.rowid,s."origin",s."recorded_at_ms" FROM "priced_charge" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" LEFT JOIN priced_charge map_charge ON map_charge.charge_id=s.supersedes ORDER BY s.rowid;

CREATE TABLE "__compact_pricing_pending" (
    event_key INTEGER PRIMARY KEY,
    CONSTRAINT pricing_pending_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT
) WITHOUT ROWID;
INSERT INTO "__compact_pricing_pending" ("event_key")
SELECT map_event_id.rowid FROM "pricing_pending" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" ORDER BY s.event_id;

-- A transition must name its event; NULL must not auto-allocate an integer key.
CREATE TABLE "__compact_chargeability_transition" (
    event_key        INTEGER NOT NULL PRIMARY KEY,
    from_state      TEXT NOT NULL,
    to_state        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    resolved_by     TEXT NOT NULL,
    occurred_at_ms  INTEGER NOT NULL,

    CONSTRAINT chargeability_transition_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT,
    CONSTRAINT chargeability_transition_starts_unresolved
        CHECK (from_state = 'unresolved'),
    CONSTRAINT chargeability_transition_terminal_target
        CHECK (to_state IN ('billed', 'written_off', 'unbillable')),
    CONSTRAINT chargeability_transition_authority_vocabulary
        CHECK (resolved_by IN ('correction', 'later_record', 'statement', 'operator')),
    CONSTRAINT chargeability_transition_authority_target
        CHECK (
            (to_state = 'billed' AND resolved_by IN ('later_record', 'statement'))
            OR (to_state = 'written_off' AND resolved_by IN ('correction', 'operator'))
            OR (to_state = 'unbillable' AND resolved_by = 'operator')
        )
) WITHOUT ROWID;
INSERT INTO "__compact_chargeability_transition" ("event_key","from_state","to_state","reason","resolved_by","occurred_at_ms")
SELECT map_event_id.rowid,s."from_state",s."to_state",s."reason",s."resolved_by",s."occurred_at_ms" FROM "chargeability_transition" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_adjustment" (
    adjustment_id  TEXT NOT NULL PRIMARY KEY,
    event_key       INTEGER NOT NULL,
    currency       TEXT NOT NULL,
    amount_nanos   INTEGER NOT NULL
        CHECK (typeof(amount_nanos) = 'integer'),
    reason         TEXT NOT NULL
        CHECK (reason IN ('write_off', 'vendor_restatement', 'our_repricing', 'refund', 'goodwill')),
    occurred_at_ms INTEGER NOT NULL
        CHECK (typeof(occurred_at_ms) = 'integer' AND occurred_at_ms >= 0),
    recorded_at_ms INTEGER NOT NULL
        CHECK (typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0),
    origin         TEXT NOT NULL
        CHECK (origin IN ('operator', 'statement', 'correction_ingest')),
    operator       TEXT,
    note           TEXT, correction_id TEXT CHECK (
    correction_id IS NULL OR (
        typeof(correction_id) = 'text'
        AND length(CAST(correction_id AS BLOB)) BETWEEN 1 AND 128
    )
),

    CONSTRAINT adjustment_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT
);
INSERT INTO "__compact_adjustment" ("adjustment_id","event_key","currency","amount_nanos","reason","occurred_at_ms","recorded_at_ms","origin","operator","note","correction_id")
SELECT s."adjustment_id",map_event_id.rowid,s."currency",s."amount_nanos",s."reason",s."occurred_at_ms",s."recorded_at_ms",s."origin",s."operator",s."note",s."correction_id" FROM "adjustment" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_quarantine_evidence" (
    event_key           INTEGER NOT NULL,
    target_event_key    INTEGER NOT NULL,
    role               TEXT NOT NULL,
    unit               TEXT NOT NULL,
    stored_quantity    TEXT NOT NULL
        CHECK (json_valid(stored_quantity) AND json_type(stored_quantity) = 'object'),
    incoming_quantity  TEXT NOT NULL
        CHECK (json_valid(incoming_quantity) AND json_type(incoming_quantity) = 'object'),
    detected_at_ms     INTEGER NOT NULL
        CHECK (typeof(detected_at_ms) = 'integer' AND detected_at_ms >= 0),

    PRIMARY KEY (event_key, target_event_key, role, unit),
    CONSTRAINT quarantine_evidence_distinct_events CHECK (event_key <> target_event_key),
    CONSTRAINT quarantine_evidence_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT,
    CONSTRAINT quarantine_evidence_target_fk
        FOREIGN KEY (target_event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT
);
INSERT INTO "__compact_quarantine_evidence" ("event_key","target_event_key","role","unit","stored_quantity","incoming_quantity","detected_at_ms")
SELECT map_event_id.rowid,map_target_event_id.rowid,s."role",s."unit",s."stored_quantity",s."incoming_quantity",s."detected_at_ms" FROM "quarantine_evidence" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" LEFT JOIN usage_event map_target_event_id ON map_target_event_id.event_id=s."target_event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_rejected_delivery" (
    producer           TEXT NOT NULL,
    producer_lineage   TEXT,
    producer_ref       TEXT NOT NULL,
    incoming_payload   TEXT NOT NULL
        CHECK (json_valid(incoming_payload) AND json_type(incoming_payload) = 'object'),
    stored_event_key    INTEGER NOT NULL,
    detected_at_ms     INTEGER NOT NULL
        CHECK (typeof(detected_at_ms) = 'integer' AND detected_at_ms >= 0),

    CONSTRAINT rejected_delivery_stored_event_fk
        FOREIGN KEY (stored_event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT
);
INSERT INTO "__compact_rejected_delivery" ("producer","producer_lineage","producer_ref","incoming_payload","stored_event_key","detected_at_ms")
SELECT s."producer",s."producer_lineage",s."producer_ref",s."incoming_payload",map_stored_event_id.rowid,s."detected_at_ms" FROM "rejected_delivery" s LEFT JOIN usage_event map_stored_event_id ON map_stored_event_id.event_id=s."stored_event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_redelivery_log" (
    producer           TEXT NOT NULL,
    producer_lineage   TEXT,
    producer_ref       TEXT NOT NULL,
    event_key           INTEGER NOT NULL,
    detected_at_ms     INTEGER NOT NULL
        CHECK (typeof(detected_at_ms) = 'integer' AND detected_at_ms >= 0),

    CONSTRAINT redelivery_log_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT
);
INSERT INTO "__compact_redelivery_log" ("producer","producer_lineage","producer_ref","event_key","detected_at_ms")
SELECT s."producer",s."producer_lineage",s."producer_ref",map_event_id.rowid,s."detected_at_ms" FROM "redelivery_log" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_statement_line" (
    statement_line_id  TEXT NOT NULL PRIMARY KEY,
    statement_id       TEXT NOT NULL,
    line_id            TEXT NOT NULL CHECK (length(line_id) > 0),
    occurred_at_ms     INTEGER NOT NULL
        CHECK (typeof(occurred_at_ms) = 'integer' AND occurred_at_ms >= 0),
    description        TEXT NOT NULL,
    quantity           INTEGER NOT NULL
        CHECK (typeof(quantity) = 'integer' AND quantity >= 0),
    unit               TEXT NOT NULL CHECK (length(unit) > 0),
    amount_minor       INTEGER NOT NULL CHECK (typeof(amount_minor) = 'integer'),
    currency           TEXT NOT NULL CHECK (length(currency) > 0),
    correlation_ref    TEXT,
    match_outcome      TEXT NOT NULL
        CHECK (match_outcome IN (
            'unmatched', 'settled', 'ambiguous', 'already-settled',
            'unpriced_target', 'currency_mismatch'
        )),
    matched_event_key   INTEGER,

    CONSTRAINT statement_line_identity UNIQUE (statement_id, line_id),
    CONSTRAINT statement_line_statement_fk
        FOREIGN KEY (statement_id) REFERENCES statement(statement_id) ON DELETE RESTRICT,
    CONSTRAINT statement_line_event_fk
        FOREIGN KEY (matched_event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT,
    CONSTRAINT statement_line_match_shape CHECK (
        (match_outcome IN ('settled', 'unpriced_target', 'currency_mismatch')
            AND matched_event_key IS NOT NULL)
        OR (match_outcome IN ('unmatched', 'ambiguous') AND matched_event_key IS NULL)
        OR match_outcome = 'already-settled'
    )
);
INSERT INTO "__compact_statement_line" ("statement_line_id","statement_id","line_id","occurred_at_ms","description","quantity","unit","amount_minor","currency","correlation_ref","match_outcome","matched_event_key")
SELECT s."statement_line_id",s."statement_id",s."line_id",s."occurred_at_ms",s."description",s."quantity",s."unit",s."amount_minor",s."currency",s."correlation_ref",s."match_outcome",map_matched_event_id.rowid FROM "statement_line" s LEFT JOIN usage_event map_matched_event_id ON map_matched_event_id.event_id=s."matched_event_id" ORDER BY s.rowid;

CREATE TABLE "__compact_match_attempt" (
    attempt_id          TEXT NOT NULL PRIMARY KEY,
    statement_line_id   TEXT NOT NULL,
    outcome             TEXT NOT NULL
        CHECK (outcome IN (
            'unmatched', 'settled', 'ambiguous', 'already-settled',
            'unpriced_target', 'currency_mismatch'
        )),
    event_key            INTEGER,
    ledger_amount_nanos INTEGER CHECK (
        ledger_amount_nanos IS NULL OR typeof(ledger_amount_nanos) = 'integer'
    ),
    vendor_amount_nanos INTEGER NOT NULL CHECK (typeof(vendor_amount_nanos) = 'integer'),
    attempted_at_ms     INTEGER NOT NULL
        CHECK (typeof(attempted_at_ms) = 'integer' AND attempted_at_ms >= 0),

    CONSTRAINT match_attempt_line_fk
        FOREIGN KEY (statement_line_id)
        REFERENCES "__compact_statement_line"(statement_line_id) ON DELETE RESTRICT,
    CONSTRAINT match_attempt_event_fk
        FOREIGN KEY (event_key) REFERENCES "__compact_usage_event"(event_key) ON DELETE RESTRICT,
    CONSTRAINT match_attempt_identified_outcome CHECK (
        (outcome IN ('settled', 'unpriced_target', 'currency_mismatch')
            AND event_key IS NOT NULL AND ledger_amount_nanos IS NOT NULL)
        OR (outcome IN ('unmatched', 'ambiguous')
            AND event_key IS NULL AND ledger_amount_nanos IS NULL)
        OR (outcome = 'already-settled'
            AND ((event_key IS NULL AND ledger_amount_nanos IS NULL)
                OR (event_key IS NOT NULL AND ledger_amount_nanos IS NOT NULL)))
    )
);
INSERT INTO "__compact_match_attempt" ("attempt_id","statement_line_id","outcome","event_key","ledger_amount_nanos","vendor_amount_nanos","attempted_at_ms")
SELECT s."attempt_id",s."statement_line_id",s."outcome",map_event_id.rowid,s."ledger_amount_nanos",s."vendor_amount_nanos",s."attempted_at_ms" FROM "match_attempt" s LEFT JOIN usage_event map_event_id ON map_event_id.event_id=s."event_id" ORDER BY s.rowid;

DROP TABLE "match_attempt";

DROP TABLE "statement_line";

DROP TABLE "adjustment";

DROP TABLE "chargeability_transition";

DROP TABLE "quarantine_evidence";

DROP TABLE "rejected_delivery";

DROP TABLE "redelivery_log";

DROP TABLE "pricing_pending";

DROP TABLE "priced_charge";

DROP TABLE "measurement";

DROP TABLE "usage_event";
ALTER TABLE "__compact_usage_event" RENAME TO "usage_event";
ALTER TABLE "__compact_measurement" RENAME TO "measurement";
ALTER TABLE "__compact_priced_charge" RENAME TO "priced_charge";
ALTER TABLE "__compact_pricing_pending" RENAME TO "pricing_pending";
ALTER TABLE "__compact_chargeability_transition" RENAME TO "chargeability_transition";
ALTER TABLE "__compact_adjustment" RENAME TO "adjustment";
ALTER TABLE "__compact_quarantine_evidence" RENAME TO "quarantine_evidence";
ALTER TABLE "__compact_rejected_delivery" RENAME TO "rejected_delivery";
ALTER TABLE "__compact_redelivery_log" RENAME TO "redelivery_log";
ALTER TABLE "__compact_statement_line" RENAME TO "statement_line";
ALTER TABLE "__compact_match_attempt" RENAME TO "match_attempt";
DROP INDEX astrocyte_v23_currency;
CREATE UNIQUE INDEX priced_charge_supersedes_identity ON priced_charge(supersedes);

CREATE UNIQUE INDEX priced_charge_generation_identity ON priced_charge(event_key,role,unit,pricing_generation);

CREATE UNIQUE INDEX adjustment_correction_id
    ON adjustment (correction_id) WHERE correction_id IS NOT NULL;

CREATE INDEX adjustment_event_lookup ON adjustment (event_key);

CREATE INDEX match_attempt_line_history
    ON match_attempt (statement_line_id, attempted_at_ms, attempt_id);

CREATE UNIQUE INDEX priced_charge_one_chain_root
    ON priced_charge (event_key, role, unit)
    WHERE supersedes IS NULL;

CREATE INDEX priced_charge_subscription_backfill ON priced_charge (event_key, charge_key)
    WHERE outcome='unpriced' AND unpriced_reason='no_subscription_in_force';

CREATE UNIQUE INDEX usage_event_currency_identity
    ON usage_event (event_key, currency);

CREATE INDEX usage_event_multiplier_scope_time ON usage_event (producer, provider, occurred_at_ms, model);

CREATE UNIQUE INDEX usage_event_natural_key_lineage_absent
    ON usage_event (producer, producer_ref)
    WHERE producer_lineage IS NULL;

CREATE INDEX usage_event_prior_event_lookup
    ON usage_event (producer, producer_lineage, prior_event_ref)
    WHERE prior_event_ref IS NOT NULL;

CREATE INDEX usage_event_statement_match ON usage_event (provider, account_ref, producer_ref);

CREATE TRIGGER account_subscription_no_delete
BEFORE DELETE ON account_subscription
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_no_delete');
END;

CREATE TRIGGER account_subscription_no_overlap
BEFORE INSERT ON account_subscription
WHEN EXISTS (
    SELECT 1 FROM account_subscription AS existing
    WHERE existing.provider = NEW.provider
      AND existing.account_ref = NEW.account_ref
      AND (existing.effective_to_ms IS NULL
           OR NEW.effective_from_ms < existing.effective_to_ms)
      AND (NEW.effective_to_ms IS NULL
           OR existing.effective_from_ms < NEW.effective_to_ms)
)
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_no_overlap');
END;

CREATE TRIGGER account_subscription_update_guard
BEFORE UPDATE ON account_subscription
WHEN NOT (
    NEW.account_ref IS OLD.account_ref
    AND NEW.provider IS OLD.provider
    AND NEW.plan_name IS OLD.plan_name
    AND NEW.recurring_cost_minor IS OLD.recurring_cost_minor
    AND NEW.currency IS OLD.currency
    AND NEW.amount_exponent IS OLD.amount_exponent
    AND NEW.period IS OLD.period
    AND NEW.effective_from_ms IS OLD.effective_from_ms
    AND NEW.operator IS OLD.operator
    AND OLD.effective_to_ms IS NULL
    AND NEW.effective_to_ms IS NOT NULL
    AND NEW.effective_to_ms > OLD.effective_from_ms
)
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_update_guard');
END;

CREATE TRIGGER adjustment_currency_matches_event_insert
BEFORE INSERT ON adjustment
WHEN NOT EXISTS (SELECT 1 FROM usage_event e WHERE e.event_key=NEW.event_key AND e.currency=NEW.currency)
BEGIN SELECT RAISE(ABORT, 'adjustment_currency_matches_event'); END;

CREATE TRIGGER adjustment_currency_matches_event_update
BEFORE UPDATE ON adjustment
WHEN NOT EXISTS (SELECT 1 FROM usage_event e WHERE e.event_key=NEW.event_key AND e.currency=NEW.currency)
BEGIN SELECT RAISE(ABORT, 'adjustment_currency_matches_event'); END;

CREATE TRIGGER chargeability_transition_no_delete
BEFORE DELETE ON chargeability_transition
BEGIN
    SELECT RAISE(ABORT, 'chargeability_transition_no_delete');
END;

CREATE TRIGGER chargeability_transition_no_update
BEFORE UPDATE ON chargeability_transition
BEGIN
    SELECT RAISE(ABORT, 'chargeability_transition_no_update');
END;

CREATE TRIGGER match_attempt_no_delete
BEFORE DELETE ON match_attempt
BEGIN
    SELECT RAISE(ABORT, 'match_attempt_no_delete');
END;

CREATE TRIGGER match_attempt_no_update
BEFORE UPDATE ON match_attempt
BEGIN
    SELECT RAISE(ABORT, 'match_attempt_no_update');
END;

CREATE TRIGGER measurement_no_delete
BEFORE DELETE ON measurement
BEGIN
    SELECT RAISE(ABORT, 'measurement_no_delete');
END;

CREATE TRIGGER measurement_no_update
BEFORE UPDATE ON measurement
BEGIN
    SELECT RAISE(ABORT, 'measurement_no_update');
END;

CREATE TRIGGER multiplier_observation_counts_nonnegative_insert
BEFORE INSERT ON multiplier_observation
WHEN NEW.window_minutes<0 OR NEW.input_tokens<0 OR NEW.cache_read_tokens<0 OR NEW.cache_write_tokens<0
 OR NEW.output_tokens<0 OR NEW.reasoning_tokens<0 OR NEW.tokens_outside_quota_coverage<0
 OR NEW.base_rate_tier_untested_tokens<0 OR NEW.accounts_included_count<0
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_counts_nonnegative'); END;

CREATE TRIGGER multiplier_observation_counts_nonnegative_update
BEFORE UPDATE ON multiplier_observation
WHEN NEW.window_minutes<0 OR NEW.input_tokens<0 OR NEW.cache_read_tokens<0 OR NEW.cache_write_tokens<0
 OR NEW.output_tokens<0 OR NEW.reasoning_tokens<0 OR NEW.tokens_outside_quota_coverage<0
 OR NEW.base_rate_tier_untested_tokens<0 OR NEW.accounts_included_count<0
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_counts_nonnegative'); END;

CREATE TRIGGER multiplier_observation_day_matches_end_insert
BEFORE INSERT ON multiplier_observation
WHEN NEW.observation_day IS NOT strftime('%Y-%m-%d', NEW.interval_end_ms / 1000.0, 'unixepoch')
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_day_matches_end'); END;

CREATE TRIGGER multiplier_observation_day_matches_end_update
BEFORE UPDATE ON multiplier_observation
WHEN NEW.observation_day IS NOT strftime('%Y-%m-%d', NEW.interval_end_ms / 1000.0, 'unixepoch')
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_day_matches_end'); END;

CREATE TRIGGER multiplier_observation_interval_ordered_insert
BEFORE INSERT ON multiplier_observation
WHEN NEW.interval_start_ms>=NEW.interval_end_ms
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_interval_ordered'); END;

CREATE TRIGGER multiplier_observation_interval_ordered_update
BEFORE UPDATE ON multiplier_observation
WHEN NEW.interval_start_ms>=NEW.interval_end_ms
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_interval_ordered'); END;

CREATE TRIGGER multiplier_observation_no_delete
BEFORE DELETE ON multiplier_observation
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_no_delete');
END;

CREATE TRIGGER multiplier_observation_no_update
BEFORE UPDATE ON multiplier_observation
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_no_update');
END;

CREATE TRIGGER priced_charge_matches_event_generation_and_basis
BEFORE INSERT ON priced_charge
WHEN NOT EXISTS (
    SELECT 1 FROM usage_event event
    WHERE event.event_key = NEW.event_key
      AND event.pricing_generation = NEW.pricing_generation
      AND event.charge_basis = NEW.charge_basis
)
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_matches_event_generation_and_basis');
END;

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

CREATE TRIGGER priced_charge_no_update
BEFORE UPDATE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_update');
END;

CREATE TRIGGER priced_charge_nonnegative_insert
BEFORE INSERT ON priced_charge
WHEN NEW.charge_nanos<0 OR NEW.rate_units<0 OR NEW.floor_units<0
BEGIN SELECT RAISE(ABORT, 'priced_charge_nonnegative'); END;

CREATE TRIGGER priced_charge_nonnegative_update
BEFORE UPDATE ON priced_charge
WHEN NEW.charge_nanos<0 OR NEW.rate_units<0 OR NEW.floor_units<0
BEGIN SELECT RAISE(ABORT, 'priced_charge_nonnegative'); END;

CREATE TRIGGER statement_import_no_delete
BEFORE DELETE ON statement_import
BEGIN
    SELECT RAISE(ABORT, 'statement_import_no_delete');
END;

CREATE TRIGGER statement_import_no_update
BEFORE UPDATE ON statement_import
BEGIN
    SELECT RAISE(ABORT, 'statement_import_no_update');
END;

CREATE TRIGGER statement_line_matches_statement
BEFORE INSERT ON statement_line
WHEN NOT EXISTS (
    SELECT 1 FROM statement
    WHERE statement.statement_id = NEW.statement_id
      AND NEW.occurred_at_ms >= statement.period_start_ms
      AND NEW.occurred_at_ms < statement.period_end_ms
      AND NEW.currency = statement.currency
)
BEGIN
    SELECT RAISE(ABORT, 'statement_line_matches_statement');
END;

CREATE TRIGGER statement_line_no_delete
BEFORE DELETE ON statement_line
BEGIN
    SELECT RAISE(ABORT, 'statement_line_no_delete');
END;

CREATE TRIGGER statement_line_update_guard
BEFORE UPDATE ON statement_line
WHEN NOT (
    NEW.statement_line_id IS OLD.statement_line_id
    AND NEW.statement_id IS OLD.statement_id
    AND NEW.line_id IS OLD.line_id
    AND NEW.occurred_at_ms IS OLD.occurred_at_ms
    AND NEW.description IS OLD.description
    AND NEW.quantity IS OLD.quantity
    AND NEW.unit IS OLD.unit
    AND NEW.amount_minor IS OLD.amount_minor
    AND NEW.currency IS OLD.currency
    AND NEW.correlation_ref IS OLD.correlation_ref
)
BEGIN
    SELECT RAISE(ABORT, 'statement_line_update_guard');
END;

CREATE TRIGGER statement_no_delete
BEFORE DELETE ON statement
BEGIN
    SELECT RAISE(ABORT, 'statement_no_delete');
END;

CREATE TRIGGER statement_no_update
BEFORE UPDATE ON statement
BEGIN
    SELECT RAISE(ABORT, 'statement_no_update');
END;

CREATE TRIGGER usage_event_currency_starts_null
BEFORE INSERT ON usage_event
WHEN NEW.currency IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'usage_event_currency_starts_null');
END;

CREATE TRIGGER usage_event_no_delete
BEFORE DELETE ON usage_event
BEGIN
    SELECT RAISE(ABORT, 'usage_event_no_delete');
END;

CREATE TRIGGER usage_event_pricing_generation_starts_zero
BEFORE INSERT ON usage_event
WHEN NEW.pricing_generation <> 0
BEGIN
    SELECT RAISE(ABORT, 'usage_event_pricing_generation_starts_zero');
END;

CREATE TRIGGER usage_event_update_guard
BEFORE UPDATE ON usage_event
WHEN NOT (
    NEW.event_key IS OLD.event_key
    AND NEW.event_id IS OLD.event_id
    AND NEW.producer IS OLD.producer
    AND NEW.producer_lineage IS OLD.producer_lineage
    AND NEW.producer_ref IS OLD.producer_ref
    AND NEW.prior_event_ref IS OLD.prior_event_ref
    AND NEW.harness IS OLD.harness
    AND NEW.project_root IS OLD.project_root
    AND NEW.session IS OLD.session
    AND NEW.run_ref IS OLD.run_ref
    AND NEW.work_class IS OLD.work_class
    AND NEW.occurred_at_ms IS OLD.occurred_at_ms
    AND NEW.emitted_at_ms IS OLD.emitted_at_ms
    AND NEW.recorded_at_ms IS OLD.recorded_at_ms
    AND NEW.terminal_status IS OLD.terminal_status
    AND NEW.attempt_ordinal IS OLD.attempt_ordinal
    AND NEW.vendor_request_id IS OLD.vendor_request_id
    AND NEW.provider IS OLD.provider
    AND NEW.model IS OLD.model
    AND NEW.account_ref IS OLD.account_ref
    AND NEW.auth_selection IS OLD.auth_selection
    AND NEW.charge_basis_mapping_version IS OLD.charge_basis_mapping_version
    AND NEW.charge_basis_overrides IS OLD.charge_basis_overrides
    AND (
        NEW.charge_basis IS OLD.charge_basis
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.pricing_generation = OLD.pricing_generation
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.currency IS OLD.currency
        OR (OLD.currency IS NULL AND NEW.currency IS NOT NULL)
    )
)
BEGIN
    SELECT RAISE(ABORT, 'usage_event_update_guard');
END;

CREATE TRIGGER priced_charge_supersedes_same_measurement
BEFORE INSERT ON priced_charge
WHEN NEW.supersedes IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM priced_charge p WHERE p.charge_key=NEW.supersedes
      AND p.event_key IS NEW.event_key AND p.role IS NEW.role AND p.unit IS NEW.unit
)
BEGIN SELECT RAISE(ABORT, 'priced_charge_supersedes_same_measurement'); END;

CREATE TRIGGER adjustment_no_update
BEFORE UPDATE ON adjustment
BEGIN SELECT RAISE(ABORT, 'adjustment_no_update'); END;
CREATE TRIGGER adjustment_no_delete
BEFORE DELETE ON adjustment
BEGIN SELECT RAISE(ABORT, 'adjustment_no_delete'); END;
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_usage_event', count(*) FROM pragma_foreign_key_check('usage_event');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_measurement', count(*) FROM pragma_foreign_key_check('measurement');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_priced_charge', count(*) FROM pragma_foreign_key_check('priced_charge');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_pricing_pending', count(*) FROM pragma_foreign_key_check('pricing_pending');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_chargeability_transition', count(*) FROM pragma_foreign_key_check('chargeability_transition');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_adjustment', count(*) FROM pragma_foreign_key_check('adjustment');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_quarantine_evidence', count(*) FROM pragma_foreign_key_check('quarantine_evidence');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_rejected_delivery', count(*) FROM pragma_foreign_key_check('rejected_delivery');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_redelivery_log', count(*) FROM pragma_foreign_key_check('redelivery_log');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_statement_line', count(*) FROM pragma_foreign_key_check('statement_line');
INSERT INTO astrocyte_v23_check SELECT 'post_foreign_key_check_match_attempt', count(*) FROM pragma_foreign_key_check('match_attempt');
INSERT INTO astrocyte_v23_check
SELECT 'post_same_measurement_predecessor', count(*) FROM priced_charge c
WHERE c.supersedes IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM priced_charge p WHERE p.charge_key=c.supersedes
    AND p.event_key=c.event_key AND p.role=c.role AND p.unit=c.unit
);
INSERT INTO astrocyte_v23_check SELECT 'post_count_usage_event', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='usage_event')-(SELECT count(*) FROM usage_event));
INSERT INTO astrocyte_v23_check SELECT 'post_count_measurement', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='measurement')-(SELECT count(*) FROM measurement));
INSERT INTO astrocyte_v23_check SELECT 'post_count_priced_charge', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='priced_charge')-(SELECT count(*) FROM priced_charge));
INSERT INTO astrocyte_v23_check SELECT 'post_count_pricing_pending', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='pricing_pending')-(SELECT count(*) FROM pricing_pending));
INSERT INTO astrocyte_v23_check SELECT 'post_count_chargeability_transition', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='chargeability_transition')-(SELECT count(*) FROM chargeability_transition));
INSERT INTO astrocyte_v23_check SELECT 'post_count_adjustment', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='adjustment')-(SELECT count(*) FROM adjustment));
INSERT INTO astrocyte_v23_check SELECT 'post_count_quarantine_evidence', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='quarantine_evidence')-(SELECT count(*) FROM quarantine_evidence));
INSERT INTO astrocyte_v23_check SELECT 'post_count_rejected_delivery', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='rejected_delivery')-(SELECT count(*) FROM rejected_delivery));
INSERT INTO astrocyte_v23_check SELECT 'post_count_redelivery_log', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='redelivery_log')-(SELECT count(*) FROM redelivery_log));
INSERT INTO astrocyte_v23_check SELECT 'post_count_statement_line', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='statement_line')-(SELECT count(*) FROM statement_line));
INSERT INTO astrocyte_v23_check SELECT 'post_count_match_attempt', abs((SELECT rows FROM astrocyte_v23_counts WHERE name='match_attempt')-(SELECT count(*) FROM match_attempt));
DROP TABLE astrocyte_v23_counts;
DROP TRIGGER astrocyte_v23_refuse;
DROP TABLE astrocyte_v23_check;
PRAGMA cache_size=-2000;
"#,
    },
    Migration {
        version: 24,
        statements: r#"-- Disposable derived data. Per-call facts and immutable charge history remain authoritative.
CREATE TABLE summary_value (
    value_id INTEGER PRIMARY KEY CHECK (value_id >= 0),
    value TEXT UNIQUE,
    CHECK ((value_id = 0) = (value IS NULL))
);
INSERT INTO summary_value VALUES (0,NULL);

CREATE TABLE summary_hour (
    kind INTEGER NOT NULL CHECK (kind IN (0,1)),
    hour_number INTEGER NOT NULL,
    revision INTEGER NOT NULL CHECK (typeof(revision)='integer' AND revision>0),
    built_revision INTEGER,
    format_version INTEGER,
    events_total INTEGER NOT NULL DEFAULT 0 CHECK (events_total>=0),
    first_ms INTEGER,
    last_ms INTEGER,
    PRIMARY KEY(kind,hour_number),
    CHECK (kind=0 OR hour_number=0)
) WITHOUT ROWID;
CREATE TABLE summary_dirty_hour (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    PRIMARY KEY(kind,hour_number),
    FOREIGN KEY(kind,hour_number) REFERENCES summary_hour(kind,hour_number)
) WITHOUT ROWID;
CREATE TABLE summary_cell (
    cell_id INTEGER PRIMARY KEY,
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    producer INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(producer>0),
    provider INTEGER NOT NULL REFERENCES summary_value(value_id),
    model INTEGER NOT NULL REFERENCES summary_value(value_id),
    account INTEGER NOT NULL REFERENCES summary_value(value_id),
    auth_selection INTEGER NOT NULL REFERENCES summary_value(value_id),
    charge_basis INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(charge_basis>0),
    work_class INTEGER NOT NULL REFERENCES summary_value(value_id),
    project INTEGER NOT NULL REFERENCES summary_value(value_id),
    currency INTEGER NOT NULL REFERENCES summary_value(value_id),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    UNIQUE(kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency),
    FOREIGN KEY(kind,hour_number) REFERENCES summary_hour(kind,hour_number)
);
CREATE TABLE summary_quantity (
    cell_id INTEGER NOT NULL REFERENCES summary_cell(cell_id) ON DELETE CASCADE,
    unit INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(unit>0),
    role INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(role>0),
    exponent INTEGER NOT NULL CHECK(exponent BETWEEN -9 AND 1),
    units TEXT NOT NULL,
    observations INTEGER NOT NULL CHECK(observations>=0),
    covered_units TEXT NOT NULL,
    covered_observations INTEGER NOT NULL CHECK(covered_observations>=0),
    PRIMARY KEY(cell_id,unit,role,exponent)
) WITHOUT ROWID;
CREATE TABLE summary_money (
    cell_id INTEGER NOT NULL REFERENCES summary_cell(cell_id) ON DELETE CASCADE,
    kind INTEGER NOT NULL CHECK(kind IN (0,1)),
    currency INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(currency>0),
    nanos TEXT NOT NULL,
    contributions INTEGER NOT NULL CHECK(contributions>0),
    PRIMARY KEY(cell_id,kind,currency)
) WITHOUT ROWID;
CREATE TABLE summary_cost_frequency (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    producer INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(producer>0),
    work_class INTEGER NOT NULL REFERENCES summary_value(value_id),
    currency INTEGER NOT NULL REFERENCES summary_value(value_id) CHECK(currency>0),
    nanos INTEGER NOT NULL,
    samples INTEGER NOT NULL CHECK(samples>0),
    PRIMARY KEY(kind,hour_number,producer,work_class,currency,nanos),
    FOREIGN KEY(kind,hour_number) REFERENCES summary_hour(kind,hour_number)
) WITHOUT ROWID;
CREATE TABLE summary_audit_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    after_kind INTEGER,
    after_hour INTEGER,
    next_due_ms INTEGER NOT NULL DEFAULT 0,
    examined INTEGER NOT NULL DEFAULT 0,
    mismatches INTEGER NOT NULL DEFAULT 0,
    hours INTEGER NOT NULL DEFAULT 0,
    chunks INTEGER NOT NULL DEFAULT 0,
    spent_ms INTEGER NOT NULL DEFAULT 0,
    max_chunk_ms INTEGER NOT NULL DEFAULT 0,
    started_at_ms INTEGER,
    CHECK((after_kind IS NULL)=(after_hour IS NULL)),
    CHECK(after_kind IS NULL OR after_kind IN (0,1)),
    CHECK(after_kind IS NULL OR after_kind=0 OR after_hour=0)
);
INSERT INTO summary_audit_state(id) VALUES(1);
CREATE INDEX usage_event_summary_time ON usage_event(occurred_at_ms,event_key);

-- SQLite division truncates; subtracting for a negative remainder gives UTC floor hours.
INSERT INTO summary_hour(kind,hour_number,revision,events_total,first_ms,last_ms)
SELECT occurred_at_ms IS NULL,
       COALESCE(occurred_at_ms/3600000-(occurred_at_ms%3600000<0),0),1,
       COUNT(*),MIN(occurred_at_ms),MAX(occurred_at_ms)
FROM usage_event GROUP BY 1,2;
INSERT INTO summary_dirty_hour SELECT kind,hour_number FROM summary_hour;

-- Measurements are inserted with their event. Charge writers update the header first
-- in the same transaction; priced_charge_matches_event_generation_and_basis enforces
-- header/charge agreement, not transaction identity. Child inserts need no dirty write.
CREATE TRIGGER summary_event_insert AFTER INSERT ON usage_event BEGIN
    INSERT INTO summary_hour(kind,hour_number,revision)
    VALUES(NEW.occurred_at_ms IS NULL,COALESCE(NEW.occurred_at_ms/3600000-(NEW.occurred_at_ms%3600000<0),0),1)
    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1;
    INSERT OR IGNORE INTO summary_dirty_hour
    VALUES(NEW.occurred_at_ms IS NULL,COALESCE(NEW.occurred_at_ms/3600000-(NEW.occurred_at_ms%3600000<0),0));
END;
CREATE TRIGGER summary_event_update AFTER UPDATE OF pricing_generation,currency,charge_basis ON usage_event BEGIN
    INSERT INTO summary_hour(kind,hour_number,revision)
    VALUES(NEW.occurred_at_ms IS NULL,COALESCE(NEW.occurred_at_ms/3600000-(NEW.occurred_at_ms%3600000<0),0),1)
    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1;
    INSERT OR IGNORE INTO summary_dirty_hour
    VALUES(NEW.occurred_at_ms IS NULL,COALESCE(NEW.occurred_at_ms/3600000-(NEW.occurred_at_ms%3600000<0),0));
END;
CREATE TRIGGER summary_adjustment_insert AFTER INSERT ON adjustment BEGIN
    INSERT INTO summary_hour(kind,hour_number,revision)
    SELECT occurred_at_ms IS NULL,COALESCE(occurred_at_ms/3600000-(occurred_at_ms%3600000<0),0),1
    FROM usage_event WHERE event_key=NEW.event_key
    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1;
    INSERT OR IGNORE INTO summary_dirty_hour
    SELECT occurred_at_ms IS NULL,COALESCE(occurred_at_ms/3600000-(occurred_at_ms%3600000<0),0)
    FROM usage_event WHERE event_key=NEW.event_key;
END;
CREATE TRIGGER summary_transition_insert AFTER INSERT ON chargeability_transition BEGIN
    INSERT INTO summary_hour(kind,hour_number,revision)
    SELECT occurred_at_ms IS NULL,COALESCE(occurred_at_ms/3600000-(occurred_at_ms%3600000<0),0),1
    FROM usage_event WHERE event_key=NEW.event_key
    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1;
    INSERT OR IGNORE INTO summary_dirty_hour
    SELECT occurred_at_ms IS NULL,COALESCE(occurred_at_ms/3600000-(occurred_at_ms%3600000<0),0)
    FROM usage_event WHERE event_key=NEW.event_key;
END;
"#,
    },
    Migration {
        version: 25,
        statements: r#"-- UNIQUE(event_id) already enforces natural-key uniqueness for lineage-absent
-- events: ingest's only writer stores the hash of (producer, lineage, ref) and
-- never an id from its caller. This index duplicated that check and was its
-- largest avoidable cost, a random-key insert on every recorded event.
DROP INDEX usage_event_natural_key_lineage_absent;
"#,
    },
    Migration {
        version: 26,
        statements: r#"-- BROCA's batch decision and extraction models answer batches that belong to
-- no session. A call made inside a run names the run (run_ref); a call made
-- inside a flow names the flow instead, stored here. Nullable with no default:
-- every event recorded before this column existed, and every event from any
-- other producer, has no flow, and ALTER TABLE ADD COLUMN rewrites no row.
ALTER TABLE usage_event ADD COLUMN flow_ref TEXT;

-- The update guard lists every column that must stay unchanged, so a new
-- column is mutable until it is listed. The guard is recreated from the
-- version 23 definition with flow_ref added beside run_ref; every other
-- condition is carried unchanged.
DROP TRIGGER usage_event_update_guard;
CREATE TRIGGER usage_event_update_guard
BEFORE UPDATE ON usage_event
WHEN NOT (
    NEW.event_key IS OLD.event_key
    AND NEW.event_id IS OLD.event_id
    AND NEW.producer IS OLD.producer
    AND NEW.producer_lineage IS OLD.producer_lineage
    AND NEW.producer_ref IS OLD.producer_ref
    AND NEW.prior_event_ref IS OLD.prior_event_ref
    AND NEW.harness IS OLD.harness
    AND NEW.project_root IS OLD.project_root
    AND NEW.session IS OLD.session
    AND NEW.run_ref IS OLD.run_ref
    AND NEW.flow_ref IS OLD.flow_ref
    AND NEW.work_class IS OLD.work_class
    AND NEW.occurred_at_ms IS OLD.occurred_at_ms
    AND NEW.emitted_at_ms IS OLD.emitted_at_ms
    AND NEW.recorded_at_ms IS OLD.recorded_at_ms
    AND NEW.terminal_status IS OLD.terminal_status
    AND NEW.attempt_ordinal IS OLD.attempt_ordinal
    AND NEW.vendor_request_id IS OLD.vendor_request_id
    AND NEW.provider IS OLD.provider
    AND NEW.model IS OLD.model
    AND NEW.account_ref IS OLD.account_ref
    AND NEW.auth_selection IS OLD.auth_selection
    AND NEW.charge_basis_mapping_version IS OLD.charge_basis_mapping_version
    AND NEW.charge_basis_overrides IS OLD.charge_basis_overrides
    AND (
        NEW.charge_basis IS OLD.charge_basis
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.pricing_generation = OLD.pricing_generation
        OR NEW.pricing_generation = OLD.pricing_generation + 1
    )
    AND (
        NEW.currency IS OLD.currency
        OR (OLD.currency IS NULL AND NEW.currency IS NOT NULL)
    )
)
BEGIN
    SELECT RAISE(ABORT, 'usage_event_update_guard');
END;
"#,
    },
    Migration {
        version: 27,
        statements: r#"-- Per-step detail a harness reader keeps beside an event for cache
-- diagnostics: the finish reason OpenCode and Pi report, and the context window
-- Codex reports. It lives outside usage_event on purpose. The ledger compares
-- every re-read against the stored event and quarantines a difference, so a
-- reader that started storing a new event field would make every event it
-- recorded earlier look contradicted. A row here is written with its event
-- (or, for an older event, when a reader re-reads it exactly), only when a
-- field is present, and is never compared with anything.
CREATE TABLE step_detail (
    event_key     INTEGER PRIMARY KEY REFERENCES usage_event(event_key),
    finish        TEXT,
    context_limit INTEGER,
    CHECK (finish IS NOT NULL OR context_limit IS NOT NULL)
);
CREATE TRIGGER step_detail_no_update BEFORE UPDATE ON step_detail
BEGIN
    SELECT RAISE(ABORT, 'step_detail_no_update');
END;
CREATE TRIGGER step_detail_no_delete BEFORE DELETE ON step_detail
BEGIN
    SELECT RAISE(ABORT, 'step_detail_no_delete');
END;
-- INSERT OR REPLACE deletes the row it replaces without firing delete
-- triggers (recursive triggers are off), so a second row for an event is
-- refused before it can replace the first.
CREATE TRIGGER step_detail_insert_once BEFORE INSERT ON step_detail
WHEN EXISTS (SELECT 1 FROM step_detail WHERE event_key=NEW.event_key)
BEGIN
    SELECT RAISE(ABORT, 'step_detail_insert_once');
END;

-- Cache diagnostics read one session's steps newest first. The rowid that
-- ends every index entry is the event_key tie-break, so this index serves
-- ORDER BY occurred_at_ms, event_key within a session without a sort.
CREATE INDEX usage_event_session_time ON usage_event(session, occurred_at_ms);
"#,
    },
    Migration {
        version: 28,
        statements: r#"-- BROCA model steps are diagnostics, never additional spend.
CREATE TABLE broca_step (
    step_key INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_id INTEGER NOT NULL,
    fact_id TEXT NOT NULL UNIQUE,
    lineage_id TEXT,
    session_ref TEXT,
    project_root TEXT NOT NULL,
    harness TEXT NOT NULL,
    session TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    variant TEXT,
    wire_family TEXT,
    input_tokens INTEGER,
    cached_input_tokens INTEGER,
    cache_write_tokens INTEGER,
    output_tokens INTEGER,
    reasoning_tokens INTEGER,
    finish_reason TEXT,
    attempts INTEGER,
    retried INTEGER,
    request_body_len INTEGER,
    request_body_sha256 TEXT,
    started_at_ms INTEGER,
    first_content_at_ms INTEGER,
    finished_at_ms INTEGER NOT NULL,
    context_limit INTEGER,
    cache_policy TEXT,
    agent_id TEXT,
    agent_run_id TEXT,
    recorded_at_ms INTEGER NOT NULL,
    UNIQUE(run_id, step_id)
);
CREATE INDEX broca_step_session_time ON broca_step(session, finished_at_ms);
CREATE INDEX broca_step_expiry ON broca_step(finished_at_ms);
CREATE TRIGGER broca_step_no_update BEFORE UPDATE ON broca_step
BEGIN
    SELECT RAISE(ABORT, 'broca_step_no_update');
END;
-- DELETE is allowed for bounded retention; these diagnostics expire after 30 days.
-- Refuse replacement by either identity, even with recursive triggers disabled.
CREATE TRIGGER broca_step_insert_once BEFORE INSERT ON broca_step
WHEN EXISTS (SELECT 1 FROM broca_step WHERE step_key=NEW.step_key
    OR fact_id=NEW.fact_id OR (run_id=NEW.run_id AND step_id=NEW.step_id))
BEGIN
    SELECT RAISE(ABORT, 'broca_step_insert_once');
END;

-- One durable commit-order cursor covers ledger events and BROCA diagnostics.
-- Preserve existing event cursors at upgrade. AUTOINCREMENT prevents reuse after
-- retention deletes the most recently allocated BROCA cursor.
CREATE TABLE cache_step_sequence (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key INTEGER UNIQUE REFERENCES usage_event(event_key),
    broca_step_key INTEGER UNIQUE REFERENCES broca_step(step_key) ON DELETE CASCADE,
    CHECK ((event_key IS NULL) != (broca_step_key IS NULL))
);
INSERT INTO cache_step_sequence(cursor,event_key)
    SELECT event_key,event_key FROM usage_event ORDER BY event_key;
CREATE TRIGGER cache_event_cursor AFTER INSERT ON usage_event
BEGIN
    INSERT INTO cache_step_sequence(event_key) VALUES (NEW.event_key);
END;
CREATE TRIGGER cache_broca_cursor AFTER INSERT ON broca_step
BEGIN
    INSERT INTO cache_step_sequence(broca_step_key) VALUES (NEW.step_key);
END;
"#,
    },
    Migration {
        version: 29,
        statements: r#"-- The live step feed behind usage.steps.read. Each row is one emission of a
-- served cache step: a step's row is appended again, at a new feed_cursor,
-- whenever any served field of it changes, and the newest row for a
-- (harness, step_ref) wins. AUTOINCREMENT keeps cursors from being reused
-- after the 24-hour retention deletes the newest rows. row_json is the served
-- row without its cursor.
CREATE TABLE step_feed (
    feed_cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    harness TEXT NOT NULL,
    step_ref TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('file', 'runner')),
    session TEXT NOT NULL,
    committed_at_ms INTEGER NOT NULL,
    row_json TEXT NOT NULL
);
CREATE INDEX step_feed_identity ON step_feed(harness, step_ref, feed_cursor);
CREATE INDEX step_feed_committed ON step_feed(committed_at_ms);
-- Rows are never edited. DELETE is allowed for the 24-hour retention.
CREATE TRIGGER step_feed_no_update BEFORE UPDATE ON step_feed
BEGIN
    SELECT RAISE(ABORT, 'step_feed_no_update');
END;
-- The cache cursor the feed has caught up to, and the newest feed cursor that
-- retention has deleted. Starting at the current cache cursor means an
-- upgraded store does not replay its history into the feed.
CREATE TABLE step_feed_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cache_high_water INTEGER NOT NULL,
    pruned_through INTEGER NOT NULL
);
INSERT INTO step_feed_state(id, cache_high_water, pruned_through) VALUES (
    1,
    COALESCE((SELECT seq FROM sqlite_sequence WHERE name='cache_step_sequence'), 0),
    0
);
"#,
    },
    Migration {
        version: 30,
        statements: r#"-- Another module can declare harness stores for astrocyte to read, such as
-- the private OpenCode data folder of each session thalamus-harness runs. One
-- source_runtime row covers all of a source's stores, so each store keeps its
-- own durable read position here, one row per store and on-disk layout. The
-- columns mean what the same columns of source_runtime mean. A row is removed
-- once its store is no longer declared.
CREATE TABLE declared_store_cursor (
    source_key         TEXT NOT NULL,
    store_path         TEXT NOT NULL,
    layout             TEXT NOT NULL,
    lineage            TEXT NOT NULL,
    consumed_ordinal   INTEGER NOT NULL,
    high_water_ordinal INTEGER NOT NULL,
    PRIMARY KEY (source_key, store_path, layout)
) WITHOUT ROWID;

-- A read watermark per reader source: the time before which everything the
-- source's declared stores held has been recorded, so the declaring module may
-- erase a store it has finished with. The value only moves forward; no row
-- means no complete read yet.
CREATE TABLE read_watermark (
    source_key    TEXT PRIMARY KEY,
    watermark_ms  INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) WITHOUT ROWID;
"#,
    },
    Migration {
        version: 31,
        statements: r#"-- REPLACE deletes conflicting rows without firing DELETE triggers when
-- recursive_triggers is off. Check every unique identity before insertion so
-- immutable content cannot be replaced, regardless of the writer's pragmas.
-- Separate EXISTS probes let each identity use its own unique index.
-- Rowid tables with non-integer primary keys also have a hidden unique rowid.
CREATE TRIGGER usage_event_insert_once BEFORE INSERT ON usage_event
WHEN EXISTS (SELECT 1 FROM usage_event WHERE event_key=NEW.event_key)
  OR EXISTS (SELECT 1 FROM usage_event WHERE event_id=NEW.event_id)
  OR EXISTS (SELECT 1 FROM usage_event WHERE producer=NEW.producer
      AND producer_lineage=NEW.producer_lineage AND producer_ref=NEW.producer_ref)
BEGIN
    SELECT RAISE(ABORT, 'usage_event_insert_once');
END;
CREATE TRIGGER measurement_insert_once BEFORE INSERT ON measurement
WHEN EXISTS (SELECT 1 FROM measurement WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM measurement WHERE event_key=NEW.event_key
    AND role=NEW.role AND unit=NEW.unit)
BEGIN
    SELECT RAISE(ABORT, 'measurement_insert_once');
END;
CREATE TRIGGER chargeability_transition_insert_once BEFORE INSERT ON chargeability_transition
WHEN EXISTS (SELECT 1 FROM chargeability_transition WHERE event_key=NEW.event_key)
BEGIN
    SELECT RAISE(ABORT, 'chargeability_transition_insert_once');
END;
CREATE TRIGGER priced_charge_insert_once BEFORE INSERT ON priced_charge
WHEN EXISTS (SELECT 1 FROM priced_charge WHERE charge_key=NEW.charge_key)
  OR EXISTS (SELECT 1 FROM priced_charge WHERE supersedes=NEW.supersedes)
  OR EXISTS (SELECT 1 FROM priced_charge WHERE event_key=NEW.event_key
      AND role=NEW.role AND unit=NEW.unit AND pricing_generation=NEW.pricing_generation)
  OR (NEW.supersedes IS NULL AND EXISTS (SELECT 1 FROM priced_charge
      WHERE event_key=NEW.event_key AND role=NEW.role AND unit=NEW.unit AND supersedes IS NULL))
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_insert_once');
END;
CREATE TRIGGER adjustment_insert_once BEFORE INSERT ON adjustment
WHEN EXISTS (SELECT 1 FROM adjustment WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM adjustment WHERE adjustment_id=NEW.adjustment_id)
  OR EXISTS (SELECT 1 FROM adjustment WHERE correction_id=NEW.correction_id)
BEGIN
    SELECT RAISE(ABORT, 'adjustment_insert_once');
END;
CREATE TRIGGER statement_insert_once BEFORE INSERT ON statement
WHEN EXISTS (SELECT 1 FROM statement WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM statement WHERE statement_id=NEW.statement_id)
  OR EXISTS (SELECT 1 FROM statement WHERE provider=NEW.provider AND account_ref=NEW.account_ref
      AND period_start_ms=NEW.period_start_ms AND period_end_ms=NEW.period_end_ms)
BEGIN
    SELECT RAISE(ABORT, 'statement_insert_once');
END;
CREATE TRIGGER statement_line_insert_once BEFORE INSERT ON statement_line
WHEN EXISTS (SELECT 1 FROM statement_line WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM statement_line WHERE statement_line_id=NEW.statement_line_id)
  OR EXISTS (SELECT 1 FROM statement_line WHERE statement_id=NEW.statement_id AND line_id=NEW.line_id)
BEGIN
    SELECT RAISE(ABORT, 'statement_line_insert_once');
END;
CREATE TRIGGER match_attempt_insert_once BEFORE INSERT ON match_attempt
WHEN EXISTS (SELECT 1 FROM match_attempt WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM match_attempt WHERE attempt_id=NEW.attempt_id)
BEGIN
    SELECT RAISE(ABORT, 'match_attempt_insert_once');
END;
CREATE TRIGGER statement_import_insert_once BEFORE INSERT ON statement_import
WHEN EXISTS (SELECT 1 FROM statement_import WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM statement_import WHERE import_id=NEW.import_id)
BEGIN
    SELECT RAISE(ABORT, 'statement_import_insert_once');
END;
CREATE TRIGGER account_subscription_insert_once BEFORE INSERT ON account_subscription
WHEN EXISTS (SELECT 1 FROM account_subscription WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM account_subscription WHERE provider=NEW.provider
    AND account_ref=NEW.account_ref AND effective_from_ms=NEW.effective_from_ms)
  OR (NEW.effective_to_ms IS NULL AND EXISTS (SELECT 1 FROM account_subscription
      WHERE provider=NEW.provider AND account_ref=NEW.account_ref AND effective_to_ms IS NULL))
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_insert_once');
END;
CREATE TRIGGER multiplier_observation_insert_once BEFORE INSERT ON multiplier_observation
WHEN EXISTS (SELECT 1 FROM multiplier_observation WHERE observation_id=NEW.observation_id)
  OR EXISTS (SELECT 1 FROM multiplier_observation WHERE supersedes=NEW.supersedes)
  OR (NEW.supersedes IS NULL AND EXISTS (SELECT 1 FROM multiplier_observation
      WHERE quota_provider=NEW.quota_provider AND observation_day=NEW.observation_day AND supersedes IS NULL))
BEGIN
    SELECT RAISE(ABORT, 'multiplier_observation_insert_once');
END;
CREATE TRIGGER step_feed_insert_once BEFORE INSERT ON step_feed
WHEN EXISTS (SELECT 1 FROM step_feed WHERE feed_cursor=NEW.feed_cursor)
BEGIN
    SELECT RAISE(ABORT, 'step_feed_insert_once');
END;
"#,
    },
    Migration {
        version: 32,
        statements: r#"
CREATE INDEX step_feed_session_cursor ON step_feed(harness,session,feed_cursor);
"#,
    },
    Migration {
        version: 33,
        statements: r#"-- BROCA steps come from its step log, not export_facts. Refuse to
-- discard any diagnostic cursor that an older reader may have recorded.
CREATE TEMP TABLE astrocyte_v33_guard (invalid INTEGER);
CREATE TEMP TRIGGER astrocyte_v33_no_broca_cursors BEFORE INSERT ON astrocyte_v33_guard
WHEN NEW.invalid != 0
BEGIN
    SELECT RAISE(ABORT, 'cache_step_sequence_has_broca_steps');
END;
INSERT INTO astrocyte_v33_guard SELECT EXISTS (
    SELECT 1 FROM cache_step_sequence WHERE broca_step_key IS NOT NULL);

-- AUTOINCREMENT's high water may exceed every surviving cursor. Preserve it
-- separately so rebuilding the table cannot reuse a cursor already served.
CREATE TEMP TABLE astrocyte_v33_sequence AS
    SELECT seq FROM sqlite_sequence WHERE name='cache_step_sequence';
DROP TRIGGER cache_event_cursor;
DROP TRIGGER cache_broca_cursor;
CREATE TABLE cache_step_sequence_new (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key INTEGER NOT NULL UNIQUE REFERENCES usage_event(event_key)
);
INSERT INTO cache_step_sequence_new(cursor,event_key)
    SELECT cursor,event_key FROM cache_step_sequence;
DROP TABLE cache_step_sequence;
ALTER TABLE cache_step_sequence_new RENAME TO cache_step_sequence;
DELETE FROM sqlite_sequence WHERE name='cache_step_sequence';
INSERT INTO sqlite_sequence(name,seq)
    SELECT 'cache_step_sequence',seq FROM astrocyte_v33_sequence;
CREATE TRIGGER cache_event_cursor AFTER INSERT ON usage_event
BEGIN
    INSERT INTO cache_step_sequence(event_key) VALUES (NEW.event_key);
END;
DROP TABLE broca_step;

-- Check all foreign keys before the migration transaction can commit.
CREATE TEMP TRIGGER astrocyte_v33_foreign_keys BEFORE INSERT ON astrocyte_v33_guard
WHEN NEW.invalid != 0
BEGIN
    SELECT RAISE(ABORT, 'cache_step_sequence_foreign_key_check');
END;
DROP TRIGGER astrocyte_v33_no_broca_cursors;
INSERT INTO astrocyte_v33_guard SELECT count(*) FROM pragma_foreign_key_check;
DROP TABLE astrocyte_v33_guard;
DROP TABLE astrocyte_v33_sequence;
"#,
    },
    Migration {
        version: 34,
        statements: r#"
-- Runs share the hourly summary revision, dirty queue, and audit. A currency
-- map in each model cell avoids duplicating step and token counts by currency.
CREATE TABLE summary_run_cell (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    run_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    first_ms INTEGER NOT NULL,
    last_ms INTEGER NOT NULL,
    steps INTEGER NOT NULL,
    input INTEGER,
    output INTEGER,
    reasoning INTEGER,
    cache_read INTEGER,
    cache_write INTEGER,
    priced TEXT NOT NULL,
    covered INTEGER NOT NULL,
    unpriced INTEGER NOT NULL,
    pending INTEGER NOT NULL,
    reasons TEXT NOT NULL,
    session TEXT,
    work_class TEXT,
    PRIMARY KEY(kind,hour_number,run_key,provider,model),
    FOREIGN KEY(kind,hour_number) REFERENCES summary_hour(kind,hour_number)
) WITHOUT ROWID;
CREATE INDEX summary_run_key_time ON summary_run_cell(run_key,kind,hour_number);
CREATE INDEX summary_run_activity ON summary_run_cell(kind,last_ms DESC,run_key);
CREATE INDEX usage_event_run_time ON usage_event(run_ref,occurred_at_ms)
    WHERE run_ref IS NOT NULL;
CREATE INDEX usage_event_flow_time ON usage_event(flow_ref,occurred_at_ms)
    WHERE flow_ref IS NOT NULL;
INSERT OR IGNORE INTO summary_dirty_hour SELECT kind,hour_number FROM summary_hour;
"#,
    },
    Migration {
        version: 35,
        statements: r#"
ALTER TABLE multiplier_observation ADD COLUMN plan TEXT;
ALTER TABLE multiplier_observation ADD COLUMN routing_eligible INTEGER NOT NULL DEFAULT 0 CHECK(routing_eligible IN (0,1));
ALTER TABLE multiplier_observation ADD COLUMN eligibility_reasons TEXT NOT NULL DEFAULT '["legacy_estimator"]';
ALTER TABLE multiplier_observation ADD COLUMN clean_hours_share REAL NOT NULL DEFAULT 0 CHECK(clean_hours_share BETWEEN 0 AND 1);
ALTER TABLE multiplier_observation ADD COLUMN saturated_share REAL NOT NULL DEFAULT 0 CHECK(saturated_share BETWEEN 0 AND 1);
ALTER TABLE multiplier_observation ADD COLUMN gap_share REAL NOT NULL DEFAULT 0 CHECK(gap_share BETWEEN 0 AND 1);
ALTER TABLE multiplier_observation ADD COLUMN unpriced_share REAL NOT NULL DEFAULT 0 CHECK(unpriced_share BETWEEN 0 AND 1);
ALTER TABLE multiplier_observation ADD COLUMN unconfigured_accounts TEXT NOT NULL DEFAULT '[]';
ALTER TABLE multiplier_observation ADD COLUMN source_backlog TEXT NOT NULL DEFAULT '[]';
ALTER TABLE multiplier_observation ADD COLUMN unattributed_event_share REAL NOT NULL DEFAULT 0 CHECK(unattributed_event_share BETWEEN 0 AND 1);
ALTER TABLE multiplier_observation ADD COLUMN zero_measurement_events INTEGER NOT NULL DEFAULT 0 CHECK(zero_measurement_events>=0);
ALTER TABLE multiplier_observation ADD COLUMN missing_quantity_events INTEGER NOT NULL DEFAULT 0 CHECK(missing_quantity_events>=0);
DROP TRIGGER multiplier_observation_insert_once;
CREATE TRIGGER multiplier_observation_insert_once BEFORE INSERT ON multiplier_observation
WHEN EXISTS (SELECT 1 FROM multiplier_observation WHERE observation_id=NEW.observation_id)
  OR EXISTS (SELECT 1 FROM multiplier_observation WHERE supersedes=NEW.supersedes)
  OR (NEW.supersedes IS NULL AND EXISTS (SELECT 1 FROM multiplier_observation
      WHERE quota_provider=NEW.quota_provider AND observation_day=NEW.observation_day
        AND plan IS NEW.plan AND supersedes IS NULL))
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_insert_once'); END;
DROP INDEX multiplier_observation_one_original_per_day;
CREATE UNIQUE INDEX multiplier_observation_one_original_per_day
ON multiplier_observation(quota_provider, observation_day, COALESCE(plan,'')) WHERE supersedes IS NULL;
CREATE TRIGGER multiplier_observation_same_plan BEFORE INSERT ON multiplier_observation
WHEN NEW.supersedes IS NOT NULL AND EXISTS (
 SELECT 1 FROM multiplier_observation parent WHERE parent.observation_id=NEW.supersedes
 AND parent.plan IS NOT NEW.plan)
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_same_plan'); END;
"#,
    },
    Migration {
        version: 36,
        statements: r#"
CREATE TABLE credential_window_sample (
    provider           TEXT NOT NULL CHECK(length(provider)>0),
    credential_ref     TEXT NOT NULL CHECK(length(credential_ref)>0),
    api_provider       TEXT,
    pool_key           TEXT NOT NULL,
    fetched_at_instant INTEGER NOT NULL,
    used_percent       REAL NOT NULL CHECK(used_percent BETWEEN 0 AND 100),
    raw_used_percent   REAL,
    resets_at          TEXT,
    window_minutes     INTEGER,
    source             TEXT,
    PRIMARY KEY(provider,credential_ref,pool_key,fetched_at_instant)
);
CREATE TRIGGER credential_window_sample_no_update BEFORE UPDATE ON credential_window_sample
BEGIN SELECT RAISE(ABORT, 'credential_window_sample is append-only'); END;
CREATE TRIGGER credential_window_sample_no_delete BEFORE DELETE ON credential_window_sample
BEGIN SELECT RAISE(ABORT, 'credential_window_sample is append-only'); END;
"#,
    },
];

// The migration table and configured SQLite timeout are small fixed values.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#[cfg(test)]
mod tests {
    include!("store_v33_tests.rs");

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

    /// The hourly store migrated to the latest schema, for tests that record
    /// through ingest, whose event queries include `flow_ref`.
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
        let mut conn = pre19();
        let mut event = sample_event("overflow");
        event.attempt_ordinal = None;
        insert_event(&conn, &event).unwrap();
        conn.execute_batch("DROP TRIGGER usage_event_update_guard")
            .unwrap();
        conn.execute("UPDATE usage_event SET attempt_ordinal=4294967296", [])
            .unwrap();
        // Ingest reads `flow_ref` (added in v26), which this pre-v19 store
        // lacks; add it so the read reaches the overflowing ordinal.
        conn.execute_batch("ALTER TABLE usage_event ADD COLUMN flow_ref TEXT")
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
        // The backfill query is measured on its first batch, which starts at
        // the lowest event id (`>= ''`) rather than after a cursor.
        let subscription = real_query(
            include_str!("subscription.rs"),
            "SELECT DISTINCT event.event_id",
        )
        .replace("{comparison}", ">=");
        let tokens = real_query(
            include_str!("multiplier.rs"),
            "SELECT e.event_key, e.producer, e.provider, e.model, m.role, m.unit, m.source,",
        );
        let mut cases = vec![(
            "backfill",
            subscription,
            vec![Integer(100), Text(String::new())],
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
            .map(|(_, sql, values, _)| query_metrics(&conn, sql, values))
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
            crate::subscription::coverage_backfill_event_ids(&conn, None, 100)
                .unwrap()
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

    /// The chain restarts at v1 and is contiguous; the runner records every
    /// version exactly once even when run twice. Derived from MIGRATIONS so a
    /// new migration cannot silently drop this test's coverage (the typed
    /// count it used to carry would have needed editing on every migration,
    /// a hand-written count to maintain whenever a migration is added).
    #[test]
    fn migration_chain_restarts_at_v1_and_runner_records_it_once() {
        assert!(!MIGRATIONS.is_empty());
        let expected: Vec<u32> = (1..=MIGRATIONS.len() as u32).collect();
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
