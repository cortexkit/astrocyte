// SQL for migrations 1 through 20.
// Applied migrations are never edited.

pub(in crate::store) const V1: &str = r#"
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
"#;

pub(in crate::store) const V2: &str = r#"
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
"#;

pub(in crate::store) const V3: &str = r#"-- window_sample: append-only capacity history (issue #1).
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
"#;

pub(in crate::store) const V4: &str = r#"-- auth_selection: the producer's credential id, carried verbatim.
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
"#;

pub(in crate::store) const V5: &str = r#"-- Generalise producer runtime coordinates. BROCA's pair is encoded as
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
"#;

pub(in crate::store) const V6: &str = r#"-- Widen priced_charge.unpriced_reason with 'rate_inherited': a catalog rate the
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
"#;

pub(in crate::store) const V7: &str = r#"-- The store cannot establish which path wrote a pre-v7 row, and a migration
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
"#;

pub(in crate::store) const V8: &str = r#"-- Basis corrections now append their own priced-charge heads, so
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

"#;

pub(in crate::store) const V9: &str = r#"-- File-backed readers can complete a drain without a producer lineage or
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
"#;

pub(in crate::store) const V10: &str = r#"-- redelivery_log counts a producer sending the same record again. The Pi and
-- OpenCode file readers wrote a row every time they re-read a session file,
-- which records our own re-read as the producer's behaviour. Those rows are
-- false, nothing reads the table yet, and ingest no longer writes them for
-- read-path arrivals, so they are deleted. Rows from pushing producers are
-- genuine and stay untouched. Space is reclaimed out of band; VACUUM cannot
-- run inside the migrator's transaction.
DELETE FROM redelivery_log WHERE producer IN ('pi','opencode');
"#;

pub(in crate::store) const V11: &str = r#"-- The THALAMUS usage.export producer lane is retired. A producer reports only
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
"#;

pub(in crate::store) const V12: &str = r#"-- The pricing pass used to find its work by scanning every measurement in
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
"#;

pub(in crate::store) const V13: &str = r#"-- BROCA's charge-basis provenance, carried verbatim on every fact it exports.
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
"#;

pub(in crate::store) const V14: &str = r#"-- A file reader that succeeded before can fail its latest drain. Its
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
"#;

pub(in crate::store) const V15: &str = r#"-- The derived subscription multiplier: its desired configuration, and the
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
"#;

pub(in crate::store) const V16: &str = r#"-- A recorded multiplier day can be corrected. Rows stay append-only: a
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
"#;

pub(in crate::store) const V17: &str = r#"-- desired_config.harness_ingest holds the member's canonical JSON: which
-- producers the refresher drains. NULL on a generation committed before this
-- migration means that generation did not record the member; it drained every
-- producer, so it is read as "all on", never as "off".
ALTER TABLE desired_config ADD COLUMN harness_ingest TEXT
    CHECK (harness_ingest IS NULL OR json_valid(harness_ingest));
"#;

pub(in crate::store) const V18: &str = r#"-- window_sample.breakdown holds the window's per-surface consumption breakdown
-- serialized as JSON exactly as decoded (asOf, windowStartedAt, rows with key
-- and sharePercent, in the order received), or NULL when the window carried none.
-- NULL means insula published no breakdown for that reading, never "no other surface".
ALTER TABLE window_sample ADD COLUMN breakdown TEXT
    CHECK (breakdown IS NULL OR json_valid(breakdown));
"#;

pub(in crate::store) const V19: &str = r#"-- Validate before changing any durable schema or row. The migration runner
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
"#;

pub(in crate::store) const V20: &str = r#"-- Caller keys distinguish correction retries from separate equal adjustments.
-- Internal statement settlements and historical adjustments have no caller key.
ALTER TABLE adjustment ADD COLUMN correction_id TEXT CHECK (
    correction_id IS NULL OR (
        typeof(correction_id) = 'text'
        AND length(CAST(correction_id AS BLOB)) BETWEEN 1 AND 128
    )
);
CREATE UNIQUE INDEX adjustment_correction_id
    ON adjustment (correction_id) WHERE correction_id IS NOT NULL;
"#;
