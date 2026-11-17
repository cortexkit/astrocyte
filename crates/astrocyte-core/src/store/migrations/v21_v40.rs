// SQL for migrations 21 through 40.
// Applied migrations are never edited.

pub(in crate::store) const V21: &str = r#"CREATE TABLE set_aside_record (
    source_key TEXT NOT NULL,
    locator TEXT NOT NULL,
    reason TEXT NOT NULL,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL CHECK (last_seen_ms >= first_seen_ms),
    PRIMARY KEY (source_key, locator)
);
"#;

pub(in crate::store) const V22: &str = r#"CREATE TRIGGER adjustment_no_update
BEFORE UPDATE ON adjustment
BEGIN
    SELECT RAISE(ABORT, 'adjustment_no_update');
END;

CREATE TRIGGER adjustment_no_delete
BEFORE DELETE ON adjustment
BEGIN
    SELECT RAISE(ABORT, 'adjustment_no_delete');
END;
"#;

pub(in crate::store) const V23: &str = r#"-- Offline rebuild: stop readers and writers and keep a rollback backup first.
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
"#;

pub(in crate::store) const V24: &str = r#"-- Disposable derived data. Per-call facts and immutable charge history remain authoritative.
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
"#;

pub(in crate::store) const V25: &str = r#"-- UNIQUE(event_id) already enforces natural-key uniqueness for lineage-absent
-- events: ingest's only writer stores the hash of (producer, lineage, ref) and
-- never an id from its caller. This index duplicated that check and was its
-- largest avoidable cost, a random-key insert on every recorded event.
DROP INDEX usage_event_natural_key_lineage_absent;
"#;

pub(in crate::store) const V26: &str = r#"-- BROCA's batch decision and extraction models answer batches that belong to
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
"#;

pub(in crate::store) const V27: &str = r#"-- Per-step detail a harness reader keeps beside an event for cache
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
"#;

pub(in crate::store) const V28: &str = r#"-- BROCA model steps are diagnostics, never additional spend.
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
"#;

pub(in crate::store) const V29: &str = r#"-- The live step feed behind usage.steps.read. Each row is one emission of a
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
"#;

pub(in crate::store) const V30: &str = r#"-- Another module can declare harness stores for astrocyte to read, such as
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
"#;

pub(in crate::store) const V31: &str = r#"-- REPLACE deletes conflicting rows without firing DELETE triggers when
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
"#;

pub(in crate::store) const V32: &str = r#"
CREATE INDEX step_feed_session_cursor ON step_feed(harness,session,feed_cursor);
"#;

pub(in crate::store) const V33: &str = r#"-- BROCA steps come from its step log, not export_facts. Refuse to
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
"#;

pub(in crate::store) const V34: &str = r#"
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
"#;

pub(in crate::store) const V35: &str = r#"
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
"#;

pub(in crate::store) const V36: &str = r#"
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
"#;

pub(in crate::store) const V37: &str = r#"
ALTER TABLE usage_event ADD COLUMN media_facts TEXT
    CHECK (media_facts IS NULL OR (json_valid(media_facts) AND json_type(media_facts) = 'object'));
-- The existing guard protects the older columns; this companion guard makes
-- the added producer facts immutable without changing any previous condition.
CREATE TRIGGER usage_event_media_facts_update_guard
BEFORE UPDATE ON usage_event
WHEN NEW.media_facts IS NOT OLD.media_facts
BEGIN SELECT RAISE(ABORT, 'usage_event_media_facts_update_guard'); END;

-- Feed sequence numbers are unsigned 64-bit wire values, so text preserves the
-- full domain rather than narrowing them to SQLite's signed integer range.
CREATE TABLE media_feed_cursor (
    source_key TEXT PRIMARY KEY CHECK (source_key = 'colliculus'),
    through_seq TEXT NOT NULL,
    skipped_local INTEGER NOT NULL DEFAULT 0 CHECK (skipped_local >= 0)
);
"#;

pub(in crate::store) const V38: &str = r#"
CREATE TABLE thalamus_payer_cursor (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    incarnation TEXT NOT NULL,
    through_seq INTEGER NOT NULL
);
CREATE TABLE thalamus_payer_session (
    harness TEXT NOT NULL,
    session TEXT NOT NULL,
    owner_principal TEXT,
    declared_at_ms INTEGER NOT NULL,
    PRIMARY KEY(harness,session)
) WITHOUT ROWID;
CREATE TABLE thalamus_payer_record (
    attempt_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    incarnation TEXT NOT NULL,
    harness TEXT NOT NULL,
    session TEXT NOT NULL,
    request_kind TEXT NOT NULL,
    step_ref TEXT,
    recorded_at_ms INTEGER NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    charge_basis TEXT,
    auth_selection TEXT NOT NULL,
    account_ref TEXT,
    outcome TEXT,
    provider_request_id TEXT,
    provider_response_id TEXT,
    usage_json TEXT
) WITHOUT ROWID;
CREATE INDEX thalamus_payer_step ON thalamus_payer_record(harness,step_ref);
CREATE TRIGGER thalamus_payer_record_insert_once BEFORE INSERT ON thalamus_payer_record
WHEN EXISTS(SELECT 1 FROM thalamus_payer_record WHERE attempt_id=NEW.attempt_id)
BEGIN SELECT RAISE(ABORT, 'thalamus_payer_record_insert_once'); END;
CREATE TRIGGER thalamus_payer_record_no_update BEFORE UPDATE ON thalamus_payer_record
BEGIN SELECT RAISE(ABORT, 'thalamus_payer_record is append-only'); END;
CREATE TRIGGER thalamus_payer_record_no_delete BEFORE DELETE ON thalamus_payer_record
BEGIN SELECT RAISE(ABORT, 'thalamus_payer_record is append-only'); END;
-- Restarting a transcript reader must not restart an undated call's wait.
CREATE TABLE thalamus_payer_first_seen (
    event_id TEXT PRIMARY KEY,
    first_seen_ms INTEGER NOT NULL
) WITHOUT ROWID;
-- A distinct identity counts once even after a cursor resets or a file is copied.
CREATE TABLE thalamus_payer_coverage (
    kind TEXT NOT NULL,
    identity TEXT NOT NULL,
    PRIMARY KEY(kind,identity)
) WITHOUT ROWID;
"#;

pub(in crate::store) const V39: &str = r#"
CREATE TABLE vault_plan_history (
    history_id INTEGER PRIMARY KEY,
    credential_id TEXT NOT NULL CHECK(length(credential_id)>0),
    provider TEXT,
    account_id TEXT,
    plan_tier TEXT,
    plan_key TEXT,
    source TEXT NOT NULL,
    effective_from_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL
);
CREATE INDEX vault_plan_record_history ON vault_plan_history(credential_id,effective_from_ms,history_id);
CREATE INDEX vault_plan_account_history ON vault_plan_history(account_id,effective_from_ms);
CREATE TRIGGER vault_plan_history_no_update BEFORE UPDATE ON vault_plan_history
BEGIN SELECT RAISE(ABORT, 'vault_plan_history is append-only'); END;
CREATE TRIGGER vault_plan_history_no_delete BEFORE DELETE ON vault_plan_history
BEGIN SELECT RAISE(ABORT, 'vault_plan_history is append-only'); END;
CREATE TRIGGER vault_plan_history_insert_once BEFORE INSERT ON vault_plan_history
WHEN EXISTS (SELECT 1 FROM vault_plan_history WHERE history_id=NEW.history_id)
BEGIN SELECT RAISE(ABORT, 'vault_plan_history_insert_once'); END;
CREATE TABLE vault_plan_seed (singleton INTEGER PRIMARY KEY CHECK(singleton=1));
"#;

pub(in crate::store) const V40: &str = r#"
-- Null preserves the complete incomplete-call discovery on the first v2 read
-- after upgrade. Later full reads commit the floor with their timestamp cursor.
ALTER TABLE source_runtime ADD COLUMN open_row_floor INTEGER;
ALTER TABLE declared_store_cursor ADD COLUMN open_row_floor INTEGER;
"#;
