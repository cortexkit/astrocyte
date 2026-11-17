// SQL for migrations 41 through 58.
// Applied migrations are never edited.

pub(in crate::store) const V41: &str = r#"
CREATE TABLE step_message (
    event_key INTEGER PRIMARY KEY REFERENCES usage_event(event_key),
    message_id TEXT NOT NULL CHECK(length(message_id)>0)
);
CREATE TRIGGER step_message_no_update BEFORE UPDATE ON step_message
BEGIN SELECT RAISE(ABORT, 'step_message_no_update'); END;
CREATE TRIGGER step_message_no_delete BEFORE DELETE ON step_message
BEGIN SELECT RAISE(ABORT, 'step_message_no_delete'); END;
CREATE TRIGGER step_message_insert_once BEFORE INSERT ON step_message
WHEN EXISTS (SELECT 1 FROM step_message WHERE event_key=NEW.event_key)
BEGIN SELECT RAISE(ABORT, 'step_message_insert_once'); END;
"#;

pub(in crate::store) const V42: &str = r#"
-- A day's head lookups name its provider, day and plan. The only index that
-- starts with provider and day is partial on supersedes IS NULL, which a head
-- query cannot use because a correction supersedes its day's original. This
-- index covers every row, so finding a day's head, or whether the day has any
-- row, is a seek rather than a scan of every observation.
CREATE INDEX multiplier_observation_provider_day_plan
    ON multiplier_observation(quota_provider, observation_day, plan);
-- A listing is compared with the credential's newest plan row by history ID.
-- vault_plan_record_history orders a credential's rows by effective time
-- first, so that comparison sorted the credential's whole history.
CREATE INDEX vault_plan_credential_latest
    ON vault_plan_history(credential_id, history_id);
"#;

pub(in crate::store) const V43: &str = r#"
-- Statement imports by statement: an import's ordinal counts that statement's
-- earlier imports, which otherwise scans the import log of every statement.
CREATE INDEX statement_import_by_statement ON statement_import (statement_id);

-- Closed plan versions by end time, so checking a new plan against an
-- account's closed history seeks the versions ending after its start instead
-- of walking every version the account ever had.
CREATE INDEX account_subscription_closed_end
    ON account_subscription (provider, account_ref, effective_to_ms)
    WHERE effective_to_ms IS NOT NULL;

-- The same overlap rule as before, split by whether the existing version is
-- open or closed so each half can seek an index: the open version through
-- account_subscription_one_open_interval, closed versions through
-- account_subscription_closed_end.
DROP TRIGGER account_subscription_no_overlap;
CREATE TRIGGER account_subscription_no_overlap
BEFORE INSERT ON account_subscription
WHEN EXISTS (
    SELECT 1 FROM account_subscription AS existing
    WHERE existing.provider = NEW.provider
      AND existing.account_ref = NEW.account_ref
      AND existing.effective_to_ms IS NULL
      AND (NEW.effective_to_ms IS NULL
           OR existing.effective_from_ms < NEW.effective_to_ms)
) OR EXISTS (
    SELECT 1 FROM account_subscription AS existing
    WHERE existing.provider = NEW.provider
      AND existing.account_ref = NEW.account_ref
      AND existing.effective_to_ms > NEW.effective_from_ms
      AND (NEW.effective_to_ms IS NULL
           OR existing.effective_from_ms < NEW.effective_to_ms)
)
BEGIN
    SELECT RAISE(ABORT, 'account_subscription_no_overlap');
END;

-- Summary hours that are not clean because they were never built at their
-- current revision or were built in an older format. Run detail finds them
-- here instead of reading every hour. Format 2 is the current summary format;
-- a query that names another format can't use this index and reads the table.
CREATE INDEX summary_hour_stale ON summary_hour (kind, hour_number)
    WHERE built_revision IS NOT revision OR format_version IS NOT 2;
"#;

pub(in crate::store) const V44: &str = r#"
-- A derived projection of the cache-read facts already in usage_event,
-- measurement and cache_step_sequence. Cache diagnostics ask, after every
-- commit that touches a session, for the last step before a point that
-- reported a cache read and whether any dated step read a positive count.
-- Answered from the ledger, both walk the session's whole history when it has
-- no such step; answered here, each is one seek.

-- One row per (producer, session) with a dated step that has a plain
-- cache-read measurement (role 'cache_read', exponent 0). first_positive_cursor
-- is the smallest cache cursor of such a step whose measurement is above
-- zero; NULL means no dated step of the session has read a positive count.
CREATE TABLE cache_read_session (
    session_id INTEGER PRIMARY KEY,
    producer TEXT NOT NULL,
    session TEXT NOT NULL,
    first_positive_cursor INTEGER,
    UNIQUE (producer, session)
);
-- One row per dated step with a plain cache-read measurement, zero reads
-- included: a step that read zero is still the baseline of the next one. The
-- key orders a session's steps by time, then event key, as the baseline read
-- orders them, so the newest one before a point is one backwards seek.
CREATE TABLE cache_read_step (
    session_id INTEGER NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    event_key INTEGER NOT NULL,
    cursor INTEGER NOT NULL,
    PRIMARY KEY (session_id, occurred_at_ms, event_key)
) WITHOUT ROWID;

-- Backfill from the ledger before the trigger exists.
INSERT INTO cache_read_session(producer, session, first_positive_cursor)
SELECT e.producer, e.session, MIN(CASE WHEN m.quantity_units > 0 THEN s.cursor END)
FROM measurement m
CROSS JOIN usage_event e ON e.event_key = m.event_key
CROSS JOIN cache_step_sequence s ON s.event_key = e.event_key
WHERE m.role = 'cache_read' AND m.quantity_exponent = 0
  AND e.session IS NOT NULL AND e.occurred_at_ms IS NOT NULL
GROUP BY e.producer, e.session;
INSERT INTO cache_read_step(session_id, occurred_at_ms, event_key, cursor)
SELECT DISTINCT c.session_id, e.occurred_at_ms, e.event_key, s.cursor
FROM measurement m
CROSS JOIN usage_event e ON e.event_key = m.event_key
CROSS JOIN cache_step_sequence s ON s.event_key = e.event_key
CROSS JOIN cache_read_session c ON c.producer = e.producer AND c.session = e.session
WHERE m.role = 'cache_read' AND m.quantity_exponent = 0
  AND e.occurred_at_ms IS NOT NULL
ORDER BY 1, 2, 3;

-- Measurements are insert-only and an event's producer, session and time
-- never change, so maintaining the projection on measurement insert keeps it
-- exact in any ingest order, including a late measurement for an old event.
-- NOT EXISTS guards, rather than OR IGNORE, keep every statement free of
-- conflicts whatever conflict clause the outer insert uses.
CREATE TRIGGER cache_read_projection AFTER INSERT ON measurement
WHEN NEW.role = 'cache_read' AND NEW.quantity_exponent = 0
BEGIN
    INSERT INTO cache_read_session(producer, session)
    SELECT e.producer, e.session FROM usage_event e
    WHERE e.event_key = NEW.event_key AND e.session IS NOT NULL
      AND e.occurred_at_ms IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM cache_read_session c
          WHERE c.producer = e.producer AND c.session = e.session);
    INSERT INTO cache_read_step(session_id, occurred_at_ms, event_key, cursor)
    SELECT c.session_id, e.occurred_at_ms, e.event_key, s.cursor
    FROM usage_event e
    CROSS JOIN cache_read_session c ON c.producer = e.producer AND c.session = e.session
    CROSS JOIN cache_step_sequence s ON s.event_key = e.event_key
    WHERE e.event_key = NEW.event_key AND e.occurred_at_ms IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM cache_read_step r WHERE r.session_id = c.session_id
          AND r.occurred_at_ms = e.occurred_at_ms AND r.event_key = e.event_key);
    UPDATE cache_read_session
    SET first_positive_cursor =
        (SELECT s.cursor FROM cache_step_sequence s WHERE s.event_key = NEW.event_key)
    WHERE NEW.quantity_units > 0
      AND session_id = (SELECT c.session_id FROM usage_event e
          CROSS JOIN cache_read_session c ON c.producer = e.producer AND c.session = e.session
          WHERE e.event_key = NEW.event_key AND e.occurred_at_ms IS NOT NULL)
      AND (first_positive_cursor IS NULL OR first_positive_cursor >
          (SELECT s.cursor FROM cache_step_sequence s WHERE s.event_key = NEW.event_key));
END;
"#;

pub(in crate::store) const V45: &str = r#"
-- Keep stale-hour discovery indexed after observed readings changed the
-- hourly spend populations and run cost totals to summary format 3.
DROP INDEX summary_hour_stale;
CREATE INDEX summary_hour_stale ON summary_hour (kind, hour_number)
    WHERE built_revision IS NOT revision OR format_version IS NOT 3;
"#;

pub(in crate::store) const V46: &str = r#"-- BROCA's routing credential can differ from the vault credential that paid.
-- Preserve the paying credential verbatim from newly read facts. Existing rows
-- stay NULL: adding the nullable column does not backfill the immutable ledger.
ALTER TABLE usage_event ADD COLUMN resolved_credential TEXT;

-- Retain the version 26 guard's conditions and guard the new fact beside the
-- admission credential. The separate media_facts guard remains unchanged.
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
    AND NEW.resolved_credential IS OLD.resolved_credential
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

pub(in crate::store) const V47: &str = r#"
-- Ledger plans for vault credentials that carry no provider account id, such
-- as an API key. A subscription call with no account_ref is covered by the
-- plan whose credential_ref equals the call's paying credential. The columns
-- mirror account_subscription with credential_ref in place of account_ref,
-- and every guard account_subscription has is repeated for the new key: one
-- open interval, no overlap, insert-once (no REPLACE), an update that may only
-- close an open interval, and no delete. account_subscription is unchanged.
CREATE TABLE credential_subscription (
    credential_ref       TEXT NOT NULL CHECK (length(credential_ref) > 0),
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

    PRIMARY KEY (provider, credential_ref, effective_from_ms)
);

CREATE UNIQUE INDEX credential_subscription_one_open_interval
    ON credential_subscription (provider, credential_ref)
    WHERE effective_to_ms IS NULL;

CREATE INDEX credential_subscription_closed_end
    ON credential_subscription (provider, credential_ref, effective_to_ms)
    WHERE effective_to_ms IS NOT NULL;

CREATE TRIGGER credential_subscription_insert_once BEFORE INSERT ON credential_subscription
WHEN EXISTS (SELECT 1 FROM credential_subscription WHERE rowid=NEW.rowid)
  OR EXISTS (SELECT 1 FROM credential_subscription WHERE provider=NEW.provider
    AND credential_ref=NEW.credential_ref AND effective_from_ms=NEW.effective_from_ms)
  OR (NEW.effective_to_ms IS NULL AND EXISTS (SELECT 1 FROM credential_subscription
      WHERE provider=NEW.provider AND credential_ref=NEW.credential_ref AND effective_to_ms IS NULL))
BEGIN
    SELECT RAISE(ABORT, 'credential_subscription_insert_once');
END;

-- SQLite fires the newest trigger first, so the overlap trigger is created
-- after insert-once: an overlapping plan is refused as an overlap, as the
-- account_subscription overlap trigger recreated in version 43 refuses it.
CREATE TRIGGER credential_subscription_no_overlap
BEFORE INSERT ON credential_subscription
WHEN EXISTS (
    SELECT 1 FROM credential_subscription AS existing
    WHERE existing.provider = NEW.provider
      AND existing.credential_ref = NEW.credential_ref
      AND existing.effective_to_ms IS NULL
      AND (NEW.effective_to_ms IS NULL
           OR existing.effective_from_ms < NEW.effective_to_ms)
) OR EXISTS (
    SELECT 1 FROM credential_subscription AS existing
    WHERE existing.provider = NEW.provider
      AND existing.credential_ref = NEW.credential_ref
      AND existing.effective_to_ms > NEW.effective_from_ms
      AND (NEW.effective_to_ms IS NULL
           OR existing.effective_from_ms < NEW.effective_to_ms)
)
BEGIN
    SELECT RAISE(ABORT, 'credential_subscription_no_overlap');
END;

CREATE TRIGGER credential_subscription_update_guard
BEFORE UPDATE ON credential_subscription
WHEN NOT (
    NEW.credential_ref IS OLD.credential_ref
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
    SELECT RAISE(ABORT, 'credential_subscription_update_guard');
END;

CREATE TRIGGER credential_subscription_no_delete
BEFORE DELETE ON credential_subscription
BEGIN
    SELECT RAISE(ABORT, 'credential_subscription_no_delete');
END;
"#;

pub(in crate::store) const V48: &str = r#"
-- Format 3 lacks the paying credential. Discard only its derived cells;
-- retained hour metadata keeps reports on the ledger until the prioritized
-- stale-format rebuild publishes format 4. Ledger dirty-hour triggers remain.
DROP TABLE summary_quantity;
DROP TABLE summary_money;
DROP TABLE summary_cell;
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
    paying_credential INTEGER NOT NULL REFERENCES summary_value(value_id),
    UNIQUE(kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,paying_credential),
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
DROP INDEX summary_hour_stale;
CREATE INDEX summary_hour_stale ON summary_hour (kind, hour_number)
    WHERE built_revision IS NOT revision OR format_version IS NOT 4;
"#;

pub(in crate::store) const V49: &str = r#"
-- Old quantity sums cannot distinguish missing quantities from stated zero.
-- Discard derived cells only; retained hours rebuild at format 5, together
-- with their run cells, while readers use the ledger for stale hours.
DROP TABLE summary_quantity;
DROP TABLE summary_money;
DROP TABLE summary_cell;
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
    paying_credential INTEGER NOT NULL REFERENCES summary_value(value_id),
    UNIQUE(kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,paying_credential),
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
    stated_observations INTEGER NOT NULL CHECK(stated_observations>=0 AND stated_observations<=observations),
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
DROP INDEX summary_hour_stale;
CREATE INDEX summary_hour_stale ON summary_hour (kind, hour_number)
    WHERE built_revision IS NOT revision OR format_version IS NOT 5;
"#;

pub(in crate::store) const V50: &str = r#"
ALTER TABLE usage_event ADD COLUMN runner TEXT
    CHECK(runner IN ('broca','thalamus','colliculus'));
DROP TRIGGER usage_event_update_guard;
-- Historical runner evidence is frozen before the write-once guard returns.
-- Uncorrelated membership avoids repeated scans of unindexed payer sessions.
-- Claude Code's stored owner session can differ from its native payer session,
-- so a payer's step reference is also evidence. Harness qualifies both keys.
UPDATE usage_event SET runner = CASE
    WHEN producer='broca' THEN 'broca'
    WHEN producer='thalamus-harness' THEN 'thalamus'
    WHEN producer='colliculus' THEN 'colliculus'
    WHEN (producer, session) IN (SELECT harness, session FROM thalamus_payer_session
                                 UNION SELECT harness, session FROM thalamus_payer_record)
      OR (producer, producer_ref) IN (SELECT harness, step_ref FROM thalamus_payer_record
                                      WHERE step_ref IS NOT NULL) THEN 'thalamus'
    ELSE NULL END
WHERE producer IN ('broca','thalamus-harness','colliculus')
   OR (producer, session) IN (SELECT harness, session FROM thalamus_payer_session
                              UNION SELECT harness, session FROM thalamus_payer_record)
   OR (producer, producer_ref) IN (SELECT harness, step_ref FROM thalamus_payer_record
                                   WHERE step_ref IS NOT NULL);
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
    AND NEW.runner IS OLD.runner
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
    AND NEW.resolved_credential IS OLD.resolved_credential
    AND NEW.charge_basis_mapping_version IS OLD.charge_basis_mapping_version
    AND NEW.charge_basis_overrides IS OLD.charge_basis_overrides
    AND (NEW.charge_basis IS OLD.charge_basis
         OR NEW.pricing_generation = OLD.pricing_generation + 1)
    AND (NEW.pricing_generation = OLD.pricing_generation
         OR NEW.pricing_generation = OLD.pricing_generation + 1)
    AND (NEW.currency IS OLD.currency
         OR (OLD.currency IS NULL AND NEW.currency IS NOT NULL))
)
BEGIN
    SELECT RAISE(ABORT, 'usage_event_update_guard');
END;
-- The existing insert-once guard also prevents REPLACE from changing runner.
-- Only derived cells are discarded; old hours use the ledger until rebuilt.
DROP TABLE summary_quantity;
DROP TABLE summary_money;
DROP TABLE summary_cell;
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
    paying_credential INTEGER NOT NULL REFERENCES summary_value(value_id),
    runner INTEGER NOT NULL REFERENCES summary_value(value_id),
    harness INTEGER NOT NULL REFERENCES summary_value(value_id),
    UNIQUE(kind,hour_number,producer,provider,model,account,auth_selection,charge_basis,work_class,project,currency,paying_credential,runner,harness),
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
    stated_observations INTEGER NOT NULL CHECK(stated_observations>=0 AND stated_observations<=observations),
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
DROP INDEX summary_hour_stale;
CREATE INDEX summary_hour_stale ON summary_hour (kind, hour_number)
    WHERE built_revision IS NOT revision OR format_version IS NOT 6;
"#;

pub(in crate::store) const V51: &str = r#"-- Runner events are not replayed. Keep their content-free evidence for
-- 30 days independently of the feed, and intern runner-local digests once.
CREATE TABLE step_digest (
    digest_key INTEGER PRIMARY KEY,
    value TEXT NOT NULL UNIQUE
);
CREATE TABLE step_evidence (
    evidence_key INTEGER PRIMARY KEY AUTOINCREMENT,
    harness TEXT NOT NULL,
    step_ref TEXT NOT NULL,
    runner TEXT,
    first_feed_cursor INTEGER,
    recorded_at_ms INTEGER NOT NULL,
    facts_json TEXT NOT NULL,
    tools_key INTEGER REFERENCES step_digest(digest_key),
    system_key INTEGER REFERENCES step_digest(digest_key),
    UNIQUE(harness,step_ref)
);
CREATE INDEX step_evidence_tools ON step_evidence(tools_key);
CREATE INDEX step_evidence_system ON step_evidence(system_key);
-- Each expired or changed evidence row can orphan only its two digest keys.
-- Indexed references avoid a full-table orphan scan during retention.
CREATE TRIGGER step_evidence_delete_digests AFTER DELETE ON step_evidence
BEGIN
    DELETE FROM step_digest WHERE digest_key IN (OLD.tools_key,OLD.system_key)
      AND NOT EXISTS (SELECT 1 FROM step_evidence WHERE tools_key=digest_key)
      AND NOT EXISTS (SELECT 1 FROM step_evidence WHERE system_key=digest_key);
END;
CREATE TRIGGER step_evidence_update_digests AFTER UPDATE ON step_evidence
BEGIN
    DELETE FROM step_digest WHERE digest_key IN (OLD.tools_key,OLD.system_key)
      AND NOT EXISTS (SELECT 1 FROM step_evidence WHERE tools_key=digest_key)
      AND NOT EXISTS (SELECT 1 FROM step_evidence WHERE system_key=digest_key);
END;
-- Explanations outlive their evidence. NULL is an appended correction to no
-- cause, not a deletion of history; newest cause_key wins for a step identity.
CREATE TABLE step_cause (
    cause_key INTEGER PRIMARY KEY AUTOINCREMENT,
    harness TEXT NOT NULL,
    step_ref TEXT NOT NULL,
    cause TEXT
);
CREATE INDEX step_cause_identity ON step_cause(harness,step_ref,cause_key);
CREATE TRIGGER step_cause_no_update BEFORE UPDATE ON step_cause
BEGIN SELECT RAISE(ABORT, 'step_cause_no_update'); END;
CREATE TRIGGER step_cause_no_delete BEFORE DELETE ON step_cause
BEGIN SELECT RAISE(ABORT, 'step_cause_no_delete'); END;
CREATE TRIGGER step_cause_insert_once BEFORE INSERT ON step_cause
WHEN EXISTS (SELECT 1 FROM step_cause WHERE cause_key=NEW.cause_key)
BEGIN SELECT RAISE(ABORT, 'step_cause_insert_once'); END;
"#;

pub(in crate::store) const V52: &str = r#"-- Keep comparison keys separate from immutable feed JSON. A null latest
-- cursor excludes evidence whose one-day feed rows have already expired.
ALTER TABLE step_evidence ADD COLUMN session TEXT;
ALTER TABLE step_evidence ADD COLUMN order_ms INTEGER;
ALTER TABLE step_evidence ADD COLUMN feed_source TEXT;
ALTER TABLE step_evidence ADD COLUMN occurred_at_ms INTEGER;
ALTER TABLE step_evidence ADD COLUMN reports_cache INTEGER NOT NULL DEFAULT 0;
ALTER TABLE step_evidence ADD COLUMN positive_read INTEGER NOT NULL DEFAULT 0;
ALTER TABLE step_evidence ADD COLUMN latest_feed_cursor INTEGER;

-- Older stores can have feed rows without evidence placeholders. Preserve
-- their first retained emission, but never invent runner-event facts.
INSERT INTO step_evidence(harness,step_ref,recorded_at_ms,facts_json,first_feed_cursor)
SELECT harness,step_ref,MIN(committed_at_ms),'{}',MIN(feed_cursor)
FROM step_feed GROUP BY harness,step_ref
ON CONFLICT(harness,step_ref) DO UPDATE SET
    first_feed_cursor=COALESCE(step_evidence.first_feed_cursor,excluded.first_feed_cursor);
UPDATE step_evidence SET order_ms=json_extract(facts_json,'$.started_at_ms'),
    latest_feed_cursor=(SELECT MAX(feed_cursor) FROM step_feed f
    WHERE f.harness=step_evidence.harness AND f.step_ref=step_evidence.step_ref);
UPDATE step_evidence SET
    session=f.session,feed_source=f.source,
    occurred_at_ms=json_extract(f.row_json,'$.occurred_at_ms'),
    order_ms=COALESCE(json_extract(facts_json,'$.started_at_ms'),json_extract(f.row_json,'$.occurred_at_ms')),
    reports_cache=COALESCE(json_type(f.row_json,'$.cache_read')='integer',0),
    positive_read=COALESCE(json_extract(f.row_json,'$.cache_read')>0,0)
FROM step_feed f WHERE f.feed_cursor=step_evidence.latest_feed_cursor;

CREATE INDEX step_evidence_neighbours ON step_evidence(harness,session,runner,order_ms,first_feed_cursor)
    WHERE latest_feed_cursor IS NOT NULL;
CREATE INDEX step_evidence_runner_previous ON step_evidence(harness,session,occurred_at_ms,latest_feed_cursor)
    WHERE feed_source='runner' AND latest_feed_cursor IS NOT NULL;
CREATE INDEX step_evidence_runner_cache ON step_evidence(harness,session,occurred_at_ms,latest_feed_cursor)
    WHERE feed_source='runner' AND latest_feed_cursor IS NOT NULL AND reports_cache=1;
CREATE INDEX step_evidence_runner_positive ON step_evidence(harness,session,latest_feed_cursor)
    WHERE feed_source='runner' AND latest_feed_cursor IS NOT NULL AND positive_read=1;

-- Corrections and file replacements are emissions too. Maintaining the live
-- projection here keeps every emission path, including successor corrections,
-- in the same transaction as its indexed facts.
CREATE TRIGGER step_feed_index_evidence AFTER INSERT ON step_feed
BEGIN
    INSERT INTO step_evidence(harness,step_ref,recorded_at_ms,facts_json,first_feed_cursor,
        session,order_ms,feed_source,occurred_at_ms,reports_cache,positive_read,latest_feed_cursor)
    VALUES (NEW.harness,NEW.step_ref,NEW.committed_at_ms,'{}',NEW.feed_cursor,
        NEW.session,json_extract(NEW.row_json,'$.occurred_at_ms'),NEW.source,
        json_extract(NEW.row_json,'$.occurred_at_ms'),
        COALESCE(json_type(NEW.row_json,'$.cache_read')='integer',0),
        COALESCE(json_extract(NEW.row_json,'$.cache_read')>0,0),NEW.feed_cursor)
    ON CONFLICT(harness,step_ref) DO UPDATE SET
        first_feed_cursor=COALESCE(first_feed_cursor,NEW.feed_cursor),
        session=NEW.session,feed_source=NEW.source,latest_feed_cursor=NEW.feed_cursor,
        occurred_at_ms=excluded.occurred_at_ms,reports_cache=excluded.reports_cache,
        positive_read=excluded.positive_read,
        order_ms=COALESCE(json_extract(facts_json,'$.started_at_ms'),excluded.occurred_at_ms);
END;
CREATE TRIGGER step_feed_unindex_evidence AFTER DELETE ON step_feed
BEGIN
    UPDATE step_evidence SET latest_feed_cursor=NULL
    WHERE harness=OLD.harness AND step_ref=OLD.step_ref AND latest_feed_cursor=OLD.feed_cursor;
END;
-- Evidence expires from its first emission, but a recent replacement can
-- still be in the feed. Drop the event facts on time and retain only a live,
-- unknown-runner placeholder, just as a feed row without evidence compares.
-- Each bounded evidence deletion performs identity seeks, never a feed scan.
CREATE TRIGGER step_evidence_restore_live_placeholder AFTER DELETE ON step_evidence
BEGIN
    INSERT INTO step_evidence(harness,step_ref,recorded_at_ms,facts_json,first_feed_cursor,
        session,order_ms,feed_source,occurred_at_ms,reports_cache,positive_read,latest_feed_cursor)
    SELECT f.harness,f.step_ref,f.committed_at_ms,'{}',
        (SELECT MIN(feed_cursor) FROM step_feed WHERE harness=OLD.harness AND step_ref=OLD.step_ref),
        f.session,json_extract(f.row_json,'$.occurred_at_ms'),f.source,
        json_extract(f.row_json,'$.occurred_at_ms'),
        COALESCE(json_type(f.row_json,'$.cache_read')='integer',0),
        COALESCE(json_extract(f.row_json,'$.cache_read')>0,0),f.feed_cursor
    FROM step_feed f WHERE f.feed_cursor=(SELECT MAX(feed_cursor) FROM step_feed
        WHERE harness=OLD.harness AND step_ref=OLD.step_ref);
END;
"#;

pub(in crate::store) const V53: &str = r#"-- A completed whole-ledger queue check remains fresh across restarts.
-- An absent completion leaves the check due, including after an interrupted pass.
ALTER TABLE pricing_selection_state ADD COLUMN last_queue_reconcile_completed_ms INTEGER
    CHECK (last_queue_reconcile_completed_ms IS NULL OR
           (typeof(last_queue_reconcile_completed_ms) = 'integer' AND last_queue_reconcile_completed_ms >= 0));
"#;

pub(in crate::store) const V54: &str = r#"-- Served timelines are derived caches, never ledger prices.
CREATE TABLE rate_cache_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    catalog_version INTEGER NOT NULL DEFAULT -1,
    generation INTEGER NOT NULL DEFAULT 0,
    after_event_key INTEGER NOT NULL DEFAULT 0,
    twins TEXT NOT NULL DEFAULT '{}'
);
INSERT INTO rate_cache_state(id) VALUES(1);
CREATE TABLE rate_model (
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    catalog_version INTEGER,
    reference_provider TEXT,
    reference_model TEXT,
    last_error TEXT,
    last_attempt INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(provider,model),
    CHECK((reference_provider IS NULL)=(reference_model IS NULL))
) WITHOUT ROWID;
CREATE INDEX rate_model_reference ON rate_model(reference_provider,reference_model);
CREATE TABLE rate_era (
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    fact_key TEXT NOT NULL,
    from_ms INTEGER NOT NULL,
    until_ms INTEGER,
    segment TEXT NOT NULL CHECK(json_valid(segment)),
    PRIMARY KEY(provider,model,fact_key,from_ms),
    FOREIGN KEY(provider,model) REFERENCES rate_model(provider,model),
    CHECK(until_ms IS NULL OR until_ms>from_ms)
) WITHOUT ROWID;
CREATE TABLE rate_dirty_hour (
    hour_number INTEGER PRIMARY KEY
);
"#;

pub(in crate::store) const V55: &str = r#"-- Valuations are independently rebuildable; ledger charges are untouched.
CREATE TABLE valuation_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    rate_generation INTEGER NOT NULL DEFAULT 0,
    config_generation INTEGER NOT NULL DEFAULT 0,
    plan_generation INTEGER NOT NULL DEFAULT 0,
    config TEXT NOT NULL DEFAULT ''
);
INSERT INTO valuation_state(id) VALUES(1);
CREATE TABLE valuation_hour (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    rate_generation INTEGER,
    config_generation INTEGER,
    plan_generation INTEGER,
    state TEXT NOT NULL DEFAULT 'dirty' CHECK(state IN ('dirty','built','not_yet_valued')),
    PRIMARY KEY(kind,hour_number)
) WITHOUT ROWID;
CREATE INDEX valuation_hour_dirty ON valuation_hour(kind,hour_number) WHERE state='dirty';
CREATE TABLE valuation_cell (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    cell_key TEXT NOT NULL CHECK(json_valid(cell_key)),
    summary_key TEXT NOT NULL CHECK(json_valid(summary_key)),
    plan TEXT NOT NULL,
    plan_account TEXT,
    recorded TEXT NOT NULL CHECK(json_valid(recorded)),
    list_numerator TEXT NOT NULL,
    unvalued TEXT NOT NULL CHECK(json_valid(unvalued)),
    PRIMARY KEY(kind,hour_number,cell_key),
    FOREIGN KEY(kind,hour_number) REFERENCES valuation_hour(kind,hour_number)
) WITHOUT ROWID;
CREATE TRIGGER valuation_summary_insert AFTER INSERT ON summary_hour BEGIN
    INSERT INTO valuation_hour(kind,hour_number,revision) VALUES(NEW.kind,NEW.hour_number,1)
    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1,state='dirty';
END;
CREATE TRIGGER valuation_summary_update AFTER UPDATE OF revision ON summary_hour BEGIN
    INSERT INTO valuation_hour(kind,hour_number,revision) VALUES(NEW.kind,NEW.hour_number,1)
    ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1,state='dirty';
END;
INSERT INTO valuation_hour(kind,hour_number) SELECT kind,hour_number FROM summary_hour;
CREATE TRIGGER valuation_account_insert AFTER INSERT ON account_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number>=NEW.effective_from_ms/3600000
      AND (NEW.effective_to_ms IS NULL OR hour_number<NEW.effective_to_ms/3600000+(NEW.effective_to_ms%3600000>0));
    UPDATE valuation_hour SET plan_generation=plan_generation+1 WHERE state='built'
      AND rate_generation=(SELECT rate_generation FROM valuation_state WHERE id=1)
      AND config_generation=(SELECT config_generation FROM valuation_state WHERE id=1)
      AND plan_generation=(SELECT plan_generation FROM valuation_state WHERE id=1);
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
CREATE TRIGGER valuation_credential_insert AFTER INSERT ON credential_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number>=NEW.effective_from_ms/3600000
      AND (NEW.effective_to_ms IS NULL OR hour_number<NEW.effective_to_ms/3600000+(NEW.effective_to_ms%3600000>0));
    UPDATE valuation_hour SET plan_generation=plan_generation+1 WHERE state='built'
      AND rate_generation=(SELECT rate_generation FROM valuation_state WHERE id=1)
      AND config_generation=(SELECT config_generation FROM valuation_state WHERE id=1)
      AND plan_generation=(SELECT plan_generation FROM valuation_state WHERE id=1);
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
-- Closing a plan changes membership from its new end onward, not before it.
CREATE TRIGGER valuation_account_close AFTER UPDATE OF effective_to_ms ON account_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number>=NEW.effective_to_ms/3600000;
    UPDATE valuation_hour SET plan_generation=plan_generation+1 WHERE state='built'
      AND rate_generation=(SELECT rate_generation FROM valuation_state WHERE id=1)
      AND config_generation=(SELECT config_generation FROM valuation_state WHERE id=1)
      AND plan_generation=(SELECT plan_generation FROM valuation_state WHERE id=1);
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
CREATE TRIGGER valuation_credential_close AFTER UPDATE OF effective_to_ms ON credential_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number>=NEW.effective_to_ms/3600000;
    UPDATE valuation_hour SET plan_generation=plan_generation+1 WHERE state='built'
      AND rate_generation=(SELECT rate_generation FROM valuation_state WHERE id=1)
      AND config_generation=(SELECT config_generation FROM valuation_state WHERE id=1)
      AND plan_generation=(SELECT plan_generation FROM valuation_state WHERE id=1);
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
"#;

pub(in crate::store) const V56: &str = r#"-- This table is a rebuildable cache. Retain the ledger record identity
-- so sequential plans and equal account/credential strings never share weights.
DROP TABLE valuation_cell;
CREATE TABLE valuation_cell (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    cell_key TEXT NOT NULL CHECK(json_valid(cell_key)),
    summary_key TEXT NOT NULL CHECK(json_valid(summary_key)),
    plan TEXT NOT NULL,
    plan_account TEXT,
    plan_record TEXT CHECK(plan_record IS NULL OR json_valid(plan_record)),
    recorded TEXT NOT NULL CHECK(json_valid(recorded)),
    list_numerator TEXT NOT NULL,
    unvalued TEXT NOT NULL CHECK(json_valid(unvalued)),
    PRIMARY KEY(kind,hour_number,cell_key),
    FOREIGN KEY(kind,hour_number) REFERENCES valuation_hour(kind,hour_number)
) WITHOUT ROWID;
UPDATE valuation_hour SET state='dirty',revision=revision+1;
"#;

pub(in crate::store) const V57: &str = r#"-- Seed the rate cache with every model already in the ledger. Left to the
-- refresher, discovery reads 2,000 calls per full tick, so a store holding three
-- million calls would take about twelve hours to learn every model in use, and
-- list values and new multiplier days would wait on it. Later calls are
-- discovered incrementally from the newest call recorded here.
INSERT OR IGNORE INTO rate_model(provider,model)
    SELECT DISTINCT provider, model FROM usage_event
    WHERE provider IS NOT NULL AND model IS NOT NULL;
UPDATE rate_cache_state
    SET after_event_key = max(after_event_key, coalesce((SELECT max(event_key) FROM usage_event), 0))
    WHERE id = 1;
"#;

pub(in crate::store) const V58: &str = r#"-- Attribution is publisher evidence, never a mutation of ledger identity.
CREATE TABLE attribution_fact (
    fact_id TEXT PRIMARY KEY,
    publisher TEXT NOT NULL,
    body TEXT NOT NULL CHECK(json_valid(body)),
    subject_kind TEXT NOT NULL CHECK(subject_kind IN ('session','run','flow')),
    subject_runner TEXT,
    subject_harness TEXT,
    subject_session TEXT,
    subject_run_id TEXT,
    subject_flow_id TEXT,
    alfonso TEXT,
    parent_kind TEXT CHECK(parent_kind IN ('session','run','flow')),
    parent_runner TEXT,
    parent_harness TEXT,
    parent_session TEXT,
    parent_run_id TEXT,
    parent_flow_id TEXT,
    workload TEXT NOT NULL,
    task TEXT,
    project_id TEXT,
    effective_from_ms INTEGER NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    supersedes TEXT
) WITHOUT ROWID;
CREATE INDEX attribution_fact_session ON attribution_fact(subject_runner,subject_session)
    WHERE subject_kind='session';
CREATE INDEX attribution_fact_run ON attribution_fact(subject_run_id) WHERE subject_kind='run';
CREATE INDEX attribution_fact_flow ON attribution_fact(subject_flow_id) WHERE subject_kind='flow';
CREATE INDEX attribution_fact_project ON attribution_fact(project_id);
CREATE INDEX attribution_fact_supersedes ON attribution_fact(supersedes) WHERE supersedes IS NOT NULL;
CREATE INDEX attribution_parent_session ON attribution_fact(parent_runner,parent_session) WHERE parent_kind='session';
CREATE INDEX attribution_parent_run ON attribution_fact(parent_run_id) WHERE parent_kind='run';
CREATE INDEX attribution_parent_flow ON attribution_fact(parent_flow_id) WHERE parent_kind='flow';
CREATE TRIGGER attribution_fact_no_update BEFORE UPDATE ON attribution_fact
BEGIN SELECT RAISE(ABORT, 'attribution_fact_no_update'); END;
CREATE TRIGGER attribution_fact_no_delete BEFORE DELETE ON attribution_fact
BEGIN SELECT RAISE(ABORT, 'attribution_fact_no_delete'); END;
CREATE TRIGGER attribution_fact_insert_once BEFORE INSERT ON attribution_fact
WHEN EXISTS (SELECT 1 FROM attribution_fact WHERE fact_id=NEW.fact_id)
BEGIN SELECT RAISE(ABORT, 'attribution_fact_insert_once'); END;

CREATE TABLE placement_fact (
    fact_id TEXT PRIMARY KEY,
    publisher TEXT NOT NULL,
    body TEXT NOT NULL CHECK(json_valid(body)),
    project_id TEXT NOT NULL,
    workspace_id TEXT,
    effective_from_ms INTEGER NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    supersedes TEXT
) WITHOUT ROWID;
CREATE INDEX placement_fact_project_time ON placement_fact(project_id,effective_from_ms,recorded_at_ms);
CREATE INDEX placement_fact_supersedes ON placement_fact(supersedes) WHERE supersedes IS NOT NULL;
CREATE TRIGGER placement_fact_no_update BEFORE UPDATE ON placement_fact
BEGIN SELECT RAISE(ABORT, 'placement_fact_no_update'); END;
CREATE TRIGGER placement_fact_no_delete BEFORE DELETE ON placement_fact
BEGIN SELECT RAISE(ABORT, 'placement_fact_no_delete'); END;
CREATE TRIGGER placement_fact_insert_once BEFORE INSERT ON placement_fact
WHEN EXISTS (SELECT 1 FROM placement_fact WHERE fact_id=NEW.fact_id)
BEGIN SELECT RAISE(ABORT, 'placement_fact_insert_once'); END;

CREATE TABLE attribution_cursor (
    publisher TEXT PRIMARY KEY,
    read_name TEXT NOT NULL,
    cursor TEXT,
    through TEXT,
    last_read_at_ms INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE session_alias (
    runner TEXT NOT NULL,
    harness TEXT NOT NULL,
    host_session TEXT NOT NULL,
    bound_session TEXT NOT NULL,
    PRIMARY KEY(runner,harness,host_session,bound_session)
) WITHOUT ROWID;
CREATE INDEX session_alias_host ON session_alias(host_session);
CREATE INDEX session_alias_bound ON session_alias(bound_session);
CREATE TRIGGER session_alias_no_update BEFORE UPDATE ON session_alias
BEGIN SELECT RAISE(ABORT, 'session_alias_no_update'); END;
CREATE TRIGGER session_alias_no_delete BEFORE DELETE ON session_alias
BEGIN SELECT RAISE(ABORT, 'session_alias_no_delete'); END;
CREATE TRIGGER session_alias_insert_once BEFORE INSERT ON session_alias
WHEN EXISTS (SELECT 1 FROM session_alias WHERE runner=NEW.runner AND harness=NEW.harness
    AND host_session=NEW.host_session AND bound_session=NEW.bound_session)
BEGIN SELECT RAISE(ABORT, 'session_alias_insert_once'); END;

-- Independent completion marks let attribution catch up without invalidating
-- current ordinary reports. Attribution evidence has its own revision as well.
CREATE TABLE attribution_hour (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    summary_revision INTEGER,
    summary_attribution_revision INTEGER,
    valuation_revision INTEGER,
    valuation_attribution_revision INTEGER,
    FOREIGN KEY(kind,hour_number) REFERENCES summary_hour(kind,hour_number),
    PRIMARY KEY(kind,hour_number)
) WITHOUT ROWID;
INSERT INTO attribution_hour(kind,hour_number) SELECT kind,hour_number FROM summary_hour;
CREATE INDEX attribution_summary_stale ON attribution_hour(kind,hour_number)
    WHERE summary_revision IS NULL OR summary_attribution_revision IS NOT revision;
CREATE INDEX attribution_valuation_stale ON attribution_hour(kind,hour_number)
    WHERE valuation_revision IS NULL OR valuation_attribution_revision IS NOT revision;
CREATE TRIGGER attribution_summary_hour_insert AFTER INSERT ON summary_hour BEGIN
    INSERT OR IGNORE INTO attribution_hour(kind,hour_number) VALUES(NEW.kind,NEW.hour_number);
END;
CREATE TRIGGER attribution_summary_hour_revision AFTER UPDATE OF revision ON summary_hour BEGIN
    UPDATE attribution_hour SET summary_revision=NULL WHERE kind=NEW.kind AND hour_number=NEW.hour_number;
END;
CREATE TRIGGER attribution_valuation_hour_revision AFTER UPDATE OF revision ON valuation_hour BEGIN
    UPDATE attribution_hour SET valuation_revision=NULL WHERE kind=NEW.kind AND hour_number=NEW.hour_number;
END;
-- Rowid pages hold more of each JSON payload locally, reducing overflow at
-- ordinary summary payload sizes. The composite key remains unique.
CREATE TABLE attribution_cell (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    currency INTEGER NOT NULL REFERENCES summary_value(value_id),
    attribution INTEGER NOT NULL REFERENCES summary_value(value_id),
    provider INTEGER NOT NULL REFERENCES summary_value(value_id),
    model INTEGER NOT NULL REFERENCES summary_value(value_id),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    PRIMARY KEY(kind,hour_number,currency,attribution,provider,model),
    FOREIGN KEY(kind,hour_number) REFERENCES attribution_hour(kind,hour_number)
);
-- Keep long valuation payloads in rowid table pages rather than the smaller
-- local records of a WITHOUT ROWID index. The logical key stays unique.
CREATE TABLE attribution_valuation_cell (
    kind INTEGER NOT NULL,
    hour_number INTEGER NOT NULL,
    currency INTEGER NOT NULL REFERENCES summary_value(value_id),
    attribution INTEGER NOT NULL REFERENCES summary_value(value_id),
    provider INTEGER NOT NULL REFERENCES summary_value(value_id),
    model INTEGER NOT NULL REFERENCES summary_value(value_id),
    plan_key TEXT NOT NULL CHECK(json_valid(plan_key)),
    plan TEXT NOT NULL,
    plan_account TEXT,
    plan_record TEXT CHECK(plan_record IS NULL OR json_valid(plan_record)),
    recorded TEXT NOT NULL CHECK(json_valid(recorded)),
    list_numerator TEXT NOT NULL,
    unvalued TEXT NOT NULL CHECK(json_valid(unvalued)),
    PRIMARY KEY(kind,hour_number,currency,attribution,provider,model,plan_key),
    FOREIGN KEY(kind,hour_number) REFERENCES attribution_hour(kind,hour_number)
);
"#;

// SQLite can't compute a session_ref, so this migration's rows are filled in
// Rust by `migrate_store`, in the same transaction that runs this SQL.
pub(in crate::store) const V59: &str = r#"-- Each session fact filed under the session_ref computed from its subject, so
-- attribution can find a fact by that name through an index.
CREATE TABLE attribution_session_ref (
    runner TEXT NOT NULL,
    session_ref TEXT NOT NULL,
    fact_id TEXT NOT NULL,
    PRIMARY KEY(runner,session_ref,fact_id)
) WITHOUT ROWID;
CREATE TRIGGER attribution_session_ref_no_update BEFORE UPDATE ON attribution_session_ref
BEGIN SELECT RAISE(ABORT, 'attribution_session_ref_no_update'); END;
CREATE TRIGGER attribution_session_ref_no_delete BEFORE DELETE ON attribution_session_ref
BEGIN SELECT RAISE(ABORT, 'attribution_session_ref_no_delete'); END;
CREATE TRIGGER attribution_session_ref_insert_once BEFORE INSERT ON attribution_session_ref
WHEN EXISTS (SELECT 1 FROM attribution_session_ref WHERE runner=NEW.runner
    AND session_ref=NEW.session_ref AND fact_id=NEW.fact_id)
BEGIN SELECT RAISE(ABORT, 'attribution_session_ref_insert_once'); END;
"#;
