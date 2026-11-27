pub(in crate::store) const V60: &str = r#"
CREATE TABLE broca_segment_evidence (
    segment_key INTEGER PRIMARY KEY,
    fact_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0)
) STRICT;

CREATE TABLE broca_segment_measurement (
    segment_key INTEGER NOT NULL REFERENCES broca_segment_evidence(segment_key) ON DELETE RESTRICT,
    role TEXT NOT NULL,
    unit TEXT NOT NULL,
    quantity_units INTEGER CHECK (quantity_units >= 0),
    PRIMARY KEY (segment_key,role,unit)
) STRICT, WITHOUT ROWID;

CREATE TABLE broca_attempt_detail (
    event_key INTEGER PRIMARY KEY REFERENCES usage_event(event_key) ON DELETE RESTRICT,
    fact_id TEXT NOT NULL,
    share_index INTEGER NOT NULL CHECK (share_index >= 0),
    attempt_id TEXT NOT NULL,
    step_id INTEGER NOT NULL CHECK (step_id >= 0),
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    outcome TEXT NOT NULL,
    http_status INTEGER,
    content_received INTEGER NOT NULL CHECK (content_received IN (0,1)),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
    requested_service_tier TEXT NOT NULL,
    served_service_tier TEXT,
    served_speed TEXT,
    served_account_ref TEXT,
    mode TEXT,
    segment_fact_id TEXT,
    UNIQUE (fact_id,share_index)
) STRICT;
CREATE INDEX broca_attempt_by_segment ON broca_attempt_detail(segment_fact_id)
    WHERE segment_fact_id IS NOT NULL;

CREATE TABLE broca_segment_check_state (
    id INTEGER PRIMARY KEY CHECK (id=1),
    checked_through INTEGER NOT NULL CHECK (checked_through >= 0)
) STRICT;
INSERT INTO broca_segment_check_state VALUES (1,0);

CREATE TABLE broca_segment_mismatch (
    fact_id TEXT NOT NULL REFERENCES broca_segment_evidence(fact_id) ON DELETE RESTRICT,
    role TEXT NOT NULL,
    unit TEXT NOT NULL,
    segment_quantity INTEGER CHECK (segment_quantity >= 0),
    attempts_quantity INTEGER CHECK (attempts_quantity >= 0),
    segment_reported INTEGER NOT NULL CHECK (segment_reported IN (0,1)),
    attempts_reported INTEGER NOT NULL CHECK (attempts_reported IN (0,1)),
    over_range INTEGER NOT NULL DEFAULT 0 CHECK (over_range IN (0,1)),
    checked_at_ms INTEGER NOT NULL CHECK (checked_at_ms >= 0),
    CHECK (over_range=0 OR (attempts_reported=1 AND attempts_quantity IS NULL)),
    PRIMARY KEY (fact_id,role,unit)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER broca_segment_evidence_no_update BEFORE UPDATE ON broca_segment_evidence
BEGIN SELECT RAISE(ABORT,'broca_segment_evidence_no_update'); END;
CREATE TRIGGER broca_segment_evidence_no_delete BEFORE DELETE ON broca_segment_evidence
BEGIN SELECT RAISE(ABORT,'broca_segment_evidence_no_delete'); END;
CREATE TRIGGER broca_segment_evidence_insert_once BEFORE INSERT ON broca_segment_evidence
WHEN EXISTS (SELECT 1 FROM broca_segment_evidence WHERE fact_id=NEW.fact_id)
  OR EXISTS (SELECT 1 FROM broca_segment_evidence WHERE segment_key=NEW.segment_key)
BEGIN SELECT RAISE(ABORT,'broca_segment_evidence_insert_once'); END;

CREATE TRIGGER broca_segment_measurement_no_update BEFORE UPDATE ON broca_segment_measurement
BEGIN SELECT RAISE(ABORT,'broca_segment_measurement_no_update'); END;
CREATE TRIGGER broca_segment_measurement_no_delete BEFORE DELETE ON broca_segment_measurement
BEGIN SELECT RAISE(ABORT,'broca_segment_measurement_no_delete'); END;
CREATE TRIGGER broca_segment_measurement_insert_once BEFORE INSERT ON broca_segment_measurement
WHEN EXISTS (SELECT 1 FROM broca_segment_measurement
             WHERE segment_key=NEW.segment_key AND role=NEW.role AND unit=NEW.unit)
BEGIN SELECT RAISE(ABORT,'broca_segment_measurement_insert_once'); END;

CREATE TRIGGER broca_attempt_detail_no_update BEFORE UPDATE ON broca_attempt_detail
BEGIN SELECT RAISE(ABORT,'broca_attempt_detail_no_update'); END;
CREATE TRIGGER broca_attempt_detail_no_delete BEFORE DELETE ON broca_attempt_detail
BEGIN SELECT RAISE(ABORT,'broca_attempt_detail_no_delete'); END;
CREATE TRIGGER broca_attempt_detail_insert_once BEFORE INSERT ON broca_attempt_detail
WHEN EXISTS (SELECT 1 FROM broca_attempt_detail WHERE event_key=NEW.event_key)
  OR EXISTS (SELECT 1 FROM broca_attempt_detail WHERE fact_id=NEW.fact_id AND share_index=NEW.share_index)
BEGIN SELECT RAISE(ABORT,'broca_attempt_detail_insert_once'); END;

CREATE TRIGGER broca_segment_mismatch_no_update BEFORE UPDATE ON broca_segment_mismatch
BEGIN SELECT RAISE(ABORT,'broca_segment_mismatch_no_update'); END;
CREATE TRIGGER broca_segment_mismatch_no_delete BEFORE DELETE ON broca_segment_mismatch
BEGIN SELECT RAISE(ABORT,'broca_segment_mismatch_no_delete'); END;
CREATE TRIGGER broca_segment_mismatch_insert_once BEFORE INSERT ON broca_segment_mismatch
WHEN EXISTS (SELECT 1 FROM broca_segment_mismatch
             WHERE fact_id=NEW.fact_id AND role=NEW.role AND unit=NEW.unit)
BEGIN SELECT RAISE(ABORT,'broca_segment_mismatch_insert_once'); END;
"#;

pub(in crate::store) const V61: &str = r#"-- Provider and model must outlive the one-day feed copy for rewrite causes.
ALTER TABLE step_evidence ADD COLUMN provider TEXT;
ALTER TABLE step_evidence ADD COLUMN model TEXT;

UPDATE step_evidence SET
    provider=json_extract(f.row_json,'$.provider'),
    model=json_extract(f.row_json,'$.model')
FROM step_feed f WHERE f.feed_cursor=step_evidence.latest_feed_cursor;

CREATE INDEX step_evidence_retained_neighbours
    ON step_evidence(harness,session,runner,order_ms,first_feed_cursor)
    WHERE latest_feed_cursor IS NULL;

DROP TRIGGER step_feed_index_evidence;
CREATE TRIGGER step_feed_index_evidence AFTER INSERT ON step_feed
BEGIN
    INSERT INTO step_evidence(harness,step_ref,recorded_at_ms,facts_json,first_feed_cursor,
        session,order_ms,feed_source,occurred_at_ms,reports_cache,positive_read,latest_feed_cursor,
        provider,model)
    VALUES (NEW.harness,NEW.step_ref,NEW.committed_at_ms,'{}',NEW.feed_cursor,
        NEW.session,json_extract(NEW.row_json,'$.occurred_at_ms'),NEW.source,
        json_extract(NEW.row_json,'$.occurred_at_ms'),
        COALESCE(json_type(NEW.row_json,'$.cache_read')='integer',0),
        COALESCE(json_extract(NEW.row_json,'$.cache_read')>0,0),NEW.feed_cursor,
        json_extract(NEW.row_json,'$.provider'),json_extract(NEW.row_json,'$.model'))
    ON CONFLICT(harness,step_ref) DO UPDATE SET
        first_feed_cursor=COALESCE(first_feed_cursor,NEW.feed_cursor),
        session=NEW.session,feed_source=NEW.source,latest_feed_cursor=NEW.feed_cursor,
        occurred_at_ms=excluded.occurred_at_ms,reports_cache=excluded.reports_cache,
        positive_read=excluded.positive_read,
        order_ms=COALESCE(json_extract(facts_json,'$.started_at_ms'),excluded.occurred_at_ms),
        provider=excluded.provider,model=excluded.model;
END;

DROP TRIGGER step_evidence_restore_live_placeholder;
CREATE TRIGGER step_evidence_restore_live_placeholder AFTER DELETE ON step_evidence
BEGIN
    INSERT INTO step_evidence(harness,step_ref,recorded_at_ms,facts_json,first_feed_cursor,
        session,order_ms,feed_source,occurred_at_ms,reports_cache,positive_read,latest_feed_cursor,
        provider,model)
    SELECT f.harness,f.step_ref,f.committed_at_ms,'{}',
        (SELECT MIN(feed_cursor) FROM step_feed WHERE harness=OLD.harness AND step_ref=OLD.step_ref),
        f.session,json_extract(f.row_json,'$.occurred_at_ms'),f.source,
        json_extract(f.row_json,'$.occurred_at_ms'),
        COALESCE(json_type(f.row_json,'$.cache_read')='integer',0),
        COALESCE(json_extract(f.row_json,'$.cache_read')>0,0),f.feed_cursor,
        json_extract(f.row_json,'$.provider'),json_extract(f.row_json,'$.model')
    FROM step_feed f WHERE f.feed_cursor=(SELECT MAX(feed_cursor) FROM step_feed
        WHERE harness=OLD.harness AND step_ref=OLD.step_ref);
END;
"#;

pub(in crate::store) const V62: &str = r#"
CREATE INDEX rate_model_version ON rate_model(catalog_version);

-- One unit list serves both cache-step quantities and their baseline projection.
CREATE VIEW cache_token_unit AS
SELECT 'token' AS unit UNION ALL SELECT 'token_cache_5m' UNION ALL SELECT 'token_cache_1h';

DROP TRIGGER cache_read_projection;
CREATE TRIGGER cache_read_projection AFTER INSERT ON measurement
WHEN NEW.role = 'cache_read' AND NEW.unit IN (SELECT unit FROM cache_token_unit)
     AND NEW.quantity_exponent = 0
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
    SET first_positive_cursor = (SELECT s.cursor FROM cache_step_sequence s WHERE s.event_key = NEW.event_key)
    WHERE NEW.quantity_units > 0
      AND session_id = (SELECT c.session_id FROM usage_event e
          CROSS JOIN cache_read_session c ON c.producer = e.producer AND c.session = e.session
          WHERE e.event_key = NEW.event_key AND e.occurred_at_ms IS NOT NULL)
      AND (first_positive_cursor IS NULL OR first_positive_cursor >
          (SELECT s.cursor FROM cache_step_sequence s WHERE s.event_key = NEW.event_key));
END;
"#;

pub(in crate::store) const V63: &str = r#"
-- An hour's revision invalidates its payload. The plan generation fences builds
-- in flight; it need not be copied to hours outside the changed interval.
DROP TRIGGER valuation_account_insert;
DROP TRIGGER valuation_credential_insert;
DROP TRIGGER valuation_account_close;
DROP TRIGGER valuation_credential_close;
CREATE TRIGGER valuation_account_insert AFTER INSERT ON account_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number BETWEEN NEW.effective_from_ms/3600000
        AND COALESCE((NEW.effective_to_ms-1)/3600000,9223372036854775807);
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
CREATE TRIGGER valuation_credential_insert AFTER INSERT ON credential_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number BETWEEN NEW.effective_from_ms/3600000
        AND COALESCE((NEW.effective_to_ms-1)/3600000,9223372036854775807);
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
CREATE TRIGGER valuation_account_close AFTER UPDATE OF effective_to_ms ON account_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number>=NEW.effective_to_ms/3600000;
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
CREATE TRIGGER valuation_credential_close AFTER UPDATE OF effective_to_ms ON credential_subscription BEGIN
    UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0
      AND hour_number>=NEW.effective_to_ms/3600000;
    UPDATE valuation_state SET plan_generation=plan_generation+1 WHERE id=1;
END;
-- Start with attempt details, not ledger events. Publication of revised
-- valuation rows also invalidates the matching attribution valuations.
UPDATE valuation_hour SET state='dirty',revision=revision+1
WHERE (kind,hour_number) IN (
    SELECT DISTINCT e.occurred_at_ms IS NULL,
        COALESCE(e.occurred_at_ms/3600000-(e.occurred_at_ms%3600000<0),0)
FROM broca_attempt_detail b CROSS JOIN usage_event e ON e.event_key=b.event_key
);

-- Only these legacy output measurements change the summary quantities. Bump
-- the summary revision to invalidate the same hour's valuation and attribution
-- payloads through their existing triggers. Leave other format-6 hours current.
CREATE TEMP TABLE legacy_output_hour(kind INTEGER,hour_number INTEGER,PRIMARY KEY(kind,hour_number)) WITHOUT ROWID;
INSERT INTO legacy_output_hour
SELECT DISTINCT e.occurred_at_ms IS NULL,
    COALESCE(e.occurred_at_ms/3600000-(e.occurred_at_ms%3600000<0),0)
FROM usage_event e INDEXED BY usage_event_multiplier_scope_time
CROSS JOIN measurement o ON o.event_key=e.event_key AND o.role='output' AND o.unit='token'
CROSS JOIN measurement r ON r.event_key=e.event_key AND r.role='reasoning' AND r.unit='token'
WHERE e.producer='broca' AND e.provider IS NOT 'xai'
    AND o.source='usage.output_tokens' AND o.quantity_exponent=0 AND o.quantity_units IS NOT NULL
    AND r.quantity_exponent=0 AND r.quantity_units>0;
INSERT INTO summary_hour(kind,hour_number,revision)
SELECT kind,hour_number,1 FROM legacy_output_hour WHERE 1
ON CONFLICT(kind,hour_number) DO UPDATE SET revision=revision+1;
INSERT OR IGNORE INTO summary_dirty_hour(kind,hour_number)
SELECT kind,hour_number FROM legacy_output_hour;
DROP TABLE legacy_output_hour;

-- Unit prices were not included in cached list value. Invalidate the two
-- valuation projections for matching hours without rebuilding recorded spend
-- or run summaries. Start with measurements and look up each event by key.
UPDATE valuation_hour SET state='dirty',revision=revision+1
WHERE state='built' AND (kind,hour_number) IN (
    SELECT DISTINCT 0,e.occurred_at_ms/3600000-(e.occurred_at_ms%3600000<0)
    FROM measurement m CROSS JOIN usage_event e ON e.event_key=m.event_key
    WHERE m.unit IN ('second','character','image','request','result')
        AND m.quantity_units>0 AND e.occurred_at_ms IS NOT NULL
);

-- Before failed refreshes retained the last good version, they could leave a
-- built hour whose model (or alias target) no longer resolved in a live read.
-- We cannot distinguish that NULL from an intentional reference reset, so
-- invalidate built hours for those models and let the next fetch resolve them.
WITH RECURSIVE unavailable(provider,model) AS (
    SELECT provider,model FROM rate_model WHERE catalog_version IS NULL
    UNION
    SELECT m.provider,m.model FROM unavailable u
    JOIN rate_model m ON m.reference_provider=u.provider AND m.reference_model=u.model
)
UPDATE valuation_hour SET state='dirty',revision=revision+1
WHERE state='built' AND kind=0 AND hour_number IN (
    SELECT DISTINCT c.hour_number FROM unavailable u
    JOIN summary_value p ON p.value=u.provider
    JOIN summary_value m ON m.value=u.model
    JOIN summary_cell c ON c.provider=p.value_id AND c.model=m.value_id
    WHERE c.kind=0
);

-- Use the same normalization for the reader, insert triggers and backfill.
-- SQLite's date functions do not accept the entire RFC 3339 offset range.
CREATE VIEW quota_reset_samples AS
WITH raw(credential,provider,identity,pool_key,fetched_at_instant,reset) AS (
    SELECT 0,provider,account_ref,pool_key,fetched_at_instant,resets_at FROM window_sample
    UNION ALL
    SELECT 1,provider,credential_ref,pool_key,fetched_at_instant,resets_at FROM credential_window_sample
), parts AS (
    SELECT *, CASE WHEN upper(substr(reset,-1))='Z' THEN 'Z' ELSE substr(reset,-6) END AS zone,
        substr(reset,20,length(reset)-19-CASE WHEN upper(substr(reset,-1))='Z' THEN 1 ELSE 6 END) AS fraction,
        substr(reset,18,2) AS second
    FROM raw
)
SELECT credential,provider,identity,pool_key,fetched_at_instant,
    CASE WHEN length(reset)>=20 AND substr(reset,1,19) GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][Tt ][0-9][0-9]:[0-9][0-9]:[0-9][0-9]'
          AND date(substr(reset,1,10),'+0 days')=substr(reset,1,10)
          AND substr(reset,12,2)<='23' AND substr(reset,15,2)<='59' AND second<='60'
          AND (zone='Z' OR (length(reset)>=25 AND zone GLOB '[+-][0-9][0-9]:[0-9][0-9]' AND substr(zone,2,2)<='23' AND substr(zone,5,2)<='59'))
          AND (fraction='' OR (substr(fraction,1,1)='.' AND length(fraction)>1 AND substr(fraction,2) NOT GLOB '*[^0-9]*'))
         THEN unixepoch(substr(reset,1,10)||'T'||substr(reset,12,6)||min(second,'59')||'Z')*1000
            + CASE WHEN second='60' THEN 1000 ELSE 0 END
            + CAST(substr(substr(fraction,2)||'000',1,3) AS INTEGER)
            - CASE WHEN zone='Z' THEN 0 ELSE
                (CASE WHEN substr(zone,1,1)='-' THEN -1 ELSE 1 END)
                * (CAST(substr(zone,2,2) AS INTEGER)*60 + CAST(substr(zone,5,2) AS INTEGER))*60000 END
    END AS reset_ms
FROM parts;

-- Rebuildable projection: delayed readings can lower the earliest fetched
-- time of a known period. No other update or replacement is allowed.
CREATE TABLE quota_reset_period (
    credential INTEGER NOT NULL CHECK(credential IN (0,1)),
    provider TEXT NOT NULL,
    identity TEXT NOT NULL,
    pool_key TEXT NOT NULL,
    reset_ms INTEGER NOT NULL,
    first_fetched INTEGER NOT NULL,
    PRIMARY KEY(credential,provider,identity,pool_key,reset_ms)
) STRICT, WITHOUT ROWID;

INSERT INTO quota_reset_period
SELECT credential,provider,identity,pool_key,reset_ms,min(fetched_at_instant)
FROM quota_reset_samples WHERE reset_ms IS NOT NULL
GROUP BY credential,provider,identity,pool_key,reset_ms;

CREATE TRIGGER quota_reset_period_no_delete BEFORE DELETE ON quota_reset_period
BEGIN SELECT RAISE(ABORT,'quota_reset_period_no_delete'); END;
CREATE TRIGGER quota_reset_period_guard_update BEFORE UPDATE ON quota_reset_period
WHEN NEW.credential IS NOT OLD.credential OR NEW.provider IS NOT OLD.provider
    OR NEW.identity IS NOT OLD.identity OR NEW.pool_key IS NOT OLD.pool_key
    OR NEW.reset_ms IS NOT OLD.reset_ms OR NEW.first_fetched>OLD.first_fetched
BEGIN SELECT RAISE(ABORT,'quota_reset_period_guard_update'); END;

-- Insert through a view: update or insert explicitly, so a direct duplicate
-- insert (including INSERT OR REPLACE) cannot evade the guard.
CREATE TRIGGER quota_reset_period_insert_once BEFORE INSERT ON quota_reset_period
WHEN EXISTS (SELECT 1 FROM quota_reset_period WHERE credential=NEW.credential
    AND provider=NEW.provider AND identity=NEW.identity AND pool_key=NEW.pool_key AND reset_ms=NEW.reset_ms)
BEGIN SELECT RAISE(ABORT,'quota_reset_period_insert_once'); END;

CREATE TRIGGER window_sample_reset_period AFTER INSERT ON window_sample BEGIN
    UPDATE quota_reset_period SET first_fetched=NEW.fetched_at_instant
    WHERE credential=0 AND provider=NEW.provider AND identity=NEW.account_ref AND pool_key=NEW.pool_key
      AND reset_ms=(SELECT reset_ms FROM quota_reset_samples WHERE credential=0 AND provider=NEW.provider
        AND identity=NEW.account_ref AND pool_key=NEW.pool_key AND fetched_at_instant=NEW.fetched_at_instant)
      AND first_fetched>NEW.fetched_at_instant;
    INSERT INTO quota_reset_period
    SELECT s.credential,s.provider,s.identity,s.pool_key,s.reset_ms,s.fetched_at_instant
    FROM quota_reset_samples s WHERE s.credential=0 AND s.provider=NEW.provider AND s.identity=NEW.account_ref
      AND s.pool_key=NEW.pool_key AND s.fetched_at_instant=NEW.fetched_at_instant AND s.reset_ms IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM quota_reset_period p WHERE p.credential=s.credential AND p.provider=s.provider
        AND p.identity=s.identity AND p.pool_key=s.pool_key AND p.reset_ms=s.reset_ms);
END;
CREATE TRIGGER credential_window_sample_reset_period AFTER INSERT ON credential_window_sample BEGIN
    UPDATE quota_reset_period SET first_fetched=NEW.fetched_at_instant
    WHERE credential=1 AND provider=NEW.provider AND identity=NEW.credential_ref AND pool_key=NEW.pool_key
      AND reset_ms=(SELECT reset_ms FROM quota_reset_samples WHERE credential=1 AND provider=NEW.provider
        AND identity=NEW.credential_ref AND pool_key=NEW.pool_key AND fetched_at_instant=NEW.fetched_at_instant)
      AND first_fetched>NEW.fetched_at_instant;
    INSERT INTO quota_reset_period
    SELECT s.credential,s.provider,s.identity,s.pool_key,s.reset_ms,s.fetched_at_instant
    FROM quota_reset_samples s WHERE s.credential=1 AND s.provider=NEW.provider AND s.identity=NEW.credential_ref
      AND s.pool_key=NEW.pool_key AND s.fetched_at_instant=NEW.fetched_at_instant AND s.reset_ms IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM quota_reset_period p WHERE p.credential=s.credential AND p.provider=s.provider
        AND p.identity=s.identity AND p.pool_key=s.pool_key AND p.reset_ms=s.reset_ms);
END;
"#;

pub(in crate::store) const V64: &str = r#"
-- These isolated declines repeat the call's basis and carry no money. A row
-- that names or is named by another charge is evidence of a pricing change.
DROP TRIGGER priced_charge_no_delete;

DELETE FROM priced_charge AS charge
WHERE charge.outcome = 'unpriced'
  AND charge.unpriced_reason = 'charge_basis_unknown'
  AND charge.supersedes IS NULL
  AND EXISTS (
      SELECT 1 FROM usage_event AS event
      WHERE event.event_key = charge.event_key AND event.charge_basis = 'unknown'
  )
  AND NOT EXISTS (
      SELECT 1 FROM priced_charge AS child WHERE child.supersedes = charge.charge_key
  );

CREATE TRIGGER priced_charge_no_delete
BEFORE DELETE ON priced_charge
BEGIN
    SELECT RAISE(ABORT, 'priced_charge_no_delete');
END;

DELETE FROM pricing_pending
WHERE EXISTS (
    SELECT 1 FROM usage_event AS event
    WHERE event.event_key = pricing_pending.event_key AND event.charge_basis = 'unknown'
);
"#;

pub(in crate::store) const V65: &str = r#"
ALTER TABLE source_runtime ADD COLUMN file_identity TEXT;
ALTER TABLE declared_store_cursor ADD COLUMN file_identity TEXT;
"#;

// Rust computes the parent references in the same transaction as this SQL.
pub(in crate::store) const V66: &str = r#"
CREATE TABLE attribution_parent_session_ref (
    parent_runner TEXT NOT NULL,
    session_ref TEXT NOT NULL,
    fact_id TEXT NOT NULL,
    PRIMARY KEY(parent_runner,session_ref,fact_id)
) WITHOUT ROWID;
CREATE TRIGGER attribution_parent_session_ref_no_update BEFORE UPDATE ON attribution_parent_session_ref
BEGIN SELECT RAISE(ABORT, 'attribution_parent_session_ref_no_update'); END;
CREATE TRIGGER attribution_parent_session_ref_no_delete BEFORE DELETE ON attribution_parent_session_ref
BEGIN SELECT RAISE(ABORT, 'attribution_parent_session_ref_no_delete'); END;
CREATE TRIGGER attribution_parent_session_ref_insert_once BEFORE INSERT ON attribution_parent_session_ref
WHEN EXISTS (SELECT 1 FROM attribution_parent_session_ref WHERE parent_runner=NEW.parent_runner
    AND session_ref=NEW.session_ref AND fact_id=NEW.fact_id)
BEGIN SELECT RAISE(ABORT, 'attribution_parent_session_ref_insert_once'); END;
"#;

pub(in crate::store) const V67: &str = r#"
CREATE TABLE broca_scope_stamp (
    event_key INTEGER PRIMARY KEY REFERENCES usage_event(event_key),
    agent_id TEXT,
    agent_run_id TEXT,
    flow_id TEXT,
    call_kind TEXT NOT NULL CHECK(call_kind IN ('segment','model_attempt','system_one')),
    CHECK(agent_id IS NOT NULL OR agent_run_id IS NOT NULL OR flow_id IS NOT NULL)
);
CREATE INDEX broca_scope_stamp_run ON broca_scope_stamp(agent_run_id) WHERE agent_run_id IS NOT NULL;
CREATE INDEX broca_scope_stamp_flow ON broca_scope_stamp(flow_id) WHERE flow_id IS NOT NULL;
CREATE TRIGGER broca_scope_stamp_no_update BEFORE UPDATE ON broca_scope_stamp
    BEGIN SELECT RAISE(ABORT,'broca_scope_stamp is append-only'); END;
CREATE TRIGGER broca_scope_stamp_no_delete BEFORE DELETE ON broca_scope_stamp
    BEGIN SELECT RAISE(ABORT,'broca_scope_stamp is append-only'); END;
CREATE TRIGGER broca_scope_stamp_insert_once BEFORE INSERT ON broca_scope_stamp
    WHEN EXISTS (SELECT 1 FROM broca_scope_stamp WHERE event_key=NEW.event_key)
    BEGIN SELECT RAISE(ABORT,'broca_scope_stamp is append-only'); END;
CREATE TRIGGER broca_scope_stamp_dirty_hour AFTER INSERT ON broca_scope_stamp BEGIN
    UPDATE attribution_hour SET revision=revision+1 WHERE (kind,hour_number) IN (
        SELECT CASE WHEN occurred_at_ms IS NULL THEN 1 ELSE 0 END,
               coalesce(occurred_at_ms/3600000-(occurred_at_ms<0 AND occurred_at_ms%3600000<>0),0)
        FROM usage_event WHERE event_key=NEW.event_key
    );
END;

CREATE TABLE broca_scope_backfill (
    id INTEGER PRIMARY KEY CHECK(id=1),
    lineage TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal>=0),
    done INTEGER NOT NULL CHECK(done IN (0,1))
);
"#;

pub(in crate::store) const V68: &str = r#"
ALTER TABLE window_sample ADD COLUMN model_scope TEXT CHECK (model_scope IN ('all','subset'));
ALTER TABLE credential_window_sample ADD COLUMN model_scope TEXT CHECK (model_scope IN ('all','subset'));

ALTER TABLE multiplier_observation ADD COLUMN pool_scope TEXT
    CHECK (pool_scope IS NULL OR (pool_scope IN ('all','subset','unknown') AND plan IS NOT NULL));
ALTER TABLE multiplier_observation ADD COLUMN catalog_provider TEXT
    CHECK (catalog_provider IS NULL OR (pool_scope IS NOT NULL AND catalog_provider <> ''));
ALTER TABLE multiplier_observation ADD COLUMN pool_models TEXT
    CHECK (pool_models IS NULL OR (json_valid(pool_models) AND json_type(pool_models)='array'));
ALTER TABLE multiplier_observation ADD COLUMN pool_models_at_ms INTEGER;
ALTER TABLE multiplier_observation ADD COLUMN pool_member_models TEXT
    CHECK (pool_member_models IS NULL OR (pool_models IS NOT NULL
        AND json_valid(pool_member_models) AND json_type(pool_member_models)='array'));

CREATE TABLE multiplier_provider_day (
    quota_provider TEXT NOT NULL,
    observation_day TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    PRIMARY KEY(quota_provider, observation_day)
) WITHOUT ROWID;
CREATE TRIGGER multiplier_provider_day_no_update BEFORE UPDATE ON multiplier_provider_day
BEGIN SELECT RAISE(ABORT, 'multiplier_provider_day_no_update'); END;
CREATE TRIGGER multiplier_provider_day_no_delete BEFORE DELETE ON multiplier_provider_day
BEGIN SELECT RAISE(ABORT, 'multiplier_provider_day_no_delete'); END;
CREATE TRIGGER multiplier_provider_day_insert_once BEFORE INSERT ON multiplier_provider_day
WHEN EXISTS (SELECT 1 FROM multiplier_provider_day
    WHERE quota_provider=NEW.quota_provider AND observation_day=NEW.observation_day)
BEGIN SELECT RAISE(ABORT, 'multiplier_provider_day_insert_once'); END;

DROP INDEX multiplier_observation_one_original_per_day;
CREATE UNIQUE INDEX multiplier_observation_one_original_pre_cut
ON multiplier_observation(quota_provider, observation_day, COALESCE(plan,''))
WHERE supersedes IS NULL AND pool_scope IS NULL;
CREATE UNIQUE INDEX multiplier_observation_one_original_post_cut
ON multiplier_observation(quota_provider, observation_day, plan, window_pool_key)
WHERE supersedes IS NULL AND pool_scope IS NOT NULL;

-- REPLACE must not delete an existing observation, even with recursive
-- triggers disabled. The two lineages share the same id and parent space.
DROP TRIGGER multiplier_observation_insert_once;
CREATE TRIGGER multiplier_observation_pre_cut_insert_once BEFORE INSERT ON multiplier_observation
WHEN NEW.pool_scope IS NULL AND (
     EXISTS (SELECT 1 FROM multiplier_observation WHERE observation_id=NEW.observation_id)
  OR EXISTS (SELECT 1 FROM multiplier_observation WHERE supersedes=NEW.supersedes)
  OR (NEW.supersedes IS NULL AND EXISTS (SELECT 1 FROM multiplier_observation
      WHERE pool_scope IS NULL AND supersedes IS NULL
        AND quota_provider=NEW.quota_provider AND observation_day=NEW.observation_day
        AND COALESCE(plan,'')=COALESCE(NEW.plan,''))))
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_insert_once'); END;
CREATE TRIGGER multiplier_observation_post_cut_insert_once BEFORE INSERT ON multiplier_observation
WHEN NEW.pool_scope IS NOT NULL AND (
     EXISTS (SELECT 1 FROM multiplier_observation WHERE observation_id=NEW.observation_id)
  OR EXISTS (SELECT 1 FROM multiplier_observation WHERE supersedes=NEW.supersedes)
  OR (NEW.supersedes IS NULL AND EXISTS (SELECT 1 FROM multiplier_observation
      WHERE pool_scope IS NOT NULL AND supersedes IS NULL
        AND quota_provider=NEW.quota_provider AND observation_day=NEW.observation_day
        AND plan=NEW.plan AND window_pool_key=NEW.window_pool_key)))
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_insert_once'); END;

-- A plan-wide observation had no recorded scope and its correction could
-- choose a different window. A pool observation must stay in its own chain.
DROP TRIGGER multiplier_observation_same_plan;
CREATE TRIGGER multiplier_observation_same_lineage BEFORE INSERT ON multiplier_observation
WHEN EXISTS (SELECT 1 FROM multiplier_observation parent WHERE parent.observation_id=NEW.supersedes
    AND (parent.plan IS NOT NEW.plan
        OR (parent.pool_scope IS NULL) <> (NEW.pool_scope IS NULL)
        OR (NEW.pool_scope IS NOT NULL AND parent.window_pool_key IS NOT NEW.window_pool_key)))
BEGIN SELECT RAISE(ABORT, 'multiplier_observation_same_lineage'); END;
"#;

pub(in crate::store) const V69: &str = r#"
CREATE TABLE media_caller (
    event_key INTEGER PRIMARY KEY REFERENCES usage_event(event_key),
    agent_id TEXT,
    device TEXT CHECK(device IS NULL OR (json_valid(device) AND json_type(device) = 'object')),
    CHECK(agent_id IS NOT NULL OR device IS NOT NULL)
);
CREATE TRIGGER media_caller_no_update BEFORE UPDATE ON media_caller
    BEGIN SELECT RAISE(ABORT,'media_caller is append-only'); END;
CREATE TRIGGER media_caller_no_delete BEFORE DELETE ON media_caller
    BEGIN SELECT RAISE(ABORT,'media_caller is append-only'); END;
CREATE TRIGGER media_caller_insert_once BEFORE INSERT ON media_caller
    WHEN EXISTS (SELECT 1 FROM media_caller WHERE event_key=NEW.event_key)
    BEGIN SELECT RAISE(ABORT,'media_caller is append-only'); END;
CREATE TRIGGER media_caller_dirty_hour AFTER INSERT ON media_caller BEGIN
    UPDATE attribution_hour SET revision=revision+1 WHERE (kind,hour_number) IN (
        SELECT CASE WHEN occurred_at_ms IS NULL THEN 1 ELSE 0 END,
               coalesce(occurred_at_ms/3600000-(occurred_at_ms<0 AND occurred_at_ms%3600000<>0),0)
        FROM usage_event WHERE event_key=NEW.event_key
    );
END;
"#;

pub(in crate::store) const V70: &str = r#"
-- The token-only insert trigger did not remove projections of older non-token reads.
DELETE FROM cache_read_step AS r
WHERE NOT EXISTS (
    SELECT 1 FROM measurement m
    WHERE m.event_key = r.event_key AND m.role = 'cache_read'
      AND m.unit IN (SELECT unit FROM cache_token_unit) AND m.quantity_exponent = 0
);
DELETE FROM cache_read_session AS c
WHERE NOT EXISTS (SELECT 1 FROM cache_read_step r WHERE r.session_id = c.session_id);

-- A step can remain in the projection because it reported zero tokens, even
-- though a non-token measurement was the reason it counted as positive.
UPDATE cache_read_session AS c
SET first_positive_cursor = (
    SELECT MIN(r.cursor) FROM cache_read_step r
    WHERE r.session_id = c.session_id AND EXISTS (
        SELECT 1 FROM measurement m
        WHERE m.event_key = r.event_key AND m.role = 'cache_read'
          AND m.unit IN (SELECT unit FROM cache_token_unit) AND m.quantity_exponent = 0
          AND m.quantity_units > 0
    )
)
WHERE c.first_positive_cursor IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM cache_step_sequence s
    CROSS JOIN measurement m ON m.event_key = s.event_key
    WHERE s.cursor = c.first_positive_cursor AND m.role = 'cache_read'
      AND m.unit IN (SELECT unit FROM cache_token_unit) AND m.quantity_exponent = 0
      AND m.quantity_units > 0
);

-- A SQLite CHECK accepts NULL as well as true. Guard the required fields
-- on insert without rebuilding or rewriting the immutable charge history.
CREATE TRIGGER priced_charge_required_outcome_fields BEFORE INSERT ON priced_charge
WHEN (NEW.outcome = 'priced' AND NEW.rate_exponent IS NULL)
  OR (NEW.outcome = 'covered' AND NEW.charge_nanos IS NULL)
BEGIN SELECT RAISE(ABORT, 'priced_charge_required_outcome_fields'); END;
"#;
