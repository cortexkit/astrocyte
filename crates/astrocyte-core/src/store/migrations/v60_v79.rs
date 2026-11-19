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
