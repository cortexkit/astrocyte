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
