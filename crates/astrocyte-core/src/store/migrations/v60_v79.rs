pub(in crate::store) const V60: &str = r#"
CREATE TABLE broca_segment_evidence (
    fact_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL,
    usage_json TEXT NOT NULL CHECK (json_valid(usage_json) AND json_type(usage_json)='object'),
    recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0)
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
    usage_json TEXT CHECK (json_valid(usage_json) AND json_type(usage_json)='object'),
    fact_json TEXT CHECK (json_valid(fact_json) AND json_type(fact_json)='object'),
    CHECK ((segment_fact_id IS NULL) = (usage_json IS NULL)),
    UNIQUE (fact_id,share_index)
) STRICT;
CREATE INDEX broca_attempt_by_segment ON broca_attempt_detail(segment_fact_id)
    WHERE segment_fact_id IS NOT NULL;
CREATE UNIQUE INDEX broca_attempt_body_once ON broca_attempt_detail(fact_id)
    WHERE fact_json IS NOT NULL;

CREATE TRIGGER broca_segment_evidence_no_update BEFORE UPDATE ON broca_segment_evidence
BEGIN SELECT RAISE(ABORT,'broca_segment_evidence_no_update'); END;
CREATE TRIGGER broca_segment_evidence_no_delete BEFORE DELETE ON broca_segment_evidence
BEGIN SELECT RAISE(ABORT,'broca_segment_evidence_no_delete'); END;
CREATE TRIGGER broca_segment_evidence_insert_once BEFORE INSERT ON broca_segment_evidence
WHEN EXISTS (SELECT 1 FROM broca_segment_evidence WHERE fact_id=NEW.fact_id)
BEGIN SELECT RAISE(ABORT,'broca_segment_evidence_insert_once'); END;

CREATE TRIGGER broca_attempt_detail_no_update BEFORE UPDATE ON broca_attempt_detail
BEGIN SELECT RAISE(ABORT,'broca_attempt_detail_no_update'); END;
CREATE TRIGGER broca_attempt_detail_no_delete BEFORE DELETE ON broca_attempt_detail
BEGIN SELECT RAISE(ABORT,'broca_attempt_detail_no_delete'); END;
CREATE TRIGGER broca_attempt_detail_insert_once BEFORE INSERT ON broca_attempt_detail
WHEN EXISTS (SELECT 1 FROM broca_attempt_detail WHERE event_key=NEW.event_key)
  OR EXISTS (SELECT 1 FROM broca_attempt_detail WHERE fact_id=NEW.fact_id AND share_index=NEW.share_index)
  OR (NEW.fact_json IS NOT NULL AND EXISTS (
      SELECT 1 FROM broca_attempt_detail WHERE fact_id=NEW.fact_id AND fact_json IS NOT NULL))
BEGIN SELECT RAISE(ABORT,'broca_attempt_detail_insert_once'); END;
"#;
