//! BROCA's per-request calls and the segment evidence they replace for billing.

use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use serde::{Deserialize, Serialize};

use crate::ingest::{self, DeliveryPath, EventRecordRequest, IngestError, RecordDisposition};

mod scope;
pub use scope::{record_scope_in_tx, CallKind, ScopeStamp};

/// BROCA's raw usage counts, before any pricing interpretation. A provider
/// that does not report a quantity is different from one that reports zero.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    /// Parts of the total for cache lifetimes that price at different rates.
    pub cache_write_5m_tokens: Option<u64>,
    pub cache_write_1h_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct MarkedSegment {
    pub fact_id: String,
    pub run_id: String,
    pub measurements: Vec<ingest::MeasurementInput>,
}

#[derive(Debug, Clone)]
pub struct Attempt {
    pub fact_id: String,
    pub detail: AttemptDetail,
    pub calls: Vec<AttemptCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AttemptDetail {
    pub attempt_id: String,
    pub step_id: i64,
    pub attempt: i64,
    pub outcome: String,
    pub http_status: Option<u16>,
    pub content_received: bool,
    pub response_bytes: i64,
    pub recorded_at_ms: i64,
    pub requested_service_tier: String,
    pub served_speed: Option<String>,
    pub served_account_ref: Option<String>,
}

impl AttemptDetail {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            attempt_id: row.get(2)?,
            step_id: row.get(3)?,
            attempt: row.get(4)?,
            outcome: row.get(5)?,
            http_status: row.get(6)?,
            content_received: row.get(7)?,
            response_bytes: row.get(8)?,
            recorded_at_ms: row.get(9)?,
            requested_service_tier: row.get(10)?,
            served_speed: row.get(11)?,
            served_account_ref: row.get(12)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AttemptCall {
    pub event: EventRecordRequest,
    pub served_service_tier: Option<String>,
    pub mode: Option<String>,
    pub segment_fact_id: Option<String>,
}

pub struct AttemptRecordOutcome<'a> {
    pub disposition: RecordDisposition,
    pub skipped_shares: Vec<(usize, &'a str)>,
}

const SEGMENT_HAS_ATTEMPTS: &str =
    "SELECT EXISTS(SELECT 1 FROM broca_attempt_detail WHERE segment_fact_id=?1)";
const SEGMENT_IS_BILLED: &str = "SELECT EXISTS(SELECT 1 FROM usage_event WHERE event_id=?1)";

/// Check whether an attempt already references an unmarked segment before
/// recording that segment as a call.
pub fn segment_has_attempts(conn: &Connection, fact_id: &str) -> rusqlite::Result<bool> {
    conn.query_row(SEGMENT_HAS_ATTEMPTS, [fact_id], |row| row.get(0))
}

fn segment_already_billed(conn: &Connection, fact_id: &str) -> Result<bool, IngestError> {
    let legacy_event_id = crate::fact::event_id("broca", None, fact_id)?;
    Ok(conn.query_row(SEGMENT_IS_BILLED, [legacy_event_id], |row| row.get(0))?)
}

/// An already seen segment is safe to skip only if its evidence is unchanged.
pub fn record_segment_in_tx(
    tx: &Transaction<'_>,
    segment: &MarkedSegment,
    recorded_at_ms: i64,
) -> rusqlite::Result<bool> {
    let stored = tx
        .query_row(
            "SELECT segment_key,run_id FROM broca_segment_evidence WHERE fact_id=?1",
            [&segment.fact_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((key, run)) = stored {
        let mut actual = tx.prepare_cached(
            "SELECT role,unit,quantity_units FROM broca_segment_measurement
               WHERE segment_key=?1 ORDER BY role,unit",
        )?;
        let mut incoming = segment.measurements.iter().collect::<Vec<_>>();
        incoming.sort_by_key(|m| (&m.role, &m.unit));
        let mut actual = actual.query([key])?;
        for measurement in incoming {
            let Some(row) = actual.next()? else {
                return Ok(false);
            };
            if row.get::<_, String>(0)? != measurement.role
                || row.get::<_, String>(1)? != measurement.unit
                || row.get::<_, Option<i64>>(2)? != measurement.quantity_units
            {
                return Ok(false);
            }
        }
        return Ok(run == segment.run_id && actual.next()?.is_none());
    }
    let key = tx.query_row(
        "INSERT INTO broca_segment_evidence(fact_id,run_id,recorded_at_ms)
         VALUES(?1,?2,?3) RETURNING segment_key",
        params![segment.fact_id, segment.run_id, recorded_at_ms],
        |row| row.get::<_, i64>(0),
    )?;
    let mut insert = tx.prepare_cached(
        "INSERT INTO broca_segment_measurement(segment_key,role,unit,quantity_units)
         VALUES(?1,?2,?3,?4)",
    )?;
    for m in &segment.measurements {
        insert.execute(params![key, m.role, m.unit, m.quantity_units])?;
    }
    Ok(true)
}

fn rejected_attempt_payload(attempt: &Attempt, eligible: &[(usize, &AttemptCall)]) -> String {
    let mut payload = serde_json::json!(attempt.detail);
    payload["fact_id"] = serde_json::json!(attempt.fact_id);
    payload["shares"] = serde_json::json!(eligible
        .iter()
        .map(|&(index, call)| {
            serde_json::json!({
                "share_index": index,
                "event": ingest::incoming_payload_value(&call.event, None),
                "served_service_tier": call.served_service_tier,
                "mode": call.mode,
                "segment_fact_id": call.segment_fact_id,
            })
        })
        .collect::<Vec<_>>());
    payload.to_string()
}

fn attempt_redelivery<'a>(same: bool) -> AttemptRecordOutcome<'a> {
    AttemptRecordOutcome {
        disposition: if same {
            RecordDisposition::ExactRedelivery
        } else {
            RecordDisposition::QuarantinedRedelivery
        },
        skipped_shares: Vec::new(),
    }
}

/// Compare all eligible shares before inserting any of them. Changes confined
/// to skipped shares are immaterial; any change to a recorded call, its
/// detail, or the set of eligible positions quarantines the delivery.
pub fn record_attempt_in_tx<'a>(
    tx: &Transaction<'_>,
    attempt: &'a Attempt,
    recorded_at_ms: i64,
) -> Result<AttemptRecordOutcome<'a>, IngestError> {
    let mut skipped_shares = Vec::new();
    let mut eligible = Vec::new();
    for (index, call) in attempt.calls.iter().enumerate() {
        if let Some(segment) = &call.segment_fact_id {
            if segment_already_billed(tx, segment)? {
                skipped_shares.push((index, segment.as_str()));
                continue;
            }
        }
        eligible.push((index, call));
    }

    let mut lookup = tx.prepare_cached(
        "SELECT event_key,share_index,attempt_id,step_id,attempt,outcome,http_status,
                content_received,response_bytes,recorded_at_ms,requested_service_tier,
                served_speed,served_account_ref,served_service_tier,mode,segment_fact_id
           FROM broca_attempt_detail WHERE fact_id=?1 ORDER BY share_index",
    )?;
    let mut stored = lookup.query([&attempt.fact_id])?;
    if let Some(first) = stored.next()? {
        let event_key = first.get::<_, i64>(0)?;
        let matches = |row: &Row<'_>, index: usize, call: &AttemptCall| {
            Ok::<_, IngestError>(
                row.get::<_, usize>(1)? == index
                    && AttemptDetail::read(row)? == attempt.detail
                    && row.get::<_, Option<String>>(13)? == call.served_service_tier
                    && row.get::<_, Option<String>>(14)? == call.mode
                    && row.get::<_, Option<String>>(15)? == call.segment_fact_id
                    && ingest::strictly_restates_event(tx, &call.event)?,
            )
        };
        let mut same = true;
        let mut row = Some(first);
        for &(index, call) in &eligible {
            if let Some(current) = row {
                if matches(current, index, call)? {
                    row = stored.next()?;
                    continue;
                }
            }
            same = false;
            break;
        }
        same &= row.is_none();
        drop(stored);
        if !same {
            ingest::record_rejected_delivery_in_tx(
                tx,
                event_key,
                &rejected_attempt_payload(attempt, &eligible),
                recorded_at_ms,
            )?;
        }
        return Ok(attempt_redelivery(same));
    }
    drop(stored);

    // An attempt skipped in full still claims its fact ID. There is no ledger
    // event to identify it or to anchor the evidence when a share becomes eligible.
    let empty_recorded: bool = tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM broca_empty_attempt WHERE fact_id=?1)")?
        .query_row([&attempt.fact_id], |row| row.get(0))?;
    if empty_recorded {
        if !eligible.is_empty() {
            tx.execute(
                "INSERT INTO broca_empty_attempt_conflict(fact_id,incoming_payload,detected_at_ms)
                 SELECT ?1,?2,?3 WHERE NOT EXISTS (
                     SELECT 1 FROM broca_empty_attempt_conflict WHERE fact_id=?1 AND incoming_payload=?2
                 )",
                params![attempt.fact_id, rejected_attempt_payload(attempt, &eligible), recorded_at_ms],
            )?;
        }
        return Ok(attempt_redelivery(eligible.is_empty()));
    }
    if eligible.is_empty() {
        tx.execute(
            "INSERT INTO broca_empty_attempt(fact_id,recorded_at_ms) VALUES(?1,?2)",
            params![attempt.fact_id, recorded_at_ms],
        )?;
        return Ok(AttemptRecordOutcome {
            disposition: RecordDisposition::Recorded,
            skipped_shares,
        });
    }

    let detail = &attempt.detail;
    let mut insert = tx.prepare_cached(
        "INSERT INTO broca_attempt_detail (
             event_key,fact_id,share_index,attempt_id,step_id,attempt,outcome,http_status,
             content_received,response_bytes,recorded_at_ms,requested_service_tier,served_service_tier,
             served_speed,served_account_ref,mode,segment_fact_id)
         VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),
                ?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
    )?;
    for (index, call) in eligible {
        let outcome =
            ingest::record_event_in_tx(tx, &call.event, recorded_at_ms, DeliveryPath::Read)?;
        if outcome.disposition != RecordDisposition::Recorded {
            return Err(ingest::stored_value_error(
                0,
                "unowned BROCA attempt event",
                &outcome.event_id,
            )
            .into());
        }
        insert.execute(params![
            outcome.event_id,
            attempt.fact_id,
            index,
            detail.attempt_id,
            detail.step_id,
            detail.attempt,
            detail.outcome,
            detail.http_status,
            detail.content_received,
            detail.response_bytes,
            detail.recorded_at_ms,
            detail.requested_service_tier,
            call.served_service_tier,
            detail.served_speed,
            detail.served_account_ref,
            call.mode,
            call.segment_fact_id,
        ])?;
        if let Some(segment) = &call.segment_fact_id {
            segment_check::requeue_checked_segment(tx, segment)?;
        }
    }
    Ok(AttemptRecordOutcome {
        disposition: RecordDisposition::Recorded,
        skipped_shares,
    })
}

mod segment_check;
pub use segment_check::check_segments_in_tx;
pub(crate) use segment_check::segment_usage_mismatches;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{conn, measurement};

    fn attempt(segment: Option<&str>) -> Attempt {
        let event = serde_json::from_value(serde_json::json!({
            "producer": "broca", "producer_ref": "attempt-1",
            "occurred_at_ms": 1, "charge_basis": "metered_api",
            "measurements": [measurement("input", Some(42))],
        }))
        .unwrap();
        Attempt {
            fact_id: "attempt-1".into(),
            detail: AttemptDetail {
                attempt_id: "request-1".into(),
                step_id: 1,
                attempt: 0,
                outcome: "complete".into(),
                http_status: None,
                content_received: true,
                response_bytes: 42,
                recorded_at_ms: 1,
                requested_service_tier: "standard".into(),
                served_speed: None,
                served_account_ref: None,
            },
            calls: vec![AttemptCall {
                event,
                served_service_tier: None,
                mode: None,
                segment_fact_id: segment.map(str::to_owned),
            }],
        }
    }

    #[test]
    fn repeated_broca_attempt_contradiction_retains_only_distinct_evidence() {
        let mut conn = conn();
        let mut attempt = attempt(None);
        let tx = conn.transaction().unwrap();
        record_attempt_in_tx(&tx, &attempt, 1).unwrap();
        for (at, bytes) in [(2, 43), (3, 43), (4, 44)] {
            attempt.detail.response_bytes = bytes;
            assert_eq!(
                record_attempt_in_tx(&tx, &attempt, at).unwrap().disposition,
                RecordDisposition::QuarantinedRedelivery
            );
        }
        tx.commit().unwrap();
        let mut query = conn
            .prepare("SELECT detected_at_ms FROM rejected_delivery ORDER BY detected_at_ms")
            .unwrap();
        let timestamps = query
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(timestamps, [2, 4]);
    }

    #[test]
    fn broca_all_skipped_attempt_cannot_gain_an_eligible_share_on_replay() {
        let mut conn = conn();
        let mut attempt = attempt(Some("legacy"));
        let mut legacy = attempt.calls[0].event.clone();
        legacy.producer_ref = "legacy".into();
        ingest::record_event(&mut conn, &legacy, 1).unwrap();

        let tx = conn.transaction().unwrap();
        let initial = record_attempt_in_tx(&tx, &attempt, 2).unwrap();
        assert_eq!(initial.skipped_shares, [(0, "legacy")]);
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        assert_eq!(
            record_attempt_in_tx(&tx, &attempt, 3).unwrap().disposition,
            RecordDisposition::ExactRedelivery
        );
        attempt.calls[0].segment_fact_id = None;
        for at in [4, 5] {
            let replay = record_attempt_in_tx(&tx, &attempt, at).unwrap();
            assert_eq!(replay.disposition, RecordDisposition::QuarantinedRedelivery);
        }
        tx.commit().unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM usage_event", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        let conflicts = conn.query_row(
            "SELECT count(*),min(detected_at_ms) FROM broca_empty_attempt_conflict
             WHERE fact_id='attempt-1' AND json_extract(incoming_payload,'$.shares[0].share_index')=0",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        ).unwrap();
        assert_eq!(conflicts, (1, 4));
    }

    #[test]
    fn broca_late_attempt_reconciles_current_mismatch_without_erasing_evidence() {
        let mut conn = conn();
        let tx = conn.transaction().unwrap();
        for fact_id in ["matched-late", "still-different"] {
            let segment = MarkedSegment {
                fact_id: fact_id.into(),
                run_id: "run".into(),
                measurements: vec![measurement("input", Some(42))],
            };
            record_segment_in_tx(&tx, &segment, 1).unwrap();
        }
        assert_eq!(check_segments_in_tx(&tx, 3_600_002).unwrap(), 2);
        tx.commit().unwrap();
        assert_eq!(segment_usage_mismatches(&conn).unwrap(), 2);

        let tx = conn.transaction().unwrap();
        record_attempt_in_tx(&tx, &attempt(Some("matched-late")), 3_600_003).unwrap();
        let mut different = attempt(Some("still-different"));
        different.fact_id = "attempt-2".into();
        different.calls[0].event.producer_ref = "attempt-2".into();
        different.calls[0].event.measurements[0].quantity_units = Some(41);
        record_attempt_in_tx(&tx, &different, 3_600_003).unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        check_segments_in_tx(&tx, 3_600_004).unwrap();
        tx.commit().unwrap();
        assert_eq!(segment_usage_mismatches(&conn).unwrap(), 1);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM broca_segment_mismatch WHERE attempts_reported=0 AND checked_at_ms=3600002", [], |row| row.get::<_, i64>(0)).unwrap(),
            2
        );
    }

    #[test]
    fn broca_segment_billing_and_coverage_lookups_seek_their_indexes() {
        let conn = Connection::open_in_memory().unwrap();
        crate::store::initialize_connection(&conn).unwrap();
        for migration in crate::store::MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        for (sql, index) in [
            (SEGMENT_IS_BILLED, "sqlite_autoindex_usage_event_1"),
            (SEGMENT_HAS_ATTEMPTS, "broca_attempt_by_segment"),
        ] {
            let plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(["fact"], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert!(
                plan.iter()
                    .any(|line| line.starts_with("SEARCH ") && line.contains(index)),
                "{plan:?}"
            );
        }
    }
}
