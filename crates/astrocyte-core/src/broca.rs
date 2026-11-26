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
            tx.execute(
                "INSERT INTO rejected_delivery
                     (producer,producer_lineage,producer_ref,incoming_payload,stored_event_key,detected_at_ms)
                 SELECT producer,producer_lineage,producer_ref,?2,event_key,?3
                   FROM usage_event WHERE event_key=?1",
                params![event_key, payload.to_string(), recorded_at_ms],
            )?;
        }
        return Ok(AttemptRecordOutcome {
            disposition: if same {
                RecordDisposition::ExactRedelivery
            } else {
                RecordDisposition::QuarantinedRedelivery
            },
            skipped_shares: Vec::new(),
        });
    }
    drop(stored);

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
