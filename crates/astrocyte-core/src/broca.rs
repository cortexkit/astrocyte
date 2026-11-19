//! BROCA's per-request calls and the segment evidence they replace for billing.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Deserialize;

use crate::ingest::{self, DeliveryPath, EventRecordRequest, IngestError, RecordDisposition};
use crate::report::ReportError;

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

impl TokenUsage {
    fn counts(&self) -> [Option<u128>; 7] {
        [
            self.input_tokens,
            self.cached_input_tokens,
            self.cache_write_tokens,
            self.cache_write_5m_tokens,
            self.cache_write_1h_tokens,
            self.output_tokens,
            self.reasoning_tokens,
        ]
        .map(|count| count.map(u128::from))
    }
}

#[derive(Debug, Clone)]
pub struct MarkedSegment {
    pub fact_id: String,
    pub run_id: String,
    pub usage_json: String,
}

#[derive(Debug, Clone)]
pub struct Attempt {
    pub fact_id: String,
    pub fact_json: String,
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
    pub calls: Vec<AttemptCall>,
}

#[derive(Debug, Clone)]
pub struct AttemptCall {
    pub event: EventRecordRequest,
    pub served_service_tier: Option<String>,
    pub mode: Option<String>,
    pub segment_fact_id: Option<String>,
    pub usage_json: Option<String>,
}

pub struct AttemptRecordOutcome<'a> {
    pub disposition: RecordDisposition,
    pub skipped_shares: Vec<(usize, &'a str)>,
}

const SEGMENT_HAS_ATTEMPTS: &str =
    "SELECT EXISTS(SELECT 1 FROM broca_attempt_detail WHERE segment_fact_id=?1)";
const SEGMENT_IS_BILLED: &str = "SELECT EXISTS(SELECT 1 FROM usage_event WHERE event_id=?1)";
const SEGMENT_SHARE_USAGE: &str =
    "SELECT usage_json FROM broca_attempt_detail WHERE segment_fact_id=?1";

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
            "SELECT run_id,usage_json FROM broca_segment_evidence WHERE fact_id=?1",
            [&segment.fact_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((run, usage)) = stored {
        return Ok(run == segment.run_id && usage == segment.usage_json);
    }
    tx.execute(
        "INSERT INTO broca_segment_evidence(fact_id,run_id,usage_json,recorded_at_ms)
         VALUES(?1,?2,?3,?4)",
        params![
            segment.fact_id,
            segment.run_id,
            segment.usage_json,
            recorded_at_ms
        ],
    )?;
    Ok(true)
}

/// Check the source fact before writing any share. If its body changed (even
/// if only an outcome or the share count changed), quarantine it as a whole.
pub fn record_attempt_in_tx<'a>(
    tx: &Transaction<'_>,
    attempt: &'a Attempt,
    recorded_at_ms: i64,
) -> Result<AttemptRecordOutcome<'a>, IngestError> {
    let stored = tx
        .query_row(
            "SELECT event_key,fact_json FROM broca_attempt_detail
              WHERE fact_id=?1 AND fact_json IS NOT NULL",
            [&attempt.fact_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((event_key, fact_json)) = stored {
        if fact_json == attempt.fact_json {
            return Ok(AttemptRecordOutcome {
                disposition: RecordDisposition::ExactRedelivery,
                skipped_shares: Vec::new(),
            });
        }
        tx.execute(
            "INSERT INTO rejected_delivery
                 (producer,producer_lineage,producer_ref,incoming_payload,stored_event_key,detected_at_ms)
             SELECT producer,producer_lineage,producer_ref,?2,event_key,?3
               FROM usage_event WHERE event_key=?1",
            params![event_key, attempt.fact_json, recorded_at_ms],
        )?;
        return Ok(AttemptRecordOutcome {
            disposition: RecordDisposition::QuarantinedRedelivery,
            skipped_shares: Vec::new(),
        });
    }

    let mut skipped_shares = Vec::new();
    let mut anchored = false;
    for (index, call) in attempt.calls.iter().enumerate() {
        if let Some(segment) = &call.segment_fact_id {
            if segment_already_billed(tx, segment)? {
                skipped_shares.push((index, segment.as_str()));
                continue;
            }
        }
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
        // The first share may have been skipped; anchor the whole body to the
        // first recorded call without renumbering its source position.
        tx.execute(
            "INSERT INTO broca_attempt_detail (
                 event_key,fact_id,share_index,attempt_id,step_id,attempt,outcome,http_status,
                 content_received,response_bytes,recorded_at_ms,requested_service_tier,served_service_tier,
                 served_speed,served_account_ref,mode,segment_fact_id,usage_json,fact_json)
             VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),
                    ?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
            params![
                outcome.event_id,
                attempt.fact_id,
                index,
                attempt.attempt_id,
                attempt.step_id,
                attempt.attempt,
                attempt.outcome,
                attempt.http_status,
                attempt.content_received,
                attempt.response_bytes,
                attempt.recorded_at_ms,
                attempt.requested_service_tier,
                call.served_service_tier,
                attempt.served_speed,
                attempt.served_account_ref,
                call.mode,
                call.segment_fact_id,
                call.usage_json,
                (!anchored).then_some(&attempt.fact_json),
            ],
        )?;
        anchored = true;
    }
    Ok(AttemptRecordOutcome {
        disposition: RecordDisposition::Recorded,
        skipped_shares,
    })
}

/// Compare what BROCA reported, not normalized or priced measurements. In
/// particular an omitted quantity is distinct from any explicit zero.
pub(crate) fn segment_usage_mismatches(conn: &Connection) -> Result<u64, ReportError> {
    fn counts(usage_json: &str) -> Result<[Option<u128>; 7], ReportError> {
        serde_json::from_str::<TokenUsage>(usage_json)
            .map(|usage| usage.counts())
            .map_err(|_| ReportError::Integrity("invalid BROCA usage evidence"))
    }

    let mut segments = conn.prepare("SELECT fact_id,usage_json FROM broca_segment_evidence")?;
    let mut shares = conn.prepare(SEGMENT_SHARE_USAGE)?;
    let mut segments = segments.query([])?;
    let mut mismatches = 0;
    while let Some(segment) = segments.next()? {
        let segment_id: String = segment.get(0)?;
        let usage: String = segment.get(1)?;
        let mut sum = [None; 7];
        let mut shares = shares.query([segment_id])?;
        while let Some(share) = shares.next()? {
            let usage: String = share.get(0)?;
            for (total, quantity) in sum.iter_mut().zip(counts(&usage)?) {
                if let Some(quantity) = quantity {
                    *total = Some(total.unwrap_or(0) + quantity);
                }
            }
        }
        mismatches += u64::from(counts(&usage)? != sum);
    }
    Ok(mismatches)
}

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
            (SEGMENT_SHARE_USAGE, "broca_attempt_by_segment"),
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
