use std::collections::BTreeMap;

use rusqlite::{params, Connection, Error, ErrorCode, Transaction};

use crate::report::ReportError;

const GRACE_MS: i64 = 3_600_000;
const CHECK_BATCH: u32 = 128;

const NEXT_SEGMENTS: &str = "SELECT segment_key,fact_id,recorded_at_ms
    FROM broca_segment_evidence WHERE segment_key>?1 ORDER BY segment_key LIMIT ?2";

// The query selects only shares linked to the requested segment. On both
// sides, output excludes separately measured reasoning and a valid cache-write
// split uses one unit per lifetime instead of the combined total.
// Unknown measured quantities cannot be added to known quantities; the
// reported flags separately distinguish a missing role from a reported NULL.
const CHECK_SEGMENT: &str = "WITH segment AS (
        SELECT role,unit,quantity_units FROM broca_segment_measurement WHERE segment_key=?1
    ), attempts AS MATERIALIZED (
        SELECT m.role,m.unit,
               CASE WHEN count(*)=count(m.quantity_units) THEN sum(m.quantity_units) END AS quantity_units
          FROM broca_attempt_detail a CROSS JOIN measurement m ON m.event_key=a.event_key
         WHERE a.segment_fact_id=?2 GROUP BY m.role,m.unit
    )
    INSERT INTO broca_segment_mismatch(fact_id,role,unit,segment_quantity,attempts_quantity,
                                       segment_reported,attempts_reported,checked_at_ms)
    SELECT ?2,coalesce(s.role,a.role),coalesce(s.unit,a.unit),s.quantity_units,a.quantity_units,
           s.role IS NOT NULL,a.role IS NOT NULL,?3
      FROM segment s FULL OUTER JOIN attempts a ON s.role=a.role AND s.unit=a.unit
     WHERE s.role IS NULL OR a.role IS NULL OR s.quantity_units IS NOT a.quantity_units";

#[derive(Default)]
struct WideComparison {
    segment: Option<Option<i64>>,
    reported: bool,
    unknown: bool,
    sum: i128,
}

// The attempt sums are all evaluated before the INSERT sees its first row.
// If SQLite cannot represent a sum, reread just this segment's measurements
// and sum them exactly in a wider integer. Keep the other mismatches too.
fn check_wide_segment(
    tx: &Transaction<'_>,
    key: i64,
    fact_id: &str,
    now_ms: i64,
) -> rusqlite::Result<()> {
    let mut measured = tx.prepare_cached(
        "SELECT 0,role,unit,quantity_units FROM broca_segment_measurement WHERE segment_key=?1
         UNION ALL
         SELECT 1,m.role,m.unit,m.quantity_units FROM broca_attempt_detail a
         CROSS JOIN measurement m ON m.event_key=a.event_key WHERE a.segment_fact_id=?2",
    )?;
    let mut measurements = measured.query(params![key, fact_id])?;
    let mut quantities = BTreeMap::<(String, String), WideComparison>::new();
    while let Some(row) = measurements.next()? {
        let comparison = quantities.entry((row.get(1)?, row.get(2)?)).or_default();
        let quantity: Option<i64> = row.get(3)?;
        if row.get::<_, bool>(0)? {
            comparison.reported = true;
            if let Some(quantity) = quantity {
                comparison.sum += i128::from(quantity);
            } else {
                comparison.unknown = true;
            }
        } else {
            comparison.segment = Some(quantity);
        }
    }
    let mut insert = tx.prepare_cached(
        "INSERT INTO broca_segment_mismatch(fact_id,role,unit,segment_quantity,attempts_quantity,
                segment_reported,attempts_reported,over_range,checked_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )?;
    for ((role, unit), comparison) in quantities {
        let exact = i64::try_from(comparison.sum).ok();
        let over_range = exact.is_none();
        let attempts = comparison
            .reported
            .then_some(exact.filter(|_| !comparison.unknown));
        if over_range || comparison.segment != attempts {
            insert.execute(params![
                fact_id,
                role,
                unit,
                comparison.segment.flatten(),
                attempts.flatten(),
                comparison.segment.is_some(),
                comparison.reported,
                over_range,
                now_ms
            ])?;
        }
    }
    Ok(())
}

/// Compare at most one batch of mature segments. The caller commits the
/// cursor together with the evidence, so a failed batch is safe to retry.
pub fn check_segments_in_tx(tx: &Transaction<'_>, now_ms: i64) -> rusqlite::Result<usize> {
    let after: i64 = tx.query_row(
        "SELECT checked_through FROM broca_segment_check_state WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    let mut next = tx.prepare_cached(NEXT_SEGMENTS)?;
    let mut check = tx.prepare_cached(CHECK_SEGMENT)?;
    let mut candidates = next.query(params![after, CHECK_BATCH])?;
    let mut checked = 0;
    let mut through = after;
    let cutoff = now_ms.saturating_sub(GRACE_MS);
    while let Some(candidate) = candidates.next()? {
        if candidate.get::<_, i64>(2)? >= cutoff {
            // Do not step over an immature segment, even if a later ingest
            // has an earlier time because the wall clock moved backwards.
            break;
        }
        let key: i64 = candidate.get(0)?;
        let fact_id: String = candidate.get(1)?;
        match check.execute(params![key, fact_id, now_ms]) {
            Ok(_) => {}
            Err(Error::SqliteFailure(code, Some(message)))
                if code.code == ErrorCode::Unknown && message == "integer overflow" =>
            {
                check_wide_segment(tx, key, &fact_id, now_ms)?;
            }
            Err(error) => return Err(error),
        }
        through = key;
        checked += 1;
    }
    if through != after {
        tx.execute(
            "UPDATE broca_segment_check_state SET checked_through=?1 WHERE id=1",
            [through],
        )?;
    }
    Ok(checked)
}

pub(crate) fn segment_usage_mismatches(conn: &Connection) -> Result<u64, ReportError> {
    Ok(conn.query_row(
        "SELECT count(DISTINCT fact_id) FROM broca_segment_mismatch",
        [],
        |row| row.get(0),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{initialize_connection, VmCounter, MIGRATIONS};

    fn check_with_history(older: i64) -> u64 {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        let tx = conn.transaction().unwrap();
        let end = older + i64::from(CHECK_BATCH) + 1;
        tx.execute(
            "WITH RECURSIVE keys(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM keys WHERE n<?1)
             INSERT INTO broca_segment_evidence(segment_key,fact_id,run_id,recorded_at_ms)
             SELECT n, 'segment-'||n, 'run', 1 FROM keys",
            [end],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO broca_segment_measurement(segment_key,role,unit,quantity_units)
             SELECT segment_key,'input','token',42 FROM broca_segment_evidence",
            [],
        )
        .unwrap();
        tx.execute(
            "UPDATE broca_segment_check_state SET checked_through=?1",
            [older],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO usage_event(event_key,event_id,producer,producer_ref,recorded_at_ms,charge_basis)
             SELECT segment_key,'event-'||segment_key,'broca',fact_id,1,'metered_api'
               FROM broca_segment_evidence",
            [],
        ).unwrap();
        tx.execute(
            "INSERT INTO broca_attempt_detail(event_key,fact_id,share_index,attempt_id,step_id,attempt,
                outcome,content_received,response_bytes,recorded_at_ms,requested_service_tier,segment_fact_id)
             SELECT segment_key,'attempt-'||segment_key,0,'att-'||segment_key,1,1,'complete',1,42,1,'standard',fact_id
               FROM broca_segment_evidence",
            [],
        ).unwrap();
        tx.execute(
            "INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             SELECT event_key,'input','token','billed','usage.input_tokens',
                    CASE WHEN event_key=?1+1 THEN 43 ELSE 42 END,0 FROM usage_event",
            [older],
        ).unwrap();
        tx.commit().unwrap();

        let vm = VmCounter::start(&conn);
        let tx = conn.unchecked_transaction().unwrap();
        assert_eq!(
            check_segments_in_tx(&tx, GRACE_MS + 2).unwrap(),
            usize::try_from(CHECK_BATCH).unwrap()
        );
        tx.commit().unwrap();
        let steps = vm.steps();
        assert_eq!(segment_usage_mismatches(&conn).unwrap(), 1);
        assert_eq!(
            conn.query_row(
                "SELECT checked_through FROM broca_segment_check_state",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            end - 1
        );
        steps
    }

    #[test]
    fn broca_segment_check_tick_steps_stay_flat_after_tenfold_history_growth() {
        let small = check_with_history(1_000);
        let large = check_with_history(10_000);
        println!("BROCA_SEGMENT_CHECK_128: 1000 prior segments={small}, 10000 prior segments={large} VM steps");
        assert_eq!(small, large);
    }
}
