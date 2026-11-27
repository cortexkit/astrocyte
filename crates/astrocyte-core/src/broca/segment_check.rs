use std::collections::BTreeMap;

use rusqlite::{params, Connection, Error, ErrorCode, Statement, Transaction};

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
    SELECT coalesce(s.role,a.role),coalesce(s.unit,a.unit),s.quantity_units,a.quantity_units,
            s.role IS NOT NULL,a.role IS NOT NULL
      FROM segment s FULL OUTER JOIN attempts a ON s.role=a.role AND s.unit=a.unit
     WHERE s.role IS NULL OR a.role IS NULL OR s.quantity_units IS NOT a.quantity_units";

const INSERT_MISMATCH: &str = "INSERT INTO broca_segment_mismatch(
    fact_id,role,unit,segment_quantity,attempts_quantity,segment_reported,attempts_reported,
    over_range,checked_at_ms
) SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9 WHERE NOT EXISTS (
    SELECT 1 FROM broca_segment_mismatch WHERE fact_id=?1 AND role=?2 AND unit=?3
)";

pub(super) fn requeue_checked_segment(tx: &Transaction<'_>, fact_id: &str) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "INSERT OR IGNORE INTO broca_segment_recheck(segment_key)
         SELECT segment_key FROM broca_segment_evidence WHERE fact_id=?1 AND segment_key <= (
             SELECT checked_through FROM broca_segment_check_state WHERE id=1
         )",
    )?
    .execute([fact_id])?;
    Ok(())
}

#[derive(Default)]
struct WideComparison {
    segment: Option<Option<i64>>,
    reported: bool,
    unknown: bool,
    sum: i128,
}

// SQLite evaluates all attempt sums before returning a comparison. If a sum
// exceeds its integer range, use wider integers for this segment alone.
fn check_wide_segment(
    tx: &Transaction<'_>,
    insert: &mut Statement<'_>,
    key: i64,
    fact_id: &str,
    now_ms: i64,
) -> rusqlite::Result<bool> {
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
    let mut differing = false;
    for ((role, unit), comparison) in quantities {
        let exact = i64::try_from(comparison.sum).ok();
        let over_range = exact.is_none();
        let attempts = comparison
            .reported
            .then_some(exact.filter(|_| !comparison.unknown));
        if over_range || comparison.segment != attempts {
            differing = true;
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
    Ok(differing)
}

fn refresh_segment(
    tx: &Transaction<'_>,
    key: i64,
    fact_id: &str,
    now_ms: i64,
) -> rusqlite::Result<()> {
    let mut check = tx.prepare_cached(CHECK_SEGMENT)?;
    let mut insert = tx.prepare_cached(INSERT_MISMATCH)?;
    let mut rows = check.query(params![key, fact_id])?;
    let mut differing = false;
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                differing = true;
                insert.execute(params![
                    fact_id,
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                    false,
                    now_ms,
                ])?;
            }
            Ok(None) => break,
            Err(Error::SqliteFailure(code, Some(message)))
                if code.code == ErrorCode::Unknown && message == "integer overflow" =>
            {
                differing = check_wide_segment(tx, &mut insert, key, fact_id, now_ms)?;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    let update = if differing {
        "INSERT OR IGNORE INTO broca_segment_current_mismatch(segment_key) VALUES(?1)"
    } else {
        "DELETE FROM broca_segment_current_mismatch WHERE segment_key=?1"
    };
    tx.prepare_cached(update)?.execute([key])?;
    Ok(())
}

/// Compare at most one batch of mature segments. The caller commits the
/// cursor and queued rechecks with the evidence, so a failed batch is safe to retry.
pub fn check_segments_in_tx(tx: &Transaction<'_>, now_ms: i64) -> rusqlite::Result<usize> {
    let after: i64 = tx.query_row(
        "SELECT checked_through FROM broca_segment_check_state WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    // Give both streams room to progress even if late or new deliveries never stop.
    let mut queued = tx.prepare_cached(
        "SELECT q.segment_key,e.fact_id FROM broca_segment_recheck q
         CROSS JOIN broca_segment_evidence e USING(segment_key)
         ORDER BY q.segment_key LIMIT ?1",
    )?;
    let queued = queued
        .query_map([CHECK_BATCH / 2], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut clear = tx.prepare_cached("DELETE FROM broca_segment_recheck WHERE segment_key=?1")?;
    for (key, fact_id) in &queued {
        refresh_segment(tx, *key, fact_id, now_ms)?;
        clear.execute([key])?;
    }
    let mut checked = queued.len();
    let remaining = CHECK_BATCH - u32::try_from(checked).expect("rechecks fit batch limit");
    let mut next = tx.prepare_cached(NEXT_SEGMENTS)?;
    let mut candidates = next.query(params![after, remaining])?;
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
        refresh_segment(tx, key, &fact_id, now_ms)?;
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
        "SELECT count(*) FROM broca_segment_current_mismatch",
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

    #[test]
    fn broca_recheck_migration_seeds_only_historical_mismatches() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let upgrade = MIGRATIONS
            .iter()
            .position(|migration| {
                migration
                    .statements
                    .contains("CREATE TABLE broca_segment_recheck")
            })
            .unwrap();
        let (earlier, later) = MIGRATIONS.split_at(upgrade);
        for migration in earlier {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute_batch(
            "INSERT INTO broca_segment_evidence VALUES(1,'matched','run',1),(2,'different','run',1);
             UPDATE broca_segment_check_state SET checked_through=2 WHERE id=1;
             INSERT INTO broca_segment_mismatch(fact_id,role,unit,segment_quantity,attempts_quantity,
                      segment_reported,attempts_reported,checked_at_ms)
             VALUES ('different','input','token',42,NULL,1,0,3600002),
                    ('different','output','token',12,NULL,1,0,3600002);",
        ).unwrap();
        conn.execute_batch(later[0].statements).unwrap();
        assert_eq!(segment_usage_mismatches(&conn).unwrap(), 1);
        let queued = conn
            .prepare("SELECT segment_key FROM broca_segment_recheck ORDER BY segment_key")
            .unwrap()
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(queued, [2]);
    }
}
