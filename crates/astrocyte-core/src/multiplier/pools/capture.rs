use rusqlite::{params, Connection, Row};

use crate::capacity::{pool_identity, WindowModelScope};
use crate::subscription::PlanLedger;

use super::super::{clamp_nanos, clean, millis_to_nanos, MultiplierError};
use super::history::VaultPlanTimeline;
use super::inheritance::inherit_metadata;
use super::{PoolLogin, PoolReading, PoolReadingCapture, Stream};

pub(super) fn sample_table(ledger: PlanLedger) -> (&'static str, &'static str) {
    match ledger {
        PlanLedger::Account => ("window_sample", "account_ref"),
        PlanLedger::Credential => ("credential_window_sample", "credential_ref"),
    }
}

/// Capture both login namespaces and their effective vault plans from the
/// caller's read transaction. Usage for the same observation must be read in
/// that transaction too, before contacting the catalog.
pub fn capture_pool_readings(
    tx: &Connection,
    quota_provider: &str,
    interval_start_ms: i64,
    interval_end_ms: i64,
) -> Result<PoolReadingCapture, MultiplierError> {
    if interval_start_ms >= interval_end_ms {
        return Err(MultiplierError::Refused {
            code: "invalid_range",
            message: "interval start must precede its end".into(),
        });
    }
    let applied_at_unix: i64 = tx.query_row(
        "SELECT applied_at_unix FROM cortexkit_schema_version WHERE namespace='astrocyte' AND version=68",
        [],
        |row| row.get(0),
    )?;
    let cut_ns = clamp_nanos(i128::from(applied_at_unix) * 1_000_000_000);
    let mut streams = Vec::new();
    let mut keys = Vec::new();
    for ledger in [PlanLedger::Account, PlanLedger::Credential] {
        let namespace_keys = stream_keys(tx, quota_provider, ledger)?;
        capture_namespace(
            tx,
            quota_provider,
            (interval_start_ms, interval_end_ms),
            ledger,
            &namespace_keys,
            &mut streams,
        )?;
        keys.extend(
            namespace_keys
                .into_iter()
                .map(|(id, key)| (PoolLogin { ledger, id }, key)),
        );
    }
    let mut bounds = (interval_start_ms, interval_end_ms);
    for stream in &streams {
        if let Some(first) = stream.readings.first() {
            bounds.0 = bounds.0.min(first.fetched_at_ms());
        }
        if let Some(last) = stream.readings.last() {
            bounds.1 = bounds.1.max(last.fetched_at_ms());
        }
    }
    let needs_metadata = streams
        .iter()
        .flat_map(|stream| &stream.readings)
        .any(|reading| {
            reading.api_provider.is_none()
                || (reading.fetched_at_instant_ns < cut_ns && reading.model_scope.is_none())
        });
    if needs_metadata {
        // A donor may report after the measured interval. Match its plan at
        // the donor's own timestamp, not at the end of the observation.
        bounds.1 = bounds
            .1
            .max(i64::MAX.div_euclid(super::super::NANOS_PER_MS));
    }
    let history = VaultPlanTimeline::capture(tx, bounds)?;
    if needs_metadata {
        inherit_metadata(tx, quota_provider, cut_ns, &history, &keys, &mut streams)?;
    }
    Ok(PoolReadingCapture {
        quota_provider: quota_provider.into(),
        interval_start_ms,
        interval_end_ms,
        streams,
        history,
    })
}

fn capture_namespace(
    tx: &Connection,
    provider: &str,
    (start_ms, end_ms): (i64, i64),
    ledger: PlanLedger,
    keys: &[(String, String)],
    streams: &mut Vec<Stream>,
) -> rusqlite::Result<()> {
    let (table, field) = sample_table(ledger);
    let start_ns = clamp_nanos(millis_to_nanos(start_ms));
    let end_ns = clamp_nanos(millis_to_nanos(end_ms));
    let tail_ns = clamp_nanos(millis_to_nanos(end_ms.saturating_add(clean::MAX_GAP_MS)));
    let predicate = format!("provider=?1 AND {field}=?2 AND pool_key=?3");
    let base = format!(
        "SELECT fetched_at_instant, COALESCE(raw_used_percent, used_percent), resets_at,
                window_minutes, model_scope, source, api_provider FROM {table} WHERE {predicate}"
    );
    // As in the counter reader, the upper bound includes the first sample
    // beyond the gap allowance, so a quiet pair can still certify coverage.
    let mut bounded = tx.prepare_cached(&format!(
        "{base} AND fetched_at_instant>=COALESCE((
             SELECT MAX(fetched_at_instant) FROM {table} WHERE {predicate}
             AND fetched_at_instant<?4), ?4)
         AND fetched_at_instant<=COALESCE((
             SELECT MIN(fetched_at_instant) FROM {table} WHERE {predicate}
             AND fetched_at_instant>?5), ?5)
         ORDER BY fetched_at_instant"
    ))?;
    let mut lookback = tx.prepare_cached(&format!(
        "{base} AND fetched_at_instant>=?4 AND fetched_at_instant<?5 ORDER BY fetched_at_instant"
    ))?;
    for (id, key) in keys {
        let Some(pool_id) = pool_identity(key) else {
            continue;
        };
        let mut readings = read_samples(&mut bounded, provider, id, key, (start_ns, tail_ns))?;
        // A changed window has no single duration yet. Keep the largest
        // possible lookback and classify its interval endpoints in the planner.
        let through_closer = readings.partition_point(|r| r.fetched_at_instant_ns < end_ns);
        let minutes = readings
            .iter()
            .take(through_closer.saturating_add(1))
            .filter_map(|reading| reading.window_minutes)
            .max()
            .unwrap_or(0);
        let lower_ms = start_ms.saturating_sub(minutes.saturating_mul(60_000));
        let lower_ns = clamp_nanos(millis_to_nanos(lower_ms));
        let older_end = readings
            .first()
            .map_or(start_ns, |r| start_ns.min(r.fetched_at_instant_ns));
        if lower_ns < older_end {
            let mut older = read_samples(&mut lookback, provider, id, key, (lower_ns, older_end))?;
            older.append(&mut readings);
            readings = older;
        } else if lower_ns > older_end
            && readings
                .first()
                .is_some_and(|r| r.fetched_at_instant_ns < start_ns)
        {
            // The pre-start seed was only used to discover the duration.
            readings.remove(0);
        }
        if readings.len() < 2 {
            continue;
        }
        streams.push(Stream {
            login: PoolLogin {
                ledger,
                id: id.clone(),
            },
            pool_id: pool_id.into(),
            window_pool_key: key.clone(),
            readings,
            scope_conflicts: Default::default(),
        });
    }

    Ok(())
}

fn read_samples(
    stmt: &mut rusqlite::Statement<'_>,
    provider: &str,
    id: &str,
    key: &str,
    (from, to): (i64, i64),
) -> rusqlite::Result<Vec<PoolReading>> {
    let rows = stmt.query_map(params![provider, id, key, from, to], |row| {
        Ok((
            PoolReading {
                fetched_at_instant_ns: row.get(0)?,
                used_percent: row.get(1)?,
                resets_at: row.get(2)?,
                window_minutes: row.get(3)?,
                model_scope: read_scope(row, 4)?,
                api_provider: row.get::<_, Option<String>>(6)?.filter(|id| !id.is_empty()),
            },
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    let mut readings = Vec::new();
    for row in rows {
        let (reading, source) = row?;
        if !clean::historical_sample_excluded(
            provider,
            source.as_deref(),
            reading.fetched_at_instant_ns,
        ) {
            readings.push(reading);
        }
    }
    Ok(readings)
}

pub(super) fn read_scope(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<WindowModelScope>> {
    Ok(match row.get_ref(index)?.as_str_or_null()? {
        Some("all") => Some(WindowModelScope::All),
        Some("subset") => Some(WindowModelScope::Subset),
        _ => None,
    })
}

// Jump over reading history using the primary key's login and pool prefixes.
fn stream_keys(
    tx: &Connection,
    provider: &str,
    ledger: PlanLedger,
) -> rusqlite::Result<Vec<(String, String)>> {
    let (table, field) = sample_table(ledger);
    let sql = format!(
        "WITH RECURSIVE identities(id) AS (
             SELECT (SELECT {field} FROM {table} WHERE provider=?1 ORDER BY {field} LIMIT 1)
             UNION ALL
             SELECT (SELECT {field} FROM {table} WHERE provider=?1 AND {field}>identities.id
                     ORDER BY {field} LIMIT 1) FROM identities WHERE id IS NOT NULL
         ), pools(id,key) AS (
             SELECT id,(SELECT pool_key FROM {table} WHERE provider=?1 AND {field}=identities.id
                        ORDER BY pool_key LIMIT 1) FROM identities WHERE id IS NOT NULL
             UNION ALL
             SELECT id,(SELECT pool_key FROM {table} WHERE provider=?1 AND {field}=pools.id
                          AND pool_key>pools.key ORDER BY pool_key LIMIT 1)
             FROM pools WHERE key IS NOT NULL
         ) SELECT id,key FROM pools WHERE id<>'' AND key IS NOT NULL"
    );
    let mut stmt = tx.prepare_cached(&sql)?;
    let keys = stmt.query_map([provider], |row| Ok((row.get(0)?, row.get(1)?)))?;
    keys.collect()
}
