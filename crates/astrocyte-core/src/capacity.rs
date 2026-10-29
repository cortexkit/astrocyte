//! The CAPACITY axis: QTA `usage.get` quota-window
//! utilization, consumed read-only and joined to spend by `account_ref`.
//!
//! This is NOT money and NOT a fact — it is a cache of the latest observed
//! rate-limit windows, refreshed wholesale each successful poll. It never
//! blocks metering, ingestion, or verdicts; a malformed/unavailable poll
//! leaves the prior rows in place and marks the axis stale (never silently
//! blank). It is advisory only: nothing prices, refuses or caps from it.
//!
//! The types here are astrocyte's OWN normalized shape. The wire types QTA
//! serves are mirrored in the module layer (`qta_wire`) and mapped into these
//! before they reach core, so core carries no `Option` soup and no wire
//! spelling — the same boundary discipline `wire_map` gives the broca seam.

use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};

/// One observed quota window with its complete durable pool identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityWindow {
    pub provider: String,
    pub api_provider: Option<String>,
    pub account_ref: String,
    /// Compatibility projection of `pool_key`; persistence and identity use
    /// `pool_key` exclusively.
    pub pool: String,
    pub pool_key: String,
    pub slot_kind: String,
    pub external_id: Option<String>,
    pub title: Option<String>,
    pub extra_ordinal: Option<i64>,
    pub legacy_pool_key: Option<String>,
    pub used_percent: f64,
    pub raw_used_percent: Option<f64>,
    pub resets_at: Option<String>,
    pub window_minutes: Option<i64>,
    pub used_count: Option<i64>,
    pub total_count: Option<i64>,
    pub source: Option<String>,
    pub fetched_at: Option<String>,
    pub stale_since: Option<String>,
    pub stale_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub breakdown: Option<String>,
}

/// Validates the producer chronology for a preserved reading without changing
/// either timestamp.
pub fn validate_preserved_reading(
    fetched_at: Option<&str>,
    stale_since: &str,
) -> Result<(), String> {
    let fetched_at =
        fetched_at.ok_or_else(|| "preserved capacity entry is missing fetchedAt".to_string())?;
    let fetched_at = chrono::DateTime::parse_from_rfc3339(fetched_at)
        .map_err(|_| "preserved capacity entry has invalid fetchedAt".to_string())?;
    let stale_since = chrono::DateTime::parse_from_rfc3339(stale_since)
        .map_err(|_| "preserved capacity entry has invalid stale.since".to_string())?;
    if stale_since <= fetched_at {
        return Err("capacity stale.since must be later than fetchedAt".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapacityObservation {
    pub provider: String,
    pub api_provider: Option<String>,
    pub account_ref: String,
    pub observation_state: String,
    pub error_class: Option<String>,
    pub error_detail: Option<String>,
    pub source: Option<String>,
    pub fetched_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapacityProviderReading {
    pub provider: String,
    pub api_provider: Option<String>,
    /// Present only when the source has no account identity.
    pub credential_ref: Option<String>,
    pub pool_key: String,
    pub slot_kind: String,
    pub external_id: Option<String>,
    pub title: Option<String>,
    pub extra_ordinal: Option<i64>,
    pub used_percent: f64,
    pub raw_used_percent: Option<f64>,
    pub resets_at: Option<String>,
    pub window_minutes: Option<i64>,
    pub used_count: Option<i64>,
    pub total_count: Option<i64>,
    pub source: Option<String>,
    pub fetched_at: Option<String>,
}

/// One well-formed response. The `complete_providers` set applies only to this
/// snapshot transaction and is not persisted for use by later responses.
#[derive(Debug, Clone, PartialEq)]
pub struct CapacitySnapshot {
    pub windows: Vec<CapacityWindow>,
    pub observations: Vec<CapacityObservation>,
    pub provider_readings: Vec<CapacityProviderReading>,
    pub complete_providers: Option<std::collections::BTreeSet<String>>,
    pub degraded_entries: u64,
}

/// Durable capacity runtime chronology and the cadence governing freshness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityFreshness {
    pub runtime_variant: String,
    pub effective_cadence_ms: i64,
    pub changed_at_ms: Option<i64>,
    pub enabled_at_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub last_attempt_ms: Option<i64>,
    pub degraded_entries: u64,
    pub detail: Option<String>,
}

impl CapacityFreshness {
    /// The existing capacity freshness horizon, shared with event-mode backstop scheduling.
    pub fn horizon_ms(&self) -> i64 {
        self.effective_cadence_ms.saturating_mul(4)
    }
}

/// Persists one validated response inside a transaction owned by the caller,
/// deleting missing labeled accounts only for providers declared complete by
/// that same response.
pub fn apply_snapshot_in_tx(
    tx: &Transaction<'_>,
    snapshot: &CapacitySnapshot,
    now_ms: i64,
) -> rusqlite::Result<()> {
    apply_readings_in_tx(tx, snapshot, now_ms, true)
}

/// Apply an event for exactly one quota account, without inventory retirement authority.
/// An absent account label addresses only provider-current readings, never labeled accounts.
pub fn apply_account_in_tx(
    tx: &Transaction<'_>,
    snapshot: &CapacitySnapshot,
    provider: &str,
    account: Option<&str>,
    now_ms: i64,
) -> rusqlite::Result<()> {
    if snapshot.complete_providers.is_some()
        || snapshot.observations.iter().any(|reading| {
            reading.provider != provider || Some(reading.account_ref.as_str()) != account
        })
        || snapshot.windows.iter().any(|reading| {
            reading.provider != provider || Some(reading.account_ref.as_str()) != account
        })
        || snapshot
            .provider_readings
            .iter()
            .any(|reading| reading.provider != provider || account.is_some())
    {
        return Err(rusqlite::Error::InvalidParameterName(
            "quota event is not confined to its account".into(),
        ));
    }
    apply_readings_in_tx(tx, snapshot, now_ms, false)
}

fn apply_readings_in_tx(
    tx: &Transaction<'_>,
    snapshot: &CapacitySnapshot,
    now_ms: i64,
    retire_accounts: bool,
) -> rusqlite::Result<()> {
    let degraded_entries = i64::try_from(snapshot.degraded_entries).map_err(|_| {
        rusqlite::Error::InvalidParameterName("degraded entry count exceeds SQLite range".into())
    })?;
    let runtime_variant: String = tx.query_row(
        "SELECT runtime_variant FROM capacity_runtime WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    if runtime_variant == "disabled" {
        return Err(rusqlite::Error::InvalidParameterName(
            "capacity polling is disabled".into(),
        ));
    }

    for observation in &snapshot.observations {
        tx.execute(
            "INSERT INTO capacity_observation
             (provider,api_provider,account_ref,observation_state,error_class,error_detail,
              source,fetched_at,observed_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(provider,account_ref) DO UPDATE SET
               api_provider=excluded.api_provider,
               observation_state=excluded.observation_state,
               error_class=excluded.error_class,
               error_detail=excluded.error_detail,
               source=excluded.source,
               fetched_at=excluded.fetched_at,
               observed_at_ms=excluded.observed_at_ms",
            params![
                observation.provider,
                observation.api_provider,
                observation.account_ref,
                observation.observation_state,
                observation.error_class,
                observation.error_detail,
                observation.source,
                observation.fetched_at,
                now_ms,
            ],
        )?;
        if observation.observation_state == "usable" {
            tx.execute(
                "DELETE FROM capacity_window WHERE provider=?1 AND account_ref=?2",
                params![observation.provider, observation.account_ref],
            )?;
        }
    }

    {
        let mut insert = tx.prepare(
            "INSERT INTO capacity_window
             (provider,api_provider,account_ref,pool_key,slot_kind,external_id,title,
               extra_ordinal,legacy_pool_key,used_percent,raw_used_percent,resets_at,
               window_minutes,used_count,total_count,source,fetched_at,stale_since,
               stale_class,observed_at_ms)
              VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
        )?;
        for window in &snapshot.windows {
            insert.execute(params![
                window.provider,
                window.api_provider,
                window.account_ref,
                window.pool_key,
                window.slot_kind,
                window.external_id,
                window.title,
                window.extra_ordinal,
                window.legacy_pool_key,
                window.used_percent,
                window.raw_used_percent,
                window.resets_at,
                window.window_minutes,
                window.used_count,
                window.total_count,
                window.source,
                window.fetched_at,
                window.stale_since,
                window.stale_class,
                now_ms,
            ])?;
        }
    }

    // RETENTION: none, by decision. window_sample is append-only and nothing
    // deletes from it, so it grows for the life of the store. Measured on the
    // production store on 2026-09-24: 269,153 rows over 12.9 days, about 20,800
    // rows and 4 MB a day, so roughly 1.5 GB a year. A bound would destroy
    // readings nobody can fetch again, and a fitted curve needs its oldest points
    // as much as its newest. Revisit if the daily rate changes materially, for
    // example when more providers or accounts are polled.
    {
        let mut sample = tx.prepare(
            "INSERT INTO window_sample
             (provider,api_provider,account_ref,pool_key,fetched_at_instant,used_percent,
              raw_used_percent,resets_at,window_minutes,source,breakdown)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(provider,account_ref,pool_key,fetched_at_instant) DO NOTHING",
        )?;
        for window in &snapshot.windows {
            // Readings without a parseable source timestamp have no place in history. The explicit
            // conflict target makes repeat samples idempotent without hiding other constraint
            // failures. The parsed instant is the key, so equivalent timestamp spellings still
            // describe one reading.
            //
            // Keep the time reported by the source, not the time of the poll that received it.
            // Providers can return cached readings that predate the poll, and that delay cannot be
            // reconstructed later if only poll time is stored.
            //
            // Paid-tier history from deployed versions 485f0f66 through 2beb896d0153
            // (corrected on 2026-09-12) includes readings from the public-service quota path.
            // On those versions, missing account identity sent paid accounts to that path,
            // whose quota service reports a different pool. Samples record the pool key but
            // not which lookup path produced the reading; a change between pool series cannot
            // identify which value is the paid-tier reading. Mixing them in a paid-tier usage
            // delta would turn a pool switch into apparent consumption, so exclude readings
            // from that known deployment interval from paid-tier analysis.
            let Some(fetched_at_instant) = fetched_at_instant(window.fetched_at.as_deref()) else {
                continue;
            };
            sample.execute(params![
                window.provider,
                window.api_provider,
                window.account_ref,
                window.pool_key,
                fetched_at_instant,
                window.used_percent,
                window.raw_used_percent,
                window.resets_at,
                window.window_minutes,
                window.source,
                window.breakdown,
            ])?;
        }
    }

    if let Some(complete_providers) = snapshot
        .complete_providers
        .as_ref()
        .filter(|_| retire_accounts)
    {
        for provider in complete_providers {
            // One JSON set avoids a delete per missing account and one bind slot per survivor.
            let survivors = snapshot
                .observations
                .iter()
                .filter(|observation| observation.provider == *provider)
                .map(|observation| &observation.account_ref)
                .collect::<Vec<_>>();
            let survivors_json = serde_json::to_string(&survivors)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            tx.execute(
                "DELETE FROM capacity_window WHERE provider=?1
                 AND NOT EXISTS (
                     SELECT 1 FROM json_each(?2) AS survivor
                     WHERE survivor.value=capacity_window.account_ref
                 )",
                params![provider, survivors_json],
            )?;
            tx.execute(
                "DELETE FROM capacity_observation WHERE provider=?1
                 AND NOT EXISTS (
                     SELECT 1 FROM json_each(?2) AS survivor
                     WHERE survivor.value=capacity_observation.account_ref
                 )",
                params![provider, survivors_json],
            )?;
        }
    }

    {
        let mut insert = tx.prepare(
            "INSERT INTO capacity_provider_reading
             (provider,api_provider,pool_key,slot_kind,external_id,title,extra_ordinal,
              used_percent,raw_used_percent,resets_at,window_minutes,used_count,total_count,
              source,fetched_at,observed_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)
             ON CONFLICT(provider,pool_key) DO UPDATE SET
               api_provider=excluded.api_provider,
               slot_kind=excluded.slot_kind,
               external_id=excluded.external_id,
               title=excluded.title,
               extra_ordinal=excluded.extra_ordinal,
               used_percent=excluded.used_percent,
               raw_used_percent=excluded.raw_used_percent,
               resets_at=excluded.resets_at,
               window_minutes=excluded.window_minutes,
               used_count=excluded.used_count,
               total_count=excluded.total_count,
               source=excluded.source,
               fetched_at=excluded.fetched_at,
               observed_at_ms=excluded.observed_at_ms",
        )?;
        for reading in &snapshot.provider_readings {
            if let Some(credential_ref) = &reading.credential_ref {
                if let Some(instant) = fetched_at_instant(reading.fetched_at.as_deref()) {
                    tx.execute(
                        "INSERT INTO credential_window_sample
                         (provider,credential_ref,api_provider,pool_key,fetched_at_instant,
                          used_percent,raw_used_percent,resets_at,window_minutes,source)
                         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                         ON CONFLICT(provider,credential_ref,pool_key,fetched_at_instant) DO NOTHING",
                        params![reading.provider, credential_ref, reading.api_provider,
                            reading.pool_key, instant, reading.used_percent,
                            reading.raw_used_percent, reading.resets_at,
                            reading.window_minutes, reading.source],
                    )?;
                }
                continue;
            }
            insert.execute(params![
                reading.provider,
                reading.api_provider,
                reading.pool_key,
                reading.slot_kind,
                reading.external_id,
                reading.title,
                reading.extra_ordinal,
                reading.used_percent,
                reading.raw_used_percent,
                reading.resets_at,
                reading.window_minutes,
                reading.used_count,
                reading.total_count,
                reading.source,
                reading.fetched_at,
                now_ms,
            ])?;
        }
    }

    let has_usable = !snapshot.windows.is_empty() || !snapshot.provider_readings.is_empty();
    let detail = if degraded_entries > 0 {
        Some(format!("{degraded_entries} degraded capacity entries"))
    } else if has_usable {
        None
    } else {
        Some("capacity response contained no usable windows".to_string())
    };
    match (runtime_variant.as_str(), has_usable) {
        ("configured_unavailable", true) | ("active", true) => {
            tx.execute(
                "UPDATE capacity_runtime SET runtime_variant='active', last_attempt_ms=?1,
                 last_success_ms=?1, degraded_entries=?2, detail=?3 WHERE id=1",
                params![now_ms, degraded_entries, detail],
            )?;
        }
        ("configured_unavailable", false) | ("active", false) => {
            tx.execute(
                "UPDATE capacity_runtime SET last_attempt_ms=?1, degraded_entries=?2,
                 detail=?3 WHERE id=1",
                params![now_ms, degraded_entries, detail],
            )?;
        }
        _ => return Err(rusqlite::Error::InvalidQuery),
    }
    Ok(())
}

/// Parses insula's `fetchedAt` into a nanosecond instant since the Unix epoch.
///
/// Returns `None` when the field is absent, unparseable, or outside the
/// representable nanosecond range (chrono's `timestamp_nanos_opt` returns
/// `None` beyond roughly the year 2262) — such a reading has no place on the
/// time axis and is skipped, never clamped or wrapped. The instant, not the
/// string, is the dedupe key: chrono's `to_rfc3339` picks precision from the
/// value (a whole second prints no fraction, a nanosecond value prints nine
/// digits, and six-digit forms exist in the wild), so string equality is not
/// instant equality.
fn fetched_at_instant(fetched_at: Option<&str>) -> Option<i64> {
    let raw = fetched_at?;
    let parsed = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
    parsed.timestamp_nanos_opt()
}

/// Records an enabled attempt that produced no snapshot. Last-known snapshot
/// state and success chronology remain unchanged.
pub fn record_failed_attempt(conn: &Connection, now_ms: i64, detail: &str) -> rusqlite::Result<()> {
    let changed = conn.execute(
        "UPDATE capacity_runtime SET last_attempt_ms=?1, detail=?2
         WHERE id=1 AND runtime_variant IN ('configured_unavailable','active')",
        params![now_ms, detail],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::InvalidParameterName(
            "capacity polling is disabled".into(),
        ));
    }
    Ok(())
}

/// Reads the singleton durable runtime row.
pub fn freshness(conn: &Connection) -> rusqlite::Result<CapacityFreshness> {
    conn.query_row(
        "SELECT runtime_variant,effective_cadence_ms,changed_at_ms,enabled_at_ms,
                last_success_ms,last_attempt_ms,degraded_entries,detail
         FROM capacity_runtime WHERE id=1",
        [],
        |row| {
            let degraded: i64 = row.get(6)?;
            let degraded_entries = u64::try_from(degraded).map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    6,
                    "degraded_entries".into(),
                    rusqlite::types::Type::Integer,
                )
            })?;
            Ok(CapacityFreshness {
                runtime_variant: row.get(0)?,
                effective_cadence_ms: row.get(1)?,
                changed_at_ms: row.get(2)?,
                enabled_at_ms: row.get(3)?,
                last_success_ms: row.get(4)?,
                last_attempt_ms: row.get(5)?,
                degraded_entries,
                detail: row.get(7)?,
            })
        },
    )
}

/// Derives staleness from the runtime variant and caller-supplied age horizon.
pub fn is_stale(freshness: &CapacityFreshness, now_ms: i64, max_age_ms: i64) -> bool {
    match freshness.runtime_variant.as_str() {
        "disabled" => false,
        "configured_unavailable" => freshness
            .enabled_at_ms
            .is_none_or(|enabled| now_ms.saturating_sub(enabled) > max_age_ms),
        "active" => freshness
            .last_success_ms
            .is_none_or(|success| now_ms.saturating_sub(success) > max_age_ms),
        _ => true,
    }
}

/// Reads labeled cached windows with their complete durable identity.
pub fn windows(conn: &Connection) -> rusqlite::Result<Vec<CapacityWindow>> {
    let mut stmt = conn.prepare(
        "SELECT provider,api_provider,account_ref,pool_key,slot_kind,external_id,title,
                extra_ordinal,legacy_pool_key,used_percent,raw_used_percent,resets_at,
                window_minutes,used_count,total_count,source,fetched_at,stale_since,stale_class
         FROM capacity_window
         ORDER BY provider,account_ref,pool_key",
    )?;
    let rows = stmt.query_map([], |row| {
        let pool_key: String = row.get(3)?;
        Ok(CapacityWindow {
            provider: row.get(0)?,
            api_provider: row.get(1)?,
            account_ref: row.get(2)?,
            pool: pool_key.clone(),
            pool_key,
            slot_kind: row.get(4)?,
            external_id: row.get(5)?,
            title: row.get(6)?,
            extra_ordinal: row.get(7)?,
            legacy_pool_key: row.get(8)?,
            used_percent: row.get(9)?,
            raw_used_percent: row.get(10)?,
            resets_at: row.get(11)?,
            window_minutes: row.get(12)?,
            used_count: row.get(13)?,
            total_count: row.get(14)?,
            source: row.get(15)?,
            fetched_at: row.get(16)?,
            stale_since: row.get(17)?,
            stale_class: row.get(18)?,
            breakdown: None,
        })
    })?;
    rows.collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn credential_history_migration_preserves_accounts_and_enforces_identity() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for migration in crate::store::MIGRATIONS
            .iter()
            .filter(|migration| migration.version < 36)
        {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent) VALUES ('p','account','primary',1,10)", []).unwrap();
        conn.execute_batch(
            crate::store::MIGRATIONS
                .iter()
                .find(|migration| migration.version == 36)
                .unwrap()
                .statements,
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT account_ref FROM window_sample", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "account"
        );
        assert!(conn.execute("INSERT INTO credential_window_sample(provider,credential_ref,pool_key,fetched_at_instant,used_percent) VALUES ('p','','primary',1,20)", []).is_err());
        conn.execute("INSERT INTO credential_window_sample(provider,credential_ref,pool_key,fetched_at_instant,used_percent) VALUES ('p','credential','primary',1,20)", []).unwrap();
        assert!(conn
            .execute("UPDATE credential_window_sample SET used_percent=30", [])
            .is_err());
        assert!(conn
            .execute("DELETE FROM credential_window_sample", [])
            .is_err());
        assert_eq!(
            conn.query_row(
                "SELECT used_percent FROM credential_window_sample",
                [],
                |r| r.get::<_, f64>(0)
            )
            .unwrap()
            .to_bits(),
            20.0_f64.to_bits()
        );
    }
    use super::*;
    use crate::store::{initialize_connection, MIGRATIONS};
    use std::collections::BTreeSet;

    /// Applies a snapshot the way the refresher does: through
    /// `apply_snapshot_in_tx` inside one transaction that commits on success.
    fn apply_snapshot(
        conn: &mut Connection,
        snapshot: &CapacitySnapshot,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        apply_snapshot_in_tx(&tx, snapshot, now_ms)?;
        tx.commit()
    }

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute(
            "UPDATE capacity_runtime SET runtime_variant='configured_unavailable',
             changed_at_ms=NULL, enabled_at_ms=0, detail='no usable poll completed' WHERE id=1",
            [],
        )
        .unwrap();
        conn
    }

    fn win(provider: &str, account: &str, pool_key: &str, used: f64) -> CapacityWindow {
        CapacityWindow {
            provider: provider.into(),
            api_provider: None,
            account_ref: account.into(),
            pool: pool_key.into(),
            pool_key: pool_key.into(),
            slot_kind: pool_key.into(),
            external_id: None,
            title: None,
            extra_ordinal: None,
            legacy_pool_key: None,
            used_percent: used,
            raw_used_percent: None,
            resets_at: None,
            window_minutes: None,
            used_count: None,
            total_count: None,
            source: None,
            fetched_at: None,
            stale_since: None,
            stale_class: None,
            breakdown: None,
        }
    }

    fn win_at(
        provider: &str,
        account: &str,
        pool_key: &str,
        used: f64,
        fetched_at: &str,
    ) -> CapacityWindow {
        let mut window = win(provider, account, pool_key, used);
        window.fetched_at = Some(fetched_at.into());
        window
    }

    fn sample_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM window_sample", [], |row| row.get(0))
            .unwrap()
    }

    unsafe extern "C" fn capture_sql_statement(
        _mask: std::os::raw::c_uint,
        context: *mut std::ffi::c_void,
        _statement: *mut std::ffi::c_void,
        sql: *mut std::ffi::c_void,
    ) -> std::os::raw::c_int {
        let statements = unsafe { &mut *context.cast::<Vec<String>>() };
        let sql = unsafe { std::ffi::CStr::from_ptr(sql.cast()) };
        statements.push(sql.to_string_lossy().into_owned());
        0
    }

    fn start_sql_trace(conn: &Connection, statements: &mut Vec<String>) {
        let result = unsafe {
            rusqlite::ffi::sqlite3_trace_v2(
                conn.handle(),
                rusqlite::ffi::SQLITE_TRACE_STMT as u32,
                Some(capture_sql_statement),
                (statements as *mut Vec<String>).cast(),
            )
        };
        assert_eq!(result, rusqlite::ffi::SQLITE_OK);
    }

    fn stop_sql_trace(conn: &Connection) {
        let result = unsafe {
            rusqlite::ffi::sqlite3_trace_v2(conn.handle(), 0_u32, None, std::ptr::null_mut())
        };
        assert_eq!(result, rusqlite::ffi::SQLITE_OK);
    }

    fn usable(window: &CapacityWindow) -> CapacityObservation {
        CapacityObservation {
            provider: window.provider.clone(),
            api_provider: window.api_provider.clone(),
            account_ref: window.account_ref.clone(),
            observation_state: "usable".into(),
            error_class: None,
            error_detail: None,
            source: window.source.clone(),
            fetched_at: window.fetched_at.clone(),
        }
    }

    fn snapshot(
        windows: Vec<CapacityWindow>,
        complete_providers: Option<&[&str]>,
    ) -> CapacitySnapshot {
        let mut observations = Vec::new();
        for window in &windows {
            if !observations
                .iter()
                .any(|observation: &CapacityObservation| {
                    observation.provider == window.provider
                        && observation.account_ref == window.account_ref
                })
            {
                observations.push(usable(window));
            }
        }
        CapacitySnapshot {
            windows,
            observations,
            provider_readings: Vec::new(),
            complete_providers: complete_providers.map(|providers| {
                providers
                    .iter()
                    .map(|provider| (*provider).to_string())
                    .collect()
            }),
            degraded_entries: 0,
        }
    }

    #[test]
    fn apply_then_read_roundtrips_complete_pool_identity() {
        let mut conn = conn();
        let mut extra = win("codex", "account-a", "extra:7:model-a", 42.0);
        extra.slot_kind = "extra".into();
        extra.external_id = Some("model-a".into());
        extra.title = Some("Model A".into());
        extra.window_minutes = Some(300);
        extra.used_count = Some(10_336);
        extra.total_count = Some(40_000);
        extra.fetched_at = Some("2026-08-13T12:00:00Z".into());
        extra.stale_since = Some("2026-08-13T12:01:00Z".into());
        extra.stale_class = Some("upstream_unavailable".into());
        apply_snapshot(&mut conn, &snapshot(vec![extra.clone()], None), 1_000).unwrap();

        let stored = windows(&conn).unwrap();
        assert_eq!(stored, vec![extra]);
        let runtime = freshness(&conn).unwrap();
        assert_eq!(runtime.runtime_variant, "active");
        assert_eq!(runtime.last_success_ms, Some(1_000));
    }

    #[test]
    fn usable_account_replacement_drops_only_its_vanished_pools() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![
                    win("codex", "account-a", "primary", 30.0),
                    win("codex", "account-a", "secondary", 60.0),
                    win("codex", "account-b", "primary", 70.0),
                ],
                None,
            ),
            1_000,
        )
        .unwrap();
        apply_snapshot(
            &mut conn,
            &snapshot(vec![win("codex", "account-a", "primary", 35.0)], None),
            2_000,
        )
        .unwrap();

        let stored = windows(&conn).unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored
            .iter()
            .any(|window| window.account_ref == "account-b"));
        assert!(!stored.iter().any(|window| window.pool_key == "secondary"));
    }

    #[test]
    fn provider_omission_has_no_deletion_authority() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![
                    win("codex", "account-a", "primary", 30.0),
                    win("claude", "account-b", "primary", 40.0),
                ],
                None,
            ),
            1_000,
        )
        .unwrap();
        apply_snapshot(
            &mut conn,
            &snapshot(vec![win("codex", "account-a", "primary", 33.0)], None),
            2_000,
        )
        .unwrap();

        assert!(windows(&conn)
            .unwrap()
            .iter()
            .any(|window| window.provider == "claude"));
    }

    #[test]
    fn complete_provider_retirement_uses_constant_delete_statements() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![
                    win("codex", "keep", "primary", 30.0),
                    win("codex", "retire-a", "primary", 40.0),
                    win("codex", "retire-b", "primary", 50.0),
                    win("codex", "retire-c", "primary", 60.0),
                ],
                None,
            ),
            1_000,
        )
        .unwrap();

        let mut statements = Vec::new();
        start_sql_trace(&conn, &mut statements);
        apply_snapshot(
            &mut conn,
            &CapacitySnapshot {
                windows: Vec::new(),
                observations: vec![CapacityObservation {
                    provider: "codex".into(),
                    api_provider: None,
                    account_ref: "keep".into(),
                    observation_state: "degraded".into(),
                    error_class: Some("future_failure".into()),
                    error_detail: None,
                    source: None,
                    fetched_at: None,
                }],
                provider_readings: Vec::new(),
                complete_providers: Some(BTreeSet::from(["codex".to_string()])),
                degraded_entries: 1,
            },
            2_000,
        )
        .unwrap();
        stop_sql_trace(&conn);

        let retirement_deletes = statements
            .iter()
            .filter(|sql| {
                sql.starts_with("DELETE FROM capacity_window")
                    || sql.starts_with("DELETE FROM capacity_observation")
            })
            .count();
        assert_eq!(
            retirement_deletes, 2,
            "retiring three absent accounts should use one delete per cached table"
        );
        let stored = windows(&conn).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].account_ref, "keep");
    }

    #[test]
    fn complete_provider_deletes_only_absent_labeled_accounts() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![
                    win("codex", "account-a", "primary", 30.0),
                    win("codex", "account-b", "primary", 40.0),
                ],
                None,
            ),
            1_000,
        )
        .unwrap();
        let degraded = CapacityObservation {
            provider: "codex".into(),
            api_provider: None,
            account_ref: "account-a".into(),
            observation_state: "degraded".into(),
            error_class: Some("future_failure".into()),
            error_detail: Some("still represented".into()),
            source: None,
            fetched_at: None,
        };
        apply_snapshot(
            &mut conn,
            &CapacitySnapshot {
                windows: Vec::new(),
                observations: vec![degraded],
                provider_readings: Vec::new(),
                complete_providers: Some(BTreeSet::from(["codex".to_string()])),
                degraded_entries: 1,
            },
            2_000,
        )
        .unwrap();

        let stored = windows(&conn).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].account_ref, "account-a");
        let retired: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM capacity_observation WHERE account_ref='account-b'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            retired, 0,
            "completion must remove the absent labeled identity"
        );
    }

    #[test]
    fn complete_provider_retirement_is_labeled_only() {
        let mut conn = conn();
        let reading = CapacityProviderReading {
            credential_ref: None,
            provider: "codex".into(),
            api_provider: Some("openai".into()),
            pool_key: "primary".into(),
            slot_kind: "primary".into(),
            external_id: None,
            title: None,
            extra_ordinal: None,
            used_percent: 20.0,
            raw_used_percent: None,
            resets_at: None,
            window_minutes: Some(300),
            used_count: Some(20),
            total_count: Some(100),
            source: None,
            fetched_at: None,
        };
        apply_snapshot(
            &mut conn,
            &CapacitySnapshot {
                windows: Vec::new(),
                observations: Vec::new(),
                provider_readings: vec![reading],
                complete_providers: None,
                degraded_entries: 0,
            },
            1_000,
        )
        .unwrap();
        apply_snapshot(
            &mut conn,
            &CapacitySnapshot {
                windows: Vec::new(),
                observations: Vec::new(),
                provider_readings: Vec::new(),
                complete_providers: Some(BTreeSet::from(["codex".to_string()])),
                degraded_entries: 0,
            },
            2_000,
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM capacity_provider_reading",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "labeled completion cannot retire provider-current state"
        );
    }

    #[test]
    fn failed_attempt_preserves_last_known_snapshot() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(vec![win("codex", "account-a", "primary", 30.0)], None),
            1_000,
        )
        .unwrap();
        record_failed_attempt(&conn, 2_000, "connect timeout").unwrap();

        assert_eq!(windows(&conn).unwrap().len(), 1);
        let runtime = freshness(&conn).unwrap();
        assert_eq!(runtime.last_success_ms, Some(1_000));
        assert_eq!(runtime.last_attempt_ms, Some(2_000));
        assert_eq!(runtime.detail.as_deref(), Some("connect timeout"));
    }

    #[test]
    fn valid_no_usable_snapshot_does_not_advance_success() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &CapacitySnapshot {
                windows: Vec::new(),
                observations: vec![CapacityObservation {
                    provider: "codex".into(),
                    api_provider: None,
                    account_ref: "account-a".into(),
                    observation_state: "degraded".into(),
                    error_class: Some("unknown_future_class".into()),
                    error_detail: None,
                    source: None,
                    fetched_at: None,
                }],
                provider_readings: Vec::new(),
                complete_providers: None,
                degraded_entries: 1,
            },
            1_000,
        )
        .unwrap();

        let runtime = freshness(&conn).unwrap();
        assert_eq!(runtime.runtime_variant, "configured_unavailable");
        assert_eq!(runtime.last_success_ms, None);
        assert_eq!(runtime.last_attempt_ms, Some(1_000));
        let class: String = conn
            .query_row(
                "SELECT error_class FROM capacity_observation WHERE account_ref='account-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(class, "unknown_future_class");
    }

    #[test]
    fn disabled_runtime_rejects_snapshot_without_effect() {
        let mut conn = conn();
        conn.execute(
            "UPDATE capacity_runtime SET runtime_variant='disabled', changed_at_ms=5,
             enabled_at_ms=NULL,last_attempt_ms=NULL,last_success_ms=NULL,degraded_entries=0,detail=NULL",
            [],
        )
        .unwrap();
        let result = apply_snapshot(
            &mut conn,
            &snapshot(vec![win("codex", "account-a", "primary", 30.0)], None),
            1_000,
        );
        assert!(result.is_err());
        assert_eq!(windows(&conn).unwrap().len(), 0);
    }

    #[test]
    fn unknown_runtime_variant_rejects_snapshot_without_advancing_attempt() {
        let mut conn = conn();
        conn.execute_batch("PRAGMA ignore_check_constraints=ON;")
            .unwrap();
        conn.execute(
            "UPDATE capacity_runtime SET runtime_variant='unknown_future_state' WHERE id=1",
            [],
        )
        .unwrap();

        assert!(apply_snapshot(&mut conn, &snapshot(Vec::new(), None), 1_000).is_err());

        let runtime = freshness(&conn).unwrap();
        assert_eq!(runtime.runtime_variant, "unknown_future_state");
        assert_eq!(runtime.last_attempt_ms, None);
    }

    #[test]
    fn capacity_queries_use_v5_pool_identity_and_runtime_tables() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(vec![win("codex", "account-a", "primary", 30.0)], None),
            1_000,
        )
        .unwrap();
        assert_eq!(windows(&conn).unwrap()[0].pool_key, "primary");
        assert!(conn.prepare("SELECT pool FROM capacity_window").is_err());
        assert!(conn
            .prepare("SELECT last_success_ms FROM capacity_fetch")
            .is_err());
        assert_eq!(freshness(&conn).unwrap().last_success_ms, Some(1_000));
    }

    #[test]
    fn staleness_uses_durable_cadence_and_strict_boundary() {
        let active = CapacityFreshness {
            runtime_variant: "active".into(),
            effective_cadence_ms: 10_000,
            changed_at_ms: None,
            enabled_at_ms: Some(1),
            last_success_ms: Some(100_000),
            last_attempt_ms: Some(100_000),
            degraded_entries: 0,
            detail: None,
        };
        let horizon = active.effective_cadence_ms * 4;
        assert!(
            !is_stale(&active, 140_000, horizon),
            "horizon equality is fresh"
        );
        assert!(is_stale(&active, 140_001, horizon));

        let disabled = CapacityFreshness {
            runtime_variant: "disabled".into(),
            effective_cadence_ms: 1,
            changed_at_ms: Some(0),
            enabled_at_ms: None,
            last_success_ms: None,
            last_attempt_ms: None,
            degraded_entries: 0,
            detail: None,
        };
        assert!(!is_stale(&disabled, i64::MAX, 4));
    }

    #[test]
    fn windows_reads_every_account_in_provider_account_order() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![
                    win("codex", "account-a", "primary", 30.0),
                    win("claude", "account-b", "primary", 40.0),
                ],
                None,
            ),
            1_000,
        )
        .unwrap();
        let selected = windows(&conn).unwrap();
        let providers: Vec<&str> = selected
            .iter()
            .map(|window| window.provider.as_str())
            .collect();
        assert_eq!(providers, ["claude", "codex"]);
    }

    #[test]
    fn window_sample_same_fetched_at_repoll_dedupes_to_one_row() {
        let mut conn = conn();
        let first = win_at(
            "codex",
            "account-a",
            "primary",
            30.0,
            "2026-08-13T16:03:50Z",
        );
        apply_snapshot(&mut conn, &snapshot(vec![first], None), 1_000).unwrap();
        let repoll = win_at(
            "codex",
            "account-a",
            "primary",
            30.0,
            "2026-08-13T16:03:50Z",
        );
        apply_snapshot(&mut conn, &snapshot(vec![repoll], None), 2_000).unwrap();
        assert_eq!(sample_count(&conn), 1);
    }

    #[test]
    fn window_sample_check_violations_are_not_ignored() {
        let mut conn = conn();
        let mut invalid = win_at(
            "codex",
            "account-a",
            "primary",
            30.0,
            "2026-08-13T16:03:50Z",
        );
        invalid.breakdown = Some("not json".into());

        let result = apply_snapshot(&mut conn, &snapshot(vec![invalid], None), 1_000);

        assert!(
            result.is_err(),
            "invalid history breakdown must surface its CHECK failure"
        );
        assert_eq!(sample_count(&conn), 0);
    }

    #[test]
    fn window_sample_not_null_violations_are_not_ignored() {
        let mut conn = conn();
        conn.execute_batch(
            "DROP TABLE window_sample;
             CREATE TABLE window_sample (
                 provider TEXT NOT NULL,
                 api_provider TEXT,
                 account_ref TEXT NOT NULL,
                 pool_key TEXT NOT NULL,
                 fetched_at_instant INTEGER NOT NULL,
                 used_percent REAL NOT NULL,
                 raw_used_percent REAL,
                 resets_at TEXT,
                 window_minutes INTEGER,
                 source TEXT NOT NULL,
                 breakdown TEXT,
                 PRIMARY KEY (provider, account_ref, pool_key, fetched_at_instant)
             );",
        )
        .unwrap();
        let invalid = win_at(
            "codex",
            "account-a",
            "primary",
            30.0,
            "2026-08-13T16:03:50Z",
        );

        let result = apply_snapshot(&mut conn, &snapshot(vec![invalid], None), 1_000);

        assert!(
            result.is_err(),
            "NULL history source must surface its NOT NULL failure"
        );
        assert!(windows(&conn).unwrap().is_empty());
    }

    #[test]
    fn window_sample_unlabeled_entry_creates_no_row() {
        let mut conn = conn();
        let reading = CapacityProviderReading {
            credential_ref: None,
            provider: "codex".into(),
            api_provider: None,
            pool_key: "primary".into(),
            slot_kind: "primary".into(),
            external_id: None,
            title: None,
            extra_ordinal: None,
            used_percent: 20.0,
            raw_used_percent: None,
            resets_at: None,
            window_minutes: Some(300),
            used_count: None,
            total_count: None,
            source: None,
            fetched_at: Some("2026-08-13T16:03:50Z".into()),
        };
        apply_snapshot(
            &mut conn,
            &CapacitySnapshot {
                windows: Vec::new(),
                observations: Vec::new(),
                provider_readings: vec![reading],
                complete_providers: None,
                degraded_entries: 0,
            },
            1_000,
        )
        .unwrap();
        assert_eq!(sample_count(&conn), 0);
    }

    #[test]
    fn window_sample_entry_without_fetched_at_creates_no_sample_row() {
        let mut conn = conn();
        let window = win("codex", "account-a", "primary", 30.0);
        apply_snapshot(&mut conn, &snapshot(vec![window], None), 1_000).unwrap();

        assert_eq!(
            windows(&conn).unwrap().len(),
            1,
            "current-state row still lands without a fetchedAt"
        );
        assert_eq!(
            sample_count(&conn),
            0,
            "an undateable reading must not enter the append-only curve"
        );
    }

    #[test]
    fn window_sample_entry_with_malformed_fetched_at_creates_no_sample_row() {
        let mut conn = conn();
        let window = win_at("codex", "account-a", "primary", 30.0, "not-a-timestamp");
        apply_snapshot(&mut conn, &snapshot(vec![window], None), 1_000).unwrap();

        assert_eq!(
            windows(&conn).unwrap().len(),
            1,
            "current-state row still lands with a malformed fetchedAt"
        );
        assert_eq!(
            sample_count(&conn),
            0,
            "an unparseable fetchedAt must not enter the append-only curve"
        );
    }

    #[test]
    fn window_sample_distinct_fetched_at_appends_second_row() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![win_at(
                    "codex",
                    "account-a",
                    "primary",
                    30.0,
                    "2026-08-13T16:03:50Z",
                )],
                None,
            ),
            1_000,
        )
        .unwrap();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![win_at(
                    "codex",
                    "account-a",
                    "primary",
                    40.0,
                    "2026-08-13T16:04:50Z",
                )],
                None,
            ),
            2_000,
        )
        .unwrap();
        assert_eq!(sample_count(&conn), 2);
    }

    #[test]
    fn window_sample_fractional_second_spellings_dedupe_to_same_instant() {
        let mut conn = conn();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![win_at(
                    "codex",
                    "account-a",
                    "primary",
                    30.0,
                    "2026-08-13T16:03:50.098805+00:00",
                )],
                None,
            ),
            1_000,
        )
        .unwrap();
        apply_snapshot(
            &mut conn,
            &snapshot(
                vec![win_at(
                    "codex",
                    "account-a",
                    "primary",
                    30.0,
                    "2026-08-13T16:03:50.098805000+00:00",
                )],
                None,
            ),
            2_000,
        )
        .unwrap();
        assert_eq!(sample_count(&conn), 1);
    }

    #[test]
    fn window_sample_roundtrips_api_provider_for_canonical_join() {
        let mut conn = conn();
        let mut window = win_at(
            "claude",
            "account-a",
            "primary",
            30.0,
            "2026-08-13T16:03:50Z",
        );
        window.api_provider = Some("anthropic".into());
        apply_snapshot(&mut conn, &snapshot(vec![window], None), 1_000).unwrap();

        let (provider, api_provider): (String, Option<String>) = conn
            .query_row(
                "SELECT provider, api_provider FROM window_sample WHERE provider='claude'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(provider, "claude");
        assert_eq!(
            api_provider.as_deref(),
            Some("anthropic"),
            "the canonical-provider axis must be written so curve readers can join historically"
        );
    }

    #[test]
    fn window_sample_breakdown_roundtrips_json_verbatim() {
        let mut conn = conn();
        let mut window = win_at(
            "claude",
            "account-a",
            "secondary",
            45.0,
            "2026-09-28T12:00:00Z",
        );
        let breakdown_json = r#"{"asOf":"2026-09-28T11:55:00Z","windowStartedAt":"2026-09-21T12:00:00Z","rows":[{"key":"claude_code","sharePercent":100.0}]}"#;
        window.breakdown = Some(breakdown_json.to_string());
        apply_snapshot(&mut conn, &snapshot(vec![window], None), 1_000).unwrap();

        let stored: Option<String> = conn
            .query_row(
                "SELECT breakdown FROM window_sample WHERE provider='claude' AND pool_key='secondary'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored.as_deref(), Some(breakdown_json));
    }
}
