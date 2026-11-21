//! Recorded quota readings, grouped into UTC buckets without estimating usage.

use rusqlite::{params, Connection, OptionalExtension};

use crate::multiplier::clean::{reset_tolerance_ms, RESET_TOLERANCE_MS};
use serde::{Deserialize, Serialize};

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;
const NANOS_PER_MS: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Grain {
    Hour,
    Day,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaHistoryRequest {
    pub from_ms: i64,
    pub to_ms: i64,
    pub grain: Grain,
    pub quota_providers: Option<Vec<String>>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct QuotaHistory {
    pub windows: Vec<Window>,
    pub earliest_sample_ms: Option<i64>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Window {
    pub quota_provider: String,
    pub account: Option<String>,
    pub credential: Option<String>,
    pub pool: String,
    /// The duration recorded on the last sample in the selected range.
    pub window_minutes: Option<i64>,
    pub buckets: Vec<Bucket>,
    pub resets: Vec<Reset>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Bucket {
    pub start_ms: i64,
    pub used_percent_max: f64,
    pub samples: u64,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Reset {
    pub at_ms: i64,
    pub from_percent: f64,
    pub to_percent: f64,
}

#[derive(Debug)]
pub enum HistoryError {
    InvalidRange,
    RangeTooLarge,
    InvalidFilter,
    Store(rusqlite::Error),
}

impl HistoryError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRange => "invalid_range",
            Self::RangeTooLarge => "range_too_large",
            Self::InvalidFilter => "invalid_filter",
            Self::Store(_) => "internal",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::InvalidRange => "from_ms must be less than to_ms".into(),
            Self::RangeTooLarge => "quota history range, widened to whole UTC buckets, exceeds 31 days for hour grain or 400 days for day grain; align the range to the grain or shorten it".into(),
            Self::InvalidFilter => "quota_providers must not be empty".into(),
            Self::Store(error) => format!("quota history: {error}"),
        }
    }
}

impl From<rusqlite::Error> for HistoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

pub fn quota_history(
    conn: &Connection,
    request: &QuotaHistoryRequest,
) -> Result<QuotaHistory, HistoryError> {
    let (grain_ms, days) = match request.grain {
        Grain::Hour => (HOUR_MS, 31),
        Grain::Day => (DAY_MS, 400),
    };
    if i128::from(request.to_ms) <= i128::from(request.from_ms) {
        return Err(HistoryError::InvalidRange);
    }
    if request.quota_providers.as_ref().is_some_and(Vec::is_empty) {
        return Err(HistoryError::InvalidFilter);
    }
    // Use wider arithmetic before clamping to the store's nanosecond domain:
    // valid millisecond ranges can lie wholly outside that domain.
    let grain = i128::from(grain_ms);
    let from_ms = i128::from(request.from_ms).div_euclid(grain) * grain;
    let to_ms = (i128::from(request.to_ms) - 1).div_euclid(grain) * grain + grain;
    // The cap applies to the widened range, which is the work the read does:
    // a 31-day hourly range that doesn't start on the hour widens to 745 hours
    // and is refused, so an hourly reply never exceeds 744 buckets.
    if to_ms - from_ms > i128::from(days * DAY_MS) {
        return Err(HistoryError::RangeTooLarge);
    }
    let from = from_ms * i128::from(NANOS_PER_MS);
    let to = to_ms * i128::from(NANOS_PER_MS);
    let intersects = from <= i128::from(i64::MAX) && to > i128::from(i64::MIN);
    let start = clamp_instant(from);
    // An inclusive upper bound retains the final representable nanosecond
    // when the requested exclusive bucket edge lies beyond SQLite's integer.
    let end = clamp_instant(to - 1);
    let mut report = QuotaHistory {
        windows: Vec::new(),
        earliest_sample_ms: None,
    };
    for credential in [false, true] {
        let (table, field) = sample_table(credential);
        let mut next_pool = conn.prepare_cached(&next_series_sql(credential, NextSeries::Pool))?;
        let mut next_identity =
            conn.prepare_cached(&next_series_sql(credential, NextSeries::Identity))?;
        let mut next_provider =
            conn.prepare_cached(&next_series_sql(credential, NextSeries::Provider))?;
        let mut first = conn.prepare_cached(&first_sample_sql(credential))?;
        let mut current = conn.query_row(
            &format!("SELECT provider,{field},pool_key FROM {table} ORDER BY provider,{field},pool_key LIMIT 1"),
            [],
            series_key,
        ).optional()?;
        while let Some((provider, identity, pool)) = current {
            if request
                .quota_providers
                .as_ref()
                .is_none_or(|allowed| allowed.contains(&provider))
            {
                let earliest: i64 =
                    first.query_row(params![provider, identity, pool], |row| row.get(0))?;
                let earliest_ms = earliest.div_euclid(NANOS_PER_MS);
                report.earliest_sample_ms = Some(
                    report
                        .earliest_sample_ms
                        .map_or(earliest_ms, |old| old.min(earliest_ms)),
                );
                if intersects {
                    let window = read_window(
                        conn,
                        credential,
                        &provider,
                        &identity,
                        &pool,
                        (start, end),
                        grain_ms,
                    )?;
                    if !window.buckets.is_empty() {
                        report.windows.push(window);
                    }
                }
            }
            current = next_pool
                .query_row(params![provider, identity, pool], series_key)
                .optional()?;
            if current.is_none() {
                current = next_identity
                    .query_row(params![provider, identity], series_key)
                    .optional()?;
            }
            if current.is_none() {
                current = next_provider.query_row([provider], series_key).optional()?;
            }
        }
    }
    report.windows.sort_by(|a, b| {
        (
            &a.quota_provider,
            a.account.is_none(),
            &a.account,
            &a.credential,
            &a.pool,
        )
            .cmp(&(
                &b.quota_provider,
                b.account.is_none(),
                &b.account,
                &b.credential,
                &b.pool,
            ))
    });
    Ok(report)
}

fn clamp_instant(value: i128) -> i64 {
    i64::try_from(value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)))
        .expect("clamped instant fits i64")
}

fn sample_table(credential: bool) -> (&'static str, &'static str) {
    if credential {
        ("credential_window_sample", "credential_ref")
    } else {
        ("window_sample", "account_ref")
    }
}

fn series_key(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, String)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}

enum NextSeries {
    Pool,
    Identity,
    Provider,
}

// DISTINCT and a tuple comparison can step through every reading of an equal
// key prefix. Seek each key component separately so the index skips the whole
// previous series, including its oldest and newest readings.
fn next_series_sql(credential: bool, next: NextSeries) -> String {
    let (table, field) = sample_table(credential);
    let predicate = match next {
        NextSeries::Pool => format!("provider=?1 AND {field}=?2 AND pool_key>?3"),
        NextSeries::Identity => format!("provider=?1 AND {field}>?2"),
        NextSeries::Provider => "provider>?1".into(),
    };
    format!(
        "SELECT provider,{field},pool_key FROM {table}
             WHERE {predicate}
             ORDER BY provider,{field},pool_key LIMIT 1"
    )
}

fn first_sample_sql(credential: bool) -> String {
    let (table, field) = sample_table(credential);
    format!(
        "SELECT fetched_at_instant FROM {table}
             WHERE provider=?1 AND {field}=?2 AND pool_key=?3
             ORDER BY fetched_at_instant LIMIT 1"
    )
}

fn predecessor_sql(credential: bool) -> String {
    let (table, field) = sample_table(credential);
    format!(
        "SELECT COALESCE(raw_used_percent,used_percent) FROM {table}
             WHERE provider=?1 AND {field}=?2 AND pool_key=?3 AND fetched_at_instant<?4
             ORDER BY fetched_at_instant DESC LIMIT 1"
    )
}

fn samples_sql(credential: bool) -> String {
    let (table, field) = sample_table(credential);
    let reset = reset_lookup_sql(credential);
    format!(
        "SELECT fetched_at_instant,COALESCE(raw_used_percent,used_percent),window_minutes,{reset}
         FROM {table} s WHERE provider=?1 AND {field}=?2 AND pool_key=?3
         AND fetched_at_instant BETWEEN ?4 AND ?5 ORDER BY fetched_at_instant"
    )
}

fn reset_lookup_sql(credential: bool) -> String {
    let (_, field) = sample_table(credential);
    format!(
        "(SELECT r.reset_ms FROM quota_reset_samples r WHERE r.credential={}
        AND r.provider=s.provider AND r.identity=s.{field} AND r.pool_key=s.pool_key
        AND r.fetched_at_instant=s.fetched_at_instant)",
        i64::from(credential)
    )
}

fn previous_accepted(
    conn: &Connection,
    credential: bool,
    key: (&str, &str, &str),
    start: i64,
    period: Option<(i64, i64)>,
) -> rusqlite::Result<Option<f64>> {
    let (provider, identity, pool) = key;
    let Some((horizon, first)) = period else {
        return conn
            .prepare_cached(&predecessor_sql(credential))?
            .query_row(params![provider, identity, pool, start], |row| row.get(0))
            .optional();
    };
    // Seek on the raw time index first. Ordering the UNION view itself would
    // sort every sample of the period before returning the first predecessor.
    let (table, field) = sample_table(credential);
    let reset = reset_lookup_sql(credential);
    let mut statement = conn.prepare_cached(&format!(
        "SELECT COALESCE(raw_used_percent,used_percent),window_minutes,{reset} FROM {table} s
         WHERE provider=?1 AND {field}=?2 AND pool_key=?3
           AND fetched_at_instant<?4 AND fetched_at_instant>=?5 ORDER BY fetched_at_instant DESC"
    ))?;
    let mut rows = statement.query(params![provider, identity, pool, start, first])?;
    while let Some(row) = rows.next()? {
        let minutes: Option<i64> = row.get(1)?;
        let reset: Option<i64> = row.get(2)?;
        if reset.is_none_or(|reset| {
            horizon.saturating_sub(reset) <= minutes.map_or(RESET_TOLERANCE_MS, reset_tolerance_ms)
        }) {
            return row.get(0).map(Some);
        }
    }
    Ok(None)
}

fn read_window(
    conn: &Connection,
    credential: bool,
    provider: &str,
    identity: &str,
    pool: &str,
    (start, end): (i64, i64),
    grain_ms: i64,
) -> rusqlite::Result<Window> {
    let period = conn
        .prepare_cached(
            "SELECT reset_ms,first_fetched FROM quota_reset_period
        WHERE credential=?1 AND provider=?2 AND identity=?3 AND pool_key=?4 AND first_fetched<?5
        ORDER BY reset_ms DESC LIMIT 1",
        )?
        .query_row(
            params![i64::from(credential), provider, identity, pool, start],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let mut previous =
        previous_accepted(conn, credential, (provider, identity, pool), start, period)?;
    // The latest reset time seen so far: the current allowance period's end.
    let mut horizon = period.map(|(reset, _)| reset);
    let mut statement = conn.prepare_cached(&samples_sql(credential))?;
    let mut rows = statement.query(params![provider, identity, pool, start, end])?;
    let mut window = Window {
        quota_provider: provider.into(),
        account: (!credential).then(|| identity.into()),
        credential: credential.then(|| identity.into()),
        pool: pool.into(),
        window_minutes: None,
        buckets: Vec::new(),
        resets: Vec::new(),
    };
    while let Some(row) = rows.next()? {
        let at_ns: i64 = row.get(0)?;
        let used: f64 = row.get(1)?;
        let at_ms = at_ns.div_euclid(NANOS_PER_MS);
        let start_ms = at_ms.div_euclid(grain_ms) * grain_ms;
        let minutes: Option<i64> = row.get(2)?;
        let reset = row.get(3)?;
        let tolerance = minutes.map_or(RESET_TOLERANCE_MS, reset_tolerance_ms);
        // A reading whose reset time is older than the current period's is a
        // provider's cached answer from the previous allowance. The subscription
        // multiplier skips it, and so does this history, so it can't draw a
        // spike back to the old period's usage.
        if let (Some(latest), Some(reset)) = (horizon, reset) {
            if latest.saturating_sub(reset) > tolerance {
                continue;
            }
        }
        window.window_minutes = minutes;
        if let Some(bucket) = window
            .buckets
            .last_mut()
            .filter(|bucket| bucket.start_ms == start_ms)
        {
            bucket.used_percent_max = bucket.used_percent_max.max(used);
            bucket.samples += 1;
        } else {
            window.buckets.push(Bucket {
                start_ms,
                used_percent_max: used,
                samples: 1,
            });
        }
        // A reset is the window's reset time moving forward into a new
        // allowance period, the boundary the subscription multiplier uses. A
        // drop in the percent alone is not one: readings from two sources can
        // differ by a point and alternate, and providers re-state the same reset
        // time with seconds of jitter, which the tolerance absorbs. A reading
        // with no reset time can't start a period.
        if let (Some(old), Some(new)) = (horizon, reset) {
            if new.saturating_sub(old) > tolerance {
                if let Some(from_percent) = previous {
                    window.resets.push(Reset {
                        at_ms,
                        from_percent,
                        to_percent: used,
                    });
                }
            }
        }
        if let Some(reset) = reset {
            horizon = Some(horizon.map_or(reset, |latest| latest.max(reset)));
        }
        previous = Some(used);
    }
    Ok(window)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use serde_json::json;

    const PERIOD_ONE: Option<&str> = Some("2026-11-01T00:00:00Z");
    const PERIOD_TWO: Option<&str> = Some("2026-11-01T05:00:00Z");
    const PERIOD_THREE: Option<&str> = Some("2026-11-01T10:00:00Z");

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::store::initialize_connection(&conn).unwrap();
        for migration in crate::store::MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn sample(
        conn: &Connection,
        credential: bool,
        identity: &str,
        at_ms: i64,
        used: f64,
        raw: Option<f64>,
        resets_at: Option<&str>,
    ) {
        let (table, field) = if credential {
            ("credential_window_sample", "credential_ref")
        } else {
            ("window_sample", "account_ref")
        };
        conn.execute(
            &format!("INSERT INTO {table} (provider,{field},pool_key,fetched_at_instant,used_percent,raw_used_percent,window_minutes,resets_at) VALUES ('claude',?1,'primary',?2,?3,?4,300,?5)"),
            params![identity, at_ms * NANOS_PER_MS, used, raw, resets_at],
        ).unwrap();
    }

    fn request(from_ms: i64, to_ms: i64, grain: &str) -> QuotaHistoryRequest {
        serde_json::from_value(json!({"from_ms":from_ms,"to_ms":to_ms,"grain":grain})).unwrap()
    }

    #[test]
    fn quota_history_buckets_raw_percent_predecessor_resets_and_credentials() {
        let conn = conn();
        sample(&conn, false, "account", -1, 97.0, Some(197.0), PERIOD_ONE);
        sample(&conn, false, "account", 0, 3.0, Some(103.0), PERIOD_TWO);
        sample(
            &conn,
            false,
            "account",
            HOUR_MS / 2,
            8.0,
            Some(108.0),
            PERIOD_TWO,
        );
        sample(&conn, false, "account", HOUR_MS, 8.0, None, PERIOD_THREE);
        sample(
            &conn,
            false,
            "account",
            2 * HOUR_MS,
            99.0,
            None,
            PERIOD_THREE,
        );
        sample(&conn, true, "account", 0, 42.0, None, PERIOD_ONE);
        let report = quota_history(&conn, &request(1, HOUR_MS + 1, "hour")).unwrap();
        assert_eq!(
            serde_json::to_value(report).unwrap(),
            json!({
                "earliest_sample_ms": -1,
                "windows": [
                    {"quota_provider":"claude","account":"account","credential":null,"pool":"primary","window_minutes":300,
                     "buckets":[{"start_ms":0,"used_percent_max":108.0,"samples":2},{"start_ms":HOUR_MS,"used_percent_max":8.0,"samples":1}],
                     "resets":[{"at_ms":0,"from_percent":197.0,"to_percent":103.0},{"at_ms":HOUR_MS,"from_percent":108.0,"to_percent":8.0}]},
                    {"quota_provider":"claude","account":null,"credential":"account","pool":"primary","window_minutes":300,
                     "buckets":[{"start_ms":0,"used_percent_max":42.0,"samples":1}],"resets":[]}
                ]
            })
        );
    }

    #[test]
    fn ledger_regression_quota_predecessor_does_not_restore_a_stale_period() {
        for credential in [false, true] {
            let conn = conn();
            for (at, used, reset) in [
                (0, 91.0, PERIOD_ONE),
                (HOUR_MS / 2, 4.0, PERIOD_TWO),
                (HOUR_MS - 1, 91.0, PERIOD_ONE),
                (HOUR_MS, 92.0, PERIOD_ONE),
                (HOUR_MS + 1, 5.0, PERIOD_TWO),
            ] {
                sample(&conn, credential, "account", at, used, None, reset);
            }
            let wide = quota_history(&conn, &request(0, 2 * HOUR_MS, "hour")).unwrap();
            let narrow = quota_history(&conn, &request(HOUR_MS, 2 * HOUR_MS, "hour")).unwrap();
            assert_eq!(narrow.windows[0].buckets, wide.windows[0].buckets[1..]);
            assert_eq!(
                narrow.windows[0].buckets,
                [Bucket {
                    start_ms: HOUR_MS,
                    used_percent_max: 5.0,
                    samples: 1
                }]
            );
            assert!(narrow.windows[0].resets.is_empty());
        }
    }

    #[test]
    fn quota_reset_projection_backfill_normalizes_and_guards_periods() {
        let conn = Connection::open_in_memory().unwrap();
        crate::store::initialize_connection(&conn).unwrap();
        let (previous, migration) = crate::store::migration_parts(63);
        for migration in previous {
            conn.execute_batch(migration.statements).unwrap();
        }
        for credential in [false, true] {
            let (table, field) = sample_table(credential);
            conn.execute_batch(&format!("WITH RECURSIVE n(i) AS (SELECT 200 UNION ALL SELECT i+1 FROM n WHERE i<10199)
                INSERT INTO {table}(provider,{field},pool_key,fetched_at_instant,used_percent,resets_at,window_minutes)
                SELECT 'claude','account','primary', i*{NANOS_PER_MS},80,'1970-01-02T23:59:00.0009+23:59',300 FROM n")).unwrap();
        }
        let started = std::time::Instant::now();
        conn.execute_batch(migration.statements).unwrap();
        let count = conn
            .query_row("SELECT count(*) FROM quota_reset_period", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap();
        eprintln!(
            "quota projection backfill: 20,000 synthetic samples -> {count} period rows in {:?}",
            started.elapsed()
        );
        assert_eq!(count, 2);
        for credential in [false, true] {
            sample(
                &conn,
                credential,
                "account",
                100,
                79.0,
                None,
                Some("1970-01-02t00:00:00z"),
            );
            sample(
                &conn,
                credential,
                "account",
                50,
                90.0,
                None,
                Some("2026-02-30T00:00:00Z"),
            );
            sample(
                &conn,
                credential,
                "account",
                70,
                90.0,
                None,
                Some("bad reset"),
            );
            let lookup = || {
                conn.query_row(
                    "SELECT reset_ms,first_fetched FROM quota_reset_period WHERE credential=?1",
                    [i64::from(credential)],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )
                .unwrap()
            };
            assert_eq!(lookup(), (DAY_MS, 100 * NANOS_PER_MS));
            assert_eq!(
                previous_accepted(
                    &conn,
                    credential,
                    ("claude", "account", "primary"),
                    150 * NANOS_PER_MS,
                    Some(lookup())
                )
                .unwrap(),
                Some(79.0)
            );
        }
        for sql in [
            "DELETE FROM quota_reset_period",
            "UPDATE quota_reset_period SET first_fetched=first_fetched+1",
            "UPDATE quota_reset_period SET reset_ms=reset_ms+1",
            "INSERT OR REPLACE INTO quota_reset_period SELECT * FROM quota_reset_period LIMIT 1",
        ] {
            assert!(conn.execute(sql, []).is_err(), "guard accepted {sql}");
        }
        assert_eq!(
            conn.query_row("SELECT count(*) FROM quota_reset_period", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn quota_reset_projection_read_cost_follows_periods_not_samples() {
        let measure = |history| {
            let conn = conn();
            for index in 0..history {
                sample(
                    &conn,
                    false,
                    "account",
                    -DAY_MS + index,
                    95.0,
                    None,
                    PERIOD_ONE,
                );
                sample(
                    &conn,
                    false,
                    "account",
                    -HOUR_MS + index,
                    50.0,
                    None,
                    PERIOD_TWO,
                );
            }
            sample(&conn, false, "account", 0, 5.0, None, PERIOD_THREE);
            let (report, steps) = crate::multiplier::vm_steps(&conn, || {
                quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap()
            });
            assert_eq!(report.windows[0].resets.len(), 1);
            let count = conn
                .query_row("SELECT count(*) FROM quota_reset_period", [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap();
            assert_eq!(count, 3);
            (report, steps)
        };
        let (small, small_steps) = measure(100);
        let (large, large_steps) = measure(10_000);
        assert_eq!(small, large);
        eprintln!("quota 100/10,000 samples per period: {small_steps}/{large_steps} VM steps");
        assert!(large_steps <= small_steps + 100);
    }

    #[test]
    fn quota_history_day_rounding_filter_and_earliest_outside_range() {
        let conn = conn();
        sample(&conn, false, "old", -2 * DAY_MS, 40.0, None, PERIOD_ONE);
        sample(&conn, false, "old", -1, 30.0, None, PERIOD_TWO);
        conn.execute("INSERT INTO credential_window_sample(provider,credential_ref,pool_key,fetched_at_instant,used_percent) VALUES('codex','key','secondary',?1,25)", [(-3 * DAY_MS) * NANOS_PER_MS]).unwrap();
        let report = quota_history(&conn, &request(-1, 1, "day")).unwrap();
        assert_eq!(report.windows.len(), 1);
        assert_eq!(report.windows[0].buckets[0].start_ms, -DAY_MS);
        assert_eq!(report.windows[0].buckets[0].samples, 1);
        assert_eq!(report.windows[0].resets[0].at_ms, -1);
        assert_eq!(report.earliest_sample_ms, Some(-3 * DAY_MS));
        let mut filtered = request(0, HOUR_MS, "hour");
        filtered.quota_providers = Some(vec!["claude".into(), "claude".into()]);
        let report = quota_history(&conn, &filtered).unwrap();
        assert!(report.windows.is_empty());
        assert_eq!(report.earliest_sample_ms, Some(-2 * DAY_MS));
        filtered.quota_providers = Some(vec!["absent".into()]);
        assert_eq!(
            quota_history(&conn, &filtered).unwrap().earliest_sample_ms,
            None
        );
    }

    #[test]
    fn quota_history_reset_rule_same_time_one_point_drop_is_not_reset() {
        for credential in [false, true] {
            let conn = conn();
            sample(&conn, credential, "id", 0, 42.0, None, PERIOD_ONE);
            sample(&conn, credential, "id", HOUR_MS / 2, 41.0, None, PERIOD_ONE);
            let report = quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap();
            let window = &report.windows[0];
            assert!(window.resets.is_empty(), "{:?}", window.resets);
            assert_eq!(
                window.buckets,
                vec![Bucket {
                    start_ms: 0,
                    used_percent_max: 42.0,
                    samples: 2,
                }]
            );
        }
    }

    #[test]
    fn quota_history_reset_rule_forward_time_stamps_later_reading() {
        for credential in [false, true] {
            let conn = conn();
            for (at, used, reset) in [
                (0, 42.0, PERIOD_ONE),
                (HOUR_MS / 4, 41.0, Some("2026-11-01T00:10:00Z")),
                (HOUR_MS / 2, 43.0, Some("2026-11-01T00:20:00.001Z")),
                (3 * HOUR_MS / 4, 44.0, Some("2026-11-01T00:20:00.001Z")),
            ] {
                sample(&conn, credential, "id", at, used, None, reset);
            }
            let report = quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap();
            let window = &report.windows[0];
            assert_eq!(
                window.resets,
                vec![Reset {
                    at_ms: HOUR_MS / 2,
                    from_percent: 41.0,
                    to_percent: 43.0,
                }]
            );
            assert_eq!(
                window.buckets,
                vec![Bucket {
                    start_ms: 0,
                    used_percent_max: 44.0,
                    samples: 4,
                }]
            );
        }
    }

    #[test]
    fn quota_history_reset_rule_jitter_keeps_latest_period_time() {
        for credential in [false, true] {
            let conn = conn();
            for (at, used, reset) in [
                (0, 43.0, PERIOD_ONE),
                (HOUR_MS / 4, 42.0, Some("2026-11-01T00:04:00Z")),
                (HOUR_MS / 2, 41.0, Some("2026-10-31T23:56:00Z")),
                (3 * HOUR_MS / 4, 40.0, Some("2026-11-01T00:07:00Z")),
            ] {
                sample(&conn, credential, "id", at, used, None, reset);
            }
            let report = quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap();
            let window = &report.windows[0];
            assert!(window.resets.is_empty(), "{:?}", window.resets);
            assert_eq!(
                window.buckets,
                vec![Bucket {
                    start_ms: 0,
                    used_percent_max: 43.0,
                    samples: 4,
                }]
            );
        }
    }

    #[test]
    fn quota_history_reset_rule_cached_previous_period_is_excluded() {
        for credential in [false, true] {
            let conn = conn();
            for (at, used, reset) in [
                (0, 90.0, PERIOD_ONE),
                (HOUR_MS / 4, 4.0, PERIOD_TWO),
                (HOUR_MS, 99.0, PERIOD_ONE),
                (2 * HOUR_MS, 5.0, PERIOD_TWO),
            ] {
                sample(&conn, credential, "id", at, used, None, reset);
            }
            let report = quota_history(&conn, &request(0, 3 * HOUR_MS, "hour")).unwrap();
            let window = &report.windows[0];
            assert_eq!(
                window.buckets,
                vec![
                    Bucket {
                        start_ms: 0,
                        used_percent_max: 90.0,
                        samples: 2
                    },
                    Bucket {
                        start_ms: 2 * HOUR_MS,
                        used_percent_max: 5.0,
                        samples: 1
                    },
                ]
            );
            assert_eq!(
                window.resets,
                vec![Reset {
                    at_ms: HOUR_MS / 4,
                    from_percent: 90.0,
                    to_percent: 4.0,
                }]
            );
        }
    }

    #[test]
    fn quota_history_reset_rule_unknown_time_never_starts_period() {
        for credential in [false, true] {
            let conn = conn();
            for (at, used, reset) in [
                (0, 99.0, None),
                (HOUR_MS / 4, 5.0, PERIOD_ONE),
                (HOUR_MS / 2, 4.0, None),
                (3 * HOUR_MS / 4, 6.0, PERIOD_TWO),
            ] {
                sample(&conn, credential, "id", at, used, None, reset);
            }
            let report = quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap();
            let window = &report.windows[0];
            assert_eq!(
                window.resets,
                vec![Reset {
                    at_ms: 3 * HOUR_MS / 4,
                    from_percent: 4.0,
                    to_percent: 6.0,
                }]
            );
            assert_eq!(
                window.buckets,
                vec![Bucket {
                    start_ms: 0,
                    used_percent_max: 99.0,
                    samples: 4,
                }]
            );
        }
    }

    #[test]
    fn quota_history_reset_rule_predecessor_supplies_starting_period() {
        for credential in [false, true] {
            let conn = conn();
            sample(&conn, credential, "id", -1, 40.0, None, PERIOD_ONE);
            sample(&conn, credential, "id", 0, 41.0, None, PERIOD_TWO);
            sample(&conn, credential, "id", HOUR_MS / 2, 42.0, None, PERIOD_TWO);
            let report = quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap();
            let window = &report.windows[0];
            assert_eq!(
                window.resets,
                vec![Reset {
                    at_ms: 0,
                    from_percent: 40.0,
                    to_percent: 41.0,
                }]
            );
            assert_eq!(
                window.buckets,
                vec![Bucket {
                    start_ms: 0,
                    used_percent_max: 42.0,
                    samples: 2,
                }]
            );
            assert_eq!(report.earliest_sample_ms, Some(-1));
        }
    }

    #[test]
    fn quota_history_refuses_invalid_ranges_and_empty_filters() {
        let conn = conn();
        for (from, to, grain, code) in [
            (1, 1, "hour", "invalid_range"),
            (1, 0, "day", "invalid_range"),
            (i64::MIN, i64::MAX, "day", "range_too_large"),
            (0, 31 * DAY_MS + 1, "hour", "range_too_large"),
            (0, 400 * DAY_MS + 1, "day", "range_too_large"),
            // Exactly the cap as asked, but off the hour or day: widening
            // adds a bucket, and the widened range is what is capped.
            (1, 31 * DAY_MS + 1, "hour", "range_too_large"),
            (1, 400 * DAY_MS + 1, "day", "range_too_large"),
        ] {
            assert_eq!(
                quota_history(&conn, &request(from, to, grain))
                    .unwrap_err()
                    .code(),
                code
            );
        }
        for (days, grain) in [(31, "hour"), (400, "day")] {
            assert!(quota_history(&conn, &request(0, days * DAY_MS, grain)).is_ok());
        }
        let mut filtered = request(0, 1, "hour");
        filtered.quota_providers = Some(vec![]);
        assert_eq!(
            quota_history(&conn, &filtered).unwrap_err().code(),
            "invalid_filter"
        );
    }

    #[test]
    fn quota_history_closed_decoding() {
        for value in [
            json!({"from_ms":0,"to_ms":1,"grain":"week"}),
            json!({"from_ms":0,"to_ms":1,"grain":"hour","unknown":true}),
            json!({"from_ms":0,"grain":"hour"}),
            json!({"from_ms":0,"to_ms":1,"grain":"hour","quota_providers":[null]}),
        ] {
            assert!(serde_json::from_value::<QuotaHistoryRequest>(value).is_err());
        }
    }

    fn query_plan(conn: &Connection, sql: &str) -> String {
        let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let unbound = vec![rusqlite::types::Null; statement.parameter_count()];
        let details: Vec<String> = statement
            .query_map(rusqlite::params_from_iter(unbound), |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        details.join("\n")
    }

    #[test]
    fn quota_history_primary_key_seeks_cover_range_predecessor_earliest_and_series() {
        let conn = conn();
        for credential in [false, true] {
            let (table, field) = sample_table(credential);
            let index = format!("sqlite_autoindex_{table}_1");
            for (sql, expected) in [
                (samples_sql(credential), format!("SEARCH s USING INDEX {index} (provider=? AND {field}=? AND pool_key=? AND fetched_at_instant>? AND fetched_at_instant<?)")),
                (predecessor_sql(credential), format!("SEARCH {table} USING INDEX {index} (provider=? AND {field}=? AND pool_key=? AND fetched_at_instant<?)")),
                (first_sample_sql(credential), format!("SEARCH {table} USING COVERING INDEX {index} (provider=? AND {field}=? AND pool_key=?)")),
                (next_series_sql(credential, NextSeries::Pool), format!("SEARCH {table} USING COVERING INDEX {index} (provider=? AND {field}=? AND pool_key>?)")),
                (next_series_sql(credential, NextSeries::Identity), format!("SEARCH {table} USING COVERING INDEX {index} (provider=? AND {field}>?)")),
                (next_series_sql(credential, NextSeries::Provider), format!("SEARCH {table} USING COVERING INDEX {index} (provider>?)")),
            ] {
                let plan = query_plan(&conn, &sql);
                assert!(plan.contains(&expected), "{expected} missing from:\n{plan}");
                assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
                assert!(!plan.contains(&format!("SCAN {table}")), "{plan}");
            }
        }
    }

    #[test]
    fn quota_history_work_follows_range_not_old_or_future_readings() {
        let measure = |history| {
            let conn = conn();
            for credential in [false, true] {
                sample(&conn, credential, "id", -2 * DAY_MS, 95.0, None, PERIOD_ONE);
                for n in 1..=history {
                    sample(&conn, credential, "id", -DAY_MS + n, 95.0, None, PERIOD_ONE);
                    sample(
                        &conn,
                        credential,
                        "id",
                        DAY_MS + n,
                        99.0,
                        None,
                        PERIOD_THREE,
                    );
                }
                sample(&conn, credential, "id", 0, 5.0, None, PERIOD_TWO);
                sample(&conn, credential, "id", HOUR_MS / 2, 10.0, None, PERIOD_TWO);
            }
            crate::multiplier::vm_steps(&conn, || {
                quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap()
            })
        };
        let (small, small_work) = measure(100);
        let (large, large_work) = measure(10_000);
        assert_eq!(small, large);
        assert_eq!(large.windows.len(), 2);
        assert_eq!(large.windows[0].buckets[0].samples, 2);
        assert_eq!(large.windows[0].resets.len(), 1);
        assert!(
            large_work <= small_work + 100,
            "100 to 10,000 old and future readings: {small_work} -> {large_work} VM instructions"
        );
    }

    #[test]
    fn quota_history_extreme_millisecond_ranges_and_submillisecond_edges() {
        let conn = conn();
        sample(&conn, false, "id", 0, 3.0, None, PERIOD_TWO);
        conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,resets_at) VALUES('claude','id','primary',-1,90,?1)", [PERIOD_ONE]).unwrap();
        conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent) VALUES('claude','id','primary',?1,99)", [HOUR_MS * NANOS_PER_MS]).unwrap();
        let report = quota_history(&conn, &request(0, HOUR_MS, "hour")).unwrap();
        assert_eq!(report.earliest_sample_ms, Some(-1));
        assert_eq!(report.windows[0].resets[0].from_percent, 90.0);
        assert_eq!(report.windows[0].buckets[0].samples, 1);
        for (from, to) in [(i64::MIN, i64::MIN + 1), (i64::MAX - 1, i64::MAX)] {
            let report = quota_history(&conn, &request(from, to, "hour")).unwrap();
            assert!(report.windows.is_empty());
            assert_eq!(report.earliest_sample_ms, Some(-1));
        }
    }

    struct ReferenceSample {
        at: i64,
        used: f64,
        minutes: Option<i64>,
        reset: Option<i64>,
    }

    // The deliberately unbounded reader is a test oracle only. It compares
    // every reply member against all recorded samples, independently of the
    // production reader's series enumeration and predecessor/range seeks.
    fn scan_reference(conn: &Connection, request: &QuotaHistoryRequest) -> QuotaHistory {
        use std::collections::BTreeMap;
        let grain = match request.grain {
            Grain::Hour => HOUR_MS,
            Grain::Day => DAY_MS,
        };
        let from = request.from_ms - request.from_ms.rem_euclid(grain);
        let to = request.to_ms + (grain - request.to_ms.rem_euclid(grain)) % grain;
        let mut groups = BTreeMap::<_, Vec<ReferenceSample>>::new();
        for credential in [false, true] {
            let (table, field) = if credential {
                ("credential_window_sample", "credential_ref")
            } else {
                ("window_sample", "account_ref")
            };
            let mut statement = conn.prepare(&format!("SELECT provider,{field},pool_key,fetched_at_instant,used_percent,raw_used_percent,window_minutes,resets_at FROM {table} ORDER BY fetched_at_instant")).unwrap();
            let mut rows = statement.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let provider: String = row.get(0).unwrap();
                if request
                    .quota_providers
                    .as_ref()
                    .is_some_and(|allowed| !allowed.contains(&provider))
                {
                    continue;
                }
                let identity: String = row.get(1).unwrap();
                let pool: String = row.get(2).unwrap();
                let at: i64 = row.get(3).unwrap();
                let used: f64 = row.get(4).unwrap();
                let raw: Option<f64> = row.get(5).unwrap();
                let reset: Option<String> = row.get(7).unwrap();
                groups
                    .entry((provider, credential, identity, pool))
                    .or_default()
                    .push(ReferenceSample {
                        at: at.div_euclid(NANOS_PER_MS),
                        used: raw.unwrap_or(used),
                        minutes: row.get(6).unwrap(),
                        reset: reset.and_then(|text| {
                            chrono::DateTime::parse_from_rfc3339(&text)
                                .ok()
                                .map(|time| time.timestamp_millis())
                        }),
                    });
            }
        }
        let mut report = QuotaHistory {
            windows: vec![],
            earliest_sample_ms: groups
                .values()
                .filter_map(|samples| samples.first().map(|sample| sample.at))
                .min(),
        };
        for ((provider, credential, identity, pool), samples) in groups {
            let selected: Vec<_> = samples
                .iter()
                .filter(|sample| sample.at >= from && sample.at < to)
                .collect();
            if selected.is_empty() {
                continue;
            }
            let mut period_ends = Vec::new();
            let mut predecessor = None;
            let mut accepted: Vec<&ReferenceSample> = Vec::new();
            let mut resets = Vec::new();
            for sample in samples.iter().filter(|sample| sample.at < to) {
                // Compare against every accepted period end rather than sharing
                // the indexed reader's running horizon or tolerance helper.
                let tolerance = sample.minutes.map_or(600_000, |minutes| {
                    (i128::from(minutes) * 30_000).min(600_000)
                });
                if let (Some(latest), Some(reset)) = (period_ends.iter().max(), sample.reset) {
                    let shift = i128::from(reset) - i128::from(*latest);
                    if shift < -tolerance {
                        continue;
                    }
                    if shift > tolerance && sample.at >= from {
                        if let Some(previous) = accepted.last().copied().or(predecessor) {
                            resets.push(Reset {
                                at_ms: sample.at,
                                from_percent: previous.used,
                                to_percent: sample.used,
                            });
                        }
                    }
                }
                if let Some(reset) = sample.reset {
                    period_ends.push(reset);
                }
                if sample.at < from {
                    predecessor = Some(sample);
                } else {
                    accepted.push(sample);
                }
            }
            if accepted.is_empty() {
                continue;
            }
            let mut buckets = BTreeMap::<_, Vec<f64>>::new();
            for sample in &accepted {
                buckets
                    .entry(sample.at - sample.at.rem_euclid(grain))
                    .or_default()
                    .push(sample.used);
            }
            report.windows.push(Window {
                quota_provider: provider,
                account: (!credential).then(|| identity.clone()),
                credential: credential.then_some(identity),
                pool,
                window_minutes: accepted.last().and_then(|sample| sample.minutes),
                buckets: buckets
                    .into_iter()
                    .map(|(start_ms, values)| Bucket {
                        start_ms,
                        used_percent_max: values.iter().copied().reduce(f64::max).unwrap(),
                        samples: u64::try_from(values.len()).unwrap(),
                    })
                    .collect(),
                resets,
            });
        }
        report
    }

    #[test]
    fn quota_history_indexed_reader_equals_full_scan_for_every_member_and_filter() {
        let conn = conn();
        for credential in [false, true] {
            let (table, field) = sample_table(credential);
            for provider in ["claude", "codex"] {
                for identity in ["account-a", "account-b"] {
                    for (pool, minutes) in [
                        ("primary", Some(300)),
                        ("secondary", None),
                        ("short", Some(1)),
                    ] {
                        for (at, used, raw, reset) in [
                            (-DAY_MS - 1, 90.0, None, None),
                            (-1, 97.0, Some(197.0), PERIOD_ONE),
                            (0, 3.0, Some(103.0), PERIOD_TWO),
                            (HOUR_MS / 4, 2.0, Some(102.0), PERIOD_TWO),
                            (HOUR_MS / 2, 99.0, Some(199.0), PERIOD_ONE),
                            (HOUR_MS, 10.0, None, Some("2026-11-01T05:02:00Z")),
                            (HOUR_MS + 1, 9.0, None, PERIOD_TWO),
                            (DAY_MS, 20.0, None, Some("not a timestamp")),
                            (DAY_MS + HOUR_MS, 21.0, None, None),
                            (2 * DAY_MS, 2.0, None, PERIOD_THREE),
                        ] {
                            conn.execute(&format!("INSERT INTO {table}(provider,{field},pool_key,fetched_at_instant,used_percent,raw_used_percent,window_minutes,resets_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)"), params![provider,identity,pool,at*NANOS_PER_MS,used,raw,minutes,reset]).unwrap();
                        }
                    }
                }
            }
        }
        for grain in ["hour", "day"] {
            for (from, to) in [
                (-DAY_MS - 1, 1),
                (1, DAY_MS + 1),
                (0, HOUR_MS),
                (3 * DAY_MS, 4 * DAY_MS),
            ] {
                for filter in [
                    None,
                    Some(vec!["claude".into()]),
                    Some(vec!["codex".into(), "missing".into()]),
                    Some(vec!["missing".into()]),
                ] {
                    let mut request = request(from, to, grain);
                    request.quota_providers = filter;
                    assert_eq!(
                        quota_history(&conn, &request).unwrap(),
                        scan_reference(&conn, &request),
                        "{request:?}"
                    );
                }
            }
        }
    }
}
