//! Discovery and append-only writes for a provider's complete day.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, Transaction};
use serde::Serialize;

use super::{clamp_nanos, insert_row, millis_to_nanos, MultiplierError, Observation};

#[derive(Clone, Copy)]
pub struct ProviderDay<'a> {
    pub provider: &'a str,
    pub day: &'a str,
    pub interval_start_ms: i64,
    pub interval_end_ms: i64,
}

fn interval_ns(start: i64, end: i64) -> (i64, i64) {
    (
        clamp_nanos(millis_to_nanos(start)),
        clamp_nanos(millis_to_nanos(end)),
    )
}

/// A row in `multiplier_provider_day` keeps a provider in later days even when
/// its readings disappear. It does not make the provider a candidate for earlier days.
pub fn providers(
    conn: &Connection,
    day: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<String>, MultiplierError> {
    let (start, end) = interval_ns(start_ms, end_ms);
    let mut statement = conn.prepare_cached(
        "SELECT provider FROM window_sample WHERE fetched_at_instant BETWEEN ?1 AND ?2
         UNION
         SELECT provider FROM credential_window_sample WHERE fetched_at_instant BETWEEN ?1 AND ?2
         UNION
         SELECT quota_provider FROM multiplier_provider_day WHERE observation_day <= ?3
         ORDER BY 1",
    )?;
    let rows = statement.query_map(params![start, end, day], |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Explicit recomputes may name any provider with stored history, including
/// one with only observations recorded before pool scopes were stored.
pub fn known(conn: &Connection, provider: &str) -> Result<bool, MultiplierError> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM window_sample WHERE provider=?1)
             OR EXISTS (SELECT 1 FROM credential_window_sample WHERE provider=?1)
             OR EXISTS (SELECT 1 FROM multiplier_observation WHERE quota_provider=?1)
             OR EXISTS (SELECT 1 FROM multiplier_provider_day WHERE quota_provider=?1)",
        [provider],
        |row| row.get(0),
    )?)
}

impl ProviderDay<'_> {
    pub fn has_readings(self, conn: &Connection) -> Result<bool, MultiplierError> {
        let (start, end) = interval_ns(self.interval_start_ms, self.interval_end_ms);
        Ok(conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM window_sample WHERE provider=?1 AND fetched_at_instant BETWEEN ?2 AND ?3)
                 OR EXISTS (SELECT 1 FROM credential_window_sample WHERE provider=?1 AND fetched_at_instant BETWEEN ?2 AND ?3)",
            params![self.provider, start, end],
            |row| row.get(0),
        )?)
    }

    pub fn recorded(self, conn: &Connection) -> Result<bool, MultiplierError> {
        Ok(conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM multiplier_provider_day WHERE quota_provider=?1 AND observation_day=?2)",
            params![self.provider, self.day],
            |row| row.get(0),
        )?)
    }

    /// An outstanding backlog must still be on a head. A superseding set that
    /// cleared that backlog already repaired this day, even after restart.
    pub fn needs_source_repair(self, conn: &Connection) -> Result<bool, MultiplierError> {
        Ok(conn.query_row(
            "SELECT EXISTS (
                 SELECT 1 FROM multiplier_observation o
                 WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.pool_scope IS NOT NULL
                   AND o.source_backlog<>'[]'
                   AND NOT EXISTS (SELECT 1 FROM multiplier_observation child WHERE child.supersedes=o.observation_id)
             ) AND NOT EXISTS (
                 SELECT 1 FROM multiplier_observation o
                 JOIN multiplier_observation child ON child.supersedes=o.observation_id
                 WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.pool_scope IS NOT NULL
                   AND o.source_backlog<>'[]' AND child.source_backlog='[]'
                   AND NOT EXISTS (
                       SELECT 1 FROM multiplier_observation sibling
                       WHERE sibling.quota_provider=?1 AND sibling.observation_day=?2
                         AND sibling.snapshot_at_ms=child.snapshot_at_ms AND sibling.source_backlog<>'[]'
                   )
             )",
            params![self.provider, self.day],
            |row| row.get(0),
        )?)
    }

    /// The daily stage must not clear an old backlog just because its source
    /// was removed from the runtime inventory.
    pub fn missing_backlog_sources(
        self,
        conn: &Connection,
    ) -> Result<Vec<String>, MultiplierError> {
        let mut statement = conn.prepare_cached(
            "SELECT DISTINCT backlog.value
             FROM multiplier_observation o CROSS JOIN json_each(o.source_backlog) backlog
             WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.pool_scope IS NOT NULL
               AND o.source_backlog<>'[]'
               AND NOT EXISTS (SELECT 1 FROM multiplier_observation child WHERE child.supersedes=o.observation_id)
               AND NOT EXISTS (SELECT 1 FROM source_runtime r WHERE r.source_key=backlog.value)",
        )?;
        let rows = statement.query_map(params![self.provider, self.day], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// The provider-day marker and all pool observations commit together. A recompute that
    /// recorded the day while its facts were being read wins over a daily tick.
    pub fn record(
        self,
        tx: &Transaction<'_>,
        snapshot_at_ms: i64,
        rows: &[Observation],
    ) -> Result<bool, MultiplierError> {
        if self.recorded(tx)? {
            return Ok(false);
        }
        self.check_rows(rows)?;
        for row in rows {
            insert_row(tx, row, None)?;
        }
        self.mark_recorded(tx, snapshot_at_ms)?;
        Ok(true)
    }

    /// Recheck heads under the writer transaction: the daily stage or another
    /// recompute could have written a day while this one's facts were in flight.
    pub fn reconcile(
        self,
        tx: &Transaction<'_>,
        snapshot_at_ms: i64,
        rows: &[Observation],
    ) -> Result<Vec<Recomputed>, MultiplierError> {
        self.check_rows(rows)?;
        let mut heads = self.heads(tx)?;
        let mut appended = Vec::new();
        for row in rows {
            let key = (
                row.plan.clone().unwrap_or_default(),
                row.window_pool_key.clone(),
            );
            let supersedes = heads.remove(&key);
            let observation_id = insert_row(tx, row, supersedes)?;
            appended.push(self.recomputed(
                key.0,
                observation_id,
                supersedes,
                row.multiplier,
                row.fee_multiplier,
            ));
        }
        for ((plan, _), head) in heads {
            let id = retire(tx, head, snapshot_at_ms)?;
            appended.push(self.recomputed(plan, id, Some(head), None, None));
        }
        if !self.recorded(tx)? {
            self.mark_recorded(tx, snapshot_at_ms)?;
        }
        Ok(appended)
    }

    fn check_rows(self, rows: &[Observation]) -> Result<(), MultiplierError> {
        let mut keys = BTreeSet::new();
        for row in rows {
            if row.quota_provider != self.provider
                || row.observation_day != self.day
                || row.interval_start_ms != self.interval_start_ms
                || row.interval_end_ms != self.interval_end_ms
                || row.pool_scope.is_none()
                || row.plan.is_none()
                || !keys.insert((&row.plan, &row.window_pool_key))
            {
                return Err(MultiplierError::InvalidObservation(
                    "a provider day needs one scoped row per plan and window pool".into(),
                ));
            }
        }
        Ok(())
    }

    fn mark_recorded(self, tx: &Transaction<'_>, at_ms: i64) -> Result<(), MultiplierError> {
        tx.execute(
            "INSERT INTO multiplier_provider_day (quota_provider, observation_day, recorded_at_ms)
             VALUES (?1,?2,?3)",
            params![self.provider, self.day, at_ms],
        )?;
        Ok(())
    }

    fn heads(
        self,
        tx: &Transaction<'_>,
    ) -> Result<BTreeMap<(String, String), i64>, MultiplierError> {
        let mut statement = tx.prepare_cached(
            "SELECT o.plan, o.window_pool_key, o.observation_id
             FROM multiplier_observation o
             WHERE o.quota_provider=?1 AND o.observation_day=?2 AND o.pool_scope IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM multiplier_observation child WHERE child.supersedes=o.observation_id)",
        )?;
        let rows = statement.query_map(params![self.provider, self.day], |row| {
            Ok(((row.get(0)?, row.get(1)?), row.get(2)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    fn recomputed(
        self,
        plan: String,
        observation_id: i64,
        supersedes: Option<i64>,
        multiplier: Option<f64>,
        fee_multiplier: Option<f64>,
    ) -> Recomputed {
        Recomputed {
            quota_provider: self.provider.into(),
            observation_day: self.day.into(),
            plan,
            observation_id,
            supersedes,
            multiplier,
            fee_multiplier,
        }
    }
}

#[derive(Serialize)]
pub struct Recomputed {
    quota_provider: String,
    observation_day: String,
    plan: String,
    observation_id: i64,
    supersedes: Option<i64>,
    multiplier: Option<f64>,
    fee_multiplier: Option<f64>,
}

/// Copy the parent's dated facts without resolving prices for a plan that no
/// longer holds the readings. The measurement columns use unmeasured placeholders.
fn retire(tx: &Transaction<'_>, head: i64, snapshot_at_ms: i64) -> Result<i64, MultiplierError> {
    tx.execute(
        r#"INSERT INTO multiplier_observation (
            quota_provider, observation_day, snapshot_at_ms, interval_start_ms, interval_end_ms,
            token_interval_start_ms, token_interval_end_ms, window_pool_key, window_minutes,
            input_tokens, cache_read_tokens, cache_write_tokens, output_tokens, reasoning_tokens,
            api_nanos, api_nanos_by_model, unpriced_tokens, tokens_outside_quota_coverage,
            base_rate_tier_untested_tokens, subscription_nanos, fee_paid_nanos, delta_percent,
            accounts_included_count, accounts_included, accounts_excluded, excluded_pairs,
            low_resolution, scope_rules, plan_prices, catalog_resolved_at_ms, api_twins,
            reasoning_billed_as_output, error_deflating, error_inflating, multiplier,
            fee_multiplier, supersedes, plan, routing_eligible, eligibility_reasons,
            clean_hours_share, saturated_share, gap_share, unpriced_share, unconfigured_accounts,
            source_backlog, unattributed_event_share, zero_measurement_events, missing_quantity_events,
            pool_scope, pool_models, pool_models_at_ms, pool_member_models, catalog_provider
        ) SELECT quota_provider, observation_day, ?2, interval_start_ms, interval_end_ms,
            token_interval_start_ms, token_interval_end_ms, window_pool_key, window_minutes,
            0, 0, 0, 0, 0,
            0, '[]', '[]', 0,
            0, 0, 0, 0,
            0, '[]', '[]', '[]',
            low_resolution, '[]', plan_prices, catalog_resolved_at_ms, '{}',
            '[]', error_deflating, error_inflating, NULL,
            NULL, observation_id, plan, 0, '["pool_retired"]',
            0, 0, 0, 0, '[]',
            '[]', 0, 0, 0,
            pool_scope, pool_models, pool_models_at_ms, pool_member_models, catalog_provider
        FROM multiplier_observation WHERE observation_id=?1"#,
        params![head, snapshot_at_ms],
    )?;
    Ok(tx.last_insert_rowid())
}
