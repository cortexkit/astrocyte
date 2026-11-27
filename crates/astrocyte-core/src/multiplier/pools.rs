//! Read-time quota coverage and the per-plan longest-window choice.

use std::collections::{BTreeMap, BTreeSet};

use fusiform_protocol::QuotaPoolWire;

use crate::{capacity::WindowModelScope, subscription::PlanLedger};

use super::PoolScope;

mod calls;
mod capture;
mod history;
mod inheritance;
mod rows;
#[cfg(test)]
pub(super) use rows::empty_row;

pub use calls::{
    capture_pool_day, resolve_pool_models, PoolCall, PoolCallSelection, PoolDayCapture,
    PoolModelIdentities,
};
pub use capture::capture_pool_readings;
pub(in crate::multiplier) use history::VaultPlanTimeline;
pub use rows::compute_pool_observations;

/// Account and credential counters use separate identity namespaces.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolLogin {
    pub ledger: PlanLedger,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoolReading {
    pub fetched_at_instant_ns: i64,
    pub used_percent: f64,
    pub resets_at: Option<String>,
    pub window_minutes: Option<i64>,
    /// Effective catalog provider for this computation, not the quota provider.
    pub api_provider: Option<String>,
    /// Effective for this computation, including any pre-cut inheritance.
    pub model_scope: Option<WindowModelScope>,
}

impl PoolReading {
    pub fn fetched_at_ms(&self) -> i64 {
        self.fetched_at_instant_ns.div_euclid(super::NANOS_PER_MS)
    }
}

#[derive(Debug)]
struct Stream {
    login: PoolLogin,
    pool_id: String,
    window_pool_key: String,
    readings: Vec<PoolReading>,
    scope_conflicts: BTreeSet<i64>,
}

/// Owned inputs from the provider day's read transaction. No catalog facts
/// are needed until after the transaction has closed.
#[derive(Debug)]
pub struct PoolReadingCapture {
    pub quota_provider: String,
    pub interval_start_ms: i64,
    pub interval_end_ms: i64,
    streams: Vec<Stream>,
    history: history::VaultPlanTimeline,
}

impl PoolReadingCapture {
    /// The vault plan at the instant of a reading or call. The timeline covers
    /// the captured interval and any readings used to anchor its counters.
    pub fn plan_at(&self, ledger: PlanLedger, login: &str, at_ms: i64) -> Option<&str> {
        self.history.plan_at(ledger, login, at_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolDiagnostic {
    ScopeUnstated,
    ScopeChanged,
    WindowChanged,
    PoolListMissing,
    WindowTie,
}

impl PoolDiagnostic {
    pub fn code(self) -> &'static str {
        match self {
            Self::ScopeUnstated => "scope_unstated",
            Self::ScopeChanged => "scope_changed",
            Self::WindowChanged => "window_changed",
            Self::PoolListMissing => "pool_list_missing",
            Self::WindowTie => "window_tie",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolCoverage<'a> {
    All,
    Subset(&'a [String]),
}

impl PoolCoverage<'_> {
    /// Pass the call's own catalog id, before any substitution for pricing.
    /// Unresolved models count only for all-model windows.
    pub fn covers_model(&self, catalog_id: Option<&str>) -> bool {
        match self {
            Self::All => true,
            Self::Subset(models) => {
                catalog_id.is_some_and(|id| models.iter().any(|model| model == id))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoolReadingRun<'a> {
    pub login: &'a PoolLogin,
    /// Consecutive readings assigned to this plan, including any pre-interval
    /// readings needed to establish the counter's high-water mark. Do not
    /// form pairs across separate runs of the same login.
    pub readings: &'a [PoolReading],
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedPool<'a> {
    pub quota_provider: &'a str,
    pub plan: &'a str,
    pub pool_id: &'a str,
    pub window_pool_key: &'a str,
    pub pool_scope: PoolScope,
    pub catalog_provider: Option<&'a str>,
    pub pool_models: Option<&'a [String]>,
    pub pool_models_at_ms: Option<i64>,
    pub window_minutes: i64,
    pub diagnostic: Option<PoolDiagnostic>,
    pub reading_runs: Vec<PoolReadingRun<'a>>,
}

impl<'a> PlannedPool<'a> {
    /// Only a measured window or a tie has known coverage and one duration.
    pub fn coverage(&self) -> Option<PoolCoverage<'a>> {
        if !matches!(self.diagnostic, None | Some(PoolDiagnostic::WindowTie)) {
            return None;
        }
        match self.pool_scope {
            PoolScope::All => Some(PoolCoverage::All),
            PoolScope::Subset => self.pool_models.map(PoolCoverage::Subset),
            PoolScope::Unknown => None,
        }
    }
}

#[derive(Default)]
struct PoolShape<'a> {
    runs: Vec<PoolReadingRun<'a>>,
    scopes: [bool; 3],
    scope_conflict: bool,
    minutes: BTreeSet<Option<i64>>,
    api_providers: BTreeSet<Option<&'a str>>,
    newest_duration: Option<(i64, i64)>,
    newest_key: Option<(i64, &'a str)>,
}

impl<'a> PoolShape<'a> {
    fn observe(&mut self, reading: &'a PoolReading, stream: &'a Stream) {
        let instant = reading.fetched_at_instant_ns;
        self.scope_conflict |= stream.scope_conflicts.contains(&instant);
        self.scopes[match reading.model_scope {
            None => 0,
            Some(WindowModelScope::All) => 1,
            Some(WindowModelScope::Subset) => 2,
        }] = true;
        self.minutes.insert(reading.window_minutes);
        self.api_providers
            .insert(reading.api_provider.as_deref().filter(|id| !id.is_empty()));
        if let Some(minutes) = reading.window_minutes {
            self.newest_duration = self.newest_duration.max(Some((instant, minutes)));
        }
        self.newest_key = self
            .newest_key
            .max(Some((instant, &stream.window_pool_key)));
    }

    fn classify(
        self,
        capture: &'a PoolReadingCapture,
        plan: &'a str,
        pool_id: &'a str,
        lists: &BTreeMap<(&str, &str), &'a [String]>,
    ) -> Option<PlannedPool<'a>> {
        use PoolDiagnostic::{PoolListMissing, ScopeChanged, ScopeUnstated, WindowChanged};

        let (_, window_minutes) = self.newest_duration?;
        let (_, window_pool_key) = self.newest_key?;
        let (pool_scope, mut diagnostic) = match self.scopes {
            _ if self.scope_conflict => (PoolScope::Unknown, Some(ScopeChanged)),
            [true, _, _] => (PoolScope::Unknown, Some(ScopeUnstated)),
            [false, true, true] => (PoolScope::Unknown, Some(ScopeChanged)),
            [false, false, true] => (PoolScope::Subset, None),
            _ => (PoolScope::All, None),
        };
        let subset = pool_scope == PoolScope::Subset;
        // The whole pool must agree on one catalog provider before a list
        // can define its coverage. A missing value cannot certify that match.
        let catalog_provider = if self.api_providers.len() == 1 {
            self.api_providers.first().copied().flatten()
        } else {
            None
        };
        let pool_models = catalog_provider
            .filter(|_| subset)
            .and_then(|provider| lists.get(&(provider, pool_id)).copied());
        if diagnostic.is_none() {
            if self.minutes.len() > 1 {
                diagnostic = Some(WindowChanged);
            } else if subset && pool_models.is_none() {
                diagnostic = Some(PoolListMissing);
            }
        }
        Some(PlannedPool {
            quota_provider: &capture.quota_provider,
            plan,
            pool_id,
            window_pool_key,
            pool_scope,
            catalog_provider,
            pool_models,
            pool_models_at_ms: subset.then_some(capture.interval_end_ms),
            window_minutes,
            diagnostic,
            reading_runs: self.runs,
        })
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum CoverageKey<'a> {
    All,
    Subset(BTreeSet<&'a str>),
}

/// Select pool rows using the quota lists returned for `interval_end_ms`.
/// Coverage equality uses sets, but the recorded subset list retains the
/// catalog's original values. No network or database access happens here.
pub fn plan_pools<'a>(
    capture: &'a PoolReadingCapture,
    quota_pools: &'a [QuotaPoolWire],
) -> Vec<PlannedPool<'a>> {
    let lists = quota_pools
        .iter()
        .filter(|pool| !pool.models.is_empty())
        .map(|pool| {
            (
                (pool.provider_id.as_str(), pool.pool_id.as_str()),
                pool.models.as_slice(),
            )
        })
        .collect();
    let mut shapes = BTreeMap::<(&str, &str), PoolShape<'a>>::new();
    for stream in &capture.streams {
        let plan_at = |reading: &PoolReading| {
            capture.plan_at(
                stream.login.ledger,
                &stream.login.id,
                reading.fetched_at_ms(),
            )
        };
        for readings in stream.readings.chunk_by(|a, b| {
            plan_at(a).is_some_and(|plan| {
                plan_at(b) == Some(plan)
                    && capture.history.unchanged(
                        stream.login.ledger,
                        &stream.login.id,
                        (a.fetched_at_ms(), b.fetched_at_ms()),
                        plan,
                    )
            })
        }) {
            let Some(plan) = plan_at(&readings[0]) else {
                continue;
            };
            let from = readings.partition_point(|r| r.fetched_at_ms() < capture.interval_start_ms);
            let to = readings.partition_point(|r| r.fetched_at_ms() < capture.interval_end_ms);
            // A pair needs an opener before the end and a closer at or after
            // the start. A reset exactly at the start still enters the counter.
            let pair_start = from.saturating_sub(1);
            let pair_end = to.saturating_add(1).min(readings.len());
            if pair_end.saturating_sub(pair_start) < 2 {
                continue;
            }
            let shape = shapes.entry((plan, &stream.pool_id)).or_default();
            for reading in &readings[pair_start..pair_end] {
                shape.observe(reading, stream);
            }
            shape.runs.push(PoolReadingRun {
                login: &stream.login,
                readings: &readings[..pair_end],
            });
        }
    }
    let mut pools = Vec::new();
    let mut longest = BTreeMap::<(&str, CoverageKey<'a>), Vec<PlannedPool<'a>>>::new();
    for ((plan, id), shape) in shapes {
        let Some(pool) = shape.classify(capture, plan, id, &lists) else {
            continue;
        };
        let key = match pool.coverage() {
            Some(PoolCoverage::All) => CoverageKey::All,
            Some(PoolCoverage::Subset(models)) => {
                CoverageKey::Subset(models.iter().map(String::as_str).collect())
            }
            None => {
                pools.push(pool);
                continue;
            }
        };
        let best = longest.entry((plan, key)).or_default();
        if let Some(previous) = best.first() {
            match pool.window_minutes.cmp(&previous.window_minutes) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => (),
                std::cmp::Ordering::Greater => best.clear(),
            }
        }
        best.push(pool);
    }
    for mut best in longest.into_values() {
        if best.len() > 1 {
            for pool in &mut best {
                pool.diagnostic = Some(PoolDiagnostic::WindowTie);
            }
        }
        pools.extend(best);
    }
    pools.sort_unstable_by_key(|pool| (pool.plan, pool.pool_id));
    pools
}

#[cfg(test)]
mod tests;
