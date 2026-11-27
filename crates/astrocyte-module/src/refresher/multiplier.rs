//! Daily multiplier observations and rate timeline retries are separate from loop scheduling.

use super::*;
use astrocyte_core::multiplier::{
    pools,
    provider_day::{self, ProviderDay},
};
use fusiform_protocol::PlanPricesRequest;
use std::collections::BTreeSet;

pub(super) const DAY_MS: i64 = 86_400_000;
/// Each observation covers the seven UTC days that end at the day boundary.
pub(super) const MULTIPLIER_INTERVAL_MS: i64 = 7 * DAY_MS;
// Originals wait from the later of interval end and process start, so a restart
// can drain without letting a broken source block backfill forever. Recompute
// waits from interval end alone.
const MULTIPLIER_SOURCE_WAIT_MS: i64 = 2 * 3_600_000;

/// Records a complete set of pool observations per quota provider per UTC day,
/// over the trailing seven days. Missing provider days are retried for the seven
/// most recent completed UTC days, oldest first.
///
/// A computation failure writes nothing and is retried on every later tick
/// until recorded or outside the look-back. The provider-day row makes retries
/// idempotent, even across restarts or for a day with no pools. A failure is
/// logged once per day and reason; a configuration-only failure once per reason.
pub(crate) struct MultiplierStage {
    pub(super) plan_prices: Option<Arc<dyn PlanPriceSource>>,
    process_started_at_ms: i64,
    /// Provider/day pairs in the look-back with no outstanding automatic work.
    recorded: BTreeSet<(String, String)>,
    /// Provider, day (`None` for configuration-only failures), and reason.
    /// Keep every reason so interleaved retries of missed days do not repeat logs.
    failure_logged: BTreeSet<(String, Option<String>, String)>,
    /// The rates version whose model timelines are still being read, set
    /// when a full tick's pass ran out of time. Passes then continue between
    /// ticks until every model is read.
    pub(super) rates_pending: Option<i64>,
    /// Models whose last timeline read failed, and when to try them again.
    pub(super) rate_backoff: RateBackoff,
}

/// Failed timeline reads wait before they are tried again. The price catalog
/// (fusiform) keeps building a timeline after this side stops waiting, so
/// retrying a model it cannot serve in time on every pass loads the catalog
/// and holds the refresher for a full read deadline each try. The wait starts
/// at a minute and doubles up to half an hour; a successful read clears it, and
/// a new rates version (`CatalogClient::rates_version`) clears every wait,
/// since the change may be the cure.
#[derive(Default)]
pub(crate) struct RateBackoff {
    waits: std::collections::BTreeMap<(String, String), (std::time::Instant, Duration)>,
    catalog_version: Option<i64>,
}

impl RateBackoff {
    const FIRST: Duration = Duration::from_secs(60);
    const MAX: Duration = Duration::from_secs(30 * 60);

    fn waiting(&self, model: &(String, String), now: std::time::Instant) -> bool {
        self.waits.get(model).is_some_and(|(until, _)| *until > now)
    }

    /// Returns whether this is the model's first failure since it last read
    /// cleanly, so the warning is logged once rather than on every retry.
    fn failed(&mut self, model: (String, String), now: std::time::Instant) -> bool {
        let wait = self
            .waits
            .get(&model)
            .map_or(Self::FIRST, |(_, wait)| (*wait * 2).min(Self::MAX));
        self.waits.insert(model, (now + wait, wait)).is_none()
    }

    fn succeeded(&mut self, model: &(String, String)) {
        self.waits.remove(model);
    }

    fn catalog_seen(&mut self, version: i64) {
        if self.catalog_version.replace(version) != Some(version) {
            self.waits.clear();
        }
    }
}

/// Why a day was not recorded, and whether only a configuration change can
/// cure it (in which case retrying is still correct, but logging it every day
/// would repeat a known fact).
struct DayFailure {
    reason: String,
    needs_config: bool,
}

impl MultiplierStage {
    /// For fixtures that do not exercise a recent process start.
    #[cfg(test)]
    pub(crate) fn new(plan_prices: Option<Arc<dyn PlanPriceSource>>) -> Self {
        Self::starting_at(plan_prices, i64::MIN)
    }

    pub(super) fn starting_at(
        plan_prices: Option<Arc<dyn PlanPriceSource>>,
        process_started_at_ms: i64,
    ) -> Self {
        Self {
            plan_prices,
            process_started_at_ms,
            recorded: BTreeSet::new(),
            failure_logged: BTreeSet::new(),
            rates_pending: None,
            rate_backoff: RateBackoff::default(),
        }
    }

    /// True while model timelines for the current catalog are still unread.
    pub(crate) fn rates_behind(&self) -> bool {
        self.rates_pending.is_some()
    }

    /// One between-tick pass over the unread timelines. A failure leaves the
    /// rest to the next full tick, which reads the catalog version again.
    pub(crate) fn fill_rates(&mut self, state: &SharedState) {
        let Some(version) = self.rates_pending else {
            return;
        };
        self.rates_pending = match rate_era_fill(state, version, &mut self.rate_backoff) {
            Ok(true) => Some(version),
            Ok(false) => None,
            Err(error) => {
                tracing::warn!(target: "valuation", %error, "rate timeline refresh deferred");
                None
            }
        };
    }

    #[cfg(test)]
    pub(crate) fn tick(&mut self, state: &SharedState, catalog_pending: bool, now_ms: i64) {
        self.tick_until_stopped(state, catalog_pending, now_ms, &AtomicBool::new(false));
    }

    /// `catalog_pending`: fusiform is configured but its history boundary is
    /// not yet discovered, so rates cannot be read yet. The day waits for it
    /// rather than failing.
    #[cfg(test)]
    pub(crate) fn tick_until_stopped(
        &mut self,
        state: &SharedState,
        catalog_pending: bool,
        now_ms: i64,
        stop: &AtomicBool,
    ) {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        self.rates_pending = match rate_era_tick(state, &mut self.rate_backoff) {
            Ok(pending) => pending,
            Err(error) => {
                tracing::warn!(target: "valuation", %error, "rate timeline refresh deferred");
                None
            }
        };
        self.tick_days(state, catalog_pending, now_ms, stop);
    }

    pub(super) fn tick_days(
        &mut self,
        state: &SharedState,
        catalog_pending: bool,
        now_ms: i64,
        stop: &AtomicBool,
    ) {
        // A newly ingested model needs a timeline even when the catalog has
        // not moved. Only status RPCs are wake/backstop-driven; local model
        // discovery and unread timelines still advance on every full tick.
        if let Some(version) = self.rate_backoff.catalog_version {
            self.rates_pending = match rate_era_refresh(state, version, &mut self.rate_backoff) {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::warn!(target: "valuation", %error, "rate timeline refresh deferred");
                    None
                }
            };
        }
        let interval_end_ms = now_ms - now_ms.rem_euclid(DAY_MS);
        // The seven days the look-back covers, oldest first, each with the
        // end of its interval. A clock so far out that one of them has no
        // calendar day records nothing this tick rather than a made-up day.
        let days = match (0..7_i64)
            .rev()
            .map(|days_back| {
                let end_ms = days_back
                    .checked_mul(DAY_MS)
                    .and_then(|back_ms| interval_end_ms.checked_sub(back_ms))
                    .ok_or_else(|| format!("{days_back} days before {interval_end_ms} ms"))?;
                let day = multiplier::utc_day(end_ms).map_err(|error| error.message())?;
                Ok((end_ms, day))
            })
            .collect::<Result<Vec<(i64, String)>, String>>()
        {
            Ok(days) => days,
            Err(error) => {
                tracing::warn!(
                    target: "multiplier",
                    now_ms,
                    %error,
                    "daily multiplier skipped: the clock is outside the representable UTC calendar"
                );
                return;
            }
        };
        let Some((_, oldest_day)) = days.first() else {
            return;
        };
        self.recorded.retain(|(_, day)| day >= oldest_day);
        self.failure_logged
            .retain(|(_, day, _)| day.as_ref().is_none_or(|day| day >= oldest_day));
        for (end_ms, day) in days {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let start_ms = end_ms - MULTIPLIER_INTERVAL_MS;
            let providers = match state.with_read_op(|conn| {
                provider_day::providers(conn, &day, start_ms, end_ms).map_err(multiplier_refusal)
            }) {
                Ok(providers) => providers,
                Err((_, error)) => {
                    tracing::warn!(target: "multiplier", %day, %error, "provider day discovery failed");
                    continue;
                }
            };
            for quota_provider in providers {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let recorded_key = (quota_provider.clone(), day.clone());
                if self.recorded.contains(&recorded_key) {
                    continue;
                }
                let provider_day = ProviderDay {
                    provider: &quota_provider,
                    day: &day,
                    interval_start_ms: start_ms,
                    interval_end_ms: end_ms,
                };
                let mode = match state.with_read_op(|conn| {
                    if !provider_day.recorded(conn).map_err(multiplier_refusal)? {
                        Ok(Some(DayMode::Original {
                            process_started_at_ms: self.process_started_at_ms,
                        }))
                    } else if provider_day
                        .needs_source_repair(conn)
                        .map_err(multiplier_refusal)?
                    {
                        Ok(Some(DayMode::SourceRepair))
                    } else {
                        Ok(None)
                    }
                }) {
                    Ok(Some(mode)) => mode,
                    Ok(None) => {
                        self.recorded.insert(recorded_key);
                        continue;
                    }
                    Err((code, message)) => {
                        self.log_failure(
                            &quota_provider,
                            &day,
                            DayError::from((code, message)).into(),
                        );
                        continue;
                    }
                };
                match record_multiplier_day(
                    state,
                    self.plan_prices.as_deref(),
                    provider_day,
                    now_ms,
                    mode,
                    catalog_pending,
                    stop,
                ) {
                    Ok(DailyOutcome::Stopped) => return,
                    Ok(DailyOutcome::Deferred) => continue,
                    Ok(DailyOutcome::Recorded(observations)) => {
                        if observations.iter().all(|row| row.source_backlog.is_empty()) {
                            self.recorded.insert(recorded_key);
                        }
                        for observation in observations {
                            tracing::info!(
                                target: "multiplier",
                                quota_provider,
                                %day,
                                pool_key = observation.window_pool_key,
                                multiplier = ?observation.multiplier,
                                fee_multiplier = ?observation.fee_multiplier,
                                delta_percent = observation.delta_percent,
                                api_nanos = observation.api_nanos,
                                subscription_nanos = observation.subscription_nanos,
                                "multiplier observation recorded"
                            );
                        }
                    }
                    Err(failure) => {
                        self.log_failure(&quota_provider, &day, failure);
                    }
                }
            }
        }
    }

    /// Logs a failed day unless the same failure was already logged: the same
    /// day and reason, or (for a failure only the configuration can cure) the
    /// same reason on any day.
    /// Returns whether it logged.
    fn log_failure(&mut self, quota_provider: &str, day: &str, failure: DayFailure) -> bool {
        let key = (
            quota_provider.to_string(),
            (!failure.needs_config).then(|| day.to_string()),
            failure.reason,
        );
        if self.failure_logged.contains(&key) {
            return false;
        }
        tracing::warn!(
            target: "multiplier",
            quota_provider,
            %day,
            reason = %key.2,
            needs_config = failure.needs_config,
            "multiplier observation not recorded; retried on every later tick"
        );
        self.failure_logged.insert(key);
        true
    }
}

fn multiplier_refusal(error: multiplier::MultiplierError) -> (String, String) {
    (error.code().into(), error.message())
}

/// Why a day could not be computed, and whether retries need a configuration change.
struct DayError {
    code: String,
    message: String,
    needs_config: bool,
}

impl From<(String, String)> for DayError {
    fn from((code, message): (String, String)) -> Self {
        Self {
            code,
            message,
            needs_config: false,
        }
    }
}

impl From<DayError> for DayFailure {
    fn from(error: DayError) -> Self {
        Self {
            reason: format!("{}: {}", error.code, error.message),
            needs_config: error.needs_config,
        }
    }
}

/// Tokens can arrive through any ingest source. A recent start can leave even
/// old intervals incomplete until sources have had a chance to drain.
fn multiplier_stale_sources(
    conn: &rusqlite::Connection,
    day: ProviderDay<'_>,
    snapshot_at_ms: i64,
    mode: DayMode,
) -> Result<Vec<String>, (String, String)> {
    let internal = |error: rusqlite::Error| ("internal".into(), error.to_string());
    let mut statement = conn
        .prepare_cached(
            "SELECT source_key FROM source_runtime
         WHERE last_success_ms IS NULL OR last_success_ms < ?1
            OR pending_known > 0 OR pending_unknown != 0
         ORDER BY source_key",
        )
        .map_err(internal)?;
    let mut sources = statement
        .query_map([day.interval_end_ms], |row| row.get::<_, String>(0))
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
        .map_err(internal)?;
    if matches!(mode, DayMode::SourceRepair) {
        sources.extend(
            day.missing_backlog_sources(conn)
                .map_err(multiplier_refusal)?,
        );
        sources.sort_unstable();
    }
    let wait_from_ms = match mode {
        DayMode::Original {
            process_started_at_ms,
        } => day.interval_end_ms.max(process_started_at_ms),
        DayMode::Recompute | DayMode::SourceRepair => day.interval_end_ms,
    };
    if !sources.is_empty()
        && (matches!(mode, DayMode::SourceRepair)
            || snapshot_at_ms < wait_from_ms.saturating_add(MULTIPLIER_SOURCE_WAIT_MS))
    {
        return Err((
            "source_reads_pending".into(),
            format!(
                "waiting for sources to drain through the interval end: {}",
                sources.join(", ")
            ),
        ));
    }
    Ok(sources)
}

fn disclose_stale_sources(
    observation: &mut multiplier::Observation,
    sources: &[String],
    add_reason: bool,
) {
    if !sources.is_empty() {
        observation.source_backlog = sources.to_vec();
        observation.routing_eligible = false;
        if add_reason {
            observation
                .eligibility_reasons
                .push("source_backlog".into());
        }
        observation.error_deflating.push_str(&format!(
            " stale_sources: {}; source read is stale or backlog remains, so late usage may be missing.",
            sources.join(", ")
        ));
    }
}

/// How long one pass keeps starting new model reads. Every catalog change makes
/// every model's timeline stale, a few hundred reads, so passes continue between
/// full ticks until none is left, as the summary backlog does.
const RATE_FETCH_PASS_BUDGET: Duration = Duration::from_millis(1500);
/// The deadline for one model's read, which is up to three fusiform calls
/// (current identity, its served history, and an alias target's
/// history). Each model gets its own: when models shared the pass deadline,
/// the first model's calls left the next too little time, and its read timed
/// out on this side, not fusiform's.
const RATE_FETCH_MODEL_BUDGET: Duration = Duration::from_secs(5);

/// Refresh served timelines before multiplier work. The writer is held only for
/// discovery and publication. Timelines are keyed on fusiform's rates version,
/// so a catalog move that changes no price queues nothing. Returns that version
/// when models that may be tried now are still unread after the pass.
pub(super) fn rate_era_tick(
    state: &SharedState,
    backoff: &mut RateBackoff,
) -> Result<Option<i64>, String> {
    let Some(catalog) = state.catalog_client() else {
        return Ok(None);
    };
    let version = catalog
        .with_budget(RATE_FETCH_MODEL_BUDGET)
        .rates_version()
        .map_err(|error| format!("{error:?}"))?;
    backoff.catalog_seen(version);
    rate_era_refresh(state, version, backoff)
}

fn rate_era_refresh(
    state: &SharedState,
    version: i64,
    backoff: &mut RateBackoff,
) -> Result<Option<i64>, String> {
    use astrocyte_core::rate_era;
    state.ordered_write(|tx| {
        rate_era::discover(tx, 2000).map_err(|error| error.message())?;
        rate_era::prepare(tx, version).map_err(|error| error.message())?;
        Ok(((), true))
    })?;
    Ok(rate_era_fill(state, version, backoff)?.then_some(version))
}

/// Reads unread model timelines for `version` until the pass budget is spent,
/// skipping models still waiting after a failure. Returns whether a model that
/// may be tried now is still unread; models only waiting out a failure leave
/// to the full tick after their wait.
fn rate_era_fill(
    state: &SharedState,
    version: i64,
    backoff: &mut RateBackoff,
) -> Result<bool, String> {
    use astrocyte_core::rate_era;
    let started = std::time::Instant::now();
    let Some(catalog) = state.catalog_client() else {
        return Ok(false);
    };
    let pending = state
        .with_read_op(|conn| {
            rate_era::pending(conn, 256).map_err(|error| (error.code().into(), error.message()))
        })
        .map_err(|(_, message)| message)?;
    let at_limit = pending.len() == 256;
    let mut attempted = false;
    let mut found_reference = false;
    for key in pending {
        if backoff.waiting(&key, std::time::Instant::now()) {
            continue;
        }
        if started.elapsed() >= RATE_FETCH_PASS_BUDGET {
            return Ok(true);
        }
        attempted = true;
        let (provider, model) = key;
        let call = catalog.with_budget(RATE_FETCH_MODEL_BUDGET);
        let fetched = match rate_era::fetch(&call, &provider, &model) {
            Ok(fetched) => fetched,
            Err(error) => {
                if backoff.failed((provider.clone(), model.clone()), std::time::Instant::now()) {
                    tracing::warn!(target: "valuation", %provider, %model, error = %error.message(), "served timeline fetch deferred; retrying with backoff");
                }
                state.ordered_write(|tx| {
                    rate_era::record_failure(tx, &provider, &model, &error)
                        .map_err(|error| error.message())?;
                    Ok(((), true))
                })?;
                continue;
            }
        };
        state.ordered_write(|tx| {
            rate_era::publish(tx, &fetched, version).map_err(|error| error.message())?;
            Ok(((), true))
        })?;
        found_reference |= fetched.reference.is_some();
        backoff.succeeded(&(provider, model));
    }
    // Publishing can discover a reference target that wasn't in this page.
    Ok(found_reference || (at_limit && attempted))
}

enum DailyOutcome {
    Stopped,
    Deferred,
    Recorded(Vec<multiplier::Observation>),
}

fn record_multiplier_day(
    state: &SharedState,
    plan_prices: Option<&dyn PlanPriceSource>,
    day: ProviderDay<'_>,
    snapshot_at_ms: i64,
    mode: DayMode,
    catalog_pending: bool,
    stop: &AtomicBool,
) -> Result<DailyOutcome, DayFailure> {
    let observations = match compute_multiplier_day(state, plan_prices, day, snapshot_at_ms, mode) {
        Ok(observations) => observations,
        Err(error) if catalog_pending && error.code == "catalog_unavailable" => {
            return Ok(DailyOutcome::Deferred);
        }
        Err(error)
            if matches!(mode, DayMode::SourceRepair) && error.code == "source_reads_pending" =>
        {
            return Ok(DailyOutcome::Deferred);
        }
        Err(error) => return Err(error.into()),
    };
    // A plan-price or catalog read may finish after shutdown starts. Leave the
    // complete day for the next refresher instead of committing part of it.
    if stop.load(Ordering::Relaxed) {
        return Ok(DailyOutcome::Stopped);
    }
    let written = state.ordered_write(|tx| {
        let format_error =
            |error: multiplier::MultiplierError| format!("{}: {}", error.code(), error.message());
        let written = if matches!(mode, DayMode::SourceRepair) {
            // An operator recompute or a new backlog may have arrived during
            // the served-price reads. Neither is a reason to publish this repair.
            if !day.needs_source_repair(tx).map_err(format_error)? {
                return Ok((false, false));
            }
            match multiplier_stale_sources(tx, day, snapshot_at_ms, mode) {
                Ok(_) => {}
                Err((code, _)) if code == "source_reads_pending" => return Ok((false, false)),
                Err((code, message)) => return Err(format!("{code}: {message}")),
            }
            !day.reconcile(tx, snapshot_at_ms, &observations)
                .map_err(format_error)?
                .is_empty()
        } else {
            day.record(tx, snapshot_at_ms, &observations)
                .map_err(format_error)?
        };
        if stop.load(Ordering::Relaxed) {
            return Err("multiplier stopped".into());
        }
        Ok((written, written))
    });
    if written.is_err() && stop.load(Ordering::Relaxed) {
        return Ok(DailyOutcome::Stopped);
    }
    if written.map_err(|reason| DayFailure {
        reason,
        needs_config: false,
    })? {
        crate::bus_publications::publish_multiplier(state);
        Ok(DailyOutcome::Recorded(observations))
    } else {
        Ok(DailyOutcome::Deferred)
    }
}

#[derive(Clone, Copy)]
enum DayMode {
    Original { process_started_at_ms: i64 },
    SourceRepair,
    Recompute,
}

/// Capture the whole provider day before reading any served facts. Every pool
/// uses the same readings, vault plans and traffic, including the measurements.
fn compute_multiplier_day(
    state: &SharedState,
    plan_prices: Option<&dyn PlanPriceSource>,
    day: ProviderDay<'_>,
    snapshot_at_ms: i64,
    mode: DayMode,
) -> Result<Vec<multiplier::Observation>, DayError> {
    let (capture, stale_sources) = state
        .with_read_op(|conn| {
            if matches!(mode, DayMode::SourceRepair) {
                multiplier_stale_sources(conn, day, snapshot_at_ms, mode)?;
            }
            if !day.has_readings(conn).map_err(multiplier_refusal)? {
                return Ok((None, Vec::new()));
            }
            let capture = pools::capture_pool_day(
                conn,
                day.provider,
                day.interval_start_ms,
                day.interval_end_ms,
            )
            .map_err(multiplier_refusal)?;
            let potential_pools = pools::plan_pools(&capture.readings, &[]);
            if potential_pools.is_empty() {
                return Ok((None, Vec::new()));
            }
            let might_measure = potential_pools.iter().any(|pool| {
                matches!(
                    pool.diagnostic,
                    None | Some(
                        pools::PoolDiagnostic::WindowTie | pools::PoolDiagnostic::PoolListMissing
                    )
                )
            });
            let stale_sources = if might_measure && !matches!(mode, DayMode::SourceRepair) {
                multiplier_stale_sources(conn, day, snapshot_at_ms, mode)?
            } else {
                Vec::new()
            };
            Ok((Some(capture), stale_sources))
        })
        .map_err(DayError::from)?;
    let Some(capture) = capture else {
        return Ok(Vec::new());
    };
    let plan_prices = plan_prices.ok_or_else(|| DayError {
        code: "plan_prices_unavailable".into(),
        message: "plan prices are needed and fusiform is not configured".into(),
        needs_config: true,
    })?;
    let served = plan_prices
        .plan_prices(PlanPricesRequest {
            provider_id: None,
            at_ms: Some(day.interval_end_ms),
        })
        .map_err(|error| {
            DayError::from(multiplier_refusal(multiplier::MultiplierError::PlanPrices(
                format!("plan.prices: {}: {}", error.code, error.message),
            )))
        })?;
    let planned = pools::plan_pools(&capture.readings, &served.quota_pools);
    let models = capture.models_for_pools(&planned);
    let subset_models = capture.models_for_subsets(&planned);
    let (identities, rates) = if models.is_empty() {
        (
            pools::PoolModelIdentities::default(),
            multiplier::ResolvedRates {
                resolved_at_ms: snapshot_at_ms,
                ..multiplier::ResolvedRates::default()
            },
        )
    } else {
        let catalog = state.catalog_client().ok_or_else(|| DayError {
            code: "catalog_unavailable".into(),
            message: "no fusiform catalog is connected, so no API rate can be read".into(),
            needs_config: true,
        })?;
        let resolve = |models| {
            pools::resolve_pool_models(&catalog, models, snapshot_at_ms)
                .map_err(multiplier_refusal)
                .map_err(DayError::from)
        };
        if !matches!(mode, DayMode::Recompute) {
            let rates = state
                .with_read_op(|conn| {
                    astrocyte_core::rate_era::resolved_rates(conn, &models, snapshot_at_ms)
                        .map_err(multiplier_refusal)
                })
                .map_err(DayError::from)?;
            (resolve(&subset_models)?.0, rates)
        } else if subset_models.is_empty() {
            (
                pools::PoolModelIdentities::default(),
                multiplier::resolve_rates(&catalog, &models, snapshot_at_ms)
                    .map_err(multiplier_refusal)
                    .map_err(DayError::from)?,
            )
        } else {
            resolve(&models)?
        }
    };
    let mut observations = pools::compute_pool_observations(
        &capture,
        &planned,
        &rates,
        &identities,
        &served,
        snapshot_at_ms,
    )
    .map_err(multiplier_refusal)
    .map_err(DayError::from)?;
    for (row, pool) in observations.iter_mut().zip(&planned) {
        if pool.coverage().is_some() {
            disclose_stale_sources(row, &stale_sources, pool.diagnostic.is_none());
        }
    }
    Ok(observations)
}

/// Recompute the same seven-day interval as the daily stage. Fusiform's
/// plan-price and catalog reads all finish before the write transaction; a refusal leaves all requested days
/// unchanged. Only chains with a stored pool scope are reconciled.
pub(crate) fn recompute_multiplier_day(
    state: &SharedState,
    plan_prices: Option<&dyn PlanPriceSource>,
    request: &multiplier::MultiplierRecomputeRequest,
    now_ms: i64,
) -> Result<serde_json::Value, (String, String)> {
    let day = &request.observation_day;
    let interval_end_ms = multiplier::utc_day_start_ms(day).ok_or_else(|| {
        (
            "malformed".to_string(),
            format!("observation_day {day:?} is not a YYYY-MM-DD date"),
        )
    })?;
    let interval_start_ms = interval_end_ms - MULTIPLIER_INTERVAL_MS;
    let quota_providers = state.with_read_op(|conn| {
        if let Some(provider) = &request.quota_provider {
            if !provider_day::known(conn, provider).map_err(multiplier_refusal)? {
                return Err((
                    "unknown_quota_provider".into(),
                    format!("no readings or multiplier history for '{provider}'"),
                ));
            }
            Ok(vec![provider.clone()])
        } else {
            provider_day::providers(conn, day, interval_start_ms, interval_end_ms)
                .map_err(multiplier_refusal)
        }
    })?;
    if quota_providers.is_empty() {
        return Err((
            "unknown_quota_provider".into(),
            "no quota provider has readings or a recorded day in range".into(),
        ));
    }
    let mut calculations = Vec::new();
    for provider in &quota_providers {
        let day = ProviderDay {
            provider,
            day,
            interval_start_ms,
            interval_end_ms,
        };
        let rows = compute_multiplier_day(state, plan_prices, day, now_ms, DayMode::Recompute)
            .map_err(|error| (error.code, error.message))?;
        calculations.push((day, rows));
    }
    let reply = state.ordered_op_write(|tx| {
        let mut recomputed = Vec::new();
        for (day, rows) in &calculations {
            recomputed.extend(
                day.reconcile(tx, now_ms, rows)
                    .map_err(multiplier_refusal)?,
            );
        }
        Ok((serde_json::json!({ "recomputed": recomputed }), true))
    })?;
    crate::bus_publications::publish_multiplier(state);
    Ok(reply)
}

#[cfg(test)]
mod multiplier_stage_tests {
    include!("antigravity_pool_tests.rs");
    include!("stop_multiplier_tests.rs");
    include!("provider_day_tests.rs");
    include!("../refresher_bus_publications_tests.rs");
    include!("../refresher_multiplier_v2_tests.rs");
    include!("../refresher_rate_era_tests.rs");
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use astrocyte_core::catalog_client::CatalogCallError;
    use astrocyte_core::store::MIGRATIONS;
    use fusiform_protocol::{
        CatalogGetRequest, CatalogGetResponse, PlanAmount, PlanPriceWire, PlanPricesRequest,
        PlanPricesResponse, QuotaPoolWire,
    };
    use serde_json::json;

    use super::*;

    /// 2024-10-04T00:00Z plus an hour: the first tick of that UTC day.
    const DAY_START: i64 = 20_000 * DAY_MS;
    const NOW: i64 = DAY_START + 3_600_000;

    /// Serves one $10-per-million rate for every model, the Pro 20x plan
    /// price, and fusiform's history-boundary refusal. Plan-price calls are
    /// counted and can be made to fail. The rate can be changed, a rate key
    /// this decoder does not know can be added beside it, and every model can
    /// be refused as outside the catalog's coverage.
    struct FakeFusiform {
        catalog_calls: AtomicUsize,
        plan_calls: AtomicUsize,
        plan_instants: std::sync::Mutex<Vec<Option<i64>>>,
        quota_pools: std::sync::Mutex<Vec<QuotaPoolWire>>,
        future_plan_price: AtomicBool,
        plan_prices_fail: std::sync::atomic::AtomicBool,
        input_rate_units: std::sync::atomic::AtomicI64,
        unrecognized_key: std::sync::atomic::AtomicBool,
        unit_billing: std::sync::atomic::AtomicBool,
        reasoning_billing: AtomicBool,
        cache_write_ttl: std::sync::atomic::AtomicBool,
        uncovered: std::sync::atomic::AtomicBool,
        dated_snapshot_uncovered: AtomicBool,
        history_fingerprint: std::sync::atomic::AtomicI64,
        history_version: std::sync::atomic::AtomicI64,
        history_calls: AtomicUsize,
        /// Served as `rates_version`; 0 is what an older fusiform reports.
        rates_version: std::sync::atomic::AtomicI64,
        /// Catalog moves that change no price, such as a grade write.
        other_catalog_moves: std::sync::atomic::AtomicI64,
    }

    impl FakeFusiform {
        fn new(plan_prices_fail: bool) -> Arc<Self> {
            Arc::new(Self {
                catalog_calls: AtomicUsize::new(0),
                plan_calls: AtomicUsize::new(0),
                plan_instants: std::sync::Mutex::default(),
                quota_pools: std::sync::Mutex::default(),
                future_plan_price: AtomicBool::new(false),
                plan_prices_fail: std::sync::atomic::AtomicBool::new(plan_prices_fail),
                input_rate_units: std::sync::atomic::AtomicI64::new(10_000_000_000),
                unrecognized_key: std::sync::atomic::AtomicBool::new(false),
                unit_billing: std::sync::atomic::AtomicBool::new(false),
                reasoning_billing: AtomicBool::new(false),
                cache_write_ttl: std::sync::atomic::AtomicBool::new(false),
                uncovered: std::sync::atomic::AtomicBool::new(false),
                dated_snapshot_uncovered: AtomicBool::new(false),
                history_fingerprint: std::sync::atomic::AtomicI64::new(320_000_000_000),
                history_version: std::sync::atomic::AtomicI64::new(1),
                history_calls: AtomicUsize::new(0),
                rates_version: std::sync::atomic::AtomicI64::new(0),
                other_catalog_moves: std::sync::atomic::AtomicI64::new(0),
            })
        }
    }

    impl CatalogConnection for FakeFusiform {
        fn catalog_history(
            &self,
            request: fusiform_protocol::HistoryRequest,
            _: Duration,
        ) -> Result<fusiform_protocol::HistoryResponse, CatalogCallError> {
            assert_eq!(request.served, Some(true));
            assert_eq!(request.fact_prefix.as_deref(), Some("rate."));
            self.history_calls.fetch_add(1, Ordering::SeqCst);
            let response = self.catalog_get(CatalogGetRequest {
                paged: false,
                page_token: None,
                source: None,
                at_ms: Some(1),
                fact_prefixes: Some(vec!["rate.".into()]),
                include_retired: true,
                provider_id: Some(request.provider_id.clone()),
                model_id: Some(request.model_id.clone()),
                auth_method: None,
            })?;
            let facts = response.models.values().next().unwrap();
            let served: BTreeMap<_, _> = facts
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        json!([{
                            "from_ms":1,"until_ms":null,"state":"value","value":value
                        }]),
                    )
                })
                .collect();
            Ok(serde_json::from_value(json!({"source":"fake", "provider_id":request.provider_id, "model_id":request.model_id,
                "fact_prefix":"rate.","served_by_key":served,"eras":[]})).unwrap())
        }

        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            let fingerprint = self.input_rate_units.load(Ordering::SeqCst) * 32
                + i64::from(self.unrecognized_key.load(Ordering::SeqCst))
                + 2 * i64::from(self.unit_billing.load(Ordering::SeqCst))
                + 4 * i64::from(self.cache_write_ttl.load(Ordering::SeqCst))
                + 8 * i64::from(self.uncovered.load(Ordering::SeqCst));
            if self.history_fingerprint.swap(fingerprint, Ordering::SeqCst) != fingerprint {
                self.history_version.fetch_add(1, Ordering::SeqCst);
            }
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake",
                "catalog_version": self.history_version.load(Ordering::SeqCst) + self.other_catalog_moves.load(Ordering::SeqCst),
                "rates_version": self.rates_version.load(Ordering::SeqCst),
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            self.catalog_calls.fetch_add(1, Ordering::SeqCst);
            if request.at_ms == Some(0) {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "fusiform's history begins at 1".into(),
                });
            }
            // The served history fixture reads its rate at 1, independently
            // of the dated snapshots the multiplier queries for the day.
            if self.uncovered.load(Ordering::SeqCst)
                || (self.dated_snapshot_uncovered.load(Ordering::SeqCst)
                    && request.at_ms.is_some_and(|at_ms| at_ms > 1))
            {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "the catalog has no row for this model".into(),
                });
            }
            let key = format!(
                "{}/{}",
                request.provider_id.unwrap(),
                request.model_id.unwrap()
            );
            let rate = json!({"state":"priced","units":self.input_rate_units.load(Ordering::SeqCst),"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
            let mut facts = BTreeMap::from([("rate.input".to_string(), rate.clone())]);
            facts.insert("rate.output".into(), json!({"state":"priced","units":20_000_000_000_i64,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}));
            if self.reasoning_billing.load(Ordering::SeqCst) {
                facts.insert(
                    "rate.reasoning".into(),
                    json!({"state":"billed_as","key":"rate.output"}),
                );
            }
            if self.unrecognized_key.load(Ordering::SeqCst) {
                facts.insert("rate.input.above_context.272k".to_string(), rate);
            }
            if self.unit_billing.load(Ordering::SeqCst) {
                let unit_rate = json!({"state":"priced","units":4_500_000,"exponent":9,"currency":"USD",
                    "unit_provenance":{"kind":"stated"},"per":60,
                    "increment":{"kind":"unknown"},"minimum_quantity":{"kind":"unknown"}});
                facts.insert("rate.input_audio.per_second".into(), unit_rate.clone());
                facts.insert("rate.input.per_character".into(), unit_rate.clone());
                facts.insert(
                    "rate.output.per_image.size.large.quality.high".into(),
                    unit_rate,
                );
                for role in ["output_audio", "cached_input_audio"] {
                    facts.insert(
                        format!("rate.{role}"),
                        json!({"state":"priced","units":1,"exponent":9,
                        "currency":"USD","unit_provenance":{"kind":"stated"}}),
                    );
                }
            }
            if self.cache_write_ttl.load(Ordering::SeqCst) {
                for (key, units) in [
                    ("rate.cache_write", 10_000_000_000_i64),
                    ("rate.cache_write.ttl.5m", 12_500_000_000),
                    ("rate.cache_write.ttl.1h", 20_000_000_000),
                    (
                        "rate.cache_write.above_context.272000.ttl.12h",
                        40_000_000_000,
                    ),
                ] {
                    facts.insert(key.into(), json!({"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}));
                }
            }
            Ok(CatalogGetResponse {
                overrides: Vec::new(),
                source: "models.dev".into(),
                resolved_at_ms: request.at_ms.unwrap_or(1),
                catalog_version: 1,
                models: BTreeMap::from([(key, facts)]),
                withheld: vec![],
                uncertain: vec![],
                next_page_token: None,
                curated: vec![],
                quality_scores: None,
                canonical_links: vec![],
                wire_corrections: vec![],
                fact_overrides: vec![],
                retired: vec![],
                aliased: vec![],
                plane: None,
                billing_rules: vec![],
                login_methods: vec![],
                speed_twins: vec![],
            })
        }
    }

    impl PlanPriceSource for FakeFusiform {
        fn plan_prices(
            &self,
            request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            self.plan_instants.lock().unwrap().push(request.at_ms);
            if self.plan_prices_fail.load(Ordering::SeqCst) {
                return Err(CatalogCallError {
                    code: "unavailable".into(),
                    message: "fusiform is restarting".into(),
                });
            }
            let price = |tier: &str, minor_units, boundary_at_ms| PlanPriceWire {
                provider_id: "openai".into(),
                tier: tier.into(),
                price: Some(PlanAmount {
                    minor_units,
                    exponent: 2,
                    currency: "USD".into(),
                    period: "month".into(),
                }),
                quota_value: None,
                service_tiers: None,
                concurrency: None,
                refusal_reason: None,
                boundary_at_ms,
                established_by: "fusiform".into(),
                established_at_ms: 0,
                review_by_ms: i64::MAX,
                source_ref: "https://example.invalid".into(),
            };
            let mut prices = vec![price("pro_20x", 20_000, 0), price("pro_100", 10_000, 0)];
            if self.future_plan_price.load(Ordering::SeqCst) {
                prices.push(price("pro_20x", 99_000, DAY_START + 1));
            }
            Ok(PlanPricesResponse {
                prices,
                resolved_at_ms: NOW,
                tier_vocabulary: "vendor".into(),
                unit_policy: "list".into(),
                quota_pools: self.quota_pools.lock().unwrap().clone(),
            })
        }
    }

    const DESIRED: &str = r#"{
        "poll_interval_secs": 60,
        "harness_ingest": {"enabled": true, "exclude": []}
    }"#;

    fn publish_desired(state: &Arc<SharedState>, directory: &tempfile::TempDir) {
        publish_document(state, directory, DESIRED);
    }

    fn publish_document(state: &Arc<SharedState>, directory: &tempfile::TempDir, document: &str) {
        let path = directory.path().join("astrocyte.jsonc");
        std::fs::write(&path, document).unwrap();
        reload_config(state, Ok(path)).unwrap();
    }

    #[test]
    fn ingest_exclusions_do_not_define_the_day() {
        for ingest in [
            r#"{"enabled": true, "exclude": ["opencode"]}"#,
            r#"{"enabled": false, "exclude": []}"#,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            let document = DESIRED.replace(
                r#""harness_ingest": {"enabled": true, "exclude": []}"#,
                &format!(r#""harness_ingest": {ingest}"#),
            );
            publish_document(&state, &directory, &document);
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            seed_usage(&state);
            let mut stage = MultiplierStage::new(Some(fusiform.clone()));
            stage.tick(&state, false, NOW);
            assert_eq!(
                current_day_observations(&state),
                [("codex".into(), DAY.into(), 10_000_000)]
            );
            assert!(stage.failure_logged.is_empty());
        }
    }

    fn add_vault_plan(state: &SharedState, id: &str, account: Option<&str>, tier: &str) {
        state
            .ordered_write(|tx| {
                astrocyte_core::vault_plan::append_in_tx(
                    tx,
                    &astrocyte_core::vault_plan::VaultPlan {
                        credential_id: id.into(),
                        provider: Some("openai".into()),
                        account_id: account.map(str::to_owned),
                        plan_tier: Some(tier.into()),
                        plan_key: Some(format!("openai/{tier}")),
                        source: "detected".into(),
                        effective_from_ms: 0,
                        observed_at_ms: 0,
                    },
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    fn install_catalog(state: &SharedState, fusiform: &Arc<FakeFusiform>) {
        let connection: Arc<dyn CatalogConnection> = fusiform.clone();
        state
            .install_catalog(Some(CatalogClient::connect(connection).unwrap()))
            .unwrap();
    }

    /// Dense, unsaturated quota history and 1,000 opencode/openai tokens inside
    /// the seven days before `DAY_START`, with no active reader runtime rows.
    fn seed_usage(state: &SharedState) {
        seed_usage_with(state, true);
    }

    fn seed_usage_with(state: &SharedState, with_event: bool) {
        seed_usage_with_window(state, with_event, Some(10080));
    }

    fn seed_usage_with_window(state: &SharedState, with_event: bool, minutes: Option<i64>) {
        state
            .ordered_write(|tx| {
                astrocyte_core::vault_plan::append_in_tx(tx,&astrocyte_core::vault_plan::VaultPlan {
                    credential_id:"fixture:acct".into(),provider:Some("openai".into()),account_id:Some("acct".into()),
                    plan_tier:Some("pro_20x".into()),plan_key:Some("openai/pro_20x".into()),source:"config".into(),
                    effective_from_ms:0,observed_at_ms:0,
                }).map_err(|error|error.to_string())?;
                // The leased fixture seeds BROCA for drain tests; this fixture
                // has no active store reader unless a test adds one explicitly.
                tx.execute("DELETE FROM source_runtime", [])
                    .map_err(|error| error.to_string())?;
                 tx.execute("UPDATE cortexkit_schema_version SET applied_at_unix=0 WHERE namespace='astrocyte' AND version=68", [])
                     .map_err(|error| error.to_string())?;
                 for minute in (0..=7 * 24 * 60).step_by(5) {
                    let at_ms = DAY_START - 7 * DAY_MS + minute * 60_000;
                    let used = minute as f64 / 240.0;
                    tx.execute(
                        "INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider)
                         VALUES ('codex','acct','primary',?1,?2,?3,'2024-10-10T00:00:00Z','all','openai')",
                         params![at_ms * 1_000_000, used, minutes],
                    )
                    .map_err(|error| error.to_string())?;
                }
                 if with_event {
                 tx.execute(
                     "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref)
                      VALUES ('e1','opencode','lineage','e1',?1,?1,'openai','m','subscription_included','acct')",
                    [DAY_START - 2 * DAY_MS],
                )
                .map_err(|error| error.to_string())?;
                tx.execute(
                     "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                      VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'input','token','billed','usage.input',1000,0)",
                     [],
                 )
                 .map_err(|error| error.to_string())?;
                 }
                Ok(((), true))
            })
            .unwrap();
    }

    fn current_day_observations(state: &SharedState) -> Vec<(String, String, i64)> {
        state
            .with_read_op(|conn| {
                let mut statement = conn
                    .prepare("SELECT quota_provider, observation_day, api_nanos FROM multiplier_observation WHERE observation_day=?1 ORDER BY observation_id")
                    .unwrap();
                let rows = statement
                    .query_map([DAY], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                Ok(rows)
            })
            .unwrap()
    }

    /// A catalog move that changes no price, such as a grade write, must not
    /// re-read every rate timeline; a rates-version move must. An older
    /// fusiform serves no rates version (0), and then any catalog move
    /// re-reads, as before rates_version existed.
    #[test]
    fn rate_timelines_are_reread_only_when_the_rates_version_moves() {
        for served in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            publish_desired(&state, &directory);
            seed_usage(&state);
            let fusiform = FakeFusiform::new(false);
            if served {
                fusiform.rates_version.store(5, Ordering::SeqCst);
            }
            install_catalog(&state, &fusiform);
            let mut stage = MultiplierStage::new(Some(fusiform.clone()));
            stage.tick(&state, false, NOW);
            let first = fusiform.history_calls.load(Ordering::SeqCst);
            assert!(first > 0, "the first tick reads the timelines");
            stage.tick(&state, false, NOW);
            assert_eq!(fusiform.history_calls.load(Ordering::SeqCst), first);

            fusiform.other_catalog_moves.fetch_add(1, Ordering::SeqCst);
            stage.tick(&state, false, NOW);
            let after_grade = fusiform.history_calls.load(Ordering::SeqCst);
            if served {
                assert_eq!(after_grade, first, "a grade write re-read the timelines");
                fusiform.rates_version.store(6, Ordering::SeqCst);
                stage.tick(&state, false, NOW);
                assert!(fusiform.history_calls.load(Ordering::SeqCst) > first);
            } else {
                assert!(
                    after_grade > first,
                    "an older fusiform keys on the catalog version"
                );
            }
        }
    }

    #[test]
    fn multiplier_values_reasoning_only_with_the_served_billing_rule() {
        for declared in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            publish_desired(&state, &directory);
            seed_usage(&state);
            state.ordered_write(|tx| {
                tx.execute("INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                    VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'reasoning','token','billed','usage.reasoning',500,0)", [])
                    .map_err(|error| error.to_string())?;
                Ok(((), true))
            }).unwrap();
            let fusiform = FakeFusiform::new(false);
            fusiform.reasoning_billing.store(declared, Ordering::SeqCst);
            install_catalog(&state, &fusiform);
            MultiplierStage::new(Some(fusiform)).tick(&state, false, NOW);
            assert_eq!(
                current_day_observations(&state),
                [(
                    "codex".into(),
                    DAY.into(),
                    if declared { 20_000_000 } else { 0 }
                )]
            );
            let recorded: String = state.with_conn(|conn| conn.query_row(
                "SELECT reasoning_billed_as_output FROM multiplier_observation WHERE observation_day=?1", [DAY], |row| row.get(0))
                .map_err(|error| error.to_string())).unwrap();
            assert_eq!(recorded, "[]");
        }
    }

    #[test]
    fn cache_write_ttl_multiplier_day_is_recorded_and_values_split_writes() {
        for missing_lifetime in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            publish_desired(&state, &directory);
            seed_usage(&state);
            state
                .ordered_write(|tx| {
                    let measurements = [
                        ("token", 100),
                        ("token_cache_5m", 200),
                        ("token_cache_1h", 300),
                    ]
                    .into_iter()
                    .chain(missing_lifetime.then_some(("token_cache_12h", 400)));
                    for (unit, quantity) in measurements {
                        tx.execute(
                            "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                             VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'cache_write',?1,'billed','usage.cache_write',?2,0)",
                            params![unit, quantity],
                        )
                        .map_err(|error| error.to_string())?;
                    }
                    Ok(((), true))
                })
                .unwrap();
            let fusiform = FakeFusiform::new(false);
            fusiform.cache_write_ttl.store(true, Ordering::SeqCst);
            install_catalog(&state, &fusiform);
            MultiplierStage::new(Some(fusiform)).tick(&state, false, NOW);
            assert_eq!(
                current_day_observations(&state),
                [(
                    "codex".into(),
                    DAY.into(),
                    if missing_lifetime { 0 } else { 19_500_000 }
                )]
            );
            let unpriced: String = state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT unpriced_tokens FROM multiplier_observation WHERE observation_day=?1",
                        [DAY],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap();
            let unpriced: serde_json::Value = serde_json::from_str(&unpriced).unwrap();
            if missing_lifetime {
                // Missing rates exclude their hour from value and quota points,
                // but do not fail the day or claim the lifetime's price is zero.
                assert_eq!(unpriced[0]["reason"], "no_rate");
                assert_eq!(unpriced[0]["tokens"], 400);
            } else {
                assert_eq!(unpriced, json!([]));
            }
        }
    }

    #[test]
    fn unit_billing_multiplier_day_ignores_non_token_catalog_rates() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        seed_usage(&state);
        let fusiform = FakeFusiform::new(false);
        fusiform.unit_billing.store(true, Ordering::SeqCst);
        install_catalog(&state, &fusiform);
        MultiplierStage::new(Some(fusiform)).tick(&state, false, NOW);
        assert_eq!(
            current_day_observations(&state),
            [("codex".into(), DAY.into(), 10_000_000)]
        );
    }

    fn observation_days(state: &SharedState) -> Vec<String> {
        state
            .with_read_op(|conn| {
                let mut statement = conn
                    .prepare("SELECT observation_day FROM multiplier_observation ORDER BY observation_id")
                    .unwrap();
                Ok(statement
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap())
            })
            .unwrap()
    }

    fn source_success(state: &SharedState, source: &str, last_success_ms: Option<i64>) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO source_runtime
                     (source_key,source_module,desired_generation,last_success_ms,pending_known,pending_unknown)
                      VALUES (?1,?1,(SELECT generation FROM desired_config WHERE id = 1),?2,0,?3)",
                    params![source, last_success_ms, i64::from(last_success_ms.is_none())],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn a_day_that_failed_until_midnight_is_recorded_on_a_later_tick() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(true);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(observation_days(&state).is_empty());
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + DAY_MS);
        assert!(observation_days(&state).contains(&DAY.to_string()));
        let before = all_rows(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + DAY_MS + 60_000);
        assert_eq!(
            all_rows(&state),
            before,
            "restarts must not add another original"
        );
    }

    #[test]
    fn a_day_eight_days_back_is_never_attempted_automatically() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(true);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        fusiform.plan_calls.store(0, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 8 * DAY_MS);
        assert_eq!(
            recorded_provider_days(&state)
                .into_iter()
                .map(|(_, day, _)| day)
                .collect::<Vec<_>>(),
            (2..=8)
                .map(|offset| multiplier::utc_day(DAY_START + offset * DAY_MS).unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            observation_days(&state).len(),
            6,
            "the last day has no in-interval readings"
        );
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn two_missed_days_are_recorded_oldest_first() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        let before = observation_days(&state).len();
        fusiform.plan_prices_fail.store(true, Ordering::SeqCst);
        stage.tick(&state, false, NOW + DAY_MS);
        stage.tick(&state, false, NOW + 2 * DAY_MS);
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 2 * DAY_MS + 60_000);
        assert_eq!(
            &observation_days(&state)[before..],
            ["2024-10-05", "2024-10-06"]
        );
    }

    #[test]
    fn stale_multiplier_day_is_superseded_once_when_late_usage_drains() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", None);
        let timed_out = DAY_START + MULTIPLIER_SOURCE_WAIT_MS;
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, timed_out);
        let original = all_rows(&state);
        let report = || {
            crate::ops::dispatch(
                &state,
                crate::ops::OP_MULTIPLIER_REPORT,
                json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
            )
            .unwrap()
        };
        let stale = &report()["series"][0];
        assert_eq!(stale["source_backlog"], json!(["opencode"]));
        assert_eq!(stale["api_nanos"], 10_000_000);
        stage.tick(&state, false, timed_out + 1);
        assert_eq!(
            all_rows(&state),
            original,
            "do not recompute while a source is still behind"
        );
        state
            .ordered_write(|tx| {
                tx.execute("DELETE FROM source_runtime WHERE source_key='opencode'", [])
                    .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
        stage.tick(&state, false, timed_out + 2);
        assert_eq!(
            all_rows(&state),
            original,
            "a source disappearing does not prove it drained"
        );

        record_late_usage(&state, timed_out + 3);
        source_success(&state, "opencode", Some(timed_out + 3));
        let mut restarted = MultiplierStage::new(Some(fusiform.clone()));
        restarted.tick(&state, false, timed_out + 3);
        let repaired = &report()["series"][0];
        assert_eq!(repaired["api_nanos"], 20_000_000);
        assert_eq!(repaired["source_backlog"], json!([]));
        assert_eq!(repaired["supersedes"], stale["observation_id"]);
        let history = all_rows(&state);
        assert_eq!(&history[..original.len()], original);
        assert!(history.len() > original.len());
        restarted.tick(&state, false, timed_out + 4);
        MultiplierStage::new(Some(fusiform)).tick(&state, false, timed_out + 5);
        assert_eq!(
            all_rows(&state),
            history,
            "the repair is not repeated, even after restart"
        );
    }

    #[test]
    fn the_day_waits_for_active_sources_and_includes_their_late_usage() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", Some(DAY_START + 1));
        source_success(&state, "opencode-v2", Some(DAY_START - 1));
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(!observation_days(&state).contains(&DAY.to_string()));
        record_late_usage(&state, NOW);
        source_success(&state, "opencode-v2", Some(NOW + 60_000));
        stage.tick(&state, false, NOW + 60_000);
        let day_rows: Vec<_> = current_day_observations(&state)
            .into_iter()
            .filter(|row| row.1 == DAY)
            .collect();
        assert_eq!(day_rows, [("codex".into(), DAY.into(), 20_000_000)]);
        stage.tick(&state, false, NOW + 120_000);
        assert_eq!(
            observation_days(&state)
                .iter()
                .filter(|day| *day == DAY)
                .count(),
            1
        );
    }

    fn record_late_usage(state: &SharedState, recorded_at_ms: i64) {
        state.ordered_write(|tx| {
            tx.execute(
                "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref)
                 VALUES ('late','opencode','lineage','late',?1,?2,'openai','m','subscription_included','acct')",
                params![DAY_START - DAY_MS, recorded_at_ms],
            ).map_err(|error| error.to_string())?;
            tx.execute(
                "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES ((SELECT event_key FROM usage_event WHERE event_id='late'),'input','token','billed','usage.input',1000,0)", [],
            ).map_err(|error| error.to_string())?;
            Ok(((), true))
        }).unwrap();
    }

    #[test]
    fn backfill_waits_for_the_first_source_drain_but_has_a_startup_deadline() {
        let started_at_ms = DAY_START + 2 * DAY_MS;
        let before_deadline = started_at_ms + 7_200_000 - 1;
        for drained in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            publish_desired(&state, &directory);
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            seed_usage(&state);
            source_success(&state, "opencode", None);
            let mut stage = MultiplierStage::starting_at(Some(fusiform), started_at_ms);
            stage.tick(&state, false, started_at_ms + 1);
            assert!(current_day_observations(&state).is_empty());
            assert!(!recorded_provider_days(&state)
                .iter()
                .any(|(_, day, _)| day == DAY));

            let record_at = if drained {
                let drained_at = started_at_ms + 2;
                source_success(&state, "opencode", Some(drained_at));
                drained_at
            } else {
                stage.tick(&state, false, before_deadline);
                assert!(current_day_observations(&state).is_empty());
                before_deadline + 1
            };
            stage.tick(&state, false, record_at);
            assert_eq!(
                current_day_observations(&state),
                [("codex".into(), DAY.into(), 10_000_000)],
                "after sources drain or the deadline elapses"
            );
            let report = crate::ops::dispatch(
                &state,
                crate::ops::OP_MULTIPLIER_REPORT,
                json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
            )
            .unwrap();
            let row = &report["series"][0];
            assert!(row["subscription_nanos"].as_i64().unwrap() > 0);
            if drained {
                assert_eq!(row["source_backlog"], json!([]));
            } else {
                assert_eq!(row["source_backlog"], json!(["opencode"]));
                assert_eq!(row["routing_eligible"], false);
                assert!(row["eligibility_reasons"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("source_backlog")));
            }
        }
    }

    #[test]
    fn recompute_creates_a_missing_day_without_supersedes_and_refuses_unknown_provider() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let outcome = recompute(&state, &fusiform, DAY, Some("codex"), NOW).unwrap();
        assert!(outcome["recomputed"][0]["supersedes"].is_null());
        assert_eq!(
            current_day_observations(&state),
            [("codex".into(), DAY.into(), 10_000_000)]
        );
        let before = all_rows(&state);
        assert_eq!(
            recompute(&state, &fusiform, DAY, Some("unknown"), NOW)
                .unwrap_err()
                .0,
            "unknown_quota_provider"
        );
        assert_eq!(all_rows(&state), before);
        let again = recompute(&state, &fusiform, DAY, Some("codex"), NOW + 1).unwrap();
        assert_eq!(
            again["recomputed"][0]["supersedes"],
            outcome["recomputed"][0]["observation_id"]
        );
    }

    #[test]
    fn recompute_rechecks_even_an_existing_days_sources() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", None);
        assert_eq!(
            recompute(&state, &fusiform, DAY, Some("codex"), NOW)
                .unwrap_err()
                .0,
            "source_reads_pending"
        );
        assert!(all_rows(&state).is_empty());
        let created =
            recompute(&state, &fusiform, DAY, Some("codex"), DAY_START + 7_200_000).unwrap();
        assert!(created["recomputed"][0]["supersedes"].is_null());
        let text: String = state
            .with_read_op(|conn| {
                conn.query_row(
                    "SELECT error_deflating FROM multiplier_observation",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap();
        assert!(text.contains("stale_sources: opencode"), "{text}");
        assert_eq!(
            recompute(&state, &fusiform, DAY, Some("codex"), NOW)
                .unwrap_err()
                .0,
            "source_reads_pending"
        );
        let corrected =
            recompute(&state, &fusiform, DAY, Some("codex"), DAY_START + 7_200_001).unwrap();
        assert_eq!(
            corrected["recomputed"][0]["supersedes"],
            created["recomputed"][0]["observation_id"]
        );
        assert_eq!(all_rows(&state).len(), 2);
    }

    #[test]
    fn a_pre_multiplier_generation_does_not_gate_the_provider_day() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("ledger.db");
        {
            let store = open_sqlite(&StorageDescriptor {
                module_id: "ck-astrocyte-leased-test".to_string(),
                storage_namespace: "astrocyte".to_string(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: database.to_string_lossy().into_owned(),
                },
            })
            .unwrap();
            let before_multiplier = MIGRATIONS
                .iter()
                .position(|migration| migration.version == 15)
                .unwrap();
            store
                .migrate("astrocyte", &MIGRATIONS[..before_multiplier])
                .unwrap();
        }
        {
            let conn = rusqlite::Connection::open(&database).unwrap();
            conn.execute(
                "UPDATE desired_config SET generation=3, poll_interval_secs=45,
                     content_hash='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                 WHERE id=1",
                [],
            )
            .unwrap();
            conn.execute("UPDATE source_runtime SET desired_generation=(SELECT generation FROM desired_config WHERE id=1)", [])
                .unwrap();
        }
        let state = Arc::new(state::leased_test_state(&directory));
        assert_eq!(state.desired_generation(), 3);
        assert_eq!(state.desired_config().poll_interval_secs, 45);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);

        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert_eq!(
            current_day_observations(&state),
            [("codex".into(), DAY.into(), 10_000_000)]
        );
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 7);

        publish_desired(&state, &directory);
        assert_eq!(state.desired_generation(), 4);
        stage.tick(&state, false, NOW);
        stage.tick(&state, false, NOW + 60_000);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + 120_000);
        // 1,000 tokens at $10 per million.
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 7);
    }

    #[test]
    fn stored_generation_hydrates_without_reading_removed_members() {
        let directory = tempfile::tempdir().unwrap();
        let old = json!({
            "poll_interval_secs": 45,
            "harness_ingest": { "enabled": true, "exclude": ["omp"] },
            "reasoning_billed_as_output": ["openai"],
            "subscription_multiplier": {
                "api_twins": {"openai-codex":"openai"},
                "plan_price_overrides": {},
                "quota_providers": {"codex": {"accounts":{"old":{"plan":"openai/pro_20x","price_override":null}}}}
            }
        });
        let bytes = serde_json::to_vec(&old).unwrap();
        let column = old["subscription_multiplier"].to_string();
        {
            let state = state::leased_test_state(&directory);
            state.ordered_write(|tx| {
                tx.execute("UPDATE desired_config SET generation=3,poll_interval_secs=45,harness_ingest=?1,canonical_bytes=?2,subscription_multiplier=?3 WHERE id=1",
                    params![old["harness_ingest"].to_string(), bytes, column]).map_err(|error| error.to_string())?;
                tx.execute("UPDATE source_runtime SET desired_generation=3", []).map_err(|error| error.to_string())?;
                Ok(((), true))
            }).unwrap();
        }
        let state = Arc::new(state::leased_test_state(&directory));
        assert_eq!(state.desired_generation(), 3);
        assert_eq!(
            state.desired_config(),
            config::decode(
                r#"{"poll_interval_secs":45,"harness_ingest":{"enabled":true,"exclude":["omp"]}}"#
            )
            .unwrap()
            .config
        );
        state.with_conn(|conn| {
            let stored: (Vec<u8>, String) = conn.query_row("SELECT canonical_bytes,subscription_multiplier FROM desired_config WHERE id=1", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
            assert_eq!(stored, (bytes, column));
            assert!(astrocyte_core::vault_plan::at(conn, 1).unwrap().is_empty());
            Ok(())
        }).unwrap();
        publish_desired(&state, &directory);
        assert_eq!(state.desired_generation(), 4);
        state.with_conn(|conn| {
            let stored: (Vec<u8>, Option<String>) = conn.query_row("SELECT canonical_bytes,subscription_multiplier FROM desired_config WHERE id=1", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
            assert_eq!(stored, (config::intentional_empty().canonical_bytes, None));
            Ok(())
        }).unwrap();
    }

    /// A day whose inputs cannot be read writes nothing and is retried on
    /// every later tick: the first tick after 00:00Z fails, a later one
    /// succeeds, and exactly one observation exists for the day.
    #[test]
    fn a_failed_day_is_retried_until_it_is_recorded_once() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(true);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        stage.tick(&state, false, NOW + 60_000);
        assert!(current_day_observations(&state).is_empty());
        assert_eq!(
            fusiform.plan_calls.load(Ordering::SeqCst),
            14,
            "every tick retries each missing day"
        );
        fusiform.plan_prices_fail.store(false, Ordering::SeqCst);
        stage.tick(&state, false, NOW + 120_000);
        stage.tick(&state, false, NOW + 180_000);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
        assert_eq!(
            fusiform.plan_calls.load(Ordering::SeqCst),
            21,
            "recorded days are not recomputed"
        );
    }

    /// A failure is logged once per day and reason, and a failure only the
    /// configuration can cure once per reason whatever the day.
    #[test]
    fn a_failed_day_is_logged_once_per_day_and_reason() {
        let mut stage = MultiplierStage::new(None);
        let failure = |reason: &str, needs_config| DayFailure {
            reason: reason.into(),
            needs_config,
        };
        assert!(stage.log_failure("codex", "d1", failure("unavailable", false)));
        assert!(!stage.log_failure("codex", "d1", failure("unavailable", false)));
        assert!(stage.log_failure("codex", "d1", failure("refused", false)));
        assert!(stage.log_failure("codex", "d2", failure("refused", false)));
        assert!(!stage.log_failure("codex", "d1", failure("unavailable", false)));
        assert!(!stage.log_failure("codex", "d1", failure("refused", false)));
        assert!(stage.log_failure("codex", "d2", failure("no catalog", true)));
        assert!(
            !stage.log_failure("codex", "d3", failure("no catalog", true)),
            "a configuration-only failure is logged once per reason, not per day"
        );
        assert!(stage.log_failure("codex", "d3", failure("no plan prices", true)));
    }

    /// While fusiform's history boundary is still being discovered the day
    /// waits for it instead of failing, and is recorded once it is known.
    #[test]
    fn a_pending_catalog_defers_the_day_instead_of_failing_it() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        seed_usage(&state);
        let fusiform = FakeFusiform::new(false);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, true, NOW);
        assert!(current_day_observations(&state).is_empty());
        install_catalog(&state, &fusiform);
        stage.tick(&state, false, NOW + 60_000);
        assert_eq!(current_day_observations(&state).len(), 1);
    }

    const DAY: &str = "2024-10-04";

    /// A catalog serving a rate key this decoder does not know cannot be
    /// read, so the day fails as an outage does: no row, the reason logged
    /// once for the day, and the row written on a later tick once the
    /// catalog reads cleanly.
    #[test]
    fn an_unreadable_catalog_writes_no_row_until_it_reads_cleanly() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        fusiform.unrecognized_key.store(true, Ordering::SeqCst);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(
            current_day_observations(&state).is_empty(),
            "an unreadable day writes nothing"
        );
        let (_, logged_day, reason) = stage
            .failure_logged
            .iter()
            .find(|(provider, day, _)| provider == "codex" && day.as_deref() == Some(DAY))
            .unwrap()
            .clone();
        assert_eq!(logged_day.as_deref(), Some(DAY));
        assert!(
            reason.starts_with("rate_fact_unrecognized: ")
                && reason.contains("rate.input.above_context.272k"),
            "{reason}"
        );
        stage.tick(&state, false, NOW + 60_000);
        assert!(
            current_day_observations(&state).is_empty(),
            "a retry of the same catalog fails again"
        );
        assert!(
            !stage.log_failure(
                "codex",
                DAY,
                DayFailure {
                    reason: reason.clone(),
                    needs_config: false,
                }
            ),
            "the same day and reason is already logged"
        );
        fusiform.unrecognized_key.store(false, Ordering::SeqCst);
        // The failed model's timeline read waits before its next try; this
        // stands in for that wait passing.
        stage.rate_backoff.waits.clear();
        stage.tick(&state, false, NOW + 120_000);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), DAY.to_string(), 10_000_000)]
        );
    }

    /// A model the catalog has no row for is something the catalog says, not
    /// a failure to read it: the row is written, its tokens counted unpriced.
    #[test]
    fn no_catalog_coverage_still_writes_the_row_with_unpriced_tokens() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        fusiform.uncovered.store(true, Ordering::SeqCst);
        seed_usage(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), DAY.to_string(), 0)]
        );
        let unpriced: serde_json::Value = state
            .with_read_op(|conn| {
                conn.query_row(
                    "SELECT unpriced_tokens FROM multiplier_observation WHERE observation_day=?1",
                    [DAY],
                    |row| row.get::<_, String>(0),
                )
                .map(|text| serde_json::from_str(&text).unwrap())
                .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap();
        assert_eq!(
            unpriced,
            json!([{"reason":"no_catalog_coverage","model":"opencode/openai/m","role":"input","tokens":1000,"measurements":1}])
        );
        let report = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        let head = &report["latest"][0];
        assert!(head["subscription_nanos"].as_i64().unwrap() > 0);
        assert!(
            head["delta_percent"].as_f64().unwrap() < 42.0,
            "the unknown-price hour is excluded, not the other clean hours"
        );
        assert!(head["fee_paid_nanos"].as_i64().unwrap() > 0);
        assert_eq!(head["multiplier"], serde_json::Value::Null);
        assert_eq!(head["fee_multiplier"], serde_json::Value::Null);
        assert_eq!(head["unpriced_tokens"], unpriced);

        let originals = all_rows(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW + 60_000);
        assert_eq!(
            all_rows(&state),
            originals,
            "an unmeasurable day is complete"
        );
        let outcome = recompute(&state, &fusiform, DAY, None, NOW + 120_000).unwrap();
        assert_eq!(
            outcome["recomputed"][0]["multiplier"],
            serde_json::Value::Null
        );
        assert_eq!(
            outcome["recomputed"][0]["fee_multiplier"],
            serde_json::Value::Null
        );
        let corrected = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        assert_eq!(corrected["series"].as_array().unwrap().len(), 1);
        assert_eq!(corrected["series"][0]["api_nanos"], 0);
        assert_eq!(
            corrected["series"][0]["multiplier"],
            serde_json::Value::Null
        );
        assert_eq!(
            corrected["series"][0]["fee_multiplier"],
            serde_json::Value::Null
        );
        assert_eq!(corrected["series"][0]["unpriced_tokens"], unpriced);
        let pace = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_PACE,
            json!({"quota_provider":"codex", "from_ms":DAY_START - DAY_MS, "to_ms":DAY_START}),
        )
        .unwrap_err();
        assert_eq!(
            pace.0, "no_multiplier_observation",
            "pace never uses an ineligible calibration"
        );
    }

    fn recompute(
        state: &SharedState,
        fusiform: &Arc<FakeFusiform>,
        day: &str,
        quota_provider: Option<&str>,
        now_ms: i64,
    ) -> Result<serde_json::Value, (String, String)> {
        recompute_multiplier_day(
            state,
            Some(fusiform.as_ref()),
            &multiplier::MultiplierRecomputeRequest {
                observation_day: day.into(),
                quota_provider: quota_provider.map(str::to_string),
            },
            now_ms,
        )
    }

    /// Every multiplier row, all columns, to prove a refusal changed nothing.
    fn all_rows(state: &SharedState) -> Vec<Vec<rusqlite::types::Value>> {
        state
            .with_read_op(|conn| {
                let mut statement = conn
                    .prepare("SELECT * FROM multiplier_observation ORDER BY observation_id")
                    .unwrap();
                let count = statement.column_count();
                let rows = statement
                    .query_map([], |row| {
                        (0..count)
                            .map(|index| row.get::<_, rusqlite::types::Value>(index))
                            .collect()
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                Ok(rows)
            })
            .unwrap()
    }

    /// A recompute appends a row superseding the day's head, priced with the
    /// catalog as it is now. The report then serves that row in the day's
    /// slot, and the daily stage does not add another row for the day.
    #[test]
    fn a_recompute_supersedes_the_day_and_the_report_serves_it() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
        let original_id = all_rows(&state).last().unwrap()[0].clone();

        fusiform
            .input_rate_units
            .store(20_000_000_000, Ordering::SeqCst);
        let later = NOW + 3 * DAY_MS;
        let outcome = recompute(&state, &fusiform, DAY, None, later).unwrap();
        assert_eq!(outcome["recomputed"].as_array().unwrap().len(), 1);
        assert_eq!(
            current_day_observations(&state),
            [
                ("codex".to_string(), DAY.to_string(), 10_000_000),
                ("codex".to_string(), DAY.to_string(), 20_000_000),
            ]
        );
        let rusqlite::types::Value::Integer(original_id) = original_id else {
            panic!("observation_id is an integer");
        };
        assert_eq!(outcome["recomputed"][0]["supersedes"], original_id);

        let report = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        assert_eq!(report["latest"].as_array().unwrap().len(), 1);
        assert_eq!(report["latest"][0]["api_nanos"], 20_000_000);
        assert_eq!(report["latest"][0]["supersedes"], original_id);
        assert_eq!(report["latest"][0]["interval_end_ms"], DAY_START);
        assert_eq!(report["latest"][0]["snapshot_at_ms"], later);
        let series = report["series"].as_array().unwrap();
        assert_eq!(series.len(), 1, "one row per provider and day: {series:?}");
        assert_eq!(series[0]["api_nanos"], 20_000_000);

        // The daily stage sees the day as recorded, corrected or not.
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW + 60_000);
        assert_eq!(current_day_observations(&state).len(), 2);

        // A second recompute supersedes the correction, not the original.
        fusiform
            .input_rate_units
            .store(30_000_000_000, Ordering::SeqCst);
        let again = recompute(&state, &fusiform, DAY, Some("codex"), later + 1).unwrap();
        assert_eq!(
            again["recomputed"][0]["supersedes"],
            outcome["recomputed"][0]["observation_id"]
        );
    }

    #[test]
    fn served_recompute_accepts_a_provider_known_only_by_an_empty_day() {
        let directory = tempfile::tempdir().unwrap();
        let state = state::leased_test_state(&directory);
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO multiplier_provider_day VALUES ('empty',?1,?2)",
                    params![DAY, NOW],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        let before = all_rows(&state);
        let request = |provider| json!({"observation_day": DAY, "quota_provider": provider});
        let dispatch =
            |params| crate::ops::dispatch(&state, crate::ops::OP_MULTIPLIER_RECOMPUTE, params);
        assert_eq!(
            dispatch(request("missing")).unwrap_err().0,
            "unknown_quota_provider"
        );
        assert_eq!(
            dispatch(request("empty")).unwrap(),
            json!({"recomputed": []})
        );
        assert_eq!(
            dispatch(json!({"observation_day": DAY})).unwrap(),
            json!({"recomputed": []})
        );
        assert_eq!(all_rows(&state), before);
    }

    /// Each refusal writes nothing: a malformed day, an unknown provider,
    /// and a computation that fails because the catalog still cannot be read.
    #[test]
    fn a_refused_recompute_leaves_the_store_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        MultiplierStage::new(Some(fusiform.clone())).tick(&state, false, NOW);
        let before = all_rows(&state);
        assert_eq!(before.len(), 7);

        let code = |result: Result<serde_json::Value, (String, String)>| result.unwrap_err().0;
        assert_eq!(
            code(recompute(&state, &fusiform, "2024-10-4", None, NOW)),
            "malformed"
        );
        assert_eq!(
            code(recompute(&state, &fusiform, DAY, Some("claude"), NOW)),
            "unknown_quota_provider"
        );
        fusiform.unrecognized_key.store(true, Ordering::SeqCst);
        assert_eq!(
            code(recompute(&state, &fusiform, DAY, None, NOW)),
            "rate_fact_unrecognized"
        );
        assert_eq!(all_rows(&state), before);

        // Dispatch preserves unknown-provider refusal and rejects an operator
        // identity supplied by the caller.
        for (params, expected) in [
            (
                json!({"observation_day": "2024-10-05", "quota_provider": "unknown"}),
                "unknown_quota_provider",
            ),
            (
                json!({"observation_day": DAY, "operator": "someone"}),
                "malformed",
            ),
        ] {
            let (code, message) =
                crate::ops::dispatch(&state, crate::ops::OP_MULTIPLIER_RECOMPUTE, params.clone())
                    .unwrap_err();
            assert_eq!(code, expected, "{params}: {message}");
        }
        assert_eq!(all_rows(&state), before);
    }
}
