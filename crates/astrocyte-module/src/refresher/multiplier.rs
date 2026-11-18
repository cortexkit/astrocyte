//! Daily multiplier observations and rate timeline retries are separate from loop scheduling.

use super::*;
use std::collections::BTreeSet;

pub(super) const DAY_MS: i64 = 86_400_000;
/// Each observation covers the seven UTC days that end at the day boundary.
pub(super) const MULTIPLIER_INTERVAL_MS: i64 = 7 * DAY_MS;
// Preserve a bounded two-hour drain wait after midnight; later diagnostic rows
// may be recorded but stay ineligible until their source backlog is resolved.
const MULTIPLIER_SOURCE_WAIT_MS: i64 = 2 * 3_600_000;

/// Records one subscription multiplier observation per quota provider per UTC
/// day, over the trailing seven days. Missing rows are retried for the seven
/// most recent completed UTC days, oldest first.
///
/// A computation failure writes nothing and is retried on every later tick
/// until recorded or outside the look-back. The table's partial unique index
/// makes original-row retries idempotent, even across restarts. A failure is
/// logged once per day and reason; a configuration-only failure once per reason.
pub(crate) struct MultiplierStage {
    pub(super) plan_prices: Option<Arc<dyn PlanPriceSource>>,
    /// Provider/day pairs known to be recorded within the current look-back.
    recorded: BTreeSet<(String, String)>,
    /// Provider, day (`None` for configuration-only failures), and reason.
    /// Keep every reason so interleaved retries of missed days do not repeat logs.
    failure_logged: BTreeSet<(String, Option<String>, String)>,
    /// The UTC day the "member not recorded" skip was last logged.
    not_recorded_logged: Option<String>,
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
    pub(crate) fn new(plan_prices: Option<Arc<dyn PlanPriceSource>>) -> Self {
        Self {
            plan_prices,
            recorded: BTreeSet::new(),
            failure_logged: BTreeSet::new(),
            not_recorded_logged: None,
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
        let (Some((_, oldest_day)), Some((_, day))) = (days.first(), days.last()) else {
            return;
        };
        let (oldest_day, day) = (oldest_day.clone(), day.clone());
        let config = state.desired_config();
        let multiplier_config = match &config.subscription_multiplier {
            config::MultiplierSetting::Recorded(multiplier_config) => multiplier_config,
            config::MultiplierSetting::NotRecorded => {
                if self.not_recorded_logged.as_ref() != Some(&day) {
                    tracing::info!(
                        target: "multiplier",
                        %day,
                        "daily multiplier skipped: the committed desired generation does not record \
                         the subscription_multiplier member"
                    );
                    self.not_recorded_logged = Some(day);
                }
                return;
            }
        };
        self.recorded.retain(|(_, day)| day >= &oldest_day);
        self.failure_logged
            .retain(|(_, day, _)| day.as_ref().is_none_or(|day| day >= &oldest_day));
        for quota_provider in multiplier_config.quota_providers.keys() {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            for (interval_end_ms, day) in &days {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let (interval_end_ms, day) = (*interval_end_ms, day.clone());
                let recorded_key = (quota_provider.clone(), day.clone());
                if self.recorded.contains(&recorded_key) {
                    continue;
                }
                match state.with_read_op(|conn| {
                    let provider = multiplier::resolve_membership(
                        conn,
                        &multiplier_config.quota_providers[quota_provider],
                        quota_provider,
                        interval_end_ms,
                    )
                    .map_err(|error| (error.code().to_string(), error.message()))?;
                    if provider.members().is_empty() {
                        return Ok(false);
                    }
                    for account in provider.members().values() {
                        if multiplier::observation_plan_head(
                            conn,
                            quota_provider,
                            &day,
                            Some(&account.plan),
                        )
                        .map_err(|error| ("internal".to_string(), error.to_string()))?
                        .is_none()
                        {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }) {
                    Ok(true) => {
                        self.recorded.insert(recorded_key);
                        continue;
                    }
                    Ok(false) => {}
                    Err((_, error)) => {
                        self.log_failure(
                            quota_provider,
                            &day,
                            DayFailure {
                                reason: format!("existence check: {error}"),
                                needs_config: false,
                            },
                        );
                        continue;
                    }
                }
                let not_ingested = scoped_producers_not_ingested(&config, quota_provider);
                if !not_ingested.is_empty() {
                    self.log_failure(
                        quota_provider,
                        &day,
                        DayFailure {
                            reason: format!(
                                "scoped_producer_not_ingested: {}",
                                not_ingested.join(", ")
                            ),
                            needs_config: true,
                        },
                    );
                    continue;
                }
                let catalog = state.catalog_client();
                if catalog.is_none() && catalog_pending {
                    continue;
                }
                let outcome = match catalog {
                    Some(catalog) => record_multiplier_day(
                        state,
                        &catalog,
                        self.plan_prices.as_deref(),
                        multiplier_config,
                        quota_provider,
                        interval_end_ms,
                        now_ms,
                        stop,
                    ),
                    None => Err(DayFailure {
                        reason: "no fusiform catalog is configured, so no API rate can be read"
                            .into(),
                        needs_config: true,
                    }),
                };
                // A day that fails, including one whose catalog cannot be read
                // (`rate_fact_unrecognized`), writes nothing and is retried here
                // on every later tick; `log_failure` logs it once per day and reason
                // so a failure that lasts all day is not repeated every minute.
                match outcome {
                    Ok(None) => return,
                    Ok(Some(observations)) => {
                        self.recorded.insert(recorded_key);
                        for observation in observations {
                            tracing::info!(
                                target: "multiplier",
                                quota_provider,
                                %day,
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
                        self.log_failure(quota_provider, &day, failure);
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

/// The producers a quota provider's scope names that the configuration in
/// force does not ingest: excluded by `harness_ingest`, or all of ingestion
/// off. The quota's used percent still counts their traffic, so a day
/// computed without their tokens would understate the multiplier with nothing
/// to show it; the day fails by name instead.
///
/// This is checked against the configuration, not against `source_runtime`
/// rows: a producer whose store is absent (BROCA not installed included) also
/// has no row, but "nothing on disk to read" loses no tokens, while "told not
/// to read it" does.
///
/// The check reads only the configuration in force now. A producer excluded
/// for part of the trailing seven days and re-enabled before the day is
/// computed still leaves a gap in that interval; nothing records when
/// ingestion was off.
fn scoped_producers_not_ingested(
    config: &config::AstrocyteConfig,
    quota_provider: &str,
) -> Vec<String> {
    let config::MultiplierSetting::Recorded(multiplier) = &config.subscription_multiplier else {
        return Vec::new();
    };
    let Some(provider) = multiplier.quota_providers.get(quota_provider) else {
        return Vec::new();
    };
    provider
        .scope
        .iter()
        .map(|rule| rule.producer.as_str())
        .filter(|producer| !config.harness_ingest.ingests(producer))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Why a day's observation could not be computed: a wire code, its message,
/// and whether only a configuration change can cure it.
pub(crate) struct DayError {
    code: String,
    message: String,
    needs_config: bool,
}

impl From<DayError> for DayFailure {
    fn from(error: DayError) -> Self {
        Self {
            reason: format!("{}: {}", error.code, error.message),
            needs_config: error.needs_config,
        }
    }
}

/// A completed day's tokens may still be waiting in any ingested source, not
/// just a source in this provider's scope. Absent and skipped sources have no
/// runtime row. Bound the wait so a broken source cannot leave a permanent gap.
fn multiplier_stale_sources(
    state: &SharedState,
    interval_end_ms: i64,
    snapshot_at_ms: i64,
    provider: &multiplier::QuotaProviderConfig,
) -> Result<Vec<String>, DayError> {
    let sources = state
        .with_read_op(|conn| {
            let mut statement = conn
                .prepare(
                    "SELECT source_key FROM source_runtime
                      WHERE last_success_ms IS NULL OR last_success_ms < ?1
                         OR pending_known > 0 OR pending_unknown != 0
                     ORDER BY source_key",
                )
                .map_err(|error| ("internal".to_string(), error.to_string()))?;
            let sources = statement
                .query_map([interval_end_ms], |row| row.get::<_, String>(0))
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
                .map_err(|error| ("internal".to_string(), error.to_string()))?;
            Ok(sources)
        })
        .map_err(|(code, message)| DayError {
            code,
            message,
            needs_config: false,
        })?;
    let sources: Vec<_> = sources
        .into_iter()
        .filter(|source| multiplier::source_in_scope(provider, source))
        .collect();
    if !sources.is_empty() && snapshot_at_ms < interval_end_ms + MULTIPLIER_SOURCE_WAIT_MS {
        return Err(DayError {
            code: "source_reads_pending".into(),
            message: format!(
                "waiting for a successful read after midnight: {}",
                sources.join(", ")
            ),
            needs_config: false,
        });
    }
    Ok(sources)
}

fn disclose_stale_sources(observation: &mut multiplier::Observation, sources: &[String]) {
    if !sources.is_empty() {
        observation.source_backlog = sources.to_vec();
        observation.routing_eligible = false;
        observation
            .eligibility_reasons
            .push("source_backlog".into());
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
/// (current identity, its served history, and an alias or twin target's
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
    let config = state.desired_config();
    let empty_twins = std::collections::BTreeMap::new();
    let twins = match &config.subscription_multiplier {
        config::MultiplierSetting::Recorded(config) => &config.api_twins,
        config::MultiplierSetting::NotRecorded => &empty_twins,
    };
    state.ordered_write(|tx| {
        rate_era::discover(tx, 2000).map_err(|error| error.message())?;
        rate_era::prepare(tx, version, twins).map_err(|error| error.message())?;
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
    let config = state.desired_config();
    let empty_twins = std::collections::BTreeMap::new();
    let twins = match &config.subscription_multiplier {
        config::MultiplierSetting::Recorded(config) => &config.api_twins,
        config::MultiplierSetting::NotRecorded => &empty_twins,
    };
    let mut attempted = std::collections::BTreeSet::new();
    while started.elapsed() < RATE_FETCH_PASS_BUDGET {
        let pending = state
            .with_read_op(|conn| {
                rate_era::pending(conn, 256).map_err(|error| (error.code().into(), error.message()))
            })
            .map_err(|(_, message)| message)?;
        let now = std::time::Instant::now();
        let Some((provider, model)) = pending
            .into_iter()
            .find(|model| !attempted.contains(model) && !backoff.waiting(model, now))
        else {
            break;
        };
        attempted.insert((provider.clone(), model.clone()));
        let call = catalog.with_budget(RATE_FETCH_MODEL_BUDGET);
        let fetched = match rate_era::fetch(&call, &provider, &model, twins) {
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
        backoff.succeeded(&(provider, model));
    }
    let remaining = state
        .with_read_op(|conn| {
            rate_era::pending(conn, 256).map_err(|error| (error.code().into(), error.message()))
        })
        .map_err(|(_, message)| message)?;
    let now = std::time::Instant::now();
    Ok(remaining
        .iter()
        .any(|model| !attempted.contains(model) && !backoff.waiting(model, now)))
}

/// Computes one day and appends it as the day's original row.
#[allow(clippy::too_many_arguments)]
fn record_multiplier_day(
    state: &SharedState,
    catalog: &CatalogClient,
    plan_prices: Option<&dyn PlanPriceSource>,
    config: &MultiplierConfig,
    quota_provider: &str,
    interval_end_ms: i64,
    snapshot_at_ms: i64,
    stop: &AtomicBool,
) -> Result<Option<Vec<multiplier::Observation>>, DayFailure> {
    if stop.load(Ordering::Relaxed) {
        return Ok(None);
    }
    let stale_sources = multiplier_stale_sources(
        state,
        interval_end_ms,
        snapshot_at_ms,
        &config.quota_providers[quota_provider],
    )?;
    let mut observations = compute_multiplier_day(
        state,
        catalog,
        plan_prices,
        config,
        quota_provider,
        interval_end_ms,
        snapshot_at_ms,
        true,
    )?;
    // Plan prices are read outside the writer transaction. If shutdown arrived
    // during that call, leave the entire provider/day to the next refresher.
    if stop.load(Ordering::Relaxed) {
        return Ok(None);
    }
    for observation in &mut observations {
        disclose_stale_sources(observation, &stale_sources);
    }
    let written = state.ordered_write(|tx| {
        for observation in &observations {
            if stop.load(Ordering::Relaxed) {
                return Err("multiplier stopped".into());
            }
            if multiplier::observation_plan_head(
                tx,
                &observation.quota_provider,
                &observation.observation_day,
                observation.plan.as_deref(),
            )
            .map_err(|error| error.to_string())?
            .is_none()
            {
                multiplier::insert_observation_in_tx(tx, observation)
                    .map_err(|error| format!("{}: {}", error.code(), error.message()))?;
            }
        }
        if stop.load(Ordering::Relaxed) {
            return Err("multiplier stopped".into());
        }
        Ok(((), true))
    });
    if written.is_err() && stop.load(Ordering::Relaxed) {
        return Ok(None);
    }
    written.map_err(|reason| DayFailure {
        reason,
        needs_config: false,
    })?;
    crate::bus_publications::publish_multiplier(state);
    Ok(Some(observations))
}

/// Read an owned plan, resolve fees, then value it without writing. Automatic
/// days use cached timelines; explicit operator recomputes retain catalog reads.
/// No network call holds a store transaction, and appending is a separate write.
#[allow(clippy::too_many_arguments)]
fn compute_multiplier_day(
    state: &SharedState,
    catalog: &CatalogClient,
    plan_prices: Option<&dyn PlanPriceSource>,
    config: &MultiplierConfig,
    quota_provider: &str,
    interval_end_ms: i64,
    snapshot_at_ms: i64,
    cached: bool,
) -> Result<Vec<multiplier::Observation>, DayError> {
    let failed = |error: multiplier::MultiplierError| DayError {
        code: error.code().to_string(),
        message: error.message(),
        needs_config: false,
    };
    let read_failed = |(code, message): (String, String)| DayError {
        code,
        message,
        needs_config: false,
    };
    let plan = state
        .with_read_op(|conn| {
            multiplier::plan_observation(
                conn,
                config,
                quota_provider,
                interval_end_ms - MULTIPLIER_INTERVAL_MS,
                interval_end_ms,
            )
            .map_err(|error| (error.code().to_string(), error.message()))
        })
        .map_err(read_failed)?;
    let plan_prices =
        multiplier::resolve_plans(plan_prices, config, &plan.provider, snapshot_at_ms).map_err(
            |error| DayError {
                needs_config: plan_prices.is_none(),
                ..failed(error)
            },
        )?;
    let models = multiplier::catalog_models(config, &plan);
    let rates = if cached {
        state
            .with_read_op(|conn| {
                astrocyte_core::rate_era::resolved_rates(conn, &models, snapshot_at_ms)
                    .map_err(|error| (error.code().to_string(), error.message()))
            })
            .map_err(read_failed)?
    } else {
        multiplier::resolve_rates(catalog, &models, snapshot_at_ms).map_err(failed)?
    };
    let observation = state
        .with_read_op(|conn| {
            multiplier::compute_plan_observations(
                conn,
                config,
                &plan,
                &rates,
                &plan_prices,
                snapshot_at_ms,
                &state.desired_config().reasoning_billed_as_output,
            )
            .map_err(|error| (error.code().to_string(), error.message()))
        })
        .map_err(read_failed)?;
    Ok(observation)
}

/// `spend.multiplier.recompute`: computes a day over the interval
/// the daily stage used for it (the seven days ending at its 00:00Z) with the
/// current configuration and catalog, and appends one row per quota provider
/// that supersedes that day's head, or creates its original if none exists.
///
/// Every refusal writes nothing: an unknown provider, a missing day still
/// waiting for source reads, or a computation that fails (an unreadable catalog
/// is the same failure the daily stage retries). All fusiform calls finish
/// before the write transaction opens, and all providers are appended in that
/// one transaction or none is.
///
/// The request carries no operator identity, as for `spend.event.reprice`:
/// the caller is whoever the transport admitted, and the request type refuses
/// any extra field that would let a caller name themselves.
pub(crate) fn recompute_multiplier_day(
    state: &SharedState,
    plan_prices: Option<&dyn PlanPriceSource>,
    request: &multiplier::MultiplierRecomputeRequest,
    now_ms: i64,
) -> Result<serde_json::Value, (String, String)> {
    let config = state.desired_config();
    let config::MultiplierSetting::Recorded(multiplier_config) = &config.subscription_multiplier
    else {
        return Err((
            "multiplier_not_recorded".into(),
            "the committed desired generation does not record the subscription_multiplier member"
                .into(),
        ));
    };
    let day = &request.observation_day;
    let interval_end_ms = multiplier::utc_day_start_ms(day).ok_or_else(|| {
        (
            "malformed".to_string(),
            format!("observation_day {day:?} is not a YYYY-MM-DD date"),
        )
    })?;
    let quota_providers: Vec<String> = match &request.quota_provider {
        Some(quota_provider)
            if multiplier_config
                .quota_providers
                .contains_key(quota_provider) =>
        {
            vec![quota_provider.clone()]
        }
        Some(quota_provider) => {
            return Err((
                "unknown_quota_provider".into(),
                format!("quota provider '{quota_provider}' is not configured"),
            ))
        }
        None => multiplier_config.quota_providers.keys().cloned().collect(),
    };
    if quota_providers.is_empty() {
        return Err((
            "unknown_quota_provider".into(),
            "no quota provider is configured".into(),
        ));
    }
    // Preflight every requested scope before any catalog call: quota usage
    // includes excluded producers, but their tokens would be missing.
    for quota_provider in &quota_providers {
        let not_ingested = scoped_producers_not_ingested(&config, quota_provider);
        if !not_ingested.is_empty() {
            return Err((
                "scoped_producer_not_ingested".into(),
                format!(
                    "quota provider '{quota_provider}': {}",
                    not_ingested.join(", ")
                ),
            ));
        }
    }
    let catalog = state.catalog_client().ok_or_else(|| {
        (
            "catalog_unavailable".to_string(),
            "no fusiform catalog is connected, so no API rate can be read".to_string(),
        )
    })?;
    let mut observations = Vec::new();
    for quota_provider in &quota_providers {
        let stale_sources = multiplier_stale_sources(
            state,
            interval_end_ms,
            now_ms,
            &multiplier_config.quota_providers[quota_provider],
        )
        .map_err(|error| (error.code, error.message))?;
        let mut rows = compute_multiplier_day(
            state,
            &catalog,
            plan_prices,
            multiplier_config,
            quota_provider,
            interval_end_ms,
            now_ms,
            false,
        )
        .map_err(|error| (error.code, error.message))?;
        for observation in &mut rows {
            disclose_stale_sources(observation, &stale_sources);
        }
        observations.extend(rows);
    }
    let reply = state.ordered_op_write(|tx| {
        let mut recomputed = Vec::new();
        for observation in &observations {
            // Recheck inside the writer transaction: another writer may have
            // created this day while its catalog calls were in flight.
            let (observation_id, supersedes) = match multiplier::observation_plan_head(
                tx,
                &observation.quota_provider,
                &observation.observation_day,
                observation.plan.as_deref(),
            )
            .map_err(|error| ("internal".to_string(), error.to_string()))?
            {
                Some(_) => {
                    let superseded =
                        multiplier::insert_superseding_observation_in_tx(tx, observation)
                            .map_err(|error| (error.code().to_string(), error.message()))?;
                    (superseded.observation_id, Some(superseded.supersedes))
                }
                None => {
                    multiplier::insert_observation_in_tx(tx, observation)
                        .map_err(|error| (error.code().to_string(), error.message()))?;
                    (tx.last_insert_rowid(), None)
                }
            };
            recomputed.push(serde_json::json!({
                "quota_provider": observation.quota_provider,
                "plan": observation.plan,
                "observation_day": observation.observation_day,
                "observation_id": observation_id,
                "supersedes": supersedes,
                "multiplier": observation.multiplier,
                "fee_multiplier": observation.fee_multiplier,
            }));
        }
        Ok((serde_json::json!({ "recomputed": recomputed }), true))
    })?;
    crate::bus_publications::publish_multiplier(state);
    Ok(reply)
}

#[cfg(test)]
mod multiplier_stage_tests {
    include!("stop_multiplier_tests.rs");
    include!("../refresher_bus_publications_tests.rs");
    include!("../refresher_multiplier_v2_tests.rs");
    include!("../refresher_rate_era_tests.rs");
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use astrocyte_core::catalog_client::CatalogCallError;
    use astrocyte_core::store::MIGRATIONS;
    use fusiform_protocol::{
        CatalogGetRequest, CatalogGetResponse, PlanAmount, PlanPriceWire, PlanPricesRequest,
        PlanPricesResponse,
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
        plan_prices_fail: std::sync::atomic::AtomicBool,
        input_rate_units: std::sync::atomic::AtomicI64,
        unrecognized_key: std::sync::atomic::AtomicBool,
        unit_billing: std::sync::atomic::AtomicBool,
        cache_write_ttl: std::sync::atomic::AtomicBool,
        uncovered: std::sync::atomic::AtomicBool,
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
                plan_prices_fail: std::sync::atomic::AtomicBool::new(plan_prices_fail),
                input_rate_units: std::sync::atomic::AtomicI64::new(10_000_000_000),
                unrecognized_key: std::sync::atomic::AtomicBool::new(false),
                unit_billing: std::sync::atomic::AtomicBool::new(false),
                cache_write_ttl: std::sync::atomic::AtomicBool::new(false),
                uncovered: std::sync::atomic::AtomicBool::new(false),
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
            if self.uncovered.load(Ordering::SeqCst) {
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
            _request: PlanPricesRequest,
        ) -> Result<PlanPricesResponse, CatalogCallError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            if self.plan_prices_fail.load(Ordering::SeqCst) {
                return Err(CatalogCallError {
                    code: "unavailable".into(),
                    message: "fusiform is restarting".into(),
                });
            }
            Ok(PlanPricesResponse {
                prices: vec![PlanPriceWire {
                    provider_id: "openai".into(),
                    tier: "pro_20x".into(),
                    price: Some(PlanAmount {
                        minor_units: 20_000,
                        exponent: 2,
                        currency: "USD".into(),
                        period: "month".into(),
                    }),
                    quota_value: None,
                    service_tiers: None,
                    concurrency: None,
                    refusal_reason: None,
                    boundary_at_ms: 0,
                    established_by: "fusiform".into(),
                    established_at_ms: 0,
                    review_by_ms: i64::MAX,
                    source_ref: "https://example.invalid".into(),
                }],
                resolved_at_ms: NOW,
                tier_vocabulary: "vendor".into(),
                unit_policy: "list".into(),
            })
        }
    }

    const DESIRED: &str = r#"{
        "poll_interval_secs": 60,
        "qta_connection_file": null,
        "fusiform_connection_file": null,
        "reasoning_billed_as_output": ["openai"],
        "subscription_multiplier": {
            "api_twins": {},
            "plan_price_overrides": {},
            "quota_providers": {"codex": {
                "window": {"pool_key": "primary", "window_minutes": 10080},
                "scope": [{"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null}]
            }}
        },
        "harness_ingest": {"enabled": true, "exclude": []}
    }"#;

    fn publish_desired(state: &Arc<SharedState>, directory: &tempfile::TempDir) {
        publish_document(state, directory, DESIRED);
    }

    fn publish_document(state: &Arc<SharedState>, directory: &tempfile::TempDir, document: &str) {
        let mut document: serde_json::Value = serde_json::from_str(document).unwrap();
        if let Some(providers) =
            document["subscription_multiplier"]["quota_providers"].as_object_mut()
        {
            for provider in providers.values_mut() {
                for field in ["accounts", "credentials"] {
                    if let Some(members) = provider.as_object_mut().unwrap().remove(field) {
                        for (id, member) in members.as_object().unwrap() {
                            let key = member["plan"].as_str().unwrap();
                            let (provider, tier) = key.split_once('/').unwrap();
                            state
                                .ordered_write(|tx| {
                                    astrocyte_core::vault_plan::append_in_tx(
                                        tx,
                                        &astrocyte_core::vault_plan::VaultPlan {
                                            credential_id: if field == "credentials" {
                                                id.clone()
                                            } else {
                                                format!("fixture:{id}")
                                            },
                                            provider: Some(provider.into()),
                                            account_id: (field == "accounts").then(|| id.clone()),
                                            plan_tier: Some(tier.into()),
                                            plan_key: Some(key.into()),
                                            source: "config".into(),
                                            effective_from_ms: 0,
                                            observed_at_ms: 0,
                                        },
                                    )
                                    .map_err(|error| error.to_string())?;
                                    Ok(((), true))
                                })
                                .unwrap();
                        }
                    }
                }
            }
        }
        let path = directory.path().join("astrocyte.jsonc");
        std::fs::write(&path, document.to_string()).unwrap();
        reload_config(state, Ok(path)).unwrap();
    }

    /// `DESIRED` with its `harness_ingest` member replaced.
    fn desired_with_ingest(ingest: &str) -> String {
        DESIRED.replace(
            r#""harness_ingest": {"enabled": true, "exclude": []}"#,
            &format!(r#""harness_ingest": {ingest}"#),
        )
    }

    /// A reason the stage logged for the `codex` quota provider.
    fn codex_failure(stage: &MultiplierStage) -> Option<String> {
        stage
            .failure_logged
            .iter()
            .find(|(provider, _, _)| provider == "codex")
            .map(|(_, _, reason)| reason.clone())
    }

    /// A scope naming a producer the configuration excludes would count none
    /// of its tokens while the quota's used percent still counts its traffic.
    /// The day fails by name instead, writes nothing, and makes no fusiform
    /// call; it is recorded once the producer is ingested again.
    #[test]
    fn a_scope_naming_an_excluded_producer_fails_the_day_by_name() {
        for ingest in [
            r#"{"enabled": true, "exclude": ["opencode"]}"#,
            r#"{"enabled": false, "exclude": []}"#,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            publish_document(&state, &directory, &desired_with_ingest(ingest));
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            seed_usage(&state);
            let mut stage = MultiplierStage::new(Some(fusiform.clone()));
            stage.tick(&state, false, NOW);
            assert!(current_day_observations(&state).is_empty(), "{ingest}");
            assert_eq!(
                codex_failure(&stage).as_deref(),
                Some("scoped_producer_not_ingested: opencode"),
                "{ingest}"
            );
            assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0, "{ingest}");

            publish_desired(&state, &directory);
            stage.tick(&state, false, NOW + 60_000);
            assert_eq!(
                current_day_observations(&state),
                [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)],
                "{ingest}"
            );
        }
    }

    /// Excluding a producer the scope does not name changes nothing: the day
    /// is recorded as before.
    #[test]
    fn excluding_a_producer_outside_every_scope_still_records_the_day() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_document(
            &state,
            &directory,
            &desired_with_ingest(r#"{"enabled": true, "exclude": ["goose", "broca"]}"#),
        );
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
        assert_eq!(codex_failure(&stage), None);
    }

    /// BROCA is read from its store like every other reader and needs no
    /// configuration, so a scope naming it is refused only when BROCA is
    /// excluded. An absent store (BROCA not installed) is not a refusal:
    /// nothing on disk means no BROCA tokens exist to leave out.
    #[test]
    fn a_scope_naming_broca_is_refused_only_when_broca_is_excluded() {
        let with_broca_scope = |document: &str| {
            document.replace(
                r#""scope": [{"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null}]"#,
                r#""scope": [{"producer": "opencode", "provider": "openai", "charge_basis": null, "model_prefix": null}, {"producer": "broca", "provider": "openai", "charge_basis": "subscription_included", "model_prefix": null}]"#,
            )
        };
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_document(
            &state,
            &directory,
            &with_broca_scope(&desired_with_ingest(
                r#"{"enabled": true, "exclude": ["broca"]}"#,
            )),
        );
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(current_day_observations(&state).is_empty());
        assert_eq!(
            codex_failure(&stage).as_deref(),
            Some("scoped_producer_not_ingested: broca")
        );

        publish_document(&state, &directory, &with_broca_scope(DESIRED));
        stage.tick(&state, false, NOW + 60_000);
        assert_eq!(
            current_day_observations(&state),
            [("codex".to_string(), "2024-10-04".to_string(), 10_000_000)]
        );
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
                for minute in (0..=7 * 24 * 60).step_by(5) {
                    let at_ms = DAY_START - 7 * DAY_MS + minute * 60_000;
                    let used = minute as f64 / 240.0;
                    tx.execute(
                        "INSERT INTO window_sample (provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at)
                         VALUES ('codex','acct','primary',?1,?2,10080,'2024-10-10T00:00:00Z')",
                        params![at_ms * 1_000_000, used],
                    )
                    .map_err(|error| error.to_string())?;
                }
                tx.execute(
                    "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis)
                     VALUES ('e1','opencode','lineage','e1',?1,?1,'openai','m','unknown')",
                    [DAY_START - 2 * DAY_MS],
                )
                .map_err(|error| error.to_string())?;
                tx.execute(
                    "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                     VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'input','token','billed','usage.input',1000,0)",
                    [],
                )
                .map_err(|error| error.to_string())?;
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
    fn multiplier_uses_and_records_top_level_reasoning_billing() {
        for declared in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            let document = if declared {
                DESIRED.to_owned()
            } else {
                DESIRED.replace(
                    "\"reasoning_billed_as_output\": [\"openai\"]",
                    "\"reasoning_billed_as_output\": []",
                )
            };
            publish_document(&state, &directory, &document);
            seed_usage(&state);
            state.ordered_write(|tx| {
                tx.execute("INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                    VALUES ((SELECT event_key FROM usage_event WHERE event_id='e1'),'reasoning','token','billed','usage.reasoning',500,0)", [])
                    .map_err(|error| error.to_string())?;
                Ok(((), true))
            }).unwrap();
            let fusiform = FakeFusiform::new(false);
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
            assert_eq!(recorded, if declared { "[\"openai\"]" } else { "[]" });
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
            observation_days(&state),
            (2..=8)
                .map(|offset| multiplier::utc_day(DAY_START + offset * DAY_MS).unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 7);
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
    fn the_day_waits_for_every_in_scope_source_and_includes_its_late_usage() {
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
        state.ordered_write(|tx| {
            tx.execute(
                "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis)
                 VALUES ('late','opencode','lineage','late',?1,?2,'openai','m','unknown')",
                params![DAY_START - DAY_MS, NOW],
            ).map_err(|error| error.to_string())?;
            tx.execute(
                "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES ((SELECT event_key FROM usage_event WHERE event_id='late'),'input','token','billed','usage.input',1000,0)", [],
            ).map_err(|error| error.to_string())?;
            Ok(((), true))
        }).unwrap();
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

    #[test]
    fn at_two_hours_the_day_records_and_names_all_stale_sources() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        source_success(&state, "opencode", None);
        source_success(&state, "opencode-v2", Some(DAY_START - 1));
        source_success(&state, "codex", Some(DAY_START + 1));
        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, DAY_START + 7_200_000 - 1);
        assert!(!observation_days(&state).contains(&DAY.to_string()));
        stage.tick(&state, false, DAY_START + 7_200_000);
        let text: String = state
            .with_read_op(|conn| {
                conn.query_row(
                    "SELECT error_deflating FROM multiplier_observation WHERE observation_day=?1",
                    [DAY],
                    |row| row.get(0),
                )
                .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap();
        assert!(
            text.contains("stale_sources: opencode, opencode-v2"),
            "{text}"
        );
        assert!(!text.contains("codex"), "{text}");
        let report = crate::ops::dispatch(
            &state,
            crate::ops::OP_MULTIPLIER_REPORT,
            json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
        )
        .unwrap();
        let row = &report["series"][0];
        assert_eq!(row["routing_eligible"], false);
        assert_eq!(row["source_backlog"], json!(["opencode", "opencode-v2"]));
        assert!(row["eligibility_reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "source_backlog"));
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

    /// A store whose committed generation predates the multiplier column
    /// boots with the member "not recorded" -- not as an empty declaration --
    /// and the daily stage writes nothing. The next reload from a valid file
    /// publishes a generation that records the member, and the stage then
    /// records the day exactly once, across repeated ticks and a restart.
    #[test]
    fn a_pre_multiplier_generation_records_nothing_until_a_reload_publishes_the_member() {
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
        assert_eq!(
            state.desired_config().subscription_multiplier,
            config::MultiplierSetting::NotRecorded
        );
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);

        let mut stage = MultiplierStage::new(Some(fusiform.clone()));
        stage.tick(&state, false, NOW);
        assert!(current_day_observations(&state).is_empty());
        assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0);

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
    fn stored_generations_lift_reasoning_billing_and_new_generations_keep_it() {
        for nested in [None, Some(serde_json::json!(["openai", "anthropic"]))] {
            let directory = tempfile::tempdir().unwrap();
            let mut old: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
            old.as_object_mut()
                .unwrap()
                .remove("reasoning_billed_as_output");
            if let Some(providers) = &nested {
                old["subscription_multiplier"]["reasoning_billed_as_output"] = providers.clone();
            }
            {
                let state = state::leased_test_state(&directory);
                state.ordered_write(|tx| {
                    tx.execute("UPDATE desired_config SET generation=3,canonical_bytes=?1,subscription_multiplier=?2 WHERE id=1",
                        rusqlite::params![old.to_string().as_bytes(), old["subscription_multiplier"].to_string()])
                        .map_err(|error| error.to_string())?;
                    tx.execute("UPDATE source_runtime SET desired_generation=3", []).map_err(|error| error.to_string())?;
                    Ok(((), true))
                }).unwrap();
            }
            let state = Arc::new(state::leased_test_state(&directory));
            let expected = if nested.is_some() {
                vec!["openai", "anthropic"]
            } else {
                vec![]
            };
            assert_eq!(state.desired_config().reasoning_billed_as_output, expected);
            assert!(matches!(
                state.desired_config().subscription_multiplier,
                config::MultiplierSetting::Recorded(_)
            ));
            publish_desired(&state, &directory);
            drop(state);
            let state = state::leased_test_state(&directory);
            assert_eq!(
                state.desired_config().reasoning_billed_as_output,
                ["openai"]
            );
            state
                .with_conn(|conn| {
                    let stored: String = conn
                        .query_row(
                            "SELECT subscription_multiplier FROM desired_config",
                            [],
                            |row| row.get(0),
                        )
                        .map_err(|error| error.to_string())?;
                    assert!(!stored.contains("reasoning_billed_as_output"));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn stored_generations_without_model_prefix_hydrate_as_null() {
        let directory = tempfile::tempdir().unwrap();
        let mut old: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
        old["subscription_multiplier"]["quota_providers"]["codex"]["scope"][0]
            .as_object_mut()
            .unwrap()
            .remove("model_prefix");
        {
            let state = state::leased_test_state(&directory);
            state.ordered_write(|tx| {
                tx.execute("UPDATE desired_config SET generation=3,canonical_bytes=?1,subscription_multiplier=?2 WHERE id=1",
                    rusqlite::params![old.to_string().as_bytes(), old["subscription_multiplier"].to_string()])
                    .map_err(|error| error.to_string())?;
                tx.execute("UPDATE source_runtime SET desired_generation=3", []).map_err(|error| error.to_string())?;
                Ok(((), true))
            }).unwrap();
        }
        let state = Arc::new(state::leased_test_state(&directory));
        let config::MultiplierSetting::Recorded(multiplier) =
            state.desired_config().subscription_multiplier
        else {
            panic!("multiplier expected")
        };
        assert_eq!(
            multiplier.quota_providers["codex"].scope[0].model_prefix,
            None
        );
        assert_eq!(state.desired_generation(), 3);
        publish_desired(&state, &directory);
        state
            .with_conn(|conn| {
                let stored: String = conn
                    .query_row(
                        "SELECT subscription_multiplier FROM desired_config",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())?;
                let stored: serde_json::Value = serde_json::from_str(&stored).unwrap();
                assert_eq!(
                    stored["quota_providers"]["codex"]["scope"][0].get("model_prefix"),
                    Some(&serde_json::Value::Null)
                );
                Ok(())
            })
            .unwrap();
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

    /// A recompute must not undercount an excluded producer's tokens. Even
    /// an all-provider request checks every scope before calling the catalog.
    #[test]
    fn served_recompute_refuses_excluded_scope_without_calls_or_rows() {
        for quota_provider in [None, Some("z-excluded")] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(state::leased_test_state(&directory));
            let mut document: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
            document["subscription_multiplier"]["plan_price_overrides"]["openai/pro_20x"] =
                json!({"minor_units": 20000, "exponent": 2, "currency": "USD", "period": "month"});
            let mut excluded =
                document["subscription_multiplier"]["quota_providers"]["codex"].clone();
            excluded["scope"][0]["producer"] = json!("broca");
            document["subscription_multiplier"]["quota_providers"]["z-excluded"] = excluded;
            document["harness_ingest"]["exclude"] = json!(["broca"]);
            publish_document(&state, &directory, &document.to_string());
            let fusiform = FakeFusiform::new(false);
            install_catalog(&state, &fusiform);
            seed_usage(&state);
            let calls_before = fusiform.catalog_calls.load(Ordering::SeqCst);
            assert!(all_rows(&state).is_empty());

            let mut params = json!({"observation_day": DAY});
            if let Some(provider) = quota_provider {
                params["quota_provider"] = json!(provider);
            }
            let (code, message) =
                crate::ops::dispatch(&state, crate::ops::OP_MULTIPLIER_RECOMPUTE, params)
                    .unwrap_err();
            assert_eq!(code, "scoped_producer_not_ingested", "{message}");
            assert!(
                message.contains("broca") && message.contains("z-excluded"),
                "{message}"
            );
            assert!(all_rows(&state).is_empty(), "no provider may write a row");
            assert_eq!(fusiform.catalog_calls.load(Ordering::SeqCst), calls_before);
            assert_eq!(fusiform.plan_calls.load(Ordering::SeqCst), 0);

            // The excluded scope must not block a request for an ingested one.
            let reply = crate::ops::dispatch(
                &state,
                crate::ops::OP_MULTIPLIER_RECOMPUTE,
                json!({"observation_day": DAY, "quota_provider": "codex"}),
            )
            .unwrap();
            assert_eq!(reply["recomputed"].as_array().unwrap().len(), 1);
            assert_eq!(reply["recomputed"][0]["quota_provider"], "codex");
            assert_eq!(all_rows(&state).len(), 1);
        }
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
