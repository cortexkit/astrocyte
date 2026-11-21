//! Bounded pricing walks and coverage backfills live here so their transaction rules stay together.

use super::*;
use std::collections::HashSet;

/// How many events that already carry an uncovered charge head one pass may
/// re-resolve against newly declared subscription coverage.
///
/// Measurements with no head at all are freshly recorded facts and keep the
/// priority they have today: this bounds only the events added on top of them,
/// never the ones the pass already had.
///
/// The size trades two costs against each other. Coverage is answered from our
/// own `account_subscription` table with no catalog call, so one of these
/// re-resolves is a local read, a decision and a small write; several hundred
/// of them take a fraction of a second, which is a low single-digit share of
/// the sixty-second default tick and never long enough for an operator's write
/// to queue behind the pass. Repricing a whole backlog in one tick is the other
/// cost: the backlog this was written for is 34,378 events, and taking the
/// store writer for all of them at once would stall ingest and operator writes
/// for minutes. At this size that backlog drains in roughly an hour of ordinary
/// running, and a routine declaration covering a few hundred events is absorbed
/// by a single tick.
const COVERAGE_BACKFILL_PER_PASS: usize = 512;

/// How many priced events one write transaction commits.
///
/// Each commit is one fsync of the write-ahead log (the store runs WAL with
/// `synchronous` left at FULL), so committing event by event costs one fsync
/// per event. On a first boot over a large harness history that was about
/// 142,000 fsyncs, and 9 GB written in six minutes into a 146 MB store.
///
/// Measured on a copy of our production store (7.3 GB) with 10,000 events
/// re-queued, release build: one event per transaction made 10,001 commits and
/// wrote 997 MB of log; groups of 50, 100, 500 and 1,000 made 201, 101, 21 and
/// 11 commits and wrote 505, 495, 483 and 479 MB. The log volume halves at any
/// group size and is flat from about 50 on, and the commit count is already
/// one per hundred events at 100. What keeps growing with the group is how
/// long one transaction holds the store writer: about 1 to 3 seconds for a
/// group of 500 on that store, so ingest and operator writes would queue
/// behind it for that long, against roughly a fifth of that at 100. A smaller
/// group also lets the tick budget below stop closer to its limit.
const PRICING_COMMIT_GROUP: usize = 100;

/// How many queued events the refresher loop prices in one tick at most.
///
/// With the time budget below, this bounds how long pricing holds up a tick,
/// so a large backlog is spread over ticks and every tick still drains the
/// producers first. A first boot of about 530,000 queued events on a small
/// store, where pricing is cheap and this cap binds, takes 106 ticks: about
/// an hour at the production 30-second poll and two hours at the 60-second
/// default, with 50 commits a tick where the old pass made one per event.
const PRICING_TICK_EVENTS: usize = 5_000;

/// Wall time one tick may spend pricing queued events before it returns to
/// the loop and lets the next tick drain the sources. Checked before each
/// event's catalog resolution: an in-flight call may finish past the budget,
/// but no later event starts. The resolved prefix still commits as one group.
/// Five seconds is a small share of a 30- or 60-second poll, so ingest stays
/// fresh.
///
/// On a large store this budget binds before the event cap. On the 7.3 GB
/// production copy (see `PRICING_COMMIT_GROUP`) pricing cost about 2.5 to 3.5
/// ms an event with a warm page cache, catalog lookups included, and up to 8.5
/// ms cold: 1,400 to 2,000 events a tick warm, about 600 cold. A tick runs
/// for its poll interval plus up to this budget, so 530,000 events take
/// 265 to 380 ticks warm, under seven hours at the 60-second default and
/// under four at the 30-second production poll; cold, about 900 ticks, under
/// seventeen hours at the default and under nine at the production poll.
const PRICING_TICK_BUDGET: Duration = Duration::from_secs(5);

/// How much of the pricing queue one call to the pass may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PricingBudget {
    /// Events committed per write transaction.
    pub(crate) group_events: usize,
    /// Queued events one call may take before it stops for this tick.
    pub(crate) tick_events: usize,
    /// Wall time one call may spend on the queue before it stops for this
    /// tick.
    pub(crate) tick_time: Duration,
    /// Most coverage-backfill events one call reads; each one read is
    /// repriced unless the stop flag or the time budget ends the call first.
    pub(crate) backfill_events: usize,
}

impl PricingBudget {
    /// The refresher loop's budget: bounded per tick.
    pub(crate) const PER_TICK: Self = Self {
        group_events: PRICING_COMMIT_GROUP,
        tick_events: PRICING_TICK_EVENTS,
        tick_time: PRICING_TICK_BUDGET,
        backfill_events: COVERAGE_BACKFILL_PER_PASS,
    };

    /// The whole queue in one call, in the loop's commit groups: what the
    /// test fixtures that price directly use.
    #[cfg(test)]
    pub(crate) const WHOLE_QUEUE: Self = Self {
        group_events: PRICING_COMMIT_GROUP,
        tick_events: usize::MAX,
        tick_time: Duration::MAX,
        backfill_events: COVERAGE_BACKFILL_PER_PASS,
    };
}

/// The pricing queue walk, carried across refresher ticks.
///
/// The walk reads the queue in event-id order and resumes each tick after
/// the last event it reached, wrapping to the start once it reaches the end.
/// Starting every tick from the beginning instead would let a run of events
/// that keep failing at the head of the queue use up every tick's budget, so
/// the events behind them would never be priced.
#[derive(Debug)]
pub(crate) struct PricingWalk {
    budget: PricingBudget,
    /// Last queued event id the walk reached; `None` starts at the beginning.
    after: Option<String>,
    /// A budget stop was logged and the queue has not yet been walked to its
    /// end since, so the log says once when a backlog starts and once when it
    /// is caught up rather than on every tick in between.
    backlog_logged: bool,
    /// Where the coverage backfill resumes; see [`BackfillSweep`].
    backfill: BackfillSweep,
}

/// The coverage backfill's position, carried across refresher ticks.
///
/// Each tick reads one bounded page of candidate charges in index order,
/// starting after the last candidate the previous tick reached, and wraps to the
/// start once a batch reaches the end of the set. An event whose reprice
/// fails keeps its uncovered head, so it stays in the set and is retried when
/// the sweep comes round to it again; it is never dropped, and it cannot hold
/// every later tick on the same batch while the events behind it wait.
///
/// A sweep is one pass from the start of the set to its end, possibly over
/// many ticks. The plan generation is marked drained only at the end of a
/// sweep in which every event was repriced: an event that failed anywhere in
/// the sweep is still in the set, so the backfill is not finished.
#[derive(Debug, Default)]
struct BackfillSweep {
    /// The plan generation this sweep is draining. A plan write moves the
    /// generation and can put events anywhere in the set, including behind
    /// the cursor, so a new generation starts a new sweep from the start.
    generation: Option<i64>,
    /// Last candidate charge reached, even if the page returned no events.
    after: Option<astrocyte_core::subscription::CoverageCursor>,
    /// An event failed to reprice since this sweep started.
    failed: bool,
}

impl BackfillSweep {
    /// Start a fresh sweep from the beginning of the set.
    fn restart(&mut self, generation: i64) {
        *self = Self {
            generation: Some(generation),
            after: None,
            failed: false,
        };
    }
}

impl PricingWalk {
    pub(crate) fn new(budget: PricingBudget) -> Self {
        Self {
            budget,
            after: None,
            backlog_logged: false,
            backfill: BackfillSweep::default(),
        }
    }

    /// Log a backlog's start and end, once each. Called after the queue part
    /// of every pass: `paused` when it stopped on its budget, `reached_end`
    /// when it read to the end of the queue.
    fn note_progress(&mut self, state: &SharedState, paused: bool, reached_end: bool) {
        let should_log = (paused && !self.backlog_logged) || (reached_end && self.backlog_logged);
        if !should_log {
            return;
        }
        let queued = state.with_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM pricing_pending", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|error| error.to_string())
        });
        let queued = match queued {
            Ok(queued) => queued,
            Err(error) => {
                tracing::warn!(target: "pricing", %error, "could not count the pricing queue");
                -1
            }
        };
        if paused {
            self.backlog_logged = true;
            tracing::info!(
                target: "pricing",
                queued,
                tick_events = self.budget.tick_events,
                tick_budget_ms = u64::try_from(self.budget.tick_time.as_millis()).unwrap_or(u64::MAX),
                "pricing pass stopped on its tick budget with events still queued; it continues on the next tick"
            );
        } else {
            self.backlog_logged = false;
            // What is still queued here is only events whose pricing failed on
            // this walk; each was reported as a pricing failure.
            tracing::info!(
                target: "pricing",
                still_queued = queued,
                "pricing backlog caught up"
            );
        }
    }
}

/// What one pricing pass did, for the refresher's callers and for tests that
/// must see which queries ran rather than only what they produced.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PricingPass {
    /// Events whose reprice committed.
    pub(crate) priced: usize,
    /// Whether the coverage backfill query ran. A drained backfill returns
    /// nothing either way, so its result cannot say whether it was skipped.
    pub(crate) backfill_queried: bool,
    /// SQLite virtual-machine steps spent reading the pending queue.
    pub(crate) queue_selection_steps: i64,
    /// SQLite VM steps spent selecting the coverage backfill page.
    pub(crate) backfill_selection_steps: i64,
    /// The pass stopped on its per-tick budget before reaching all selected
    /// queue or coverage-backfill events.
    pub(crate) paused: bool,
}

/// An event's reprice plan and its resolution, both produced with no
/// transaction open.
struct ResolvedEvent<'a> {
    event_id: &'a str,
    plan: astrocyte_core::subscription::SubscriptionRepricePlan,
    resolved: astrocyte_core::subscription::ResolvedSubscriptionReprice,
}

enum EventResolutionError {
    NoMeasurementsToPrice,
    Failed(String),
}

fn resolve_event<'a>(
    state: &SharedState,
    catalog: Option<&CatalogClient>,
    event_id: &'a str,
    deadline: Option<std::time::Instant>,
    now: &dyn Fn() -> std::time::Instant,
) -> Result<Option<ResolvedEvent<'a>>, EventResolutionError> {
    let plan = state
        .with_conn(|conn| Ok(astrocyte_core::subscription::read_reprice(conn, event_id)))
        .map_err(EventResolutionError::Failed)?
        .map_err(|error| match error {
            astrocyte_core::subscription::SubscriptionError::Pricing(
                astrocyte_core::pricing::PricingError::Refused(
                    astrocyte_core::pricing::PricingRefusal::NoMeasurementsToPrice,
                ),
            ) => EventResolutionError::NoMeasurementsToPrice,
            error => EventResolutionError::Failed(format!("{}: {}", error.code(), error.message())),
        })?;
    // A queue row left by an older binary is not a request to reprice by hand.
    if plan.charge_basis() == astrocyte_core::fact::ChargeBasis::Unknown {
        return Err(EventResolutionError::NoMeasurementsToPrice);
    }
    if deadline.is_some_and(|deadline| now() >= deadline) {
        return Ok(None);
    }
    // Plan reading committed above; catalog resolution therefore never
    // runs while a SQLite transaction or store mutex is held.
    let resolved = match catalog {
        Some(catalog) => astrocyte_core::subscription::resolve_reprice(
            &plan,
            catalog,
            &state.desired_config().reasoning_billed_as_output,
        ),
        None => astrocyte_core::subscription::resolve_reprice_without_catalog(&plan),
    }
    .map_err(|error| {
        EventResolutionError::Failed(format!("{}: {}", error.code(), error.message()))
    })?;
    Ok(Some(ResolvedEvent {
        event_id,
        plan,
        resolved,
    }))
}

/// Commit one resolved event inside the group's transaction, under its own
/// savepoint. The outer `Result` is a failure of the savepoint statements
/// themselves, which the group cannot survive; the inner one is this event's
/// own outcome.
///
/// The reprice re-checks the event against the plan it was resolved from
/// (its pricing generation and the subscription coverage it assumed) and
/// refuses if either moved. A refusal, or any other error part-way through
/// its writes, rolls back to the savepoint: exactly this event's writes are
/// undone, the events already committed in the group keep theirs, and the
/// event stays queued because its dequeue was among the writes undone.
fn commit_in_savepoint(
    tx: &Transaction<'_>,
    event: &ResolvedEvent<'_>,
    recorded_at_ms: i64,
) -> Result<Result<(), String>, String> {
    tx.execute_batch("SAVEPOINT pricing_event")
        .map_err(|error| format!("open pricing savepoint: {error}"))?;
    match astrocyte_core::subscription::commit_reprice_in_tx(
        tx,
        &event.plan,
        &event.resolved,
        recorded_at_ms,
        astrocyte_core::pricing::PricingOrigin::PricingPass,
    ) {
        Ok(_) => {
            tx.execute_batch("RELEASE pricing_event")
                .map_err(|error| format!("release pricing savepoint: {error}"))?;
            Ok(Ok(()))
        }
        Err(error) => {
            tx.execute_batch("ROLLBACK TO pricing_event; RELEASE pricing_event")
                .map_err(|failure| format!("roll back pricing savepoint: {failure}"))?;
            Ok(Err(format!("{}: {}", error.code(), error.message())))
        }
    }
}

enum PricingOutcome {
    Priced,
    NothingToPrice,
}

struct PricedGroup {
    attempted: usize,
    outcomes: Vec<(String, Result<PricingOutcome, String>)>,
}

/// Price a group of events with one write transaction, and return each
/// event's outcome and the number attempted. `None` means `stop` was set before the group committed:
/// nothing in the group was written, and every event in it stays queued.
///
/// Every plan is read and every resolution (which may call the catalog) is
/// done first, with no transaction open. Then the whole group commits in one
/// `ordered_write`, one fsync for the group, with a savepoint per event so
/// that an event refused as stale (see `commit_in_savepoint`) does not take
/// the others with it. A savepoint is used rather than retrying the group
/// without the refused event because it keeps the group at one transaction
/// however many of its events are refused, and because it undoes exactly one
/// event's writes even when an error comes after some of them were made.
///
/// A plan refusal of `NoMeasurementsToPrice` instead removes obsolete queue
/// work in the write transaction, after rechecking that no billed measurement
/// still needs a head. Other read or resolution errors leave the event queued.
/// If the group's transaction itself fails to begin or commit, no
/// event in it was written and each is reported with that error.
/// A time-budget stop commits only the resolved prefix; the untouched suffix
/// stays queued and is not included in the attempted count.
fn price_event_group(
    state: &SharedState,
    catalog: Option<&CatalogClient>,
    event_ids: &[String],
    recorded_at_ms: i64,
    stop: &AtomicBool,
    deadline: Option<std::time::Instant>,
    now: &dyn Fn() -> std::time::Instant,
) -> Option<PricedGroup> {
    let mut outcomes = Vec::with_capacity(event_ids.len());
    let mut resolved = Vec::with_capacity(event_ids.len());
    let mut nothing_to_price = Vec::new();
    for event_id in event_ids {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        if deadline.is_some_and(|deadline| now() >= deadline) {
            break;
        }
        match resolve_event(state, catalog, event_id, deadline, now) {
            Ok(Some(event)) => resolved.push(event),
            Ok(None) => break,
            Err(EventResolutionError::NoMeasurementsToPrice) => nothing_to_price.push(event_id),
            Err(EventResolutionError::Failed(error)) => {
                outcomes.push((event_id.clone(), Err(error)))
            }
        }
    }
    if stop.load(Ordering::Relaxed) {
        return None;
    }
    let attempted = outcomes.len() + resolved.len() + nothing_to_price.len();
    if resolved.is_empty() && nothing_to_price.is_empty() {
        return Some(PricedGroup {
            attempted,
            outcomes,
        });
    }
    let committed = state.ordered_write(|tx| {
        let mut results = Vec::with_capacity(resolved.len() + nothing_to_price.len());
        let mut any_committed = false;
        for event in &resolved {
            let result = commit_in_savepoint(tx, event, recorded_at_ms)?;
            any_committed |= result.is_ok();
            results.push((
                event.event_id.to_string(),
                result.map(|()| PricingOutcome::Priced),
            ));
        }
        for event_id in &nothing_to_price {
            let removed = astrocyte_core::pricing::dequeue_if_fully_headed(tx, event_id)
                .map_err(|error| format!("dequeue empty pricing event: {error}"))?;
            any_committed |= removed != 0;
            results.push(((*event_id).clone(), Ok(PricingOutcome::NothingToPrice)));
        }
        Ok((results, any_committed))
    });
    match committed {
        Ok(results) => outcomes.extend(results),
        Err(error) => outcomes.extend(
            resolved
                .iter()
                .map(|event| event.event_id)
                .chain(nothing_to_price.iter().map(|id| id.as_str()))
                .map(|event_id| {
                    (
                        event_id.to_string(),
                        Err(format!("group commit failed: {error}")),
                    )
                }),
        ),
    }
    Some(PricedGroup {
        attempted,
        outcomes,
    })
}

/// Price every queued event, then any event a newly declared or closed plan
/// now covers, the whole queue in one call. The refresher loop uses
/// `run_pricing_walk` with a per-tick budget instead; this is for tests.
#[cfg(test)]
pub fn price_unpriced_events(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
) -> Result<usize, String> {
    run_pricing_pass(state, recorded_at_ms, stop).map(|pass| pass.priced)
}

#[cfg(test)]
pub(crate) fn run_pricing_pass(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
) -> Result<PricingPass, String> {
    run_pricing_walk(
        state,
        recorded_at_ms,
        stop,
        &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
    )
}

/// Price queued events from where `walk` left off until its budget is spent
/// or the queue ends, then any event a newly declared or closed plan now
/// covers.
///
/// An event whose pricing fails -- a catalog that cannot be reached or refuses,
/// a commit refused because the event moved since its plan was read -- writes
/// no head and so stays queued; the walk comes back to it on a later pass.
/// Any failure makes the pass return an error naming every failed event,
/// after the rest of the pass has run.
///
/// Setting `stop` ends the pass before the next event, and commits nothing
/// partial: a group that has not committed when the stop is seen is dropped
/// whole, each committed group was one transaction, and within it each event
/// was fully written or rolled back to its savepoint. So every event is
/// either fully priced or untouched, and an untouched event is still queued
/// for the next pass. A stopped pass returns the count priced so far;
/// stopping is not a pricing failure.
pub(crate) fn run_pricing_walk(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
    walk: &mut PricingWalk,
) -> Result<PricingPass, String> {
    run_pricing_walk_with_clock(state, recorded_at_ms, stop, walk, &std::time::Instant::now)
}

fn run_pricing_walk_with_clock(
    state: &SharedState,
    recorded_at_ms: i64,
    stop: &AtomicBool,
    walk: &mut PricingWalk,
    now: &dyn Fn() -> std::time::Instant,
) -> Result<PricingPass, String> {
    let catalog = state.catalog_client();
    let budget = walk.budget;
    let group_events = budget.group_events.max(1);
    let mut pass = PricingPass::default();
    let mut failures = Vec::new();
    let mut failed = HashSet::new();
    let mut attempted = HashSet::new();

    // Keyset pagination: each group starts after the last id of the one
    // before, so an event that stays queued because it failed is not read
    // again in this pass and cannot hold the pass on one group.
    let tick_started = now();
    let deadline = tick_started.checked_add(budget.tick_time);
    let mut slowest_group = Duration::ZERO;
    let mut taken = 0_usize;
    let mut reached_end = false;
    loop {
        let remaining = budget.tick_events.saturating_sub(taken);
        if remaining == 0 {
            pass.paused = true;
            break;
        }
        let wanted = group_events.min(remaining);
        let (mut group, steps) = match state.with_conn(|conn| {
            astrocyte_core::pricing::pending_event_ids(conn, walk.after.as_deref(), wanted)
                .map_err(|error| format!("read pricing queue: {error}"))
        }) {
            Ok(group) => group,
            Err(error) => {
                failures.push(error);
                return finish_pricing_pass(pass, failures);
            }
        };
        pass.queue_selection_steps += steps;
        let complete = group.len() < wanted;
        let group_started = now();
        let Some(PricedGroup {
            attempted: processed,
            outcomes,
        }) = price_event_group(
            state,
            catalog.as_ref(),
            &group,
            recorded_at_ms,
            stop,
            deadline,
            now,
        )
        else {
            // The cursor stays before this group, so the next pass starts
            // with it.
            return finish_pricing_pass(pass, failures);
        };
        for (event_id, outcome) in outcomes {
            match outcome {
                Ok(PricingOutcome::Priced) => pass.priced += 1,
                Ok(PricingOutcome::NothingToPrice) => {}
                Err(error) => {
                    failures.push(format!("event {event_id}: {error}"));
                    failed.insert(event_id);
                }
            }
        }
        if processed < group.len() {
            group.truncate(processed);
            if let Some(last) = group.last() {
                walk.after = Some(last.clone());
            }
            attempted.extend(group);
            pass.paused = true;
            break;
        }
        taken += group.len();
        if complete {
            walk.after = None;
            reached_end = true;
            attempted.extend(group);
            break;
        }
        walk.after = group.last().cloned();
        attempted.extend(group);
        slowest_group = slowest_group.max(now().duration_since(group_started));
        // Start another group only if one as slow as the slowest seen this
        // tick would still finish inside the budget.
        if now()
            .duration_since(tick_started)
            .saturating_add(slowest_group)
            > budget.tick_time
        {
            pass.paused = true;
            break;
        }
    }
    walk.note_progress(state, pass.paused, reached_end);

    // An event a newly declared plan covers already has a head saying no plan
    // covered it, so it is not queued. Those are found by the backfill query,
    // which is only run while a plan write has not yet been drained. Its batch
    // is bounded by the budget's `backfill_events` on its own, so it runs on a
    // tick whose queue walk paused on the event cap too, but shares its
    // wall-time budget. Each tick's batch resumes where the last one ended;
    // see `BackfillSweep`.
    if stop.load(Ordering::Relaxed) || deadline.is_some_and(|deadline| now() >= deadline) {
        return finish_pricing_pass(pass, failures);
    }
    let due = match state.with_conn(|conn| {
        astrocyte_core::subscription::coverage_backfill_due(conn)
            .map_err(|error| format!("{}: {}", error.code(), error.message()))
    }) {
        Ok(due) => due,
        Err(error) => {
            failures.push(error);
            return finish_pricing_pass(pass, failures);
        }
    };
    if let Some(generation) = due {
        pass.backfill_queried = true;
        let sweep = &mut walk.backfill;
        if sweep.generation != Some(generation) {
            sweep.restart(generation);
        }
        let batch_events = budget.backfill_events.max(1);
        let backfill = match state.with_conn(|conn| {
            astrocyte_core::subscription::coverage_backfill_page(conn, sweep.after, batch_events)
                .map_err(|error| format!("{}: {}", error.code(), error.message()))
        }) {
            Ok(backfill) => backfill,
            Err(error) => {
                failures.push(error);
                return finish_pricing_pass(pass, failures);
            }
        };
        // A short candidate page read to the end of the index.
        let reaches_end = backfill.reached_end;
        let batch_last = backfill.through;
        pass.backfill_selection_steps = backfill.vm_steps;
        // Every event in the batch was attempted; false when the pass
        // stopped or ran out of time partway through it.
        let mut batch_attempted = true;
        // The last event of the batch whose reprice was attempted, which is
        // where the next tick resumes when this one stops partway.
        let mut last_attempted = None;
        // An event answers both the queue and the backfill if one of its
        // measurements has no head while another carries the uncovered
        // head. Recording writes all of an event's measurements with the
        // event, so nothing produces that today, and the guard is here
        // because the failure would be silent and misleading: the same
        // event priced twice in one pass makes the second attempt refuse
        // as already priced, which reports a healthy pass as a failure.
        let mut fresh = Vec::with_capacity(backfill.events.len());
        let mut positions = std::collections::HashMap::with_capacity(backfill.events.len());
        for (cursor, event_id) in backfill.events {
            if attempted.contains(&event_id) {
                sweep.failed |= failed.contains(&event_id);
            } else {
                positions.insert(event_id.clone(), cursor);
                fresh.push(event_id);
            }
        }
        for group in fresh.chunks(group_events) {
            let Some(PricedGroup {
                attempted: processed,
                outcomes,
            }) = price_event_group(
                state,
                catalog.as_ref(),
                group,
                recorded_at_ms,
                stop,
                deadline,
                now,
            )
            else {
                // Nothing in this group was written, so the sweep resumes
                // with it on the next tick.
                batch_attempted = false;
                break;
            };
            for (event_id, outcome) in outcomes {
                match outcome {
                    Ok(PricingOutcome::Priced) => pass.priced += 1,
                    Ok(PricingOutcome::NothingToPrice) => {}
                    Err(error) => {
                        // The event keeps its uncovered head, so it stays in
                        // the set and the next sweep retries it.
                        sweep.failed = true;
                        failures.push(format!("event {event_id}: {error}"));
                    }
                }
            }
            // The attempted events are always a prefix of the group.
            if let Some(last) = group.get(..processed).and_then(<[String]>::last) {
                last_attempted = positions.get(last).copied();
            }
            if processed < group.len() {
                batch_attempted = false;
                pass.paused = true;
                break;
            }
        }
        if !batch_attempted {
            if last_attempted.is_some() {
                sweep.after = last_attempted;
            }
        } else if !reaches_end {
            sweep.after = batch_last;
        } else {
            // The sweep has read the whole set from its start. An event that
            // failed anywhere in it is still there, so only a sweep with no
            // failure proves nothing covered is left. Either way the next
            // tick starts a new sweep from the beginning, which is how a
            // failed event is retried.
            let drained = !sweep.failed;
            sweep.restart(generation);
            if drained {
                if let Err(error) = state.ordered_write(|tx| {
                    let marked =
                        astrocyte_core::subscription::mark_coverage_backfill_drained_in_tx(
                            tx, generation,
                        )
                        .map_err(|error| format!("{}: {}", error.code(), error.message()))?;
                    Ok(((), marked))
                }) {
                    failures.push(error);
                }
            }
        }
    }

    finish_pricing_pass(pass, failures)
}

fn finish_pricing_pass(pass: PricingPass, failures: Vec<String>) -> Result<PricingPass, String> {
    if failures.is_empty() {
        Ok(pass)
    } else {
        Err(failures.join("; "))
    }
}

// The backfill and timestamp fixtures in these tests stay within i64.
#[allow(clippy::cast_possible_wrap)]
#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::refresher::tests::*;
    use crate::state::test_state;
    use astrocyte_core::catalog_client::CatalogCallError;
    use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
    use std::sync::atomic::AtomicUsize;

    fn assert_path_origin(operator: bool, expected: &str) {
        let state = test_state();
        seed_unpriced_event(&state);
        let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        if operator {
            crate::ops::dispatch(
                &state,
                crate::ops::OP_EVENT_REPRICE,
                serde_json::json!({"event_id":"ev-late"}),
            )
            .unwrap();
        } else {
            assert_eq!(
                price_unpriced_events(&state, 1_800_000_000_001, &NEVER_STOP).unwrap(),
                1
            );
        }
        let row: (String, String, i64) = state.with_conn(|conn| {
            conn.query_row("SELECT origin,outcome,pricing_generation FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='ev-late')", [],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|e.to_string())
        }).unwrap();
        assert_eq!(row, (expected.into(), "priced".into(), 1));
    }

    #[test]
    fn refresher_pass_writes_pricing_pass_origin() {
        assert_path_origin(false, "pricing_pass");
    }

    #[test]
    fn operator_reprice_writes_operator_reprice_origin() {
        assert_path_origin(true, "operator_reprice");
    }

    /// The pricing pass checks the stop flag before each event. A pass that
    /// starts with the flag already set prices nothing, and reports that as
    /// success because a stop is not a pricing failure; the event stays
    /// unpriced and the next pass picks it up.
    #[test]
    fn a_stopped_pricing_pass_prices_no_further_event() {
        let state = test_state();
        seed_unpriced_event(&state);
        let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
        state.install_catalog(Some(catalog)).unwrap();

        assert_eq!(
            price_unpriced_events(&state, 1_800_000_000_001, &AtomicBool::new(true)).unwrap(),
            0
        );
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(
            price_unpriced_events(&state, 1_800_000_000_002, &NEVER_STOP).unwrap(),
            1,
            "the event skipped by the stopped pass is priced by the next one"
        );
    }

    #[test]
    fn reasoning_billing_reaches_pricing_pass_reprice_and_basis_correction() {
        use serde_json::json;
        for path in ["pass", "reprice", "basis"] {
            for declared in [false, true] {
                let state = test_state();
                let mut desired = config::intentional_empty().config;
                if declared {
                    desired.reasoning_billed_as_output.push("anthropic".into());
                }
                state.publish_desired(desired, 1);
                let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
                state.install_catalog(Some(catalog)).unwrap();
                let event_id = crate::ops::dispatch(&state, crate::ops::OP_EVENT_INGEST, json!({
                    "producer":"producer", "producer_lineage":"lineage", "producer_ref":"thinking",
                    "provider":"anthropic", "model":"claude", "occurred_at_ms":1_800_000_000_000_i64,
                    "charge_basis": if path == "basis" { "unknown" } else { "metered_api" },
                    "measurements":[{"role":"reasoning","unit":"token","kind":"billed","source":"usage.reasoning","quantity_units":1000,"quantity_exponent":0}]
                })).unwrap()["event_id"].as_str().unwrap().to_owned();
                match path {
                    "pass" => {
                        assert_eq!(
                            price_unpriced_events(
                                &state,
                                1_800_000_000_001,
                                &AtomicBool::new(false)
                            )
                            .unwrap(),
                            1
                        );
                    }
                    "reprice" => {
                        crate::ops::dispatch(
                            &state,
                            crate::ops::OP_EVENT_REPRICE,
                            json!({"event_id":event_id}),
                        )
                        .unwrap();
                    }
                    _ => {
                        crate::ops::dispatch(&state, crate::ops::OP_BASIS_CORRECT, json!({"event_id":event_id,"corrected_basis":"metered_api","reason":"API-key evidence"})).unwrap();
                    }
                }
                let stored: (String, Option<i64>, Option<String>, String) = state.with_conn(|conn| {
                    conn.query_row("SELECT outcome,charge_nanos,unpriced_reason,json_extract(catalog_entry,'$.fact_key') FROM priced_charge", [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).map_err(|error| error.to_string())
                }).unwrap();
                let expected = if declared {
                    ("priced".into(), Some(6), None, "rate.output".into())
                } else {
                    (
                        "unpriced".into(),
                        None,
                        Some("no_rate".into()),
                        "rate.reasoning".into(),
                    )
                };
                assert_eq!(stored, expected, "{path}, declared={declared}");
            }
        }
    }

    /// The account and provider the plans in the tests below are declared for.
    /// An event naming this pair is inside the join, so only its instant or its
    /// head decides whether it is selected. Two tests deliberately record an
    /// event under a different provider and under a different account: coverage
    /// is keyed on both, and a fixture that never varies a column cannot hold
    /// the predicate that reads it.
    pub(in crate::refresher) const ACCOUNT: &str = "account-a";
    pub(in crate::refresher) const PROVIDER: &str = "anthropic";

    fn declare_plan(state: &SharedState, effective_from: i64) {
        state
            .ordered_write(|tx| {
                astrocyte_core::subscription::set_subscription_in_tx(
                    tx,
                    &astrocyte_core::subscription::SetSubscriptionRequest {
                        account_ref: ACCOUNT.into(),
                        provider: PROVIDER.into(),
                        plan_name: "team".into(),
                        recurring_cost_minor: 2_000,
                        currency: "USD".into(),
                        amount_exponent: 2,
                        period: astrocyte_core::subscription::SubscriptionPeriod::Month,
                        effective_from,
                    },
                    None,
                )
                .map_err(|error| error.message())?;
                Ok(((), true))
            })
            .unwrap();
    }

    /// Stands in for recording, so it queues the event in the same transaction
    /// exactly as recording does.
    fn insert_event(
        tx: &Transaction<'_>,
        event_id: &str,
        provider: &str,
        account_ref: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) -> Result<(), String> {
        insert_event_unqueued(
            tx,
            event_id,
            provider,
            account_ref,
            occurred_at_ms,
            charge_basis,
            model,
        )?;
        astrocyte_core::pricing::enqueue_if_unpriced_in_tx(tx, event_id)
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// An event and its measurement written WITHOUT the queue row: what a
    /// write path that forgot to enqueue would leave behind.
    pub(in crate::refresher) fn insert_event_unqueued(
        tx: &Transaction<'_>,
        event_id: &str,
        provider: &str,
        account_ref: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) -> Result<(), String> {
        tx.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_ref,recorded_at_ms,occurred_at_ms,
                 provider,model,account_ref,charge_basis
             ) VALUES (?1,'broca',?1,1,?2,?3,?4,?5,?6)",
            params![
                event_id,
                occurred_at_ms,
                provider,
                model,
                account_ref,
                charge_basis
            ],
        )
        .map_err(|error| error.to_string())?;
        tx.execute(
            "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','producer',10,0)",
            [event_id],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// An event under the pair the plans here are declared for.
    pub(in crate::refresher) fn insert_account_event(
        tx: &Transaction<'_>,
        event_id: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) -> Result<(), String> {
        insert_event(
            tx,
            event_id,
            PROVIDER,
            ACCOUNT,
            occurred_at_ms,
            charge_basis,
            model,
        )
    }

    pub(in crate::refresher) fn seed_account_event(
        state: &SharedState,
        event_id: &str,
        occurred_at_ms: i64,
        charge_basis: &str,
        model: &str,
    ) {
        state
            .ordered_write(|tx| {
                insert_account_event(tx, event_id, occurred_at_ms, charge_basis, model)?;
                Ok(((), true))
            })
            .unwrap();
    }

    /// A subscription-basis event under a provider and account the caller
    /// names, for the arms that must differ from the pair the plan is declared
    /// for. Both halves of that pair decide coverage, so both have to vary
    /// somewhere: a predicate on a column every fixture fills with one value is
    /// a predicate no test can hold.
    fn seed_event_under(
        state: &SharedState,
        event_id: &str,
        provider: &str,
        account_ref: &str,
        occurred_at_ms: i64,
    ) {
        state
            .ordered_write(|tx| {
                insert_event(
                    tx,
                    event_id,
                    provider,
                    account_ref,
                    occurred_at_ms,
                    "subscription_included",
                    "claude",
                )?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Head {
        charge_key: i64,
        outcome: String,
        reason: Option<String>,
        charge_nanos: Option<i64>,
        currency: Option<String>,
        /// Every charge row this event has, superseded ones included. A
        /// re-resolve that landed on the outcome already recorded would still
        /// leave a row behind, so this is what says nothing was re-resolved.
        rows: i64,
    }

    /// The current head of an event's single measurement.
    fn head(state: &SharedState, event_id: &str) -> Head {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT head.charge_key,head.outcome,head.unpriced_reason,head.charge_nanos,
                            head.currency,
                            (SELECT COUNT(*) FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1))
                     FROM priced_charge AS head
                     WHERE head.event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)
                       AND NOT EXISTS (
                           SELECT 1 FROM priced_charge AS child
                           WHERE child.supersedes=head.charge_key
                       )",
                    [event_id],
                    |row| {
                        Ok(Head {
                            charge_key: row.get(0)?,
                            outcome: row.get(1)?,
                            reason: row.get(2)?,
                            charge_nanos: row.get(3)?,
                            currency: row.get(4)?,
                            rows: row.get(5)?,
                        })
                    },
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn covered_heads(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM priced_charge AS head
                     WHERE head.outcome='covered'
                       AND NOT EXISTS (
                           SELECT 1 FROM priced_charge AS child
                           WHERE child.supersedes=head.charge_key
                       )",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    pub(in crate::refresher) fn queued_event_ids(state: &SharedState) -> Vec<String> {
        state
            .with_conn(|conn| {
                conn.prepare("SELECT event_id FROM pricing_pending JOIN usage_event USING(event_key) ORDER BY event_id")
                    .and_then(|mut statement| {
                        statement
                            .query_map([], |row| row.get(0))?
                            .collect::<rusqlite::Result<Vec<String>>>()
                    })
                    .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn seed_media_pricing_event(state: &SharedState, record_id: &str, billed: bool) -> String {
        let mut measurements = vec![serde_json::json!({
            "role":"input", "unit":"credit", "quantity":"20",
            "measure":"observed", "source":"vendor_reply"
        })];
        if billed {
            measurements.push(serde_json::json!({
                "role":"input", "unit":"token", "quantity":"1000000",
                "measure":"billed", "source":"vendor_reply"
            }));
        }
        let record = serde_json::from_value(serde_json::json!({
            "record_id":record_id, "principal":"person-a", "job_id":"job-1",
            "provider":"anthropic", "model":"claude", "charge_basis":"vendor",
            "auth_method":"api_key", "started_at_ms":1_800_000_000_000_i64,
            "finished_at_ms":1_800_000_000_100_i64, "status":"done",
            "selectors":{}, "measurements":measurements
        }))
        .unwrap();
        let (request, facts) = astrocyte_core::media::map_record(&record).unwrap().unwrap();
        state
            .ordered_write(|tx| {
                let outcome = astrocyte_core::ingest::record_media_event_in_tx(
                    tx,
                    &request,
                    &facts,
                    1_800_000_000_200,
                )
                .map_err(|error| format!("{error:?}"))?;
                Ok((outcome.event_id, true))
            })
            .unwrap()
    }

    #[test]
    fn unknown_basis_never_queues_or_writes_automatic_charge_heads() {
        let state = test_state();
        let id = "unknown-basis";
        seed_account_event(&state, id, 1_800_000_000_000, "unknown", "claude");
        let after_recording = queued_event_ids(&state);
        let reconciled = state.ordered_write(|tx| {
            tx.execute("DELETE FROM pricing_pending", []).map_err(|error| error.to_string())?;
            let reconciled = astrocyte_core::pricing::reconcile_pending_range_in_tx(tx, None, None)
                .map_err(|error| error.to_string())?;
            tx.execute("INSERT OR IGNORE INTO pricing_pending (event_key) SELECT event_key FROM usage_event WHERE event_id=?1", [id])
                .map_err(|error| error.to_string())?;
            Ok((reconciled, true))
        }).unwrap();
        let pass = run_pricing_pass(&state, 1_800_000_000_300, &NEVER_STOP).unwrap();
        assert!(after_recording.is_empty());
        assert_eq!(reconciled, 0);
        assert_eq!(pass.priced, 0);
        assert!(queued_event_ids(&state).is_empty());
        assert_eq!(priced_charge_rows(&state), 0);
        let reprice = crate::ops::dispatch(
            &state,
            "spend.event.reprice",
            serde_json::json!({"event_id": id}),
        );
        assert!(reprice.is_ok(), "{reprice:?}");
        assert_eq!(
            charge_rows_by_origin(&state, id),
            [("operator_reprice".into(), 1)]
        );
    }

    #[test]
    fn observed_only_pricing_walk_removes_legacy_queue_row_without_charges() {
        let state = test_state();
        let id = seed_media_pricing_event(&state, "observed-only-walk", false);
        // Older queue reconciliation included observed readings as headless work.
        state.ordered_write(|tx| {
            tx.execute("INSERT OR IGNORE INTO pricing_pending (event_key) SELECT event_key FROM usage_event WHERE event_id=?1", [&id])
                .map_err(|error| error.to_string())?;
            Ok(((), true))
        }).unwrap();
        assert_eq!(queued_event_ids(&state), vec![id]);
        let pass = run_pricing_pass(&state, 1_800_000_000_300, &NEVER_STOP);
        assert!(
            pass.is_ok(),
            "observed-only queue entry is not a pricing failure: {pass:?}"
        );
        assert_eq!(pass.unwrap().priced, 0);
        assert!(queued_event_ids(&state).is_empty());
        assert_eq!(priced_charge_rows(&state), 0);
    }

    #[test]
    fn observed_mixed_pricing_walk_prices_billed_measurement_and_dequeues() {
        let state = test_state();
        let id = seed_media_pricing_event(&state, "mixed-observed-walk", true);
        state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
            ))
            .unwrap();
        assert_eq!(queued_event_ids(&state), vec![id.clone()]);
        assert_eq!(priced_charge_rows(&state), 0);
        let removed = state
            .ordered_write(|tx| {
                let removed = astrocyte_core::pricing::dequeue_if_fully_headed(tx, &id)
                    .map_err(|error| error.to_string())?;
                Ok((removed, removed != 0))
            })
            .unwrap();
        assert_eq!(
            removed, 0,
            "cleanup must preserve a headless billed measurement"
        );
        assert_eq!(queued_event_ids(&state), vec![id.clone()]);
        let pass = run_pricing_pass(&state, 1_800_000_000_300, &NEVER_STOP).unwrap();
        assert_eq!(pass.priced, 1);
        let charge = head(&state, &id);
        assert_eq!(charge.outcome, "priced");
        assert_eq!(charge.charge_nanos, Some(3_000));
        assert_eq!(charge.rows, 1, "observed credits must not receive a charge");
        assert!(queued_event_ids(&state).is_empty());
    }

    fn backfill_due(state: &SharedState) -> Option<i64> {
        state
            .with_conn(|conn| {
                astrocyte_core::subscription::coverage_backfill_due(conn)
                    .map_err(|error| error.message())
            })
            .unwrap()
    }

    /// An installation that has declared no plan is the state production is in
    /// right now, with 34,378 events carrying an uncovered head. In that state
    /// the pass must select exactly what it selected before this existed: an
    /// uncovered head is not work until somebody declares the plan covering it.
    #[test]
    fn with_no_plan_declared_the_pass_still_selects_only_measurements_with_no_head() {
        let state = test_state();
        seed_account_event(
            &state,
            "ev-uncovered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        assert_eq!(
            head(&state, "ev-uncovered").reason.as_deref(),
            Some("no_subscription_in_force")
        );

        seed_account_event(
            &state,
            "ev-fresh",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        // The pass selects the queue, plus the backfill while a plan write is
        // undrained. No plan has been written, so the selection is the queue.
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-fresh".to_string()],
            "with no plan declared, an event that already has a head is not work"
        );
        assert_eq!(backfill_due(&state), None);
    }

    /// The operator flow this was written for: declare the plan, and the events
    /// it covers become covered on the next tick, at zero money and in the
    /// plan's currency.
    #[test]
    fn declaring_a_plan_reprices_the_event_it_covers_to_covered_at_zero() {
        let state = test_state();
        seed_account_event(
            &state,
            "ev-covered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let uncovered = head(&state, "ev-covered");
        assert_eq!(
            uncovered.reason.as_deref(),
            Some("no_subscription_in_force")
        );

        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 1);

        let covered = head(&state, "ev-covered");
        assert_eq!(
            (
                covered.outcome.as_str(),
                covered.reason.as_deref(),
                covered.charge_nanos,
                covered.currency.as_deref(),
                covered.rows,
            ),
            ("covered", None, Some(0), Some("USD"), 2)
        );
        assert_ne!(
            covered.charge_key, uncovered.charge_key,
            "the covered answer supersedes the uncovered one"
        );
    }

    /// Ruling 55: coverage is decided at the event's instant, never at now. An
    /// event recorded before the earliest plan an account has is outside every
    /// range, and must keep the head it has -- byte for byte, since a re-resolve
    /// would reach the same reason and still leave a second row behind.
    #[test]
    fn an_event_earlier_than_every_declared_plan_keeps_its_uncovered_head() {
        let state = test_state();
        seed_account_event(
            &state,
            "ev-early",
            1_700_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let before = head(&state, "ev-early");
        assert_eq!(before.reason.as_deref(), Some("no_subscription_in_force"));

        // Declared to start after the event happened.
        declare_plan(&state, 1_800_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a plan that was not in force at the event's instant covers nothing"
        );
        assert_eq!(head(&state, "ev-early"), before);
    }

    /// Coverage is keyed by provider as well as account: a plan is a contract
    /// with one vendor, and spend with a second vendor is not inside it however
    /// the dates line up. The event here is inside the plan's range and names
    /// the account the plan was declared for, so the provider alone decides.
    ///
    /// Selecting it would be worse than a wasted re-resolve. It would resolve
    /// to the reason it already carries -- coverage is looked up by provider,
    /// account and instant together -- so it would never leave the set, and
    /// every tick for the life of the event would append another superseding
    /// charge row. The set drains only because what it selects becomes covered.
    #[test]
    fn a_plan_under_a_different_provider_does_not_cover_this_account_s_event() {
        let state = test_state();
        seed_event_under(
            &state,
            "ev-other-vendor",
            "openai",
            ACCOUNT,
            1_800_000_000_000,
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let before = head(&state, "ev-other-vendor");
        assert_eq!(before.reason.as_deref(), Some("no_subscription_in_force"));

        // Declared for the same account under PROVIDER, with a range that
        // contains the event's instant.
        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a plan with another vendor covers nothing bought from this one"
        );
        assert_eq!(head(&state, "ev-other-vendor"), before);
    }

    /// The other half of the same key, and the same consequence: one account's
    /// plan says nothing about another account's spend, and an event selected
    /// on the provider alone would be rewritten with the reason it already
    /// carries on every tick, forever.
    #[test]
    fn a_plan_for_a_different_account_does_not_cover_this_providers_event() {
        let state = test_state();
        seed_event_under(
            &state,
            "ev-other-account",
            PROVIDER,
            "account-b",
            1_800_000_000_000,
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        let before = head(&state, "ev-other-account");
        assert_eq!(before.reason.as_deref(), Some("no_subscription_in_force"));

        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a plan held by another account covers nothing this one spent"
        );
        assert_eq!(head(&state, "ev-other-account"), before);
    }

    /// Ruling 95: repricing preserves a head that already carries an answer.
    /// Both events here name the account the plan was declared for and sit
    /// inside its range, so the join alone does not keep them out -- the
    /// outcome of their heads does. The charge id is the assertion because a
    /// re-resolve landing on the same outcome would pass a weaker one.
    #[test]
    fn a_priced_or_covered_head_is_never_offered_to_the_pass_again() {
        let state = test_state();
        let catalog = CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        declare_plan(&state, 1_700_000_000_000);
        seed_account_event(
            &state,
            "ev-metered",
            1_800_000_000_000,
            "metered_api",
            "claude",
        );
        seed_account_event(
            &state,
            "ev-covered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 2);
        let priced = head(&state, "ev-metered");
        let covered = head(&state, "ev-covered");
        assert_eq!(
            (priced.outcome.as_str(), covered.outcome.as_str()),
            ("priced", "covered")
        );

        assert_eq!(
            price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(),
            0,
            "a pass with nothing to do reports nothing, rather than refusing an \
             event it should never have selected"
        );
        assert_eq!(head(&state, "ev-metered"), priced);
        assert_eq!(head(&state, "ev-covered"), covered);
    }

    /// A backlog of tens of thousands must not be repriced inside one tick, and
    /// must still drain: each pass takes a bounded slice and the next pass
    /// resumes past those candidates, even though the old charges still exist.
    #[test]
    fn one_pass_takes_at_most_the_cap_and_the_next_pass_takes_the_rest() {
        let state = test_state();
        let surplus = 2;
        let total = COVERAGE_BACKFILL_PER_PASS + surplus;
        state
            .ordered_write(|tx| {
                for ordinal in 0..total {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:05}"),
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();

        let mut walk = PricingWalk::new(PricingBudget::WHOLE_QUEUE);
        let mut tick = |at| run_pricing_walk(&state, at, &NEVER_STOP, &mut walk).unwrap();

        // No plan yet, so all of these are priced through the no-head selection
        // the pass already had. That selection is uncapped and stays uncapped:
        // newly recorded facts keep their priority over the backfill.
        assert_eq!(tick(1).priced, total);
        assert_eq!(covered_heads(&state), 0);

        declare_plan(&state, 1_700_000_000_000);
        assert_eq!(tick(2).priced, COVERAGE_BACKFILL_PER_PASS);
        assert_eq!(covered_heads(&state), COVERAGE_BACKFILL_PER_PASS as i64);

        assert_eq!(tick(3).priced, surplus);
        assert_eq!(covered_heads(&state), total as i64);
        let quiet = tick(4);
        assert_eq!(quiet.priced, 0);
        assert!(!quiet.backfill_queried);
    }

    /// A catalog that answers discovery but refuses the next `refusals`
    /// pricing lookups, as a peer that goes away after the module connected.
    struct RefusesPricing {
        refusals: AtomicUsize,
        inner: LateCatalog,
    }

    struct BreaksLaterPricingRead {
        state: Arc<SharedState>,
        queue: bool,
    }

    impl CatalogConnection for BreaksLaterPricingRead {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            if request.at_ms == Some(0) {
                return LateCatalog::warming(0).catalog_get(request);
            }
            self.state
                .with_conn(|conn| {
                    conn.execute_batch(if self.queue {
                        "DROP TABLE pricing_pending"
                    } else {
                        "DROP TABLE pricing_selection_state"
                    })
                    .map_err(|error| error.to_string())
                })
                .unwrap();
            Err(CatalogCallError {
                code: "unavailable".into(),
                message: "injected event failure".into(),
            })
        }
    }

    fn assert_later_pricing_read_retains_failures(queue: bool) {
        let state = Arc::new(test_state());
        seed_unpriced_event(&state);
        state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(BreaksLaterPricingRead {
                    state: Arc::clone(&state),
                    queue,
                }))
                .unwrap(),
            ))
            .unwrap();
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 1,
            tick_events: 10,
            tick_time: Duration::from_secs(30),
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });
        let error = run_pricing_walk(&state, 1, &NEVER_STOP, &mut walk).unwrap_err();
        state.install_catalog(None).unwrap();
        assert_eq!(
            priced_charge_rows(&state),
            0,
            "failed event must stay headless"
        );
        assert!(
            error.contains("ev-late") && error.contains("injected event failure"),
            "lost event failure: {error}"
        );
        assert!(
            error.contains("no such table"),
            "lost later stage failure: {error}"
        );
    }

    #[test]
    fn later_queue_read_failure_keeps_prior_event_failures() {
        assert_later_pricing_read_retains_failures(true);
    }

    #[test]
    fn later_backfill_read_failure_keeps_prior_event_failures() {
        assert_later_pricing_read_retains_failures(false);
    }

    /// Takes one from `count` if it is above zero, and says whether it did.
    /// Written as a compare-exchange loop because `fetch_update` is deprecated
    /// from Rust 1.99 while its replacement `try_update` doesn't exist on 1.98,
    /// and this has to build on both.
    fn take_one(count: &AtomicUsize) -> bool {
        let mut current = count.load(Ordering::SeqCst);
        while let Some(next) = current.checked_sub(1) {
            match count.compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    impl CatalogConnection for RefusesPricing {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            let pricing_lookup = request.at_ms != Some(0);
            if pricing_lookup && take_one(&self.refusals) {
                return Err(CatalogCallError {
                    code: "unavailable".into(),
                    message: "fusiform is restarting".into(),
                });
            }
            self.inner.catalog_get(request)
        }
    }

    /// Deferred pricing writes no head, so the event must stay queued and be
    /// priced by a later pass. Two deferrals are exercised: the tick that
    /// skips pricing while the catalog is still pending, and a pass whose
    /// catalog lookup is refused. A queue that dropped an event when it was
    /// selected, rather than when it was headed, would lose it at the second.
    #[test]
    fn a_deferred_event_stays_queued_and_is_priced_on_a_later_pass() {
        let state = test_state();
        seed_unpriced_event(&state);
        let stop = AtomicBool::new(false);
        assert_eq!(queued_event_ids(&state), vec!["ev-late".to_string()]);

        run_iteration_full(&state, &[], None, true, &stop);
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-late".to_string()],
            "a tick that skips pricing leaves the event queued"
        );

        let catalog = CatalogClient::connect(Arc::new(RefusesPricing {
            refusals: AtomicUsize::new(1),
            inner: LateCatalog::warming(0),
        }))
        .unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        assert!(price_unpriced_events(&state, 1, &NEVER_STOP).is_err());
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-late".to_string()],
            "a refused catalog lookup writes no head, so the event stays queued"
        );

        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 1);
        assert!(priced_charge_rows(&state) > 0);
        assert!(queued_event_ids(&state).is_empty());
    }

    /// A catalog that runs `action` once, during the lookup for the event that
    /// occurred at `trigger_at_ms`. The pass resolves a whole group before it
    /// commits any of it, so the action lands after the events ahead of that
    /// one were resolved and before the group commits: the window in which an
    /// operator can move an event under a plan the pass already holds.
    struct ActsDuringLookup {
        inner: LateCatalog,
        trigger_at_ms: i64,
        action: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl ActsDuringLookup {
        fn new(trigger_at_ms: i64, action: impl FnOnce() + Send + 'static) -> Self {
            Self {
                inner: LateCatalog::warming(0),
                trigger_at_ms,
                action: std::sync::Mutex::new(Some(Box::new(action))),
            }
        }
    }

    impl CatalogConnection for ActsDuringLookup {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            if request.at_ms == Some(self.trigger_at_ms) {
                // Taken out before it runs, so an action that itself calls
                // the catalog does not wait on this lock.
                let action = self.action.lock().unwrap().take();
                if let Some(action) = action {
                    action();
                }
            }
            self.inner.catalog_get(request)
        }
    }

    fn charge_rows_by_origin(state: &SharedState, event_id: &str) -> Vec<(String, i64)> {
        state
            .with_conn(|conn| {
                conn.prepare(
                    "SELECT origin,pricing_generation FROM priced_charge
                     WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY pricing_generation",
                )
                .and_then(|mut statement| {
                    statement
                        .query_map([event_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// Five events are priced in one group. While the last one is being
    /// resolved, an operator declares the plan covering `ev-b` and reprices
    /// `ev-d` by hand, so the plans the pass holds for those two are stale:
    /// `ev-b`'s assumed no coverage, `ev-d`'s names a pricing generation that
    /// has moved on. Both are refused at commit, and the refusal takes neither
    /// the events before them nor the ones after them in the group with it.
    #[test]
    fn events_that_moved_before_their_group_commits_are_refused_and_the_rest_commit() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        seed_account_event(&state, "ev-a", instant, "metered_api", "claude");
        seed_account_event(
            &state,
            "ev-b",
            instant + 1_000,
            "subscription_included",
            "claude",
        );
        seed_account_event(&state, "ev-c", instant + 2_000, "metered_api", "claude");
        seed_account_event(&state, "ev-d", instant + 3_000, "metered_api", "claude");
        seed_account_event(&state, "ev-e", instant + 4_000, "metered_api", "claude");
        let operator = state.clone();
        let peer = ActsDuringLookup::new(instant + 4_000, move || {
            declare_plan(&operator, 1_700_000_000_000);
            crate::ops::dispatch(
                &operator,
                crate::ops::OP_EVENT_REPRICE,
                serde_json::json!({"event_id": "ev-d"}),
            )
            .unwrap();
        });
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();

        let error = price_unpriced_events(&state, 1, &NEVER_STOP).unwrap_err();
        assert!(error.contains("event ev-b:"), "{error}");
        assert!(error.contains("event ev-d:"), "{error}");
        for committed in ["ev-a", "ev-c", "ev-e"] {
            assert!(!error.contains(committed), "{error}");
            assert_eq!(
                charge_rows_by_origin(&state, committed),
                vec![("pricing_pass".to_string(), 1)],
                "{committed} commits in the same group as the refused events"
            );
        }
        assert!(
            charge_rows_by_origin(&state, "ev-b").is_empty(),
            "the event whose coverage moved is untouched"
        );
        assert_eq!(
            charge_rows_by_origin(&state, "ev-d"),
            vec![("operator_reprice".to_string(), 1)],
            "only the operator's reprice of ev-d is recorded; the pass's stale one was refused"
        );
        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-b".to_string()],
            "the refused event stays queued"
        );

        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 1);
        assert_eq!(head(&state, "ev-b").outcome, "covered");
        assert!(queued_event_ids(&state).is_empty());
    }

    /// A stop seen while a group is being resolved drops the whole group:
    /// the events resolved before the stop are not committed either.
    #[test]
    fn a_stop_during_a_group_commits_nothing_from_it() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        for (ordinal, event_id) in ["ev-a", "ev-b", "ev-c"].into_iter().enumerate() {
            seed_account_event(
                &state,
                event_id,
                instant + 1_000 * ordinal as i64,
                "metered_api",
                "claude",
            );
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let peer = ActsDuringLookup::new(instant + 1_000, move || {
            stopping.store(true, Ordering::SeqCst);
        });
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();

        assert_eq!(price_unpriced_events(&state, 1, &stop).unwrap(), 0);
        assert_eq!(priced_charge_rows(&state), 0);
        assert_eq!(queued_event_ids(&state), vec!["ev-a", "ev-b", "ev-c"]);
    }

    #[test]
    fn pricing_time_budget_commits_the_resolved_prefix_and_resumes_remaining_events() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        for (ordinal, event_id) in ["ev-a", "ev-b", "ev-c"].into_iter().enumerate() {
            seed_account_event(
                &state,
                event_id,
                instant + ordinal as i64,
                "metered_api",
                "claude",
            );
        }
        let elapsed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let lookup_elapsed = elapsed.clone();
        let peer = ActsDuringLookup::new(instant, move || {
            lookup_elapsed.store(200, Ordering::SeqCst);
        });
        let origin = std::time::Instant::now();
        let now = || origin + Duration::from_millis(elapsed.load(Ordering::SeqCst));
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 100,
            tick_events: usize::MAX,
            tick_time: Duration::from_millis(200),
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });

        let pass = run_pricing_walk_with_clock(&state, 1, &NEVER_STOP, &mut walk, &now).unwrap();
        assert_eq!(now().duration_since(origin), walk.budget.tick_time);
        assert_eq!(
            pass.priced, 1,
            "no catalog call may start after the budget expires"
        );
        assert!(pass.paused);
        assert_eq!(queued_event_ids(&state), vec!["ev-b", "ev-c"]);
        assert_eq!(walk.after.as_deref(), Some("ev-a"));
        assert_eq!(
            charge_rows_by_origin(&state, "ev-a"),
            vec![("pricing_pass".to_string(), 1)]
        );

        walk.budget.tick_time = Duration::MAX;
        assert_eq!(
            run_pricing_walk_with_clock(&state, 2, &NEVER_STOP, &mut walk, &now)
                .unwrap()
                .priced,
            2
        );
        assert!(queued_event_ids(&state).is_empty());
        for event_id in ["ev-a", "ev-b", "ev-c"] {
            assert_eq!(
                charge_rows_by_origin(&state, event_id),
                vec![("pricing_pass".to_string(), 1)]
            );
        }
    }

    #[test]
    fn pricing_time_budget_keeps_backfill_due_after_queue_uses_budget() {
        let state = test_state();
        let instant = 1_800_000_000_000;
        for (ordinal, event_id) in ["ev-a", "ev-b", "ev-c"].into_iter().enumerate() {
            seed_account_event(
                &state,
                event_id,
                instant + ordinal as i64,
                "subscription_included",
                "claude",
            );
        }
        state
            .install_catalog(Some(
                CatalogClient::connect(Arc::new(LateCatalog::warming(0))).unwrap(),
            ))
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 3);
        declare_plan(&state, 1_700_000_000_000);
        seed_account_event(&state, "ev-0", instant - 1, "metered_api", "claude");
        let elapsed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let lookup_elapsed = elapsed.clone();
        let peer = ActsDuringLookup::new(instant - 1, move || {
            lookup_elapsed.store(200, Ordering::SeqCst);
        });
        let origin = std::time::Instant::now();
        let now = || origin + Duration::from_millis(elapsed.load(Ordering::SeqCst));
        state
            .install_catalog(Some(CatalogClient::connect(Arc::new(peer)).unwrap()))
            .unwrap();
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 100,
            tick_events: usize::MAX,
            tick_time: Duration::from_millis(200),
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });
        let pass = run_pricing_walk_with_clock(&state, 2, &NEVER_STOP, &mut walk, &now).unwrap();
        assert_eq!(now().duration_since(origin), walk.budget.tick_time);
        assert_eq!(pass.priced, 1);
        assert!(queued_event_ids(&state).is_empty());
        assert!(!pass.backfill_queried);
        assert_eq!(charge_rows_by_origin(&state, "ev-a").len(), 1);
        assert_eq!(charge_rows_by_origin(&state, "ev-b").len(), 1);
        assert!(state
            .with_conn(
                |conn| astrocyte_core::subscription::coverage_backfill_due(conn)
                    .map_err(|error| error.message().to_string())
            )
            .unwrap()
            .is_some());
        walk.budget.tick_time = Duration::MAX;
        assert_eq!(
            run_pricing_walk_with_clock(&state, 3, &NEVER_STOP, &mut walk, &now)
                .unwrap()
                .priced,
            3
        );
        assert!(state
            .with_conn(
                |conn| astrocyte_core::subscription::coverage_backfill_due(conn)
                    .map_err(|error| error.message().to_string())
            )
            .unwrap()
            .is_none());
        for event_id in ["ev-a", "ev-b", "ev-c"] {
            assert_eq!(
                charge_rows_by_origin(&state, event_id),
                vec![
                    ("pricing_pass".to_string(), 1),
                    ("pricing_pass".to_string(), 2)
                ]
            );
        }
        assert_eq!(
            charge_rows_by_origin(&state, "ev-0"),
            vec![("pricing_pass".to_string(), 1)]
        );
    }

    /// What a SQLite write-ahead log holds: every frame, and the commit
    /// frames among them, one per committed write transaction and so one per
    /// fsync of the log under `synchronous=FULL`. Read from the file itself
    /// rather than counted by the code under test. Frames are counted while
    /// they carry the header's salt, which is where a log that was restarted
    /// ends.
    #[derive(Debug, Clone, Copy, Default)]
    struct WalFrames {
        frames: usize,
        commits: usize,
        /// Bytes those frames take in the log, frame headers included.
        bytes: usize,
    }

    fn wal_frames(path: &std::path::Path) -> WalFrames {
        let bytes = std::fs::read(path).unwrap_or_default();
        if bytes.len() < 32 {
            return WalFrames::default();
        }
        let word = |at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
        let page_size = word(8) as usize;
        let salt = &bytes[16..24];
        let mut counted = WalFrames::default();
        let mut frame = 32;
        while frame + 24 + page_size <= bytes.len() && &bytes[frame + 8..frame + 16] == salt {
            counted.frames += 1;
            counted.bytes += 24 + page_size;
            // A frame's database size after commit is non-zero only on the
            // last frame of a transaction.
            if word(frame + 4) != 0 {
                counted.commits += 1;
            }
            frame += 24 + page_size;
        }
        counted
    }

    fn wal_commit_frames(path: &std::path::Path) -> usize {
        wal_frames(path).commits
    }

    /// Prices whatever is queued in a COPY of the live store
    /// (`PRICING_REHEARSAL_DB`) in commit groups of `PRICING_REHEARSAL_GROUP`
    /// events (default `PRICING_COMMIT_GROUP`; 1 is the old pass's one
    /// transaction per event), through the real fusiform client when
    /// `PRICING_REHEARSAL_SUBC` names a daemon connection file and with no
    /// catalog otherwise. Prints the events priced, the write-ahead-log
    /// commits and bytes, and the wall time. Checkpoints are turned off on the
    /// copy so the log keeps every frame the pass wrote. Ignored by default:
    /// it needs the copy, and it writes to it.
    #[test]
    #[ignore]
    fn pricing_rehearsal_on_a_store_copy() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let database = std::env::var("PRICING_REHEARSAL_DB").expect("PRICING_REHEARSAL_DB");
        let group = std::env::var("PRICING_REHEARSAL_GROUP")
            .map(|group| group.parse::<usize>().expect("PRICING_REHEARSAL_GROUP"))
            .unwrap_or(PRICING_COMMIT_GROUP);
        let store = open_sqlite(&StorageDescriptor {
            module_id: "ck-astrocyte-pricing-rehearsal".to_string(),
            storage_namespace: "astrocyte".to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: database.clone(),
            },
        })
        .unwrap();
        astrocyte_core::store::migrate_store(&store).unwrap();
        let state = SharedState::from_store(store).unwrap();
        let synchronous: i64 = state
            .with_conn(|conn| {
                conn.pragma_update(None, "wal_autocheckpoint", 0)
                    .and_then(|()| conn.pragma_query_value(None, "synchronous", |row| row.get(0)))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        if let Ok(subc) = std::env::var("PRICING_REHEARSAL_SUBC") {
            let client = Arc::new(crate::fusiform_client::FusiformClient::new(subc).unwrap());
            let connection: Arc<dyn CatalogConnection> = client;
            state
                .install_catalog(Some(CatalogClient::connect(connection).unwrap()))
                .unwrap();
        }
        let queued = queued_event_ids(&state).len();
        let wal = std::path::PathBuf::from(format!("{database}-wal"));
        let before = wal_frames(&wal);
        let started = std::time::Instant::now();
        let outcome = run_pricing_walk(
            &state,
            current_time_ms(),
            &NEVER_STOP,
            &mut PricingWalk::new(PricingBudget {
                group_events: group,
                tick_events: usize::MAX,
                tick_time: Duration::MAX,
                backfill_events: COVERAGE_BACKFILL_PER_PASS,
            }),
        );
        let elapsed = started.elapsed();
        let after = wal_frames(&wal);
        let (priced, failed) = match &outcome {
            Ok(pass) => (pass.priced, 0),
            Err(error) => (
                queued - queued_event_ids(&state).len(),
                error.matches("event ").count(),
            ),
        };
        println!(
            "group {group} synchronous {synchronous} queued {queued} priced {priced} failed {failed} \
             commits {} wal_frames {} wal_bytes {} wall_ms {}",
            after.commits - before.commits,
            after.frames - before.frames,
            after.bytes - before.bytes,
            elapsed.as_millis()
        );
    }

    /// Pricing N queued events commits ceil(N / group) transactions, counted
    /// as commit frames in the store's write-ahead log.
    #[test]
    fn pricing_commits_one_transaction_per_group() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let wal = directory.path().join("ledger.db-wal");
        // No checkpoint may restart the log while it is being counted.
        state
            .with_conn(|conn| {
                conn.pragma_update(None, "wal_autocheckpoint", 0)
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        // The first pass on a store also marks the coverage backfill drained,
        // which is one more commit; take it out of the count.
        run_pricing_pass(&state, 1, &NEVER_STOP).unwrap();
        let events = 2 * PRICING_COMMIT_GROUP + 1;
        state
            .ordered_write(|tx| {
                for ordinal in 0..events {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:05}"),
                        1_800_000_000_000,
                        "metered_api",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();

        let before = wal_commit_frames(&wal);
        let pass = run_pricing_pass(&state, 2, &NEVER_STOP).unwrap();
        let commits = wal_commit_frames(&wal) - before;

        assert_eq!(pass.priced, events);
        assert!(queued_event_ids(&state).is_empty());
        assert_eq!(commits, events.div_ceil(PRICING_COMMIT_GROUP));
    }

    /// Seed `events` queued events, then run refresher ticks with `budget`
    /// until the queue is empty. Returns how many events were still queued
    /// after each tick, after checking that every tick drained the producer
    /// and that every event ended up priced exactly once.
    fn queue_left_after_each_tick(events: usize, budget: PricingBudget) -> Vec<usize> {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..events {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:02}"),
                        1_800_000_000_000,
                        "metered_api",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        let pulls = Arc::new(AtomicUsize::new(0));
        let producers = [Producer {
            // The test store seeds a runtime row only for this key.
            source_key: "broca".into(),
            client: Arc::new(CountingSource {
                pulls: Arc::clone(&pulls),
            }),
        }];
        let mut pricing = PricingWalk::new(budget);
        let mut reconciler = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        let file_sources = FileSources::default();
        let mut left = Vec::new();
        while left.last() != Some(&0) {
            assert!(left.len() < 20, "the backlog never drained: {left:?}");
            let pulls_before = pulls.load(Ordering::SeqCst);
            run_tick(
                &state,
                &producers,
                &file_sources,
                None,
                &mut None,
                &mut MultiplierStage::new(None),
                &mut pricing,
                &mut reconciler,
                &NEVER_STOP,
            );
            assert!(
                pulls.load(Ordering::SeqCst) > pulls_before,
                "tick {} did not drain the producer",
                left.len() + 1
            );
            left.push(queued_event_ids(&state).len());
        }
        for ordinal in 0..events {
            let event_id = format!("ev-{ordinal:02}");
            assert_eq!(
                charge_rows_by_origin(&state, &event_id),
                vec![("pricing_pass".to_string(), 1)],
                "{event_id} is priced exactly once"
            );
        }
        left
    }

    /// A queue larger than the event cap is spread over ticks, three events a
    /// tick in groups of two, while every tick still polls the fake BROCA source.
    #[test]
    fn a_backlog_over_the_event_cap_is_priced_across_ticks_and_every_tick_drains() {
        let capped = PricingBudget {
            group_events: 2,
            tick_events: 3,
            tick_time: Duration::MAX,
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        };
        assert_eq!(queue_left_after_each_tick(10, capped), vec![7, 4, 1, 0]);
    }

    #[test]
    fn a_zero_pricing_time_budget_leaves_every_event_queued() {
        let state = test_state();
        seed_account_event(&state, "ev-a", 1_800_000_000_000, "metered_api", "claude");
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 2,
            tick_events: usize::MAX,
            tick_time: Duration::ZERO,
            backfill_events: COVERAGE_BACKFILL_PER_PASS,
        });
        let pass = run_pricing_walk(&state, 1, &NEVER_STOP, &mut walk).unwrap();
        assert_eq!(pass.priced, 0);
        assert!(pass.paused);
        assert_eq!(queued_event_ids(&state), vec!["ev-a"]);
        assert!(charge_rows_by_origin(&state, "ev-a").is_empty());
    }

    /// The coverage backfill query runs only while a plan write is undrained.
    /// Asserted on whether the query ran, because a drained backfill returns
    /// nothing either way and its result cannot say whether it was skipped.
    #[test]
    fn regression_backfill_advances_across_empty_candidate_pages() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..5 {
                    let at = if ordinal < 3 {
                        1_600_000_000_000
                    } else {
                        1_800_000_000_000
                    };
                    insert_account_event(
                        tx,
                        &format!("coverage-{ordinal}"),
                        at,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        run_pricing_pass(&state, 1, &NEVER_STOP).unwrap();
        declare_plan(&state, 1_700_000_000_000);
        let mut walk = PricingWalk::new(PricingBudget {
            backfill_events: 2,
            ..PricingBudget::WHOLE_QUEUE
        });
        let mut run = |at| {
            run_pricing_walk(&state, at, &NEVER_STOP, &mut walk)
                .unwrap()
                .priced
        };
        assert_eq!(
            run(2),
            0,
            "the first two candidates are outside plan coverage"
        );
        assert!(
            backfill_due(&state).is_some(),
            "an empty page is not the end"
        );
        assert_eq!(run(3), 1);
        assert_eq!(run(4), 1);
        assert_eq!(backfill_due(&state), None);
        for event in ["coverage-3", "coverage-4"] {
            assert_eq!(head(&state, event).outcome, "covered");
        }
    }

    #[test]
    fn regression_backfill_selection_vm_steps_follow_page_not_history() {
        let mut steps = Vec::new();
        for count in [20, 400] {
            let state = test_state();
            state
                .ordered_write(|tx| {
                    for ordinal in 0..count {
                        insert_account_event(
                            tx,
                            &format!("old-{ordinal:05}"),
                            1_600_000_000_000,
                            "subscription_included",
                            "claude",
                        )?;
                    }
                    insert_account_event(
                        tx,
                        "in-window",
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                    Ok(((), true))
                })
                .unwrap();
            run_pricing_pass(&state, 1, &NEVER_STOP).unwrap();
            declare_plan(&state, 1_700_000_000_000);
            let mut walk = PricingWalk::new(PricingBudget {
                backfill_events: 8,
                ..PricingBudget::WHOLE_QUEUE
            });
            let pass = run_pricing_walk(&state, 2, &NEVER_STOP, &mut walk).unwrap();
            assert_eq!(pass.priced, 0);
            assert!(pass.backfill_selection_steps > 0);
            steps.push(pass.backfill_selection_steps);
        }
        eprintln!(
            "coverage backfill first-page SQLite steps at 20 / 400 historical charges: {steps:?}"
        );
        assert!(steps[1] <= steps[0] + 32, "{steps:?}");
    }

    #[test]
    fn coverage_backfill_runs_after_a_plan_write_and_not_on_a_quiet_tick() {
        let state = test_state();
        assert!(
            run_pricing_pass(&state, 1, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "the first pass after the upgrade drains whatever plan writes preceded it"
        );
        assert!(
            !run_pricing_pass(&state, 2, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "no plan changed since the backfill drained, so the query does not run"
        );

        declare_plan(&state, 1_700_000_000_000);
        assert!(
            run_pricing_pass(&state, 3, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
        assert!(
            !run_pricing_pass(&state, 4, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );

        state
            .ordered_write(|tx| {
                astrocyte_core::subscription::close_subscription_in_tx(
                    tx,
                    &astrocyte_core::subscription::CloseSubscriptionRequest {
                        account_ref: ACCOUNT.into(),
                        provider: PROVIDER.into(),
                        effective_to: 1_750_000_000_000,
                    },
                )
                .map_err(|error| error.message())?;
                Ok(((), true))
            })
            .unwrap();
        assert!(
            run_pricing_pass(&state, 5, &NEVER_STOP)
                .unwrap()
                .backfill_queried,
            "closing a plan moves the generation too"
        );
        assert!(
            !run_pricing_pass(&state, 6, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
    }

    /// A stopped pass has not repriced what the backfill would find, so it
    /// must leave the plan write undrained for the next pass.
    #[test]
    fn a_stopped_pass_leaves_the_coverage_backfill_due() {
        let state = test_state();
        declare_plan(&state, 1_700_000_000_000);
        let due = backfill_due(&state);
        assert!(due.is_some());
        let stopped = run_pricing_pass(&state, 1, &AtomicBool::new(true)).unwrap();
        assert!(!stopped.backfill_queried);
        assert_eq!(backfill_due(&state), due);
        assert!(
            run_pricing_pass(&state, 2, &NEVER_STOP)
                .unwrap()
                .backfill_queried
        );
        assert_eq!(backfill_due(&state), None);
    }

    /// A backfill batch that came back full has not proved the backlog is
    /// empty, so the generation stays undrained until a short batch.
    #[test]
    fn a_full_backfill_batch_does_not_mark_the_generation_drained() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..COVERAGE_BACKFILL_PER_PASS {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal:05}"),
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        run_pricing_pass(&state, 1, &NEVER_STOP).unwrap();
        declare_plan(&state, 1_700_000_000_000);
        let mut walk = PricingWalk::new(PricingBudget::WHOLE_QUEUE);
        let mut tick = |at| run_pricing_walk(&state, at, &NEVER_STOP, &mut walk).unwrap();
        let full = tick(2);
        assert!(full.backfill_queried);
        assert_eq!(full.priced, COVERAGE_BACKFILL_PER_PASS);
        assert!(
            tick(3).backfill_queried,
            "a full batch may have left work, so the next pass looks again"
        );
        assert!(!tick(4).backfill_queried);
    }

    /// An event that keeps failing to reprice must not hold the backfill on
    /// itself. The batch here is one event, the shape in which a set that
    /// always started from its beginning would return the failing event on
    /// every tick and never reach the events behind it. The failure is a
    /// store trigger refusing that one event's charge row, which stays in
    /// force until the test drops it, so it is a refusal on every attempt and
    /// not a transient one a retry would clear by itself.
    #[test]
    fn a_failing_backfill_event_is_retried_without_starving_the_rest() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for ordinal in 0..4 {
                    insert_account_event(
                        tx,
                        &format!("ev-{ordinal}"),
                        1_800_000_000_000,
                        "subscription_included",
                        "claude",
                    )?;
                }
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 4);
        declare_plan(&state, 1_700_000_000_000);
        state
            .with_conn(|conn| {
                conn.execute_batch(
                    "CREATE TRIGGER refuse_ev_0 BEFORE INSERT ON priced_charge
                     WHEN NEW.event_key=(SELECT event_key FROM usage_event WHERE event_id='ev-0')
                     BEGIN SELECT RAISE(ABORT, 'injected refusal'); END;",
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        let before = head(&state, "ev-0");
        let mut walk = PricingWalk::new(PricingBudget {
            group_events: 1,
            tick_events: usize::MAX,
            tick_time: Duration::MAX,
            backfill_events: 1,
        });
        let mut tick = |at: i64| run_pricing_walk(&state, at, &NEVER_STOP, &mut walk);

        let refused = tick(2).unwrap_err();
        assert!(refused.contains("ev-0"), "{refused}");
        for at in 3..=5 {
            assert_eq!(
                tick(at).unwrap().priced,
                1,
                "tick {at} reprices the next event"
            );
        }
        for event_id in ["ev-1", "ev-2", "ev-3"] {
            assert_eq!(
                head(&state, event_id).outcome,
                "covered",
                "{event_id} is reached past the failing event"
            );
        }
        // This batch reaches the end of the set, so the sweep ends here; the
        // refused event is still in the set, so the plan write is not drained.
        assert_eq!(tick(6).unwrap().priced, 0);
        assert!(backfill_due(&state).is_some());
        let retried = tick(7).unwrap_err();
        assert!(
            retried.contains("ev-0"),
            "the next sweep retries it: {retried}"
        );
        assert_eq!(
            head(&state, "ev-0"),
            before,
            "a refused event keeps its head, unsuperseded"
        );
        assert_eq!(
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COUNT(*) FROM usage_event WHERE event_id='ev-0'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap(),
            1,
            "a refused event is never deleted"
        );

        state
            .with_conn(|conn| {
                conn.execute_batch("DROP TRIGGER refuse_ev_0")
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        // The sweep that retried it saw it fail. It still has to visit the
        // three now-superseded candidates before reaching the end.
        for at in 8..=11 {
            assert_eq!(tick(at).unwrap().priced, 0);
        }
        assert!(backfill_due(&state).is_some());
        assert_eq!(tick(12).unwrap().priced, 1);
        assert_eq!(head(&state, "ev-0").outcome, "covered");
        assert!(
            backfill_due(&state).is_some(),
            "a full batch is not the end"
        );
        for at in 13..=16 {
            assert_eq!(tick(at).unwrap().priced, 0);
        }
        assert_eq!(
            backfill_due(&state),
            None,
            "a sweep with no failure drains the plan write"
        );
    }

    /// Finding pricing work costs what is queued, not what the ledger holds.
    /// With nothing queued, the queue read over a ledger of 400 events takes
    /// exactly as many SQLite steps as over a ledger of 20. SQLite's own
    /// per-statement step counter is used rather than a timing, so the
    /// comparison is exact and cannot flake. A selection that scanned
    /// measurements, as the pass once did, would take steps in proportion to
    /// the ledger.
    #[test]
    fn with_nothing_queued_selection_cost_does_not_grow_with_the_ledger() {
        fn steps_over_ledger(events: usize) -> i64 {
            let state = test_state();
            state
                .ordered_write(|tx| {
                    for ordinal in 0..events {
                        insert_account_event(
                            tx,
                            &format!("ev-{ordinal:05}"),
                            1_800_000_000_000,
                            "metered_api",
                            "claude",
                        )?;
                    }
                    Ok(((), true))
                })
                .unwrap();
            assert_eq!(
                run_pricing_pass(&state, 1, &NEVER_STOP).unwrap().priced,
                events
            );
            assert!(queued_event_ids(&state).is_empty());
            let pass = run_pricing_pass(&state, 2, &NEVER_STOP).unwrap();
            assert_eq!(pass.priced, 0);
            assert!(!pass.backfill_queried);
            pass.queue_selection_steps
        }

        let small = steps_over_ledger(20);
        let large = steps_over_ledger(400);
        assert!(small > 0, "the queue read ran and was counted");
        assert_eq!(small, large);
    }

    /// Ruling 86 is why this is affordable at all: coverage is answered from
    /// our own subscription table, so backfilling it must not put a catalog
    /// call in a tick. The decoy is the event left unpriced for a catalog
    /// reason -- it names the same covered account and instant, and only the
    /// reason on its head keeps it out of the backfill. Selecting it would
    /// reach the catalog, and the call count is what says it was not selected.
    #[test]
    fn backfilling_declared_coverage_makes_no_catalog_call() {
        let state = test_state();
        let peer = Arc::new(LateCatalog::warming(0));
        state
            .install_catalog(Some(CatalogClient::connect(peer.clone()).unwrap()))
            .unwrap();
        seed_account_event(
            &state,
            "ev-covered",
            1_800_000_000_000,
            "subscription_included",
            "claude",
        );
        // A model this catalog serves no rate for: its head is unpriced for a
        // catalog reason, and resolving it again calls the catalog again.
        seed_account_event(
            &state,
            "ev-no-rate",
            1_800_000_000_000,
            "metered_api",
            "opus",
        );
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 2);
        assert_eq!(
            head(&state, "ev-no-rate").reason.as_deref(),
            Some("no_rate")
        );

        declare_plan(&state, 1_700_000_000_000);
        let calls_before = peer.calls.load(Ordering::SeqCst);
        let repriced = price_unpriced_events(&state, 2, &NEVER_STOP).unwrap();

        // Asserted before the count, so that this test fails on the claim it
        // exists for rather than on a symptom of the same defect.
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            calls_before,
            "a coverage backfill resolves from local tables only"
        );
        assert_eq!(repriced, 1);
        assert_eq!(head(&state, "ev-covered").outcome, "covered");
        assert_eq!(
            head(&state, "ev-no-rate").reason.as_deref(),
            Some("no_rate")
        );
    }
}
