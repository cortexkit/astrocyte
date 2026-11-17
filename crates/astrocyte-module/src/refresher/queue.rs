//! The daily pricing queue backstop has its own resumable walk, separate from ordinary pricing.

use super::*;

/// How long after one pricing queue check finishes the next one starts. The
/// last completion is durable, so restarting does not repeat a recent check.
///
/// The check re-runs the old whole-ledger selection. It is expensive: on the
/// production store (1.26 million events, 6.3 million measurements, 251
/// chunks) a full pass took about four minutes of wall time in the running
/// daemon, roughly one second per chunk, almost all of it inside
/// `reconcile_pending_range_in_tx`; on an idle copy of that store it took
/// 146 seconds, 0.3 to 1.5 seconds per chunk with a median of 0.5. That is
/// why a pass is spread over loop ticks under `QUEUE_RECONCILE_TICK_BUDGET`
/// instead of run in one go: run in one go it held up every drain for those
/// four minutes. Spread out, the copy's pass took 88 ticks, about 45 minutes
/// at a 30-second poll.
///
/// What it guards against is a write path that records an event without
/// enqueuing it; such an event would otherwise never be priced and nothing
/// would say so. Recording enqueues in the same transaction, so a miss means
/// a write path that bypassed it. A pass reads the whole ledger, so the check
/// runs daily: an unqueued event is found within one day plus one pass, which
/// is short for a spend ledger read in days.
const QUEUE_RECONCILE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Events per reconciliation chunk. Each chunk is one short write transaction
/// over a range of event ids, so the whole-ledger check never holds the store
/// for longer than one chunk and a stop is noticed between chunks.
pub(super) const QUEUE_RECONCILE_CHUNK: NonZeroUsize = NonZeroUsize::new(5_000).unwrap();

/// Wall time one loop tick may spend on the pricing queue check before it
/// returns to the loop and lets the next tick drain the sources.
///
/// A chunk costs about half a second to one second on the production store
/// (see `QUEUE_RECONCILE_INTERVAL`), so two seconds is two to four chunks per
/// tick: small next to a poll interval of tens of
/// seconds, so ingest stays fresh, while still making steady progress. A tick
/// always runs at least one chunk, so a pass finishes however slow a chunk
/// is, and it starts another only when the slowest chunk seen this tick would
/// still fit, so a tick overruns the budget by less than one chunk only when
/// chunk times vary.
pub(super) const QUEUE_RECONCILE_TICK_BUDGET: Duration = Duration::from_secs(2);

/// How often a running reconciliation statement looks at the stop flag.
const RECONCILE_INTERRUPT_POLL: Duration = Duration::from_millis(20);

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileOutcome {
    /// Every event was checked; `enqueued` were needing pricing but unqueued.
    Completed { enqueued: usize },
    /// The stop flag ended the check early; chunks already committed keep
    /// what they enqueued.
    Stopped { enqueued: usize },
}

/// Check the pricing queue against the whole ledger in one call and enqueue
/// every event that needs pricing but is not queued. The loop does not use
/// this; it spreads the same walk over ticks with `QueueReconciler`.
#[cfg(test)]
pub(crate) fn reconcile_pricing_queue(
    state: &SharedState,
    stop: &AtomicBool,
) -> Result<ReconcileOutcome, String> {
    reconcile_pricing_queue_in_chunks(state, stop, QUEUE_RECONCILE_CHUNK)
}

#[cfg(test)]
fn reconcile_pricing_queue_in_chunks(
    state: &SharedState,
    stop: &AtomicBool,
    chunk_events: NonZeroUsize,
) -> Result<ReconcileOutcome, String> {
    let mut walk = ReconcileWalk::default();
    loop {
        match walk.advance(state, stop, chunk_events, Duration::MAX)? {
            WalkProgress::Paused => {}
            WalkProgress::Completed => {
                return Ok(ReconcileOutcome::Completed {
                    enqueued: walk.enqueued,
                })
            }
            WalkProgress::Stopped => {
                return Ok(ReconcileOutcome::Stopped {
                    enqueued: walk.enqueued,
                })
            }
        }
    }
}

/// One reconciliation chunk's result.
enum ChunkStep {
    /// The stop flag ended the chunk; it committed nothing.
    Stopped,
    /// The chunk committed. `through` is the last event id it covered, or
    /// `None` when it was the final, open-ended chunk.
    Advanced {
        through: Option<String>,
        added: usize,
        examined: usize,
    },
}

/// Check the events in the id range after `after` (from the start when
/// `None`), up to `chunk_events` of them, in one write transaction. A stop
/// during the chunk interrupts its SQLite statement, which rolls it back.
fn reconcile_chunk(
    state: &SharedState,
    stop: &AtomicBool,
    after: Option<&str>,
    chunk_events: NonZeroUsize,
) -> Result<ChunkStep, String> {
    let mut stopped = false;
    let chunk = state.ordered_write(|tx| {
        let result = interrupt_on_stop(tx, stop, || {
            let through = astrocyte_core::pricing::reconcile_chunk_end(tx, after, chunk_events)?;
            let added = astrocyte_core::pricing::reconcile_pending_range_in_tx(
                tx,
                after,
                through.as_deref(),
            )?;
            // A bounded chunk covers exactly `chunk_events` events by
            // construction; only the open-ended last one has to be counted.
            let examined = match through {
                Some(_) => chunk_events.get(),
                None => {
                    let (comparison, cursor) = match after {
                        Some(after) => (">", after),
                        None => (">=", ""),
                    };
                    let count: i64 = tx.query_row(
                        &format!("SELECT COUNT(*) FROM usage_event WHERE event_id {comparison} ?1"),
                        [cursor],
                        |row| row.get(0),
                    )?;
                    usize::try_from(count).unwrap_or(0)
                }
            };
            if through.is_none() {
                astrocyte_core::pricing::mark_queue_reconcile_completed_in_tx(
                    tx,
                    current_time_ms(),
                )?;
            }
            Ok((through, added, examined))
        });
        match result {
            Ok((through, added, examined)) => Ok(((through, added, examined), added > 0)),
            // SQLITE_INTERRUPT counts as a stop only when our own stop
            // flag is set; an interrupt nobody here asked for is a failure.
            Err(error)
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted)
                    && stop.load(Ordering::Relaxed) =>
            {
                stopped = true;
                Err("pricing queue reconciliation stopped".to_string())
            }
            Err(error) => Err(format!("reconcile pricing queue: {error}")),
        }
    });
    if stopped {
        return Ok(ChunkStep::Stopped);
    }
    let (through, added, examined) = chunk?;
    Ok(ChunkStep::Advanced {
        through,
        added,
        examined,
    })
}

/// Where one tick left a reconciliation walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkProgress {
    /// The tick's budget ran out; the walk resumes at its cursor next tick.
    Paused,
    /// Every event was checked.
    Completed,
    /// The stop flag ended the walk.
    Stopped,
}

/// A pass over the ledger in event-id chunks that can be paused between any
/// two chunks and resumed later.
///
/// The chunks are contiguous id ranges `(after, through]`, each starting where
/// the last one ended, so a pass examines every event that existed when its
/// chunk ran exactly once, whether the chunks ran in one tick or in many, the
/// same as the old whole-ledger selection. Events recorded while a pass is
/// paused may fall behind the cursor and not be seen until the next pass;
/// that is fine, because every write path enqueues its own events and this
/// walk is only the backstop for one that does not.
#[derive(Debug, Default)]
struct ReconcileWalk {
    /// Last event id checked; `None` before the first chunk.
    after: Option<String>,
    enqueued: usize,
    examined: usize,
    ticks: usize,
    spent: Duration,
}

impl ReconcileWalk {
    /// Run chunks from the cursor until the walk ends or `budget` is spent.
    /// At least one chunk runs, so a walk always progresses.
    fn advance(
        &mut self,
        state: &SharedState,
        stop: &AtomicBool,
        chunk_events: NonZeroUsize,
        budget: Duration,
    ) -> Result<WalkProgress, String> {
        let tick_started = std::time::Instant::now();
        self.ticks += 1;
        let mut slowest_chunk = Duration::ZERO;
        let progress = loop {
            if stop.load(Ordering::Relaxed) {
                break Ok(WalkProgress::Stopped);
            }
            let chunk_started = std::time::Instant::now();
            match reconcile_chunk(state, stop, self.after.as_deref(), chunk_events) {
                Err(error) => break Err(error),
                Ok(ChunkStep::Stopped) => break Ok(WalkProgress::Stopped),
                Ok(ChunkStep::Advanced {
                    through,
                    added,
                    examined,
                }) => {
                    self.enqueued += added;
                    self.examined += examined;
                    match through {
                        Some(through) => self.after = Some(through),
                        None => break Ok(WalkProgress::Completed),
                    }
                }
            }
            slowest_chunk = slowest_chunk.max(chunk_started.elapsed());
            // Start another chunk only if one as slow as the slowest seen
            // this tick would still finish inside the budget.
            if tick_started.elapsed().saturating_add(slowest_chunk) > budget {
                break Ok(WalkProgress::Paused);
            }
        };
        self.spent += tick_started.elapsed();
        progress
    }
}

/// Schedules the pricing queue check and carries a pass across loop ticks.
pub(super) struct QueueReconciler {
    walk: Option<ReconcileWalk>,
    /// When the next pass may start; loaded from the store on the first tick.
    next_due: Option<std::time::Instant>,
    chunk_events: NonZeroUsize,
    tick_budget: Duration,
}

impl QueueReconciler {
    pub(super) fn new(chunk_events: NonZeroUsize, tick_budget: Duration) -> Self {
        Self {
            walk: None,
            next_due: None,
            chunk_events,
            tick_budget,
        }
    }

    #[cfg(test)]
    fn in_progress(&self) -> bool {
        self.walk.is_some()
    }

    /// Spend at most one tick's budget on the check: resume the pass in
    /// progress, or start one if the last finished at least an interval ago.
    ///
    /// A failed chunk abandons the pass and the next one starts a full
    /// interval later from the beginning. Retrying the same chunk every tick
    /// would spend the budget on every tick against what is most likely a
    /// store fault, and a fault that clears is covered by the next pass.
    pub(super) fn tick(&mut self, state: &SharedState, stop: &AtomicBool) {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let next_due = *self.next_due.get_or_insert_with(|| {
            let completed_ms = state
                .with_conn(|conn| {
                    astrocyte_core::pricing::last_queue_reconcile_completed_ms(conn)
                        .map_err(|error| error.to_string())
                })
                .unwrap_or_else(|error| {
                    tracing::error!(target: "pricing", %error,
                        "cannot read pricing queue check completion; starting a full check");
                    None
                });
            // A future completion means the wall clock moved backwards. Treat
            // it like no completion rather than postponing the backstop.
            let delay = completed_ms
                .and_then(|completed| current_time_ms().checked_sub(completed))
                .and_then(|age| u64::try_from(age).ok())
                .map(Duration::from_millis)
                .map(|age| QUEUE_RECONCILE_INTERVAL.saturating_sub(age))
                .unwrap_or(Duration::ZERO);
            std::time::Instant::now() + delay
        });
        let mut walk = match self.walk.take() {
            Some(walk) => walk,
            None if std::time::Instant::now() >= next_due => ReconcileWalk::default(),
            None => return,
        };
        match walk.advance(state, stop, self.chunk_events, self.tick_budget) {
            Ok(WalkProgress::Paused) => {
                self.walk = Some(walk);
                return;
            }
            Ok(WalkProgress::Completed) => {
                // One line per pass at info, so the cost of the check is
                // visible in the log even when it finds nothing.
                tracing::info!(
                    target: "pricing",
                    examined = walk.examined,
                    enqueued = walk.enqueued,
                    ticks = walk.ticks,
                    spent_ms = u64::try_from(walk.spent.as_millis()).unwrap_or(u64::MAX),
                    "pricing queue reconciliation pass completed"
                );
                if walk.enqueued > 0 {
                    log_unqueued_events(walk.enqueued, true);
                }
            }
            Ok(WalkProgress::Stopped) => {
                if walk.enqueued > 0 {
                    log_unqueued_events(walk.enqueued, false);
                }
            }
            Err(error) => {
                if walk.enqueued > 0 {
                    log_unqueued_events(walk.enqueued, false);
                }
                tracing::error!(
                    target: "pricing",
                    %error,
                    after = walk.after.as_deref().unwrap_or(""),
                    examined = walk.examined,
                    ticks = walk.ticks,
                    "pricing queue reconciliation failed; the pass is abandoned and the next one starts after the interval"
                );
            }
        }
        self.next_due = Some(std::time::Instant::now() + QUEUE_RECONCILE_INTERVAL);
    }
}

/// Run `work` on `conn` while a watcher interrupts the connection's running
/// statement if `stop` is set. A stop flag checked between statements is not
/// enough on its own: one chunk statement can take a noticeable time on a
/// large ledger. The watcher keeps interrupting while the flag is set,
/// because an interrupt that lands between two statements does not carry over
/// to the next one, and it is joined before this returns, so it can never
/// interrupt whatever the connection runs afterwards.
pub(super) fn interrupt_on_stop<T>(
    conn: &rusqlite::Connection,
    stop: &AtomicBool,
    work: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let interrupt = conn.get_interrupt_handle();
    let (work_done, watcher_exit) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|scope| {
        // The watcher ends when `work_done` is dropped, which disconnects the
        // channel as soon as the work returns.
        scope.spawn(move || {
            while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                watcher_exit.recv_timeout(RECONCILE_INTERRUPT_POLL)
            {
                if stop.load(Ordering::Relaxed) {
                    interrupt.interrupt();
                }
            }
        });
        let result = work();
        drop(work_done);
        result
    })
}

/// A non-zero count means some path recorded an event without enqueuing it in
/// the same transaction. The events are queued now and will be priced, but
/// the write path is a defect and this warning is how it is noticed.
fn log_unqueued_events(enqueued: usize, complete: bool) {
    tracing::warn!(
        target: "pricing",
        enqueued,
        complete,
        "pricing queue reconciliation found events needing pricing that were never queued; a write path is missing its enqueue"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::pricing::tests::*;
    use crate::refresher::tests::*;
    use crate::state::test_state;
    use std::sync::atomic::AtomicUsize;

    /// The reconciliation re-runs the old whole-ledger selection and queues
    /// what it finds unqueued, reporting how many. An event written without
    /// its queue row -- what a write path missing its enqueue leaves -- must be
    /// found; a queued event and a fully headed one must not be counted. The
    /// chunk size is shrunk so the missed events sit in different chunks and
    /// the walk across chunk boundaries is exercised, including the last,
    /// open-ended one.
    #[test]
    fn reconciliation_enqueues_and_counts_events_that_were_never_queued() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                insert_account_event(tx, "ev-a-headed", 1_800_000_000_000, "unknown", "claude")?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        state
            .ordered_write(|tx| {
                for event_id in ["ev-b-missed", "ev-d-missed", "ev-f-missed"] {
                    insert_event_unqueued(
                        tx,
                        event_id,
                        PROVIDER,
                        ACCOUNT,
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                for event_id in ["ev-c-queued", "ev-e-queued"] {
                    insert_account_event(tx, event_id, 1_800_000_000_000, "unknown", "claude")?;
                }
                Ok(((), true))
            })
            .unwrap();
        let never_stop = AtomicBool::new(false);

        assert_eq!(
            reconcile_pricing_queue_in_chunks(&state, &never_stop, NonZeroUsize::new(2).unwrap())
                .unwrap(),
            ReconcileOutcome::Completed { enqueued: 3 }
        );
        assert_eq!(
            queued_event_ids(&state),
            vec![
                "ev-b-missed",
                "ev-c-queued",
                "ev-d-missed",
                "ev-e-queued",
                "ev-f-missed"
            ]
        );
        assert_eq!(
            reconcile_pricing_queue(&state, &never_stop).unwrap(),
            ReconcileOutcome::Completed { enqueued: 0 },
            "a queue that agrees with the ledger reports nothing"
        );
        assert_eq!(price_unpriced_events(&state, 2, &NEVER_STOP).unwrap(), 5);
        assert!(queued_event_ids(&state).is_empty());
    }

    /// A stop set before the reconciliation starts ends it before its first
    /// chunk, enqueuing nothing.
    #[test]
    fn a_stopped_reconciliation_enqueues_nothing_further() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                insert_event_unqueued(
                    tx,
                    "ev-missed",
                    PROVIDER,
                    ACCOUNT,
                    1_800_000_000_000,
                    "unknown",
                    "claude",
                )?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(
            reconcile_pricing_queue(&state, &AtomicBool::new(true)).unwrap(),
            ReconcileOutcome::Stopped { enqueued: 0 }
        );
        assert!(queued_event_ids(&state).is_empty());
    }

    fn set_queue_check_completion(state: &SharedState, completed_ms: Option<i64>) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "UPDATE pricing_selection_state SET last_queue_reconcile_completed_ms=?1 WHERE id=1",
                    [completed_ms],
                )
                .map_err(|error| error.to_string())?;
                Ok(((), false))
            })
            .unwrap();
    }

    fn queue_check_completion(state: &SharedState) -> Option<i64> {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT last_queue_reconcile_completed_ms FROM pricing_selection_state WHERE id=1",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    fn seed_queue_check_miss(state: &SharedState, event_id: &str) {
        state
            .ordered_write(|tx| {
                insert_event_unqueued(
                    tx,
                    event_id,
                    PROVIDER,
                    ACCOUNT,
                    1_800_000_000_000,
                    "unknown",
                    "claude",
                )?;
                Ok(((), true))
            })
            .unwrap();
    }

    #[test]
    fn queue_check_restart_skips_a_recent_completed_pass() {
        let state = test_state();
        let completed_ms = current_time_ms() - 23 * 60 * 60 * 1_000;
        set_queue_check_completion(&state, Some(completed_ms));
        seed_queue_check_miss(&state, "ev-missed");

        let mut restarted = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        restarted.tick(&state, &NEVER_STOP);

        assert!(queued_event_ids(&state).is_empty());
        assert!(!restarted.in_progress());
        assert_eq!(queue_check_completion(&state), Some(completed_ms));
    }

    #[test]
    fn queue_check_restart_runs_an_overdue_pass() {
        let state = test_state();
        let started_ms = current_time_ms();
        set_queue_check_completion(&state, Some(started_ms - 25 * 60 * 60 * 1_000));
        seed_queue_check_miss(&state, "ev-missed");

        let mut restarted = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        restarted.tick(&state, &NEVER_STOP);

        assert_eq!(queued_event_ids(&state), vec!["ev-missed"]);
        assert!(!restarted.in_progress());
        assert!(queue_check_completion(&state).unwrap() >= started_ms);
    }

    #[test]
    fn queue_check_restart_runs_without_a_completed_pass() {
        let state = test_state();
        let started_ms = current_time_ms();
        assert_eq!(queue_check_completion(&state), None);
        seed_queue_check_miss(&state, "ev-missed");

        let mut first = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        first.tick(&state, &NEVER_STOP);

        assert_eq!(queued_event_ids(&state), vec!["ev-missed"]);
        assert!(!first.in_progress());
        let completed_ms = queue_check_completion(&state).unwrap();
        assert!(completed_ms >= started_ms);

        // A new scheduler must use the committed completion, not its constructor's time.
        seed_queue_check_miss(&state, "ev-next-missed");
        let mut restarted = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        restarted.tick(&state, &NEVER_STOP);
        assert_eq!(queued_event_ids(&state), vec!["ev-missed"]);
        assert_eq!(queue_check_completion(&state), Some(completed_ms));
    }

    #[test]
    fn queue_check_restart_restarts_an_interrupted_pass() {
        let state = test_state();
        let started_ms = current_time_ms();
        let old_completion = started_ms - 25 * 60 * 60 * 1_000;
        set_queue_check_completion(&state, Some(old_completion));
        for event_id in ["ev-10-missed", "ev-20-missed"] {
            seed_queue_check_miss(&state, event_id);
        }
        let mut interrupted = QueueReconciler::new(NonZeroUsize::new(1).unwrap(), Duration::ZERO);
        interrupted.tick(&state, &NEVER_STOP);
        assert!(interrupted.in_progress());
        assert_eq!(queued_event_ids(&state), vec!["ev-10-missed"]);
        assert_eq!(queue_check_completion(&state), Some(old_completion));
        drop(interrupted);

        // A miss behind the discarded cursor proves that a restart walks from the start.
        seed_queue_check_miss(&state, "ev-00-missed");
        let mut restarted = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        restarted.tick(&state, &NEVER_STOP);

        assert_eq!(
            queued_event_ids(&state),
            vec!["ev-00-missed", "ev-10-missed", "ev-20-missed"]
        );
        assert!(!restarted.in_progress());
        assert!(queue_check_completion(&state).unwrap() >= started_ms);
    }

    #[test]
    fn queue_check_restart_runs_when_completion_is_in_the_future() {
        let state = test_state();
        let started_ms = current_time_ms();
        let future_ms = started_ms + 60 * 60 * 1_000;
        set_queue_check_completion(&state, Some(future_ms));
        seed_queue_check_miss(&state, "ev-missed");

        let mut restarted = QueueReconciler::new(QUEUE_RECONCILE_CHUNK, Duration::MAX);
        restarted.tick(&state, &NEVER_STOP);

        assert_eq!(queued_event_ids(&state), vec!["ev-missed"]);
        assert!(!restarted.in_progress());
        let completed_ms = queue_check_completion(&state).unwrap();
        assert!(completed_ms >= started_ms);
        assert!(completed_ms < future_ms);
    }

    /// The queue check must not hold up ingest: with a budget that fits one
    /// chunk, a tick returns with the pass unfinished, and every tick of the
    /// pass drains the producer. The event recorded without its queue row is
    /// last in id order, so it is found only by the pass's final chunks,
    /// several ticks in.
    #[test]
    fn queue_check_spreads_over_ticks_and_every_tick_drains() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                for index in 0..9 {
                    insert_account_event(
                        tx,
                        &format!("ev-{index:02}-queued"),
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                insert_event_unqueued(
                    tx,
                    "ev-99-missed",
                    PROVIDER,
                    ACCOUNT,
                    1_800_000_000_000,
                    "unknown",
                    "claude",
                )?;
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
        // A zero budget fits exactly the one chunk every tick runs.
        let mut reconciler = QueueReconciler::new(NonZeroUsize::new(2).unwrap(), Duration::ZERO);
        let file_sources = FileSources::default();
        let is_queued =
            |state: &SharedState| queued_event_ids(state).contains(&"ev-99-missed".to_string());

        // Checked after every tick, because the tick after the one that
        // enqueues the event prices it and takes it off the queue.
        let mut queued_at_tick = None;
        let mut ticks = 0;
        loop {
            let pulls_before = pulls.load(Ordering::SeqCst);
            run_tick(
                &state,
                &producers,
                &file_sources,
                None,
                &mut None,
                &mut MultiplierStage::new(None),
                &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
                &mut reconciler,
                &NEVER_STOP,
            );
            ticks += 1;
            assert!(
                pulls.load(Ordering::SeqCst) > pulls_before,
                "tick {ticks} did not drain the producer"
            );
            if ticks == 1 {
                assert!(
                    reconciler.in_progress(),
                    "the first tick must leave the pass unfinished"
                );
                assert!(
                    !is_queued(&state),
                    "the first chunk cannot reach the last event"
                );
            }
            if queued_at_tick.is_none() && is_queued(&state) {
                queued_at_tick = Some(ticks);
            }
            if !reconciler.in_progress() {
                break;
            }
            assert!(ticks < 50, "the pass never finished");
        }
        // Ten events in chunks of two: five bounded chunks and the empty
        // open-ended one, one per tick.
        assert_eq!(ticks, 6);
        // Its chunk, (ev-08..ev-99], is the fifth.
        assert_eq!(
            queued_at_tick,
            Some(5),
            "the unqueued event reaches pricing_pending on the tick whose chunk covers it"
        );
    }

    /// A pass paused and resumed at every chunk boundary examines each event
    /// exactly once: none skipped where a chunk ends in one tick and the next
    /// starts in another, none examined twice. The missed events sit on both
    /// sides of every boundary, and the last chunk is the open-ended one.
    #[test]
    fn resumed_queue_check_examines_each_event_once_per_pass() {
        let state = test_state();
        state
            .ordered_write(|tx| {
                insert_account_event(tx, "ev-01-headed", 1_800_000_000_000, "unknown", "claude")?;
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(price_unpriced_events(&state, 1, &NEVER_STOP).unwrap(), 1);
        state
            .ordered_write(|tx| {
                for event_id in [
                    "ev-03-missed",
                    "ev-04-missed",
                    "ev-06-missed",
                    "ev-07-missed",
                ] {
                    insert_event_unqueued(
                        tx,
                        event_id,
                        PROVIDER,
                        ACCOUNT,
                        1_800_000_000_000,
                        "unknown",
                        "claude",
                    )?;
                }
                for event_id in ["ev-02-queued", "ev-05-queued", "ev-08-queued"] {
                    insert_account_event(tx, event_id, 1_800_000_000_000, "unknown", "claude")?;
                }
                Ok(((), true))
            })
            .unwrap();

        // Chunks of three over eight events: (..03], (03..06], (06..], one
        // per call, as three ticks would run them.
        let mut walk = ReconcileWalk::default();
        let mut progress = Vec::new();
        for _ in 0..10 {
            let step = walk
                .advance(
                    &state,
                    &NEVER_STOP,
                    NonZeroUsize::new(3).unwrap(),
                    Duration::ZERO,
                )
                .unwrap();
            progress.push((step, walk.after.clone()));
            if step == WalkProgress::Completed {
                break;
            }
        }
        assert_eq!(
            progress,
            vec![
                (WalkProgress::Paused, Some("ev-03-missed".to_string())),
                (WalkProgress::Paused, Some("ev-06-missed".to_string())),
                (WalkProgress::Completed, Some("ev-06-missed".to_string())),
            ]
        );
        assert_eq!(walk.examined, 8, "every event examined once");
        assert_eq!(walk.enqueued, 4, "every missed event found once");
        assert_eq!(walk.ticks, 3);
        assert_eq!(
            queued_event_ids(&state),
            vec![
                "ev-02-queued",
                "ev-03-missed",
                "ev-04-missed",
                "ev-05-queued",
                "ev-06-missed",
                "ev-07-missed",
                "ev-08-queued"
            ]
        );
    }
}
