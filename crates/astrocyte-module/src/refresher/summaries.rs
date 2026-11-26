//! Summary and valuation maintenance have independent budgets, separate from tick scheduling.

use super::*;

/// Valuation owns a separate budget, so a stale list value never delays the
/// summary maintenance that serves existing recorded-spend reports.
pub(super) fn valuation_tick(
    state: &SharedState,
    valuations: &mut astrocyte_core::report::valuation::ValuationMaintenance,
    stop: &AtomicBool,
    budget: Duration,
) {
    use astrocyte_core::report::valuation::ValuationMaintenance;
    if stop.load(Ordering::Relaxed) {
        return;
    }
    let needed = state.with_read_op(|conn| {
        ValuationMaintenance::needs_prepare(conn).map_err(|error| {
            (
                "internal".into(),
                format!("valuation prepare check: {error:?}"),
            )
        })
    });
    let needed = match needed {
        Ok(needed) => needed,
        Err((_, error)) => {
            tracing::warn!(%error,"valuation preparation check failed");
            return;
        }
    };
    let result = if needed {
        state.ordered_write(|tx| {
            ValuationMaintenance::prepare(tx)
                .map_err(|error| format!("valuation prepare: {error:?}"))?;
            Ok(((), false))
        })
    } else {
        Ok(())
    };
    if let Err(error) = result {
        tracing::warn!(%error, "valuation preparation failed");
        return;
    }
    let started = std::time::Instant::now();
    while !stop.load(Ordering::Relaxed) && started.elapsed() < budget {
        let result = state.with_read_op(|conn| {
            interrupt_on_stop(conn, stop, || Ok(valuations.read_chunk(conn)))
                .map_err(|error| ("internal".into(), error.to_string()))?
                .map_err(|error| ("internal".into(), format!("valuation read: {error:?}")))
        });
        let ready = match result {
            Ok(ready) => ready,
            Err((_, error)) => {
                tracing::warn!(%error, "valuation chunk failed");
                return;
            }
        };
        if ready && !stop.load(Ordering::Relaxed) {
            if let Err(error) = state.ordered_write(|tx| {
                valuations
                    .publish(tx)
                    .map(|published| (published, false))
                    .map_err(|error| format!("valuation publish: {error:?}"))
            }) {
                tracing::warn!(%error, "valuation publication failed");
                return;
            }
        }
        if !valuations.has_pending() && !ready {
            break;
        }
    }
}

/// Fresh summary work follows ingestion and pricing, with short snapshot chunks
/// and a separate revision-checked writer transaction for publication.
///
/// Returns true when the budget ran out while hours still wait to be rebuilt,
/// so the caller may give the rebuild more time before the next full tick.
pub(super) fn summary_tick(
    state: &SharedState,
    summaries: &mut astrocyte_core::report::SummaryMaintenance,
    stop: &AtomicBool,
) -> bool {
    summary_tick_within(state, summaries, stop, Duration::from_millis(1_500))
}

pub(super) fn summary_tick_within(
    state: &SharedState,
    summaries: &mut astrocyte_core::report::SummaryMaintenance,
    stop: &AtomicBool,
    budget: Duration,
) -> bool {
    let started = std::time::Instant::now();
    let mut slowest = Duration::ZERO;
    loop {
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        let chunk = std::time::Instant::now();
        let now = current_time_ms();
        let ready = state.with_read_op(|conn| {
            interrupt_on_stop(conn, stop, || Ok(summaries.read_chunk(conn, now)))
                .map_err(|error| ("internal".into(), error.to_string()))?
                .map_err(|error| ("internal".into(), format!("summary read: {error:?}")))
        });
        let ready = match ready {
            Ok(ready) => ready,
            Err((_, error)) => {
                if !stop.load(Ordering::Relaxed) {
                    tracing::warn!(target: "summary", %error, "summary chunk failed; retry after one hour");
                    summaries.defer_after_error(now);
                }
                return false;
            }
        };
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        if ready {
            let result = state.ordered_write(|tx| {
                let outcome = interrupt_on_stop(tx, stop, || Ok(summaries.publish(tx, now)))
                    .map_err(|error| error.to_string())?
                    .map_err(|error| format!("summary publish: {error:?}"))?;
                // Derived rows do not change the cached config/capacity projection.
                Ok((outcome, false))
            });
            match result {
                Ok(outcome) => {
                    if outcome.published && !outcome.attribution_only {
                        state.runs_summary_committed();
                    }
                    if outcome.deferred {
                        tracing::warn!(target: "summary", hour=?outcome.hour,
                            "hour exceeds bounded summary publication; retaining live aggregation");
                    }
                    if outcome.mismatch {
                        tracing::warn!(target: "summary", hour=?outcome.hour,
                            examined=outcome.examined, "clean hourly summary differs from base facts; requeued");
                    }
                    if outcome.audit_completed {
                        tracing::info!(target: "summary", examined=outcome.examined,
                            hours=outcome.hours, mismatches=outcome.mismatches,
                            chunks=outcome.chunks, reduce_compare_ms=outcome.spent_ms,
                            max_step_ms=outcome.max_chunk_ms, elapsed_ms=outcome.audit_elapsed_ms,
                            "hourly summary full-ledger audit completed");
                    }
                }
                Err(error) => {
                    if !stop.load(Ordering::Relaxed) {
                        tracing::warn!(target: "summary", %error, "summary publication failed; retry after one hour");
                        summaries.defer_after_error(now);
                    }
                    return false;
                }
            }
        } else if !summaries.has_pending() {
            return false;
        }
        slowest = slowest.max(chunk.elapsed());
        if started.elapsed().saturating_add(slowest) > budget {
            return summaries.rebuild_backlog();
        }
    }
}
