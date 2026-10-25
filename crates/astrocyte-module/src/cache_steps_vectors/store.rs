//! The store the published `usage.cache.steps` vectors are served from.
//!
//! Every row here is invented: the session ids, model names and token counts
//! name nothing real. Each step is written by the same ingest call its
//! harness reader makes, with the measurements that reader records: OMP's
//! `usage.input`, `usage.cacheRead`, `usage.cacheWrite`, `usage.output-reasoningTokens`
//! and `usage.reasoningTokens`, and Codex's derived input and cache roles. Its
//! `terminal_status` and `step_detail` are the fields those readers set. So a
//! reply is built from the ledger a live store holds, not from a shape only
//! this test knows.

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::{record_event_in_tx, DeliveryPath, EventRecordRequest, StepDetail};

use crate::state::SharedState;

/// Oh My Pi: a harness whose reader keeps the stop reason and the reasoning
/// count apart from the visible output.
pub(super) const OMP: &str = "omp";
/// Codex: the harness whose reader keeps the model's context window, and
/// whose implicit prefix cache reports no cache writes.
pub(super) const CODEX: &str = "codex";
/// 2026-09-01T00:00:00Z.
pub(super) const DAY_MS: i64 = 1_788_220_800_000;

/// One harness step to store.
pub(super) struct Step<'a> {
    pub harness: &'a str,
    pub session: &'a str,
    pub step_ref: &'a str,
    /// `None` stores a step without a time, which a reply only counts.
    pub at: Option<i64>,
    pub input: Option<u64>,
    pub cache_read: Option<u64>,
    /// Stored as two measurements, the 5-minute and 1-hour lifetimes that a
    /// reply sums back into one `cache_write`. Codex's implicit prefix cache
    /// reports none, so its steps leave this `None`.
    pub cache_write: Option<u64>,
    /// The visible output, stored apart from `reasoning`; a reply serves the
    /// two added together.
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
    pub provider: Option<&'a str>,
    pub model: Option<&'a str>,
    /// Stored as `step_detail.finish`.
    pub finish: Option<&'a str>,
    /// The harness's stop reason, read only when `step_detail.finish` is
    /// absent, which is how OMP and Claude Code store theirs.
    pub terminal_status: Option<&'a str>,
    pub context_limit: Option<i64>,
}

/// Write harness steps, each as one ledger event with its measurements.
///
/// The steps are written in one transaction. The ledger treats a second event
/// with the same producer, lineage and step ref as a redelivery and records it
/// only if its payload restates the first, so a fixture session must not share
/// a step ref with another session's.
pub(super) fn record(steps: &[Step<'_>], state: &SharedState) {
    let token = |role: &str, unit: &str, source: &str, units: u64| {
        astrocyte_core::ingest::MeasurementInput {
            role: role.into(),
            unit: unit.into(),
            kind: "billed".into(),
            source: source.into(),
            quantity_units: Some(i64::try_from(units).unwrap()),
            quantity_exponent: Some(0),
        }
    };
    let request = |step: &Step<'_>| {
        // Codex's implicit prefix cache reports no write; a Codex step with one
        // would be a fixture the reader can never produce.
        assert!(
            step.harness != CODEX || step.cache_write.is_none(),
            "Codex reports no cache write"
        );
        let mut measurements = Vec::new();
        if let Some(input) = step.input {
            // Both readers store their uncached input as `usage.input`, whether
            // they report it or derive it from the thread's growth.
            measurements.push(token("input", "token", "usage.input", input));
        }
        if let Some(read) = step.cache_read {
            measurements.push(if step.harness == CODEX {
                token("cache_read", "token", "usage.cached_input_tokens", read)
            } else {
                token("cache_read", "token", "usage.cacheRead", read)
            });
        }
        if let Some(write) = step.cache_write {
            let first = write * 2 / 3;
            measurements.push(token("cache_write", "token", "usage.cacheWrite", first));
            measurements.push(token(
                "cache_write",
                "token_cache_1h",
                "usage.cacheWrite",
                write - first,
            ));
        }
        if let Some(output) = step.output {
            measurements.push(token(
                "output",
                "token",
                "usage.output-reasoningTokens",
                output,
            ));
        }
        if let Some(reasoning) = step.reasoning {
            measurements.push(token(
                "reasoning",
                "token",
                "usage.reasoningTokens",
                reasoning,
            ));
        }
        EventRecordRequest {
            producer: step.harness.into(),
            producer_lineage: None,
            producer_ref: step.step_ref.into(),
            prior_event_ref: None,
            harness: None,
            project_root: None,
            session: Some(step.session.into()),
            run_ref: None,
            flow_ref: None,
            work_class: None,
            occurred_at_ms: step.at,
            emitted_at_ms: None,
            terminal_status: step.terminal_status.map(str::to_string),
            attempt_ordinal: None,
            vendor_request_id: None,
            provider: step.provider.map(str::to_string),
            model: step.model.map(str::to_string),
            account_ref: None,
            auth_selection: None,
            charge_basis: ChargeBasis::Unknown,
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements,
            step_detail: StepDetail {
                finish: step.finish.map(str::to_string),
                context_limit: step.context_limit,
            },
        }
    };
    let mut seen = std::collections::HashSet::new();
    for step in steps {
        let (harness, step_ref) = (step.harness, step.step_ref);
        assert!(
            seen.insert((harness, step_ref)),
            "two fixtures share the step ref {step_ref}; the ledger would take the second for a redelivery"
        );
    }
    state
        .ordered_write(|tx| {
            for step in steps {
                record_event_in_tx(tx, &request(step), 1, DeliveryPath::Read)
                    .map_err(|error| format!("{error:?}"))?;
            }
            Ok(((), true))
        })
        .unwrap();
}
