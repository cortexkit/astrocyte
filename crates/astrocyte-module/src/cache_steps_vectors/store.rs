//! The store the published `usage.cache.steps` vectors are served from.
//!
//! Every row here is invented: the session ids, model names and token counts
//! name nothing real. Harness steps are written through event ingest, so a
//! reply is built from the tables a live store holds. BROCA's diagnostic steps
//! are written straight into `broca_step`, a table that refuses updates; its
//! reader inserts them the same way. The store's own insert triggers give each
//! step its `cache_step_sequence` row, the step's cache-step cursor.

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::{record_event_in_tx, DeliveryPath, EventRecordRequest, StepDetail};

use crate::state::SharedState;

/// A harness producer whose events are one model step each.
pub(super) const PI: &str = "pi";
/// The producer of BROCA's diagnostic steps.
pub(super) const BROCA: &str = "broca";
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
    /// reply sums back into one `cache_write`.
    pub cache_write: Option<u64>,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
    pub provider: Option<&'a str>,
    pub model: Option<&'a str>,
    /// Stored as `step_detail.finish`.
    pub finish: Option<&'a str>,
    /// The event's stop reason, read only when `step_detail.finish` is absent.
    pub terminal_status: Option<&'a str>,
    pub context_limit: Option<i64>,
}

/// Write harness steps, each as one ledger event with its measurements.
pub(super) fn record(steps: &[Step<'_>], state: &SharedState) {
    let token = |role: &str, unit: &str, units: u64| astrocyte_core::ingest::MeasurementInput {
        role: role.into(),
        unit: unit.into(),
        kind: "billed".into(),
        source: format!("usage.{role}"),
        quantity_units: Some(i64::try_from(units).unwrap()),
        quantity_exponent: Some(0),
    };
    let request = |step: &Step<'_>| {
        let mut measurements = Vec::new();
        if let Some(input) = step.input {
            measurements.push(token("input", "token", input));
        }
        if let Some(read) = step.cache_read {
            measurements.push(token("cache_read", "token", read));
        }
        if let Some(write) = step.cache_write {
            let first = write * 2 / 3;
            measurements.push(token("cache_write", "token_cache_5m", first));
            measurements.push(token("cache_write", "token_cache_1h", write - first));
        }
        if let Some(output) = step.output {
            measurements.push(token("output", "token", output));
        }
        if let Some(reasoning) = step.reasoning {
            measurements.push(token("reasoning", "token", reasoning));
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

/// One BROCA diagnostic step, holding the counts exactly as BROCA sent them:
/// its `output_tokens` already include the reasoning tokens.
pub(super) struct BrocaStep<'a> {
    pub step_id: i64,
    pub finished_at_ms: i64,
    pub input: Option<i64>,
    pub cached_input: Option<i64>,
    pub cache_write: Option<i64>,
    pub output: Option<i64>,
    pub reasoning: Option<i64>,
    pub finish: Option<&'a str>,
    pub context_limit: Option<i64>,
    pub request_body_len: Option<i64>,
}

/// Write BROCA's steps of `session`.
pub(super) fn record_broca(state: &SharedState, session: &str, steps: &[BrocaStep<'_>]) {
    state
        .with_conn(|conn| {
            for step in steps {
                conn.execute(
                    "INSERT INTO broca_step(run_id,step_id,fact_id,project_root,harness,session,
                         provider,model,input_tokens,cached_input_tokens,cache_write_tokens,
                         output_tokens,reasoning_tokens,finish_reason,request_body_len,
                         finished_at_ms,context_limit,recorded_at_ms)
                     VALUES ('run-phone-diag',?1,?2,'/work/diag-fixture','broca',?3,
                         'anthropic','claude-sonnet-4',?4,?5,?6,?7,?8,?9,?10,?11,?12,?11)",
                    rusqlite::params![
                        step.step_id,
                        format!("fact-phone-diag-{}", step.step_id),
                        session,
                        step.input,
                        step.cached_input,
                        step.cache_write,
                        step.output,
                        step.reasoning,
                        step.finish,
                        step.request_body_len,
                        step.finished_at_ms,
                        step.context_limit,
                    ],
                )
                .map_err(|error| error.to_string())?;
            }
            Ok(())
        })
        .unwrap();
}
