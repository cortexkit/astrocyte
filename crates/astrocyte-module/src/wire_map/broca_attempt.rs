use astrocyte_core::broca::{Attempt, AttemptCall, AttemptDetail};
use astrocyte_core::catalog_client::stated_mode_condition;

use super::*;

pub(crate) const MODEL_ATTEMPT_KIND: &str = "model_attempt";

#[derive(Deserialize)]
pub(crate) struct WireModelAttempt {
    pub fact_id: String,
    run_id: String,
    session: WireBindIdentity,
    step_id: u64,
    attempt: u64,
    attempt_id: String,
    outcome: String,
    http_status: Option<u16>,
    provider: String,
    model: String,
    usage_by_model: Option<Vec<WireAttemptShare>>,
    reasoning_included_in_output: bool,
    requested_service_tier: String,
    served_speed: Option<String>,
    content_received: bool,
    response_bytes: u64,
    auth_selection: String,
    resolved_credential_id: Option<String>,
    account_ref: Option<String>,
    served_account_ref: Option<String>,
    charge_basis: ChargeBasis,
    charge_basis_mapping_version: Option<u32>,
    charge_basis_overrides: Option<String>,
    sent_at_ms: u64,
    recorded_at_ms: u64,
}

#[derive(Deserialize)]
struct WireAttemptShare {
    provider: String,
    model: String,
    usage: WireUsage,
    served_service_tier: Option<String>,
    segment_fact_id: String,
}

fn stored_integer(value: u64, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds stored integer range"))
}

pub(crate) fn map_attempt(wire: WireModelAttempt) -> Result<Attempt, String> {
    let sent_at = stored_integer(wire.sent_at_ms, "sent_at_ms")?;
    let call = |index: usize, share: Option<&WireAttemptShare>| {
        let (provider, model) = share.map_or((&wire.provider, &wire.model), |share| {
            (&share.provider, &share.model)
        });
        let measurements = if let Some(share) = share {
            usage_measurements(&share.usage, wire.reasoning_included_in_output)?
        } else {
            Vec::new()
        };
        let served_service_tier = share.and_then(|share| share.served_service_tier.clone());
        let mode = wire
            .served_speed
            .clone()
            .or_else(|| stated_mode_condition(served_service_tier.as_deref()).map(str::to_owned));
        Ok(AttemptCall {
            event: EventRecordRequest {
                producer: BROCA_PRODUCER.into(),
                producer_lineage: None,
                // The same model can occur more than once in one provider request.
                producer_ref: format!("model_attempt:{}:{index}", wire.fact_id),
                prior_event_ref: None,
                harness: Some(wire.session.harness.clone()),
                project_root: wire.session.project_root.clone(),
                session: Some(wire.session.session.clone()),
                run_ref: Some(wire.run_id.clone()),
                flow_ref: None,
                runner: Some(BROCA_PRODUCER.into()),
                work_class: None,
                occurred_at_ms: Some(sent_at),
                emitted_at_ms: None,
                terminal_status: None,
                attempt_ordinal: None,
                vendor_request_id: None,
                provider: Some(provider.clone()),
                model: Some(model.clone()),
                account_ref: wire.account_ref.clone(),
                auth_selection: Some(wire.auth_selection.clone()),
                resolved_credential: wire.resolved_credential_id.clone(),
                charge_basis: wire.charge_basis,
                charge_basis_mapping_version: wire.charge_basis_mapping_version,
                charge_basis_overrides: wire.charge_basis_overrides.clone(),
                measurements,
                step_detail: Default::default(),
            },
            served_service_tier,
            mode,
            segment_fact_id: share.map(|share| share.segment_fact_id.clone()),
        })
    };
    let calls = match &wire.usage_by_model {
        None => vec![call(0, None)?],
        Some(shares) if shares.is_empty() => return Err("empty usage_by_model".into()),
        Some(shares) => shares
            .iter()
            .enumerate()
            .map(|(index, share)| call(index, Some(share)))
            .collect::<Result<Vec<_>, String>>()?,
    };
    Ok(Attempt {
        fact_id: wire.fact_id,
        detail: AttemptDetail {
            attempt_id: wire.attempt_id,
            step_id: stored_integer(wire.step_id, "step_id")?,
            attempt: stored_integer(wire.attempt, "attempt")?,
            outcome: wire.outcome,
            http_status: wire.http_status,
            content_received: wire.content_received,
            response_bytes: stored_integer(wire.response_bytes, "response_bytes")?,
            recorded_at_ms: stored_integer(wire.recorded_at_ms, "recorded_at_ms")?,
            requested_service_tier: wire.requested_service_tier,
            served_speed: wire.served_speed,
            served_account_ref: wire.served_account_ref,
        },
        calls,
    })
}
