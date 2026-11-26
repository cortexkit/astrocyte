use rusqlite::{params, Transaction};

use crate::{fact, ingest::IngestError};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeStamp {
    pub agent_id: Option<String>,
    pub agent_run_id: Option<String>,
    pub flow_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    Segment,
    ModelAttempt,
    SystemOne,
}

impl CallKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Segment => "segment",
            Self::ModelAttempt => "model_attempt",
            Self::SystemOne => "system_one",
        }
    }
}

/// Attach evidence only to calls that actually made it into the ledger. An
/// attempt can own several calls or none; its detail rows name the recorded
/// shares without re-running the billing decision.
pub fn record_scope_in_tx(
    tx: &Transaction<'_>,
    fact_id: &str,
    kind: CallKind,
    stamp: &ScopeStamp,
) -> Result<usize, IngestError> {
    if stamp.agent_id.is_none() && stamp.agent_run_id.is_none() && stamp.flow_id.is_none() {
        return Ok(0);
    }
    let (calls, identity) = match kind {
        CallKind::ModelAttempt => (
            "SELECT event_key FROM broca_attempt_detail WHERE fact_id=?1",
            fact_id.into(),
        ),
        CallKind::Segment | CallKind::SystemOne => (
            "SELECT event_key FROM usage_event WHERE event_id=?1",
            fact::event_id("broca", None, fact_id)?,
        ),
    };
    // A replay must not replace the first stored stamp or dirty the hour again.
    Ok(tx
        .prepare_cached(&format!(
            "INSERT INTO broca_scope_stamp(event_key,agent_id,agent_run_id,flow_id,call_kind)
             SELECT call.event_key,?2,?3,?4,?5 FROM ({calls}) call
             WHERE NOT EXISTS(SELECT 1 FROM broca_scope_stamp s WHERE s.event_key=call.event_key)"
        ))?
        .execute(params![
            identity,
            stamp.agent_id,
            stamp.agent_run_id,
            stamp.flow_id,
            kind.as_str(),
        ])?)
}
