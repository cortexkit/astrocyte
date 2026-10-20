//! Transactional usage-event recording and chargeability settlement.
//!
//! Producer payloads are immutable. Distinct later records may settle a
//! chargeability question only through an explicit producer reference, while a
//! conflicting redelivery is retained as quarantine evidence instead of
//! replacing the event already stored.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::canon::{canonical_hash, jcs_serialize, CanonError};
use crate::fact::{event_id, ChargeBasis};

#[derive(Debug)]
pub enum IngestError {
    Store(rusqlite::Error),
    Canon(CanonError),
    Refused(Refusal),
}

pub(crate) fn stored_value_error(column: usize, field: &str, value: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unrecognized stored {field}: {value:?}"),
        )),
    )
}

impl From<rusqlite::Error> for IngestError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

impl From<CanonError> for IngestError {
    fn from(error: CanonError) -> Self {
        Self::Canon(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Refusal {
    Malformed,
    NotFound,
    AlreadySettled,
    OperatorCannotBill,
    UnpricedTarget,
    CurrencyMismatch,
    UndatedTarget,
    WriteOffRequiresPricing,
    CorrectionIdReused,
}

impl Refusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::NotFound => "not_found",
            Self::AlreadySettled => "already_settled",
            Self::OperatorCannotBill => "operator_cannot_bill",
            Self::UnpricedTarget => "unpriced_target",
            Self::CurrencyMismatch => "currency_mismatch",
            Self::UndatedTarget => "undated_target",
            Self::WriteOffRequiresPricing => "write_off_requires_pricing",
            Self::CorrectionIdReused => "correction_id_reused",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementInput {
    pub role: String,
    pub unit: String,
    pub kind: String,
    pub source: String,
    pub quantity_units: Option<i64>,
    pub quantity_exponent: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRecordRequest {
    pub producer: String,
    /// `None` when the producer has no restarting reference space to
    /// disambiguate for this event — see `fact::UsageEventIdentity` for the two
    /// producer shapes that share this arm. Absence is representable and is not the empty string: the two
    /// hash to different key shapes and can never share an identity.
    #[serde(default)]
    pub producer_lineage: Option<String>,
    pub producer_ref: String,
    #[serde(default)]
    pub prior_event_ref: Option<String>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub project_root: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub run_ref: Option<String>,
    #[serde(default)]
    pub work_class: Option<String>,
    #[serde(default)]
    pub occurred_at_ms: Option<i64>,
    #[serde(default)]
    pub emitted_at_ms: Option<i64>,
    #[serde(default)]
    pub terminal_status: Option<String>,
    #[serde(default)]
    pub attempt_ordinal: Option<u32>,
    #[serde(default)]
    pub vendor_request_id: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub account_ref: Option<String>,
    /// The producer's credential id, carried verbatim and never derived. BROCA
    /// stamps this on every export segment (the pin the run was admitted under,
    /// frozen at admission); for api-key credentials account_ref is absent by
    /// construction, so this is the only per-account discriminator a PAYG fact
    /// carries. We store the string the producer sent and infer nothing from its
    /// shape.
    #[serde(default)]
    pub auth_selection: Option<String>,
    pub charge_basis: ChargeBasis,
    /// BROCA's charge-basis provenance, carried verbatim from its usage
    /// export: the shipped charge-basis table version, and which local
    /// override rows applied (`None` = the producer did not say, `"none"` =
    /// none applied, otherwise a digest of the rows). `Option` on purpose: an
    /// absent overrides value must stay distinguishable from `"none"`.
    ///
    /// Stored NULLs on BROCA facts recorded before schema 13 mean "not
    /// recorded", not "the producer did not say"; see the note on migration 13
    /// in store.rs for how a row tells the two apart.
    ///
    /// Only the BROCA export mapper sets these. They are skipped by serde, so
    /// a pushed `spend.event.ingest` payload naming them is refused as an
    /// unknown field rather than letting a pusher claim BROCA's table decided
    /// its charge basis.
    #[serde(skip)]
    pub charge_basis_mapping_version: Option<u32>,
    #[serde(skip)]
    pub charge_basis_overrides: Option<String>,
    #[serde(default)]
    pub measurements: Vec<MeasurementInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordDisposition {
    Recorded,
    ExactRedelivery,
    QuarantinedRedelivery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventRecordOutcome {
    pub event_id: String,
    pub disposition: RecordDisposition,
    pub transitions_written: usize,
    pub quarantine_evidence_written: usize,
    pub waiting_for_target: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChargeabilityState {
    Billed,
    WrittenOff,
    Unbillable,
}

impl ChargeabilityState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Billed => "billed",
            Self::WrittenOff => "written_off",
            Self::Unbillable => "unbillable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSettleRequest {
    pub event_id: String,
    pub to_state: ChargeabilityState,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventSettleOutcome {
    pub event_id: String,
    pub to_state: ChargeabilityState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjustmentReason {
    WriteOff,
    VendorRestatement,
    OurRepricing,
    Refund,
    Goodwill,
}

impl AdjustmentReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::WriteOff => "write_off",
            Self::VendorRestatement => "vendor_restatement",
            Self::OurRepricing => "our_repricing",
            Self::Refund => "refund",
            Self::Goodwill => "goodwill",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjustmentOrigin {
    Statement,
    CorrectionIngest,
}

impl AdjustmentOrigin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Statement => "statement",
            Self::CorrectionIngest => "correction_ingest",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjustmentWriteContext {
    origin: AdjustmentOrigin,
    operator: Option<String>,
    correction_id: Option<String>,
}

impl AdjustmentWriteContext {
    pub fn statement() -> Self {
        Self {
            origin: AdjustmentOrigin::Statement,
            operator: None,
            correction_id: None,
        }
    }

    pub fn correction_ingest(correction_id: String) -> Self {
        Self {
            origin: AdjustmentOrigin::CorrectionIngest,
            operator: None,
            correction_id: Some(correction_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdjustmentRecordRequest {
    pub event_id: String,
    pub currency: String,
    pub amount_nanos: i64,
    pub reason: AdjustmentReason,
    pub occurred_at_ms: i64,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdjustmentRecordOutcome {
    pub adjustment_id: String,
    pub origin: AdjustmentOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasisCorrectRequest {
    pub event_id: String,
    pub corrected_basis: ChargeBasis,
    pub reason: String,
}

#[derive(Debug)]
struct EventPayload {
    prior_event_ref: Option<String>,
    harness: Option<String>,
    project_root: Option<String>,
    session: Option<String>,
    run_ref: Option<String>,
    work_class: Option<String>,
    occurred_at_ms: Option<i64>,
    emitted_at_ms: Option<i64>,
    terminal_status: Option<String>,
    attempt_ordinal: Option<u32>,
    vendor_request_id: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    account_ref: Option<String>,
    auth_selection: Option<String>,
    charge_basis: ChargeBasis,
    charge_basis_mapping_version: Option<u32>,
    charge_basis_overrides: Option<String>,
    measurements: Vec<MeasurementInput>,
}

impl EventPayload {
    /// Whether an incoming payload restates this stored one.
    ///
    /// Every field must be equal, except that a stored NULL `auth_selection`
    /// or charge-basis provenance field accepts any incoming value. Facts
    /// ingested before astrocyte decoded those fields hold NULL although BROCA
    /// sent a value: `auth_selection` only became a column in migration 4
    /// (2026-09-11), and every BROCA event recorded before then stores NULL
    /// there. BROCA's facts are re-read from the start whenever the cursor is
    /// lost (a rebuilt store, or BROCA excluded and re-enabled), so a stored
    /// NULL there means "not recorded", not "the producer sent nothing".
    /// BROCA's stored segments are immutable, so a fact recorded with NULL
    /// after the column existed reads back NULL again and still matches. The
    /// stored row is never updated to the incoming value: the ledger is
    /// append-only. A stored value against a different incoming value
    /// (including an absent one) is still a contradiction.
    ///
    /// One more exception: a BROCA output stored with reasoning included is
    /// restated by the same output re-sent with reasoning subtracted, which is
    /// how the current wire mapping represents it. See
    /// [`restates_inclusive_output`] for the exact conditions.
    fn restated_by(&self, incoming: &EventRecordRequest) -> bool {
        if self.prior_event_ref != incoming.prior_event_ref
            || self.harness != incoming.harness
            || self.project_root != incoming.project_root
            || self.session != incoming.session
            || self.run_ref != incoming.run_ref
            || self.work_class != incoming.work_class
            || self.occurred_at_ms != incoming.occurred_at_ms
            || self.emitted_at_ms != incoming.emitted_at_ms
            || self.terminal_status != incoming.terminal_status
            || self.attempt_ordinal != incoming.attempt_ordinal
            || self.vendor_request_id != incoming.vendor_request_id
            || self.provider != incoming.provider
            || self.model != incoming.model
            || self.account_ref != incoming.account_ref
            || self.charge_basis != incoming.charge_basis
            || (self.auth_selection.is_some() && self.auth_selection != incoming.auth_selection)
            || (self.charge_basis_mapping_version.is_some()
                && self.charge_basis_mapping_version != incoming.charge_basis_mapping_version)
            || (self.charge_basis_overrides.is_some()
                && self.charge_basis_overrides != incoming.charge_basis_overrides)
        {
            return false;
        }
        let mut measurements = incoming.measurements.iter().collect::<Vec<_>>();
        measurements
            .sort_by(|left, right| (&left.role, &left.unit).cmp(&(&right.role, &right.unit)));
        let restated_output = restates_inclusive_output(&self.measurements, &incoming.measurements);
        self.measurements.len() == measurements.len()
            && self
                .measurements
                .iter()
                .zip(measurements)
                .all(|(stored, incoming)| {
                    stored == incoming || (stored.role == "output" && restated_output.is_some())
                })
    }
}

/// Source of BROCA's output count as the producer sent it, which includes
/// reasoning tokens. Ledger pricing and the multiplier read the same string
/// through [`crate::pricing::effective_output`].
pub(crate) const INCLUSIVE_OUTPUT_SOURCE: &str = "usage.output_tokens";
/// Source of BROCA's output count with reasoning subtracted. Both the wire
/// mapper and redelivery comparison use this label to distinguish it from the
/// inclusive count stored by earlier mappings.
pub const DERIVED_OUTPUT_SOURCE: &str = "usage.output_tokens-reasoning_tokens";

/// A mapping-version equivalence for BROCA's output measurement.
///
/// BROCA reports reasoning as part of output. Facts recorded before the wire
/// mapping split them store output `o` with source `usage.output_tokens`;
/// the same fact mapped now carries `o - r` with source
/// `usage.output_tokens-reasoning_tokens`, or no quantity and a `:conflicted`
/// source when `r > o`. BROCA re-sends its whole export after an
/// export-identity change, and a change in how astrocyte represents a fact
/// must not read as the producer contradicting itself, which would quarantine
/// every such fact.
///
/// Returns the stored output measurement when the incoming one is exactly
/// that re-representation of it: the event has exactly one output and one
/// reasoning measurement on each side, the reasoning measurements are equal,
/// and the incoming output is `o - r` (or conflicted with `r > o`) with every
/// other column unchanged. Only this direction is accepted: a stored derived
/// output against an incoming inclusive one is still a contradiction.
fn restates_inclusive_output<'a>(
    stored: &'a [MeasurementInput],
    incoming: &[MeasurementInput],
) -> Option<&'a MeasurementInput> {
    fn only<'a>(measurements: &'a [MeasurementInput], role: &str) -> Option<&'a MeasurementInput> {
        let mut matching = measurements.iter().filter(|m| m.role == role);
        let first = matching.next()?;
        matching.next().is_none().then_some(first)
    }
    let stored_output = only(stored, "output")?;
    let incoming_output = only(incoming, "output")?;
    let stored_reasoning = only(stored, "reasoning")?;
    let incoming_reasoning = only(incoming, "reasoning")?;
    if stored_output.source != INCLUSIVE_OUTPUT_SOURCE
        || stored_output.quantity_exponent != Some(0)
        || stored_reasoning != incoming_reasoning
        || stored_reasoning.quantity_exponent != Some(0)
    {
        return None;
    }
    let output = stored_output.quantity_units?;
    let reasoning = stored_reasoning.quantity_units?;
    let (source, units, exponent) = if reasoning > output {
        (format!("{DERIVED_OUTPUT_SOURCE}:conflicted"), None, None)
    } else {
        (
            DERIVED_OUTPUT_SOURCE.to_string(),
            Some(output - reasoning),
            stored_output.quantity_exponent,
        )
    };
    (incoming_output.role == stored_output.role
        && incoming_output.unit == stored_output.unit
        && incoming_output.kind == stored_output.kind
        && incoming_output.source == source
        && incoming_output.quantity_units == units
        && incoming_output.quantity_exponent == exponent)
        .then_some(stored_output)
}

#[derive(Default)]
struct PairEffects {
    transitions: usize,
    evidence: usize,
}

/// How a record reached the store. The caller declares it on every write
/// because the producer name cannot tell: one producer can later be served by
/// both a push path and a file reader.
///
/// `redelivery_log` counts a PRODUCER sending the same record again. That is
/// only true of a `Pushed` arrival. A `Read` arrival that matches a stored
/// record means we looked at the same file again, which says nothing about the
/// producer, so it never writes `redelivery_log`. A same-identity arrival with
/// a different payload is a contradiction in the data either way and goes to
/// `rejected_delivery` for both kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPath {
    /// The producer (or an operator acting for it) handed us this record.
    Pushed,
    /// We fetched this record ourselves, e.g. by reading a session file.
    Read,
}

/// Records one pushed event in its own transaction.
pub fn record_event(
    conn: &mut Connection,
    request: &EventRecordRequest,
    recorded_at_ms: i64,
) -> Result<EventRecordOutcome, IngestError> {
    let tx = conn.transaction()?;
    let outcome = record_event_in_tx(&tx, request, recorded_at_ms, DeliveryPath::Pushed)?;
    tx.commit()?;
    Ok(outcome)
}

pub fn record_event_in_tx(
    tx: &Transaction<'_>,
    request: &EventRecordRequest,
    recorded_at_ms: i64,
    delivery: DeliveryPath,
) -> Result<EventRecordOutcome, IngestError> {
    validate_event_request(request, recorded_at_ms)?;

    if let Some((stored_event_id, stored_payload)) = load_event_payload(tx, request)? {
        if stored_payload.restated_by(request) {
            if delivery == DeliveryPath::Pushed {
                tx.execute(
                    "INSERT INTO redelivery_log (
                     producer,producer_lineage,producer_ref,event_key,detected_at_ms
                 ) VALUES (?1,?2,?3,(SELECT event_key FROM usage_event WHERE event_id=?4),?5)",
                    params![
                        request.producer,
                        request.producer_lineage,
                        request.producer_ref,
                        stored_event_id,
                        recorded_at_ms,
                    ],
                )?;
            }
            return Ok(EventRecordOutcome {
                event_id: stored_event_id,
                disposition: RecordDisposition::ExactRedelivery,
                transitions_written: 0,
                quarantine_evidence_written: 0,
                waiting_for_target: false,
            });
        }
        let incoming_payload = canonical_incoming_payload(request)?;
        tx.execute(
            "INSERT INTO rejected_delivery (
                 producer,producer_lineage,producer_ref,incoming_payload,stored_event_key,detected_at_ms
             ) VALUES (?1,?2,?3,?4,(SELECT event_key FROM usage_event WHERE event_id=?5),?6)",
            params![
                request.producer,
                request.producer_lineage,
                request.producer_ref,
                incoming_payload,
                stored_event_id,
                recorded_at_ms,
            ],
        )?;
        return Ok(EventRecordOutcome {
            event_id: stored_event_id,
            disposition: RecordDisposition::QuarantinedRedelivery,
            transitions_written: 0,
            quarantine_evidence_written: 0,
            waiting_for_target: false,
        });
    }

    let local_event_id = insert_event(tx, request, recorded_at_ms)?;

    let mut effects = PairEffects::default();
    let mut waiting_for_target = false;
    if let Some(prior_event_ref) = request.prior_event_ref.as_deref() {
        let target = find_target_event(
            tx,
            &request.producer,
            request.producer_lineage.as_deref(),
            prior_event_ref,
            &local_event_id,
        )?;
        if let Some(target_event_id) = target {
            merge_pair_effects(
                &mut effects,
                settle_pair(
                    tx,
                    &local_event_id,
                    &target_event_id,
                    &request.measurements,
                    recorded_at_ms,
                )?,
            );
        } else {
            waiting_for_target = true;
        }
    }

    let waiting = waiting_referring_events(
        tx,
        &request.producer,
        request.producer_lineage.as_deref(),
        &request.producer_ref,
        &local_event_id,
    )?;
    for waiting_event_id in waiting {
        let measurements = load_measurements(tx, &waiting_event_id)?;
        merge_pair_effects(
            &mut effects,
            settle_pair_against_measurements(
                tx,
                &waiting_event_id,
                &local_event_id,
                &measurements,
                &request.measurements,
                recorded_at_ms,
            )?,
        );
    }

    // Measurements land after any transition. Besides keeping all effects in
    // one transaction, this ordering lets a failed event write prove that the
    // transition evidence rolls back with the settling act.
    insert_measurements(tx, &local_event_id, &request.measurements)?;
    // Queued in this transaction so the queue cannot disagree with the ledger
    // after a crash: the event and its queue row commit or vanish together.
    // An event with no measurements has nothing to price and is not queued.
    crate::pricing::enqueue_if_unpriced_in_tx(tx, &local_event_id)?;

    Ok(EventRecordOutcome {
        event_id: local_event_id,
        disposition: RecordDisposition::Recorded,
        transitions_written: effects.transitions,
        quarantine_evidence_written: effects.evidence,
        waiting_for_target,
    })
}

pub fn settle_event_in_tx(
    tx: &Transaction<'_>,
    request: &EventSettleRequest,
    occurred_at_ms: i64,
) -> Result<EventSettleOutcome, IngestError> {
    if request.event_id.is_empty() || request.reason.is_empty() || occurred_at_ms < 0 {
        return Err(IngestError::Refused(Refusal::Malformed));
    }
    if request.to_state == ChargeabilityState::Billed {
        return Err(IngestError::Refused(Refusal::OperatorCannotBill));
    }
    let currency = tx
        .query_row(
            "SELECT currency FROM usage_event WHERE event_id=?1",
            [&request.event_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .ok_or(IngestError::Refused(Refusal::NotFound))?;
    if current_chargeability_state(tx, &request.event_id)? != "unresolved" {
        return Err(IngestError::Refused(Refusal::AlreadySettled));
    }
    if currency.is_none() {
        return Err(IngestError::Refused(Refusal::UnpricedTarget));
    }
    tx.execute(
        "INSERT INTO chargeability_transition (
             event_key,from_state,to_state,reason,resolved_by,occurred_at_ms
         ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'unresolved',?2,?3,'operator',?4)",
        params![
            request.event_id,
            request.to_state.as_str(),
            request.reason,
            occurred_at_ms
        ],
    )?;
    Ok(EventSettleOutcome {
        event_id: request.event_id.clone(),
        to_state: request.to_state,
    })
}

pub fn record_adjustment_in_tx(
    tx: &Transaction<'_>,
    request: &AdjustmentRecordRequest,
    context: &AdjustmentWriteContext,
    recorded_at_ms: i64,
) -> Result<AdjustmentRecordOutcome, IngestError> {
    if let Some(correction_id) = &context.correction_id {
        if correction_id.is_empty() || correction_id.len() > 128 {
            return Err(IngestError::Refused(Refusal::Malformed));
        }
        let prior = tx
            .query_row(
                "SELECT adjustment_id,event.event_id,adjustment.currency,amount_nanos,reason,
                    adjustment.occurred_at_ms,note
             FROM adjustment JOIN usage_event event USING(event_key) WHERE correction_id=?1",
                [correction_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        AdjustmentRecordRequest {
                            event_id: row.get(1)?,
                            currency: row.get(2)?,
                            amount_nanos: row.get(3)?,
                            reason: match row.get::<_, String>(4)?.as_str() {
                                "write_off" => AdjustmentReason::WriteOff,
                                "vendor_restatement" => AdjustmentReason::VendorRestatement,
                                "our_repricing" => AdjustmentReason::OurRepricing,
                                "refund" => AdjustmentReason::Refund,
                                "goodwill" => AdjustmentReason::Goodwill,
                                _ => return Err(rusqlite::Error::InvalidQuery),
                            },
                            occurred_at_ms: row.get(5)?,
                            note: row.get(6)?,
                        },
                    ))
                },
            )
            .optional()?;
        if let Some((adjustment_id, prior)) = prior {
            if prior != *request {
                return Err(IngestError::Refused(Refusal::CorrectionIdReused));
            }
            return Ok(AdjustmentRecordOutcome {
                adjustment_id,
                origin: context.origin,
            });
        }
    }
    if request.event_id.is_empty()
        || request.currency.is_empty()
        || request.occurred_at_ms < 0
        || recorded_at_ms < 0
    {
        return Err(IngestError::Refused(Refusal::Malformed));
    }
    let target = tx
        .query_row(
            "SELECT occurred_at_ms,currency FROM usage_event WHERE event_id=?1",
            [&request.event_id],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?
        .ok_or(IngestError::Refused(Refusal::NotFound))?;
    if target.0.is_none() {
        return Err(IngestError::Refused(Refusal::UndatedTarget));
    }
    let target_currency = target
        .1
        .ok_or(IngestError::Refused(Refusal::UnpricedTarget))?;
    if target_currency != request.currency {
        return Err(IngestError::Refused(Refusal::CurrencyMismatch));
    }
    if request.reason == AdjustmentReason::WriteOff {
        // Verifying that a write-off reaches exactly zero requires summing the
        // priced charge chain. This ingest code does not own the pricing schema
        // or arithmetic, so it refuses write-offs until pricing can validate one.
        return Err(IngestError::Refused(Refusal::WriteOffRequiresPricing));
    }

    let prior_count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM adjustment WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
        [&request.event_id],
        |row| row.get(0),
    )?;
    let adjustment_id = canonical_hash(&json!({
        "event_id": request.event_id,
        "currency": request.currency,
        "amount_nanos": request.amount_nanos,
        "reason": request.reason.as_str(),
        "occurred_at_ms": request.occurred_at_ms,
        "recorded_at_ms": recorded_at_ms,
        "origin": context.origin.as_str(),
        "operator": context.operator,
        "note": request.note,
        "correction_id": context.correction_id,
        "event_adjustment_ordinal": prior_count,
    }))?;
    tx.execute(
        "INSERT INTO adjustment (
             adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,
              recorded_at_ms,origin,operator,note,correction_id
          ) VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            adjustment_id,
            request.event_id,
            request.currency,
            request.amount_nanos,
            request.reason.as_str(),
            request.occurred_at_ms,
            recorded_at_ms,
            context.origin.as_str(),
            context.operator,
            request.note,
            context.correction_id,
        ],
    )?;
    Ok(AdjustmentRecordOutcome {
        adjustment_id,
        origin: context.origin,
    })
}

/// Commits a previously resolved correction; catalog access belongs to the
/// separate resolution phase, outside the transaction and store mutex.
pub fn correct_basis_in_tx(
    tx: &Transaction<'_>,
    plan: &crate::subscription::SubscriptionRepricePlan,
    resolved: &crate::subscription::ResolvedSubscriptionReprice,
    recorded_at_ms: i64,
) -> Result<crate::pricing::RepriceOutcome, crate::subscription::SubscriptionError> {
    crate::subscription::commit_reprice_in_tx(
        tx,
        plan,
        resolved,
        recorded_at_ms,
        crate::pricing::PricingOrigin::BasisCorrection,
    )
}

fn current_chargeability_state(
    tx: &Transaction<'_>,
    event_id: &str,
) -> Result<String, IngestError> {
    Ok(tx
        .query_row(
            "SELECT to_state FROM chargeability_transition WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
            [event_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_else(|| "unresolved".to_string()))
}

fn validate_event_request(
    request: &EventRecordRequest,
    recorded_at_ms: i64,
) -> Result<(), IngestError> {
    if request.producer.is_empty()
        || request.producer_ref.is_empty()
        || request.prior_event_ref.as_deref() == Some("")
        || recorded_at_ms < 0
    {
        return Err(IngestError::Refused(Refusal::Malformed));
    }
    let mut identities = BTreeSet::new();
    for measurement in &request.measurements {
        let quantity_present =
            measurement.quantity_units.is_some() && measurement.quantity_exponent.is_some();
        let quantity_absent =
            measurement.quantity_units.is_none() && measurement.quantity_exponent.is_none();
        if measurement.role.is_empty()
            || measurement.unit.is_empty()
            || measurement.kind.is_empty()
            || measurement.source.is_empty()
            || (!quantity_present && !quantity_absent)
            || measurement.quantity_units.is_some_and(|units| units < 0)
            || measurement
                .quantity_exponent
                .is_some_and(|exponent| !(-9..=0).contains(&exponent))
            || !identities.insert((&measurement.role, &measurement.unit))
        {
            return Err(IngestError::Refused(Refusal::Malformed));
        }
    }
    Ok(())
}

fn canonical_incoming_payload(request: &EventRecordRequest) -> Result<String, CanonError> {
    let mut measurements = request.measurements.iter().collect::<Vec<_>>();
    measurements.sort_by(|left, right| (&left.role, &left.unit).cmp(&(&right.role, &right.unit)));
    jcs_serialize(&json!({
        "producer": request.producer,
        "producer_lineage": request.producer_lineage,
        "producer_ref": request.producer_ref,
        "prior_event_ref": request.prior_event_ref,
        "harness": request.harness,
        "project_root": request.project_root,
        "session": request.session,
        "run_ref": request.run_ref,
        "work_class": request.work_class,
        "occurred_at_ms": request.occurred_at_ms,
        "emitted_at_ms": request.emitted_at_ms,
        "terminal_status": request.terminal_status,
        "attempt_ordinal": request.attempt_ordinal,
        "vendor_request_id": request.vendor_request_id,
        "provider": request.provider,
        "model": request.model,
        "account_ref": request.account_ref,
        "auth_selection": request.auth_selection,
        "charge_basis": request.charge_basis,
        "charge_basis_mapping_version": request.charge_basis_mapping_version,
        "charge_basis_overrides": request.charge_basis_overrides,
        "measurements": measurements,
    }))
}

fn load_event_payload(
    tx: &Transaction<'_>,
    request: &EventRecordRequest,
) -> Result<Option<(String, EventPayload)>, IngestError> {
    // `IS` is null-safe equality: a lineage-absent request must find the
    // lineage-absent stored row, which `= NULL` could never match.
    let stored = tx
        .prepare_cached(
            "SELECT prior_event_ref,harness,project_root,session,run_ref,work_class,
                occurred_at_ms,emitted_at_ms,terminal_status,attempt_ordinal,
                vendor_request_id,provider,model,account_ref,auth_selection,charge_basis,
                charge_basis_mapping_version,charge_basis_overrides,event_id
         FROM usage_event
         WHERE producer=?1 AND producer_lineage IS ?2 AND producer_ref=?3",
        )?
        .query_row(
            params![
                request.producer,
                request.producer_lineage,
                request.producer_ref
            ],
            |row| {
                let attempt_ordinal = row.get::<_, Option<i64>>(9)?;
                let charge_basis = row.get::<_, String>(15)?;
                Ok((
                    row.get::<_, String>(18)?,
                    EventPayload {
                        prior_event_ref: row.get(0)?,
                        harness: row.get(1)?,
                        project_root: row.get(2)?,
                        session: row.get(3)?,
                        run_ref: row.get(4)?,
                        work_class: row.get(5)?,
                        occurred_at_ms: row.get(6)?,
                        emitted_at_ms: row.get(7)?,
                        terminal_status: row.get(8)?,
                        attempt_ordinal: attempt_ordinal
                            .map(|value| {
                                u32::try_from(value)
                                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(9, value))
                            })
                            .transpose()?,
                        vendor_request_id: row.get(10)?,
                        provider: row.get(11)?,
                        model: row.get(12)?,
                        account_ref: row.get(13)?,
                        auth_selection: row.get(14)?,
                        charge_basis: ChargeBasis::parse(&charge_basis).ok_or_else(|| {
                            stored_value_error(15, "usage_event.charge_basis", &charge_basis)
                        })?,
                        charge_basis_mapping_version: row.get(16)?,
                        charge_basis_overrides: row.get(17)?,
                        measurements: Vec::new(),
                    },
                ))
            },
        )
        .optional()?;
    let Some((event_id, mut payload)) = stored else {
        return Ok(None);
    };
    payload.measurements = load_measurements(tx, &event_id)?;
    Ok(Some((event_id, payload)))
}

fn load_measurements(
    tx: &Transaction<'_>,
    event_id: &str,
) -> Result<Vec<MeasurementInput>, IngestError> {
    let mut statement = tx.prepare_cached(
        "SELECT role,unit,kind,source,quantity_units,quantity_exponent
         FROM measurement WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1) ORDER BY role,unit",
    )?;
    let rows = statement.query_map([event_id], |row| {
        Ok(MeasurementInput {
            role: row.get(0)?,
            unit: row.get(1)?,
            kind: row.get(2)?,
            source: row.get(3)?,
            quantity_units: row.get(4)?,
            quantity_exponent: row.get(5)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Inserts the event under the id derived from its natural key, and returns that id.
///
/// This is the only production writer of `usage_event`, and it never accepts an id
/// from its caller. Uniqueness of the natural key rests on that: `UNIQUE(event_id)`
/// enforces it only because every stored id is the hash of the key. For an absent
/// lineage no other index does, since SQLite treats NULLs in the table's
/// `UNIQUE(producer, producer_lineage, producer_ref)` as distinct.
fn insert_event(
    tx: &Transaction<'_>,
    request: &EventRecordRequest,
    recorded_at_ms: i64,
) -> Result<String, IngestError> {
    // Identity is {producer, producer_lineage, producer_ref} and must not
    // change: the same charge re-exported with the same pin must dedupe.
    // auth_selection is deliberately excluded from the canonical hash.
    let event_id = event_id(
        &request.producer,
        request.producer_lineage.as_deref(),
        &request.producer_ref,
    )?;
    tx.execute(
        "INSERT INTO usage_event (
             event_id,producer,producer_lineage,producer_ref,prior_event_ref,
             harness,project_root,session,run_ref,work_class,
             occurred_at_ms,emitted_at_ms,recorded_at_ms,terminal_status,attempt_ordinal,
             vendor_request_id,provider,model,account_ref,auth_selection,charge_basis,pricing_generation,
             charge_basis_mapping_version,charge_basis_overrides
          ) VALUES (
              ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,
              ?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,0,?22,?23
          )",
        params![
            event_id,
            request.producer,
            request.producer_lineage,
            request.producer_ref,
            request.prior_event_ref,
            request.harness,
            request.project_root,
            request.session,
            request.run_ref,
            request.work_class,
            request.occurred_at_ms,
            request.emitted_at_ms,
            recorded_at_ms,
            request.terminal_status,
            request.attempt_ordinal.map(i64::from),
            request.vendor_request_id,
            request.provider,
            request.model,
            request.account_ref,
            request.auth_selection,
            request.charge_basis.as_str(),
            request.charge_basis_mapping_version,
            request.charge_basis_overrides,
        ],
    )?;
    Ok(event_id)
}

fn insert_measurements(
    tx: &Transaction<'_>,
    event_id: &str,
    measurements: &[MeasurementInput],
) -> Result<(), IngestError> {
    for measurement in measurements {
        tx.execute(
            "INSERT INTO measurement (
                 event_key,role,unit,kind,source,quantity_units,quantity_exponent
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),?2,?3,?4,?5,?6,?7)",
            params![
                event_id,
                measurement.role,
                measurement.unit,
                measurement.kind,
                measurement.source,
                measurement.quantity_units,
                measurement.quantity_exponent,
            ],
        )?;
    }
    Ok(())
}

fn find_target_event(
    tx: &Transaction<'_>,
    producer: &str,
    producer_lineage: Option<&str>,
    prior_event_ref: &str,
    referring_event_id: &str,
) -> Result<Option<String>, IngestError> {
    Ok(tx
        .query_row(
            "SELECT event_id FROM usage_event
             WHERE producer=?1 AND producer_lineage IS ?2 AND producer_ref=?3 AND event_id<>?4",
            params![
                producer,
                producer_lineage,
                prior_event_ref,
                referring_event_id
            ],
            |row| row.get(0),
        )
        .optional()?)
}

fn waiting_referring_events(
    tx: &Transaction<'_>,
    producer: &str,
    producer_lineage: Option<&str>,
    target_producer_ref: &str,
    target_event_id: &str,
) -> Result<Vec<String>, IngestError> {
    let mut statement = tx.prepare_cached(
        "SELECT event_id FROM usage_event
         WHERE producer=?1 AND producer_lineage IS ?2 AND prior_event_ref=?3 AND event_id<>?4
         ORDER BY event_id",
    )?;
    let rows = statement.query_map(
        params![
            producer,
            producer_lineage,
            target_producer_ref,
            target_event_id
        ],
        |row| row.get(0),
    )?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn settle_pair(
    tx: &Transaction<'_>,
    referring_event_id: &str,
    target_event_id: &str,
    referring_measurements: &[MeasurementInput],
    detected_at_ms: i64,
) -> Result<PairEffects, IngestError> {
    let target_measurements = load_measurements(tx, target_event_id)?;
    settle_pair_against_measurements(
        tx,
        referring_event_id,
        target_event_id,
        referring_measurements,
        &target_measurements,
        detected_at_ms,
    )
}

fn settle_pair_against_measurements(
    tx: &Transaction<'_>,
    referring_event_id: &str,
    target_event_id: &str,
    referring_measurements: &[MeasurementInput],
    target_measurements: &[MeasurementInput],
    detected_at_ms: i64,
) -> Result<PairEffects, IngestError> {
    if current_chargeability_state(tx, referring_event_id)? != "unresolved" {
        return Ok(PairEffects::default());
    }
    let targets = target_measurements
        .iter()
        .map(|measurement| {
            (
                (measurement.role.as_str(), measurement.unit.as_str()),
                measurement,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut qualifying = false;
    let mut contradictions = Vec::new();
    for incoming in referring_measurements {
        let (Some(incoming_units), Some(incoming_exponent)) =
            (incoming.quantity_units, incoming.quantity_exponent)
        else {
            continue;
        };
        let Some(stored) = targets.get(&(incoming.role.as_str(), incoming.unit.as_str())) else {
            continue;
        };
        match (stored.quantity_units, stored.quantity_exponent) {
            (None, None) => qualifying = true,
            (Some(stored_units), Some(stored_exponent)) => contradictions.push((
                incoming,
                (stored_units, stored_exponent),
                (incoming_units, incoming_exponent),
            )),
            _ => return Err(IngestError::Refused(Refusal::Malformed)),
        }
    }

    if !contradictions.is_empty() {
        for (measurement, stored, incoming) in &contradictions {
            let stored_quantity = decimal_json(stored.0, stored.1)?;
            let incoming_quantity = decimal_json(incoming.0, incoming.1)?;
            tx.execute(
                "INSERT INTO quarantine_evidence (
                     event_key,target_event_key,role,unit,stored_quantity,incoming_quantity,detected_at_ms
                 ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),(SELECT event_key FROM usage_event WHERE event_id=?2),?3,?4,?5,?6,?7)",
                params![
                    referring_event_id,
                    target_event_id,
                    measurement.role,
                    measurement.unit,
                    stored_quantity,
                    incoming_quantity,
                    detected_at_ms,
                ],
            )?;
        }
        return Ok(PairEffects {
            transitions: 0,
            evidence: contradictions.len(),
        });
    }

    if qualifying {
        tx.execute(
            "INSERT INTO chargeability_transition (
                 event_key,from_state,to_state,reason,resolved_by,occurred_at_ms
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'unresolved','billed','supplied_missing_quantity','later_record',?2)",
            params![referring_event_id, detected_at_ms],
        )?;
        return Ok(PairEffects {
            transitions: 1,
            evidence: 0,
        });
    }
    Ok(PairEffects::default())
}

fn decimal_json(units: i64, exponent: i32) -> Result<String, CanonError> {
    jcs_serialize(&json!({"units": units, "exponent": exponent}))
}

fn merge_pair_effects(total: &mut PairEffects, next: PairEffects) {
    total.transitions += next.transitions;
    total.evidence += next.evidence;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{initialize_connection, MIGRATIONS};

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn measurement(role: &str, units: Option<i64>) -> MeasurementInput {
        MeasurementInput {
            role: role.into(),
            unit: "token".into(),
            kind: "billed".into(),
            source: format!("usage.{role}"),
            quantity_units: units,
            quantity_exponent: units.map(|_| 0),
        }
    }

    fn request(
        producer_ref: &str,
        prior_event_ref: Option<&str>,
        measurements: Vec<MeasurementInput>,
    ) -> EventRecordRequest {
        EventRecordRequest {
            producer: "plex".into(),
            producer_lineage: Some("lineage-a".into()),
            producer_ref: producer_ref.into(),
            prior_event_ref: prior_event_ref.map(str::to_string),
            harness: Some("connector".into()),
            project_root: Some("/project".into()),
            session: Some("session-a".into()),
            run_ref: Some("run-a".into()),
            work_class: Some("generation".into()),
            occurred_at_ms: Some(1_000),
            emitted_at_ms: Some(1_010),
            terminal_status: Some("completed".into()),
            attempt_ordinal: Some(0),
            vendor_request_id: Some(format!("vendor-{producer_ref}")),
            provider: Some("provider-a".into()),
            model: Some("model-a".into()),
            account_ref: Some("account-a".into()),
            auth_selection: None,
            charge_basis: ChargeBasis::MeteredApi,
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements,
        }
    }

    #[test]
    fn unknown_stored_charge_basis_is_an_integrity_error_without_redelivery_effects() {
        let mut conn = conn();
        conn.execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        conn.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_lineage,producer_ref,recorded_at_ms,charge_basis
             ) VALUES ('corrupt','plex','lineage-a','corrupt',1000,'unrecognized')",
            [],
        )
        .unwrap();
        let incoming = request("corrupt", None, vec![measurement("input", Some(1))]);
        let error = record_event(&mut conn, &incoming, 2_000).unwrap_err();
        let IngestError::Store(error) = error else {
            panic!("expected a store integrity error")
        };
        assert!(error
            .to_string()
            .contains("unrecognized stored usage_event.charge_basis"));
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(count(&conn, "measurement"), 0);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
        assert_eq!(count(&conn, "redelivery_log"), 0);
    }

    #[test]
    #[ignore = "manual redelivery timing fixture"]
    fn redelivery_query_fixture() {
        let mut conn = conn();
        let mut requests = Vec::new();
        for index in 0..1_000 {
            let request = request(
                &format!("event-{index}"),
                None,
                vec![
                    measurement("output", Some(10)),
                    measurement("input", Some(20)),
                    measurement("reasoning", Some(5)),
                ],
            );
            record_event(&mut conn, &request, 2_000).unwrap();
            requests.push(request);
        }
        let tx = conn.transaction().unwrap();
        let start = std::time::Instant::now();
        for _ in 0..5 {
            for request in &requests {
                let outcome = record_event_in_tx(&tx, request, 2_001, DeliveryPath::Read).unwrap();
                assert_eq!(outcome.disposition, RecordDisposition::ExactRedelivery);
            }
        }
        eprintln!(
            "redelivery fixture: 5 passes x 1000 events x 3 measurements: {:?}",
            start.elapsed()
        );
        assert_eq!(count(&tx, "usage_event"), 1_000);
        assert_eq!(count(&tx, "measurement"), 3_000);
        assert_eq!(count(&tx, "rejected_delivery"), 0);
        assert_eq!(count(&tx, "redelivery_log"), 0);
        assert_eq!(count(&tx, "chargeability_transition"), 0);
    }

    fn count(conn: &Connection, table: &str) -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn state(conn: &mut Connection, event_id: &str) -> String {
        let tx = conn.transaction().unwrap();
        let state = current_chargeability_state(&tx, event_id).unwrap();
        tx.rollback().unwrap();
        state
    }

    fn establish_currency(conn: &Connection, event_id: &str, currency: &str) {
        conn.execute(
            "UPDATE usage_event SET currency=?2 WHERE event_id=?1",
            params![event_id, currency],
        )
        .unwrap();
    }

    /// Settles an event the way the served settlement operation does: through
    /// `settle_event_in_tx` inside one transaction that commits on success.
    fn settle_event(
        conn: &mut Connection,
        request: &EventSettleRequest,
        occurred_at_ms: i64,
    ) -> Result<EventSettleOutcome, IngestError> {
        let tx = conn.transaction()?;
        let outcome = settle_event_in_tx(&tx, request, occurred_at_ms)?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Records an adjustment the way the served correction-ingest operation
    /// does: through `record_adjustment_in_tx` with the correction-ingest write
    /// context, inside one transaction that commits on success.
    fn record_correction(
        conn: &mut Connection,
        request: &AdjustmentRecordRequest,
        recorded_at_ms: i64,
    ) -> Result<AdjustmentRecordOutcome, IngestError> {
        let tx = conn.transaction()?;
        let outcome = record_adjustment_in_tx(
            &tx,
            request,
            &AdjustmentWriteContext::correction_ingest("correction-1".into()),
            recorded_at_ms,
        )?;
        tx.commit()?;
        Ok(outcome)
    }

    #[test]
    fn correction_retry_moves_money_only_once() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("correction", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        establish_currency(&conn, &event, "USD");
        let correction = AdjustmentRecordRequest {
            event_id: event,
            currency: "USD".into(),
            amount_nanos: -25,
            reason: AdjustmentReason::Refund,
            occurred_at_ms: 1_000,
            note: Some("refund".into()),
        };
        let first = record_correction(&mut conn, &correction, 3_000).unwrap();
        let second = record_correction(&mut conn, &correction, 4_000).unwrap();
        let money: (i64, i64) = conn
            .query_row(
                "SELECT count(*),sum(amount_nanos) FROM adjustment",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(money, (1, -25), "a retry must not move money again");
        assert_eq!(second, first);
    }

    #[test]
    fn correction_key_conflicting_content_is_refused_without_moving_money() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("correction", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        establish_currency(&conn, &event, "USD");
        let correction = AdjustmentRecordRequest {
            event_id: event,
            currency: "USD".into(),
            amount_nanos: -25,
            reason: AdjustmentReason::Refund,
            occurred_at_ms: 1_000,
            note: Some("refund".into()),
        };
        record_correction(&mut conn, &correction, 3_000).unwrap();
        let mut variants = vec![correction.clone(); 6];
        variants[0].event_id = "missing".into();
        variants[1].currency = "JPY".into();
        variants[2].amount_nanos = -50;
        variants[3].reason = AdjustmentReason::Goodwill;
        variants[4].occurred_at_ms = -1;
        variants[5].note = None;
        for different in variants {
            assert!(matches!(
                record_correction(&mut conn, &different, 4_000),
                Err(IngestError::Refused(Refusal::CorrectionIdReused))
            ));
            let money: (i64, i64) = conn
                .query_row(
                    "SELECT count(*),sum(amount_nanos) FROM adjustment",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(money, (1, -25));
        }
        let tx = conn.transaction().unwrap();
        record_adjustment_in_tx(
            &tx,
            &correction,
            &AdjustmentWriteContext::correction_ingest("separate-correction".into()),
            4_000,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            count(&conn, "adjustment"),
            2,
            "a different key is a separate act"
        );
    }

    #[test]
    fn correction_keys_are_nonempty_and_bounded_in_utf8_bytes() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("correction", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        establish_currency(&conn, &event, "USD");
        let correction = AdjustmentRecordRequest {
            event_id: event,
            currency: "USD".into(),
            amount_nanos: -25,
            reason: AdjustmentReason::Refund,
            occurred_at_ms: 1_000,
            note: None,
        };
        for key in [String::new(), "x".repeat(129), "é".repeat(65)] {
            let tx = conn.transaction().unwrap();
            assert!(matches!(
                record_adjustment_in_tx(
                    &tx,
                    &correction,
                    &AdjustmentWriteContext::correction_ingest(key),
                    3_000
                ),
                Err(IngestError::Refused(Refusal::Malformed))
            ));
        }
        assert_eq!(count(&conn, "adjustment"), 0);
        let tx = conn.transaction().unwrap();
        record_adjustment_in_tx(
            &tx,
            &correction,
            &AdjustmentWriteContext::correction_ingest("é".repeat(64)),
            3_000,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(count(&conn, "adjustment"), 1);
    }

    #[test]
    fn correction_key_migration_preserves_unkeyed_rows_and_enforces_partial_uniqueness() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        let position = MIGRATIONS
            .iter()
            .position(|migration| migration.statements.contains("ADD COLUMN correction_id"))
            .unwrap();
        for migration in &MIGRATIONS[..position] {
            conn.execute_batch(migration.statements).unwrap();
        }
        let event = "correction";
        conn.execute("INSERT INTO usage_event (event_id,producer,producer_ref,charge_basis,recorded_at_ms,occurred_at_ms) VALUES (?1,'fixture',?1,'metered_api',2000,1000)", [event]).unwrap();
        establish_currency(&conn, event, "USD");
        conn.execute(
            "INSERT INTO adjustment (
            adjustment_id,event_id,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin
            ) VALUES ('old',?1,'USD',-25,'refund',1000,3000,'statement')",
            [&event],
        )
        .unwrap();
        for migration in &MIGRATIONS[position..] {
            conn.execute_batch(migration.statements).unwrap();
        }
        let old: (i64, Option<String>) = conn
            .query_row(
                "SELECT amount_nanos,correction_id FROM adjustment WHERE adjustment_id='old'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(old, (-25, None));
        let insert = |id: &str, key: Option<&str>| {
            conn.execute("INSERT INTO adjustment (
            adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,correction_id
            ) VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),'USD',-25,'refund',1000,3000,'statement',?3)", params![id,event,key])
        };
        insert("unkeyed", None).unwrap();
        insert("keyed", Some("key")).unwrap();
        for (id, key) in [
            ("duplicate", "key"),
            ("empty", ""),
            ("long", &"é".repeat(65)),
        ] {
            assert!(insert(id, Some(key)).is_err());
        }
        assert_eq!(count(&conn, "adjustment"), 3);
    }

    #[test]
    fn event_record_round_trips_without_pricing_or_currency_write() {
        let mut conn = conn();
        let request = request("event-a", None, vec![measurement("input", Some(4))]);
        let outcome = record_event(&mut conn, &request, 2_000).unwrap();
        assert_eq!(outcome.disposition, RecordDisposition::Recorded);
        let stored: (Option<String>, i64, i64) = conn
            .query_row(
                "SELECT currency,
                        (SELECT COUNT(*) FROM measurement WHERE measurement.event_key=usage_event.event_key),
                        (SELECT COUNT(*) FROM chargeability_transition WHERE chargeability_transition.event_key=usage_event.event_key)
                 FROM usage_event WHERE event_id=?1",
                [&outcome.event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (None, 1, 0));
    }

    fn queued(conn: &Connection) -> Vec<String> {
        conn.prepare("SELECT event_id FROM pricing_pending JOIN usage_event USING(event_key) ORDER BY event_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap()
    }

    /// The pricing queue row is written by the recording transaction itself.
    /// A queue row written afterwards in its own transaction would be lost if
    /// the process died between the two commits, and the event would then
    /// never be priced.
    ///
    /// Two halves, because each catches a different way of getting this
    /// wrong. Inside an uncommitted recording the queue row must already be
    /// visible, and rolling the recording back must take it away. And when
    /// the enqueue itself fails, the recording must fail with it, leaving no
    /// event: an event committed before its enqueue is the lost-row shape.
    #[test]
    fn recording_enqueues_in_the_recording_transaction_and_a_failed_recording_leaves_no_queue_row()
    {
        let mut conn = conn();
        let recorded = request("event-a", None, vec![measurement("input", Some(4))]);

        let tx = conn.transaction().unwrap();
        let outcome = record_event_in_tx(&tx, &recorded, 2_000, DeliveryPath::Pushed).unwrap();
        assert_eq!(
            queued(&tx),
            vec![outcome.event_id.clone()],
            "the queue row must be written by the recording transaction"
        );
        drop(tx);
        assert_eq!(count(&conn, "usage_event"), 0);
        assert!(
            queued(&conn).is_empty(),
            "a rolled-back recording leaves no queue row"
        );

        conn.execute_batch(
            "CREATE TEMP TRIGGER forced_enqueue_failure BEFORE INSERT ON pricing_pending
             BEGIN SELECT RAISE(ABORT, 'forced enqueue failure'); END;",
        )
        .unwrap();
        assert!(record_event(&mut conn, &recorded, 2_000).is_err());
        assert_eq!(
            count(&conn, "usage_event"),
            0,
            "a failed enqueue must fail the recording, not leave an unqueued event"
        );
        assert!(queued(&conn).is_empty());

        conn.execute_batch("DROP TRIGGER temp.forced_enqueue_failure")
            .unwrap();
        let outcome = record_event(&mut conn, &recorded, 2_000).unwrap();
        assert_eq!(queued(&conn), vec![outcome.event_id]);
    }

    /// Enqueuing is idempotent: an exact redelivery adds nothing, and an event
    /// with no measurements has nothing to price and is never queued.
    #[test]
    fn redelivery_and_measurementless_events_add_no_queue_row() {
        let mut conn = conn();
        let recorded = request("event-a", None, vec![measurement("input", Some(4))]);
        let first = record_event(&mut conn, &recorded, 2_000).unwrap();
        record_event(&mut conn, &recorded, 3_000).unwrap();
        record_event(&mut conn, &request("event-empty", None, Vec::new()), 2_000).unwrap();
        assert_eq!(count(&conn, "usage_event"), 2);
        assert_eq!(queued(&conn), vec![first.event_id.clone()]);
        assert_eq!(
            crate::pricing::enqueue_if_unpriced_in_tx(&conn, &first.event_id).unwrap(),
            0,
            "enqueuing an event already queued changes nothing"
        );
    }

    #[test]
    fn exact_redelivery_is_an_order_independent_content_noop_and_logs_occurrence() {
        let mut conn = conn();
        let first = request(
            "same",
            None,
            vec![
                measurement("output", Some(2)),
                measurement("input", Some(1)),
            ],
        );
        let first_outcome = record_event(&mut conn, &first, 2_000).unwrap();
        let mut reordered = first.clone();
        reordered.measurements.reverse();
        let second = record_event(&mut conn, &reordered, 3_000).unwrap();
        assert_eq!(second.disposition, RecordDisposition::ExactRedelivery);
        assert_eq!(second.event_id, first_outcome.event_id);
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(count(&conn, "measurement"), 2);
        assert_eq!(count(&conn, "redelivery_log"), 1);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
    }

    /// An output measurement under an explicit source.
    fn output_with_source(source: &str, units: Option<i64>) -> MeasurementInput {
        MeasurementInput {
            source: source.into(),
            ..measurement("output", units)
        }
    }

    /// Records `stored` and then `incoming` under the same identity, and
    /// returns the second disposition with the rejected_delivery row count.
    fn resend(
        stored: MeasurementInput,
        incoming: MeasurementInput,
        stored_reasoning: i64,
        incoming_reasoning: i64,
    ) -> (RecordDisposition, i64) {
        let mut conn = conn();
        let first = request(
            "same",
            None,
            vec![stored, measurement("reasoning", Some(stored_reasoning))],
        );
        record_event(&mut conn, &first, 2_000).unwrap();
        let second = request(
            "same",
            None,
            vec![incoming, measurement("reasoning", Some(incoming_reasoning))],
        );
        let outcome = record_event(&mut conn, &second, 3_000).unwrap();
        (outcome.disposition, count(&conn, "rejected_delivery"))
    }

    /// A fact stored with BROCA's inclusive output and re-sent in the derived
    /// form (`o - r`) is the same fact, not a contradiction.
    #[test]
    fn derived_output_restates_a_stored_inclusive_output() {
        let outcome = resend(
            output_with_source(INCLUSIVE_OUTPUT_SOURCE, Some(100)),
            output_with_source(DERIVED_OUTPUT_SOURCE, Some(70)),
            30,
            30,
        );
        assert_eq!(outcome, (RecordDisposition::ExactRedelivery, 0));
    }

    /// The derived output is conflicted exactly when reasoning exceeds the
    /// stored inclusive output, so that pair restates too.
    #[test]
    fn conflicted_output_restates_a_stored_inclusive_output_below_reasoning() {
        let outcome = resend(
            output_with_source(INCLUSIVE_OUTPUT_SOURCE, Some(10)),
            output_with_source(&format!("{DERIVED_OUTPUT_SOURCE}:conflicted"), None),
            30,
            30,
        );
        assert_eq!(outcome, (RecordDisposition::ExactRedelivery, 0));
    }

    /// Each case breaks exactly one condition of the equivalence and must
    /// still quarantine.
    #[test]
    fn the_output_equivalence_accepts_nothing_but_the_exact_rewrite() {
        let quarantined = (RecordDisposition::QuarantinedRedelivery, 1);
        // A genuinely different output: stored 100, incoming 90 - 30.
        assert_eq!(
            resend(
                output_with_source(INCLUSIVE_OUTPUT_SOURCE, Some(100)),
                output_with_source(DERIVED_OUTPUT_SOURCE, Some(60)),
                30,
                30,
            ),
            quarantined
        );
        // Reasoning changed between the two deliveries.
        assert_eq!(
            resend(
                output_with_source(INCLUSIVE_OUTPUT_SOURCE, Some(100)),
                output_with_source(DERIVED_OUTPUT_SOURCE, Some(70)),
                30,
                31,
            ),
            quarantined
        );
        // The reverse direction: a stored derived output re-sent inclusive.
        assert_eq!(
            resend(
                output_with_source(DERIVED_OUTPUT_SOURCE, Some(70)),
                output_with_source(INCLUSIVE_OUTPUT_SOURCE, Some(100)),
                30,
                30,
            ),
            quarantined
        );
        // Conflicted although reasoning does not exceed the stored output.
        assert_eq!(
            resend(
                output_with_source(INCLUSIVE_OUTPUT_SOURCE, Some(30)),
                output_with_source(&format!("{DERIVED_OUTPUT_SOURCE}:conflicted"), None),
                30,
                30,
            ),
            quarantined
        );
        // Some other source, even with the right arithmetic.
        assert_eq!(
            resend(
                output_with_source("usage.output", Some(100)),
                output_with_source(DERIVED_OUTPUT_SOURCE, Some(70)),
                30,
                30,
            ),
            quarantined
        );
    }

    fn record_via(
        conn: &mut Connection,
        request: &EventRecordRequest,
        recorded_at_ms: i64,
        delivery: DeliveryPath,
    ) -> EventRecordOutcome {
        let tx = conn.transaction().unwrap();
        let outcome = record_event_in_tx(&tx, request, recorded_at_ms, delivery).unwrap();
        tx.commit().unwrap();
        outcome
    }

    /// A reader re-reading a file it already ingested is our action, not the
    /// producer's, so it must leave no redelivery_log row. The row count is
    /// asserted rather than the disposition, which is ExactRedelivery for both
    /// delivery kinds.
    #[test]
    fn read_exact_redelivery_writes_no_redelivery_log_row() {
        let mut conn = conn();
        let first = request("same", None, vec![measurement("input", Some(1))]);
        record_via(&mut conn, &first, 2_000, DeliveryPath::Read);
        record_via(&mut conn, &first, 3_000, DeliveryPath::Read);
        record_via(&mut conn, &first, 4_000, DeliveryPath::Read);
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(count(&conn, "redelivery_log"), 0);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
    }

    /// A producer that pushes the same record again is still counted, one row
    /// per occurrence. Only read-path arrivals are exempt; suppressing the
    /// row for every arrival, or dropping the log, must fail here.
    #[test]
    fn pushed_exact_redelivery_still_writes_exactly_one_redelivery_log_row() {
        let mut conn = conn();
        let first = request("same", None, vec![measurement("input", Some(1))]);
        record_via(&mut conn, &first, 2_000, DeliveryPath::Pushed);
        let second = record_via(&mut conn, &first, 3_000, DeliveryPath::Pushed);
        assert_eq!(second.disposition, RecordDisposition::ExactRedelivery);
        assert_eq!(count(&conn, "redelivery_log"), 1);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
    }

    /// A different payload under a stored identity is a contradiction in the
    /// data whoever fetched it, so a read arrival is still quarantined.
    #[test]
    fn read_same_identity_different_payload_still_lands_in_rejected_delivery() {
        let mut conn = conn();
        let first = request("same", None, vec![measurement("input", Some(1))]);
        let stored = record_via(&mut conn, &first, 2_000, DeliveryPath::Read);
        let mut conflict = first.clone();
        conflict.measurements[0].quantity_units = Some(9);
        let outcome = record_via(&mut conn, &conflict, 3_000, DeliveryPath::Read);
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        assert_eq!(outcome.event_id, stored.event_id);
        assert_eq!(count(&conn, "rejected_delivery"), 1);
        assert_eq!(count(&conn, "redelivery_log"), 0);
        assert_eq!(count(&conn, "usage_event"), 1);
    }

    fn stored_auth_selection(conn: &Connection, event_id: &str) -> Option<String> {
        conn.query_row(
            "SELECT auth_selection FROM usage_event WHERE event_id=?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// An event stored before `auth_selection` was captured holds NULL. The
    /// same fact read again with its credential id is the same fact, not a
    /// contradiction, and the stored NULL is left as it was.
    #[test]
    fn a_stored_null_auth_selection_accepts_a_value_read_later_without_changing_the_row() {
        let mut conn = conn();
        let first = request("auth", None, vec![measurement("input", Some(1))]);
        assert_eq!(first.auth_selection, None);
        let stored = record_via(&mut conn, &first, 2_000, DeliveryPath::Read);
        let mut later = first.clone();
        later.auth_selection = Some("oauth:xai".into());
        let outcome = record_via(&mut conn, &later, 3_000, DeliveryPath::Read);
        assert_eq!(outcome.disposition, RecordDisposition::ExactRedelivery);
        assert_eq!(outcome.event_id, stored.event_id);
        assert_eq!(stored_auth_selection(&conn, &stored.event_id), None);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
        assert_eq!(count(&conn, "usage_event"), 1);
    }

    /// Only a stored NULL is lenient: a stored credential id contradicted by
    /// a different one, or by none at all, still quarantines.
    #[test]
    fn a_stored_auth_selection_is_contradicted_by_a_different_or_absent_one() {
        for incoming in [Some("apikey:other".to_string()), None] {
            let mut conn = conn();
            let mut first = request("auth", None, vec![measurement("input", Some(1))]);
            first.auth_selection = Some("oauth:xai".into());
            let stored = record_via(&mut conn, &first, 2_000, DeliveryPath::Read);
            let mut later = first.clone();
            later.auth_selection = incoming.clone();
            let outcome = record_via(&mut conn, &later, 3_000, DeliveryPath::Read);
            assert_eq!(
                outcome.disposition,
                RecordDisposition::QuarantinedRedelivery,
                "{incoming:?}"
            );
            assert_eq!(
                stored_auth_selection(&conn, &stored.event_id).as_deref(),
                Some("oauth:xai")
            );
            assert_eq!(count(&conn, "rejected_delivery"), 1, "{incoming:?}");
        }
    }

    #[test]
    fn lineage_absent_event_ingests_and_the_row_records_the_absence() {
        let mut conn = conn();
        let mut lineage_less = request(
            "event-no-lineage",
            None,
            vec![measurement("input", Some(4))],
        );
        lineage_less.producer_lineage = None;
        let outcome = record_event(&mut conn, &lineage_less, 2_000).unwrap();
        assert_eq!(outcome.disposition, RecordDisposition::Recorded);
        assert_eq!(count(&conn, "usage_event"), 1);
        let stored_lineage: Option<String> = conn
            .query_row(
                "SELECT producer_lineage FROM usage_event WHERE event_id=?1",
                [&outcome.event_id],
                |row| row.get(0),
            )
            .unwrap();
        // The stored row keeps the absence visible: an identity minted from
        // less must say so rather than leaving a reader to infer it.
        assert_eq!(stored_lineage, None);
        // The identity matches the lineage-absent key shape, not any string.
        let expected = event_id("plex", None, "event-no-lineage").unwrap();
        assert_eq!(outcome.event_id, expected);
    }

    #[test]
    fn lineage_absent_same_ref_different_payload_quarantines_without_a_second_row() {
        let mut conn = conn();
        let mut first = request("same", None, vec![measurement("input", Some(1))]);
        first.producer_lineage = None;
        let first_outcome = record_event(&mut conn, &first, 2_000).unwrap();
        assert_eq!(first_outcome.disposition, RecordDisposition::Recorded);

        // Two lineage-less events from one producer sharing a reference are
        // indistinguishable from our position; a differing payload under that
        // identity is a conflicting redelivery, not a second event.
        let mut conflict = first.clone();
        conflict.measurements[0].quantity_units = Some(9);
        let outcome = record_event(&mut conn, &conflict, 3_000).unwrap();
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        assert_eq!(outcome.event_id, first_outcome.event_id);
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(count(&conn, "measurement"), 1);
        assert_eq!(count(&conn, "rejected_delivery"), 1);
        let stored_units: i64 = conn
            .query_row(
                "SELECT quantity_units FROM measurement WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&first_outcome.event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_units, 1);
    }

    #[test]
    fn lineage_absent_exact_redelivery_is_a_content_noop() {
        let mut conn = conn();
        let mut first = request("same", None, vec![measurement("input", Some(1))]);
        first.producer_lineage = None;
        record_event(&mut conn, &first, 2_000).unwrap();
        let second = record_event(&mut conn, &first, 3_000).unwrap();
        assert_eq!(second.disposition, RecordDisposition::ExactRedelivery);
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(count(&conn, "redelivery_log"), 1);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
    }

    #[test]
    fn lineage_absent_and_empty_string_lineage_are_distinct_stored_events() {
        let mut conn = conn();
        let mut lineage_less = request("same-ref", None, vec![measurement("input", Some(1))]);
        lineage_less.producer_lineage = None;
        let mut empty_lineage = request("same-ref", None, vec![measurement("input", Some(1))]);
        empty_lineage.producer_lineage = Some(String::new());
        let absent_outcome = record_event(&mut conn, &lineage_less, 2_000).unwrap();
        let present_outcome = record_event(&mut conn, &empty_lineage, 3_000).unwrap();
        // Different key shapes: both land as their own event, neither is a
        // redelivery of the other.
        assert_ne!(absent_outcome.event_id, present_outcome.event_id);
        assert_eq!(count(&conn, "usage_event"), 2);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
    }

    #[test]
    fn same_identity_different_payload_is_quarantined_without_overwrite() {
        let mut conn = conn();
        let first = request("same", None, vec![measurement("input", Some(1))]);
        let event_id = record_event(&mut conn, &first, 2_000).unwrap().event_id;
        let mut conflict = first.clone();
        conflict.measurements[0].quantity_units = Some(9);
        let outcome = record_event(&mut conn, &conflict, 3_000).unwrap();
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        let stored_units: i64 = conn
            .query_row(
                "SELECT quantity_units FROM measurement WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_units, 1);
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(count(&conn, "measurement"), 1);
        assert_eq!(count(&conn, "rejected_delivery"), 1);
        let payload: String = conn
            .query_row(
                "SELECT incoming_payload FROM rejected_delivery",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()["measurements"][0]
                ["quantity_units"],
            9
        );
    }

    #[test]
    fn same_identity_comparison_covers_nonmeasurement_payload() {
        let mut conn = conn();
        let first = request("same-payload", None, vec![measurement("input", Some(1))]);
        record_event(&mut conn, &first, 2_000).unwrap();
        let mut conflict = first.clone();
        conflict.work_class = Some("different-class".into());
        let outcome = record_event(&mut conn, &conflict, 3_000).unwrap();
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        let stored_work_class: String = conn
            .query_row(
                "SELECT work_class FROM usage_event WHERE event_id=?1",
                [&outcome.event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_work_class, "generation");
        assert_eq!(count(&conn, "rejected_delivery"), 1);
    }

    #[test]
    fn absent_explicit_reference_never_settles_by_measurement_heuristic() {
        let mut conn = conn();
        record_event(
            &mut conn,
            &request("target", None, vec![measurement("input", None)]),
            2_000,
        )
        .unwrap();
        record_event(
            &mut conn,
            &request("similar", None, vec![measurement("input", Some(3))]),
            2_001,
        )
        .unwrap();
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn prior_reference_resolution_is_lineage_scoped_without_effect() {
        let mut conn = conn();
        let mut other_lineage_target = request("target", None, vec![measurement("input", None)]);
        other_lineage_target.producer_lineage = Some("lineage-b".into());
        record_event(&mut conn, &other_lineage_target, 2_000).unwrap();
        let referring = record_event(
            &mut conn,
            &request(
                "referring",
                Some("target"),
                vec![measurement("input", Some(3))],
            ),
            2_001,
        )
        .unwrap();
        assert!(referring.waiting_for_target);
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn target_first_settles_the_referring_record_not_the_target() {
        let mut conn = conn();
        let target = record_event(
            &mut conn,
            &request("target", None, vec![measurement("input", None)]),
            2_000,
        )
        .unwrap();
        let referring = record_event(
            &mut conn,
            &request(
                "referring",
                Some("target"),
                vec![measurement("input", Some(3))],
            ),
            2_001,
        )
        .unwrap();
        assert_eq!(referring.transitions_written, 1);
        assert_eq!(state(&mut conn, &referring.event_id), "billed");
        assert_eq!(state(&mut conn, &target.event_id), "unresolved");
    }

    #[test]
    fn referring_first_writes_zero_then_target_settles_all_waiters_in_one_transaction() {
        let mut conn = conn();
        let first = record_event(
            &mut conn,
            &request("ref-1", Some("target"), vec![measurement("input", Some(1))]),
            2_000,
        )
        .unwrap();
        let second = record_event(
            &mut conn,
            &request("ref-2", Some("target"), vec![measurement("input", Some(2))]),
            2_001,
        )
        .unwrap();
        assert!(first.waiting_for_target);
        assert!(second.waiting_for_target);
        assert_eq!(count(&conn, "chargeability_transition"), 0);

        let target = record_event(
            &mut conn,
            &request("target", None, vec![measurement("input", None)]),
            2_002,
        )
        .unwrap();
        assert_eq!(target.transitions_written, 2);
        assert_eq!(state(&mut conn, &first.event_id), "billed");
        assert_eq!(state(&mut conn, &second.event_id), "billed");
        assert_eq!(state(&mut conn, &target.event_id), "unresolved");
        let target_transitions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chargeability_transition WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                [&target.event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(target_transitions, 0);
    }

    #[test]
    fn no_measurements_all_null_and_unknown_dimensions_settle_nothing() {
        let mut conn = conn();
        record_event(
            &mut conn,
            &request("target", None, vec![measurement("input", None)]),
            2_000,
        )
        .unwrap();
        for (producer_ref, measurements) in [
            ("none", vec![]),
            ("null", vec![measurement("input", None)]),
            ("other", vec![measurement("output", Some(2))]),
        ] {
            let outcome = record_event(
                &mut conn,
                &request(producer_ref, Some("target"), measurements),
                2_001,
            )
            .unwrap();
            assert_eq!(outcome.transitions_written, 0, "{producer_ref}");
        }
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn value_against_non_null_target_quarantines_distinct_event_without_settlement() {
        let mut conn = conn();
        let target = record_event(
            &mut conn,
            &request("target", None, vec![measurement("input", Some(1))]),
            2_000,
        )
        .unwrap();
        let referring = record_event(
            &mut conn,
            &request(
                "referring",
                Some("target"),
                vec![measurement("input", Some(2))],
            ),
            2_001,
        )
        .unwrap();
        assert_eq!(referring.transitions_written, 0);
        assert_eq!(referring.quarantine_evidence_written, 1);
        assert_eq!(count(&conn, "usage_event"), 2);
        assert_eq!(count(&conn, "measurement"), 2);
        assert_eq!(count(&conn, "chargeability_transition"), 0);
        let evidence: (String, String, String, String) = conn
            .query_row(
                "SELECT (SELECT event_id FROM usage_event WHERE event_key=quarantine_evidence.event_key),
                        (SELECT event_id FROM usage_event WHERE event_key=target_event_key),stored_quantity,incoming_quantity
                 FROM quarantine_evidence",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(evidence.0, referring.event_id);
        assert_eq!(evidence.1, target.event_id);
        assert_eq!(evidence.2, r#"{"exponent":0,"units":1}"#);
        assert_eq!(evidence.3, r#"{"exponent":0,"units":2}"#);
    }

    #[test]
    fn one_non_null_target_conflict_blocks_otherwise_qualifying_settlement() {
        let mut conn = conn();
        record_event(
            &mut conn,
            &request(
                "target",
                None,
                vec![measurement("input", Some(1)), measurement("output", None)],
            ),
            2_000,
        )
        .unwrap();
        let outcome = record_event(
            &mut conn,
            &request(
                "referring",
                Some("target"),
                vec![
                    measurement("input", Some(2)),
                    measurement("output", Some(3)),
                ],
            ),
            2_001,
        )
        .unwrap();
        assert_eq!(outcome.transitions_written, 0);
        assert_eq!(outcome.quarantine_evidence_written, 1);
        assert_eq!(count(&conn, "chargeability_transition"), 0);
        assert_eq!(count(&conn, "quarantine_evidence"), 1);
    }

    #[test]
    fn completing_transaction_failure_rolls_back_target_and_all_transitions() {
        let mut conn = conn();
        let first = record_event(
            &mut conn,
            &request("ref-1", Some("target"), vec![measurement("input", Some(1))]),
            2_000,
        )
        .unwrap();
        let second = record_event(
            &mut conn,
            &request("ref-2", Some("target"), vec![measurement("input", Some(2))]),
            2_001,
        )
        .unwrap();
        let target_id = event_id("plex", Some("lineage-a"), "target").unwrap();
        conn.execute_batch(&format!(
            "CREATE TEMP TRIGGER fail_after_transitions
             BEFORE INSERT ON measurement WHEN NEW.event_key=(SELECT event_key FROM usage_event WHERE event_id='{target_id}')
             BEGIN
                 SELECT CASE
                     WHEN (SELECT COUNT(*) FROM chargeability_transition)=2
                     THEN RAISE(ABORT,'forced after transitions')
                     ELSE RAISE(ABORT,'transitions were not written first')
                 END;
             END;"
        ))
        .unwrap();
        let error = record_event(
            &mut conn,
            &request("target", None, vec![measurement("input", None)]),
            2_002,
        )
        .unwrap_err();
        assert!(
            matches!(error, IngestError::Store(ref error) if error.to_string().contains("forced after transitions"))
        );
        assert_eq!(count(&conn, "chargeability_transition"), 0);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM usage_event WHERE event_id=?1",
                [&target_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
        assert_eq!(state(&mut conn, &first.event_id), "unresolved");
        assert_eq!(state(&mut conn, &second.event_id), "unresolved");
    }

    #[test]
    fn operator_cannot_mark_candidate_billed_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("event", None, vec![]), 2_000).unwrap();
        establish_currency(&conn, &event.event_id, "USD");
        let result = settle_event(
            &mut conn,
            &EventSettleRequest {
                event_id: event.event_id,
                to_state: ChargeabilityState::Billed,
                reason: "operator assertion".into(),
            },
            3_000,
        );
        assert!(matches!(
            result,
            Err(IngestError::Refused(Refusal::OperatorCannotBill))
        ));
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn operator_settlement_refuses_null_currency_target_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("event", None, vec![]), 2_000).unwrap();
        let result = settle_event(
            &mut conn,
            &EventSettleRequest {
                event_id: event.event_id,
                to_state: ChargeabilityState::WrittenOff,
                reason: "acknowledged".into(),
            },
            3_000,
        );
        assert!(matches!(
            result,
            Err(IngestError::Refused(Refusal::UnpricedTarget))
        ));
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn settlement_is_terminal_and_current_state_comes_from_history() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("event", None, vec![]), 2_000).unwrap();
        establish_currency(&conn, &event.event_id, "USD");
        settle_event(
            &mut conn,
            &EventSettleRequest {
                event_id: event.event_id.clone(),
                to_state: ChargeabilityState::Unbillable,
                reason: "will never bill".into(),
            },
            3_000,
        )
        .unwrap();
        let second = settle_event(
            &mut conn,
            &EventSettleRequest {
                event_id: event.event_id.clone(),
                to_state: ChargeabilityState::WrittenOff,
                reason: "second opinion".into(),
            },
            3_001,
        );
        assert!(matches!(
            second,
            Err(IngestError::Refused(Refusal::AlreadySettled))
        ));
        assert_eq!(state(&mut conn, &event.event_id), "unbillable");
        assert_eq!(count(&conn, "chargeability_transition"), 1);
        let projected_columns: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('usage_event') WHERE name='chargeability_state'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(projected_columns, 0);
    }

    #[test]
    fn payload_supplied_operator_origin_currency_and_event_id_are_malformed_without_effect() {
        let conn = conn();
        let event_payload = json!({
            "producer":"plex", "producer_lineage":"lineage", "producer_ref":"ref",
            "currency":"USD", "event_id":"caller-id", "operator":"mallory", "measurements":[]
        });
        assert!(serde_json::from_value::<EventRecordRequest>(event_payload).is_err());
        let adjustment_payload = json!({
            "event_id":"event", "currency":"USD", "amount_nanos":1,
            "reason":"refund", "occurred_at_ms":1, "origin":"statement", "operator":"mallory"
        });
        assert!(serde_json::from_value::<AdjustmentRecordRequest>(adjustment_payload).is_err());
        assert_eq!(count(&conn, "usage_event"), 0);
        assert_eq!(count(&conn, "adjustment"), 0);
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn adjustment_against_undated_event_is_refused_without_rows_or_transitions() {
        let mut conn = conn();
        let mut undated = request("undated", None, vec![]);
        undated.occurred_at_ms = None;
        let event = record_event(&mut conn, &undated, 2_000).unwrap();
        establish_currency(&conn, &event.event_id, "USD");
        let result = record_correction(
            &mut conn,
            &AdjustmentRecordRequest {
                event_id: event.event_id,
                currency: "USD".into(),
                amount_nanos: -10,
                reason: AdjustmentReason::Refund,
                occurred_at_ms: 1_000,
                note: None,
            },
            3_000,
        );
        assert!(matches!(
            result,
            Err(IngestError::Refused(Refusal::UndatedTarget))
        ));
        assert_eq!(count(&conn, "adjustment"), 0);
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn adjustment_records_signed_amount_and_derives_origin_from_write_path() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("dated", None, vec![]), 2_000).unwrap();
        establish_currency(&conn, &event.event_id, "USD");
        let outcome = record_correction(
            &mut conn,
            &AdjustmentRecordRequest {
                event_id: event.event_id,
                currency: "USD".into(),
                amount_nanos: -25,
                reason: AdjustmentReason::Refund,
                occurred_at_ms: 1_500,
                note: Some("vendor refund".into()),
            },
            3_000,
        )
        .unwrap();
        assert_eq!(outcome.origin, AdjustmentOrigin::CorrectionIngest);
        let stored: (i64, String, Option<String>) = conn
            .query_row(
                "SELECT amount_nanos,origin,operator FROM adjustment WHERE adjustment_id=?1",
                [&outcome.adjustment_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, (-25, "correction_ingest".into(), None));
    }

    #[test]
    fn different_currency_adjustment_is_refused_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("dated", None, vec![]), 2_000).unwrap();
        establish_currency(&conn, &event.event_id, "JPY");
        let result = record_correction(
            &mut conn,
            &AdjustmentRecordRequest {
                event_id: event.event_id,
                currency: "USD".into(),
                amount_nanos: 1,
                reason: AdjustmentReason::Goodwill,
                occurred_at_ms: 1_000,
                note: None,
            },
            3_000,
        );
        assert!(matches!(
            result,
            Err(IngestError::Refused(Refusal::CurrencyMismatch))
        ));
        assert_eq!(count(&conn, "adjustment"), 0);
    }

    #[test]
    fn write_off_waits_for_pricing_total_without_adjustment_or_transition() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("dated", None, vec![]), 2_000).unwrap();
        establish_currency(&conn, &event.event_id, "USD");
        let result = record_correction(
            &mut conn,
            &AdjustmentRecordRequest {
                event_id: event.event_id,
                currency: "USD".into(),
                amount_nanos: -100,
                reason: AdjustmentReason::WriteOff,
                occurred_at_ms: 1_000,
                note: None,
            },
            3_000,
        );
        assert!(matches!(
            result,
            Err(IngestError::Refused(Refusal::WriteOffRequiresPricing))
        ));
        assert_eq!(count(&conn, "adjustment"), 0);
        assert_eq!(count(&conn, "chargeability_transition"), 0);
    }

    #[test]
    fn malformed_measurements_are_refused_without_any_stored_state() {
        let mut conn = conn();
        let cases = [
            MeasurementInput {
                quantity_units: Some(-1),
                ..measurement("negative", Some(1))
            },
            MeasurementInput {
                quantity_exponent: Some(-10),
                ..measurement("exponent", Some(1))
            },
            MeasurementInput {
                quantity_exponent: None,
                ..measurement("half", Some(1))
            },
        ];
        for (index, invalid) in cases.into_iter().enumerate() {
            let result = record_event(
                &mut conn,
                &request(&format!("invalid-{index}"), None, vec![invalid]),
                2_000,
            );
            assert!(matches!(
                result,
                Err(IngestError::Refused(Refusal::Malformed))
            ));
            assert_eq!(count(&conn, "usage_event"), 0);
            assert_eq!(count(&conn, "measurement"), 0);
            assert_eq!(count(&conn, "chargeability_transition"), 0);
        }
    }

    #[test]
    fn quarantine_evidence_constraints_reject_invalid_rows_without_effect() {
        let mut conn = conn();
        let first = record_event(&mut conn, &request("first", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        let second = record_event(&mut conn, &request("second", None, vec![]), 2_001)
            .unwrap()
            .event_id;
        for statement in [
            format!("INSERT INTO quarantine_evidence VALUES (NULL,(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),NULL,'r','u','{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),NULL,'u','{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r',NULL,'{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u',NULL,'{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','{{}}',NULL,1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','{{}}','{{}}',NULL)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{first}'),'r','u','{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='missing'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='missing'),'r','u','{{}}','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','bad','{{}}',1)"),
            format!("INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','{{}}','{{}}',-1)"),
        ] {
            assert!(conn.execute(&statement, []).is_err());
            assert_eq!(count(&conn, "quarantine_evidence"), 0);
        }
    }

    #[test]
    fn quarantine_evidence_identity_rejects_duplicate_without_effect() {
        let mut conn = conn();
        let first = record_event(&mut conn, &request("first", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        let second = record_event(&mut conn, &request("second", None, vec![]), 2_001)
            .unwrap()
            .event_id;
        let statement = format!(
            "INSERT INTO quarantine_evidence VALUES ((SELECT event_key FROM usage_event WHERE event_id='{first}'),(SELECT event_key FROM usage_event WHERE event_id='{second}'),'r','u','{{}}','{{}}',1)"
        );
        conn.execute(&statement, []).unwrap();
        assert!(conn.execute(&statement, []).is_err());
        assert_eq!(count(&conn, "quarantine_evidence"), 1);
    }

    #[test]
    fn rejected_delivery_constraints_reject_invalid_rows_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("stored", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        // Ruling 142 makes producer_lineage nullable: a lineage-absent
        // delivery is valid quarantine evidence, so the NULL-lineage case
        // that once stood here asserted a property the ruling deliberately
        // removed. Acceptance is covered by
        // rejected_delivery_accepts_absent_lineage below.
        for statement in [
            format!("INSERT INTO rejected_delivery VALUES (NULL,'l','r','{{}}',(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            format!("INSERT INTO rejected_delivery VALUES ('p','l',NULL,'{{}}',(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            format!("INSERT INTO rejected_delivery VALUES ('p','l','r',NULL,(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            "INSERT INTO rejected_delivery VALUES ('p','l','r','{}',NULL,1)".into(),
            format!("INSERT INTO rejected_delivery VALUES ('p','l','r','{{}}',(SELECT event_key FROM usage_event WHERE event_id='{event}'),NULL)"),
            format!("INSERT INTO rejected_delivery VALUES ('p','l','r','bad',(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            "INSERT INTO rejected_delivery VALUES ('p','l','r','{}',(SELECT event_key FROM usage_event WHERE event_id='missing'),1)".into(),
            format!("INSERT INTO rejected_delivery VALUES ('p','l','r','{{}}',(SELECT event_key FROM usage_event WHERE event_id='{event}'),-1)"),
        ] {
            assert!(conn.execute(&statement, []).is_err());
            assert_eq!(count(&conn, "rejected_delivery"), 0);
        }
    }

    #[test]
    fn redelivery_log_constraints_reject_invalid_rows_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("stored", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        // Ruling 142 makes producer_lineage nullable: a lineage-absent
        // occurrence is a valid redelivery record, so the NULL-lineage case
        // that once stood here asserted a property the ruling deliberately
        // removed. Acceptance is covered by redelivery_log_accepts_absent_lineage
        // below.
        for statement in [
            format!("INSERT INTO redelivery_log VALUES (NULL,'l','r',(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            format!("INSERT INTO redelivery_log VALUES ('p','l',NULL,(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            "INSERT INTO redelivery_log VALUES ('p','l','r',NULL,1)".into(),
            format!("INSERT INTO redelivery_log VALUES ('p','l','r',(SELECT event_key FROM usage_event WHERE event_id='{event}'),NULL)"),
            "INSERT INTO redelivery_log VALUES ('p','l','r',(SELECT event_key FROM usage_event WHERE event_id='missing'),1)".into(),
            format!("INSERT INTO redelivery_log VALUES ('p','l','r',(SELECT event_key FROM usage_event WHERE event_id='{event}'),-1)"),
        ] {
            assert!(conn.execute(&statement, []).is_err());
            assert_eq!(count(&conn, "redelivery_log"), 0);
        }
    }

    #[test]
    fn rejected_delivery_accepts_absent_lineage() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("stored", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        // Ruling 142: a conflicting lineage-less redelivery must be
        // recordable in the table built to hold it.
        conn.execute(
            &format!("INSERT INTO rejected_delivery VALUES ('p',NULL,'r','{{}}',(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            [],
        )
        .unwrap();
        assert_eq!(count(&conn, "rejected_delivery"), 1);
    }

    #[test]
    fn redelivery_log_accepts_absent_lineage() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("stored", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        // Ruling 142: an exact lineage-less redelivery must be recordable as
        // an occurrence of the stored event.
        conn.execute(
            &format!("INSERT INTO redelivery_log VALUES ('p',NULL,'r',(SELECT event_key FROM usage_event WHERE event_id='{event}'),1)"),
            [],
        )
        .unwrap();
        assert_eq!(count(&conn, "redelivery_log"), 1);
    }

    #[test]
    fn adjustment_constraints_reject_invalid_rows_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("stored", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        establish_currency(&conn, &event, "USD");
        for statement in [
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES (NULL,(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',1,1,'operator',NULL,NULL)"),
            "INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',NULL,'USD',1,'refund',1,1,'operator',NULL,NULL)".into(),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),NULL,1,'refund',1,1,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',NULL,'refund',1,1,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,NULL,1,1,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',NULL,1,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',1,NULL,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',1,1,NULL,NULL,NULL)"),
            "INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='missing'),'USD',1,'refund',1,1,'operator',NULL,NULL)".into(),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'unknown',1,1,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',1,1,'unknown',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1.5,'refund',1,1,'operator',NULL,NULL)"),
            format!("INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('a',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',-1,1,'operator',NULL,NULL)"),
        ] {
            assert!(conn.execute(&statement, []).is_err());
            assert_eq!(count(&conn, "adjustment"), 0);
        }
    }

    #[test]
    fn adjustment_primary_key_rejects_duplicate_without_effect() {
        let mut conn = conn();
        let event = record_event(&mut conn, &request("stored", None, vec![]), 2_000)
            .unwrap()
            .event_id;
        establish_currency(&conn, &event, "USD");
        let statement = format!(
            "INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,occurred_at_ms,recorded_at_ms,origin,operator,note) VALUES ('same',(SELECT event_key FROM usage_event WHERE event_id='{event}'),'USD',1,'refund',1,1,'operator',NULL,NULL)"
        );
        conn.execute(&statement, []).unwrap();
        assert!(conn.execute(&statement, []).is_err());
        assert_eq!(count(&conn, "adjustment"), 1);
    }

    fn with_provenance(
        mut request: EventRecordRequest,
        mapping_version: Option<u32>,
        overrides: Option<&str>,
    ) -> EventRecordRequest {
        request.charge_basis_mapping_version = mapping_version;
        request.charge_basis_overrides = overrides.map(str::to_string);
        request
    }

    fn stored_provenance(conn: &Connection, event_id: &str) -> (Option<i64>, Option<String>) {
        conn.query_row(
            "SELECT charge_basis_mapping_version,charge_basis_overrides
             FROM usage_event WHERE event_id=?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    /// A fact stored before the provenance fields were decoded holds NULL in
    /// both. Reading the same fact again with the fields present is the same
    /// fact, not a changed payload, so it must not be quarantined. The stored
    /// NULLs stay NULL: the row is immutable.
    #[test]
    fn provenance_stored_null_then_reread_present_is_not_rejected() {
        let mut conn = conn();
        let base = request("broca-fact", None, vec![measurement("input", Some(4))]);
        let first =
            record_event(&mut conn, &with_provenance(base.clone(), None, None), 2_000).unwrap();
        let reread = record_event(
            &mut conn,
            &with_provenance(base, Some(4), Some("none")),
            3_000,
        )
        .unwrap();
        assert_eq!(reread.disposition, RecordDisposition::ExactRedelivery);
        assert_eq!(reread.event_id, first.event_id);
        assert_eq!(count(&conn, "rejected_delivery"), 0);
        assert_eq!(count(&conn, "usage_event"), 1);
        assert_eq!(stored_provenance(&conn, &first.event_id), (None, None));
    }

    /// A stored provenance value is what the producer said. A different value
    /// for the same fact is a contradiction and is quarantined, for each field
    /// independently, and an incoming absence against a stored value is
    /// different too.
    #[test]
    fn provenance_stored_value_then_different_value_is_rejected() {
        let mut conn = conn();
        let base = request("broca-fact", None, vec![measurement("input", Some(4))]);
        let stored = with_provenance(base.clone(), Some(4), Some("none"));
        let first = record_event(&mut conn, &stored, 2_000).unwrap();
        assert_eq!(
            stored_provenance(&conn, &first.event_id),
            (Some(4), Some("none".to_string()))
        );

        let digest = "ab".repeat(32);
        let changed_overrides = with_provenance(base.clone(), Some(4), Some(&digest));
        let outcome = record_event(&mut conn, &changed_overrides, 3_000).unwrap();
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        assert_eq!(count(&conn, "rejected_delivery"), 1);
        let evidence: String = conn
            .query_row(
                "SELECT incoming_payload FROM rejected_delivery",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            evidence.contains(&digest),
            "the quarantined payload must carry the contradicting digest: {evidence}"
        );

        let changed_version = with_provenance(base.clone(), Some(5), Some("none"));
        let outcome = record_event(&mut conn, &changed_version, 4_000).unwrap();
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        assert_eq!(count(&conn, "rejected_delivery"), 2);

        let absent_overrides = with_provenance(base.clone(), Some(4), None);
        let outcome = record_event(&mut conn, &absent_overrides, 5_000).unwrap();
        assert_eq!(
            outcome.disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        assert_eq!(count(&conn, "rejected_delivery"), 3);

        let same = record_event(&mut conn, &stored, 6_000).unwrap();
        assert_eq!(same.disposition, RecordDisposition::ExactRedelivery);
        assert_eq!(count(&conn, "rejected_delivery"), 3);
        assert_eq!(
            stored_provenance(&conn, &first.event_id),
            (Some(4), Some("none".to_string()))
        );
    }

    /// The provenance fields are not part of event identity: the same
    /// producer reference with or without them names the same event.
    #[test]
    fn provenance_does_not_enter_event_identity() {
        let base = request("broca-fact", None, Vec::new());
        let digest = "cd".repeat(32);
        let mut ids = BTreeSet::new();
        for (version, overrides) in [
            (None, None),
            (Some(1), Some("none")),
            (Some(4), Some(digest.as_str())),
        ] {
            let mut conn = conn();
            let outcome = record_event(
                &mut conn,
                &with_provenance(base.clone(), version, overrides),
                2_000,
            )
            .unwrap();
            ids.insert(outcome.event_id);
        }
        assert_eq!(ids.len(), 1);
    }

    /// A pushed `spend.event.ingest` payload cannot claim BROCA provenance:
    /// the fields are not part of the pushed request shape.
    #[test]
    fn pushed_request_naming_provenance_is_refused() {
        let payload = json!({
            "producer": "plex", "producer_ref": "r", "charge_basis": "metered_api",
            "charge_basis_overrides": "none"
        });
        assert!(serde_json::from_value::<EventRecordRequest>(payload).is_err());
        let payload = json!({
            "producer": "plex", "producer_ref": "r", "charge_basis": "metered_api",
            "charge_basis_mapping_version": 4
        });
        assert!(serde_json::from_value::<EventRecordRequest>(payload).is_err());
    }
}
