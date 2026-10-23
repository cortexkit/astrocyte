//! BROCA spend-segment facts mapped to producer-neutral events and
//! measurements. The facts are read from BROCA's store (see `broca_source`)
//! in the shape BROCA's retired `usage.export` operation served them.

use astrocyte_core::fact::ChargeBasis;
use astrocyte_core::ingest::{EventRecordRequest, MeasurementInput};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WireSpendSegmentFact {
    /// The row's `export_seq` column. The stored segment JSON does not carry
    /// it; the store reader adds it at the top level, where `usage.export` put
    /// it. It stays a required field so a segment decodes only in the shape
    /// the pull decoded, but nothing after decoding reads it: the cursor comes
    /// from the column itself.
    #[expect(
        dead_code,
        reason = "required for decoding; the cursor is taken from the store column"
    )]
    pub export_seq: u64,
    pub fact_id: String,
    pub run_id: String,
    pub session: WireBindIdentity,
    pub provider: String,
    pub model: String,
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
    pub usage: WireUsage,
    pub occurred_at_ms: Option<u64>,
    pub terminal_reason: Option<String>,
    /// The producer's classification of the run, carried verbatim. BROCA
    /// freezes it at admission and OMITS the key when the run was not
    /// classified, so the three wire states map to three stored states:
    /// absent -> NULL, a non-empty string -> that exact string, and "" -> "".
    /// An empty string is a producer defaulting instead of omitting; it is still
    /// what was billed, so it is stored as sent rather than refused or turned
    /// into NULL. No trimming, case folding or vocabulary mapping happens here:
    /// other producers store their own agent names in the same column, and
    /// reconciling vocabularies is a read-side decision.
    ///
    /// A JSON `null` decodes to NULL, the same as an absent key. BROCA never
    /// sends one, and null carries no string to store, so recording anything
    /// other than "not classified" would be inventing a value.
    #[serde(default)]
    pub work_class: Option<String>,
    /// The version of BROCA's SHIPPED charge-basis table that decided
    /// `charge_basis`, carried verbatim. BROCA stamps it on every fact; it is
    /// still decoded as optional so a fact missing it is stored with NULL
    /// instead of failing the whole export page and stalling ingest.
    #[serde(default)]
    pub charge_basis_mapping_version: Option<u32>,
    /// Which local override rows BROCA applied on top of the shipped table,
    /// carried verbatim. Three wire states map to three stored states:
    /// absent (a fact written before BROCA emitted the field) -> NULL,
    /// "none" (the shipped table applied unchanged) -> "none", and a hex
    /// digest of the override rows -> that digest.
    ///
    /// This MUST stay `Option<String>`. Decoding into a plain `String` with a
    /// "none" default would claim the shipped table decided a fact the producer
    /// said nothing about, erasing the absent state. The digest's shape is not
    /// validated: refusing a malformed one would refuse the whole page, so we
    /// store what the producer sent, as for the other provenance fields.
    #[serde(default)]
    pub charge_basis_overrides: Option<String>,
    /// The version of BROCA's usage interpretation that normalized `usage`.
    /// BROCA stamps it only on facts whose provider needed the separate-
    /// reasoning fix (so far only xAI, at version 2) and omits it everywhere
    /// else, so absence alone does not mean "old". It is an input to
    /// [`map_fact`] and is not stored: see [`reasoning_is_inside_output`].
    #[serde(default)]
    pub usage_policy_version: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WireBindIdentity {
    pub project_root: String,
    pub harness: String,
    pub session: String,
}

/// The five token counts are `Option` ON PURPOSE, and the absence is the
/// point. OpenAI, Gemini, DeepSeek and the subscription lanes have NO
/// cache-write concept at all — there is no such field on their wire — so a
/// non-`Option` field with `#[serde(default)]` turns "this provider does not
/// measure that" into "a measurement was taken and came back zero". Those are
/// different propositions, and the second is the majority of this ledger's
/// traffic: 21,392 openai cache_write rows, 5,128 google, 3,263 ollama-cloud
/// (both cache_write AND reasoning), against anthropic as the only provider
/// with a positive value anywhere. Measured 2026-09-19 with 0 NULL quantities
/// across 197,404 measurements — a three-state column whose third state had
/// never occurred.
///
/// serde maps a missing key to `None` for `Option` without an attribute; the
/// test below pins that rather than trusting it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WireUsage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

pub use astrocyte_core::ingest::DERIVED_OUTPUT_SOURCE;

/// Whether BROCA's `output_tokens` already contains `reasoning_tokens` for
/// this fact.
///
/// BROCA's canonical form makes reasoning a subset of output, but the rule
/// has a start date. From broca-protocol `usage.rs`: "`reasoning_tokens >
/// output_tokens` is arithmetically impossible under subset semantics, and
/// broca's own export lane contains rows that violate it: xAI was read as a
/// subset before its sibling shape was found by live probe, so 324 of 487
/// pre-fix xAI facts are lopsided enough to expose it. Facts written after the
/// fix carry `usage_policy_version`." and "Scoped to the affected provider it
/// is exact: zero xAI rows carry an absent version after the first stamped
/// one, so `provider = 'xai' AND version IS NULL` selects precisely the
/// pre-fix population and nothing else."
///
/// So a pre-fix xAI fact's output is visible output only, and subtracting
/// reasoning from it again would undercount. The predicate follows BROCA's
/// documented query for the affected facts (`usage_policy_version IS NULL OR
/// usage_policy_version < 2`); BROCA only ever writes 2 or nothing, so the
/// `< 2` arm matches no real fact and only keeps the two definitions equal.
fn reasoning_is_inside_output(provider: &str, usage_policy_version: Option<u32>) -> bool {
    let pre_fix_xai =
        provider == "xai" && !matches!(usage_policy_version, Some(version) if version >= 2);
    !pre_fix_xai
}

/// The output measurement in disjoint form, so output and reasoning can be
/// summed without counting reasoning twice.
///
/// - Output absent: no measurement, as for every other absent count.
/// - Reasoning absent: the full output, under its original source. BROCA
///   says an absent reasoning count is "a FLOOR on reasoning spend across a
///   mixed fleet, never a measurement of it" (Bedrock Converse reports none,
///   older Anthropic responses omit it). With no reasoning measurement stored
///   nothing is counted twice, and output is what the call was billed at the
///   output rate; marking it underivable would leave every such call
///   unpriceable.
/// - Pre-fix xAI: output already excludes reasoning, so it is stored as sent.
/// - Otherwise: `output - reasoning`. A negative difference contradicts the
///   subset rule, so the measurement is kept with NO quantity and a
///   `:conflicted` source: the call still lands and counts, and pricing
///   refuses the role instead of pricing a clamped guess.
fn output_measurement(wire: &WireSpendSegmentFact) -> Result<Option<MeasurementInput>, String> {
    let Some(output) = wire.usage.output_tokens else {
        return Ok(None);
    };
    let (source, quantity) = match wire.usage.reasoning_tokens {
        Some(reasoning)
            if reasoning_is_inside_output(&wire.provider, wire.usage_policy_version) =>
        {
            match output.checked_sub(reasoning) {
                Some(visible) => (DERIVED_OUTPUT_SOURCE.to_string(), Some(visible)),
                None => (format!("{DERIVED_OUTPUT_SOURCE}:conflicted"), None),
            }
        }
        _ => ("usage.output_tokens".to_string(), Some(output)),
    };
    let quantity_units = quantity
        .map(|units| {
            i64::try_from(units)
                .map_err(|_| "output_tokens exceeds stored integer range".to_string())
        })
        .transpose()?;
    Ok(Some(MeasurementInput {
        role: "output".into(),
        unit: "token".into(),
        kind: "billed".into(),
        source,
        quantity_exponent: quantity_units.map(|_| 0),
        quantity_units,
    }))
}

/// Every reported count as a measurement, in the order given.
fn reported_token_measurements<'a>(
    counts: impl IntoIterator<Item = (&'a str, &'a str, Option<u64>)>,
) -> Result<Vec<MeasurementInput>, String> {
    counts
        .into_iter()
        // An absent field OMITS the measurement rather than recording one with a
        // null quantity. The measurement set is the set of things measured, so a
        // concept the provider does not have is not a reading of unknown size — it
        // is not a reading. A variable count is already legal and already occurs
        // (recorded anthropic events that split cache writes into 5m and 1h
        // tiers carry six), and this
        // is what lets an openai event be fully priced: three or four measurements
        // that all price, instead of five where one can never price and drags the
        // event's verdict to unpriced by the weakest-part rule.
        //
        // A null quantity stays meaningful for producers that report a measurement
        // whose size they do not know; the pricing path already refuses it as
        // `missing_quantity` per measurement rather than pricing it as zero.
        .filter_map(|(role, field, quantity)| quantity.map(|units| (role, field, units)))
        .map(|(role, field, quantity)| token_measurement(role, field, quantity))
        .collect()
}

pub fn map_fact(wire: WireSpendSegmentFact) -> Result<EventRecordRequest, String> {
    let output = output_measurement(&wire)?;
    let mut measurements = reported_token_measurements([
        ("input", "input_tokens", wire.usage.input_tokens),
        (
            "cache_read",
            "cached_input_tokens",
            wire.usage.cached_input_tokens,
        ),
        (
            "cache_write",
            "cache_write_tokens",
            wire.usage.cache_write_tokens,
        ),
        ("reasoning", "reasoning_tokens", wire.usage.reasoning_tokens),
    ])?;
    // Output goes back in its original position, before reasoning, so the
    // measurement order is unchanged from the verbatim mapping.
    let reasoning_at = measurements.len() - usize::from(wire.usage.reasoning_tokens.is_some());
    if let Some(output) = output {
        measurements.insert(reasoning_at, output);
    }
    Ok(EventRecordRequest {
        producer: BROCA_PRODUCER.into(),
        // BROCA's content-hash references survive restarts. Adding an instance
        // lineage would turn a re-export of the same charge into a new event.
        producer_lineage: None,
        producer_ref: wire.fact_id,
        prior_event_ref: None,
        harness: Some(wire.session.harness),
        project_root: Some(wire.session.project_root),
        session: Some(wire.session.session),
        run_ref: Some(wire.run_id),
        flow_ref: None,
        work_class: wire.work_class,
        occurred_at_ms: wire.occurred_at_ms.map(stored_occurred_at).transpose()?,
        emitted_at_ms: None,
        terminal_status: wire.terminal_reason,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider: Some(wire.provider),
        model: Some(wire.model),
        account_ref: wire.account_ref,
        auth_selection: wire.auth_selection,
        charge_basis: wire.charge_basis,
        charge_basis_mapping_version: wire.charge_basis_mapping_version,
        charge_basis_overrides: wire.charge_basis_overrides,
        measurements,
    })
}

/// The producer name every BROCA fact is recorded under, whatever its kind.
const BROCA_PRODUCER: &str = "broca";

/// One reported token count as a billed measurement, sourced from the wire
/// field it came from. Shared by both kinds of BROCA fact so the same field
/// is stored the same way whichever kind carried it.
fn token_measurement(role: &str, field: &str, quantity: u64) -> Result<MeasurementInput, String> {
    Ok(MeasurementInput {
        role: role.into(),
        unit: "token".into(),
        kind: "billed".into(),
        source: format!("usage.{field}"),
        quantity_units: Some(
            i64::try_from(quantity).map_err(|_| format!("{field} exceeds stored integer range"))?,
        ),
        quantity_exponent: Some(0),
    })
}

fn stored_occurred_at(ms: u64) -> Result<i64, String> {
    i64::try_from(ms).map_err(|_| "occurred_at_ms exceeds stored integer range".to_string())
}

/// The `kind` a BROCA batch fact carries in its segment. A session fact
/// carries no `kind` at all.
pub(crate) const SYSTEM_ONE_KIND: &str = "system_one";

/// A usage fact from one of BROCA's batch decision or extraction models.
/// These answer a batch of items with no session of their own, so the fact
/// names its batch and item instead, and the run OR the flow the call was
/// made in. Shared members carry the same names and nesting as on a session
/// fact. The segment carries no cost or rate members.
///
/// Members not named here are ignored, as on a session fact, so a member
/// BROCA adds later does not set the whole fact aside.
#[derive(Debug, Clone, Deserialize)]
pub struct WireSystemOneFact {
    pub fact_id: String,
    /// The batch and the item's position in it. Required so a segment
    /// decodes only in the agreed shape; neither is stored, because the
    /// event's identity is `fact_id` alone.
    #[expect(
        dead_code,
        reason = "required for decoding; the event's identity is fact_id"
    )]
    pub batch_id: String,
    #[expect(
        dead_code,
        reason = "required for decoding; the event's identity is fact_id"
    )]
    pub item_index: u64,
    /// Exactly one of these two is present; [`map_system_one_fact`] refuses
    /// a fact with both or neither.
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub flow_id: Option<String>,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub account_ref: Option<String>,
    #[serde(default)]
    pub auth_selection: Option<String>,
    pub charge_basis: ChargeBasis,
    pub occurred_at_ms: u64,
    /// Carried verbatim as on a session fact: absent or null is stored as
    /// NULL, and a string is stored unchanged.
    #[serde(default)]
    pub work_class: Option<String>,
    pub usage: WireSystemOneUsage,
}

/// A batch fact's token counts. Each is absent when the provider did not
/// report it, never zero, and an absent count produces no measurement.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WireSystemOneUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// Maps a BROCA batch fact to an event. Identity is the same as for a session
/// fact (producer `broca`, no lineage, `fact_id` as the reference), so a
/// re-read of the fact is an exact redelivery. The fact has no session,
/// harness or project, and those stay NULL rather than being invented.
pub fn map_system_one_fact(wire: WireSystemOneFact) -> Result<EventRecordRequest, String> {
    let (run_ref, flow_ref) = match (wire.run_id, wire.flow_id) {
        (Some(run_id), None) => (Some(run_id), None),
        (None, Some(flow_id)) => (None, Some(flow_id)),
        (Some(_), Some(_)) => {
            return Err(format!(
                "system_one fact {} carries both run_id and flow_id",
                wire.fact_id
            ))
        }
        (None, None) => {
            return Err(format!(
                "system_one fact {} carries neither run_id nor flow_id",
                wire.fact_id
            ))
        }
    };
    let measurements = reported_token_measurements([
        ("input", "input_tokens", wire.usage.input_tokens),
        ("output", "output_tokens", wire.usage.output_tokens),
    ])?;
    Ok(EventRecordRequest {
        producer: BROCA_PRODUCER.into(),
        producer_lineage: None,
        producer_ref: wire.fact_id,
        prior_event_ref: None,
        harness: None,
        project_root: None,
        session: None,
        run_ref,
        flow_ref,
        work_class: wire.work_class,
        occurred_at_ms: Some(stored_occurred_at(wire.occurred_at_ms)?),
        emitted_at_ms: None,
        terminal_status: None,
        attempt_ordinal: None,
        vendor_request_id: None,
        provider: Some(wire.provider),
        model: Some(wire.model),
        account_ref: wire.account_ref,
        auth_selection: wire.auth_selection,
        charge_basis: wire.charge_basis,
        charge_basis_mapping_version: None,
        charge_basis_overrides: None,
        measurements,
    })
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn wire_fixture() -> serde_json::Value {
    serde_json::json!({
        "export_seq": 7, "fact_id": "broca-content-hash", "run_id": "run-1",
        "session": {"harness":"test", "project_root":"/project", "session":"session-1"},
        "provider":"anthropic", "model":"claude", "charge_basis":"metered_api",
        "auth_selection":"apikey:openai:astro",
        "usage":{"input_tokens":100,"cached_input_tokens":50,"cache_write_tokens":0,"output_tokens":20,"reasoning_tokens":5},
        "occurred_at_ms": 1000
    })
}

/// A BROCA batch fact in the agreed shape, made in a run, with input only.
#[cfg(test)]
pub(crate) fn system_one_fixture() -> serde_json::Value {
    serde_json::json!({
        "kind": "system_one", "fact_id": "system-one-hash", "batch_id": "batch-1",
        "item_index": 0, "run_id": "run-1", "provider": "typesafe",
        "model": "typesafe/jev-1.13.0", "account_ref": "primary",
        "auth_selection": "apikey:typesafe", "charge_basis": "metered_api",
        "occurred_at_ms": 1_792_772_000_000_u64, "usage": {"input_tokens": 100}
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_state;

    /// Both arms, because only the pair distinguishes a semantic change from a
    /// type change: an absent field must OMIT its measurement, and a reported
    /// zero must still produce one. A mechanical rewrite that mapped absence to
    /// `Some(0)` would compile, pass the fixture test above, and change nothing.
    #[test]
    fn an_absent_count_omits_its_measurement_while_a_reported_zero_keeps_one() {
        let mut absent = wire_fixture();
        // An openai-shaped payload: no cache-write concept, so no such key.
        // reasoning is present AND zero, which is a real reading of none.
        absent["usage"] = serde_json::json!({
            "input_tokens": 100,
            "cached_input_tokens": 50,
            "output_tokens": 20,
            "reasoning_tokens": 0
        });
        let request = map_fact(serde_json::from_value(absent).unwrap()).unwrap();
        let roles: Vec<_> = request
            .measurements
            .iter()
            .map(|measurement| (measurement.role.clone(), measurement.quantity_units))
            .collect();
        assert_eq!(
            roles,
            vec![
                ("input".to_string(), Some(100)),
                ("cache_read".to_string(), Some(50)),
                ("output".to_string(), Some(20)),
                ("reasoning".to_string(), Some(0)),
            ],
            "cache_write must be absent entirely, and a reported zero must survive"
        );
    }

    /// Four counts are stored as sent; output is stored without reasoning
    /// (20 - 5), because BROCA's output includes it.
    #[test]
    fn maps_verbatim_reference_and_five_disjoint_token_readings_including_zero() {
        let request = map_fact(serde_json::from_value(wire_fixture()).unwrap()).unwrap();
        assert_eq!(request.producer_ref, "broca-content-hash");
        assert_eq!(request.producer_lineage, None);
        let state = test_state();
        state
            .ordered_write(|tx| {
                astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    &request,
                    2000,
                    astrocyte_core::ingest::DeliveryPath::Pushed,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        let readings = state.with_conn(|conn| {
            let mut stmt = conn.prepare("SELECT role,unit,quantity_units,quantity_exponent FROM measurement ORDER BY role").map_err(|e| e.to_string())?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,i32>(3)?))).map_err(|e|e.to_string())?;
            rows.collect::<Result<Vec<_>,_>>().map_err(|e| e.to_string())
        }).unwrap();
        assert_eq!(
            readings,
            vec![
                ("cache_read".into(), "token".into(), 50, 0),
                ("cache_write".into(), "token".into(), 0, 0),
                ("input".into(), "token".into(), 100, 0),
                ("output".into(), "token".into(), 15, 0),
                ("reasoning".into(), "token".into(), 5, 0),
            ]
        );
    }

    fn fact(
        provider: &str,
        policy: Option<u32>,
        output: u64,
        reasoning: Option<u64>,
    ) -> WireSpendSegmentFact {
        let mut wire = wire_fixture();
        wire["provider"] = provider.into();
        if let Some(policy) = policy {
            wire["usage_policy_version"] = policy.into();
        }
        wire["usage"]["output_tokens"] = output.into();
        match reasoning {
            Some(reasoning) => wire["usage"]["reasoning_tokens"] = reasoning.into(),
            None => {
                wire["usage"]
                    .as_object_mut()
                    .unwrap()
                    .remove("reasoning_tokens");
            }
        }
        serde_json::from_value(wire).unwrap()
    }

    /// What this mapping produced before output and reasoning were split:
    /// every count verbatim under `usage.<field>`. Stored facts carry this
    /// shape, and measurements are never rewritten.
    fn pre_split_request(wire: WireSpendSegmentFact) -> EventRecordRequest {
        let usage = wire.usage.clone();
        let mut request = map_fact(wire).unwrap();
        request.measurements.retain(|m| m.role != "output");
        if let Some(output) = usage.output_tokens {
            request.measurements.push(MeasurementInput {
                role: "output".into(),
                unit: "token".into(),
                kind: "billed".into(),
                source: "usage.output_tokens".into(),
                quantity_units: Some(i64::try_from(output).unwrap()),
                quantity_exponent: Some(0),
            });
        }
        request
    }

    fn record(
        state: &crate::state::SharedState,
        request: &EventRecordRequest,
    ) -> astrocyte_core::ingest::RecordDisposition {
        state
            .ordered_write(|tx| {
                let outcome = astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    request,
                    2000,
                    astrocyte_core::ingest::DeliveryPath::Pushed,
                )
                .unwrap();
                Ok((outcome.disposition, true))
            })
            .unwrap()
    }

    /// (role, source, quantity) read back from the store.
    fn stored(state: &crate::state::SharedState) -> Vec<(String, String, Option<i64>)> {
        state
            .with_conn(|conn| {
                let mut stmt = conn
                    .prepare("SELECT role,source,quantity_units FROM measurement ORDER BY role")
                    .map_err(|e| e.to_string())?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .map_err(|e| e.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    fn stored_output_and_reasoning(
        wire: WireSpendSegmentFact,
    ) -> Vec<(String, String, Option<i64>)> {
        let state = test_state();
        record(&state, &map_fact(wire).unwrap());
        stored(&state)
            .into_iter()
            .filter(|(role, _, _)| role == "output" || role == "reasoning")
            .collect()
    }

    fn row(role: &str, source: &str, quantity: Option<i64>) -> (String, String, Option<i64>) {
        (role.into(), source.into(), quantity)
    }

    #[test]
    fn output_is_stored_without_the_reasoning_it_contains() {
        assert_eq!(
            stored_output_and_reasoning(fact("google", None, 100, Some(30))),
            vec![
                row("output", "usage.output_tokens-reasoning_tokens", Some(70)),
                row("reasoning", "usage.reasoning_tokens", Some(30)),
            ]
        );
    }

    /// Pre-fix xAI facts (no `usage_policy_version`) report output WITHOUT
    /// reasoning, so they are stored as sent; a stamped xAI fact follows the
    /// subset rule like every other provider. Both cases are asserted so the
    /// exception cannot silently widen to every xAI fact.
    #[test]
    fn pre_fix_xai_output_is_stored_as_sent_and_stamped_xai_is_split() {
        assert_eq!(
            stored_output_and_reasoning(fact("xai", None, 100, Some(30))),
            vec![
                row("output", "usage.output_tokens", Some(100)),
                row("reasoning", "usage.reasoning_tokens", Some(30)),
            ]
        );
        // A lopsided pre-fix fact is ordinary for the sibling convention,
        // not a conflict.
        assert_eq!(
            stored_output_and_reasoning(fact("xai", None, 20, Some(50)))[0],
            row("output", "usage.output_tokens", Some(20))
        );
        assert_eq!(
            stored_output_and_reasoning(fact("xai", Some(2), 100, Some(30)))[0],
            row("output", "usage.output_tokens-reasoning_tokens", Some(70))
        );
    }

    /// An unreported reasoning count is a floor, not a measurement, so output
    /// is the full billed output and no reasoning row exists to double it.
    #[test]
    fn absent_reasoning_keeps_the_full_output() {
        assert_eq!(
            stored_output_and_reasoning(fact("anthropic", None, 100, None)),
            vec![row("output", "usage.output_tokens", Some(100))]
        );
    }

    struct OneRateCatalog;

    impl astrocyte_core::catalog_client::CatalogConnection for OneRateCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<
            fusiform_protocol::StatusResponse,
            astrocyte_core::catalog_client::CatalogCallError,
        > {
            Ok(
                serde_json::from_value(serde_json::json!({"source": "fake", "catalog_version": 0,
                "model_count": 0, "era_count": 0, "recent_polls": []}))
                .unwrap(),
            )
        }

        fn catalog_get(
            &self,
            request: fusiform_protocol::CatalogGetRequest,
        ) -> Result<
            fusiform_protocol::CatalogGetResponse,
            astrocyte_core::catalog_client::CatalogCallError,
        > {
            if request.at_ms == Some(0) {
                return Err(astrocyte_core::catalog_client::CatalogCallError {
                    code: "no_coverage".into(),
                    message: "no record at 0: fusiform's history begins at 1".into(),
                });
            }
            let rate = serde_json::json!({"state":"priced","units":3_000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}});
            Ok(serde_json::from_value(serde_json::json!({
                "source": "test-catalog",
                "resolved_at_ms": request.at_ms,
                "catalog_version": 1,
                "models": {"anthropic/claude": {
                    "rate.input": rate, "rate.cache_read": rate,
                    "rate.cache_write": rate, "rate.output": rate,
                    "rate.reasoning": rate
                }},
                "withheld": [], "uncertain": [], "overridden": []
            }))
            .unwrap())
        }
    }

    /// The same pricing path a BROCA session fact takes, against a catalog
    /// that knows only the batch model: the provider's own rate for input,
    /// and a provider-stated zero for output.
    struct SystemOneCatalog;

    impl astrocyte_core::catalog_client::CatalogConnection for SystemOneCatalog {
        fn catalog_status(
            &self,
            request: fusiform_protocol::StatusRequest,
        ) -> Result<
            fusiform_protocol::StatusResponse,
            astrocyte_core::catalog_client::CatalogCallError,
        > {
            OneRateCatalog.catalog_status(request)
        }

        fn catalog_get(
            &self,
            request: fusiform_protocol::CatalogGetRequest,
        ) -> Result<
            fusiform_protocol::CatalogGetResponse,
            astrocyte_core::catalog_client::CatalogCallError,
        > {
            if request.at_ms == Some(0) {
                return OneRateCatalog.catalog_get(request);
            }
            let key = format!(
                "{}/{}",
                request.provider_id.as_deref().unwrap_or_default(),
                request.model_id.as_deref().unwrap_or_default()
            );
            // $3 per million input tokens (3e9 nanodollars per million), so
            // the fixture's 100 input tokens cost 300,000 nanodollars.
            let models = if key == "typesafe/typesafe/jev-1.13.0" {
                serde_json::json!({ key: {
                    "rate.input": {"state":"priced","units":3_000_000_000_u64,"exponent":9,
                        "currency":"USD","unit_provenance":{"kind":"stated"}},
                    "rate.output": {"state":"stated_zero"}
                }})
            } else {
                serde_json::json!({})
            };
            Ok(serde_json::from_value(serde_json::json!({
                "source": "test-catalog",
                "resolved_at_ms": request.at_ms,
                "catalog_version": 1,
                "models": models,
                "withheld": [], "uncertain": [], "overridden": []
            }))
            .unwrap())
        }
    }

    /// A batch fact's input prices through the normal pricing pass. Its
    /// reported output, against a provider's own stated zero, stays unpriced
    /// as `stated_zero_requires_policy`: the existing rule for a stated zero,
    /// pinned here for batch facts rather than changed.
    #[test]
    fn a_system_one_fact_prices_input_and_leaves_a_stated_zero_output_unpriced() {
        use astrocyte_core::pricing;
        let mut wire = system_one_fixture();
        wire["usage"]["output_tokens"] = 20.into();
        let request = map_system_one_fact(serde_json::from_value(wire).unwrap()).unwrap();
        let state = test_state();
        assert_eq!(
            record(&state, &request),
            astrocyte_core::ingest::RecordDisposition::Recorded
        );
        let event_id = astrocyte_core::fact::event_id(
            &request.producer,
            request.producer_lineage.as_deref(),
            &request.producer_ref,
        )
        .unwrap();
        let catalog = astrocyte_core::catalog_client::CatalogClient::connect(std::sync::Arc::new(
            SystemOneCatalog,
        ))
        .unwrap();
        state
            .ordered_write(|tx| {
                let plan = pricing::read_reprice_in_tx(tx, &event_id).unwrap();
                let resolved = pricing::resolve_reprice(&plan, &catalog, &[]).unwrap();
                pricing::commit_reprice_in_tx(
                    tx,
                    &plan,
                    &resolved,
                    3000,
                    pricing::PricingOrigin::PricingPass,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        type Charge = (String, String, Option<String>, Option<String>, Option<i64>);
        let charges: Vec<Charge> = state
            .with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT role,outcome,unpriced_reason,currency,charge_nanos
                         FROM priced_charge ORDER BY role",
                    )
                    .map_err(|e| e.to_string())?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                    })
                    .map_err(|e| e.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())
            })
            .unwrap();
        assert_eq!(
            charges,
            vec![
                (
                    "input".into(),
                    "priced".into(),
                    None,
                    Some("USD".into()),
                    Some(300_000)
                ),
                (
                    "output".into(),
                    "unpriced".into(),
                    Some("stated_zero_requires_policy".into()),
                    None,
                    None
                ),
            ]
        );
    }

    /// Reasoning above output contradicts BROCA's subset rule. The event still
    /// lands with output stored without a quantity, and pricing refuses that
    /// role while pricing reasoning.
    #[test]
    fn reasoning_above_output_is_conflicted_lands_and_is_never_priced() {
        use astrocyte_core::pricing;
        let request = map_fact(fact("anthropic", None, 10, Some(30))).unwrap();
        let state = test_state();
        assert_eq!(
            record(&state, &request),
            astrocyte_core::ingest::RecordDisposition::Recorded
        );
        assert_eq!(
            stored(&state)
                .into_iter()
                .filter(|(role, _, _)| role == "output")
                .collect::<Vec<_>>(),
            vec![row(
                "output",
                "usage.output_tokens-reasoning_tokens:conflicted",
                None
            )]
        );
        let event_id = astrocyte_core::fact::event_id(
            &request.producer,
            request.producer_lineage.as_deref(),
            &request.producer_ref,
        )
        .unwrap();
        let catalog = astrocyte_core::catalog_client::CatalogClient::connect(std::sync::Arc::new(
            OneRateCatalog,
        ))
        .unwrap();
        state
            .ordered_write(|tx| {
                let plan = pricing::read_reprice_in_tx(tx, &event_id).unwrap();
                let resolved = pricing::resolve_reprice(&plan, &catalog, &[]).unwrap();
                pricing::commit_reprice_in_tx(
                    tx,
                    &plan,
                    &resolved,
                    3000,
                    pricing::PricingOrigin::PricingPass,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        let charges: Vec<(String, String, Option<String>)> = state
            .with_conn(|conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT role,outcome,unpriced_reason FROM priced_charge
                         WHERE role IN ('output','reasoning') ORDER BY role",
                    )
                    .map_err(|e| e.to_string())?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .map_err(|e| e.to_string())?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())
            })
            .unwrap();
        assert_eq!(
            charges,
            vec![
                (
                    "output".into(),
                    "unpriced".into(),
                    Some("missing_quantity".into())
                ),
                ("reasoning".into(), "priced".into(), None),
            ]
        );
    }

    /// BROCA re-sends its whole export after an export-identity change. A
    /// fact stored before output and reasoning were split must come back as
    /// an exact redelivery in every shape, not as a quarantined contradiction,
    /// and its stored rows stay as first written.
    #[test]
    fn a_resend_of_a_pre_split_fact_is_an_exact_redelivery_in_every_shape() {
        let shapes = [
            ("subtracted", fact("google", None, 100, Some(30))),
            ("reasoning zero", fact("openai", None, 100, Some(0))),
            ("pre-fix xai", fact("xai", None, 100, Some(30))),
            ("reasoning absent", fact("anthropic", None, 100, None)),
            ("conflicted", fact("xai", Some(2), 10, Some(30))),
        ];
        for (shape, wire) in shapes {
            let state = test_state();
            record(&state, &pre_split_request(wire.clone()));
            let before = stored(&state);
            assert_eq!(
                record(&state, &map_fact(wire).unwrap()),
                astrocyte_core::ingest::RecordDisposition::ExactRedelivery,
                "{shape}"
            );
            assert_eq!(stored(&state), before, "{shape}");
            let rejected: i64 = state
                .with_conn(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM rejected_delivery", [], |r| r.get(0))
                        .map_err(|e| e.to_string())
                })
                .unwrap();
            assert_eq!(rejected, 0, "{shape}");
        }
    }

    /// The equivalence is exact: a re-send whose output genuinely differs
    /// from the stored one is still a contradiction.
    #[test]
    fn a_resend_with_a_different_output_still_quarantines() {
        let state = test_state();
        record(
            &state,
            &pre_split_request(fact("google", None, 100, Some(30))),
        );
        assert_eq!(
            record(
                &state,
                &map_fact(fact("google", None, 120, Some(30))).unwrap()
            ),
            astrocyte_core::ingest::RecordDisposition::QuarantinedRedelivery
        );
    }

    #[test]
    fn auth_selection_survives_wire_to_store_round_trip() {
        let request = map_fact(serde_json::from_value(wire_fixture()).unwrap()).unwrap();
        assert_eq!(
            request.auth_selection.as_deref(),
            Some("apikey:openai:astro")
        );
        let state = test_state();
        state
            .ordered_write(|tx| {
                astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    &request,
                    2000,
                    astrocyte_core::ingest::DeliveryPath::Pushed,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        // Read the value back FROM THE STORE, not from the request.
        let event_id = astrocyte_core::fact::event_id(
            &request.producer,
            request.producer_lineage.as_deref(),
            &request.producer_ref,
        )
        .unwrap();
        let stored: Option<String> = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT auth_selection FROM usage_event WHERE event_id=?1",
                    [&event_id],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap();
        assert_eq!(stored.as_deref(), Some("apikey:openai:astro"));
    }

    /// Records the mapped fact and reads `work_class` back FROM THE STORE, so a
    /// value lost anywhere between the wire and the column shows up here.
    fn stored_work_class(fixture: serde_json::Value) -> Option<String> {
        let request = map_fact(serde_json::from_value(fixture).unwrap()).unwrap();
        let state = test_state();
        state
            .ordered_write(|tx| {
                astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    &request,
                    2000,
                    astrocyte_core::ingest::DeliveryPath::Pushed,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        let event_id = astrocyte_core::fact::event_id(
            &request.producer,
            request.producer_lineage.as_deref(),
            &request.producer_ref,
        )
        .unwrap();
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT work_class FROM usage_event WHERE event_id=?1",
                    [&event_id],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    #[test]
    fn work_class_present_is_stored_verbatim() {
        let mut fixture = wire_fixture();
        fixture["work_class"] = serde_json::json!("sidekick.explore");
        assert_eq!(
            stored_work_class(fixture).as_deref(),
            Some("sidekick.explore")
        );
    }

    #[test]
    fn work_class_absent_is_stored_as_null() {
        let fixture = wire_fixture();
        assert!(fixture.get("work_class").is_none());
        assert_eq!(stored_work_class(fixture), None);
    }

    #[test]
    fn work_class_empty_string_is_stored_as_empty_not_null() {
        let mut fixture = wire_fixture();
        fixture["work_class"] = serde_json::json!("");
        assert_eq!(stored_work_class(fixture).as_deref(), Some(""));
    }

    #[test]
    fn work_class_json_null_is_stored_as_null() {
        let mut fixture = wire_fixture();
        fixture["work_class"] = serde_json::Value::Null;
        assert_eq!(stored_work_class(fixture), None);
    }

    /// Records the mapped fact and reads both charge-basis provenance columns
    /// back FROM THE STORE.
    fn stored_charge_basis_provenance(fixture: serde_json::Value) -> (Option<i64>, Option<String>) {
        let request = map_fact(serde_json::from_value(fixture).unwrap()).unwrap();
        let state = test_state();
        state
            .ordered_write(|tx| {
                astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    &request,
                    2000,
                    astrocyte_core::ingest::DeliveryPath::Pushed,
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
        let event_id = astrocyte_core::fact::event_id(
            &request.producer,
            request.producer_lineage.as_deref(),
            &request.producer_ref,
        )
        .unwrap();
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT charge_basis_mapping_version,charge_basis_overrides
                     FROM usage_event WHERE event_id=?1",
                    [&event_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    /// A fact written before BROCA emitted the overrides field has NO key at
    /// all (BROCA skips serializing it), and that must be stored as NULL, not
    /// as "none": only the producer can say no override applied.
    #[test]
    fn charge_basis_overrides_absent_key_is_stored_as_null() {
        let mut fixture = wire_fixture();
        fixture["charge_basis_mapping_version"] = serde_json::json!(3);
        assert!(fixture.get("charge_basis_overrides").is_none());
        assert_eq!(stored_charge_basis_provenance(fixture), (Some(3), None));
    }

    #[test]
    fn charge_basis_overrides_none_is_stored_as_none() {
        let mut fixture = wire_fixture();
        fixture["charge_basis_mapping_version"] = serde_json::json!(4);
        fixture["charge_basis_overrides"] = serde_json::json!("none");
        assert_eq!(
            stored_charge_basis_provenance(fixture),
            (Some(4), Some("none".to_string()))
        );
    }

    #[test]
    fn charge_basis_overrides_digest_is_stored_verbatim() {
        let digest = "0123456789abcdef".repeat(4);
        let mut fixture = wire_fixture();
        fixture["charge_basis_mapping_version"] = serde_json::json!(4);
        fixture["charge_basis_overrides"] = serde_json::json!(digest);
        assert_eq!(
            stored_charge_basis_provenance(fixture),
            (Some(4), Some(digest))
        );
    }

    /// A fact without a mapping version is stored with NULL rather than
    /// failing to decode, which would refuse the whole export page.
    #[test]
    fn charge_basis_mapping_version_absent_key_is_stored_as_null() {
        let fixture = wire_fixture();
        assert!(fixture.get("charge_basis_mapping_version").is_none());
        assert_eq!(stored_charge_basis_provenance(fixture), (None, None));
    }

    #[test]
    fn auth_selection_absent_decodes_to_none() {
        let mut fixture = wire_fixture();
        fixture.as_object_mut().unwrap().remove("auth_selection");
        let request = map_fact(serde_json::from_value(fixture).unwrap()).unwrap();
        assert_eq!(request.auth_selection, None);
    }

    /// BROCA added `account_decision` (an open string naming how the account was
    /// chosen) as a top-level key beside `auth_selection`, which stays a plain
    /// string. Astrocyte doesn't store it yet, so a fact carrying it must decode
    /// to exactly the event the same fact decodes to without it. A shape change
    /// that made `auth_selection` an object, or a decoder that refused unknown
    /// keys, would fail here instead of failing every new BROCA fact in production.
    #[test]
    fn account_decision_beside_auth_selection_changes_nothing_decoded() {
        let plain = wire_fixture();
        let mut decided = plain.clone();
        decided
            .as_object_mut()
            .unwrap()
            .insert("account_decision".into(), serde_json::json!("disabled"));
        let without = map_fact(serde_json::from_value(plain).unwrap()).unwrap();
        let with = map_fact(serde_json::from_value(decided).unwrap()).unwrap();
        assert_eq!(with, without);
        assert_eq!(with.auth_selection.as_deref(), Some("apikey:openai:astro"));
    }
}
