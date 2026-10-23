//! The versioned ManagementSurface served by `ck-astrocyte`.
//!
//! The operation list is deliberately kept beside dispatch and the manifest
//! consumes the same list. This makes an accidental operation addition visible
//! as a source change instead of allowing two independently maintained counts.

use astrocyte_core::report::{ReportError, ReportRequest};
use astrocyte_core::verdict::VerdictRequest;
use serde_json::{json, Value};

use crate::state::{now_ms, SharedState};

pub const OP_VERDICT: &str = "budget.verdict";
pub const OP_REPORT: &str = "spend.report";
pub const OP_EVENT_INGEST: &str = "spend.event.ingest";
pub const OP_CORRECTION_INGEST: &str = "spend.correction.ingest";
pub const OP_CHARGEABILITY_SETTLE: &str = "spend.chargeability.settle";
pub const OP_STATEMENT_IMPORT: &str = "spend.statement.import";
pub const OP_SUBSCRIPTION_SET: &str = "spend.subscription.set";
pub const OP_SUBSCRIPTION_CLOSE: &str = "spend.subscription.close";
pub const OP_SUBSCRIPTION_REPORT: &str = "spend.subscription.report";
pub const OP_WORKCLASS_REPORT: &str = "spend.workclass.report";
pub const OP_LEDGER_COVERAGE: &str = "spend.ledger.coverage";
pub const OP_CHARGE_EXPLAIN: &str = "spend.charge.explain";
pub const OP_RECONCILE: &str = "spend.reconcile";
pub const OP_EVENT_REPRICE: &str = "spend.event.reprice";
pub const OP_BASIS_CORRECT: &str = "spend.basis.correct";
pub const OP_MULTIPLIER_REPORT: &str = "spend.multiplier.report";
pub const OP_MULTIPLIER_PACE: &str = "spend.multiplier.pace";
pub const OP_MULTIPLIER_RECOMPUTE: &str = "spend.multiplier.recompute";
pub const OP_CACHE_SESSIONS: &str = "usage.cache.sessions";
pub const OP_CACHE_STEPS: &str = "usage.cache.steps";

/// The closed public surface consumed by dispatch, the manifest, and the schema
/// artifact. Exact name-set parity prevents a same-sized but incorrect surface.
pub const PUBLIC_OPERATION_NAMES: [&str; 20] = [
    OP_VERDICT,
    OP_REPORT,
    OP_EVENT_INGEST,
    OP_CORRECTION_INGEST,
    OP_CHARGEABILITY_SETTLE,
    OP_STATEMENT_IMPORT,
    OP_SUBSCRIPTION_SET,
    OP_SUBSCRIPTION_CLOSE,
    OP_SUBSCRIPTION_REPORT,
    OP_WORKCLASS_REPORT,
    OP_LEDGER_COVERAGE,
    OP_CHARGE_EXPLAIN,
    OP_RECONCILE,
    OP_EVENT_REPRICE,
    OP_BASIS_CORRECT,
    OP_MULTIPLIER_REPORT,
    OP_MULTIPLIER_PACE,
    OP_MULTIPLIER_RECOMPUTE,
    OP_CACHE_SESSIONS,
    OP_CACHE_STEPS,
];

/// Versioned, committed operation-schema artifact embedded in the serving
/// crate. Consumers can compare this value without parsing human documentation.
///
/// Version 2 replaced the generic `{"type": "object"}` params schema that
/// version 1 published for every operation except `spend.correction.ingest`
/// with one per operation describing the fields its request type decodes.
/// Runtime acceptance did not change between the two versions.
pub const OPERATION_SCHEMA_VERSION: u32 = 2;

pub fn operation_schemas() -> Value {
    json!({
        "schema_version": OPERATION_SCHEMA_VERSION,
        "operations": PUBLIC_OPERATION_NAMES.iter().map(|name| json!({
            "name": name,
            "params": operation_params_schema(name),
        })).collect::<Vec<_>>()
    })
}

/// The params schema for one served operation.
///
/// Each schema states what the operation's serde request type decodes, no more
/// and no less: a field is `required` exactly when the type has no default for
/// it, an `Option` field also accepts `null`, integer bounds are the Rust
/// integer's range, and every request type denies unknown fields, so every
/// object is closed. Checks a handler makes after decoding (an empty reason,
/// a reversed time range, an unknown grouping name) are not repeated here,
/// because a request that fails them is refused by the operation, not by the
/// decoder. The correction schema is the one exception: it was published
/// first, with a length bound on `correction_id` and a lower bound on
/// `occurred_at_ms`, and it is kept as published. The test
/// `every_operation_schema_agrees_with_its_decoder` keeps schema and decoder
/// from drifting apart.
fn operation_params_schema(name: &str) -> Value {
    let string = json!({"type": "string"});
    let optional_string = json!({"type": ["string", "null"]});
    let i64_value = json!({"type": "integer", "minimum": i64::MIN, "maximum": i64::MAX});
    let optional_i64 =
        json!({"type": ["integer", "null"], "minimum": i64::MIN, "maximum": i64::MAX});
    let u32_value = json!({"type": "integer", "minimum": 0, "maximum": u32::MAX});
    let optional_u32 = json!({"type": ["integer", "null"], "minimum": 0, "maximum": u32::MAX});
    let charge_basis = json!({"enum": ["metered_api", "subscription_included", "unknown"]});
    match name {
        OP_CORRECTION_INGEST => correction_ingest_schema(),
        OP_VERDICT => closed_object(
            &[],
            json!({
                "project_root": optional_string,
                "provider": optional_string,
                "model": optional_string,
                "est_cost_nanos": optional_i64,
            }),
        ),
        OP_REPORT => closed_object(
            &["grouping", "grain", "timezone"],
            json!({
                "grouping": {"type": "array", "items": string},
                "grain": string,
                "timezone": string,
            }),
        ),
        OP_EVENT_INGEST => closed_object(
            &["producer", "producer_ref", "charge_basis"],
            json!({
                "producer": string,
                "producer_lineage": optional_string,
                "producer_ref": string,
                "prior_event_ref": optional_string,
                "harness": optional_string,
                "project_root": optional_string,
                "session": optional_string,
                "run_ref": optional_string,
                "work_class": optional_string,
                "occurred_at_ms": optional_i64,
                "emitted_at_ms": optional_i64,
                "terminal_status": optional_string,
                "attempt_ordinal":
                    {"type": ["integer", "null"], "minimum": 0, "maximum": u32::MAX},
                "vendor_request_id": optional_string,
                "provider": optional_string,
                "model": optional_string,
                "account_ref": optional_string,
                "auth_selection": optional_string,
                "charge_basis": charge_basis,
                "measurements": {
                    "type": "array",
                    "items": closed_object(
                        &["role", "unit", "kind", "source"],
                        json!({
                            "role": string,
                            "unit": string,
                            "kind": string,
                            "source": string,
                            "quantity_units": optional_i64,
                            "quantity_exponent": {
                                "type": ["integer", "null"],
                                "minimum": i32::MIN,
                                "maximum": i32::MAX,
                            },
                        }),
                    ),
                },
            }),
        ),
        OP_CHARGEABILITY_SETTLE => closed_object(
            &["event_id", "to_state", "reason"],
            json!({
                "event_id": string,
                "to_state": {"enum": ["billed", "written_off", "unbillable"]},
                "reason": string,
            }),
        ),
        OP_STATEMENT_IMPORT => closed_object(
            &[
                "path",
                "provider",
                "account_ref",
                "period_start",
                "period_end",
                "amount_exponent",
                "currency",
            ],
            json!({
                "path": string,
                "provider": string,
                "account_ref": string,
                "period_start": i64_value,
                "period_end": i64_value,
                "amount_exponent": u32_value,
                "currency": string,
            }),
        ),
        OP_SUBSCRIPTION_SET => closed_object(
            &[
                "account_ref",
                "provider",
                "plan_name",
                "recurring_cost_minor",
                "currency",
                "amount_exponent",
                "period",
                "effective_from",
            ],
            json!({
                "account_ref": string,
                "provider": string,
                "plan_name": string,
                "recurring_cost_minor": i64_value,
                "currency": string,
                "amount_exponent": u32_value,
                "period": {"enum": ["month", "year"]},
                "effective_from": i64_value,
            }),
        ),
        OP_SUBSCRIPTION_CLOSE => closed_object(
            &["account_ref", "provider", "effective_to"],
            json!({
                "account_ref": string,
                "provider": string,
                "effective_to": i64_value,
            }),
        ),
        OP_SUBSCRIPTION_REPORT | OP_WORKCLASS_REPORT | OP_LEDGER_COVERAGE => {
            closed_object(&[], json!({}))
        }
        OP_CHARGE_EXPLAIN | OP_EVENT_REPRICE => {
            closed_object(&["event_id"], json!({"event_id": string}))
        }
        OP_RECONCILE => closed_object(&["statement_id"], json!({"statement_id": string})),
        OP_BASIS_CORRECT => closed_object(
            &["event_id", "corrected_basis", "reason"],
            json!({
                "event_id": string,
                "corrected_basis": charge_basis,
                "reason": string,
            }),
        ),
        OP_MULTIPLIER_REPORT => closed_object(
            &["from_ms", "to_ms"],
            json!({"from_ms": i64_value, "to_ms": i64_value}),
        ),
        OP_MULTIPLIER_PACE => closed_object(
            &["quota_provider", "from_ms", "to_ms"],
            json!({
                "quota_provider": string,
                "from_ms": i64_value,
                "to_ms": i64_value,
            }),
        ),
        OP_MULTIPLIER_RECOMPUTE => closed_object(
            &["observation_day"],
            json!({
                "observation_day": string,
                "quota_provider": optional_string,
            }),
        ),
        OP_CACHE_SESSIONS => closed_object(
            &[],
            json!({
                "since_ms": optional_i64,
                "harness": optional_string,
                "limit": optional_u32,
            }),
        ),
        OP_CACHE_STEPS => closed_object(
            &["harness", "session"],
            json!({
                "harness": string,
                "session": string,
                "limit": optional_u32,
                "after_cursor": optional_i64,
            }),
        ),
        other => unreachable!("operation {other} is served without a params schema"),
    }
}

/// An object schema that refuses unknown members, as `deny_unknown_fields`
/// does on every request type served here.
fn closed_object(required: &[&str], properties: Value) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties,
    })
}

fn correction_ingest_schema() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["correction_id", "event_id", "currency", "amount_nanos", "reason", "occurred_at_ms"],
        "properties": {
            "correction_id": {"type": "string", "minLength": 1, "maxLength": 128,
                "description": "Caller-chosen retry key, at most 128 UTF-8 bytes. Identical content replays the first result; conflicting content is refused as correction_id_reused."},
            "event_id": {"type": "string", "minLength": 1},
            "currency": {"type": "string", "minLength": 1},
            "amount_nanos": {"type": "integer"},
            "reason": {"enum": ["write_off", "vendor_restatement", "our_repricing", "refund", "goodwill"]},
            "occurred_at_ms": {"type": "integer", "minimum": 0},
            "note": {"type": ["string", "null"]}
        }
    })
}

pub type OpResult = Result<Value, (String, String)>;

pub fn dispatch(state: &SharedState, method: &str, params: Value) -> OpResult {
    match method {
        OP_VERDICT => op_verdict(state, params),
        OP_REPORT => op_report(state, params),
        OP_EVENT_INGEST => op_event_ingest(state, params),
        OP_CORRECTION_INGEST => op_correction_ingest(state, params),
        OP_CHARGEABILITY_SETTLE => op_chargeability_settle(state, params),
        OP_STATEMENT_IMPORT => op_statement_import(state, params),
        OP_SUBSCRIPTION_SET => op_subscription_set(state, params),
        OP_SUBSCRIPTION_CLOSE => op_subscription_close(state, params),
        OP_SUBSCRIPTION_REPORT => op_subscription_report(state, params),
        OP_WORKCLASS_REPORT => op_workclass_report(state, params),
        OP_LEDGER_COVERAGE => op_ledger_coverage(state, params),
        OP_CHARGE_EXPLAIN => op_charge_explain(state, params),
        OP_RECONCILE => op_reconcile(state, params),
        OP_EVENT_REPRICE => op_event_reprice(state, params),
        OP_BASIS_CORRECT => op_basis_correct(state, params),
        OP_MULTIPLIER_REPORT => op_multiplier_report(state, params),
        OP_MULTIPLIER_PACE => op_multiplier_pace(state, params),
        OP_MULTIPLIER_RECOMPUTE => op_multiplier_recompute(state, params),
        OP_CACHE_SESSIONS => op_cache_sessions(state, params),
        OP_CACHE_STEPS => op_cache_steps(state, params),
        other => Err(("unknown_method".into(), format!("unknown method '{other}'"))),
    }
}

fn op_event_reprice(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::pricing::RepriceRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("reprice params: {error}")))?;
    let plan = state
        .with_conn(|conn| {
            Ok(astrocyte_core::subscription::read_reprice(
                conn,
                &request.event_id,
            ))
        })
        .map_err(|error| ("internal".into(), error))?
        .map_err(map_subscription_error)?;
    let catalog = state.catalog_client();
    let resolved = match catalog.as_ref() {
        Some(catalog) => astrocyte_core::subscription::resolve_reprice(
            &plan,
            catalog,
            &state.desired_config().reasoning_billed_as_output,
        ),
        None => astrocyte_core::subscription::resolve_reprice_without_catalog(&plan),
    }
    .map_err(map_subscription_error)?;
    let recorded_at_ms = now_ms();
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::subscription::commit_reprice_in_tx(
            tx,
            &plan,
            &resolved,
            recorded_at_ms,
            astrocyte_core::pricing::PricingOrigin::OperatorReprice,
        )
        .map_err(map_subscription_error)?;
        Ok((json!(outcome), true))
    })
}

fn op_basis_correct(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::ingest::BasisCorrectRequest = serde_json::from_value(params)
        .map_err(|error| {
            (
                "malformed".into(),
                format!("basis correction params: {error}"),
            )
        })?;
    let plan = state
        .with_conn(|conn| {
            Ok(astrocyte_core::subscription::read_basis_correction(
                conn, &request,
            ))
        })
        .map_err(|error| ("internal".into(), error))?
        .map_err(map_subscription_error)?;
    let catalog = state.catalog_client();
    let resolved = match catalog.as_ref() {
        Some(catalog) => astrocyte_core::subscription::resolve_reprice(
            &plan,
            catalog,
            &state.desired_config().reasoning_billed_as_output,
        ),
        None => astrocyte_core::subscription::resolve_reprice_without_catalog(&plan),
    }
    .map_err(map_subscription_error)?;
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::ingest::correct_basis_in_tx(tx, &plan, &resolved, now_ms())
            .map_err(map_subscription_error)?;
        Ok((json!(outcome), true))
    })
}

fn op_event_ingest(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::ingest::EventRecordRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("event params: {error}")))?;
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::ingest::record_event_in_tx(
            tx,
            &request,
            now_ms(),
            astrocyte_core::ingest::DeliveryPath::Pushed,
        )
        .map_err(map_ingest_error)?;
        Ok((json!(outcome), true))
    })
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CorrectionIngestRequest {
    correction_id: String,
    #[serde(flatten)]
    adjustment: astrocyte_core::ingest::AdjustmentRecordRequest,
}

fn op_correction_ingest(state: &SharedState, params: Value) -> OpResult {
    let request: CorrectionIngestRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("correction params: {error}")))?;
    state.ordered_op_write(|tx| {
        let context = astrocyte_core::ingest::AdjustmentWriteContext::correction_ingest(
            request.correction_id.clone(),
        );
        let outcome = astrocyte_core::ingest::record_adjustment_in_tx(
            tx,
            &request.adjustment,
            &context,
            now_ms(),
        )
        .map_err(map_ingest_error)?;
        Ok((json!(outcome), true))
    })
}

fn op_chargeability_settle(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::ingest::EventSettleRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("settlement params: {error}")))?;
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::ingest::settle_event_in_tx(tx, &request, now_ms())
            .map_err(map_ingest_error)?;
        Ok((json!(outcome), true))
    })
}

fn op_statement_import(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::statement_import::StatementImportRequest =
        serde_json::from_value(params)
            .map_err(|error| ("malformed".into(), format!("statement params: {error}")))?;
    // Read the file before taking the writer lock. The database transaction is
    // never held across filesystem or network I/O.
    let bytes = read_statement_file(&request.path)?;
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::statement_import::import_statement_in_tx(
            tx,
            &request,
            &bytes,
            None,
            now_ms(),
        )
        .map_err(map_statement_error)?;
        Ok((json!(outcome), outcome.changed()))
    })
}

const MAX_STATEMENT_BYTES: u64 = 64 * 1024 * 1024;

fn read_statement_file(path: &str) -> Result<Vec<u8>, (String, String)> {
    let source_error = |error: std::io::Error| {
        (
            "statement_source".into(),
            format!("read statement: {error}"),
        )
    };
    let validate = |metadata: &std::fs::Metadata| {
        if !metadata.is_file() {
            Err((
                "invalid_request".into(),
                "statement source must be a regular file".into(),
            ))
        } else if metadata.len() > MAX_STATEMENT_BYTES {
            Err((
                "invalid_request".into(),
                "statement source exceeds the 64 MiB limit".into(),
            ))
        } else {
            Ok(())
        }
    };
    // Refuse special files before opening them: opening a FIFO can itself wait
    // forever. Recheck the opened handle and bound the read in case it grows.
    validate(&std::fs::metadata(path).map_err(source_error)?)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // A pathname may be replaced with a FIFO after the metadata check.
        // Nonblocking open prevents that race from parking a serving task.
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(source_error)?;
    validate(&file.metadata().map_err(source_error)?)?;
    read_statement_bytes(file)
}

fn read_statement_bytes(reader: impl std::io::Read) -> Result<Vec<u8>, (String, String)> {
    use std::io::Read;

    let mut bytes = Vec::new();
    reader
        .take(MAX_STATEMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            (
                "statement_source".into(),
                format!("read statement: {error}"),
            )
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_STATEMENT_BYTES {
        return Err((
            "invalid_request".into(),
            "statement source exceeds the 64 MiB limit".into(),
        ));
    }
    Ok(bytes)
}

fn op_subscription_set(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::subscription::SetSubscriptionRequest =
        serde_json::from_value(params)
            .map_err(|error| ("malformed".into(), format!("subscription params: {error}")))?;
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::subscription::set_subscription_in_tx(tx, &request, None)
            .map_err(map_subscription_error)?;
        let reply = astrocyte_core::subscription::SetSubscription::new(outcome, now_ms());
        Ok((json!(reply), true))
    })
}

fn op_subscription_close(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::subscription::CloseSubscriptionRequest =
        serde_json::from_value(params).map_err(|error| {
            (
                "malformed".into(),
                format!("subscription close params: {error}"),
            )
        })?;
    state.ordered_op_write(|tx| {
        let outcome = astrocyte_core::subscription::close_subscription_in_tx(tx, &request)
            .map_err(map_subscription_error)?;
        Ok((json!(outcome), true))
    })
}

fn op_verdict(state: &SharedState, params: Value) -> OpResult {
    let request: VerdictRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("verdict params: {error}")))?;
    state.with_read_op(|conn| {
        astrocyte_core::verdict::evaluate(conn, &request)
            .map(|value| json!(value))
            .map_err(|error| ("internal".into(), format!("verdict: {error:?}")))
    })
}

fn op_report(state: &SharedState, params: Value) -> OpResult {
    let request: ReportRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("report params: {error}")))?;
    state.public_projection().report(&request, now_ms())
}

fn op_subscription_report(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::report::SubscriptionReportRequest = serde_json::from_value(params)
        .map_err(|error| {
            (
                "malformed".into(),
                format!("subscription report params: {error}"),
            )
        })?;
    read_value(state, |conn| {
        astrocyte_core::report::subscription_report(conn, &request)
            .map(|value| json!(value))
            .map_err(map_report_error)
    })
}

fn op_workclass_report(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::report::WorkClassReportRequest = serde_json::from_value(params)
        .map_err(|error| {
            (
                "malformed".into(),
                format!("workclass report params: {error}"),
            )
        })?;
    read_value(state, |conn| {
        astrocyte_core::report::work_class_report(conn, &request)
            .map(|value| json!(value))
            .map_err(map_report_error)
    })
}

fn op_ledger_coverage(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::report::LedgerCoverageRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("coverage params: {error}")))?;
    read_value(state, |conn| {
        astrocyte_core::report::ledger_coverage(conn, &request, now_ms())
            .map(|value| json!(value))
            .map_err(map_report_error)
    })
}

fn op_charge_explain(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::report::ChargeExplainRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("charge params: {error}")))?;
    read_value(state, |conn| {
        astrocyte_core::report::explain_charge(conn, &request)
            .map(|value| json!(value))
            .map_err(map_report_error)
    })
}

fn op_reconcile(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::reconcile::ReconcileRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("reconcile params: {error}")))?;
    read_value(state, |conn| {
        astrocyte_core::reconcile::run_reconciliation(conn, &request)
            .map(|value| json!(value))
            .map_err(|error| (error.code().into(), error.message()))
    })
}

/// The recorded subscription multiplier series: the latest observation per
/// quota provider and every observation in the requested range.
fn op_multiplier_report(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::multiplier::MultiplierReportRequest =
        serde_json::from_value(params).map_err(|error| {
            (
                "malformed".into(),
                format!("multiplier report params: {error}"),
            )
        })?;
    read_value(state, |conn| {
        astrocyte_core::multiplier::multiplier_report(conn, &request)
            .map_err(|error| (error.code().into(), error.message()))
    })
}

fn op_multiplier_pace(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::multiplier::MultiplierPaceRequest = serde_json::from_value(params)
        .map_err(|error| {
            (
                "malformed".into(),
                format!("multiplier pace params: {error}"),
            )
        })?;
    let desired = state.desired_config();
    let config = match &desired.subscription_multiplier {
        crate::config::MultiplierSetting::Recorded(config) => Some(config),
        crate::config::MultiplierSetting::NotRecorded => None,
    };
    read_value(state, |conn| {
        astrocyte_core::multiplier::multiplier_pace(conn, config, &request)
            .map_err(|error| (error.code().into(), error.message()))
    })
}

/// Computes a multiplier day, superseding its head or creating its first row.
///
/// The daily stage's plan-price source lives on the refresher thread, so this
/// operation opens its own fusiform client from the committed configuration.
/// Construction reads no socket; a call is made only when some account has no
/// configured price override. The client is dropped here, inside the
/// blocking section the transport runs dispatch in.
fn op_multiplier_recompute(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::multiplier::MultiplierRecomputeRequest =
        serde_json::from_value(params).map_err(|error| {
            (
                "malformed".into(),
                format!("multiplier recompute params: {error}"),
            )
        })?;
    let plan_prices = match &state.desired_config().fusiform_connection_file {
        Some(path) => Some(
            crate::fusiform_client::FusiformClient::new(path)
                .map_err(|error| ("plan_prices_unavailable".to_string(), error))?,
        ),
        None => None,
    };
    crate::refresher::recompute_multiplier_day(
        state,
        plan_prices
            .as_ref()
            .map(|client| client as &dyn astrocyte_core::multiplier::PlanPriceSource),
        &request,
        now_ms(),
    )
}

/// Harness sessions with recent steps, with their cache health. Read from
/// the store on the reader connection.
fn op_cache_sessions(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::cache_diagnostics::CacheSessionsRequest =
        serde_json::from_value(params).map_err(|error| {
            (
                "malformed".into(),
                format!("cache sessions params: {error}"),
            )
        })?;
    read_value(state, |conn| {
        astrocyte_core::cache_diagnostics::cache_sessions(conn, request, now_ms())
            .map(|reply| json!(reply))
            .map_err(map_cache_error)
    })
}

/// One session's newest steps with their derived cache verdicts. Answered
/// from the recent-steps memory when it holds the whole window, otherwise
/// from the store on the reader connection; both build the reply the same way.
fn op_cache_steps(state: &SharedState, params: Value) -> OpResult {
    let request: astrocyte_core::cache_diagnostics::CacheStepsRequest =
        serde_json::from_value(params)
            .map_err(|error| ("malformed".into(), format!("cache steps params: {error}")))?;
    let query = request
        .validate()
        .map_err(|refusal| (refusal.code().into(), refusal.message()))?;
    if let Some(reply) = state.recent_steps_answer(&query) {
        return Ok(json!(reply));
    }
    read_value(state, |conn| {
        astrocyte_core::cache_diagnostics::steps_from_store(conn, &query)
            .map(|reply| json!(reply))
            .map_err(|error| ("internal".into(), format!("cache steps: {error}")))
    })
}

fn map_cache_error(error: astrocyte_core::cache_diagnostics::CacheError) -> (String, String) {
    match error {
        astrocyte_core::cache_diagnostics::CacheError::Refused(refusal) => {
            (refusal.code().into(), refusal.message())
        }
        astrocyte_core::cache_diagnostics::CacheError::Store(error) => {
            ("internal".into(), format!("cache sessions: {error}"))
        }
    }
}

/// Run a served read-only operation on the store's reader connection, in one
/// read transaction (see `SharedState::with_read_op`), so the report neither
/// waits for nor holds up the writer. Only for operations that never write.
fn read_value<T>(
    state: &SharedState,
    operation: impl FnOnce(&rusqlite::Connection) -> Result<T, (String, String)>,
) -> Result<T, (String, String)> {
    state.with_read_op(operation)
}

fn map_ingest_error(error: astrocyte_core::ingest::IngestError) -> (String, String) {
    match error {
        astrocyte_core::ingest::IngestError::Refused(reason) => (
            reason.as_str().into(),
            format!("ingest refused: {}", reason.as_str()),
        ),
        astrocyte_core::ingest::IngestError::Store(error) => ("internal".into(), error.to_string()),
        astrocyte_core::ingest::IngestError::Canon(error) => {
            ("internal".into(), format!("{error:?}"))
        }
    }
}

fn map_statement_error(error: astrocyte_core::statement::StatementError) -> (String, String) {
    (error.code().into(), error.message())
}

fn map_subscription_error(
    error: astrocyte_core::subscription::SubscriptionError,
) -> (String, String) {
    (error.code().into(), error.message())
}

pub(crate) fn map_report_error(error: ReportError) -> (String, String) {
    match error {
        ReportError::Refused(refusal) => (refusal.code().into(), refusal.message()),
        other => ("internal".into(), format!("report: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse};
    use serde_json::json;

    use super::*;
    use crate::state::test_state;

    #[test]
    fn correction_ingest_requires_key_and_replays_without_moving_money() {
        let state = test_state();
        let event = dispatch(
            &state,
            OP_EVENT_INGEST,
            json!({
                "producer":"producer", "producer_ref":"correction", "charge_basis":"metered_api",
                "occurred_at_ms":1000, "measurements":[]
            }),
        )
        .unwrap()["event_id"]
            .as_str()
            .unwrap()
            .to_owned();
        state
            .ordered_op_write(|tx| {
                tx.execute(
                    "UPDATE usage_event SET currency='USD' WHERE event_id=?1",
                    [&event],
                )
                .map_err(|error| ("internal".into(), error.to_string()))?;
                Ok((Value::Null, true))
            })
            .unwrap();
        let mut params = json!({"event_id":event, "currency":"USD", "amount_nanos":-25,
            "reason":"refund", "occurred_at_ms":1000});
        assert_eq!(
            dispatch(&state, OP_CORRECTION_INGEST, params.clone())
                .unwrap_err()
                .0,
            "malformed"
        );
        for key in [json!(null), json!(1), json!(""), json!("é".repeat(65))] {
            params["correction_id"] = key;
            assert_eq!(
                dispatch(&state, OP_CORRECTION_INGEST, params.clone())
                    .unwrap_err()
                    .0,
                "malformed"
            );
        }
        params["correction_id"] = json!("caller-key");
        params["operator"] = json!("forged");
        assert_eq!(
            dispatch(&state, OP_CORRECTION_INGEST, params.clone())
                .unwrap_err()
                .0,
            "malformed"
        );
        params.as_object_mut().unwrap().remove("operator");
        let first = dispatch(&state, OP_CORRECTION_INGEST, params.clone()).unwrap();
        assert_eq!(
            dispatch(&state, OP_CORRECTION_INGEST, params.clone()).unwrap(),
            first
        );
        params["amount_nanos"] = json!(-50);
        assert_eq!(
            dispatch(&state, OP_CORRECTION_INGEST, params)
                .unwrap_err()
                .0,
            "correction_id_reused"
        );
        let money = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT count(*),sum(amount_nanos) FROM adjustment",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(money, (1, -25));
        let schema = operation_schemas();
        let correction = schema["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|op| op["name"] == OP_CORRECTION_INGEST)
            .unwrap();
        assert!(correction["params"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("correction_id")));
    }

    #[test]
    fn basis_correction_op_refusals_write_no_charges() {
        let state = test_state();
        let event_id = dispatch(&state, OP_EVENT_INGEST, json!({
            "producer":"producer", "producer_lineage":"lineage", "producer_ref":"basis-refusal",
            "charge_basis":"unknown", "occurred_at_ms":10,
            "measurements":[{"role":"input","unit":"token","kind":"raw","source":"usage","quantity_units":1,"quantity_exponent":0}]
        })).unwrap()["event_id"].as_str().unwrap().to_owned();
        for (request, code) in [
            (
                json!({"event_id":"missing","corrected_basis":"metered_api","reason":"evidence"}),
                "not_found",
            ),
            (
                json!({"event_id":event_id,"corrected_basis":"metered_api","reason":""}),
                "malformed",
            ),
            (
                json!({"event_id":event_id,"corrected_basis":"metered_api","reason":"evidence","operator":"forged"}),
                "malformed",
            ),
        ] {
            assert_eq!(
                dispatch(&state, OP_BASIS_CORRECT, request.clone())
                    .unwrap_err()
                    .0,
                code
            );
            let count: i64 = state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT COUNT(*) FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                        [request["event_id"].as_str().unwrap()],
                        |r| r.get(0),
                    )
                    .map_err(|e| e.to_string())
                })
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn subscription_set_reply_carries_the_one_way_notice_only_when_backdated() {
        let state = test_state();
        let plan = |provider: &str, effective_from: i64| json!({"account_ref":"acct","provider":provider,"plan_name":"plan","recurring_cost_minor":100,"currency":"USD","amount_exponent":2,"period":"month","effective_from":effective_from});
        let keys = |reply: &Value| {
            reply
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        // The plan fields stay at the top level, as before the two additions.
        let expected_keys = [
            "account_ref",
            "amount_exponent",
            "backdated",
            "coverage_notice",
            "currency",
            "effective_from",
            "effective_to",
            "operator",
            "period",
            "plan_name",
            "provider",
            "recurring_cost_minor",
        ];

        let backdated = dispatch(&state, OP_SUBSCRIPTION_SET, plan("past", 1)).unwrap();
        assert_eq!(keys(&backdated), expected_keys);
        assert_eq!(backdated["backdated"], true);
        assert_eq!(
            backdated["coverage_notice"],
            astrocyte_core::subscription::SET_COVERAGE_NOTICE
        );
        assert_eq!(backdated["effective_from"], 1);
        assert_eq!(backdated["provider"], "past");

        // 2100-01-01T00:00:00Z: after any clock this test runs under.
        let future = dispatch(
            &state,
            OP_SUBSCRIPTION_SET,
            plan("future", 4_102_444_800_000),
        )
        .unwrap();
        assert_eq!(keys(&future), expected_keys);
        assert_eq!(future["backdated"], false);
        assert_eq!(future["coverage_notice"], Value::Null);
    }

    #[test]
    fn basis_correction_op_reaches_all_three_arms() {
        let state = test_state();
        dispatch(&state, OP_SUBSCRIPTION_SET, json!({"account_ref":"acct","provider":"provider","plan_name":"plan","recurring_cost_minor":100,"currency":"USD","amount_exponent":2,"period":"month","effective_from":1})).unwrap();
        let event_id = dispatch(&state, OP_EVENT_INGEST, json!({
            "producer":"producer", "producer_lineage":"lineage", "producer_ref":"basis-success",
            "provider":"provider", "model":"model", "account_ref":"acct",
            "charge_basis":"unknown", "occurred_at_ms":10,
            "measurements":[{"role":"input","unit":"token","kind":"raw","source":"usage","quantity_units":1,"quantity_exponent":0}]
        })).unwrap()["event_id"].as_str().unwrap().to_owned();
        for (basis, outcome, reason) in [
            ("subscription_included", "covered", None),
            ("metered_api", "unpriced", Some("catalog_unconfigured")),
            ("unknown", "unpriced", Some("charge_basis_unknown")),
        ] {
            dispatch(
                &state,
                OP_BASIS_CORRECT,
                json!({"event_id":event_id,"corrected_basis":basis,"reason":"evidence"}),
            )
            .unwrap();
            let actual: (String,String,Option<String>) = state.with_conn(|conn| conn.query_row("SELECT e.charge_basis,p.outcome,p.unpriced_reason FROM usage_event e JOIN priced_charge p USING(event_key) WHERE e.event_id=?1 AND NOT EXISTS (SELECT 1 FROM priced_charge c WHERE c.supersedes=p.charge_key)", [&event_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|e.to_string())).unwrap();
            assert_eq!(
                actual,
                (basis.into(), outcome.into(), reason.map(str::to_owned))
            );
        }
    }

    struct CoverageCatalog {
        requests: Mutex<Vec<CatalogGetRequest>>,
    }

    impl CoverageCatalog {
        fn new() -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl astrocyte_core::catalog_client::CatalogConnection for CoverageCatalog {
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
            request: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, astrocyte_core::catalog_client::CatalogCallError> {
            self.requests.lock().unwrap().push(request.clone());
            if request.at_ms == Some(0) {
                return Err(astrocyte_core::catalog_client::CatalogCallError {
                    code: "no_coverage".into(),
                    message: "fusiform's history begins at 1".into(),
                });
            }
            Err(astrocyte_core::catalog_client::CatalogCallError {
                code: "unexpected_catalog_call".into(),
                message: "covered subscription must not consult fusiform".into(),
            })
        }
    }

    #[test]
    fn operation_surface_is_set_equal_to_the_versioned_artifact() {
        let artifact_value = operation_schemas();
        let artifact = artifact_value["operations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["name"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        let expected = [
            "budget.verdict",
            "spend.basis.correct",
            "spend.charge.explain",
            "spend.chargeability.settle",
            "spend.correction.ingest",
            "spend.event.ingest",
            "spend.event.reprice",
            "spend.ledger.coverage",
            "spend.multiplier.pace",
            "spend.multiplier.recompute",
            "spend.multiplier.report",
            "spend.reconcile",
            "spend.report",
            "spend.statement.import",
            "spend.subscription.close",
            "spend.subscription.report",
            "spend.subscription.set",
            "spend.workclass.report",
            "usage.cache.sessions",
            "usage.cache.steps",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(artifact, expected);
        // Set equality is the assertion, not the size: a count alone
        // passes on a wrong set of the right size, so an operation renamed or
        // swapped fails here even though the total is unchanged.
        assert_eq!(expected.len(), PUBLIC_OPERATION_NAMES.len());
    }

    /// Every name must reach ITS OWN handler, which is a different claim from
    /// reaching some handler. Binding a name to the wrong function leaves all
    /// the other fences intact and green: the surface has the same size, the
    /// artifact set still matches, the manifest still publishes the operation
    /// as a write, and a refusal still comes back, just from the wrong code.
    ///
    /// The discriminator is the literal each handler writes in front of its
    /// own deserialization error. The probe is an unknown field rather than
    /// empty params because every request type on this surface denies unknown
    /// fields, so all refuse -- with empty params some reads
    /// deserialize successfully and would have to be excused, and an excused
    /// operation is an unrouted one.
    #[test]
    fn every_operation_name_reaches_its_own_handler() {
        // Each operation is paired with the marker its own handler writes.
        const ROUTES: [(&str, &str); 20] = HANDLER_MARKERS;

        // The markers only discriminate while none of them is a prefix of
        // another: three of these begin with the word "subscription", and if
        // one marker ever became a prefix of a second, a swap between exactly
        // those two would satisfy the routing check below and still reach the
        // wrong handler. Identical markers fail here for the same reason.
        for (name, marker) in ROUTES {
            for (other_name, other_marker) in ROUTES {
                assert!(
                    name == other_name || !other_name.starts_with(name),
                    "{name} prefixes {other_name}"
                );
                assert!(
                    name == other_name || !other_marker.starts_with(marker),
                    "{name}'s marker is a prefix of {other_name}'s, so a swap \
                     between them would pass unnoticed"
                );
            }
        }

        // And the table covers the surface rather than a subset of it: an
        // operation missing from ROUTES is an operation whose routing nothing
        // checks.
        assert_eq!(ROUTES.len(), PUBLIC_OPERATION_NAMES.len());
        for name in PUBLIC_OPERATION_NAMES {
            assert!(
                ROUTES.iter().any(|(routed, _)| *routed == name),
                "{name} is served but no expected handler is named for it"
            );
        }

        let state = test_state();
        for (name, marker) in ROUTES {
            let (code, message) = dispatch(&state, name, json!({"astrocyte_probe": true}))
                .expect_err("an unknown field is refused by every request type on this surface");
            assert_eq!(code, "malformed", "{name}");
            assert!(
                message.starts_with(marker),
                "{name} was answered by a handler that reported {message:?}, \
                 which is not {marker:?}"
            );
        }
    }

    /// Each operation is paired with the literal its handler prefixes to a
    /// request-decoding error. A refusal with code `malformed` and a message
    /// starting with that literal came from the decoder; any other answer
    /// means the decoder accepted the params.
    const HANDLER_MARKERS: [(&str, &str); 20] = [
        (OP_VERDICT, "verdict params: "),
        (OP_REPORT, "report params: "),
        (OP_EVENT_INGEST, "event params: "),
        (OP_CORRECTION_INGEST, "correction params: "),
        (OP_CHARGEABILITY_SETTLE, "settlement params: "),
        (OP_STATEMENT_IMPORT, "statement params: "),
        (OP_SUBSCRIPTION_SET, "subscription params: "),
        (OP_SUBSCRIPTION_CLOSE, "subscription close params: "),
        (OP_SUBSCRIPTION_REPORT, "subscription report params: "),
        (OP_WORKCLASS_REPORT, "workclass report params: "),
        (OP_LEDGER_COVERAGE, "coverage params: "),
        (OP_CHARGE_EXPLAIN, "charge params: "),
        (OP_RECONCILE, "reconcile params: "),
        (OP_EVENT_REPRICE, "reprice params: "),
        (OP_BASIS_CORRECT, "basis correction params: "),
        (OP_MULTIPLIER_REPORT, "multiplier report params: "),
        (OP_MULTIPLIER_PACE, "multiplier pace params: "),
        (OP_MULTIPLIER_RECOMPUTE, "multiplier recompute params: "),
        (OP_CACHE_SESSIONS, "cache sessions params: "),
        (OP_CACHE_STEPS, "cache steps params: "),
    ];

    /// Valid params for one operation, plus one params value both the decoder
    /// and the published schema must refuse.
    struct SchemaSamples {
        name: &'static str,
        /// The first sample names exactly the schema's required members; the
        /// samples together name every member the schema lists.
        valid: Vec<Value>,
        refused: Value,
    }

    fn schema_samples() -> Vec<SchemaSamples> {
        let sample = |name, valid, refused| SchemaSamples {
            name,
            valid,
            refused,
        };
        vec![
            sample(
                OP_VERDICT,
                vec![
                    json!({}),
                    json!({"project_root": "/work", "provider": "openai", "model": "gpt", "est_cost_nanos": 5}),
                    json!({"project_root": null, "provider": null, "model": null, "est_cost_nanos": null}),
                ],
                json!({"est_cost_nanos": "5"}),
            ),
            sample(
                OP_REPORT,
                vec![
                    json!({"grouping": [], "grain": "day", "timezone": "UTC"}),
                    json!({"grouping": ["provider", "model"], "grain": "day", "timezone": "UTC"}),
                ],
                json!({"grouping": "provider", "grain": "day", "timezone": "UTC"}),
            ),
            sample(
                OP_EVENT_INGEST,
                vec![
                    json!({"producer": "p", "producer_ref": "schema-minimal", "charge_basis": "metered_api"}),
                    json!({
                        "producer": "p", "producer_lineage": "l", "producer_ref": "schema-full",
                        "prior_event_ref": "prior", "harness": "h", "project_root": "/work",
                        "session": "s", "run_ref": "run", "work_class": "w",
                        "occurred_at_ms": 10, "emitted_at_ms": 11, "terminal_status": "ok",
                        "attempt_ordinal": 4_294_967_295_u32, "vendor_request_id": "v",
                        "provider": "openai", "model": "gpt", "account_ref": "a",
                        "auth_selection": "key", "charge_basis": "subscription_included",
                        "measurements": [
                            {"role": "input", "unit": "token", "kind": "raw", "source": "usage",
                             "quantity_units": 1, "quantity_exponent": -2_147_483_648_i32},
                            {"role": "output", "unit": "token", "kind": "raw", "source": "usage",
                             "quantity_units": null, "quantity_exponent": null},
                            {"role": "output", "unit": "token", "kind": "raw", "source": "usage"}
                        ]
                    }),
                    json!({
                        "producer": "p", "producer_lineage": null, "producer_ref": "schema-nulls",
                        "prior_event_ref": null, "harness": null, "project_root": null,
                        "session": null, "run_ref": null, "work_class": null,
                        "occurred_at_ms": null, "emitted_at_ms": null, "terminal_status": null,
                        "attempt_ordinal": null, "vendor_request_id": null, "provider": null,
                        "model": null, "account_ref": null, "auth_selection": null,
                        "charge_basis": "unknown", "measurements": []
                    }),
                ],
                json!({"producer": "p", "producer_ref": "schema-bad", "charge_basis": "metered_api", "occurred_at_ms": "10"}),
            ),
            sample(
                OP_CORRECTION_INGEST,
                vec![
                    json!({"correction_id": "k", "event_id": "e", "currency": "USD", "amount_nanos": -25, "reason": "refund", "occurred_at_ms": 1000}),
                    json!({"correction_id": "k", "event_id": "e", "currency": "USD", "amount_nanos": 25, "reason": "goodwill", "occurred_at_ms": 0, "note": "n"}),
                    json!({"correction_id": "k", "event_id": "e", "currency": "USD", "amount_nanos": 25, "reason": "write_off", "occurred_at_ms": 0, "note": null}),
                ],
                json!({"correction_id": "k", "event_id": "e", "currency": "USD", "amount_nanos": "25", "reason": "refund", "occurred_at_ms": 1000}),
            ),
            sample(
                OP_CHARGEABILITY_SETTLE,
                vec![
                    json!({"event_id": "e", "to_state": "billed", "reason": "r"}),
                    json!({"event_id": "e", "to_state": "written_off", "reason": "r"}),
                    json!({"event_id": "e", "to_state": "unbillable", "reason": "r"}),
                ],
                json!({"event_id": "e", "to_state": 1, "reason": "r"}),
            ),
            sample(
                OP_STATEMENT_IMPORT,
                vec![json!({
                    "path": "astrocyte-schema-probe-does-not-exist.csv", "provider": "openai",
                    "account_ref": "a", "period_start": 0, "period_end": 100,
                    "amount_exponent": 2, "currency": "USD"
                })],
                json!({
                    "path": "astrocyte-schema-probe-does-not-exist.csv", "provider": "openai",
                    "account_ref": "a", "period_start": 0, "period_end": 100,
                    "amount_exponent": "2", "currency": "USD"
                }),
            ),
            sample(
                OP_SUBSCRIPTION_SET,
                vec![
                    json!({"account_ref": "acct", "provider": "provider", "plan_name": "plan", "recurring_cost_minor": 100, "currency": "USD", "amount_exponent": 2, "period": "month", "effective_from": 1}),
                    json!({"account_ref": "acct2", "provider": "provider", "plan_name": "plan", "recurring_cost_minor": 1200, "currency": "USD", "amount_exponent": 2, "period": "year", "effective_from": 1}),
                ],
                json!({"account_ref": "acct", "provider": "provider", "plan_name": "plan", "recurring_cost_minor": "100", "currency": "USD", "amount_exponent": 2, "period": "month", "effective_from": 1}),
            ),
            sample(
                OP_SUBSCRIPTION_CLOSE,
                vec![json!({"account_ref": "acct", "provider": "provider", "effective_to": 10})],
                json!({"account_ref": "acct", "provider": "provider", "effective_to": "10"}),
            ),
            // The three report requests have no members, so there is no member
            // to give a wrong type; the refused sample is an unknown member.
            sample(
                OP_SUBSCRIPTION_REPORT,
                vec![json!({})],
                json!({"astrocyte_probe": 1}),
            ),
            sample(
                OP_WORKCLASS_REPORT,
                vec![json!({})],
                json!({"astrocyte_probe": 1}),
            ),
            sample(
                OP_LEDGER_COVERAGE,
                vec![json!({})],
                json!({"astrocyte_probe": 1}),
            ),
            sample(
                OP_CHARGE_EXPLAIN,
                vec![json!({"event_id": "no-such-event"})],
                json!({"event_id": 1}),
            ),
            sample(
                OP_RECONCILE,
                vec![json!({"statement_id": "no-such-statement"})],
                json!({"statement_id": 1}),
            ),
            sample(
                OP_EVENT_REPRICE,
                vec![json!({"event_id": "no-such-event"})],
                json!({"event_id": true}),
            ),
            sample(
                OP_BASIS_CORRECT,
                vec![
                    json!({"event_id": "no-such-event", "corrected_basis": "unknown", "reason": "evidence"}),
                    json!({"event_id": "no-such-event", "corrected_basis": "metered_api", "reason": "evidence"}),
                    json!({"event_id": "no-such-event", "corrected_basis": "subscription_included", "reason": "evidence"}),
                ],
                json!({"event_id": "no-such-event", "corrected_basis": 2, "reason": "evidence"}),
            ),
            sample(
                OP_MULTIPLIER_REPORT,
                vec![
                    json!({"from_ms": 0, "to_ms": 10}),
                    json!({"from_ms": i64::MIN, "to_ms": i64::MAX}),
                ],
                json!({"from_ms": "0", "to_ms": 10}),
            ),
            sample(
                OP_MULTIPLIER_PACE,
                vec![json!({"quota_provider": "claude", "from_ms": 1, "to_ms": 2})],
                json!({"quota_provider": "claude", "from_ms": 1.5, "to_ms": 2}),
            ),
            sample(
                OP_MULTIPLIER_RECOMPUTE,
                vec![
                    json!({"observation_day": "2024-10-05"}),
                    json!({"observation_day": "2024-10-05", "quota_provider": "unknown"}),
                    json!({"observation_day": "2024-10-05", "quota_provider": null}),
                ],
                json!({"observation_day": 20_241_005}),
            ),
            sample(
                OP_CACHE_SESSIONS,
                vec![
                    json!({}),
                    json!({"since_ms": null, "harness": "codex", "limit": 5}),
                    json!({"since_ms": null, "harness": null, "limit": null}),
                ],
                json!({"limit": -1}),
            ),
            sample(
                OP_CACHE_STEPS,
                vec![
                    json!({"harness": "opencode", "session": "s"}),
                    json!({"harness": "pi", "session": "s", "limit": 3, "after_cursor": 7}),
                    json!({"harness": "pi", "session": "s", "limit": null, "after_cursor": null}),
                ],
                json!({"harness": "pi", "session": 1}),
            ),
        ]
    }

    /// Checks `value` against the subset of JSON Schema the operation schemas
    /// use. Any other keyword panics rather than being skipped, so a schema can
    /// never pass here only because this checker ignored part of it.
    fn schema_accepts(schema: &Value, value: &Value) -> Result<(), String> {
        let schema = schema.as_object().expect("a schema is an object");
        for keyword in schema.keys() {
            assert!(
                [
                    "type",
                    "enum",
                    "minimum",
                    "maximum",
                    "minLength",
                    "maxLength",
                    "properties",
                    "required",
                    "additionalProperties",
                    "items",
                    "description",
                ]
                .contains(&keyword.as_str()),
                "the schema checker does not understand {keyword:?}"
            );
        }
        if let Some(types) = schema.get("type") {
            let types: Vec<&str> = match types {
                Value::String(single) => vec![single.as_str()],
                Value::Array(many) => many.iter().map(|t| t.as_str().unwrap()).collect(),
                other => panic!("type {other} is neither a name nor a list"),
            };
            let matches = |kind: &str| match kind {
                "string" => value.is_string(),
                "integer" => value.is_i64() || value.is_u64(),
                "number" => value.is_number(),
                "boolean" => value.is_boolean(),
                "null" => value.is_null(),
                "object" => value.is_object(),
                "array" => value.is_array(),
                other => panic!("unknown type name {other}"),
            };
            if !types.iter().any(|kind| matches(kind)) {
                return Err(format!("{value} is not of type {types:?}"));
            }
        }
        if let Some(allowed) = schema.get("enum") {
            if !allowed.as_array().unwrap().contains(value) {
                return Err(format!("{value} is not one of {allowed}"));
            }
        }
        let as_integer = |number: &Value| -> Option<i128> {
            number
                .as_i64()
                .map(i128::from)
                .or_else(|| number.as_u64().map(i128::from))
        };
        if let Some(number) = as_integer(value) {
            if let Some(minimum) = schema.get("minimum") {
                if number < as_integer(minimum).unwrap() {
                    return Err(format!("{value} is below {minimum}"));
                }
            }
            if let Some(maximum) = schema.get("maximum") {
                if number > as_integer(maximum).unwrap() {
                    return Err(format!("{value} is above {maximum}"));
                }
            }
        }
        if let Some(text) = value.as_str() {
            let length = u64::try_from(text.chars().count()).unwrap();
            if let Some(minimum) = schema.get("minLength").and_then(Value::as_u64) {
                if length < minimum {
                    return Err(format!("{value} is shorter than {minimum}"));
                }
            }
            if let Some(maximum) = schema.get("maxLength").and_then(Value::as_u64) {
                if length > maximum {
                    return Err(format!("{value} is longer than {maximum}"));
                }
            }
        }
        if let Some(members) = value.as_object() {
            let properties = schema.get("properties").and_then(Value::as_object);
            for required in schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let required = required.as_str().unwrap();
                if !members.contains_key(required) {
                    return Err(format!("{value} lacks required member {required}"));
                }
            }
            for (member, member_value) in members {
                match properties.and_then(|properties| properties.get(member)) {
                    Some(member_schema) => schema_accepts(member_schema, member_value)
                        .map_err(|error| format!("{member}: {error}"))?,
                    None => match schema.get("additionalProperties") {
                        Some(Value::Bool(false)) => {
                            return Err(format!("{member} is not a listed member"))
                        }
                        None | Some(Value::Bool(true)) => {}
                        Some(other) => panic!("additionalProperties {other} is not supported"),
                    },
                }
            }
        }
        if let (Some(items), Some(elements)) = (schema.get("items"), value.as_array()) {
            for (index, element) in elements.iter().enumerate() {
                schema_accepts(items, element).map_err(|error| format!("[{index}]: {error}"))?;
            }
        }
        Ok(())
    }

    /// Dispatches `params` and reports whether the operation's request decoder
    /// refused them. A refusal for any other reason (an unknown event, a
    /// missing statement file) means the decoder accepted them.
    fn decoder_refuses(state: &SharedState, name: &str, params: &Value) -> Option<String> {
        let marker = HANDLER_MARKERS
            .iter()
            .find(|(routed, _)| *routed == name)
            .map(|(_, marker)| *marker)
            .unwrap_or_else(|| panic!("{name} has no handler marker"));
        match dispatch(state, name, params.clone()) {
            Err((code, message)) if code == "malformed" && message.starts_with(marker) => {
                Some(message)
            }
            _ => None,
        }
    }

    /// The published params schema of every operation and the request type
    /// its handler decodes agree: on valid samples, on a wrong-typed member,
    /// on which members are required, and on which members exist at all. A
    /// schema that is looser or tighter than its decoder fails one of these.
    #[test]
    fn every_operation_schema_agrees_with_its_decoder() {
        let artifact = operation_schemas();
        let schemas = artifact["operations"].as_array().unwrap();
        let samples = schema_samples();
        let sampled: std::collections::BTreeSet<_> = samples.iter().map(|s| s.name).collect();
        let served: std::collections::BTreeSet<_> = PUBLIC_OPERATION_NAMES.into_iter().collect();
        assert_eq!(sampled, served, "every served operation needs samples");
        assert_eq!(samples.len(), served.len(), "one sample set per operation");

        let state = test_state();
        for SchemaSamples {
            name,
            valid,
            refused,
        } in &samples
        {
            let schema = &schemas
                .iter()
                .find(|operation| operation["name"] == *name)
                .unwrap_or_else(|| panic!("{name} has no published schema"))["params"];
            // Every request type denies unknown fields, so every published
            // params object is closed.
            assert_eq!(schema["type"], "object", "{name}");
            assert_eq!(schema["additionalProperties"], false, "{name}");

            for params in valid {
                if let Some(message) = decoder_refuses(&state, name, params) {
                    panic!("{name}: the decoder refused a valid sample {params}: {message}");
                }
                if let Err(error) = schema_accepts(schema, params) {
                    panic!("{name}: the schema refused a valid sample {params}: {error}");
                }
            }

            assert!(
                decoder_refuses(&state, name, refused).is_some(),
                "{name}: the decoder accepted the malformed sample {refused}"
            );
            assert!(
                schema_accepts(schema, refused).is_err(),
                "{name}: the schema accepted the malformed sample {refused}"
            );

            // Required members: the first sample names exactly the schema's
            // required set, and dropping any one of them is refused by both.
            let required: std::collections::BTreeSet<&str> = schema["required"]
                .as_array()
                .unwrap()
                .iter()
                .map(|member| member.as_str().unwrap())
                .collect();
            let minimal = valid[0].as_object().unwrap();
            let minimal_members: std::collections::BTreeSet<&str> =
                minimal.keys().map(String::as_str).collect();
            assert_eq!(
                minimal_members, required,
                "{name}: first sample is not minimal"
            );
            for member in &required {
                let mut without = minimal.clone();
                without.remove(*member);
                let without = Value::Object(without);
                assert!(
                    decoder_refuses(&state, name, &without).is_some(),
                    "{name}: the decoder accepted params without required {member}"
                );
                assert!(
                    schema_accepts(schema, &without).is_err(),
                    "{name}: the schema accepted params without required {member}"
                );
            }

            // Listed members: the valid samples name every member the schema
            // lists, so a member the schema invents is sent to the decoder,
            // which refuses unknown members.
            let listed: std::collections::BTreeSet<&str> = schema["properties"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            let named: std::collections::BTreeSet<&str> = valid
                .iter()
                .flat_map(|params| params.as_object().unwrap().keys())
                .map(String::as_str)
                .collect();
            assert_eq!(
                named, listed,
                "{name}: samples and schema name different members"
            );
        }
    }

    #[test]
    fn op_event_reprice_routes_subscription_included_through_coverage() {
        let state = test_state();
        dispatch(
            &state,
            OP_SUBSCRIPTION_SET,
            json!({
                "account_ref":"acct",
                "provider":"provider",
                "plan_name":"plan",
                "recurring_cost_minor":100,
                "currency":"USD",
                "amount_exponent":2,
                "period":"month",
                "effective_from":1
            }),
        )
        .unwrap();
        let event = json!({
            "producer":"producer",
            "producer_lineage":"lineage",
            "producer_ref":"event-1",
            "provider":"provider",
            "account_ref":"acct",
            "occurred_at_ms":10,
            "charge_basis":"subscription_included",
            "measurements":[{
                "role":"input","unit":"token","kind":"raw","source":"usage",
                "quantity_units":1,"quantity_exponent":0
            }]
        });
        let event_id = dispatch(&state, OP_EVENT_INGEST, event).unwrap()["event_id"]
            .as_str()
            .unwrap()
            .to_string();
        let connection = Arc::new(CoverageCatalog::new());
        let catalog =
            astrocyte_core::catalog_client::CatalogClient::connect(connection.clone()).unwrap();
        state.install_catalog(Some(catalog)).unwrap();
        // Boundary discovery is one startup call. Repricing must add no second
        // call because the subscription coverage arm wins before catalog lookup.
        let result = dispatch(&state, OP_EVENT_REPRICE, json!({"event_id":event_id})).unwrap();
        assert_eq!(result["measurements_repriced"], 1);
        assert_eq!(connection.requests.lock().unwrap().len(), 1);
        let covered: String = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT outcome FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                    [&event_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(covered, "covered");
    }

    /// An unknown grouping is the caller's mistake, not a store failure: it
    /// must reach the wire under its own refusal code, the way the other
    /// served reports already report theirs.
    #[test]
    fn spend_report_refusal_keeps_its_code() {
        let state = test_state();
        let (code, message) = dispatch(
            &state,
            OP_REPORT,
            json!({"grain":"month","timezone":"UTC","grouping":["nonsense"]}),
        )
        .unwrap_err();
        assert_eq!(code, "invalid_grouping", "message: {message}");
        assert!(message.contains("nonsense"), "{message}");
    }

    /// A coverage report holds one reader snapshot while ingest commits on the
    /// writer. The open report must not stall the ingest commit. Once the test
    /// releases the paused report thread, it must still see its original snapshot.
    #[test]
    fn open_coverage_report_does_not_stall_an_ingest_commit() {
        use std::sync::mpsc;
        use std::time::Duration;

        let directory = tempfile::TempDir::new().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let count = |conn: &rusqlite::Connection| -> i64 {
            conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
                .unwrap()
        };

        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (seen_tx, seen_rx) = mpsc::channel();
        state.pause_next_read_for_test(move |conn| {
            let before = count(conn);
            held_tx.send(()).unwrap();
            // Bounded so a failing test cannot leave this thread parked.
            let _ = release_rx.recv_timeout(Duration::from_secs(30));
            seen_tx.send((before, count(conn))).unwrap();
        });
        let report_state = state.clone();
        let report =
            std::thread::spawn(move || dispatch(&report_state, OP_LEDGER_COVERAGE, json!({})));
        held_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the coverage report never started its read");

        let (done_tx, done_rx) = mpsc::channel();
        let writer_state = state.clone();
        let writer = std::thread::spawn(move || {
            let outcome = dispatch(
                &writer_state,
                OP_EVENT_INGEST,
                json!({
                    "producer":"producer", "producer_lineage":"lineage", "producer_ref":"during-report",
                    "charge_basis":"unknown", "occurred_at_ms":10,
                    "measurements":[{"role":"input","unit":"token","kind":"raw","source":"usage","quantity_units":1,"quantity_exponent":0}]
                }),
            );
            let _ = done_tx.send(outcome);
        });
        // Longer than the 5 s busy timeout, so a SQLite lock conflict surfaces
        // as an ingest error rather than as this timeout.
        let committed = done_rx.recv_timeout(Duration::from_secs(10));
        // Release the report and join both threads before asserting, so a
        // failed assertion leaves nothing running. The joins are bounded so a
        // thread that never finishes fails by name instead of hanging.
        let _ = release_tx.send(());
        let report = crate::test_support::join_within(report, "the coverage report thread");
        crate::test_support::join_within(writer, "the ingest thread");
        committed
            .expect("the ingest commit waited behind the open coverage report")
            .expect("the ingest must commit while the report is open");

        let (before, after) = seen_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(
            (before, after),
            (0, 0),
            "the report's snapshot must not move"
        );
        report.expect("the coverage report completes");
        let now = state.with_read_op(|conn| Ok(count(conn))).unwrap();
        assert_eq!(now, 1);
    }

    /// The range is the caller's to get right: an empty or inverted one is
    /// refused under its own code, and a well-formed one over an empty series
    /// answers with empty lists rather than an error.
    #[test]
    fn multiplier_report_refuses_a_malformed_range() {
        let state = test_state();
        for params in [
            json!({"from_ms": 10, "to_ms": 10}),
            json!({"from_ms": 20, "to_ms": 10}),
            json!({"from_ms": -5, "to_ms": 10}),
        ] {
            let (code, message) =
                dispatch(&state, OP_MULTIPLIER_REPORT, params.clone()).unwrap_err();
            assert_eq!(code, "invalid_range", "{params}: {message}");
        }
        let (code, _) = dispatch(&state, OP_MULTIPLIER_REPORT, json!({"from_ms": 0})).unwrap_err();
        assert_eq!(code, "malformed", "to_ms is required");
        let report = dispatch(
            &state,
            OP_MULTIPLIER_REPORT,
            json!({"from_ms": 0, "to_ms": 10}),
        )
        .unwrap();
        assert_eq!(report, json!({"latest": [], "series": []}));
    }

    #[test]
    fn multiplier_pace_is_served_from_recorded_rates_on_the_reader() {
        use astrocyte_core::multiplier;
        let directory = tempfile::TempDir::new().unwrap();
        let state = crate::state::leased_test_state(&directory);
        let mut desired = crate::config::intentional_empty().config;
        let config: multiplier::MultiplierConfig = serde_json::from_value(json!({
            "api_twins": {}, "plan_price_overrides": {},
            "quota_providers": {"claude": {"window": {"pool_key": "primary", "window_minutes": 300},
                "scope": [{"producer": "opencode", "provider": "anthropic", "charge_basis": null}], "accounts": {}}}
        })).unwrap();
        desired.subscription_multiplier =
            crate::config::MultiplierSetting::Recorded(config.clone());
        // No catalog is configured: a successful response must use stored rates.
        state.publish_desired(desired, 1);
        dispatch(&state, OP_EVENT_INGEST, json!({"producer": "opencode", "producer_lineage": "lineage", "producer_ref": "call",
            "provider": "anthropic", "model": "model", "charge_basis": "unknown", "occurred_at_ms": 1,
            "work_class": "head", "measurements": [{"role": "input", "unit": "token", "kind": "billed", "source": "usage", "quantity_units": 100, "quantity_exponent": 0}]})).unwrap();
        state.ordered_write(|tx| {
            let mut plan = multiplier::plan_observation(tx, &config, "claude", 0, 86400000).map_err(|error| error.message())?;
            plan.token_interval_ms = Some((0, 86400000));
            let rates = multiplier::ResolvedRates {
                resolved_at_ms: 86400000,
                models: std::collections::BTreeMap::from([(("anthropic".into(), "model".into()),
                    serde_json::from_value(json!({"state": "rates", "base": {"input": {"state": "priced", "units_per_million": 10000000000_i64}}, "tiers": {}})).unwrap())]),
            };
            let mut observation = multiplier::compute_observation(tx, &config, &plan, &rates, &Default::default(), 86400000, &[]).map_err(|error| error.message())?;
            observation.api_nanos = 10_000_000;
            observation.delta_percent = 2.0;
            multiplier::insert_observation_in_tx(tx, &observation).map_err(|error| error.message())?;
            Ok(((), true))
        }).unwrap();
        let response = dispatch(
            &state,
            OP_MULTIPLIER_PACE,
            json!({"quota_provider": "claude", "from_ms": 1, "to_ms": 2}),
        )
        .unwrap();
        assert_eq!(
            response,
            json!({"quota_provider": "claude", "from_ms": 0, "to_ms": 3600000,
            "basis": "pro_rata_list_value", "per_point_nanos": 5000000,
            "observation_day": "1970-01-02", "observation_id": 1, "undated_events": 0,
            "hours": [{"hour_start_ms": 0, "producers": {"opencode": {"events": 1, "list_nanos": 1000000, "estimated_points": 0.2, "unpriced_tokens": 0,
                "by_work_class": {"head": {"events": 1, "list_nanos": 1000000, "estimated_points": 0.2}}}}}]})
        );
        state
            .with_read_op(|conn| {
                assert!(conn.is_readonly(rusqlite::DatabaseName::Main).unwrap());
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM multiplier_observation", [], |row| row
                        .get::<_, i64>(0))
                        .unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn malformed_operation_is_a_refusal_without_a_write() {
        let state = test_state();
        let before = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap();
        let result = dispatch(&state, OP_EVENT_INGEST, json!({"unexpected":true}));
        assert!(result.is_err());
        let after = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(before, after);
    }
}
#[cfg(test)]
mod import_boundary_tests {
    use super::*;
    use crate::state::test_state;
    #[cfg(unix)]
    use std::sync::Arc;

    fn import_params(path: &std::path::Path) -> Value {
        json!({"path": path, "provider": "openai", "account_ref": "a",
            "period_start": 0, "period_end": 100, "amount_exponent": 2, "currency": "USD"})
    }

    fn statement_rows(state: &SharedState) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM statement_import", [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    #[test]
    fn statement_import_reads_a_regular_csv_and_commits_its_line() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("statement.csv");
        std::fs::write(&path, "line_id,occurred_at,description,quantity,unit,amount_minor,currency,correlation_ref\nline-1,50,charge,1,request,2,USD,\n").unwrap();
        let state = test_state();
        let result = dispatch(&state, OP_STATEMENT_IMPORT, import_params(&path)).unwrap();
        assert_eq!(result["lines_created"], 1);
        assert_eq!(statement_rows(&state), 1);
    }

    #[test]
    fn statement_import_refuses_a_directory_without_importing() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = test_state();
        assert_eq!(
            dispatch(&state, OP_STATEMENT_IMPORT, import_params(directory.path()))
                .unwrap_err()
                .0,
            "invalid_request"
        );
        assert_eq!(statement_rows(&state), 0);
    }

    #[cfg(unix)]
    #[test]
    fn statement_import_refuses_a_device_without_importing() {
        let state = test_state();
        assert_eq!(
            dispatch(
                &state,
                OP_STATEMENT_IMPORT,
                import_params(std::path::Path::new("/dev/null"))
            )
            .unwrap_err()
            .0,
            "invalid_request"
        );
        assert_eq!(statement_rows(&state), 0);
    }

    #[test]
    fn statement_read_stops_at_the_limit_even_if_the_file_grows() {
        use std::io::Read;

        // Supply more bytes than the earlier metadata could have reported.
        // One extra byte distinguishes an exact-limit file from an oversized one.
        let mut reader = std::io::repeat(0).take(MAX_STATEMENT_BYTES + 1024);
        let error = read_statement_bytes(&mut reader).unwrap_err();
        assert_eq!(error.0, "invalid_request");
        assert_eq!(
            reader.limit(),
            1023,
            "the reader must stop after the overflow probe byte"
        );
    }

    #[cfg(unix)]
    #[test]
    fn statement_import_refuses_fifo_without_waiting_for_a_writer() {
        let directory = tempfile::TempDir::new().unwrap();
        let fifo = directory.path().join("statement.fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let state = Arc::new(test_state());
        let importing = Arc::clone(&state);
        let params = import_params(&fifo);
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            tx.send(dispatch(&importing, OP_STATEMENT_IMPORT, params))
                .unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(2));
        if result.is_err() {
            // Release a blocked reader before failing so the test never leaves
            // a thread parked on the FIFO.
            drop(std::fs::OpenOptions::new().write(true).open(&fifo).unwrap());
        }
        thread.join().unwrap();
        assert_eq!(statement_rows(&state), 0);
        assert_eq!(
            result
                .expect("FIFO import must return without a writer")
                .unwrap_err()
                .0,
            "invalid_request"
        );
    }

    #[test]
    fn statement_import_refuses_files_over_64_mib_without_importing() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("large.csv");
        std::fs::File::create(&path)
            .unwrap()
            .set_len(64 * 1024 * 1024 + 1)
            .unwrap();
        let state = test_state();
        let error = dispatch(&state, OP_STATEMENT_IMPORT, import_params(&path)).unwrap_err();
        assert_eq!(statement_rows(&state), 0);
        assert_eq!(error.0, "invalid_request");
        assert!(error.1.contains("64 MiB"), "{error:?}");
    }

    #[test]
    fn missing_reprice_event_preserves_not_found_and_writes_nothing() {
        let state = test_state();
        let error = dispatch(&state, OP_EVENT_REPRICE, json!({"event_id": "missing"})).unwrap_err();
        let rows: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(rows, 0);
        assert_eq!(error.0, "not_found");
    }
}
