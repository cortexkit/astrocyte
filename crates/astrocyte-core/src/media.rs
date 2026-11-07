//! Lossless mapping of the media provider's immutable usage records.

use std::collections::{BTreeMap, BTreeSet};

use cortexkit_role_media::usage::{self, AuthMethod, ChargeBasis, Measure, UsageRecord};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::ingest::{EventRecordRequest, MeasurementInput};

/// Producer-stated facts stored on the event and compared on every redelivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaFacts {
    pub selectors: BTreeMap<String, String>,
    pub failure: Option<Value>,
}

pub(crate) fn read_facts(
    row: &rusqlite::Row<'_>,
    column: usize,
) -> rusqlite::Result<Option<MediaFacts>> {
    row.get::<_, Option<String>>(column)?
        .map(|text| {
            serde_json::from_str(&text).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    column,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()
}

/// `None` is local work: it contributes coverage, but is not a spend event.
pub fn map_record(
    record: &UsageRecord,
) -> Result<Option<(EventRecordRequest, MediaFacts)>, String> {
    usage::check_usage_record(record).map_err(|error| format!("invalid media record: {error}"))?;
    if record.charge_basis == ChargeBasis::Local {
        return Ok(None);
    }
    let basis = match (&record.charge_basis, &record.auth_method) {
        (ChargeBasis::Vendor, Some(AuthMethod::ApiKey)) => "metered_api",
        (ChargeBasis::Vendor, Some(AuthMethod::Oauth)) => "subscription_included",
        _ => "unknown",
    };
    let mut identities = BTreeSet::new();
    let measurements = record
        .measurements
        .iter()
        .map(|measurement| {
            if !identities.insert((measurement.role.as_str(), measurement.unit.as_str())) {
                return Err("duplicate measurement role and unit".to_string());
            }
            let (units, exponent) = decimal_quantity(&measurement.quantity).map_err(|reason| {
                format!(
                    "measurement {} {} quantity {}: {reason}",
                    measurement.role.as_str(),
                    measurement.unit.as_str(),
                    measurement.quantity
                )
            })?;
            Ok(MeasurementInput {
                role: measurement.role.as_str().to_owned(),
                unit: measurement.unit.as_str().to_owned(),
                kind: match measurement.measure {
                    Measure::Billed => "billed",
                    Measure::Observed => "observed",
                    _ => return Err("unsupported measurement kind".into()),
                }
                .into(),
                source: serde_json::to_value(measurement.source)
                    .map_err(|error| error.to_string())?
                    .as_str()
                    .ok_or("unsupported measurement source")?
                    .to_string(),
                quantity_units: Some(units),
                quantity_exponent: Some(exponent),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let started = i64::try_from(record.started_at_ms).map_err(|_| "started_at_ms out of range")?;
    let finished =
        i64::try_from(record.finished_at_ms).map_err(|_| "finished_at_ms out of range")?;
    let request = serde_json::from_value(json!({
        "producer": "colliculus",
        "producer_ref": record.record_id,
        "run_ref": record.job_id.as_ref().or(record.stream_id.as_ref()).or(record.call_id.as_ref()),
        "work_class": record.principal,
        "provider": record.provider,
        "model": record.model,
        "auth_selection": record.credential_ref,
        "vendor_request_id": record.vendor_request_id,
        "charge_basis": basis,
        "occurred_at_ms": started,
        "emitted_at_ms": finished,
        "terminal_status": record.status,
        "measurements": measurements,
    }))
    .map_err(|error| format!("media event mapping: {error}"))?;
    Ok(Some((
        request,
        MediaFacts {
            selectors: record.selectors.clone(),
            failure: record.failure.as_ref().map(|failure| json!(failure)),
        },
    )))
}

fn decimal_quantity(quantity: &str) -> Result<(i64, i32), String> {
    if !usage::decimal_is_valid(quantity) {
        return Err("invalid decimal quantity".into());
    }
    let (whole, fraction) = quantity.split_once('.').unwrap_or((quantity, ""));
    // A 0.1.0 producer may send quantities finer than the ledger stores. Remove
    // only insignificant zeroes; refusing the whole record preserves its evidence
    // without rounding a billed quantity or replacing a missing fact with zero.
    let fraction = fraction.trim_end_matches('0');
    if fraction.len() > 9 {
        return Err("quantity not representable as i64 units with exponent -9..0".into());
    }
    let coefficient = format!("{whole}{fraction}")
        .parse::<i64>()
        .map_err(|_| "quantity not representable as i64 units with exponent -9..0".to_string())?;
    let exponent = -i32::try_from(fraction.len()).map_err(|_| "quantity exponent out of range")?;
    Ok((coefficient, exponent))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{record_media_event_in_tx, RecordDisposition};
    use crate::store::MIGRATIONS;
    use rusqlite::Connection;

    pub(crate) fn record(value: Value) -> UsageRecord {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn media_usage_vectors_map_without_rounding_or_invented_measurements() {
        let vectors: Value =
            serde_json::from_str(cortexkit_role_media::vectors::file("usage.json").unwrap())
                .unwrap();
        for value in vectors["records"].as_array().unwrap() {
            let record: UsageRecord = serde_json::from_value(value.clone()).unwrap();
            let mapped = map_record(&record);
            match record.record_id.as_str() {
                "r-done" | "r-ended" => assert!(mapped.unwrap_err().contains("not representable")),
                "r-local" => assert!(mapped.unwrap().is_none()),
                "r-failed" | "r-cancelled" => {
                    let (event, facts) = mapped.unwrap().unwrap();
                    assert_eq!(event.producer, "colliculus");
                    assert_eq!(event.producer_lineage, None);
                    assert_eq!(event.producer_ref, record.record_id);
                    assert_eq!(event.work_class.as_deref(), Some("person-a"));
                    assert_eq!(event.occurred_at_ms, Some(1));
                    assert_eq!(event.emitted_at_ms, Some(2));
                    assert_eq!(event.measurements.len(), record.measurements.len());
                    if record.record_id == "r-failed" {
                        assert_eq!(event.run_ref.as_deref(), Some("stream-1"));
                        assert_eq!(event.terminal_status.as_deref(), Some("failed"));
                        assert_eq!(facts.failure.unwrap()["code"], "routing_refused");
                        assert!(event.measurements.is_empty());
                    } else {
                        assert_eq!(event.run_ref.as_deref(), Some("job-1"));
                        assert_eq!(event.terminal_status.as_deref(), Some("cancelled"));
                        assert_eq!(event.measurements[0].kind, "observed");
                        assert_eq!(event.measurements[0].quantity_units, Some(0));
                    }
                }
                other => panic!("unasserted vector {other}"),
            }
        }
        for (quantity, expected) in [
            ("90", (90, 0)),
            ("1.230000000000", (123, -2)),
            ("0.000000001", (1, -9)),
            ("9223372036854775807", (i64::MAX, 0)),
        ] {
            assert_eq!(decimal_quantity(quantity).unwrap(), expected);
        }
        assert!(decimal_quantity("9223372036854775808").is_err());
        assert!(decimal_quantity("0.0000000001").is_err());
    }

    #[test]
    fn media_migration_rehearses_empty_and_existing_store_without_rewriting_events() {
        let empty = crate::test_support::conn();
        assert_eq!(
            empty
                .query_row("SELECT count(*) FROM usage_event", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        for migration in MIGRATIONS.iter().filter(|migration| migration.version < 37) {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,charge_basis) VALUES('old','old','old',1,'unknown')", []).unwrap();
        let before = conn.total_changes();
        conn.execute_batch(
            MIGRATIONS
                .iter()
                .find(|migration| migration.version == 37)
                .unwrap()
                .statements,
        )
        .unwrap();
        assert_eq!(
            conn.total_changes(),
            before,
            "migration must not rewrite rows"
        );
        assert_eq!(
            conn.query_row(
                "SELECT media_facts FROM usage_event WHERE event_id='old'",
                [],
                |row| row.get::<_, Option<String>>(0)
            )
            .unwrap(),
            None
        );
        assert!(conn
            .execute(
                "UPDATE usage_event SET media_facts='{}' WHERE event_id='old'",
                []
            )
            .is_err());
        assert!(conn
            .execute(
                "UPDATE usage_event SET producer='changed' WHERE event_id='old'",
                []
            )
            .is_err());
        assert!(conn
            .execute("DELETE FROM usage_event WHERE event_id='old'", [])
            .is_err());
    }

    #[test]
    fn media_selectors_and_failure_are_immutable_redelivery_facts() {
        let mut conn = crate::test_support::conn();
        let record = record(
            json!({"record_id":"r","principal":"p","call_id":"c","provider":"example","model":"speech","charge_basis":"vendor","auth_method":"api_key","vendor_request_id":"v","started_at_ms":1,"finished_at_ms":2,"status":"failed","failure":{"code":"engine_crashed","class":"transient","message":"stopped"},"selectors":{"quality":"high"},"measurements":[{"role":"input_audio","unit":"second","quantity":"90","measure":"billed","source":"vendor_reply"}]}),
        );
        let (event, facts) = map_record(&record).unwrap().unwrap();
        let tx = conn.transaction().unwrap();
        let first = record_media_event_in_tx(&tx, &event, &facts, 3).unwrap();
        assert_eq!(first.disposition, RecordDisposition::Recorded);
        assert_eq!(
            record_media_event_in_tx(&tx, &event, &facts, 4)
                .unwrap()
                .disposition,
            RecordDisposition::ExactRedelivery
        );
        let mut changed = facts.clone();
        changed.selectors.insert("quality".into(), "low".into());
        assert_eq!(
            record_media_event_in_tx(&tx, &event, &changed, 5)
                .unwrap()
                .disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        changed = facts.clone();
        changed.failure = None;
        assert_eq!(
            record_media_event_in_tx(&tx, &event, &changed, 6)
                .unwrap()
                .disposition,
            RecordDisposition::QuarantinedRedelivery
        );
        tx.commit().unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM usage_event", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(conn
            .execute("UPDATE usage_event SET media_facts='{}'", [])
            .is_err());
        assert_eq!(
            conn.query_row("SELECT vendor_request_id FROM usage_event", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            "v"
        );
    }
}
