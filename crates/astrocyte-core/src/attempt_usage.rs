//! Token measurements from the shared model-attempt usage envelope.

use serde_json::{Map, Value};

use crate::ingest::MeasurementInput;

/// The model and disjoint measurements of one entry in an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptModelUsage {
    pub model: String,
    pub measurements: Vec<MeasurementInput>,
}

/// A single-model attempt or the ordered models of a fallback attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptUsage {
    pub by_model: bool,
    pub models: Vec<AttemptModelUsage>,
}

/// Decode the shared attempt envelope without inventing absent measurements.
pub fn decode_attempt_usage(json: &str, model: &str) -> Result<AttemptUsage, String> {
    let envelope: Value = serde_json::from_str(json).map_err(|error| error.to_string())?;
    let envelope = object(&envelope, "attempt usage")?;
    let reasoning_included = envelope
        .get("reasoning_included_in_output")
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| "reasoning_included_in_output must be a boolean".to_string())
        })
        .transpose()?;
    match (envelope.get("usage"), envelope.get("usage_by_model")) {
        (Some(_), None) => Ok(AttemptUsage {
            by_model: false,
            models: vec![decode_model(envelope, model, reasoning_included)?],
        }),
        (None, Some(entries)) => {
            if envelope.contains_key("cache_write_5m_tokens")
                || envelope.contains_key("cache_write_1h_tokens")
            {
                return Err("fallback cache-write splits must be on each model entry".to_string());
            }
            let entries = entries
                .as_array()
                .filter(|entries| !entries.is_empty())
                .ok_or_else(|| "usage_by_model must be a nonempty array".to_string())?;
            let models = entries
                .iter()
                .map(|entry| {
                    let entry = object(entry, "usage_by_model entry")?;
                    let model = entry
                        .get("model_used")
                        .and_then(Value::as_str)
                        .filter(|model| !model.is_empty())
                        .ok_or_else(|| "usage_by_model entry has no model_used".to_string())?;
                    decode_model(entry, model, reasoning_included)
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(AttemptUsage {
                by_model: true,
                models,
            })
        }
        _ => Err("attempt must contain exactly one of usage and usage_by_model".to_string()),
    }
}

fn object<'a>(value: &'a Value, location: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{location} must be an object"))
}

fn count(object: &Map<String, Value>, field: &str) -> Result<Option<u64>, String> {
    object
        .get(field)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| format!("{field} must be a non-negative integer"))
        })
        .transpose()
}

fn decode_model(
    entry: &Map<String, Value>,
    model: &str,
    reasoning_included: Option<bool>,
) -> Result<AttemptModelUsage, String> {
    let usage = entry
        .get("usage")
        .ok_or_else(|| "model entry has no usage object".to_string())?;
    let usage = object(usage, "usage")?;
    for field in usage.keys() {
        if !matches!(
            field.as_str(),
            "input_tokens"
                | "cached_input_tokens"
                | "cache_write_tokens"
                | "output_tokens"
                | "reasoning_tokens"
        ) {
            return Err(format!("unknown usage count {field}"));
        }
    }
    let input = count(usage, "input_tokens")?;
    let cache_read = count(usage, "cached_input_tokens")?;
    let cache_write = count(usage, "cache_write_tokens")?;
    let output = count(usage, "output_tokens")?;
    let reasoning = count(usage, "reasoning_tokens")?;
    let cache_5m = count(entry, "cache_write_5m_tokens")?;
    let cache_1h = count(entry, "cache_write_1h_tokens")?;
    if let (Some(five), Some(hour)) = (cache_5m, cache_1h) {
        let total = five
            .checked_add(hour)
            .ok_or_else(|| "cache-write split sum overflows".to_string())?;
        if cache_write != Some(total) {
            return Err("cache-write split sum must equal cache_write_tokens".to_string());
        }
    }
    if reasoning.is_some() && reasoning_included.is_none() {
        return Err("reasoning_tokens requires reasoning_included_in_output".to_string());
    }
    let (output, output_source) =
        match (output, reasoning, reasoning_included) {
            (Some(output), Some(reasoning), Some(true)) => (
                Some(output.checked_sub(reasoning).ok_or_else(|| {
                    "reasoning_tokens exceeds included output_tokens".to_string()
                })?),
                "usage.output_tokens-usage.reasoning_tokens",
            ),
            (output, _, _) => (output, "usage.output_tokens"),
        };
    let mut measurements = Vec::new();
    for (role, unit, source, quantity) in [
        ("input", "token", "usage.input_tokens", input),
        (
            "cache_read",
            "token",
            "usage.cached_input_tokens",
            cache_read,
        ),
        (
            "cache_write",
            "token",
            "usage.cache_write_tokens",
            if cache_5m.is_some() || cache_1h.is_some() {
                None
            } else {
                cache_write
            },
        ),
        (
            "cache_write",
            "token_cache_5m",
            "cache_write_5m_tokens",
            cache_5m,
        ),
        (
            "cache_write",
            "token_cache_1h",
            "cache_write_1h_tokens",
            cache_1h,
        ),
        ("output", "token", output_source, output),
        ("reasoning", "token", "usage.reasoning_tokens", reasoning),
    ] {
        if let Some(quantity) = quantity {
            let quantity = i64::try_from(quantity)
                .map_err(|_| format!("{source} exceeds stored integer range"))?;
            measurements.push(MeasurementInput {
                role: role.to_string(),
                unit: unit.to_string(),
                kind: "billed".to_string(),
                source: source.to_string(),
                quantity_units: Some(quantity),
                quantity_exponent: Some(0),
            });
        }
    }
    Ok(AttemptModelUsage {
        model: model.to_string(),
        measurements,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quantities(decoded: &AttemptUsage, model: usize) -> Vec<(&str, &str, Option<i64>)> {
        decoded.models[model]
            .measurements
            .iter()
            .map(|measurement| {
                (
                    measurement.role.as_str(),
                    measurement.unit.as_str(),
                    measurement.quantity_units,
                )
            })
            .collect()
    }

    #[test]
    fn attempt_usage_preserves_absence_and_disjoint_reasoning_with_lifetime_cache_writes() {
        let decoded = decode_attempt_usage(
            r#"{"usage":{"input_tokens":17,"cache_write_tokens":12,"output_tokens":11,"reasoning_tokens":4},"cache_write_5m_tokens":5,"cache_write_1h_tokens":7,"reasoning_included_in_output":true}"#,
            "model-a",
        )
        .unwrap();
        assert!(!decoded.by_model);
        assert_eq!(decoded.models[0].model, "model-a");
        assert_eq!(
            quantities(&decoded, 0),
            vec![
                ("input", "token", Some(17)),
                ("cache_write", "token_cache_5m", Some(5)),
                ("cache_write", "token_cache_1h", Some(7)),
                ("output", "token", Some(7)),
                ("reasoning", "token", Some(4)),
            ]
        );
        let empty = decode_attempt_usage(r#"{"usage":{}}"#, "model-a").unwrap();
        assert!(empty.models[0].measurements.is_empty());
        let zero = decode_attempt_usage(
            r#"{"usage":{"cached_input_tokens":0,"reasoning_tokens":0},"reasoning_included_in_output":false}"#,
            "model-a",
        )
        .unwrap();
        assert_eq!(
            quantities(&zero, 0),
            vec![
                ("cache_read", "token", Some(0)),
                ("reasoning", "token", Some(0))
            ]
        );
    }

    #[test]
    fn attempt_usage_fallback_keeps_model_order_and_each_models_measurements() {
        let decoded = decode_attempt_usage(
            r#"{"usage_by_model":[{"model_used":"first","usage":{"input_tokens":2,"output_tokens":8,"reasoning_tokens":3}},{"model_used":"second","usage":{"cache_write_tokens":9,"output_tokens":13},"cache_write_5m_tokens":4,"cache_write_1h_tokens":5}],"reasoning_included_in_output":false,"future_info":true}"#,
            "unused",
        )
        .unwrap();
        assert!(decoded.by_model);
        assert_eq!(
            decoded
                .models
                .iter()
                .map(|item| item.model.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            quantities(&decoded, 0),
            vec![
                ("input", "token", Some(2)),
                ("output", "token", Some(8)),
                ("reasoning", "token", Some(3))
            ]
        );
        assert_eq!(
            quantities(&decoded, 1),
            vec![
                ("cache_write", "token_cache_5m", Some(4)),
                ("cache_write", "token_cache_1h", Some(5)),
                ("output", "token", Some(13))
            ]
        );
    }

    #[test]
    fn attempt_usage_refuses_unreadable_counts_and_envelopes() {
        for json in [
            r#"{}"#,
            r#"{"usage":{},"usage_by_model":[]}"#,
            r#"{"usage_by_model":[]}"#,
            r#"{"usage_by_model":[{"usage":{}}]}"#,
            r#"{"usage":{"reasoning_tokens":0}}"#,
            r#"{"usage":{"output_tokens":3,"reasoning_tokens":4},"reasoning_included_in_output":true}"#,
            r#"{"usage":{"unknown_tokens":1}}"#,
            r#"{"usage":{"input_tokens":null}}"#,
            r#"{"usage":{"input_tokens":-1}}"#,
            r#"{"usage":{"input_tokens":1.5}}"#,
            r#"{"usage":{"input_tokens":9223372036854775808}}"#,
            r#"{"usage":{},"cache_write_5m_tokens":1,"cache_write_1h_tokens":2}"#,
            r#"{"usage":{"cache_write_tokens":4},"cache_write_5m_tokens":1,"cache_write_1h_tokens":2}"#,
            r#"{"usage":{"cache_write_tokens":1},"cache_write_5m_tokens":18446744073709551615,"cache_write_1h_tokens":1}"#,
        ] {
            assert!(
                decode_attempt_usage(json, "model-a").is_err(),
                "accepted {json}"
            );
        }
    }
}
