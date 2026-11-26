//! The request/reply adapter between Astrocyte operations and the serve helper.

use crate::ops;
use serde::{de::IgnoredAny, Deserialize};
use serde_json::json;
use subc_client_rs::HandlerOutcome;
use subc_protocol::ErrorBody;

const BODY_LIMIT: usize = subc_protocol::MAX_DELIVERED_FRAME_LEN - subc_protocol::HEADER_LEN;

pub fn from_dispatch(method: &str, outcome: ops::OpResult) -> HandlerOutcome {
    match outcome {
        Ok(result) => match serde_json::to_vec(&json!({ "result": result })) {
            Ok(body) => HandlerOutcome::Response(body),
            Err(error) => {
                tracing::error!(target: "astrocyte", method, %error,
                    "route reply could not be encoded");
                HandlerOutcome::Error {
                    code: "internal".into(),
                    message: "request could not be completed".into(),
                }
            }
        },
        Err((code, message)) => HandlerOutcome::Error { code, message },
    }
}

pub fn finish(method: &str, outcome: HandlerOutcome) -> HandlerOutcome {
    finish_with_limit(method, outcome, BODY_LIMIT)
}

pub(crate) fn finish_with_limit(
    method: &str,
    outcome: HandlerOutcome,
    limit_bytes: usize,
) -> HandlerOutcome {
    if !ops::PUBLIC_OPERATION_NAMES.contains(&method)
        || ops::LEDGER_WRITE_OPERATIONS.contains(&method)
        || matches!(method, ops::OP_STEPS_SUBSCRIBE | ops::OP_RUNS_SUBSCRIBE)
    {
        return outcome;
    }

    // The serve helper forwards Response bytes as-is. For an Error it encodes
    // ErrorBody itself; use the same type and encoder to count those bytes.
    let reply_bytes = match &outcome {
        HandlerOutcome::Response(body) => Some(body.len()),
        HandlerOutcome::Error { code, message } => error_len(ErrorBody::new(code, message)),
        HandlerOutcome::ErrorWithDetail {
            code,
            message,
            detail,
        } => error_len(ErrorBody::new(code, message).with_detail(detail.clone())),
        HandlerOutcome::Streamed => None,
    };
    let Some(reply_bytes) = reply_bytes.filter(|bytes| *bytes > limit_bytes) else {
        return outcome;
    };

    let mut detail = json!({ "reply_bytes": reply_bytes, "limit_bytes": limit_bytes });
    if let HandlerOutcome::Response(body) = outcome {
        // Only oversized replies need to be inspected. Skip row contents
        // rather than allocating a second copy of the report.
        if let Ok(Envelope {
            result: Rows { rows: Some(rows) },
        }) = serde_json::from_slice(&body)
        {
            detail["rows"] = json!(rows.len());
        }
    }
    HandlerOutcome::ErrorWithDetail {
        code: "reply_too_large".into(),
        message: "Read reply exceeds the frame limit; narrow the request".into(),
        detail,
    }
}

fn error_len(error: ErrorBody) -> Option<usize> {
    serde_json::to_vec(&error).ok().map(|body| body.len())
}

#[derive(Deserialize)]
struct Envelope {
    result: Rows,
}

#[derive(Deserialize)]
struct Rows {
    rows: Option<Vec<IgnoredAny>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_reply_uses_exact_body_size_and_keeps_refusal_code() {
        let method = ops::OP_REPORT;
        let result = json!({"rows": [{"model": "µ\nquoted\""}, {"model": null}]});
        let body = serde_json::to_vec(&json!({"result": result})).unwrap();
        let reply = || from_dispatch(method, Ok(result.clone()));
        for limit in [body.len() + 1, body.len()] {
            let HandlerOutcome::Response(actual) = finish_with_limit(method, reply(), limit) else {
                panic!("a reply at or under the limit must fit");
            };
            assert_eq!(actual, body);
        }
        let limit = body.len() - 1;
        let HandlerOutcome::ErrorWithDetail { code, detail, .. } =
            finish_with_limit(method, reply(), limit)
        else {
            panic!("a read reply one byte over the limit must be refused");
        };
        assert_eq!(code, "reply_too_large");
        assert_eq!(
            detail,
            json!({"reply_bytes": body.len(), "limit_bytes": limit, "rows": 2})
        );
    }

    #[test]
    fn read_reply_bounds_special_unary_reads_but_not_writes_or_watches() {
        let refusal = ErrorBody::new("malformed", "\"escaped\"").with_detail(json!({"error": "µ"}));
        let bytes = serde_json::to_vec(&refusal).unwrap().len();
        let reply = || HandlerOutcome::ErrorWithDetail {
            code: refusal.code.clone(),
            message: refusal.message.clone(),
            detail: refusal.detail.clone().unwrap(),
        };
        for method in [
            ops::OP_STEPS_READ,
            ops::OP_WATERMARK_GET,
            ops::OP_QUOTA_HISTORY,
            ops::OP_VERDICT,
        ] {
            let HandlerOutcome::ErrorWithDetail { code, detail, .. } =
                finish_with_limit(method, reply(), bytes - 1)
            else {
                panic!("expected a sized refusal for {method}");
            };
            assert_eq!(code, "reply_too_large");
            assert_eq!(
                detail,
                json!({"reply_bytes": bytes, "limit_bytes": bytes - 1})
            );
        }
        for method in [
            ops::OP_EVENT_INGEST,
            ops::OP_STEPS_SUBSCRIBE,
            ops::OP_RUNS_SUBSCRIBE,
        ] {
            let HandlerOutcome::ErrorWithDetail { code, .. } =
                finish_with_limit(method, reply(), bytes - 1)
            else {
                panic!("expected original refusal for {method}");
            };
            assert_eq!(code, "malformed");
        }
    }
}
