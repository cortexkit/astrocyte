//! Reader-backed handler for the recorded quota curves.

use crate::{ops::OpResult, state::SharedState};
use astrocyte_core::quota_history::{quota_history, QuotaHistoryRequest};
use serde_json::Value;

// The public operation registry is wired separately from this handler.
#[allow(dead_code)]
pub(crate) fn read(state: &SharedState, params: Value) -> OpResult {
    let request: QuotaHistoryRequest = serde_json::from_value(params)
        .map_err(|error| ("malformed".into(), format!("quota history params: {error}")))?;
    state.with_read_op(|conn| {
        quota_history(conn, &request)
            .map(|history| serde_json::json!(history))
            .map_err(|error| (error.code().into(), error.message()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    const DIR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test-vectors/analytics-quota-history-v1"
    );
    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 24 * HOUR;

    fn seed(state: &SharedState) {
        state.with_conn(|conn| {
            for (pool, at, used, raw, minutes) in [
                ("primary", -1, 97.0, Some(197.0), 300),
                ("primary", 0, 3.0, Some(103.0), 300),
                ("primary", HOUR / 2, 42.0, Some(142.0), 300),
                ("secondary", 0, 65.0, None, 10080),
            ] {
                conn.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,raw_used_percent,window_minutes) VALUES('claude','synthetic-account',?1,?2,?3,?4,?5)", params![pool,at * 1_000_000,used,raw,minutes]).map_err(|e| e.to_string())?;
            }
            conn.execute("INSERT INTO credential_window_sample(provider,credential_ref,pool_key,fetched_at_instant,used_percent,window_minutes) VALUES('codex','synthetic-credential','primary',0,12,300)", []).map_err(|e| e.to_string())?;
            Ok(())
        }).unwrap();
    }

    fn served(state: &SharedState, params: Value) -> Value {
        match read(state, params) {
            Ok(result) => json!({"result": result}),
            Err((code, message)) => json!({"error":{"code":code,"message":message}}),
        }
    }

    fn vectors(state: &SharedState) -> Vec<(&'static str, Value)> {
        vec![
            (
                "history-hour.json",
                served(state, json!({"from_ms":0,"to_ms":HOUR,"grain":"hour"})),
            ),
            (
                "history-day-credential.json",
                served(
                    state,
                    json!({"from_ms":1,"to_ms":HOUR,"grain":"day","quota_providers":["codex"]}),
                ),
            ),
            (
                "invalid-range.json",
                served(state, json!({"from_ms":1,"to_ms":1,"grain":"hour"})),
            ),
            (
                "hour-range-too-large.json",
                served(state, json!({"from_ms":0,"to_ms":31*DAY+1,"grain":"hour"})),
            ),
            (
                "day-range-too-large.json",
                served(state, json!({"from_ms":0,"to_ms":400*DAY+1,"grain":"day"})),
            ),
            (
                "invalid-filter.json",
                served(
                    state,
                    json!({"from_ms":0,"to_ms":HOUR,"grain":"hour","quota_providers":[]}),
                ),
            ),
            (
                "malformed-member.json",
                served(
                    state,
                    json!({"from_ms":0,"to_ms":HOUR,"grain":"hour","timezone":"UTC"}),
                ),
            ),
            (
                "malformed-grain.json",
                served(state, json!({"from_ms":0,"to_ms":HOUR,"grain":"week"})),
            ),
            (
                "malformed-required.json",
                served(state, json!({"from_ms":0,"grain":"hour"})),
            ),
        ]
    }

    #[test]
    fn quota_history_vectors_pin_real_handler_and_checksums() {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        seed(&state);
        let changes = state.with_conn(|conn| Ok(conn.total_changes())).unwrap();
        let vectors = vectors(&state);
        let hour = &vectors[0].1["result"];
        assert_eq!(hour["windows"].as_array().unwrap().len(), 3);
        assert_eq!(hour["windows"][0]["buckets"][0]["used_percent_max"], 142.0);
        assert_eq!(hour["windows"][0]["buckets"][0]["samples"], 2);
        assert_eq!(
            hour["windows"][0]["resets"],
            json!([{"at_ms":0,"from_percent":197.0,"to_percent":103.0}])
        );
        assert_eq!(hour["windows"][2]["account"], Value::Null);
        assert_eq!(hour["windows"][2]["credential"], "synthetic-credential");
        assert_eq!(hour["earliest_sample_ms"], -1);
        assert_eq!(vectors[1].1["result"]["earliest_sample_ms"], 0);
        assert_eq!(
            state.with_conn(|conn| Ok(conn.total_changes())).unwrap(),
            changes
        );
        let export = std::env::var_os("ASTROCYTE_EXPORT_QUOTA_HISTORY_VECTORS").is_some();
        let mut hashes = String::new();
        for (name, value) in vectors {
            let mut bytes = serde_json::to_vec(&value).unwrap();
            bytes.push(b'\n');
            hashes.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
            if export {
                std::fs::write(format!("{DIR}/{name}"), bytes).unwrap();
            } else {
                assert_eq!(
                    std::fs::read(format!("{DIR}/{name}")).unwrap(),
                    bytes,
                    "{name}"
                );
            }
        }
        hashes.push_str(&format!(
            "{:x}  README.md\n",
            Sha256::digest(std::fs::read(format!("{DIR}/README.md")).unwrap())
        ));
        if export {
            std::fs::write(format!("{DIR}/SHA256SUMS"), hashes).unwrap();
        } else {
            assert_eq!(
                std::fs::read_to_string(format!("{DIR}/SHA256SUMS")).unwrap(),
                hashes
            );
        }
    }
}
