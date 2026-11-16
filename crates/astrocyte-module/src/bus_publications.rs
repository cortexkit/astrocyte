//! Post-commit mirrors. Writers only replace desired values in memory; the existing
//! bus reader owns all network work, so a broken bus cannot hold a ledger writer.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::NatsConnection;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::state::SharedState;

type Values = BTreeMap<String, Vec<u8>>;

#[derive(Clone, Default)]
struct Desired {
    #[cfg(test)]
    generation: u64,
    steps: Option<String>,
    multiplier: Option<Values>,
    watermark: Option<Option<Vec<u8>>>,
    acknowledgements: BTreeMap<String, Values>,
}

#[derive(Clone, Default)]
pub(crate) struct Control {
    desired: Arc<Mutex<Desired>>,
    changed: Arc<Notify>,
    multiplier_snapshot: Arc<Mutex<()>>,
    watermark_snapshot: Arc<Mutex<()>>,
    warnings: Arc<Mutex<Warnings>>,
    #[cfg(test)]
    attempts: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    attempted: Arc<Notify>,
    #[cfg(test)]
    processed_generation: Arc<std::sync::atomic::AtomicU64>,
}

/// Encode every component, including valid ASCII ones, to avoid escape collisions
/// and to keep dots in opaque ids from becoming key separators.
pub(crate) fn key_component(value: &str) -> String {
    use std::fmt::Write;
    let mut encoded = String::from("x");
    for byte in value.bytes() {
        write!(encoded, "{byte:02x}").expect("writing to a String");
    }
    encoded
}

fn pair_key(provider: &str, plan: &str) -> String {
    format!("{}.{}", key_component(provider), key_component(plan))
}

impl Control {
    #[cfg(test)]
    pub(crate) fn clear_multiplier_for_test(&self) {
        self.desired.lock().unwrap().multiplier = None;
    }
    fn update(&self, change: impl FnOnce(&mut Desired) -> bool) {
        let mut desired = self.desired.lock().unwrap_or_else(|p| p.into_inner());
        if change(&mut desired) {
            #[cfg(test)]
            {
                desired.generation = desired.generation.saturating_add(1);
            }
            self.changed.notify_one();
        }
    }

    pub(crate) fn steps(&self, cursor: String) {
        self.update(|desired| {
            if desired.steps.as_ref() == Some(&cursor) {
                return false;
            }
            desired.steps = Some(cursor);
            true
        });
    }

    pub(crate) fn multiplier(&self, latest: &[Value]) {
        let values = latest
            .iter()
            .filter_map(|entry| {
                let provider = entry["quota_provider"].as_str()?;
                let key = match &entry["plan"] {
                    Value::Null => format!("{}.n", key_component(provider)),
                    Value::String(plan) => pair_key(provider, plan),
                    _ => return None,
                };
                Some((
                    key,
                    serde_json::to_vec(entry).expect("JSON value serialization"),
                ))
            })
            .collect();
        self.update(|desired| {
            if desired.multiplier.as_ref() == Some(&values) {
                return false;
            }
            desired.multiplier = Some(values);
            true
        });
    }

    pub(crate) fn watermark(&self, value: Option<Value>) {
        let value =
            value.map(|value| serde_json::to_vec(&value).expect("JSON value serialization"));
        self.update(|desired| {
            if desired.watermark.as_ref() == Some(&value) {
                return false;
            }
            desired.watermark = Some(value);
            true
        });
    }

    pub(crate) fn acknowledgements<'a>(
        &self,
        source: &str,
        entries: impl IntoIterator<Item = (&'a str, i64)>,
    ) {
        let prefix = format!("thalamus.ack.{}.", key_component(source));
        let values = entries
            .into_iter()
            .map(|(session, through_ms)| {
                (
                    format!("{prefix}{}", key_component(session)),
                    serde_json::to_vec(
                        &json!({"source":source,"session":session,"through_ms":through_ms}),
                    )
                    .expect("JSON value serialization"),
                )
            })
            .collect();
        self.update(|desired| {
            if desired.acknowledgements.get(&prefix) == Some(&values) {
                return false;
            }
            desired.acknowledgements.insert(prefix, values);
            true
        });
    }

    /// Share the existing credential/session. One future serializes all sends and
    /// snapshots again after each wake: commits during a send overwrite its successor.
    pub(crate) async fn follow(&self, connection: &NatsConnection, names: &AccountNames) {
        let mut multiplier = Mirror {
            warnings: self.warnings.clone(),
            ..Mirror::default()
        };
        let mut read_marks = Mirror {
            warnings: self.warnings.clone(),
            ..Mirror::default()
        };
        let mut sent_cursor = None;
        loop {
            let changed = self.changed.notified();
            let desired = self
                .desired
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if desired.steps != sent_cursor {
                if let Some(cursor) = &desired.steps {
                    let result = publish_steps(connection, names, cursor).await;
                    log_streak(&self.warnings, result.is_err(), "step hint");
                    if result.is_ok() {
                        sent_cursor = desired.steps;
                    }
                }
            }
            if let Some(values) = desired.multiplier {
                multiplier
                    .apply(
                        connection,
                        names,
                        "multiplier",
                        &values,
                        None,
                        declared_value_cap("multiplier"),
                    )
                    .await;
            }
            let mut values = Values::new();
            let watermark_known = desired.watermark.is_some();
            if let Some(Some(watermark)) = desired.watermark {
                values.insert("thalamus.watermark".into(), watermark);
            }
            for ack in desired.acknowledgements.values() {
                values.extend(ack.clone());
            }
            let mut prefixes: Vec<_> = desired.acknowledgements.keys().cloned().collect();
            if watermark_known {
                prefixes.push("thalamus.watermark".into());
            }
            if !values.is_empty() || !prefixes.is_empty() {
                read_marks
                    .apply(
                        connection,
                        names,
                        "read-marks",
                        &values,
                        Some(&prefixes),
                        declared_value_cap("read-marks"),
                    )
                    .await;
            }
            #[cfg(test)]
            {
                self.processed_generation
                    .store(desired.generation, std::sync::atomic::Ordering::Release);
                self.attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
                self.attempted.notify_one();
            }
            changed.await;
        }
    }
}

/// The largest value the bus accepts in one of astrocyte's buckets, as
/// bus-naming declares it. ck-bus provisions each bucket from the same
/// declaration, so a value larger than this would be refused by the server;
/// skipping it here keeps one oversized value from failing the whole mirror.
fn declared_value_cap(bucket: &str) -> usize {
    cortexkit_bus_naming::MODULE_BUCKETS
        .iter()
        .find(|row| row.owner == "astrocyte" && row.name == bucket)
        .and_then(|row| usize::try_from(row.max_value_bytes).ok())
        .unwrap_or_else(|| panic!("bus-naming declares no astrocyte bucket {bucket}"))
}

#[derive(Default)]
struct Warnings {
    failed: BTreeSet<&'static str>,
    oversized: BTreeMap<&'static str, BTreeSet<String>>,
}

fn log_streak(warnings: &Mutex<Warnings>, failure: bool, publication: &'static str) {
    let mut warnings = warnings.lock().unwrap_or_else(|p| p.into_inner());
    if failure && warnings.failed.insert(publication) {
        // Network errors can contain endpoints or credentials; keep the warning structural.
        tracing::warn!(target: "astrocyte", publication, "bus publication deferred; the next change will repair it");
    }
    if !failure {
        warnings.failed.remove(publication);
    }
}

async fn publish_steps(
    connection: &NatsConnection,
    names: &AccountNames,
    cursor: &str,
) -> Result<(), ()> {
    let subject = names
        .live_subject("astrocyte", "steps_committed", 1)
        .map_err(|_| ())?;
    let client = connection.client();
    if client.connection_state() != async_nats::connection::State::Connected {
        return Err(());
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        client
            .publish(
                subject,
                serde_json::to_vec(&json!({"cursor":cursor}))
                    .map_err(|_| ())?
                    .into(),
            )
            .await
            .map_err(|_| ())?;
        client.flush().await.map_err(|_| ())
    })
    .await
    .map_err(|_| ())?
}

#[derive(Default)]
struct Mirror {
    values: Option<Values>,
    warnings: Arc<Mutex<Warnings>>,
}

impl Mirror {
    async fn apply(
        &mut self,
        connection: &NatsConnection,
        names: &AccountNames,
        bucket: &'static str,
        desired: &Values,
        prefixes: Option<&[String]>,
        cap: usize,
    ) {
        let mut oversized = BTreeSet::new();
        {
            let mut warnings = self.warnings.lock().unwrap_or_else(|p| p.into_inner());
            let previous = warnings.oversized.entry(bucket).or_default();
            for (key, value) in desired.iter().filter(|(_, value)| value.len() > cap) {
                if !previous.contains(key) {
                    tracing::warn!(target: "astrocyte", bucket, key, bytes = value.len(),
                    "bus publication omitted: value exceeds bucket limit");
                }
                oversized.insert(key.clone());
            }
            *previous = oversized;
        }
        let desired: Values = desired
            .iter()
            .filter(|(_, value)| value.len() <= cap)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            self.reconcile(connection, names, bucket, &desired, prefixes),
        )
        .await;
        let failed = !matches!(result, Ok(Ok(())));
        if failed {
            self.values = None;
        }
        log_streak(&self.warnings, failed, bucket);
    }

    async fn reconcile(
        &mut self,
        connection: &NatsConnection,
        names: &AccountNames,
        bucket: &str,
        desired: &Values,
        prefixes: Option<&[String]>,
    ) -> Result<(), ()> {
        let client = connection.client();
        if client.connection_state() != async_nats::connection::State::Connected {
            return Err(());
        }
        let bucket = names
            .module_bucket_name("astrocyte", bucket)
            .map_err(|_| ())?;
        let store = async_nats::jetstream::new(client)
            .get_key_value(bucket)
            .await
            .map_err(|_| ())?;
        if self.values.is_none() {
            let mut values = Values::new();
            let mut keys = store.keys().await.map_err(|_| ())?;
            while let Some(key) = keys.next().await {
                let key = key.map_err(|_| ())?;
                if let Some(value) = store.get(&key).await.map_err(|_| ())? {
                    values.insert(key, value.to_vec());
                }
            }
            self.values = Some(values);
        }
        let values = self.values.as_mut().expect("mirror loaded");
        let removed: Vec<_> = values
            .keys()
            .filter(|key| {
                let managed = prefixes
                    .is_none_or(|prefixes| prefixes.iter().any(|prefix| key.starts_with(prefix)));
                managed && !desired.contains_key(*key)
            })
            .cloned()
            .collect();
        // THALAMUS may delete a session folder only after acknowledgement. Revoke
        // removed keys before replacements, and omit oversized values rather than
        // truncating the evidence THALAMUS uses to decide whether deletion is safe.
        for key in removed {
            store.delete(&key).await.map_err(|_| ())?;
            values.remove(&key);
        }
        for (key, value) in desired {
            if values.get(key) != Some(value) {
                store.put(key, value.clone().into()).await.map_err(|_| ())?;
                values.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    }
}

pub(crate) fn publish_multiplier(state: &SharedState) {
    // Serialize reading and staging together, so a slower caller cannot overwrite
    // a newer committed snapshot after another caller has already staged it.
    let _snapshot = state
        .bus_publications()
        .multiplier_snapshot
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let result = state.with_read_op(|conn| {
        astrocyte_core::multiplier::multiplier_report(
            conn,
            &astrocyte_core::multiplier::MultiplierReportRequest {
                from_ms: 0,
                to_ms: 1,
            },
        )
        .map_err(|error| (error.code().to_string(), error.message()))
    });
    if let Ok(report) = result {
        log_streak(
            &state.bus_publications().warnings,
            false,
            "multiplier snapshot",
        );
        if let Some(latest) = report["latest"].as_array() {
            state.bus_publications().multiplier(latest);
        }
    } else {
        log_streak(
            &state.bus_publications().warnings,
            true,
            "multiplier snapshot",
        );
    }
}

pub(crate) fn publish_watermark(state: &SharedState) {
    if state
        .with_conn(|connection| {
            publish_watermark_on_connection(state, connection);
            Ok(())
        })
        .is_err()
    {
        log_streak(
            &state.bus_publications().warnings,
            true,
            "watermark snapshot",
        );
    }
}

/// Read and stage the watermark after its transaction commits, while the caller
/// still holds the writer connection. Holding it through the in-memory snapshot
/// update prevents an older watermark from replacing a newer one.
pub(crate) fn publish_watermark_on_connection(
    state: &SharedState,
    connection: &rusqlite::Connection,
) {
    let _snapshot = state
        .bus_publications()
        .watermark_snapshot
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let principal = subc_protocol::Principal::Reserved {
        module_id: "thalamus-harness".into(),
    };
    let desired = state.desired_config();
    let result = crate::ops::watermark_source(&desired, Some(&principal))
        .and_then(|source| crate::ops::watermark_on_connection(connection, &desired, source));
    match result {
        Ok(value) => {
            log_streak(
                &state.bus_publications().warnings,
                false,
                "watermark snapshot",
            );
            state.bus_publications().watermark(Some(value));
        }
        Err((code, _)) if code == cortexkit_role_llm_runner::watermark::NOT_A_READ_SOURCE => {
            state.bus_publications().watermark(None);
        }
        Err(_) => log_streak(
            &state.bus_publications().warnings,
            true,
            "watermark snapshot",
        ),
    }
}

#[cfg(test)]
pub(crate) mod tests;
