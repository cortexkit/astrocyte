//! Map QTA's `usage.get` wire array (the shared `cortexkit-provider-usage`
//! types) into astrocyte's normalized `CapacitySnapshot`.
//!
//! This is the capacity axis's boundary layer, the analogue of `wire_map` for
//! the broca seam: the wire types stay at the edge, core sees only its own
//! normalized shape. We depend on the SHARED crate (not a mirror), so the wire
//! spelling cannot drift without a shared-crate PR every consumer reviews.
//!
//! Two producer semantics handled here (documented on the shared types as
//! producer behavior, enforced by the consumer):
//!   - DEGRADED entries (`error` set) are skipped and counted, never parsed as
//!     windows (QTA semantic #3).
//!   - account=null (normal for providers without vault identity) is not an
//!     account. Its windows become provider-current readings, a separate
//!     population with no account_ref, so they stay visible but can never join
//!     to spend attributed to an account. No sentinel account is minted.

use astrocyte_core::capacity::{validate_preserved_reading, CapacitySnapshot, CapacityWindow};
use cortexkit_provider_usage::{ProviderUsage, RateWindow};

/// Validates and maps one response. Malformed response structure returns an error,
/// while invalid account windows can be discarded without losing other readings.
pub fn map_usage_array(
    entries: &[ProviderUsage],
    complete_providers: Option<std::collections::BTreeSet<String>>,
) -> Result<CapacitySnapshot, String> {
    use astrocyte_core::capacity::{CapacityObservation, CapacityProviderReading};
    use std::collections::{BTreeMap, BTreeSet};

    // The checks below prove each accepted value fits SQLite's signed integer.
    #[allow(clippy::cast_possible_truncation)]
    fn count(value: Option<f64>) -> Result<Option<i64>, String> {
        value
            .map(|value| {
                if !value.is_finite() || value.fract() != 0.0 {
                    return Err("capacity count is not an integer".to_string());
                }
                if value < i64::MIN as f64 || value >= 2_f64.powi(63) {
                    return Err("capacity count exceeds SQLite range".to_string());
                }
                Ok(value as i64)
            })
            .transpose()
    }

    fn valid_window(window: &RateWindow) -> bool {
        window.used_percent.is_finite()
            && (0.0..=100.0).contains(&window.used_percent)
            && window.raw_used_percent.is_none_or(f64::is_finite)
    }

    let mut windows = Vec::new();
    let mut observations = Vec::new();
    let mut provider_readings = Vec::new();
    let mut account_pool_counts = BTreeMap::<(String, String, String), usize>::new();
    let mut invalid_account_pools = BTreeSet::<(String, String, String)>::new();
    // This is the number of degraded labeled accounts, not failed credentials. Degraded entries
    // without an account identity are intentionally excluded because the health signal can name
    // only accounts; treating this as a credential count would understate identity-less handles.
    let mut degraded_entries = 0_u64;

    for entry in entries {
        if entry.provider.is_empty() {
            return Err("capacity entry has empty provider".into());
        }
        if let Some(error_class) = &entry.error_class {
            if error_class.is_empty() {
                return Err("capacity entry has empty errorClass".into());
            }
            if let Some(account_ref) = &entry.account {
                if account_ref.is_empty() {
                    return Err("capacity entry has empty account identity".into());
                }
                degraded_entries = degraded_entries
                    .checked_add(1)
                    .ok_or_else(|| "degraded entry count overflow".to_string())?;
                observations.push(CapacityObservation {
                    provider: entry.provider.clone(),
                    api_provider: entry.api_provider.clone(),
                    account_ref: account_ref.clone(),
                    observation_state: "degraded".into(),
                    error_class: Some(error_class.clone()),
                    error_detail: entry.error.clone(),
                    source: entry.source.clone(),
                    fetched_at: entry.fetched_at.clone(),
                });
            }
            continue;
        }
        if entry.error.is_some() {
            return Err("degraded capacity entry is missing errorClass".into());
        }
        let usage = entry
            .usage
            .as_ref()
            .ok_or_else(|| "capacity entry is missing usage".to_string())?;

        let (stale_since, stale_class) = match &entry.stale {
            Some(stale) => {
                validate_preserved_reading(entry.fetched_at.as_deref(), &stale.since)?;
                (Some(stale.since.clone()), stale.class.clone())
            }
            None => (None, None),
        };

        let account_ref = entry.account.as_deref();
        if account_ref.is_some_and(str::is_empty) {
            return Err("capacity entry has empty account identity".into());
        }
        let entry_window_start = windows.len();

        let mut push = |pool_key: String,
                        slot_kind: &str,
                        external_id: Option<String>,
                        title: Option<String>,
                        extra_ordinal: Option<i64>,
                        window: &RateWindow|
         -> Result<(), String> {
            if let Some(account_ref) = account_ref {
                *account_pool_counts
                    .entry((
                        entry.provider.clone(),
                        account_ref.to_string(),
                        pool_key.clone(),
                    ))
                    .or_default() += 1;
            }
            if !valid_window(window) {
                if let Some(account_ref) = account_ref {
                    invalid_account_pools.insert((
                        entry.provider.clone(),
                        account_ref.to_string(),
                        pool_key,
                    ));
                }
                return Ok(());
            }
            let used_count = count(window.used_count)?;
            let total_count = count(window.total_count)?;
            let breakdown = window
                .breakdown
                .as_ref()
                .map(|b| serde_json::to_string(b).map_err(|e| format!("serialize breakdown: {e}")))
                .transpose()?;
            if let Some(account_ref) = account_ref {
                windows.push(CapacityWindow {
                    provider: entry.provider.clone(),
                    api_provider: entry.api_provider.clone(),
                    account_ref: account_ref.to_string(),
                    pool: pool_key.clone(),
                    pool_key,
                    slot_kind: slot_kind.to_string(),
                    external_id,
                    title,
                    extra_ordinal,
                    legacy_pool_key: None,
                    used_percent: window.used_percent,
                    raw_used_percent: window.raw_used_percent,
                    resets_at: window.resets_at.clone(),
                    window_minutes: window.window_minutes,
                    used_count,
                    total_count,
                    source: entry.source.clone(),
                    fetched_at: entry.fetched_at.clone(),
                    stale_since: stale_since.clone(),
                    stale_class: stale_class.clone(),
                    breakdown,
                });
            } else {
                provider_readings.push(CapacityProviderReading {
                    provider: entry.provider.clone(),
                    api_provider: entry.api_provider.clone(),
                    pool_key,
                    slot_kind: slot_kind.to_string(),
                    external_id,
                    title,
                    extra_ordinal,
                    used_percent: window.used_percent,
                    raw_used_percent: window.raw_used_percent,
                    resets_at: window.resets_at.clone(),
                    window_minutes: window.window_minutes,
                    used_count,
                    total_count,
                    source: entry.source.clone(),
                    fetched_at: entry.fetched_at.clone(),
                });
            }
            Ok(())
        };

        if let Some(window) = &usage.primary {
            push("primary".into(), "primary", None, None, None, window)?;
        }
        if let Some(window) = &usage.secondary {
            push("secondary".into(), "secondary", None, None, None, window)?;
        }
        if let Some(window) = &usage.tertiary {
            push("tertiary".into(), "tertiary", None, None, None, window)?;
        }
        if let Some(extras) = &usage.extra_rate_windows {
            for (ordinal, extra) in extras.iter().enumerate() {
                let Some(window) = &extra.window else {
                    continue;
                };
                let ordinal = i64::try_from(ordinal)
                    .map_err(|_| "extra window ordinal exceeds SQLite range".to_string())?;
                let (pool_key, external_id, extra_ordinal) = match &extra.id {
                    Some(id) if !id.is_empty() => {
                        (format!("extra:{}:{id}", id.len()), Some(id.clone()), None)
                    }
                    _ => (format!("extra:ordinal:{ordinal}"), None, Some(ordinal)),
                };
                push(
                    pool_key,
                    "extra",
                    external_id,
                    extra.title.clone(),
                    extra_ordinal,
                    window,
                )?;
            }
        }

        if let Some(account_ref) = account_ref {
            // PROXY, and a deliberate one: vector growth stands in for "this entry
            // produced at least one window that survived validation". The property
            // itself is not available -- the push sites are spread across the pool
            // arms above and none returns a usable/not-usable verdict.
            //
            // CONTRIBUTORS: only this entry's own pushes, because the start index is
            // captured immediately before the arms run and nothing else appends to
            // `windows` in between. That is what makes the count mean one thing, and
            // it is what a future edit would break -- moving the capture, or adding an
            // append between it and here, silently turns growth into a claim about
            // some other entry.
            //
            // DIRECTION: if it drifts, an entry whose windows were all rejected is
            // recorded as `usable`, which asserts a healthy account from a reading
            // that established nothing. It fails toward over-claiming, so a
            // suspicious `usable` is the symptom to chase rather than a missing one.
            if windows.len() > entry_window_start {
                observations.push(CapacityObservation {
                    provider: entry.provider.clone(),
                    api_provider: entry.api_provider.clone(),
                    account_ref: account_ref.to_string(),
                    observation_state: "usable".into(),
                    error_class: None,
                    error_detail: None,
                    source: entry.source.clone(),
                    fetched_at: entry.fetched_at.clone(),
                });
            }
        }
    }

    let mut duplicate_accounts = BTreeMap::<(String, String), BTreeMap<String, usize>>::new();
    for ((provider, account_ref, pool_key), count) in account_pool_counts {
        if count > 1 {
            duplicate_accounts
                .entry((provider, account_ref))
                .or_default()
                .insert(pool_key, count);
        }
    }
    for ((provider, account_ref), duplicate_pools) in &duplicate_accounts {
        let detail = duplicate_pools
            .iter()
            .map(|(pool_key, count)| format!("{pool_key} appeared {count} times"))
            .collect::<Vec<_>>()
            .join(", ");
        for (pool_key, count) in duplicate_pools {
            tracing::warn!(
                provider = %provider,
                account_ref = %account_ref,
                pool_key = %pool_key,
                count = *count,
                "discarding capacity reading with duplicate pool key; previously stored windows are retained"
            );
        }
        windows.retain(|window| window.provider != *provider || window.account_ref != *account_ref);
        observations.retain(|observation| {
            observation.provider != *provider || observation.account_ref != *account_ref
        });
        degraded_entries = degraded_entries
            .checked_add(1)
            .ok_or_else(|| "degraded entry count overflow".to_string())?;
        observations.push(CapacityObservation {
            provider: provider.clone(),
            api_provider: None,
            account_ref: account_ref.clone(),
            observation_state: "degraded".into(),
            error_class: Some("invalid_capacity_snapshot".into()),
            error_detail: Some(detail),
            source: None,
            fetched_at: None,
        });
    }

    let mut invalid_accounts = BTreeSet::new();
    for (provider, account_ref, pool_key) in invalid_account_pools {
        let has_usable_reading = observations.iter().any(|observation| {
            observation.provider == provider
                && observation.account_ref == account_ref
                && observation.observation_state == "usable"
        });
        tracing::warn!(
            provider = %provider,
            account_ref = %account_ref,
            pool_key = %pool_key,
            previous_windows = if has_usable_reading {
                "replaced by the remaining usable windows"
            } else {
                "preserved because no usable window remains"
            },
            "dropping capacity window with invalid percentage"
        );
        if !has_usable_reading {
            invalid_accounts.insert((provider, account_ref));
        }
    }
    for (provider, account_ref) in invalid_accounts {
        if observations.iter().any(|observation| {
            observation.provider == provider && observation.account_ref == account_ref
        }) {
            continue;
        }
        degraded_entries = degraded_entries
            .checked_add(1)
            .ok_or_else(|| "degraded entry count overflow".to_string())?;
        observations.push(CapacityObservation {
            provider,
            api_provider: None,
            account_ref,
            observation_state: "degraded".into(),
            error_class: Some("invalid_capacity_window".into()),
            error_detail: Some("all reported capacity windows failed validation".into()),
            source: None,
            fetched_at: None,
        });
    }

    Ok(CapacitySnapshot {
        windows,
        observations,
        provider_readings,
        complete_providers,
        degraded_entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexkit_provider_usage::{ExtraWindow, Usage};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    fn window(used: f64) -> RateWindow {
        RateWindow {
            used_percent: used,
            raw_used_percent: None,
            resets_at: None,
            window_minutes: Some(300),
            used_count: None,
            total_count: None,
            // Absent on purpose. We neither read nor persist regeneration, so a
            // fixture that set it would imply this mapper inspects it.
            regeneration: None,
            // Absent on purpose for the same reason: the mapper does not read the
            // upstream's period name, and windows are keyed by provider, account
            // and pool.
            window_kind: None,
            // Absent on the basic test helper; breakdown-specific tests provide it.
            breakdown: None,
        }
    }

    fn capacity_conn() -> rusqlite::Connection {
        use astrocyte_core::store::{initialize_connection, MIGRATIONS};

        let conn = rusqlite::Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn.execute(
            "UPDATE capacity_runtime SET runtime_variant='configured_unavailable',
             changed_at_ms=NULL,enabled_at_ms=0,detail='no usable poll completed' WHERE id=1",
            [],
        )
        .unwrap();
        conn
    }

    #[derive(Debug)]
    struct CapturedEvent {
        level: String,
        fields: BTreeMap<String, String>,
    }

    struct TestSubscriber(Arc<Mutex<Vec<CapturedEvent>>>);

    impl tracing::Subscriber for TestSubscriber {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut fields = BTreeMap::new();
            event.record(&mut EventFields(&mut fields));
            self.0.lock().unwrap().push(CapturedEvent {
                level: event.metadata().level().to_string(),
                fields,
            });
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    struct EventFields<'a>(&'a mut BTreeMap<String, String>);

    impl tracing::field::Visit for EventFields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    fn captured_map(
        entries: &[ProviderUsage],
        complete_providers: Option<std::collections::BTreeSet<String>>,
    ) -> (Result<CapacitySnapshot, String>, Vec<CapturedEvent>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatch = tracing::Dispatch::new(TestSubscriber(events.clone()));
        let snapshot = tracing::dispatcher::with_default(&dispatch, || {
            map_usage_array(entries, complete_providers)
        });
        let events = events.lock().unwrap();
        (
            snapshot,
            events
                .iter()
                .map(|event| CapturedEvent {
                    level: event.level.clone(),
                    fields: event.fields.clone(),
                })
                .collect(),
        )
    }

    fn apply_entries(
        conn: &mut rusqlite::Connection,
        entries: &[ProviderUsage],
        complete_providers: Option<std::collections::BTreeSet<String>>,
        now_ms: i64,
    ) {
        let snapshot = map_usage_array(entries, complete_providers).unwrap();
        let tx = conn.transaction().unwrap();
        astrocyte_core::capacity::apply_snapshot_in_tx(&tx, &snapshot, now_ms).unwrap();
        tx.commit().unwrap();
    }

    fn stored_percent(
        conn: &rusqlite::Connection,
        account_ref: &str,
        pool_key: &str,
    ) -> Option<f64> {
        use rusqlite::OptionalExtension;

        conn.query_row(
            "SELECT used_percent FROM capacity_window
             WHERE provider='codex' AND account_ref=?1 AND pool_key=?2",
            [account_ref, pool_key],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
    }

    fn persisted_stale_columns(raw: &str) -> (Option<String>, Option<String>) {
        use astrocyte_core::capacity::apply_snapshot_in_tx;

        let entries: Vec<ProviderUsage> = serde_json::from_str(raw).unwrap();
        let snapshot = map_usage_array(&entries, None).unwrap();
        let mut conn = capacity_conn();
        let tx = conn.transaction().unwrap();
        apply_snapshot_in_tx(&tx, &snapshot, 1_000).unwrap();
        tx.commit().unwrap();
        conn.query_row(
            "SELECT stale_since,stale_class FROM capacity_window
             WHERE provider='codex' AND account_ref='account-a' AND pool_key='primary'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    #[test]
    fn serialized_preserved_reading_persists_since_and_class() {
        let raw = r#"[{"provider":"codex","account":"account-a","source":"oauth",
            "fetchedAt":"2026-08-13T12:00:00Z",
            "usage":{"primary":{"usedPercent":10.0}},
            "stale":{"since":"2026-08-13T12:01:00Z","class":"upstream_unavailable"}}]"#;

        let stored = persisted_stale_columns(raw);
        assert_eq!(stored.0.as_deref(), Some("2026-08-13T12:01:00Z"));
        assert_eq!(stored.1.as_deref(), Some("upstream_unavailable"));
    }

    #[test]
    fn serialized_preserved_reading_without_class_persists_null_class() {
        let raw = r#"[{"provider":"codex","account":"account-a","source":"oauth",
            "fetchedAt":"2026-08-13T12:00:00Z",
            "usage":{"primary":{"usedPercent":10.0}},
            "stale":{"since":"2026-08-13T12:01:00Z"}}]"#;

        let stored = persisted_stale_columns(raw);
        assert_eq!(stored.0.as_deref(), Some("2026-08-13T12:01:00Z"));
        assert_eq!(stored.1, None);
    }

    #[test]
    fn serialized_fresh_reading_persists_null_stale_columns() {
        let raw = r#"[{"provider":"codex","account":"account-a","source":"oauth",
            "fetchedAt":"2026-08-13T12:00:00Z",
            "usage":{"primary":{"usedPercent":10.0}}}]"#;

        assert_eq!(persisted_stale_columns(raw), (None, None));
    }

    #[test]
    fn serialized_impossible_preserved_chronology_is_refused_before_persistence() {
        use astrocyte_core::capacity::apply_snapshot_in_tx;

        let raw = r#"[{"provider":"codex","account":"account-a","source":"oauth",
            "fetchedAt":"2026-08-13T12:00:00Z",
            "usage":{"primary":{"usedPercent":10.0}},
            "stale":{"since":"2026-08-13T11:59:59Z","class":"upstream_unavailable"}}]"#;
        let entries: Vec<ProviderUsage> = serde_json::from_str(raw).unwrap();
        let mapped = map_usage_array(&entries, None);
        let mut conn = capacity_conn();
        if let Ok(snapshot) = &mapped {
            let tx = conn.transaction().unwrap();
            apply_snapshot_in_tx(&tx, snapshot, 1_000).unwrap();
            tx.commit().unwrap();
        }

        assert_eq!(
            mapped.unwrap_err(),
            "capacity stale.since must be later than fetchedAt"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM capacity_window", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0, "an impossible preserved reading must not persist");
    }

    #[test]
    fn maps_named_and_extra_pool_identity_and_counts() {
        let mut extra_window = window(55.0);
        extra_window.used_count = Some(10_336.0);
        extra_window.total_count = Some(40_000.0);
        let entries = vec![ProviderUsage::healthy(
            "codex",
            Some("apikey:openai:k1".into()),
            "oauth",
            Usage {
                primary: Some(window(30.0)),
                secondary: Some(window(10.0)),
                tertiary: None,
                extra_rate_windows: Some(vec![ExtraWindow {
                    title: Some("Gemini Pro".into()),
                    id: Some("gemini-pro".into()),
                    window: Some(extra_window),
                }]),
            },
        )];
        let snapshot = map_usage_array(&entries, None).unwrap();
        assert_eq!(snapshot.windows.len(), 3);
        let extra = snapshot
            .windows
            .iter()
            .find(|window| window.slot_kind == "extra")
            .unwrap();
        assert_eq!(extra.pool_key, "extra:10:gemini-pro");
        assert_eq!(extra.external_id.as_deref(), Some("gemini-pro"));
        assert_eq!(extra.title.as_deref(), Some("Gemini Pro"));
        assert_eq!(extra.used_count, Some(10_336));
        assert_eq!(extra.total_count, Some(40_000));
    }

    #[test]
    fn duplicate_extra_pool_drops_only_that_account_and_preserves_its_windows() {
        let mut conn = capacity_conn();
        apply_entries(
            &mut conn,
            &[
                ProviderUsage::healthy(
                    "codex",
                    Some("account-bad".into()),
                    "oauth",
                    Usage {
                        primary: Some(window(10.0)),
                        secondary: None,
                        tertiary: None,
                        extra_rate_windows: None,
                    },
                ),
                ProviderUsage::healthy(
                    "codex",
                    Some("account-good".into()),
                    "oauth",
                    Usage {
                        primary: Some(window(15.0)),
                        secondary: None,
                        tertiary: None,
                        extra_rate_windows: None,
                    },
                ),
            ],
            None,
            1_000,
        );

        let duplicate = || ExtraWindow {
            title: Some("Duplicate".into()),
            id: Some("N".into()),
            window: Some(window(50.0)),
        };
        let entries = [
            ProviderUsage::healthy(
                "codex",
                Some("account-bad".into()),
                "oauth",
                Usage {
                    primary: None,
                    secondary: None,
                    tertiary: None,
                    extra_rate_windows: Some(vec![duplicate(), duplicate()]),
                },
            ),
            ProviderUsage::healthy(
                "codex",
                Some("account-good".into()),
                "oauth",
                Usage {
                    primary: Some(window(30.0)),
                    secondary: None,
                    tertiary: None,
                    extra_rate_windows: None,
                },
            ),
        ];
        let complete = Some(std::collections::BTreeSet::from(["codex".to_string()]));
        let (snapshot, events) = captured_map(&entries, complete.clone());
        let snapshot = snapshot.unwrap();
        let tx = conn.transaction().unwrap();
        astrocyte_core::capacity::apply_snapshot_in_tx(&tx, &snapshot, 2_000).unwrap();
        tx.commit().unwrap();

        assert_eq!(stored_percent(&conn, "account-bad", "primary"), Some(10.0));
        assert_eq!(stored_percent(&conn, "account-good", "primary"), Some(30.0));
        assert_eq!(snapshot.windows.len(), 1);
        assert_eq!(snapshot.observations.len(), 2);
        assert_eq!(
            snapshot
                .observations
                .iter()
                .find(|observation| observation.account_ref == "account-bad")
                .unwrap()
                .observation_state,
            "degraded"
        );
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].level, "WARN");
        assert!(events[0]
            .fields
            .get("account_ref")
            .is_some_and(|value| value.contains("account-bad")));
        assert!(events[0]
            .fields
            .get("pool_key")
            .is_some_and(|value| value.contains("extra:1:N")));
        assert!(events[0]
            .fields
            .get("count")
            .is_some_and(|count| count == "2" || count.contains("2")));
    }

    #[test]
    fn out_of_range_account_windows_warn_and_report_prior_window_retention() {
        let mut conn = capacity_conn();
        apply_entries(
            &mut conn,
            &[
                ProviderUsage::healthy(
                    "codex",
                    Some("account-replaced".into()),
                    "oauth",
                    Usage {
                        primary: Some(window(10.0)),
                        secondary: None,
                        tertiary: None,
                        extra_rate_windows: None,
                    },
                ),
                ProviderUsage::healthy(
                    "codex",
                    Some("account-retained".into()),
                    "oauth",
                    Usage {
                        primary: Some(window(15.0)),
                        secondary: None,
                        tertiary: None,
                        extra_rate_windows: None,
                    },
                ),
            ],
            None,
            1_000,
        );

        let entries = [
            ProviderUsage::healthy(
                "codex",
                Some("account-replaced".into()),
                "oauth",
                Usage {
                    primary: Some(window(104.0)),
                    secondary: Some(window(20.0)),
                    tertiary: None,
                    extra_rate_windows: None,
                },
            ),
            ProviderUsage::healthy(
                "codex",
                Some("account-retained".into()),
                "oauth",
                Usage {
                    primary: Some(window(104.0)),
                    secondary: None,
                    tertiary: None,
                    extra_rate_windows: None,
                },
            ),
        ];
        let complete = Some(std::collections::BTreeSet::from(["codex".to_string()]));
        let (snapshot, events) = captured_map(&entries, complete.clone());
        let snapshot = snapshot.unwrap();
        let tx = conn.transaction().unwrap();
        astrocyte_core::capacity::apply_snapshot_in_tx(&tx, &snapshot, 2_000).unwrap();
        tx.commit().unwrap();

        assert_eq!(stored_percent(&conn, "account-replaced", "primary"), None);
        assert_eq!(
            stored_percent(&conn, "account-replaced", "secondary"),
            Some(20.0)
        );
        assert_eq!(
            stored_percent(&conn, "account-retained", "primary"),
            Some(15.0)
        );
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(events.iter().all(|event| event.level == "WARN"));
        assert!(events.iter().all(|event| {
            event
                .fields
                .get("pool_key")
                .is_some_and(|pool_key| pool_key.contains("primary"))
        }));
        assert!(events.iter().any(|event| {
            event
                .fields
                .get("account_ref")
                .is_some_and(|account| account.contains("account-replaced"))
                && event
                    .fields
                    .get("previous_windows")
                    .is_some_and(|status| status.contains("replaced"))
        }));
        assert!(events.iter().any(|event| {
            event
                .fields
                .get("account_ref")
                .is_some_and(|account| account.contains("account-retained"))
                && event
                    .fields
                    .get("previous_windows")
                    .is_some_and(|status| status.contains("preserved"))
        }));
    }

    #[test]
    fn rejects_capacity_count_at_the_i64_upper_exclusive_bound() {
        let mut count_window = window(25.0);
        count_window.used_count = Some(2_f64.powi(63));
        let entry = ProviderUsage::healthy(
            "codex",
            Some("account-a".into()),
            "oauth",
            Usage {
                primary: Some(count_window),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            },
        );

        assert_eq!(
            map_usage_array(&[entry], None).unwrap_err(),
            "capacity count exceeds SQLite range"
        );
    }

    #[test]
    fn degraded_labeled_entry_preserves_open_error_class() {
        let mut degraded = ProviderUsage::degraded_with_class(
            "anthropic",
            "future failure",
            "future_failure_class",
        );
        degraded.account = Some("account-a".into());
        let snapshot = map_usage_array(&[degraded], None).unwrap();
        assert_eq!(snapshot.degraded_entries, 1);
        assert!(snapshot.windows.is_empty());
        assert_eq!(snapshot.observations.len(), 1);
        assert_eq!(
            snapshot.observations[0].error_class.as_deref(),
            Some("future_failure_class")
        );
    }

    /// A healthy entry with an explicit canonical api_provider (the 0.2.0 wire
    /// field), for the canonicalization tests.
    fn healthy_canon(
        provider: &str,
        api_provider: &str,
        account: Option<String>,
        usage: Usage,
    ) -> ProviderUsage {
        ProviderUsage {
            api_provider: Some(api_provider.to_string()),
            ..ProviderUsage::healthy(provider, account, "oauth", usage)
        }
    }

    #[test]
    fn unresolved_accounts_map_to_provider_current_readings() {
        let entries = vec![
            healthy_canon(
                "gemini",
                "google",
                None,
                Usage {
                    primary: Some(window(40.0)),
                    ..Default::default()
                },
            ),
            healthy_canon(
                "grok",
                "xai",
                None,
                Usage {
                    primary: Some(window(50.0)),
                    ..Default::default()
                },
            ),
        ];
        let snapshot = map_usage_array(&entries, None).unwrap();
        assert!(snapshot.windows.is_empty());
        assert!(snapshot.observations.is_empty());
        assert_eq!(snapshot.provider_readings.len(), 2);
        assert_eq!(snapshot.provider_readings[0].provider, "gemini");
        assert_eq!(
            snapshot.provider_readings[0].api_provider.as_deref(),
            Some("google")
        );
        assert_eq!(snapshot.provider_readings[1].provider, "grok");
    }

    #[test]
    fn provider_current_reading_preserves_missing_api_provider() {
        let entries = vec![ProviderUsage::healthy(
            "kimi",
            None,
            "oauth",
            Usage {
                primary: Some(window(20.0)),
                ..Default::default()
            },
        )];
        let snapshot = map_usage_array(&entries, None).unwrap();
        assert!(snapshot.windows.is_empty());
        assert_eq!(snapshot.provider_readings.len(), 1);
        assert_eq!(snapshot.provider_readings[0].provider, "kimi");
        assert_eq!(snapshot.provider_readings[0].api_provider, None);
    }

    #[test]
    fn labeled_account_carries_canonical_provider_for_the_join() {
        let entries = vec![healthy_canon(
            "claude",
            "anthropic",
            Some("00000000-0000-4000-8000-00000000000c".into()),
            Usage {
                primary: Some(window(33.0)),
                ..Default::default()
            },
        )];
        let snapshot = map_usage_array(&entries, None).unwrap();
        assert_eq!(
            snapshot.windows[0].account_ref,
            "00000000-0000-4000-8000-00000000000c"
        );
        assert_eq!(
            snapshot.windows[0].api_provider.as_deref(),
            Some("anthropic")
        );
        assert_eq!(snapshot.windows[0].provider, "claude");
        assert_eq!(snapshot.observations[0].observation_state, "usable");
    }

    #[test]
    fn healthy_but_windowless_entry_contributes_nothing() {
        let entries = vec![ProviderUsage::healthy(
            "codex",
            Some("account-a".into()),
            "oauth",
            Usage::default(),
        )];
        let snapshot = map_usage_array(&entries, None).unwrap();
        assert!(snapshot.windows.is_empty());
        assert!(snapshot.observations.is_empty());
        assert!(snapshot.provider_readings.is_empty());
        assert_eq!(snapshot.degraded_entries, 0);
    }

    #[test]
    fn secondary_window_with_breakdown_persists_json_and_roundtrips() {
        use astrocyte_core::capacity::apply_snapshot_in_tx;
        use cortexkit_provider_usage::UsageBreakdown;

        let raw = r#"[{"provider":"claude","account":"account-claude","source":"oauth",
            "fetchedAt":"2026-09-28T12:00:00Z",
            "usage":{
                "secondary":{
                    "usedPercent":45.0,
                    "windowMinutes":10080,
                    "breakdown":{
                        "asOf":"2026-09-28T11:55:00Z",
                        "windowStartedAt":"2026-09-21T12:00:00Z",
                        "rows":[
                            {"key":"claude_code","sharePercent":100.0},
                            {"key":"chat","sharePercent":0.0},
                            {"key":"cowork","sharePercent":0.0},
                            {"key":"other","sharePercent":0.0}
                        ]
                    }
                }
            }}]"#;

        let entries: Vec<ProviderUsage> = serde_json::from_str(raw).unwrap();
        let snapshot = map_usage_array(&entries, None).unwrap();
        let mut conn = capacity_conn();
        let tx = conn.transaction().unwrap();
        apply_snapshot_in_tx(&tx, &snapshot, 1_000).unwrap();
        tx.commit().unwrap();

        let stored_json: Option<String> = conn
            .query_row(
                "SELECT breakdown FROM window_sample WHERE provider='claude' AND account_ref='account-claude' AND pool_key='secondary'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let json_str = stored_json.expect("secondary window breakdown must be stored");
        let decoded: UsageBreakdown =
            serde_json::from_str(&json_str).expect("breakdown must decode as UsageBreakdown");
        assert_eq!(decoded.as_of.as_deref(), Some("2026-09-28T11:55:00Z"));
        assert_eq!(
            decoded.window_started_at.as_deref(),
            Some("2026-09-21T12:00:00Z")
        );
        assert_eq!(decoded.rows.len(), 4);
        assert_eq!(decoded.rows[0].key, "claude_code");
        assert_eq!(decoded.rows[0].share_percent, Some(100.0));
        assert_eq!(decoded.rows[1].key, "chat");
        assert_eq!(decoded.rows[1].share_percent, Some(0.0));
        assert_eq!(decoded.rows[2].key, "cowork");
        assert_eq!(decoded.rows[2].share_percent, Some(0.0));
        assert_eq!(decoded.rows[3].key, "other");
        assert_eq!(decoded.rows[3].share_percent, Some(0.0));
    }

    #[test]
    fn window_without_breakdown_stores_null() {
        use astrocyte_core::capacity::apply_snapshot_in_tx;

        let raw = r#"[{"provider":"claude","account":"account-claude","source":"oauth",
            "fetchedAt":"2026-09-28T12:00:00Z",
            "usage":{
                "secondary":{
                    "usedPercent":45.0,
                    "windowMinutes":10080
                }
            }}]"#;

        let entries: Vec<ProviderUsage> = serde_json::from_str(raw).unwrap();
        let snapshot = map_usage_array(&entries, None).unwrap();
        let mut conn = capacity_conn();
        let tx = conn.transaction().unwrap();
        apply_snapshot_in_tx(&tx, &snapshot, 1_000).unwrap();
        tx.commit().unwrap();

        let stored_json: Option<String> = conn
            .query_row(
                "SELECT breakdown FROM window_sample WHERE provider='claude' AND account_ref='account-claude' AND pool_key='secondary'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            stored_json, None,
            "window without breakdown must store NULL"
        );
    }

    #[test]
    fn primary_window_next_to_secondary_with_breakdown_only_secondary_carries_it() {
        use astrocyte_core::capacity::apply_snapshot_in_tx;

        let raw = r#"[{"provider":"claude","account":"account-claude","source":"oauth",
            "fetchedAt":"2026-09-28T12:00:00Z",
            "usage":{
                "primary":{
                    "usedPercent":20.0,
                    "windowMinutes":300
                },
                "secondary":{
                    "usedPercent":45.0,
                    "windowMinutes":10080,
                    "breakdown":{
                        "asOf":"2026-09-28T11:55:00Z",
                        "windowStartedAt":"2026-09-21T12:00:00Z",
                        "rows":[{"key":"claude_code","sharePercent":100.0}]
                    }
                }
            }}]"#;

        let entries: Vec<ProviderUsage> = serde_json::from_str(raw).unwrap();
        let snapshot = map_usage_array(&entries, None).unwrap();
        let mut conn = capacity_conn();
        let tx = conn.transaction().unwrap();
        apply_snapshot_in_tx(&tx, &snapshot, 1_000).unwrap();
        tx.commit().unwrap();

        let primary_breakdown: Option<String> = conn
            .query_row(
                "SELECT breakdown FROM window_sample WHERE provider='claude' AND account_ref='account-claude' AND pool_key='primary'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let secondary_breakdown: Option<String> = conn
            .query_row(
                "SELECT breakdown FROM window_sample WHERE provider='claude' AND account_ref='account-claude' AND pool_key='secondary'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            primary_breakdown, None,
            "primary window must have NULL breakdown"
        );
        assert!(
            secondary_breakdown.is_some(),
            "secondary window must carry breakdown"
        );
    }

    // The account-attribution join belongs to the report surface, not this mapper.
}
