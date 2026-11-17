//! Source page validation and drain health live here, apart from the loop that orders stages.

use super::*;

pub fn drain_once(
    state: &SharedState,
    source: &dyn SourceClient,
    source_key: &str,
    limit: u32,
    now_ms: i64,
) -> Result<DrainOutcome, PullError> {
    drain_once_in_lane(
        state,
        source,
        source_key,
        limit,
        now_ms,
        harness_drain::DrainMode::Full,
    )
}

pub(super) fn drain_once_in_lane(
    state: &SharedState,
    source: &dyn SourceClient,
    source_key: &str,
    limit: u32,
    now_ms: i64,
    mode: harness_drain::DrainMode,
) -> Result<DrainOutcome, PullError> {
    let current = durable_cursor(state, source_key).map_err(PullError::Store)?;
    // The read is the only I/O boundary, and is intentionally before the
    // transaction below.
    let discovery_path = source
        .resolved_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<unavailable: source did not provide a resolved path>".into());
    let read = if mode.is_full() {
        source.pull(current.clone(), limit)
    } else {
        source.pull_fast(current.clone(), limit)
    };
    let page = match read {
        Err(PullError::SourceAbsent) => {
            harness_drain::record_source_discovery(state, source_key, &discovery_path, false);
            mode.remove_runtime(state, source_key)
                .map_err(PullError::Store)?;
            return Ok(DrainOutcome::default());
        }
        result => {
            let page = result?;
            harness_drain::record_source_discovery(state, source_key, &discovery_path, true);
            page
        }
    };
    validate_page(&page)?;
    source.refuse_regression(current.as_ref(), &page)?;
    if !mode.has_work(page.events.len(), page.set_aside.len()) {
        return Ok(DrainOutcome::default());
    }
    let cursor = &page.cursor;
    let high_water = &page.high_water;
    // The schema stores one lineage for both ordinals. If the producer already
    // reports a later lineage but this page consumed only the old one, retain a
    // same-lineage high water and keep lag unknown until a new-lineage event lands.
    let persisted_high_water_ordinal = if high_water.lineage == cursor.lineage {
        high_water.ordinal
    } else {
        cursor.ordinal
    };
    let source_key = source_key.to_owned();
    let (outcome, newly_seen) = mode
        .write(state, |tx| {
            // The row is removed while the producer's store is absent or the
            // producer is excluded, so the first drain after either recreates
            // it in the same transaction as the cursor it records.
            harness_drain::ensure_runtime_row_in_tx(tx, &source_key)
                .map_err(|error| format!("initialize source runtime: {error}"))?;
            let newly_seen =
                harness_drain::record_set_aside_in_tx(tx, &source_key, &page.set_aside, now_ms)?;
            let mut events_recorded = 0;
            let mut quarantined_redeliveries = 0;
            for event in &page.events {
                // Read, not Pushed: this process fetched the record from the
                // producer's store, so reading it again says nothing about the
                // producer and must not write redelivery_log.
                let outcome = astrocyte_core::ingest::record_event_in_tx(
                    tx,
                    event,
                    now_ms,
                    astrocyte_core::ingest::DeliveryPath::Read,
                )
                .map_err(|error| format!("record producer event: {error:?}"))?;
                events_recorded += 1;
                if outcome.disposition
                    == astrocyte_core::ingest::RecordDisposition::QuarantinedRedelivery
                {
                    quarantined_redeliveries += 1;
                }
            }
            let updated = tx
                .execute(
                    "UPDATE source_runtime
                 SET lineage=?2, consumed_ordinal=?3, high_water_ordinal=?4,
                      last_success_ms=CASE WHEN ?8 THEN ?5 ELSE last_success_ms END,
                      pending_known=?6, pending_unknown=?7
                 WHERE source_key=?1",
                    params![
                        source_key,
                        cursor.lineage,
                        cursor.ordinal,
                        persisted_high_water_ordinal,
                        now_ms,
                        page.pending_known,
                        i64::from(page.pending_unknown),
                        mode.is_full(),
                    ],
                )
                .map_err(|error| format!("record source runtime: {error}"))?;
            if updated != 1 {
                return Err(format!("{source_key} source runtime row missing at commit"));
            }
            Ok((
                (
                    DrainOutcome {
                        events_recorded,
                        quarantined_redeliveries,
                        consumed_ordinal: Some(cursor.ordinal),
                        high_water_ordinal: Some(persisted_high_water_ordinal),
                    },
                    newly_seen,
                ),
                true,
            ))
        })
        .map_err(PullError::Store)?;
    harness_drain::warn_set_aside(&source_key, &newly_seen);
    Ok(outcome)
}

/// The stored cursor, or `None` when there is none yet or no row at all.
fn durable_cursor(state: &SharedState, source_key: &str) -> Result<Option<SourceCursor>, String> {
    use rusqlite::OptionalExtension;
    state.with_conn(|conn| {
        conn.query_row(
            "SELECT lineage,consumed_ordinal FROM source_runtime WHERE source_key=?1",
            [source_key],
            |row| {
                let lineage = row.get::<_, Option<String>>(0)?;
                let ordinal = row.get::<_, Option<i64>>(1)?;
                Ok(match (lineage, ordinal) {
                    (Some(lineage), Some(ordinal)) => Some(SourceCursor { lineage, ordinal }),
                    _ => None,
                })
            },
        )
        .optional()
        .map(Option::flatten)
        .map_err(|error| error.to_string())
    })
}

/// The generic guard preserves the same-lineage ordinal regression rule. Each
/// adapter additionally validates lineage ordering using its own wire semantics.
pub(super) fn refuse_regression(
    stored: Option<&SourceCursor>,
    page: &ExportPage,
) -> Result<(), PullError> {
    let Some(stored) = stored else {
        return Ok(());
    };
    let high_water = &page.high_water;
    if high_water.lineage == stored.lineage && high_water.ordinal < stored.ordinal {
        return Err(PullError::LineageRegressed {
            stored: stored.clone(),
            offered: high_water.clone(),
        });
    }
    Ok(())
}

fn validate_page(page: &ExportPage) -> Result<(), PullError> {
    if page.pending_known < 0
        || page.cursor.lineage.is_empty()
        || page.cursor.ordinal < 0
        || page.high_water.lineage.is_empty()
        || page.high_water.ordinal < 0
    {
        return Err(PullError::InvalidResponse(
            "source identity and cursors must be nonnegative and ordered".into(),
        ));
    }
    if !page.pending_unknown && page.pending_known == 0 && page.cursor != page.high_water {
        return Err(PullError::InvalidResponse(
            "a complete observation with lag must report known pending records".into(),
        ));
    }
    // Within one lineage the producer cannot have consumed past its own
    // newest position. Across lineages the two ordinals count different
    // sequences and are not compared; `drain_once` then keeps the consumed
    // ordinal as the high water. The store's CHECK refuses the same-lineage
    // case too, but only as a failed write; this names the producer's page as
    // the cause before anything is recorded from it.
    if page.cursor.lineage == page.high_water.lineage
        && page.cursor.ordinal > page.high_water.ordinal
    {
        return Err(PullError::InvalidResponse(format!(
            "consumed ordinal {} is past high water {} in lineage {}",
            page.cursor.ordinal, page.high_water.ordinal, page.cursor.lineage
        )));
    }
    Ok(())
}

/// Whether `harness_ingest` lets this producer drain on this tick. A skipped
/// producer is not read at all -- no file, no store, no peer call -- and is
/// treated the way an absent harness store is: its runtime row is removed
/// and its health cleared, so it never reads as stale in health or
/// `ingest_freshness`. Events it already recorded stay. Re-enabling it starts
/// from no row: a file reader reads its files again, and BROCA, whose cursor
/// was in the removed row, is read from its first fact; content-derived event
/// ids make both re-reads record nothing twice.
pub(super) fn ingesting(
    state: &SharedState,
    ingest: &config::HarnessIngest,
    source_key: &str,
) -> bool {
    let config_key = if source_key == opencode_v2_source::SOURCE_KEY {
        opencode_source::OPENCODE_SOURCE_KEY
    } else {
        source_key
    };
    if ingest.ingests(config_key) {
        return true;
    }
    if let Err(error) = harness_drain::remove_runtime_row(state, source_key) {
        tracing::warn!(target: "drain", producer = source_key, %error, "remove excluded producer runtime row failed");
    }
    state.clear_producer_degraded(source_key);
    false
}

pub(super) fn drain_harness(
    state: &SharedState,
    harness: &dyn harness_drain::HarnessSource,
    stop: &AtomicBool,
) {
    finish_harness_drain(
        state,
        harness.source_key(),
        harness.drain_for_health(state, current_time_ms(), stop),
    );
}

pub(super) fn drain_media_if_due(
    state: &SharedState,
    harness: &dyn harness_drain::HarnessSource,
    stop: &AtomicBool,
    full_tick: bool,
) {
    let Some(ticket) = state.event_wakes().begin(
        crate::event_wakes::Source::Media,
        full_tick,
        current_time_ms(),
    ) else {
        return;
    };
    let result = if ticket.has_notice() {
        harness.drain_after_notice(state, current_time_ms(), stop)
    } else {
        harness.drain_for_health(state, current_time_ms(), stop)
    };
    ticket.finish(result.is_ok());
    finish_harness_drain(state, harness.source_key(), result);
}

fn finish_harness_drain(
    state: &SharedState,
    key: &str,
    result: Result<Option<String>, SourceDrainError>,
) {
    match result {
        Ok(None) => state.clear_producer_degraded(key),
        // A stopped drain committed nothing and says nothing about health.
        Err(SourceDrainError::Stopped | SourceDrainError::Deferred) => {}
        Ok(Some(reason)) | Err(SourceDrainError::Failed(reason)) => {
            harness_drain::mark_drain_failed(state, key, reason)
        }
    }
}

#[cfg(test)]
fn mark_claude_code_drain_failed(state: &SharedState, reason: String) {
    harness_drain::mark_drain_failed(state, claude_code_source::CLAUDE_CODE_SOURCE_KEY, reason);
}

#[cfg(test)]
fn mark_codex_drain_failed(state: &SharedState, reason: String) {
    harness_drain::mark_drain_failed(state, codex_source::CODEX_SOURCE_KEY, reason);
}

#[cfg(test)]
fn mark_pi_drain_failed(state: &SharedState, reason: String) {
    harness_drain::mark_drain_failed(state, pi_source::PI_SOURCE_KEY, reason);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresher::tests::*;
    use crate::state::test_state;

    fn sequence_less_runtime(state: &SharedState, key: &str) -> (Option<i64>, i64) {
        state
            .with_conn(|conn| {
                conn.query_row(
                "SELECT last_success_ms,pending_unknown FROM source_runtime WHERE source_key=?1",
                [key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    #[test]
    fn harness_success_failure_success_persists_unknown_pending() {
        let state = test_state();
        let key = "gemini";
        harness_drain::ensure_runtime_row(&state, key).unwrap();
        harness_drain::record_requests(&state, key, &[], 101, &|| false).unwrap();
        assert_eq!(sequence_less_runtime(&state, key), (Some(101), 0));
        harness_drain::mark_drain_failed(&state, key, "read failed".into());
        assert_eq!(sequence_less_runtime(&state, key), (Some(101), 1));
        harness_drain::record_requests(&state, key, &[], 102, &|| false).unwrap();
        assert_eq!(sequence_less_runtime(&state, key), (Some(102), 0));
    }

    #[test]
    fn dedicated_success_failure_success_persists_unknown_pending() {
        let state = test_state();
        let temp = tempfile::TempDir::new().unwrap();
        let claude_root = temp.path().join("claude");
        let codex_root = temp.path().join("codex");
        let pi_root = temp.path().join("pi");
        for root in [&claude_root, &pi_root] {
            std::fs::create_dir(root).unwrap();
        }
        std::fs::create_dir_all(codex_root.join("sessions")).unwrap();
        let claude = claude_code_source::ClaudeCodeSource::at(&claude_root);
        let codex = codex_source::CodexSource::at(&codex_root);
        let pi = pi_source::PiSource::at(&pi_root);
        type DrainCase<'a> = (&'static str, &'a dyn Fn(i64), fn(&SharedState, String));
        let sources: [DrainCase<'_>; 3] = [
            (
                claude_code_source::CLAUDE_CODE_SOURCE_KEY,
                &|now| {
                    claude.drain(&state, now, &NEVER_STOP).unwrap();
                },
                mark_claude_code_drain_failed,
            ),
            (
                codex_source::CODEX_SOURCE_KEY,
                &|now| {
                    codex.drain(&state, now, &NEVER_STOP).unwrap();
                },
                mark_codex_drain_failed,
            ),
            (
                pi_source::PI_SOURCE_KEY,
                &|now| {
                    pi.drain(&state, now, &NEVER_STOP).unwrap();
                },
                mark_pi_drain_failed,
            ),
        ];
        for (key, success, fail) in sources {
            success(101);
            assert_eq!(sequence_less_runtime(&state, key), (Some(101), 0), "{key}");
            fail(&state, "read failed".into());
            assert_eq!(sequence_less_runtime(&state, key), (Some(101), 1), "{key}");
            success(102);
            assert_eq!(sequence_less_runtime(&state, key), (Some(102), 0), "{key}");
        }
    }

    #[test]
    fn failed_mark_store_error_is_visible_in_degraded_reason() {
        let state = test_state();
        harness_drain::ensure_runtime_row(&state, "gemini").unwrap();
        state.with_conn(|conn| {
            conn.execute_batch("CREATE TRIGGER refuse_mark BEFORE UPDATE ON source_runtime BEGIN SELECT RAISE(ABORT, 'mark refused'); END;")
                .map_err(|error| error.to_string())
        }).unwrap();
        harness_drain::mark_drain_failed(&state, "gemini", "read failed".into());
        let reason = state.producer_degraded_reason("gemini").unwrap();
        assert!(reason.contains("read failed"), "{reason}");
        assert!(reason.contains("mark refused"), "{reason}");
        harness_drain::mark_drain_failed(&state, "goose", "read failed".into());
        let reason = state.producer_degraded_reason("goose").unwrap();
        assert!(reason.contains("row missing"), "{reason}");
    }

    struct OnePage;

    impl SourceClient for OnePage {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            Ok(ExportPage {
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "instance-a:3".into(),
                    ordinal: 7,
                },
                high_water: SourceCursor {
                    lineage: "instance-a:3".into(),
                    ordinal: 8,
                },
                events: Vec::new(),
                pending_known: 1,
                pending_unknown: false,
            })
        }
    }

    #[test]
    fn malformed_hydration_refuses_a_fresh_store() {
        let state = Arc::new(test_state());
        let error =
            reject_startup_bytes_for_test(&state, br#"{"poll_interval_secs":60}"#).unwrap_err();
        assert!(error.contains("no committed generation"));
    }

    /// The production reload path, not the test helper: a store that already
    /// holds a committed generation must survive a malformed file at the next
    /// startup, keep serving that generation, and degrade health with the
    /// decode error. This arm existed and was unreachable -- reload_config
    /// returned Err before it, so a typo in the config file exited the daemon.
    #[test]
    fn malformed_file_retains_the_committed_generation_through_the_production_path() {
        let state = Arc::new(test_state());
        // pid + nanos keeps parallel test binaries and repeated runs from
        // sharing a path; the file is removed at the end.
        let path = std::env::temp_dir().join(format!(
            "astrocyte-reload-{}-{}.jsonc",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            br#"{"poll_interval_secs":45,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":[]}}"#,
        )
        .unwrap();
        reload_config(&state, Ok(path.clone())).unwrap();
        let committed = state.desired_generation();
        assert!(state.has_committed_desired_generation());
        assert_eq!(state.desired_config().poll_interval_secs, 45);

        std::fs::write(&path, br#"{"poll_interval_secs":60}"#).unwrap();
        reload_config(&state, Ok(path.clone()))
            .expect("a committed generation must be retained, not refused");
        assert_eq!(
            state.desired_generation(),
            committed,
            "the malformed file must not publish"
        );
        assert_eq!(
            state.desired_config().poll_interval_secs,
            45,
            "the retained generation must still be the one served"
        );
        let health = state.public_projection().health(0);
        assert_eq!(health.status, "degraded");
        assert!(
            health
                .degraded_reason
                .as_deref()
                .is_some_and(|r| r.contains("member")),
            "health must carry the decode error, got {:?}",
            health.degraded_reason
        );

        // The path-resolution arm routes the same way.
        reload_config(
            &state,
            Err("neither XDG_CONFIG_HOME nor HOME is set".into()),
        )
        .expect("an unresolvable path retains too");
        assert_eq!(state.desired_generation(), committed);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn successful_drain_writes_the_producer_relationship_state() {
        let state = test_state();
        let outcome = drain_once(&state, &OnePage, "broca", 20, 99).unwrap();
        assert_eq!(outcome.consumed_ordinal, Some(7));
        let values: (String, i64, i64, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,high_water_ordinal,last_success_ms
                     FROM source_runtime WHERE source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(values, ("instance-a:3".into(), 7, 8, 99));
    }

    #[test]
    fn failed_source_observation_keeps_the_cold_unavailable_marker() {
        let state = test_state();
        let result = drain_once(&state, &UnavailableSource, "broca", 20, 99);
        assert!(result.is_err());
        let values: (Option<String>, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,pending_unknown FROM source_runtime
                     WHERE source_key='broca'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(values, (None, 1));
    }

    /// A page from the SAME instance sitting behind the stored cursor: the
    /// shape a producer restore or clock rollback produces. The page carries a
    /// real event so the assertion is about non-application, not about an
    /// empty page having nothing to apply.
    struct RegressedPage {
        instance: &'static str,
        epoch: i64,
        seq: i64,
    }

    impl SourceClient for RegressedPage {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            let cursor = SourceCursor {
                lineage: format!("{}:{}", self.instance, self.epoch),
                ordinal: self.seq,
            };
            Ok(ExportPage {
                set_aside: Vec::new(),
                cursor: cursor.clone(),
                high_water: cursor,
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }

        fn refuse_regression(
            &self,
            stored: Option<&SourceCursor>,
            page: &ExportPage,
        ) -> Result<(), PullError> {
            broca_source::refuse_broca_regression(stored, page)
        }
    }

    fn seed_cursor(state: &SharedState, instance: &str, epoch: i64, seq: i64) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "UPDATE source_runtime SET lineage=?1,
                     consumed_ordinal=?2,high_water_ordinal=?2,last_success_ms=50,
                     pending_unknown=0 WHERE source_key='broca'",
                    params![format!("{instance}:{epoch}"), seq],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
    }

    fn stored_state(state: &SharedState) -> (String, i64, i64, i64) {
        state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT lineage,consumed_ordinal,last_success_ms,
                            (SELECT COUNT(*) FROM usage_event)
                     FROM source_runtime WHERE source_key='broca'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .map_err(|e| e.to_string())
            })
            .unwrap()
    }

    #[test]
    fn same_instance_lower_sequence_is_refused_and_nothing_is_written() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        let result = drain_once(
            &state,
            &RegressedPage {
                instance: "inst",
                epoch: 2,
                seq: 499,
            },
            "broca",
            20,
            99,
        );
        assert!(
            matches!(result, Err(PullError::LineageRegressed { .. })),
            "{result:?}"
        );
        assert_eq!(stored_state(&state), ("inst:2".into(), 500, 50, 0));
    }

    /// A page whose consumed position is past the high water it reports. Only
    /// a custom source can offer this: the BROCA client raises high water to
    /// at least what it consumed. The page carries a real event so the
    /// assertion is about non-application, not about an empty page.
    struct ConsumedPastHighWater {
        high_water_lineage: &'static str,
    }

    impl SourceClient for ConsumedPastHighWater {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            Ok(ExportPage {
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "inst:2".into(),
                    ordinal: 507,
                },
                high_water: SourceCursor {
                    lineage: self.high_water_lineage.into(),
                    ordinal: 506,
                },
                events: vec![event],
                pending_known: 0,
                pending_unknown: true,
            })
        }
    }

    #[test]
    fn a_page_consumed_past_its_own_high_water_is_refused_and_nothing_is_written() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        let result = drain_once(
            &state,
            &ConsumedPastHighWater {
                high_water_lineage: "inst:2",
            },
            "broca",
            20,
            99,
        );
        assert!(
            matches!(&result, Err(PullError::InvalidResponse(message))
                if message.contains("consumed ordinal 507 is past high water 506")),
            "{result:?}"
        );
        assert_eq!(stored_state(&state), ("inst:2".into(), 500, 50, 0));
    }

    /// Ordinals of different lineages count different sequences, so a higher
    /// consumed ordinal than another lineage's high water is not an ordering
    /// fault, and an equal one in the same lineage is the caught-up page.
    #[test]
    fn consumed_and_high_water_are_compared_only_within_one_lineage() {
        let page = ConsumedPastHighWater {
            high_water_lineage: "inst:3",
        }
        .pull(None, 20)
        .unwrap();
        assert_eq!(validate_page(&page), Ok(()));
        let mut caught_up = page;
        caught_up.high_water = caught_up.cursor.clone();
        assert_eq!(validate_page(&caught_up), Ok(()));
    }

    #[test]
    fn same_instance_lower_epoch_is_refused_and_nothing_is_written() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        let result = drain_once(
            &state,
            &RegressedPage {
                instance: "inst",
                epoch: 1,
                seq: 9_000,
            },
            "broca",
            20,
            99,
        );
        assert!(
            matches!(result, Err(PullError::LineageRegressed { .. })),
            "{result:?}"
        );
        assert_eq!(stored_state(&state), ("inst:2".into(), 500, 50, 0));
    }

    /// The guard must not fire on the two legitimate backward-looking shapes:
    /// a higher epoch whose sequence restarted, and a different instance
    /// (the announced-identity-change path). Without this twin the
    /// regression tests could pass against a guard that refuses every page.
    #[test]
    fn higher_epoch_with_restarted_sequence_and_new_instance_both_apply() {
        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        drain_once(
            &state,
            &RegressedPage {
                instance: "inst",
                epoch: 3,
                seq: 4,
            },
            "broca",
            20,
            99,
        )
        .unwrap();
        assert_eq!(stored_state(&state), ("inst:3".into(), 4, 99, 1));

        let state = test_state();
        seed_cursor(&state, "inst", 2, 500);
        drain_once(
            &state,
            &RegressedPage {
                instance: "other",
                epoch: 0,
                seq: 1,
            },
            "broca",
            20,
            99,
        )
        .unwrap();
        assert_eq!(stored_state(&state), ("other:0".into(), 1, 99, 1));
    }
}

#[cfg(test)]
mod harness_ingest_tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    use astrocyte_core::store::MIGRATIONS;

    use super::*;
    use crate::state::test_state;

    const INGEST_ON: &str = r#"{"enabled":true,"exclude":[]}"#;

    /// Every local reader's source key, in drain order.
    const READER_KEYS: [&str; 8] = [
        "opencode",
        "pi",
        "claude-code",
        "codex",
        "gemini",
        "goose",
        "omp",
        "kimi",
    ];

    /// Every local reader, each pointed at a path that fails if it is read: a
    /// regular file where a directory or a database is expected. A reader
    /// that is skipped leaves no trace; one that reads records the failure.
    struct Readers {
        opencode: opencode_source::OpenCodeSource,
        pi: pi_source::PiSource,
        claude_code: claude_code_source::ClaudeCodeSource,
        codex: codex_source::CodexSource,
        harnesses: Vec<Box<dyn harness_drain::HarnessSource>>,
    }

    fn readers_that_fail_if_read(directory: &tempfile::TempDir) -> Readers {
        let file = directory.path().join("not-a-store");
        std::fs::write(&file, b"not a store").unwrap();
        // Codex looks for `sessions` below its home. Below a regular file,
        // Unix answers "not a directory" but Windows answers "not found",
        // which reads as an uninstalled harness. A real home whose `sessions`
        // is a file fails the directory read on every OS.
        let codex_home = directory.path().join("codex-home");
        std::fs::create_dir(&codex_home).unwrap();
        std::fs::write(codex_home.join("sessions"), b"not a directory").unwrap();
        Readers {
            opencode: opencode_source::OpenCodeSource::at(&file),
            pi: pi_source::PiSource::at(&file),
            claude_code: claude_code_source::ClaudeCodeSource::at(&file),
            codex: codex_source::CodexSource::at(&codex_home),
            harnesses: vec![
                Box::new(gemini_source::GeminiSource::at(&file)),
                Box::new(goose_source::GooseSource::at(&file)),
                Box::new(omp_source::OmpSource::at(&file)),
                Box::new(kimi_source::KimiSource::at(&file)),
            ],
        }
    }

    /// One refresher tick over `readers` and `producers`.
    fn tick(state: &SharedState, readers: &Readers, producers: &[Producer]) {
        let harnesses: Vec<&dyn harness_drain::HarnessSource> = readers
            .harnesses
            .iter()
            .map(|source| source.as_ref())
            .collect();
        run_iteration_all(
            state,
            producers,
            Some(&readers.opencode),
            Some(&readers.pi),
            Some(&readers.claude_code),
            Some(&readers.codex),
            &harnesses,
            None,
            false,
            &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
            &AtomicBool::new(false),
        );
    }

    /// Publish a desired document with this `harness_ingest` member through
    /// the production reload path.
    fn publish(state: &Arc<SharedState>, directory: &tempfile::TempDir, ingest: &str) {
        let path = directory.path().join("astrocyte.jsonc");
        std::fs::write(
            &path,
            format!(
                r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{{"api_twins":{{}},"plan_price_overrides":{{}},"quota_providers":{{}}}},"harness_ingest":{ingest}}}"#
            ),
        )
        .unwrap();
        reload_config(state, Ok(path)).unwrap();
    }

    fn runtime_keys(state: &SharedState) -> Vec<String> {
        state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT source_key FROM source_runtime ORDER BY source_key")
                    .unwrap();
                let keys = statement
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<String>>>()
                    .unwrap();
                Ok(keys)
            })
            .unwrap()
    }

    fn count(state: &SharedState, table: &str) -> i64 {
        state
            .with_conn(|conn| {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap()
    }

    /// A BROCA stand-in that serves the wire fixture's one fact from the
    /// start of its lineage and records every cursor it was asked from.
    #[derive(Default)]
    struct RecordingBroca {
        pulls: AtomicUsize,
        since: Mutex<Vec<Option<SourceCursor>>>,
    }

    impl SourceClient for RecordingBroca {
        fn pull(&self, since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            self.since.lock().unwrap().push(since);
            let event = crate::wire_map::map_fact(
                serde_json::from_value(crate::wire_map::wire_fixture()).unwrap(),
            )
            .unwrap();
            let position = SourceCursor {
                lineage: "instance:1".into(),
                ordinal: 7,
            };
            Ok(ExportPage {
                set_aside: Vec::new(),
                cursor: position.clone(),
                high_water: position,
                events: vec![event],
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    /// An excluded reader's store is never opened, its runtime row is
    /// removed, and health reads ok. Re-enabling it reads it again.
    #[test]
    fn an_excluded_reader_is_never_read_and_its_runtime_row_is_removed() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(test_state());
        let readers = readers_that_fail_if_read(&directory);
        let pi_only = |state: &SharedState| {
            run_iteration_with_pi(state, &[], &readers.pi, &AtomicBool::new(false))
        };
        // The migrations seed a BROCA runtime row, and this test drains only
        // Pi, so that row would stay cold and degrade health. Removing it
        // lets health speak only for Pi.
        harness_drain::remove_runtime_row(&state, "broca").unwrap();

        // Control: with Pi on, the drain reads the path and fails on it.
        publish(&state, &directory, INGEST_ON);
        pi_only(&state);
        let reason = state.producer_degraded_reason("pi").expect("Pi was read");
        assert!(reason.contains("read Pi session root"), "{reason}");
        assert!(runtime_keys(&state).contains(&"pi".to_string()));

        publish(&state, &directory, r#"{"enabled":true,"exclude":["pi"]}"#);
        pi_only(&state);
        assert_eq!(state.producer_degraded_reason("pi"), None);
        assert!(!runtime_keys(&state).contains(&"pi".to_string()));
        let health = state.public_projection().health(current_time_ms());
        assert_eq!(health.status, "ok", "{:?}", health.degraded_reason);

        // Re-enabled, it reads its path from scratch again.
        publish(&state, &directory, INGEST_ON);
        pi_only(&state);
        assert!(state.producer_degraded_reason("pi").is_some());
        assert!(runtime_keys(&state).contains(&"pi".to_string()));
    }

    /// With ingestion off no producer drains -- no reader opens its store,
    /// BROCA's included -- and every producer's runtime row is removed.
    #[test]
    fn ingestion_off_drains_no_producer_and_removes_every_runtime_row() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(test_state());
        let readers = readers_that_fail_if_read(&directory);
        let broca = Arc::new(RecordingBroca::default());
        let producers = [Producer {
            source_key: "broca".into(),
            client: broca.clone(),
        }];

        // Control: every reader reads its path and fails, BROCA is read
        // once, and each has a runtime row.
        publish(&state, &directory, INGEST_ON);
        tick(&state, &readers, &producers);
        for key in READER_KEYS {
            assert!(state.producer_degraded_reason(key).is_some(), "{key}");
        }
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        let mut every_key: Vec<String> = READER_KEYS
            .iter()
            .chain(["broca"].iter())
            .map(|key| key.to_string())
            .collect();
        every_key.sort();
        assert_eq!(runtime_keys(&state), every_key);

        publish(&state, &directory, r#"{"enabled":false,"exclude":[]}"#);
        tick(&state, &readers, &producers);
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        assert!(runtime_keys(&state).is_empty());
        for key in every_key {
            assert_eq!(state.producer_degraded_reason(&key), None, "{key}");
        }
        let health = state.public_projection().health(current_time_ms());
        assert_eq!(health.status, "ok", "{:?}", health.degraded_reason);
        // The event BROCA recorded before stays.
        assert_eq!(count(&state, "usage_event"), 1);
    }

    /// An excluded BROCA's store is never opened: the real reader, pointed at
    /// a file that fails if read, records no failure, and no BROCA runtime
    /// row is left. The control run shows the same reader does fail on it.
    #[test]
    fn an_excluded_broca_never_opens_its_store() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-store");
        std::fs::write(&file, b"not a store").unwrap();
        let producers = [Producer {
            source_key: "broca".into(),
            client: Arc::new(broca_source::BrocaSource::at(
                &file,
                Arc::new(AtomicBool::new(false)),
            )),
        }];
        let state = Arc::new(test_state());

        publish(&state, &directory, INGEST_ON);
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert!(state.producer_degraded_reason("broca").is_some());

        publish(
            &state,
            &directory,
            r#"{"enabled":true,"exclude":["broca"]}"#,
        );
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert!(!runtime_keys(&state).contains(&"broca".to_string()));
        assert_eq!(state.producer_degraded_reason("broca"), None);
    }

    /// Excluding BROCA removes its runtime row and with it the cursor.
    /// Re-enabled, it is read from the start of its store; the fact it holds
    /// carries the same content-derived event id, so nothing is recorded
    /// twice and nothing is quarantined.
    #[test]
    fn re_enabled_broca_repulls_from_the_start_without_duplicates_or_quarantine() {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(test_state());
        let broca = Arc::new(RecordingBroca::default());
        let producers = [Producer {
            source_key: "broca".into(),
            client: broca.clone(),
        }];
        let consumed = |state: &SharedState| -> Option<i64> {
            state
                .with_conn(|conn| {
                    conn.query_row(
                        "SELECT consumed_ordinal FROM source_runtime WHERE source_key='broca'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| error.to_string())
                })
                .unwrap()
        };

        publish(&state, &directory, INGEST_ON);
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert_eq!(consumed(&state), Some(7));
        let events = count(&state, "usage_event");
        let measurements = count(&state, "measurement");
        assert_eq!(events, 1);

        publish(
            &state,
            &directory,
            r#"{"enabled":true,"exclude":["broca"]}"#,
        );
        run_iteration(&state, &producers, &AtomicBool::new(false));
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        assert!(!runtime_keys(&state).contains(&"broca".to_string()));

        publish(&state, &directory, INGEST_ON);
        run_iteration(&state, &producers, &AtomicBool::new(false));
        let since = broca.since.lock().unwrap().clone();
        assert_eq!(
            since,
            [None, None],
            "re-enabled BROCA is read from the start"
        );
        assert_eq!(consumed(&state), Some(7));
        assert_eq!(count(&state, "usage_event"), events);
        assert_eq!(count(&state, "measurement"), measurements);
        assert_eq!(count(&state, "quarantine_evidence"), 0);
        assert_eq!(state.producer_degraded_reason("broca"), None);
    }

    /// A generation committed before the store could hold `harness_ingest`
    /// hydrates as all on, never as off: every reader is read, BROCA's
    /// included, exactly as before the member existed.
    #[test]
    fn a_generation_without_the_member_drains_every_producer() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("ledger.db");
        {
            let store = open_sqlite(&StorageDescriptor {
                module_id: "ck-astrocyte-leased-test".to_string(),
                storage_namespace: "astrocyte".to_string(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: database.to_string_lossy().into_owned(),
                },
            })
            .unwrap();
            let before_ingest = MIGRATIONS
                .iter()
                .position(|migration| migration.version == 17)
                .unwrap();
            store
                .migrate("astrocyte", &MIGRATIONS[..before_ingest])
                .unwrap();
        }
        {
            let conn = rusqlite::Connection::open(&database).unwrap();
            conn.execute(
                "UPDATE desired_config SET generation=3, poll_interval_secs=45,
                     subscription_multiplier='{\"api_twins\":{},\"plan_price_overrides\":{},\"quota_providers\":{},\"reasoning_billed_as_output\":[]}',
                     content_hash='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                 WHERE id=1",
                [],
            )
            .unwrap();
            conn.execute("UPDATE source_runtime SET desired_generation=(SELECT generation FROM desired_config WHERE id=1)", [])
                .unwrap();
        }
        let state = state::leased_test_state(&directory);
        assert_eq!(state.desired_generation(), 3);
        assert_eq!(
            state.desired_config().harness_ingest,
            config::HarnessIngest::all_on()
        );
        let readers = readers_that_fail_if_read(&directory);
        let broca = Arc::new(RecordingBroca::default());
        tick(
            &state,
            &readers,
            &[Producer {
                source_key: "broca".into(),
                client: broca.clone(),
            }],
        );
        for key in READER_KEYS {
            assert!(
                state.producer_degraded_reason(key).is_some(),
                "{key} was read"
            );
        }
        assert_eq!(broca.pulls.load(Ordering::SeqCst), 1);
        assert_eq!(count(&state, "usage_event"), 1);
    }

    /// Runs this binary's `MIGRATIONS` over a COPY of the live store, whose
    /// path is in `INGEST_REHEARSAL_DB`, and hydrates it as startup does: the
    /// committed generation, written before `harness_ingest` existed, keeps
    /// its number and comes back all on, and every runtime row survives.
    /// Ignored by default: it needs the copy.
    #[test]
    #[ignore]
    fn ingest_rehearsal_on_a_store_copy() {
        use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
        let database = std::env::var("INGEST_REHEARSAL_DB").expect("INGEST_REHEARSAL_DB");
        let before: (i64, i64) = {
            let conn = rusqlite::Connection::open(&database).unwrap();
            conn.query_row(
                "SELECT (SELECT generation FROM desired_config WHERE id=1),
                        (SELECT COUNT(*) FROM source_runtime)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
        };
        let store = open_sqlite(&StorageDescriptor {
            module_id: "ck-astrocyte-rehearsal".to_string(),
            storage_namespace: "astrocyte".to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite { path: database },
        })
        .unwrap();
        astrocyte_core::store::migrate_store(&store).unwrap();
        let state = SharedState::from_store(store).unwrap();
        assert_eq!(state.desired_generation(), u64::try_from(before.0).unwrap());
        assert_eq!(
            state.desired_config().harness_ingest,
            config::HarnessIngest::all_on()
        );
        assert_eq!(
            runtime_keys(&state).len(),
            usize::try_from(before.1).unwrap()
        );
        println!(
            "generation {} runtime rows {} harness_ingest {:?}",
            before.0,
            before.1,
            state.desired_config().harness_ingest
        );
    }
}
