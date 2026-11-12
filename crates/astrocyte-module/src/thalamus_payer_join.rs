//! Transactional payer selection and bounded, process-local transcript waits.

use std::collections::{BTreeMap, HashMap, HashSet};

use astrocyte_core::{
    fact::{event_id, ChargeBasis},
    ingest::EventRecordRequest,
};
use rusqlite::{params, OptionalExtension, Transaction};

use super::thalamus_payer_source::{count_coverage, mark_conflicts, payer_match, PayerMatch};

pub(crate) const PAYER_WAIT_MS: i64 = 600_000;
pub(crate) const MAX_WAITING_CALLS: usize = 10_000;

/// A completed transcript call. Completion is separate from the
/// event payload, whose occurrence time may be the start of a streamed call.
#[derive(Debug, Clone)]
pub(crate) struct WaitingCall {
    pub(crate) request: EventRecordRequest,
    pub(crate) completed_at_ms: Option<i64>,
    /// The transcript's native session id (Claude Code's own id) when
    /// `request.session` holds core's bound conversation id. Payer membership
    /// and deletion acknowledgements still use this native id.
    pub(crate) native_session: Option<String>,
}

impl WaitingCall {
    fn native_session(&self) -> Option<&str> {
        self.native_session
            .as_deref()
            .or(self.request.session.as_deref())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WaitQueue {
    waiting: BTreeMap<String, WaitingCall>,
    overflow_reported: bool,
}

/// The proposed queue and events belong to one ledger transaction. Record all
/// `ready` events before committing, then replace the reader's queue with
/// `next`. On any failure keep the original queue and reader resume state.
pub(crate) struct Resolution {
    pub(crate) ready: Vec<EventRecordRequest>,
    pub(crate) next: WaitQueue,
    pub(crate) already_recorded: usize,
    pub(crate) overflow_started: bool,
}

impl WaitQueue {
    /// Refresh only the recorded session of unrecorded calls from current
    /// declarations. Keep the native id for payer membership and deletion acks.
    pub(crate) fn update_recorded_sessions(&mut self, sessions: &HashMap<&str, &str>) {
        for call in self.waiting.values_mut() {
            let Some(native_session) = call.native_session() else {
                continue;
            };
            let Some(&session) = sessions.get(native_session) else {
                continue;
            };
            let native_session = native_session.to_owned();
            call.request.session = Some(session.to_owned());
            call.native_session = Some(native_session);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }
    pub(crate) fn len(&self) -> usize {
        self.waiting.len()
    }

    /// Sessions still waiting cannot be acknowledged for deletion. A durable
    /// cursor or watermark must not advance past any of their waiting calls.
    pub(crate) fn pending_sessions(&self) -> HashSet<String> {
        self.waiting
            .values()
            .filter_map(WaitingCall::native_session)
            .map(str::to_owned)
            .collect()
    }

    /// Recheck existing waits before new calls on both the periodic full scan
    /// and the incremental scan of changed files, after the payer source has
    /// drained. This does not mutate the queue. Hold persisted read positions,
    /// deletion acknowledgements and watermarks until `next` is empty. A
    /// per-session acknowledgement may advance only for a session absent from
    /// `next.pending_sessions()`.
    pub(crate) fn resolve_in_tx(
        &self,
        tx: &Transaction<'_>,
        harness: &str,
        calls: &[WaitingCall],
        now_ms: i64,
    ) -> Result<Resolution, String> {
        let mut result = Resolution {
            ready: Vec::new(),
            next: Self {
                waiting: BTreeMap::new(),
                overflow_reported: self.overflow_reported,
            },
            already_recorded: 0,
            overflow_started: false,
        };
        for (was_waiting, call) in self
            .waiting
            .values()
            .map(|call| (true, call))
            .chain(calls.iter().map(|call| (false, call)))
        {
            let request = &call.request;
            let id = event_id(
                &request.producer,
                request.producer_lineage.as_deref(),
                &request.producer_ref,
            )
            .map_err(|e| format!("derive waiting call identity: {e:?}"))?;
            // A transcript may redeliver a queued call while replaying a held
            // durable cursor. Keep its completion and payload; the roots reader
            // refreshes only the queued session attribution from declarations.
            if !was_waiting && self.waiting.contains_key(&id) {
                continue;
            }
            let owner: Option<Option<String>> = tx
                .query_row(
                    "SELECT owner_principal FROM thalamus_payer_session WHERE harness=?1 AND session=?2",
                    params![harness, call.native_session()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?;
            let Some(owner) = owner else {
                result.ready.push(request.clone());
                continue;
            };
            if let (Some(owner), Some(roots_owner)) =
                (owner.as_deref(), request.work_class.as_deref())
            {
                if roots_owner != owner {
                    count_coverage(tx, "owner_disagreement", &id)?;
                }
            }
            // A declaration may arrive after the call was recorded. Keep the
            // first payload, including an absent work class, on transcript replay.
            let recorded: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM usage_event WHERE event_id=?1)",
                    [&id],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if recorded {
                result.already_recorded += 1;
                continue;
            }
            let mut ready = request.clone();
            if ready.work_class.is_none() {
                ready.work_class = owner;
            }
            match payer_match(tx, harness, &request.producer_ref)? {
                PayerMatch::Found {
                    basis,
                    auth,
                    account,
                } => {
                    ready.charge_basis = basis;
                    ready.auth_selection = Some(auth);
                    ready.account_ref = account;
                }
                PayerMatch::Conflict => {
                    mark_conflicts(tx, harness, &request.producer_ref, now_ms)?;
                    unknown(&mut ready);
                }
                PayerMatch::Missing => {
                    if result.next.waiting.contains_key(&id) {
                        continue;
                    }
                    let started = match call.completed_at_ms {
                        Some(completed) => completed,
                        None => first_seen(tx, &id, now_ms)?,
                    };
                    if now_ms.saturating_sub(started) < PAYER_WAIT_MS
                        && result.next.len() < MAX_WAITING_CALLS
                    {
                        result.next.waiting.insert(id, call.clone());
                        continue;
                    }
                    if now_ms.saturating_sub(started) < PAYER_WAIT_MS
                        && !result.next.overflow_reported
                    {
                        result.next.overflow_reported = true;
                        result.overflow_started = true;
                    }
                    unknown(&mut ready);
                    count_coverage(tx, "without_payer", &id)?;
                }
            }
            result.ready.push(ready);
        }
        if result.next.len() < MAX_WAITING_CALLS {
            result.next.overflow_reported = false;
        }
        Ok(result)
    }
}

fn unknown(request: &mut EventRecordRequest) {
    request.charge_basis = ChargeBasis::Unknown;
    request.auth_selection = None;
    request.account_ref = None;
}

fn first_seen(tx: &Transaction<'_>, id: &str, now_ms: i64) -> Result<i64, String> {
    tx.execute(
        "INSERT OR IGNORE INTO thalamus_payer_first_seen(event_id,first_seen_ms) VALUES (?1,?2)",
        params![id, now_ms],
    )
    .map_err(|e| e.to_string())?;
    tx.query_row(
        "SELECT first_seen_ms FROM thalamus_payer_first_seen WHERE event_id=?1",
        [id],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "waiting call first observation disappeared".into())
}

/// Call only after adopting a committed resolution, never before the write.
pub(crate) fn warn_overflow(source: &str, started: bool) {
    if started {
        tracing::warn!(
            source,
            limit = MAX_WAITING_CALLS,
            "payer wait queue full; new calls recorded without a payer"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{test_state, SharedState};
    use serde_json::json;

    fn call(reference: &str, completed: Option<i64>) -> WaitingCall {
        WaitingCall { request: serde_json::from_value(json!({"producer":"pi","producer_ref":reference,"session":"session-a","provider":"anthropic","model":"model-a","occurred_at_ms":100,"charge_basis":"unknown","measurements":[]})).unwrap(),completed_at_ms:completed, native_session:None }
    }
    fn manage(state: &SharedState) {
        state
            .ordered_write(|tx| {
                tx.execute(
                    "INSERT INTO thalamus_payer_session VALUES ('pi','session-a','owner',0)",
                    [],
                )
                .unwrap();
                Ok(((), true))
            })
            .unwrap();
    }
    fn payer(state: &SharedState, attempt: &str, step: &str, basis: Option<&str>, auth: &str) {
        state.ordered_write(|tx| {
            tx.execute("INSERT INTO thalamus_payer_record(attempt_id,seq,incarnation,harness,session,request_kind,step_ref,recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref) VALUES (?1,1,'first','pi','session-a','main',?2,0,'anthropic','model-a',?3,?4,'account')",params![attempt,step,basis,auth]).unwrap();
            Ok(((),true))
        }).unwrap();
    }
    fn pass(
        state: &SharedState,
        queue: &mut WaitQueue,
        calls: &[WaitingCall],
        now: i64,
    ) -> (usize, usize, bool) {
        let resolution = state
            .ordered_write(|tx| {
                let resolution = queue.resolve_in_tx(tx, "pi", calls, now)?;
                for request in &resolution.ready {
                    astrocyte_core::ingest::record_event_in_tx(
                        tx,
                        request,
                        now,
                        astrocyte_core::ingest::DeliveryPath::Read,
                    )
                    .map_err(|e| format!("{e:?}"))?;
                }
                Ok((resolution, true))
            })
            .unwrap();
        let counts = (
            resolution.ready.len(),
            resolution.already_recorded,
            resolution.overflow_started,
        );
        warn_overflow("fixture", resolution.overflow_started);
        *queue = resolution.next;
        counts
    }
    fn stored(state: &SharedState) -> Vec<(String, String, Option<String>, Option<String>)> {
        state.with_conn(|conn|conn.prepare("SELECT producer_ref,charge_basis,auth_selection,account_ref FROM usage_event ORDER BY producer_ref").map_err(|e|e.to_string())?.query_map([],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).map_err(|e|e.to_string())?.collect::<rusqlite::Result<Vec<_>>>().map_err(|e|e.to_string())).unwrap()
    }

    #[test]
    fn payer_wait_queue_unmanaged_call_is_byte_identical_and_immediate() {
        let state = test_state();
        let original = call("unmanaged", Some(100));
        let mut queue = WaitQueue::default();
        state
            .ordered_write(|tx| {
                let resolved =
                    queue.resolve_in_tx(tx, "pi", std::slice::from_ref(&original), 101)?;
                assert_eq!(resolved.ready, vec![original.request.clone()]);
                assert!(resolved.next.is_empty());
                Ok(((), true))
            })
            .unwrap();
        assert_eq!(pass(&state, &mut queue, &[original], 101).0, 1);
    }

    #[test]
    fn payer_wait_queue_matched_facts_are_stated_not_inferred_and_never_rewrite() {
        let state = test_state();
        manage(&state);
        payer(
            &state,
            "a",
            "known",
            Some("subscription_included"),
            "login-one",
        );
        payer(&state, "b", "null", None, "login-two");
        payer(&state, "c", "other", Some("future_basis"), "login-three");
        let calls = [
            call("known", Some(100)),
            call("null", Some(100)),
            call("other", Some(100)),
        ];
        let mut queue = WaitQueue::default();
        assert_eq!(pass(&state, &mut queue, &calls, 101).0, 3);
        assert_eq!(
            stored(&state),
            vec![
                (
                    "known".into(),
                    "subscription_included".into(),
                    Some("login-one".into()),
                    Some("account".into())
                ),
                (
                    "null".into(),
                    "unknown".into(),
                    Some("login-two".into()),
                    Some("account".into())
                ),
                (
                    "other".into(),
                    "unknown".into(),
                    Some("login-three".into()),
                    Some("account".into())
                )
            ]
        );
        payer(&state, "d", "known", Some("metered_api"), "different");
        assert_eq!(pass(&state, &mut queue, &calls, 102), (0, 3, false));
        assert_eq!(stored(&state)[0].1, "subscription_included");
    }

    #[test]
    fn payer_wait_queue_later_pass_records_once_and_timeout_uses_completion() {
        let state = test_state();
        manage(&state);
        let mut queue = WaitQueue::default();
        assert_eq!(
            pass(
                &state,
                &mut queue,
                &[call("later", Some(10_000)), call("never", Some(10_000))],
                10_001
            )
            .0,
            0
        );
        assert_eq!(
            queue.pending_sessions(),
            HashSet::from(["session-a".into()])
        );
        payer(&state, "a", "later", Some("metered_api"), "login");
        assert_eq!(pass(&state, &mut queue, &[], 10_002).0, 1);
        assert_eq!(queue.len(), 1);
        assert_eq!(pass(&state, &mut queue, &[], 609_999).0, 0);
        assert_eq!(pass(&state, &mut queue, &[], 610_000).0, 1);
        assert!(queue.is_empty());
        assert_eq!(
            stored(&state),
            vec![
                (
                    "later".into(),
                    "metered_api".into(),
                    Some("login".into()),
                    Some("account".into())
                ),
                ("never".into(), "unknown".into(), None, None)
            ]
        );
        assert_eq!(
            pass(&state, &mut queue, &[call("never", Some(10_000))], 620_000).1,
            1
        );
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind='without_payer'",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn payer_wait_queue_undated_wait_survives_restart_and_resolution_rolls_back() {
        let state = test_state();
        manage(&state);
        let original = call("undated", None);
        let mut queue = WaitQueue::default();
        pass(&state, &mut queue, std::slice::from_ref(&original), 10_000);
        // A restarted reader replays its held cursor or process-local files.
        queue = WaitQueue::default();
        assert_eq!(pass(&state, &mut queue, &[original], 609_999).0, 0);
        let failed = state.ordered_write::<()>(|tx| {
            let proposed = queue.resolve_in_tx(tx, "pi", &[], 610_000)?;
            assert_eq!(proposed.ready.len(), 1);
            assert!(proposed.next.is_empty());
            Err("injected failure before commit".into())
        });
        assert!(failed.is_err());
        assert_eq!(queue.len(), 1);
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row("SELECT COUNT(*) FROM thalamus_payer_coverage", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap(),
                    0
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(pass(&state, &mut queue, &[], 610_000).0, 1);
    }

    #[test]
    fn payer_wait_queue_conflict_sets_both_aside_and_records_unknown() {
        let state = test_state();
        manage(&state);
        payer(&state, "a", "conflicted", Some("metered_api"), "one");
        payer(&state, "b", "conflicted", Some("metered_api"), "two");
        let mut queue = WaitQueue::default();
        assert_eq!(
            pass(&state, &mut queue, &[call("conflicted", Some(100))], 101).0,
            1
        );
        assert_eq!(
            stored(&state),
            vec![("conflicted".into(), "unknown".into(), None, None)]
        );
        state
            .with_conn(|conn| {
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM set_aside_record WHERE source_key='thalamus-payer'",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    2
                );
                assert_eq!(
                    conn.query_row(
                        "SELECT COUNT(*) FROM thalamus_payer_coverage WHERE kind='set_aside'",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                    2
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn payer_wait_queue_bound_records_overflow_unknown_and_warns_once_per_episode() {
        let state = test_state();
        manage(&state);
        let calls = (0..=MAX_WAITING_CALLS)
            .map(|index| call(&format!("step-{index}"), Some(100)))
            .collect::<Vec<_>>();
        let mut queue = WaitQueue::default();
        assert_eq!(pass(&state, &mut queue, &calls, 101), (1, 0, true));
        assert_eq!(queue.len(), MAX_WAITING_CALLS);
        assert_eq!(
            pass(&state, &mut queue, &[call("another", Some(100))], 102),
            (1, 0, false)
        );
        assert_eq!(pass(&state, &mut queue, &[], 600_100).0, MAX_WAITING_CALLS);
        assert!(queue.is_empty());
    }
}
