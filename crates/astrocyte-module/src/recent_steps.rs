//! The serving side's memory of recent harness steps, so `usage.cache.steps`
//! can answer for an active session without reading the store.
//!
//! It keeps the newest steps of the sessions active in the last 24 hours,
//! within fixed bounds, as compact records. It is rebuilt from the store at
//! startup and then advanced after every committed write by reading only the
//! events recorded since the last advance (see `SharedState::refresh_projection`).
//! An answer from here is built by the same function from the same rows as an
//! answer from the store, so the two cannot differ; when memory cannot answer
//! exactly (the session is not here, or the window reaches past what is kept),
//! the caller reads the store instead.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use astrocyte_core::cache_diagnostics::{
    self as cache, Lead, StepCounts, StepRow, StepsQuery, StepsReply,
};
use rusqlite::Connection;

/// Steps kept per session: the most `usage.cache.steps` serves.
pub(crate) const MAX_STEPS_PER_SESSION: usize = 1_000;
/// Sessions kept, the most recently active first.
pub(crate) const MAX_SESSIONS: usize = 200;

const _: () = assert!(MAX_STEPS_PER_SESSION == cache::STEPS_MAX_LIMIT as usize);

/// Bit `i` of `StepRecord::present` says `counts[i]` was stored.
const INPUT: usize = 0;
const CACHE_READ: usize = 1;
const CACHE_WRITE: usize = 2;
const OUTPUT: usize = 3;
const REASONING: usize = 4;
/// Bit of `StepRecord::present` saying `context_limit` was stored.
const CONTEXT_LIMIT_BIT: u8 = 1 << 5;
const REQUEST_BODY_BIT: u8 = 1 << 6;

/// One step, compactly: the five token counts, the time and cursor, the
/// context limit, small ids for provider, model and finish, and the host's
/// step id and request body length. 96 bytes plus the step id's text.
#[derive(Clone, Debug)]
pub(crate) struct StepRecord {
    counts: [u64; 5],
    cursor: i64,
    occurred_at_ms: i64,
    context_limit: i64,
    request_body_len: u64,
    step_ref: Arc<str>,
    /// Ids into `Names`; 0 is "absent".
    provider: u16,
    model: u16,
    finish: u16,
    present: u8,
}

impl StepRecord {
    fn order(&self) -> (i64, i64) {
        (self.occurred_at_ms, self.cursor)
    }

    fn count(&self, index: usize) -> Option<u64> {
        (self.present & (1 << index) != 0).then_some(self.counts[index])
    }

    fn step_counts(&self) -> StepCounts {
        StepCounts {
            input: self.count(INPUT),
            cache_read: self.count(CACHE_READ),
            cache_write: self.count(CACHE_WRITE),
        }
    }

    fn body_len(&self) -> Option<u64> {
        (self.present & REQUEST_BODY_BIT != 0).then_some(self.request_body_len)
    }
}

/// Interned provider, model and finish names. Ids start at 1.
#[derive(Default)]
struct Names {
    ids: HashMap<Arc<str>, u16>,
    names: Vec<Arc<str>>,
}

impl Names {
    /// The id of `name`, or `None` when the table is full.
    fn id(&mut self, name: Option<&str>) -> Option<u16> {
        let Some(name) = name else {
            return Some(0);
        };
        if let Some(id) = self.ids.get(name) {
            return Some(*id);
        }
        let id = u16::try_from(self.names.len() + 1).ok()?;
        let name: Arc<str> = Arc::from(name);
        self.names.push(Arc::clone(&name));
        self.ids.insert(name, id);
        Some(id)
    }

    fn name(&self, id: u16) -> Option<String> {
        (id != 0).then(|| self.names[usize::from(id) - 1].to_string())
    }
}

/// The newest steps of one session, oldest first.
struct Window {
    records: VecDeque<StepRecord>,
    /// The lead of `records[0]`, which carries every earlier step of the
    /// session. `earlier_steps` is false only when `records` starts at the
    /// session's first step.
    lead: Lead,
    reads_cache: bool,
    undated: u64,
}

impl Window {
    fn last_activity_ms(&self) -> i64 {
        self.records
            .back()
            .map_or(i64::MIN, |record| record.occurred_at_ms)
    }
}

/// A step that memory cannot place exactly; the session is read again.
struct Stale;

#[derive(Default)]
pub(crate) struct RecentSteps {
    /// False until a rebuild succeeds, and after a failed advance. Nothing is
    /// answered from memory while false.
    ready: bool,
    /// Every event up to this key has been folded in.
    high_water: i64,
    sessions: HashMap<(String, String), Window>,
    names: Names,
}

impl RecentSteps {
    /// Replace the contents with the sessions active in the 24 hours before
    /// `now_ms`, read from `conn` in one snapshot.
    pub(crate) fn rebuild(&mut self, conn: &Connection, now_ms: i64) -> rusqlite::Result<()> {
        *self = Self::default();
        let high_water = cache::last_event_key(conn)?;
        for (producer, session, _) in cache::active_sessions(
            conn,
            now_ms.saturating_sub(cache::RECENT_WINDOW_MS),
            None,
            MAX_SESSIONS,
        )? {
            self.load(conn, producer, session)?;
        }
        self.high_water = high_water;
        self.ready = true;
        Ok(())
    }

    /// Fold in what was committed since the last call. Runs after a commit,
    /// on the writer's connection, under the writer fence, so nothing is
    /// committed meanwhile. `late` are the events that gained a
    /// `step_detail` row since the last call (the caller takes them with
    /// `cache::take_late_step_details`, because the live step feed needs
    /// them too). On failure memory stops answering; the store still does.
    pub(crate) fn advance(
        &mut self,
        conn: &Connection,
        late: &[i64],
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        if !self.ready {
            return Ok(());
        }
        let result = self.fold_in(conn, late, now_ms);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn fold_in(&mut self, conn: &Connection, late: &[i64], now_ms: i64) -> rusqlite::Result<()> {
        let recent_from = now_ms.saturating_sub(cache::RECENT_WINDOW_MS);
        let mut reload = HashSet::new();
        let recorded_steps = cache::steps_recorded_after(conn, self.high_water)?;
        // Nothing commits while this runs (the caller holds the writer
        // fence), so every event up to the store's last key is accounted for.
        self.high_water = cache::last_event_key(conn)?;
        for recorded in recorded_steps {
            let key = (recorded.producer, recorded.session);
            if reload.contains(&key) {
                continue;
            }
            let placed = match self.sessions.get_mut(&key) {
                Some(window) => match recorded.step {
                    None => {
                        window.undated += 1;
                        Ok(())
                    }
                    Some(step) => place(window, &mut self.names, &step),
                },
                // A session not in memory is read whole once a dated step of
                // it is recent; the read includes every step committed so far.
                None => match &recorded.step {
                    Some(step) if step.occurred_at_ms >= recent_from => Err(Stale),
                    _ => Ok(()),
                },
            };
            if placed.is_err() {
                reload.insert(key);
            }
        }
        for recorded in cache::steps_by_key(conn, late)? {
            let (Some(step), key) = (recorded.step, (recorded.producer, recorded.session)) else {
                continue;
            };
            if reload.contains(&key) {
                continue;
            }
            if let Some(window) = self.sessions.get_mut(&key) {
                if replace(window, &mut self.names, &step).is_err() {
                    reload.insert(key);
                }
            }
        }
        for (producer, session) in reload {
            self.sessions.remove(&(producer.clone(), session.clone()));
            self.load(conn, producer, session)?;
        }
        self.evict(recent_from);
        Ok(())
    }

    /// Read one session's newest steps from the store into memory. A session
    /// whose names do not fit the name table is left out; the store serves it.
    fn load(
        &mut self,
        conn: &Connection,
        producer: String,
        session: String,
    ) -> rusqlite::Result<()> {
        let loaded = cache::load_session(conn, &producer, &session, MAX_STEPS_PER_SESSION)?;
        let records = loaded
            .rows
            .iter()
            .map(|row| encode(&mut self.names, row))
            .collect::<Option<VecDeque<_>>>();
        if let Some(records) = records {
            self.sessions.insert(
                (producer, session),
                Window {
                    records,
                    lead: loaded.lead,
                    reads_cache: loaded.reads_cache,
                    undated: loaded.undated,
                },
            );
        }
        Ok(())
    }

    /// Drop sessions with no step in the recent window, then the least
    /// recently active ones beyond the session bound.
    fn evict(&mut self, recent_from: i64) {
        self.sessions
            .retain(|_, window| window.last_activity_ms() >= recent_from);
        if self.sessions.len() > MAX_SESSIONS {
            let mut by_activity = self
                .sessions
                .iter()
                .map(|(key, window)| (window.last_activity_ms(), key.clone()))
                .collect::<Vec<_>>();
            by_activity.sort();
            let excess = self.sessions.len() - MAX_SESSIONS;
            for (_, key) in by_activity.into_iter().take(excess) {
                self.sessions.remove(&key);
            }
        }
    }

    /// The reply for `query` from memory, or `None` when memory cannot give
    /// exactly the reply the store would.
    pub(crate) fn answer(&self, query: &StepsQuery) -> Option<StepsReply> {
        if !self.ready {
            return None;
        }
        let window = self
            .sessions
            .get(&(query.producer.clone(), query.session.clone()))?;
        let kept = window.records.len();
        let start = if kept >= query.limit {
            kept - query.limit
        } else if !window.lead.earlier_steps {
            0
        } else {
            return None;
        };
        let mut lead = window.lead;
        for record in window.records.range(..start) {
            lead.advance(&record.step_counts());
            lead.request_body_len = record.body_len();
        }
        let rows = window
            .records
            .range(start..)
            .map(|record| self.decode(record))
            .collect();
        Some(cache::steps_reply(
            query,
            rows,
            lead,
            window.reads_cache,
            window.undated,
        ))
    }

    fn decode(&self, record: &StepRecord) -> StepRow {
        StepRow {
            cursor: record.cursor,
            step_ref: record.step_ref.to_string(),
            occurred_at_ms: record.occurred_at_ms,
            input: record.count(INPUT),
            cache_read: record.count(CACHE_READ),
            cache_write: record.count(CACHE_WRITE),
            output: record.count(OUTPUT),
            reasoning: record.count(REASONING),
            provider: self.names.name(record.provider),
            model: self.names.name(record.model),
            finish: self.names.name(record.finish),
            context_limit: (record.present & CONTEXT_LIMIT_BIT != 0)
                .then_some(record.context_limit),
            request_body_len: record.body_len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_ready(&self) -> bool {
        self.ready
    }

    #[cfg(test)]
    pub(crate) fn kept(&self, producer: &str, session: &str) -> Option<usize> {
        self.sessions
            .get(&(producer.to_string(), session.to_string()))
            .map(|window| window.records.len())
    }
}

/// The compact record of `row`, or `None` when the name table is full.
fn encode(names: &mut Names, row: &StepRow) -> Option<StepRecord> {
    let mut present = 0;
    let mut counts = [0; 5];
    for (index, count) in [
        row.input,
        row.cache_read,
        row.cache_write,
        row.output,
        row.reasoning,
    ]
    .into_iter()
    .enumerate()
    {
        if let Some(count) = count {
            counts[index] = count;
            present |= 1 << index;
        }
    }
    if row.context_limit.is_some() {
        present |= CONTEXT_LIMIT_BIT;
    }
    if row.request_body_len.is_some() {
        present |= REQUEST_BODY_BIT;
    }
    Some(StepRecord {
        counts,
        cursor: row.cursor,
        occurred_at_ms: row.occurred_at_ms,
        context_limit: row.context_limit.unwrap_or(0),
        request_body_len: row.request_body_len.unwrap_or(0),
        step_ref: Arc::from(row.step_ref.as_str()),
        provider: names.id(row.provider.as_deref())?,
        model: names.id(row.model.as_deref())?,
        finish: names.id(row.finish.as_deref())?,
        present,
    })
}

/// Put a newly recorded step in its place in time order.
fn place(window: &mut Window, names: &mut Names, step: &StepRow) -> Result<(), Stale> {
    let record = encode(names, step).ok_or(Stale)?;
    let at = window
        .records
        .partition_point(|kept| kept.order() < record.order());
    if at == 0 && window.lead.earlier_steps {
        // Older than every kept step while older steps were dropped: where it
        // falls among those is unknown here, so the session is read again.
        return Err(Stale);
    }
    if record.step_counts().cache_read.is_some_and(|read| read > 0) {
        window.reads_cache = true;
    }
    window.records.insert(at, record);
    if window.records.len() > MAX_STEPS_PER_SESSION {
        if let Some(dropped) = window.records.pop_front() {
            window.lead.advance(&dropped.step_counts());
            window.lead.request_body_len = dropped.body_len();
        }
    }
    Ok(())
}

/// Replace a kept step with its newly read form (it gained `step_detail`).
/// A step memory does not keep needs nothing.
fn replace(window: &mut Window, names: &mut Names, step: &StepRow) -> Result<(), Stale> {
    if let Some(kept) = window
        .records
        .iter_mut()
        .find(|kept| kept.cursor == step.cursor)
    {
        *kept = encode(names, step).ok_or(Stale)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{test_state, SharedState};
    use astrocyte_core::fact::ChargeBasis;
    use astrocyte_core::ingest::{
        record_event_in_tx, DeliveryPath, EventRecordRequest, MeasurementInput, StepDetail,
    };

    /// The per-step cost the design budgets for: about 80 bytes plus the
    /// step id's text.
    #[test]
    fn a_kept_step_is_96_bytes_plus_its_id() {
        assert_eq!(std::mem::size_of::<StepRecord>(), 96);
    }

    const NOW: i64 = 10 * cache::RECENT_WINDOW_MS;

    fn request(
        session: &str,
        index: usize,
        at: i64,
        read: u64,
        finish: Option<&str>,
    ) -> EventRecordRequest {
        let token = |role: &str, units: u64| MeasurementInput {
            role: role.into(),
            unit: "token".into(),
            kind: "billed".into(),
            source: format!("usage.{role}"),
            quantity_units: Some(i64::try_from(units).unwrap()),
            quantity_exponent: Some(0),
        };
        EventRecordRequest {
            producer: "pi".into(),
            producer_lineage: None,
            producer_ref: format!("{session}:{index}"),
            prior_event_ref: None,
            harness: None,
            project_root: None,
            session: Some(session.into()),
            run_ref: None,
            flow_ref: None,
            work_class: None,
            occurred_at_ms: Some(at),
            emitted_at_ms: None,
            terminal_status: None,
            attempt_ordinal: None,
            vendor_request_id: None,
            provider: Some("anthropic".into()),
            model: Some(format!("model-{}", index % 3)),
            account_ref: None,
            auth_selection: None,
            charge_basis: ChargeBasis::Unknown,
            charge_basis_mapping_version: None,
            charge_basis_overrides: None,
            measurements: vec![
                token("input", 10),
                token("cache_read", read),
                token("output", 3),
            ],
            step_detail: StepDetail {
                finish: finish.map(str::to_string),
                context_limit: None,
            },
        }
    }

    /// Record through the writer, so the post-commit refresh advances memory.
    fn record(state: &SharedState, requests: &[EventRecordRequest]) {
        state
            .ordered_write(|tx| {
                for request in requests {
                    record_event_in_tx(tx, request, 1, DeliveryPath::Read)
                        .map_err(|error| format!("{error:?}"))?;
                }
                Ok(((), true))
            })
            .unwrap();
    }

    fn query(session: &str, limit: usize) -> StepsQuery {
        StepsQuery {
            producer: "pi".into(),
            session: session.into(),
            limit,
            after_cursor: None,
        }
    }

    fn from_store(state: &SharedState, query: &StepsQuery) -> StepsReply {
        state
            .with_read_op(|conn| {
                cache::steps_from_store(conn, query)
                    .map_err(|error| ("internal".into(), error.to_string()))
            })
            .unwrap()
    }

    fn ready_state() -> SharedState {
        let state = test_state();
        state.set_recent_steps_now_for_test(NOW);
        state.rebuild_recent_steps();
        state
    }

    /// An older event that gains its finish reason on an exact re-read is
    /// updated in memory too, so memory still answers what the store does.
    #[test]
    fn a_late_finish_reaches_memory() {
        let state = ready_state();
        record(
            &state,
            &[
                request("s", 0, NOW - 10, 5, None),
                request("s", 1, NOW - 5, 5, None),
            ],
        );
        let before = state.recent_steps_answer(&query("s", 10)).unwrap();
        assert_eq!(before.steps[0].finish, None);
        record(&state, &[request("s", 0, NOW - 10, 5, Some("stop"))]);
        let after = state.recent_steps_answer(&query("s", 10)).unwrap();
        assert_eq!(after.steps[0].finish.as_deref(), Some("stop"));
        assert_eq!(after, from_store(&state, &query("s", 10)));
    }

    /// A session keeps its newest 1,000 steps; the steps it dropped still
    /// shape the verdicts of the ones it keeps, and a late step older than
    /// every kept one makes memory read the session again.
    #[test]
    fn memory_keeps_the_newest_thousand_steps_and_answers_like_the_store() {
        let state = ready_state();
        let reads = |index: usize| [9_000, 0, 9_500, 4_000, 9_000][index % 5];
        let requests = (0..1_200)
            .map(|index| {
                request(
                    "long",
                    index,
                    NOW - 2_000 + i64::try_from(index).unwrap(),
                    reads(index),
                    None,
                )
            })
            .collect::<Vec<_>>();
        for chunk in requests.chunks(150) {
            record(&state, chunk);
        }
        state.with_recent_steps_for_test(|memory| {
            assert_eq!(memory.kept("pi", "long"), Some(MAX_STEPS_PER_SESSION));
        });
        for limit in [1, 200, 999, 1_000] {
            let memory = state.recent_steps_answer(&query("long", limit)).unwrap();
            assert_eq!(
                memory,
                from_store(&state, &query("long", limit)),
                "limit {limit}"
            );
        }
        // A step placed before the kept ones: memory reads the session again
        // and still agrees with the store.
        record(&state, &[request("long", 5_000, NOW - 1_999_000, 1, None)]);
        let memory = state.recent_steps_answer(&query("long", 1_000)).unwrap();
        assert_eq!(memory, from_store(&state, &query("long", 1_000)));
    }

    /// At most 200 sessions are kept, the most recently active, and none
    /// whose newest step is older than 24 hours.
    #[test]
    fn memory_keeps_at_most_two_hundred_recent_sessions() {
        let state = ready_state();
        let sessions = (0..210)
            .map(|index| format!("s{index:03}"))
            .collect::<Vec<_>>();
        let requests = sessions
            .iter()
            .enumerate()
            .map(|(index, session)| {
                request(
                    session,
                    0,
                    NOW - 1_000 + i64::try_from(index).unwrap(),
                    5,
                    None,
                )
            })
            .chain([request(
                "stale",
                0,
                NOW - cache::RECENT_WINDOW_MS - 1,
                5,
                None,
            )])
            .collect::<Vec<_>>();
        record(&state, &requests);
        state.with_recent_steps_for_test(|memory| {
            assert_eq!(memory.kept("pi", "s000"), None, "least recently active");
            assert_eq!(memory.kept("pi", "s009"), None);
            assert_eq!(memory.kept("pi", "s010"), Some(1));
            assert_eq!(memory.kept("pi", "s209"), Some(1));
            assert_eq!(memory.kept("pi", "stale"), None);
        });
        // Sessions not in memory are answered by the store.
        assert!(state.recent_steps_answer(&query("s000", 10)).is_none());
        assert_eq!(from_store(&state, &query("s000", 10)).steps.len(), 1);

        // A rebuild applies the same bounds.
        state.rebuild_recent_steps();
        state.with_recent_steps_for_test(|memory| {
            assert_eq!(memory.kept("pi", "s009"), None);
            assert_eq!(memory.kept("pi", "s010"), Some(1));
            assert_eq!(memory.kept("pi", "stale"), None);
        });
    }
}
