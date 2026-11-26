//! Resolves a ledger event's attribution by indexed lookups of only the
//! subjects, alias components and projects that the current build or report
//! asks about. Each answer is memoized for the life of one resolver, so a build
//! costs work in proportion to the distinct subjects in its events, never to
//! the total history of facts, placements or aliases in the store.
//!
//! The rules are the ones attribution has always used: a run fact wins over a
//! session fact, which wins over a flow fact. A subject's winning fact is its
//! newest valid, unsuperseded fact. A session is found under every name
//! connected to it through session aliases, and under each fact's computed
//! `session_ref`. `thalamus-harness` and `thalamus` name the same runner.
use super::{
    normalized_runner, sql, Attribution, Subject, VALID_CORRECTION, VALID_PLACEMENT_CORRECTION,
};
use crate::broca::ScopeStamp;
use cortexkit_role_llm_runner::{session_ref, SessionIdentity};
use rusqlite::{params, Connection, Row};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

const COLUMNS: &str = "f.fact_id,f.alfonso,f.parent_kind,f.parent_runner,f.parent_harness,f.parent_session,f.parent_run_id,f.parent_flow_id,f.workload,f.project_id,f.effective_from_ms,f.recorded_at_ms,f.supersedes";

pub(super) fn group_sql() -> String {
    format!("SELECT {COLUMNS} FROM attribution_fact f INDEXED BY attribution_fact_session WHERE f.subject_kind='session' AND f.subject_runner=?1 AND f.subject_session=?2 AND f.subject_harness=?3 AND {VALID_CORRECTION}")
}
pub(super) const NAME_SQL: &str = "SELECT DISTINCT subject_runner,subject_harness FROM attribution_fact INDEXED BY attribution_fact_session WHERE subject_kind='session' AND subject_runner IN (?1,?2) AND subject_session=?3";
pub(super) const REF_SQL: &str = "SELECT DISTINCT f.subject_runner,f.subject_harness,f.subject_session FROM attribution_session_ref s CROSS JOIN attribution_fact f ON f.fact_id=s.fact_id WHERE s.runner IN (?1,?2) AND s.session_ref=?3 AND f.subject_kind='session'";
pub(super) fn run_sql() -> String {
    format!("SELECT {COLUMNS},f.subject_harness FROM attribution_fact f INDEXED BY attribution_fact_run WHERE f.subject_kind='run' AND f.subject_run_id=?1 AND {VALID_CORRECTION}")
}
pub(super) fn flow_sql() -> String {
    format!("SELECT {COLUMNS} FROM attribution_fact f INDEXED BY attribution_fact_flow WHERE f.subject_kind='flow' AND f.subject_flow_id=?1 AND {VALID_CORRECTION}")
}
pub(super) fn placement_sql() -> String {
    format!("SELECT f.fact_id,f.workspace_id,f.effective_from_ms,f.recorded_at_ms,f.supersedes FROM placement_fact f INDEXED BY placement_fact_project_time WHERE f.project_id=?1 AND {VALID_PLACEMENT_CORRECTION}")
}
pub(super) const ALIAS_HOST_SQL: &str = "SELECT bound_session FROM session_alias INDEXED BY session_alias_host WHERE host_session=?1 AND runner IN (?2,?3)";
pub(super) const ALIAS_BOUND_SQL: &str = "SELECT host_session FROM session_alias INDEXED BY session_alias_bound WHERE bound_session=?1 AND runner IN (?2,?3)";

/// Both stored spellings of a normalized runner, so one query finds every row
/// the normalized name stands for.
fn spellings(normalized: &str) -> (&str, &str) {
    if normalized == "thalamus" {
        ("thalamus", "thalamus-harness")
    } else {
        (normalized, normalized)
    }
}

struct Candidate {
    fact_id: String,
    alfonso: Option<String>,
    parent: Option<Subject>,
    workload: String,
    project_id: Option<String>,
    effective_from_ms: i64,
    recorded_at_ms: i64,
    supersedes: Option<String>,
}
impl Candidate {
    fn order(&self) -> (i64, i64, &str) {
        (self.recorded_at_ms, self.effective_from_ms, &self.fact_id)
    }
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        let text = |index| row.get::<_, Option<String>>(index);
        let required = |index| {
            text(index)?.ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    index,
                    "parent subject column".into(),
                    rusqlite::types::Type::Null,
                )
            })
        };
        // The columns are written from the same subject the stored body
        // carries, so reading them avoids decoding the JSON body.
        let parent = match text(2)?.as_deref() {
            None => None,
            Some("session") => Some(Subject::Session {
                runner: text(3)?,
                harness: required(4)?,
                session: required(5)?,
            }),
            Some("run") => Some(Subject::Run {
                harness: required(4)?,
                run_id: required(6)?,
            }),
            Some(_) => Some(Subject::Flow {
                flow_id: required(7)?,
            }),
        };
        Ok(Self {
            fact_id: row.get(0)?,
            alfonso: text(1)?,
            parent,
            workload: row.get(8)?,
            project_id: text(9)?,
            effective_from_ms: row.get(10)?,
            recorded_at_ms: row.get(11)?,
            supersedes: text(12)?,
        })
    }
}

/// A subject's winner is its newest valid fact that no valid fact of the same
/// subject supersedes. Every candidate passed in is already a valid correction.
fn winner(candidates: Vec<Candidate>) -> Option<Candidate> {
    let superseded: HashSet<String> = candidates
        .iter()
        .filter_map(|fact| fact.supersedes.clone())
        .collect();
    candidates
        .into_iter()
        .filter(|fact| !superseded.contains(&fact.fact_id))
        .max_by(|a, b| a.order().cmp(&b.order()))
}

struct Placement {
    workspace_id: Option<String>,
    effective_from_ms: i64,
}

type Pair = (String, String);

/// Answers are kept for the life of one summary build, valuation build or
/// report. Within that life the attribution revision check discards a build
/// whose hour gained evidence, so memoized answers never outlive a change
/// that matters to them.
#[derive(Default)]
pub(crate) struct Resolver {
    winners: Vec<Candidate>,
    parents: HashMap<usize, Option<String>>,
    /// Session subject (runner as stored, harness, session) to its winner.
    groups: HashMap<(String, String, String), Option<usize>>,
    /// (normalized runner, name) to the newest winner filed under that name.
    names: HashMap<Pair, Option<usize>>,
    /// (normalized runner, name) to every name in its alias component.
    components: HashMap<Pair, Arc<[String]>>,
    /// (normalized runner, session) to the newest winner across its aliases.
    sessions: HashMap<Pair, Option<usize>>,
    runs: HashMap<String, Option<usize>>,
    flows: HashMap<String, Option<usize>>,
    placements: HashMap<String, Arc<[Placement]>>,
}

impl Attribution {
    fn publisher_disagreement(&self, stated_agent: Option<&str>) -> Option<bool> {
        // A publisher fact always supplies a workload, even when it does not
        // name an agent. Stated evidence cannot fill in that missing agent.
        self.workload.as_ref().map(|_| {
            self.alfonso
                .as_deref()
                .zip(stated_agent)
                .is_some_and(|(publisher, stated)| publisher != stated)
        })
    }

    pub(crate) fn with_media_caller(mut self, agent: &str) -> (Self, bool) {
        if let Some(disagrees) = self.publisher_disagreement(Some(agent)) {
            return (self, disagrees);
        }
        self.alfonso = Some(agent.to_owned());
        (self, false)
    }
}

impl Resolver {
    pub(crate) fn resolve(
        &mut self,
        conn: &Connection,
        runner: Option<&str>,
        session: Option<&str>,
        run: Option<&str>,
        flow: Option<&str>,
        at: Option<i64>,
    ) -> Result<Attribution, String> {
        let mut id = match run {
            Some(run) => self.run(conn, run)?,
            None => None,
        };
        if id.is_none() {
            if let Some((runner, session)) = runner.zip(session) {
                id = self.session(conn, runner, session)?;
            }
        }
        if id.is_none() {
            if let Some(flow) = flow {
                id = self.flow(conn, flow)?;
            }
        }
        let Some(id) = id else {
            return Ok(Attribution::default());
        };
        self.publisher_attribution(conn, id, at)
    }

    pub(crate) fn apply_stamp(
        &mut self,
        conn: &Connection,
        tuple: Attribution,
        stamp: &ScopeStamp,
        call_kind: &str,
        at: Option<i64>,
    ) -> Result<(Attribution, bool), String> {
        if let Some(disagrees) = tuple.publisher_disagreement(stamp.agent_id.as_deref()) {
            return Ok((tuple, disagrees));
        }
        if let Some(flow) = &stamp.flow_id {
            return match self.flow(conn, flow)? {
                Some(id) => Ok((self.publisher_attribution(conn, id, at)?, false)),
                None => Ok((tuple, false)),
            };
        }
        let Some(agent) = &stamp.agent_id else {
            return Ok((tuple, false));
        };
        // The stamp names the caller. The stamped run describes its launch
        // context, not the caller's workload or agent identity.
        let parent = match &stamp.agent_run_id {
            Some(run) => self.run(conn, run)?,
            None => None,
        };
        let parent_alfonso = parent.and_then(|id| self.winners[id].alfonso.clone());
        let project_id = parent.and_then(|id| self.winners[id].project_id.clone());
        let workspace = self.workspace(conn, project_id.as_deref(), at)?;
        let workload = if call_kind == "system_one" {
            "system_one"
        } else {
            "broca_session"
        };
        Ok((
            Attribution {
                alfonso: Some(agent.clone()),
                parent_alfonso,
                workload: Some(workload.into()),
                project_id,
                workspace,
            },
            false,
        ))
    }

    fn publisher_attribution(
        &mut self,
        conn: &Connection,
        id: usize,
        at: Option<i64>,
    ) -> Result<Attribution, String> {
        let parent_alfonso = self.parent_alfonso(conn, id)?;
        let fact = &self.winners[id];
        let project_id = fact.project_id.clone();
        let (alfonso, workload) = (fact.alfonso.clone(), fact.workload.clone());
        let workspace = self.workspace(conn, project_id.as_deref(), at)?;
        Ok(Attribution {
            alfonso,
            parent_alfonso,
            workload: Some(workload),
            project_id,
            workspace,
        })
    }

    fn workspace(
        &mut self,
        conn: &Connection,
        project_id: Option<&str>,
        at: Option<i64>,
    ) -> Result<Option<String>, String> {
        Ok(match (project_id, at) {
            (Some(project), Some(at)) => {
                let placements = self.placements(conn, project)?;
                let through =
                    placements.partition_point(|placement| placement.effective_from_ms <= at);
                through
                    .checked_sub(1)
                    .and_then(|index| placements[index].workspace_id.clone())
            }
            _ => None,
        })
    }

    fn newer(&self, current: Option<usize>, other: Option<usize>) -> Option<usize> {
        match (current, other) {
            (Some(a), Some(b)) => Some(if self.winners[a].order() < self.winners[b].order() {
                b
            } else {
                a
            }),
            (a, b) => a.or(b),
        }
    }
    fn keep(&mut self, winner: Option<Candidate>) -> Option<usize> {
        winner.map(|winner| {
            self.winners.push(winner);
            self.winners.len() - 1
        })
    }

    fn parent_alfonso(&mut self, conn: &Connection, id: usize) -> Result<Option<String>, String> {
        if let Some(known) = self.parents.get(&id) {
            return Ok(known.clone());
        }
        let parent = self.winners[id].parent.clone();
        let owner = match parent.as_ref() {
            Some(parent) => self
                .subject(conn, parent)?
                .and_then(|owner| self.winners[owner].alfonso.clone()),
            None => None,
        };
        self.parents.insert(id, owner.clone());
        Ok(owner)
    }

    fn subject(&mut self, conn: &Connection, subject: &Subject) -> Result<Option<usize>, String> {
        match subject {
            Subject::Session {
                runner: Some(runner),
                ..
            } => {
                let mut best = None;
                for name in subject.names() {
                    let found = self.session(conn, runner, &name)?;
                    best = self.newer(best, found);
                }
                Ok(best)
            }
            Subject::Session { runner: None, .. } => Ok(None),
            Subject::Run { run_id, .. } => self.run(conn, run_id),
            Subject::Flow { flow_id } => self.flow(conn, flow_id),
        }
    }

    fn session(
        &mut self,
        conn: &Connection,
        runner: &str,
        session: &str,
    ) -> Result<Option<usize>, String> {
        let runner = normalized_runner(runner);
        let key = (runner.to_owned(), session.to_owned());
        if let Some(known) = self.sessions.get(&key) {
            return Ok(*known);
        }
        let mut best = None;
        for name in self.component(conn, runner, session)?.iter() {
            let found = self.name(conn, runner, name)?;
            best = self.newer(best, found);
        }
        self.sessions.insert(key, best);
        Ok(best)
    }

    /// Walks the alias graph outward from one name through the host and bound
    /// indexes, comparing runners by their normalized spelling and ignoring
    /// harness labels, and remembers the whole component for each member.
    fn component(
        &mut self,
        conn: &Connection,
        runner: &str,
        name: &str,
    ) -> Result<Arc<[String]>, String> {
        let key = (runner.to_owned(), name.to_owned());
        if let Some(known) = self.components.get(&key) {
            return Ok(Arc::clone(known));
        }
        let (first, second) = spellings(runner);
        let mut host = sql(conn.prepare_cached(ALIAS_HOST_SQL))?;
        let mut bound = sql(conn.prepare_cached(ALIAS_BOUND_SQL))?;
        let mut visited = BTreeSet::from([name.to_owned()]);
        let mut stack = vec![name.to_owned()];
        while let Some(current) = stack.pop() {
            for statement in [&mut host, &mut bound] {
                let rows =
                    sql(statement
                        .query_map(params![current, first, second], |r| r.get::<_, String>(0)))?;
                for neighbor in rows {
                    let neighbor = sql(neighbor)?;
                    if visited.insert(neighbor.clone()) {
                        stack.push(neighbor);
                    }
                }
            }
        }
        let names: Arc<[String]> = visited.into_iter().collect();
        for member in names.iter() {
            self.components
                .insert((runner.to_owned(), member.clone()), Arc::clone(&names));
        }
        Ok(names)
    }

    /// The newest winner among session subjects that answer to `name`, either
    /// as their raw session or as their computed `session_ref`.
    fn name(
        &mut self,
        conn: &Connection,
        runner: &str,
        name: &str,
    ) -> Result<Option<usize>, String> {
        let key = (runner.to_owned(), name.to_owned());
        if let Some(known) = self.names.get(&key) {
            return Ok(*known);
        }
        let (first, second) = spellings(runner);
        let mut subjects = BTreeSet::new();
        let mut raw = sql(conn.prepare_cached(NAME_SQL))?;
        let rows = sql(raw.query_map(params![first, second, name], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                name.to_owned(),
            ))
        }))?;
        for row in rows {
            subjects.insert(sql(row)?);
        }
        let mut by_ref = sql(conn.prepare_cached(REF_SQL))?;
        let rows = sql(by_ref.query_map(params![first, second, name], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        }))?;
        for row in rows {
            subjects.insert(sql(row)?);
        }
        let mut best = None;
        for subject in subjects {
            let found = self.group(conn, subject)?;
            best = self.newer(best, found);
        }
        self.names.insert(key, best);
        Ok(best)
    }

    fn group(
        &mut self,
        conn: &Connection,
        subject: (String, String, String),
    ) -> Result<Option<usize>, String> {
        if let Some(known) = self.groups.get(&subject) {
            return Ok(*known);
        }
        let (runner, harness, session) = &subject;
        let mut statement = sql(conn.prepare_cached(&group_sql()))?;
        let candidates = sql(statement
            .query_map(params![runner, session, harness], Candidate::read)
            .and_then(Iterator::collect::<rusqlite::Result<Vec<_>>>))?;
        let id = self.keep(winner(candidates));
        self.groups.insert(subject, id);
        Ok(id)
    }

    /// A run is found by its id alone. Should two run harnesses ever share an
    /// id, the greatest harness with a winner answers, as it always has.
    fn run(&mut self, conn: &Connection, run_id: &str) -> Result<Option<usize>, String> {
        if let Some(known) = self.runs.get(run_id) {
            return Ok(*known);
        }
        let mut statement = sql(conn.prepare_cached(&run_sql()))?;
        let mut harnesses: BTreeMap<String, Vec<Candidate>> = BTreeMap::new();
        let rows = sql(statement.query_map([run_id], |row| {
            Ok((row.get::<_, String>(13)?, Candidate::read(row)?))
        }))?;
        for row in rows {
            let (harness, candidate) = sql(row)?;
            harnesses.entry(harness).or_default().push(candidate);
        }
        let found = harnesses.into_values().rev().find_map(winner);
        let id = self.keep(found);
        self.runs.insert(run_id.to_owned(), id);
        Ok(id)
    }

    fn flow(&mut self, conn: &Connection, flow_id: &str) -> Result<Option<usize>, String> {
        if let Some(known) = self.flows.get(flow_id) {
            return Ok(*known);
        }
        let mut statement = sql(conn.prepare_cached(&flow_sql()))?;
        let candidates = sql(statement
            .query_map([flow_id], Candidate::read)
            .and_then(Iterator::collect::<rusqlite::Result<Vec<_>>>))?;
        let id = self.keep(winner(candidates));
        self.flows.insert(flow_id.to_owned(), id);
        Ok(id)
    }

    /// A project's placements in force order: valid and unsuperseded, sorted by
    /// effective time, then record time, then fact id.
    fn placements(&mut self, conn: &Connection, project: &str) -> Result<Arc<[Placement]>, String> {
        if let Some(known) = self.placements.get(project) {
            return Ok(Arc::clone(known));
        }
        let mut statement = sql(conn.prepare_cached(&placement_sql()))?;
        let rows = sql(statement
            .query_map([project], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?,
                ))
            })
            .and_then(Iterator::collect::<rusqlite::Result<Vec<_>>>))?;
        let superseded: HashSet<&String> = rows.iter().filter_map(|row| row.4.as_ref()).collect();
        let mut kept: Vec<_> = rows
            .iter()
            .filter(|row| !superseded.contains(&row.0))
            .collect();
        kept.sort_by(|a, b| (a.2, a.3, &a.0).cmp(&(b.2, b.3, &b.0)));
        let placements: Arc<[Placement]> = kept
            .into_iter()
            .map(|row| Placement {
                workspace_id: row.1.clone(),
                effective_from_ms: row.2,
            })
            .collect();
        self.placements
            .insert(project.to_owned(), Arc::clone(&placements));
        Ok(placements)
    }
}

/// The `session_ref` a session fact answers to, kept beside the fact so the
/// resolver can find it by index. Only session subjects with a runner have one.
pub(super) fn subject_ref(runner: &str, harness: &str, session: &str) -> String {
    session_ref(&SessionIdentity {
        runner,
        harness,
        session,
    })
}

#[cfg(test)]
mod tests;
