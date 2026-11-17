//! The whole-history attribution maps the store used before resolution moved
//! to per-subject indexed lookups. Kept only for tests, as the reference the
//! resolver must agree with on every query.
use super::*;
use std::collections::BTreeMap;

impl Fact {
    fn order(&self) -> (i64, i64, &str) {
        (self.recorded_at_ms, self.effective_from_ms, &self.fact_id)
    }
}

#[derive(Default)]
pub(crate) struct Maps {
    facts: Vec<Fact>,
    aliases: Aliases,
    sessions: HashMap<String, HashMap<String, usize>>,
    runs: HashMap<String, usize>,
    flows: HashMap<String, usize>,
    parents: Vec<Option<String>>,
    placements: HashMap<String, Vec<Placement>>,
}
impl Maps {
    pub(crate) fn load(conn: &Connection) -> Result<Self, String> {
        let mut groups: BTreeMap<Subject, Vec<Fact>> = BTreeMap::new();
        let mut statement = sql(conn.prepare(&format!(
            "SELECT body FROM attribution_fact f WHERE {VALID_CORRECTION}"
        )))?;
        let rows = sql(statement.query_map([], |r| r.get::<_, String>(0)))?;
        for body in rows {
            let fact: Fact = serde_json::from_str(&sql(body)?).map_err(|e| e.to_string())?;
            groups.entry(fact.subject.clone()).or_default().push(fact);
        }
        let mut result = Self {
            aliases: Aliases::load(conn)?,
            ..Default::default()
        };
        for facts in groups.into_values() {
            let superseded: HashSet<_> = facts
                .iter()
                .filter_map(|fact| fact.supersedes.clone())
                .collect();
            if let Some(winner) = facts
                .into_iter()
                .filter(|fact| !superseded.contains(&fact.fact_id))
                .max_by(|a, b| a.order().cmp(&b.order()))
            {
                result.facts.push(winner);
            }
        }
        for (id, fact) in result.facts.iter().enumerate() {
            match &fact.subject {
                Subject::Session {
                    runner: Some(runner),
                    ..
                } => {
                    let names = result
                        .sessions
                        .entry(normalized_runner(runner).into())
                        .or_default();
                    for name in fact.subject.names() {
                        let slot = names.entry(name).or_insert(id);
                        if result.facts[*slot].order() < fact.order() {
                            *slot = id;
                        }
                    }
                }
                Subject::Session { runner: None, .. } => {}
                Subject::Run { run_id, .. } => {
                    result.runs.insert(run_id.clone(), id);
                }
                Subject::Flow { flow_id } => {
                    result.flows.insert(flow_id.clone(), id);
                }
            }
        }
        result.parents = result
            .facts
            .iter()
            .map(|fact| {
                fact.parent
                    .as_ref()
                    .and_then(|parent| result.subject_index(parent))
                    .and_then(|id| result.facts[id].alfonso.clone())
            })
            .collect();
        let mut groups: BTreeMap<String, Vec<Placement>> = BTreeMap::new();
        let mut statement = sql(conn.prepare(&format!(
            "SELECT body FROM placement_fact f WHERE {VALID_PLACEMENT_CORRECTION}"
        )))?;
        let rows = sql(statement.query_map([], |r| r.get::<_, String>(0)))?;
        for body in rows {
            let placement: Placement =
                serde_json::from_str(&sql(body)?).map_err(|e| e.to_string())?;
            groups
                .entry(placement.project_id.clone())
                .or_default()
                .push(placement);
        }
        for (project, placements) in groups {
            let superseded: HashSet<_> = placements
                .iter()
                .filter_map(|placement| placement.supersedes.clone())
                .collect();
            let mut placements: Vec<_> = placements
                .into_iter()
                .filter(|placement| !superseded.contains(&placement.fact_id))
                .collect();
            placements.sort_by(|a, b| {
                (a.effective_from_ms, a.recorded_at_ms, &a.fact_id).cmp(&(
                    b.effective_from_ms,
                    b.recorded_at_ms,
                    &b.fact_id,
                ))
            });
            result.placements.insert(project, placements);
        }
        Ok(result)
    }
    fn session_index(&self, runner: &str, session: &str) -> Option<usize> {
        let names = self.sessions.get(normalized_runner(runner))?;
        self.aliases
            .names(runner, session)
            .filter_map(|name| names.get(name).copied())
            .max_by(|a, b| self.facts[*a].order().cmp(&self.facts[*b].order()))
    }
    fn subject_index(&self, subject: &Subject) -> Option<usize> {
        match subject {
            Subject::Session {
                runner: Some(runner),
                ..
            } => subject
                .names()
                .iter()
                .filter_map(|name| self.session_index(runner, name))
                .max_by(|a, b| self.facts[*a].order().cmp(&self.facts[*b].order())),
            Subject::Session { runner: None, .. } => None,
            Subject::Run { run_id, .. } => self.runs.get(run_id).copied(),
            Subject::Flow { flow_id } => self.flows.get(flow_id).copied(),
        }
    }
    pub(crate) fn resolve(
        &self,
        runner: Option<&str>,
        session: Option<&str>,
        run: Option<&str>,
        flow: Option<&str>,
        at: Option<i64>,
    ) -> Attribution {
        let id = run
            .and_then(|run| self.runs.get(run).copied())
            .or_else(|| {
                runner
                    .zip(session)
                    .and_then(|(runner, session)| self.session_index(runner, session))
            })
            .or_else(|| flow.and_then(|flow| self.flows.get(flow).copied()));
        let Some(id) = id else {
            return Attribution::default();
        };
        let fact = &self.facts[id];
        let workspace = fact
            .project_id
            .as_ref()
            .and_then(|project| self.placements.get(project))
            .zip(at)
            .and_then(|(placements, at)| {
                let through =
                    placements.partition_point(|placement| placement.effective_from_ms <= at);
                through
                    .checked_sub(1)
                    .and_then(|index| placements[index].workspace_id.clone())
            });
        Attribution {
            alfonso: fact.alfonso.clone(),
            parent_alfonso: self.parents[id].clone(),
            workload: Some(fact.workload.clone()),
            project_id: fact.project_id.clone(),
            workspace,
        }
    }
}
