//! Publisher evidence and session names, kept separate from immutable usage.
use cortexkit_role_llm_runner::{session_ref, SessionIdentity};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Attribution {
    pub alfonso: Option<String>,
    pub parent_alfonso: Option<String>,
    pub workload: Option<String>,
    pub project_id: Option<String>,
    pub workspace: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publisher {
    Prefrontal,
    Basal,
    MagicContext,
    Entorhinal,
}
impl Publisher {
    pub const ALL: [Self; 4] = [
        Self::Prefrontal,
        Self::Basal,
        Self::MagicContext,
        Self::Entorhinal,
    ];
    pub fn module(self) -> &'static str {
        match self {
            Self::Prefrontal => "prefrontal-core",
            Self::Basal => "basal",
            Self::MagicContext => "magic-context",
            Self::Entorhinal => "entorhinal",
        }
    }
    pub fn read_name(self) -> &'static str {
        if self == Self::Entorhinal {
            "placement.read"
        } else {
            "attribution.read"
        }
    }
    pub fn source(self) -> String {
        format!("attribution/{}", self.module())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Page {
    pub facts: Vec<Value>,
    pub next: Option<Value>,
    pub through: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Subject {
    Session {
        #[serde(default)]
        runner: Option<String>,
        harness: String,
        session: String,
    },
    Run {
        harness: String,
        run_id: String,
    },
    Flow {
        flow_id: String,
    },
}
type SubjectColumns<'a> = (
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);
impl Subject {
    fn columns(&self) -> SubjectColumns<'_> {
        match self {
            Self::Session {
                runner,
                harness,
                session,
            } => (
                "session",
                runner.as_deref(),
                Some(harness),
                Some(session),
                None,
                None,
            ),
            Self::Run { harness, run_id } => ("run", None, Some(harness), None, Some(run_id), None),
            Self::Flow { flow_id } => ("flow", None, None, None, None, Some(flow_id)),
        }
    }
    fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Session { runner: None, .. } => Err("subject_missing_runner"),
            Self::Session {
                runner: Some(runner),
                harness,
                session,
            } if !runner.is_empty() && !harness.is_empty() && !session.is_empty() => Ok(()),
            Self::Run { harness, run_id } if harness == "broca" && !run_id.is_empty() => Ok(()),
            Self::Flow { flow_id } if !flow_id.is_empty() => Ok(()),
            _ => Err("malformed_subject"),
        }
    }
    fn names(&self) -> Vec<String> {
        match self {
            Self::Session {
                runner: Some(runner),
                harness,
                session,
            } => vec![
                session.clone(),
                session_ref(&SessionIdentity {
                    runner,
                    harness,
                    session,
                }),
            ],
            _ => Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Fact {
    fact_id: String,
    subject: Subject,
    alfonso: Option<String>,
    parent: Option<Subject>,
    workload: String,
    task: Option<String>,
    project_id: Option<String>,
    effective_from_ms: i64,
    recorded_at_ms: i64,
    supersedes: Option<String>,
}
#[derive(Clone, Debug, Deserialize)]
struct Placement {
    fact_id: String,
    project_id: String,
    workspace_id: Option<String>,
    effective_from_ms: i64,
    recorded_at_ms: i64,
    supersedes: Option<String>,
}
impl Fact {
    fn order(&self) -> (i64, i64, &str) {
        (self.recorded_at_ms, self.effective_from_ms, &self.fact_id)
    }
}
fn named(value: &str, max: usize, dotted: bool) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        })
        && (dotted || !value.contains('.'))
}
enum Action {
    Known,
    Aside(&'static str),
    Fact(Box<Fact>),
    Placement(Box<Placement>),
}
fn action(conn: &Connection, publisher: Publisher, body: &Value) -> Result<Action, String> {
    let table = if publisher == Publisher::Entorhinal {
        "placement_fact"
    } else {
        "attribution_fact"
    };
    let id = body
        .get("fact_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let Some(id) = id else {
        return Ok(Action::Aside("malformed_fact"));
    };
    let known = sql(conn
        .query_row(
            &format!("SELECT publisher,body FROM {table} WHERE fact_id=?1"),
            [id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional())?;
    if let Some((owner, stored)) = known {
        return Ok(if owner != publisher.module() {
            Action::Aside("subject_has_other_publisher")
        } else if serde_json::from_str::<Value>(&stored).map_err(|e| e.to_string())? != *body {
            Action::Aside("fact_body_changed")
        } else {
            Action::Known
        });
    }
    if text(body)?.len() >= 4096 {
        return Ok(Action::Aside("malformed_fact"));
    }
    if publisher == Publisher::Entorhinal {
        let Ok(fact) = serde_json::from_value::<Placement>(body.clone()) else {
            return Ok(Action::Aside("malformed_fact"));
        };
        if fact.project_id.is_empty()
            || fact.workspace_id.as_deref() == Some("")
            || fact.supersedes.as_deref() == Some(id)
        {
            return Ok(Action::Aside("malformed_fact"));
        }
        if let Some(previous) = &fact.supersedes {
            let project = sql(conn
                .query_row(
                    "SELECT project_id FROM placement_fact WHERE fact_id=?1",
                    [previous],
                    |r| r.get::<_, String>(0),
                )
                .optional())?;
            if project.is_some_and(|project| project != fact.project_id) {
                return Ok(Action::Aside("malformed_fact"));
            }
        }
        return Ok(Action::Placement(Box::new(fact)));
    }
    let Ok(fact) = serde_json::from_value::<Fact>(body.clone()) else {
        return Ok(Action::Aside("malformed_fact"));
    };
    if let Err(reason) = fact.subject.validate() {
        return Ok(Action::Aside(reason));
    }
    if !named(&fact.workload, 64, false) {
        return Ok(Action::Aside("invalid_workload"));
    }
    if fact
        .task
        .as_deref()
        .is_some_and(|task| !named(task, 128, true))
    {
        return Ok(Action::Aside("invalid_task"));
    }
    if fact.alfonso.as_deref() == Some("")
        || fact.project_id.as_deref() == Some("")
        || fact.supersedes.as_deref() == Some(id)
    {
        return Ok(Action::Aside("malformed_fact"));
    }
    if fact.parent.as_ref().is_some_and(|parent| {
        parent
            .validate()
            .is_err_and(|reason| reason != "subject_missing_runner")
    }) {
        return Ok(Action::Aside("malformed_fact"));
    }
    let (kind, runner, harness, session, run, flow) = fact.subject.columns();
    let other=sql(conn.query_row("SELECT 1 FROM attribution_fact WHERE subject_kind=?1 AND subject_runner IS ?2 AND subject_harness IS ?3 AND subject_session IS ?4 AND subject_run_id IS ?5 AND subject_flow_id IS ?6 AND publisher<>?7 LIMIT 1",params![kind,runner,harness,session,run,flow,publisher.module()],|_|Ok(())).optional())?.is_some();
    Ok(if other {
        Action::Aside("subject_has_other_publisher")
    } else {
        Action::Fact(Box::new(fact))
    })
}

fn text(value: &Value) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}
fn sql<T>(result: rusqlite::Result<T>) -> Result<T, String> {
    result.map_err(|e| e.to_string())
}
fn locator(publisher: Publisher, body: &Value) -> Result<String, String> {
    Ok(format!(
        "{}/{}",
        publisher.read_name(),
        hex::encode(Sha256::digest(text(body)?.as_bytes()))
    ))
}
fn aside(
    conn: &Connection,
    publisher: Publisher,
    body: &Value,
    reason: &str,
    now: i64,
) -> Result<bool, String> {
    Ok(sql(conn.execute("INSERT OR IGNORE INTO set_aside_record(source_key,locator,reason,first_seen_ms,last_seen_ms) VALUES(?1,?2,?3,?4,?4)",params![publisher.source(),locator(publisher,body)?,reason,now]))?!=0)
}

// The reader checks this before locking the store writer. Replaying a known
// suffix must not open an empty transaction or refresh its cursor timestamps.
pub fn page_needs_commit(
    conn: &Connection,
    publisher: Publisher,
    page: &Page,
) -> Result<bool, String> {
    for body in &page.facts {
        let id = body.get("fact_id").and_then(Value::as_str);
        let table = if publisher == Publisher::Entorhinal {
            "placement_fact"
        } else {
            "attribution_fact"
        };
        let known = if let Some(id) = id {
            sql(conn
                .query_row(
                    &format!("SELECT publisher,body FROM {table} WHERE fact_id=?1"),
                    [id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional())?
        } else {
            None
        };
        if let Some((owner, stored)) = known {
            if owner == publisher.module()
                && serde_json::from_str::<Value>(&stored).map_err(|e| e.to_string())? == *body
            {
                continue;
            }
        }
        let quarantined = sql(conn
            .query_row(
                "SELECT 1 FROM set_aside_record WHERE source_key=?1 AND locator=?2",
                params![publisher.source(), locator(publisher, body)?],
                |_| Ok(()),
            )
            .optional())?
        .is_some();
        if !quarantined {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Called only after preflight finds new evidence; the caller owns the page transaction.
pub fn commit_page(
    tx: &Transaction<'_>,
    publisher: Publisher,
    request_cursor: Option<&Value>,
    page: &Page,
    now: i64,
) -> Result<bool, String> {
    let mut changed = false;
    let mut subjects = BTreeSet::new();
    let mut placements = BTreeSet::new();
    for body in &page.facts {
        match action(tx, publisher, body)? {
            Action::Known => {}
            Action::Aside(reason) => {
                changed |= aside(tx, publisher, body, reason, now)?;
            }
            Action::Placement(fact) => {
                let previous = if let Some(previous) = &fact.supersedes {
                    sql(tx
                        .query_row(
                            "SELECT effective_from_ms FROM placement_fact WHERE fact_id=?1",
                            [previous],
                            |r| r.get::<_, i64>(0),
                        )
                        .optional())?
                } else {
                    None
                };
                changed|=sql(tx.execute("INSERT INTO placement_fact SELECT ?1,?2,?3,?4,?5,?6,?7,?8 WHERE NOT EXISTS(SELECT 1 FROM placement_fact WHERE fact_id=?1)",params![fact.fact_id,publisher.module(),text(body)?,fact.project_id,fact.workspace_id,fact.effective_from_ms,fact.recorded_at_ms,fact.supersedes]))?!=0;
                // A correction can remove an older effective placement as well
                // as introduce its own; both portions of the timeline change.
                placements.insert((
                    fact.project_id,
                    previous.map_or(fact.effective_from_ms, |previous| {
                        previous.min(fact.effective_from_ms)
                    }),
                ));
            }
            Action::Fact(fact) => {
                let (kind, runner, harness, session, run, flow) = fact.subject.columns();
                let parent = fact.parent.as_ref().map(Subject::columns);
                changed|=sql(tx.execute("INSERT INTO attribution_fact SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22 WHERE NOT EXISTS(SELECT 1 FROM attribution_fact WHERE fact_id=?1)",params![fact.fact_id,publisher.module(),text(body)?,kind,runner,harness,session,run,flow,fact.alfonso,parent.map(|p|p.0),parent.and_then(|p|p.1),parent.and_then(|p|p.2),parent.and_then(|p|p.3),parent.and_then(|p|p.4),parent.and_then(|p|p.5),fact.workload,fact.task,fact.project_id,fact.effective_from_ms,fact.recorded_at_ms,fact.supersedes]))?!=0;
                if fact
                    .parent
                    .as_ref()
                    .is_some_and(|parent| matches!(parent, Subject::Session { runner: None, .. }))
                {
                    changed |= aside(tx, publisher, body, "parent_missing_runner", now)?;
                }
                subjects.insert(fact.subject);
            }
        }
    }
    if changed {
        let aliases = Aliases::load(tx)?;
        for subject in subjects {
            dirty_with_children(tx, &aliases, &subject, None)?;
        }
        for (project, from) in placements {
            let mut statement =
                sql(tx.prepare("SELECT body FROM attribution_fact WHERE project_id=?1"))?;
            let bodies = sql(statement.query_map([project], |r| r.get::<_, String>(0)))?;
            for body in bodies {
                let fact: Fact = serde_json::from_str(&sql(body)?).map_err(|e| e.to_string())?;
                dirty_subject(tx, &aliases, &fact.subject, Some(from))?;
            }
        }
        let cursor = page
            .next
            .as_ref()
            .or(request_cursor)
            .map(text)
            .transpose()?;
        sql(tx.execute("INSERT INTO attribution_cursor VALUES(?1,?2,?3,?4,?5) ON CONFLICT(publisher) DO UPDATE SET read_name=excluded.read_name,cursor=excluded.cursor,through=excluded.through,last_read_at_ms=excluded.last_read_at_ms",params![publisher.module(),publisher.read_name(),cursor,text(&page.through)?,now]))?;
    }
    Ok(changed)
}

fn normalized_runner(runner: &str) -> &str {
    if runner == "thalamus-harness" {
        "thalamus"
    } else {
        runner
    }
}

#[derive(Default)]
struct Aliases {
    by_runner: HashMap<String, HashMap<String, Arc<Vec<String>>>>,
}
impl Aliases {
    fn load(conn: &Connection) -> Result<Self, String> {
        let mut edges: HashMap<String, HashMap<String, BTreeSet<String>>> = HashMap::new();
        let mut statement =
            sql(conn.prepare("SELECT runner,host_session,bound_session FROM session_alias"))?;
        let rows = sql(statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        }))?;
        for row in rows {
            let (runner, a, b) = sql(row)?;
            let graph = edges.entry(normalized_runner(&runner).into()).or_default();
            graph.entry(a.clone()).or_default().insert(b.clone());
            graph.entry(b).or_default().insert(a);
        }
        let mut result = Self::default();
        for (runner, graph) in edges {
            let mut visited = HashSet::new();
            let index = result.by_runner.entry(runner).or_default();
            for first in graph.keys() {
                if visited.contains(first) {
                    continue;
                }
                let mut names = BTreeSet::new();
                let mut stack = vec![first.clone()];
                while let Some(name) = stack.pop() {
                    if !visited.insert(name.clone()) {
                        continue;
                    }
                    names.insert(name.clone());
                    if let Some(neighbors) = graph.get(&name) {
                        stack.extend(neighbors.iter().cloned());
                    }
                }
                let names = Arc::new(names.into_iter().collect::<Vec<_>>());
                for name in names.iter() {
                    index.insert(name.clone(), Arc::clone(&names));
                }
            }
        }
        Ok(result)
    }
    fn names<'a>(&'a self, runner: &str, name: &'a str) -> impl Iterator<Item = &'a str> {
        self.by_runner
            .get(normalized_runner(runner))
            .and_then(|index| index.get(name))
            .into_iter()
            .flat_map(|names| names.iter().map(String::as_str))
            .chain(std::iter::once(name))
    }
}

fn dirty_sql(column: &str, index: &str, has_runner: bool, has_from: bool) -> String {
    let runner = if has_runner {
        " AND runner IN (?2,?3)"
    } else {
        ""
    };
    let from = if has_from {
        if has_runner {
            " AND occurred_at_ms>=?4"
        } else {
            " AND occurred_at_ms>=?2"
        }
    } else {
        ""
    };
    format!("UPDATE attribution_hour SET revision=revision+1 WHERE (kind,hour_number) IN (SELECT CASE WHEN occurred_at_ms IS NULL THEN 1 ELSE 0 END,coalesce(occurred_at_ms/3600000-(occurred_at_ms<0 AND occurred_at_ms%3600000<>0),0) FROM usage_event INDEXED BY {index} WHERE {column}=?1{runner}{from})")
}
fn dirty_subject(
    conn: &Connection,
    aliases: &Aliases,
    subject: &Subject,
    from: Option<i64>,
) -> Result<(), String> {
    let (column, index, runner, names) = match subject {
        Subject::Session {
            runner: Some(runner),
            ..
        } => {
            let mut names = BTreeSet::new();
            for name in subject.names() {
                names.extend(aliases.names(runner, &name).map(str::to_owned));
            }
            (
                "session",
                "usage_event_session_time",
                Some(runner.as_str()),
                names,
            )
        }
        Subject::Session { runner: None, .. } => return Ok(()),
        Subject::Run { run_id, .. } => (
            "run_ref",
            "usage_event_run_time",
            None,
            BTreeSet::from([run_id.clone()]),
        ),
        Subject::Flow { flow_id } => (
            "flow_ref",
            "usage_event_flow_time",
            None,
            BTreeSet::from([flow_id.clone()]),
        ),
    };
    let query = dirty_sql(column, index, runner.is_some(), from.is_some());
    for name in names {
        let mut parameters = vec![rusqlite::types::Value::Text(name)];
        if let Some(runner) = runner {
            let normalized = normalized_runner(runner);
            let other = if normalized == "thalamus" {
                "thalamus-harness"
            } else {
                normalized
            };
            parameters.extend([normalized.into(), other.into()].map(rusqlite::types::Value::Text));
        }
        if let Some(from) = from {
            parameters.push(rusqlite::types::Value::Integer(from));
        }
        sql(conn.execute(&query, rusqlite::params_from_iter(parameters)))?;
    }
    Ok(())
}

// Only the direct parent is read: its owning agent determines the child's
// parent_alfonso. Changes to grandparent ownership do not change that field.
fn dirty_with_children(
    conn: &Connection,
    aliases: &Aliases,
    subject: &Subject,
    from: Option<i64>,
) -> Result<(), String> {
    dirty_subject(conn, aliases, subject, from)?;
    let mut children = BTreeSet::new();
    let (query,parameters)=match subject {
        Subject::Session{runner:Some(runner),..}=>{
            let mut names=BTreeSet::new();for name in subject.names() {names.extend(aliases.names(runner,&name).map(str::to_owned));}
            let normalized=normalized_runner(runner);let other=if normalized=="thalamus" {"thalamus-harness"} else {normalized};
            let query="SELECT body FROM attribution_fact INDEXED BY attribution_parent_session WHERE parent_kind='session' AND parent_runner IN (?1,?2) AND parent_session=?3";
            let mut statement=sql(conn.prepare(query))?;
            for name in names {let rows=sql(statement.query_map(params![normalized,other,name],|r|r.get::<_,String>(0)))?;for row in rows {let fact:Fact=serde_json::from_str(&sql(row)?).map_err(|e|e.to_string())?;children.insert(fact.subject);}}
            for child in children {dirty_subject(conn,aliases,&child,from)?;}
            return Ok(());
        },
        Subject::Session{runner:None,..}=>return Ok(()),
        Subject::Run{run_id,..}=>("SELECT body FROM attribution_fact INDEXED BY attribution_parent_run WHERE parent_kind='run' AND parent_run_id=?1",run_id),
        Subject::Flow{flow_id}=>("SELECT body FROM attribution_fact INDEXED BY attribution_parent_flow WHERE parent_kind='flow' AND parent_flow_id=?1",flow_id),
    };
    let mut statement = sql(conn.prepare(query))?;
    let rows = sql(statement.query_map([parameters], |r| r.get::<_, String>(0)))?;
    for row in rows {
        let fact: Fact = serde_json::from_str(&sql(row)?).map_err(|e| e.to_string())?;
        children.insert(fact.subject);
    }
    for child in children {
        dirty_subject(conn, aliases, &child, from)?;
    }
    Ok(())
}

/// Append every known spelling around the ledger's recorded session name.
pub fn append_aliases(
    tx: &Transaction<'_>,
    runner: &str,
    harness: &str,
    recorded: &str,
    names: &[&str],
) -> Result<bool, String> {
    let mut changed = false;
    for name in names {
        if name.is_empty() || *name == recorded {
            continue;
        }
        changed|=sql(tx.execute("INSERT INTO session_alias SELECT ?1,?2,?3,?4 WHERE NOT EXISTS(SELECT 1 FROM session_alias WHERE runner=?1 AND harness=?2 AND host_session=?3 AND bound_session=?4)",params![runner,harness,recorded,name]))?!=0;
    }
    if changed {
        let aliases = Aliases::load(tx)?;
        dirty_with_children(
            tx,
            &aliases,
            &Subject::Session {
                runner: Some(runner.into()),
                harness: harness.into(),
                session: recorded.into(),
            },
            None,
        )?;
    }
    Ok(changed)
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
        let mut statement = sql(conn.prepare("SELECT body FROM attribution_fact"))?;
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
        let mut statement = sql(conn.prepare("SELECT body FROM placement_fact"))?;
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

#[cfg(test)]
mod tests;
