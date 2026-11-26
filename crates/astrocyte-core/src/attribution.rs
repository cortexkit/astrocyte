//! Publisher evidence and session names, kept separate from immutable usage.
use cortexkit_role_llm_runner::{session_ref, SessionIdentity};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};
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
    /// The publisher's identity in this store: the `publisher` column of
    /// stored facts, the cursor row and set-aside sources. Prefrontal keeps
    /// the module id it published under first (`prefrontal-core`) after its
    /// rename to `prefrontal`, so its cursor carries on and a correction made
    /// after the rename still matches the fact it replaces.
    pub fn key(self) -> &'static str {
        match self {
            Self::Prefrontal => "prefrontal-core",
            Self::Basal => "basal",
            Self::MagicContext => "magic-context",
            Self::Entorhinal => "entorhinal",
        }
    }
    /// The module ids this publisher may run under, preferred first. Routes
    /// and bus subjects try each, so one build reads prefrontal before and
    /// after its rename.
    pub fn modules(self) -> &'static [&'static str] {
        match self {
            Self::Prefrontal => &["prefrontal", "prefrontal-core"],
            Self::Basal => &["basal"],
            Self::MagicContext => &["magic-context"],
            Self::Entorhinal => &["entorhinal"],
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
        format!("attribution/{}", self.key())
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
const VALID_PLACEMENT_CORRECTION:&str="NOT EXISTS(SELECT 1 FROM placement_fact p WHERE p.fact_id=f.supersedes AND (p.publisher<>f.publisher OR p.project_id<>f.project_id))";
const VALID_CORRECTION:&str="NOT EXISTS(SELECT 1 FROM attribution_fact p WHERE p.fact_id=f.supersedes AND (p.publisher<>f.publisher OR p.subject_kind<>f.subject_kind OR p.subject_runner IS NOT f.subject_runner OR p.subject_harness IS NOT f.subject_harness OR p.subject_session IS NOT f.subject_session OR p.subject_run_id IS NOT f.subject_run_id OR p.subject_flow_id IS NOT f.subject_flow_id))";
fn correction_scope(conn: &Connection, publisher: Publisher, fact: &Fact) -> Result<bool, String> {
    let Some(previous) = &fact.supersedes else {
        return Ok(true);
    };
    let (kind, runner, harness, session, run, flow) = fact.subject.columns();
    let invalid=sql(conn.query_row("SELECT EXISTS(SELECT 1 FROM attribution_fact WHERE fact_id=?1 AND (publisher<>?2 OR subject_kind<>?3 OR subject_runner IS NOT ?4 OR subject_harness IS NOT ?5 OR subject_session IS NOT ?6 OR subject_run_id IS NOT ?7 OR subject_flow_id IS NOT ?8))",params![previous,publisher.key(),kind,runner,harness,session,run,flow],|row|row.get::<_,bool>(0)))?;
    Ok(!invalid)
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
        return Ok(if owner != publisher.key() {
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
            let previous = sql(conn
                .query_row(
                    "SELECT publisher,project_id FROM placement_fact WHERE fact_id=?1",
                    [previous],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional())?;
            if previous.is_some_and(|(owner, project)| {
                owner != publisher.key() || project != fact.project_id
            }) {
                return Ok(Action::Aside("supersedes_other_subject"));
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
    if !correction_scope(conn, publisher, &fact)? {
        return Ok(Action::Aside("supersedes_other_subject"));
    }
    let (kind, runner, harness, session, run, flow) = fact.subject.columns();
    let other=sql(conn.query_row(&format!("SELECT 1 FROM attribution_fact f WHERE subject_kind=?1 AND subject_runner IS ?2 AND subject_harness IS ?3 AND subject_session IS ?4 AND subject_run_id IS ?5 AND subject_flow_id IS ?6 AND publisher<>?7 AND {VALID_CORRECTION} LIMIT 1"),params![kind,runner,harness,session,run,flow,publisher.key()],|_|Ok(())).optional())?.is_some();
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
            if owner == publisher.key()
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
    let mut arrived = Vec::new();
    let mut placement_arrived = Vec::new();
    for body in &page.facts {
        match action(tx, publisher, body)? {
            Action::Known => {}
            Action::Aside(reason) => {
                changed |= aside(tx, publisher, body, reason, now)?;
            }
            Action::Placement(fact) => {
                placement_arrived.push(fact.fact_id.clone());
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
                changed|=sql(tx.execute("INSERT INTO placement_fact SELECT ?1,?2,?3,?4,?5,?6,?7,?8 WHERE NOT EXISTS(SELECT 1 FROM placement_fact WHERE fact_id=?1)",params![fact.fact_id,publisher.key(),text(body)?,fact.project_id,fact.workspace_id,fact.effective_from_ms,fact.recorded_at_ms,fact.supersedes]))?!=0;
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
                arrived.push(fact.fact_id.clone());
                let (kind, runner, harness, session, run, flow) = fact.subject.columns();
                let parent = fact.parent.as_ref().map(Subject::columns);
                let inserted=sql(tx.execute("INSERT INTO attribution_fact SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22 WHERE NOT EXISTS(SELECT 1 FROM attribution_fact WHERE fact_id=?1)",params![fact.fact_id,publisher.key(),text(body)?,kind,runner,harness,session,run,flow,fact.alfonso,parent.map(|p|p.0),parent.and_then(|p|p.1),parent.and_then(|p|p.2),parent.and_then(|p|p.3),parent.and_then(|p|p.4),parent.and_then(|p|p.5),fact.workload,fact.task,fact.project_id,fact.effective_from_ms,fact.recorded_at_ms,fact.supersedes]))?!=0;
                if inserted {
                    insert_session_ref(tx, &fact.fact_id, runner, harness, session)?;
                    if let Some(("session", Some(runner), Some(harness), Some(session), _, _)) =
                        parent
                    {
                        let reference = resolve::subject_ref(runner, harness, session);
                        sql(
                            sql(tx.prepare_cached(INSERT_PARENT_SESSION_REF))?.execute(params![
                                runner,
                                reference,
                                fact.fact_id
                            ]),
                        )?;
                    }
                }
                changed |= inserted;
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
    for id in arrived {
        let mut statement=sql(tx.prepare(&format!("SELECT publisher,body FROM attribution_fact f WHERE supersedes=?1 AND NOT ({VALID_CORRECTION})")))?;
        let claims = sql(statement.query_map([id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }))?;
        for claim in claims {
            let (module, body) = sql(claim)?;
            let publisher = Publisher::ALL
                .into_iter()
                .find(|publisher| publisher.key() == module)
                .ok_or("unknown attribution publisher")?;
            let body: Value = serde_json::from_str(&body).map_err(|error| error.to_string())?;
            let fact: Fact =
                serde_json::from_value(body.clone()).map_err(|error| error.to_string())?;
            // Evidence already kept for an unknown predecessor is immutable.
            // Count a later-disproved claim and rebuild the hours it used to own.
            changed |= aside(tx, publisher, &body, "supersedes_other_subject", now)?;
            subjects.insert(fact.subject);
        }
    }
    for id in placement_arrived {
        let mut statement=sql(tx.prepare(&format!("SELECT body FROM placement_fact f WHERE supersedes=?1 AND NOT ({VALID_PLACEMENT_CORRECTION})")))?;
        let claims = sql(statement.query_map([id], |row| row.get::<_, String>(0)))?;
        for body in claims {
            let body: Value =
                serde_json::from_str(&sql(body)?).map_err(|error| error.to_string())?;
            let fact: Placement =
                serde_json::from_value(body.clone()).map_err(|error| error.to_string())?;
            changed |= aside(
                tx,
                Publisher::Entorhinal,
                &body,
                "supersedes_other_subject",
                now,
            )?;
            placements.insert((fact.project_id, fact.effective_from_ms));
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
        sql(tx.execute("INSERT INTO attribution_cursor VALUES(?1,?2,?3,?4,?5) ON CONFLICT(publisher) DO UPDATE SET read_name=excluded.read_name,cursor=excluded.cursor,through=excluded.through,last_read_at_ms=excluded.last_read_at_ms",params![publisher.key(),publisher.read_name(),cursor,text(&page.through)?,now]))?;
    }
    Ok(changed)
}

const INSERT_SESSION_REF: &str =
    "INSERT INTO attribution_session_ref(runner,session_ref,fact_id) VALUES(?1,?2,?3)";
const INSERT_PARENT_SESSION_REF: &str =
    "INSERT INTO attribution_parent_session_ref(parent_runner,session_ref,fact_id) VALUES(?1,?2,?3)";

/// Files a session fact under its computed `session_ref`. The resolver finds a
/// fact by that name through this table, because SQLite can't compute the
/// reference itself and so can't index it on `attribution_fact`.
fn insert_session_ref(
    conn: &Connection,
    fact_id: &str,
    runner: Option<&str>,
    harness: Option<&str>,
    session: Option<&str>,
) -> Result<(), String> {
    if let (Some(runner), Some(harness), Some(session)) = (runner, harness, session) {
        let reference = resolve::subject_ref(runner, harness, session);
        sql(conn.execute(INSERT_SESSION_REF, params![runner, reference, fact_id]))?;
    }
    Ok(())
}

/// Fills `attribution_session_ref` for every session fact stored before the
/// table existed. The store runs this once, in the same transaction as the
/// migration that creates the table.
pub(crate) fn backfill_session_refs(conn: &Connection) -> Result<usize, String> {
    backfill_refs(conn,
        "SELECT fact_id,subject_runner,subject_harness,subject_session FROM attribution_fact WHERE subject_kind='session'",
        INSERT_SESSION_REF,
    )
}

/// Stream existing session parents through a single prepared insert. Only one
/// fact's reference is kept in memory at a time during backfill.
pub(crate) fn backfill_parent_session_refs(conn: &Connection) -> Result<usize, String> {
    let filed = backfill_refs(conn,
        "SELECT fact_id,parent_runner,parent_harness,parent_session FROM attribution_fact WHERE parent_kind='session'",
        INSERT_PARENT_SESSION_REF,
    )?;
    if filed > 0 {
        dirty_children_of_existing_computed_aliases(conn)?;
    }
    Ok(filed)
}

fn backfill_refs(conn: &Connection, select: &str, insert: &str) -> Result<usize, String> {
    let mut statement = sql(conn.prepare(select))?;
    let mut insert = sql(conn.prepare(insert))?;
    let mut rows = sql(statement.query([]))?;
    let mut filed = 0;
    while let Some(row) = sql(rows.next())? {
        let fact_id: String = sql(row.get(0))?;
        let runner: Option<String> = sql(row.get(1))?;
        let harness: Option<String> = sql(row.get(2))?;
        let session: Option<String> = sql(row.get(3))?;
        if let (Some(runner), Some(harness), Some(session)) = (runner, harness, session) {
            let reference = resolve::subject_ref(&runner, &harness, &session);
            sql(insert.execute(params![runner, reference, fact_id]))?;
            filed += 1;
        }
    }
    Ok(filed)
}

fn normalized_runner(runner: &str) -> &str {
    if runner == "thalamus-harness" {
        "thalamus"
    } else {
        runner
    }
}

fn runner_spellings(runner: &str) -> (&str, &str) {
    let normalized = normalized_runner(runner);
    let other = if normalized == "thalamus" {
        "thalamus-harness"
    } else {
        normalized
    };
    (normalized, other)
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
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Context {
    Session {
        runner: String,
        names: BTreeSet<String>,
    },
    Run(String),
    Flow(String),
}
impl Context {
    fn subject(aliases: &Aliases, subject: &Subject) -> Option<Self> {
        Some(match subject {
            Subject::Session {
                runner: Some(runner),
                ..
            } => {
                let mut names = BTreeSet::new();
                for name in subject.names() {
                    names.extend(aliases.names(runner, &name).map(str::to_owned));
                }
                Self::Session {
                    runner: normalized_runner(runner).into(),
                    names,
                }
            }
            Subject::Session { runner: None, .. } => return None,
            Subject::Run { run_id, .. } => Self::Run(run_id.clone()),
            Subject::Flow { flow_id } => Self::Flow(flow_id.clone()),
        })
    }
}
fn dirty_subject(
    conn: &Connection,
    aliases: &Aliases,
    subject: &Subject,
    from: Option<i64>,
) -> Result<(), String> {
    if let Some(context) = Context::subject(aliases, subject) {
        dirty_context(conn, &context, from)?;
    }
    Ok(())
}
fn dirty_context(conn: &Connection, context: &Context, from: Option<i64>) -> Result<(), String> {
    dirty_ledger_context(conn, context, from)?;
    // A stamped run or flow can affect attribution even when the ledger's
    // run_ref or flow_ref points somewhere else. Skip the calls already found
    // through those ledger refs.
    let stamped = match context {
        Context::Run(id) => Some((id, "agent_run_id", "broca_scope_stamp_run", "run_ref")),
        Context::Flow(id) => Some((id, "flow_id", "broca_scope_stamp_flow", "flow_ref")),
        Context::Session { .. } => None,
    };
    if let Some((id, field, index, ledger_field)) = stamped {
        sql(conn.execute(
            &dirty_stamp_sql(field, index, ledger_field),
            params![id, from],
        ))?;
    }
    Ok(())
}

fn dirty_ledger_context(
    conn: &Connection,
    context: &Context,
    from: Option<i64>,
) -> Result<(), String> {
    let (column, index, runner, names) = match context {
        Context::Session { runner, names } => (
            "session",
            "usage_event_session_time",
            Some(runner.as_str()),
            names.clone(),
        ),
        Context::Run(run_id) => (
            "run_ref",
            "usage_event_run_time",
            None,
            BTreeSet::from([run_id.clone()]),
        ),
        Context::Flow(flow_id) => (
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
            let (normalized, other) = runner_spellings(runner);
            parameters.extend([normalized.into(), other.into()].map(rusqlite::types::Value::Text));
        }
        if let Some(from) = from {
            parameters.push(rusqlite::types::Value::Integer(from));
        }
        sql(conn.execute(&query, rusqlite::params_from_iter(parameters)))?;
    }
    Ok(())
}

fn dirty_stamp_sql(field: &str, index: &str, ledger_field: &str) -> String {
    format!("UPDATE attribution_hour SET revision=revision+1 WHERE (kind,hour_number) IN (
        SELECT CASE WHEN e.occurred_at_ms IS NULL THEN 1 ELSE 0 END,
               coalesce(e.occurred_at_ms/3600000-(e.occurred_at_ms<0 AND e.occurred_at_ms%3600000<>0),0)
        FROM broca_scope_stamp s INDEXED BY {index} CROSS JOIN usage_event e ON e.event_key=s.event_key
        WHERE s.{field}=?1 AND e.{ledger_field} IS NOT ?1 AND (?2 IS NULL OR e.occurred_at_ms>=?2)
    )")
}

const COMPUTED_PARENT_CHILDREN: &str = "SELECT f.body FROM attribution_parent_session_ref p
    CROSS JOIN attribution_fact f ON f.fact_id=p.fact_id
    WHERE p.parent_runner IN (?1,?2) AND p.session_ref=?3";

fn find_session_children(
    statement: &mut rusqlite::Statement<'_>,
    runner: &str,
    name: &str,
    children: &mut BTreeSet<Subject>,
) -> Result<(), String> {
    let (normalized, other) = runner_spellings(runner);
    let rows =
        sql(statement.query_map(params![normalized, other, name], |r| r.get::<_, String>(0)))?;
    for row in rows {
        let fact: Fact = serde_json::from_str(&sql(row)?).map_err(|e| e.to_string())?;
        children.insert(fact.subject);
    }
    Ok(())
}

fn dirty_children_of_existing_computed_aliases(conn: &Connection) -> Result<(), String> {
    let aliases = Aliases::load(conn)?;
    let mut statement = sql(conn.prepare(COMPUTED_PARENT_CHILDREN))?;
    let mut children = BTreeSet::new();
    // Visit each stored alias spelling once per runner. Unlike new alias
    // commits, only the newly indexed computed references need to be checked.
    for (runner, names) in &aliases.by_runner {
        for name in names.keys() {
            find_session_children(&mut statement, runner, name, &mut children)?;
        }
    }
    for child in children {
        // The migration that fills computed parent references predates the
        // stamp table, so it can invalidate only ledger contexts.
        if let Some(context) = Context::subject(&aliases, &child) {
            dirty_ledger_context(conn, &context, None)?;
        }
    }
    Ok(())
}

// An "Alfonso" is the agent a fact names as owning the work (`alfonso`), and a
// child's `parent_alfonso` is the agent that owns its direct parent. Only the
// direct parent is read, so a change to a grandparent's owner leaves the
// child's `parent_alfonso` as it was and dirties nothing below the parent.
fn dirty_with_children(
    conn: &Connection,
    aliases: &Aliases,
    subject: &Subject,
    from: Option<i64>,
) -> Result<(), String> {
    if let Some(context) = Context::subject(aliases, subject) {
        dirty_context_with_children(conn, aliases, &context, from)?;
    }
    Ok(())
}
fn dirty_context_with_children(
    conn: &Connection,
    aliases: &Aliases,
    context: &Context,
    from: Option<i64>,
) -> Result<(), String> {
    dirty_context(conn, context, from)?;
    let mut children = BTreeSet::new();
    let (query,parameters)=match context {
        Context::Session { runner, names } => {
            let query = format!("SELECT body FROM attribution_fact INDEXED BY attribution_parent_session
                WHERE parent_kind='session' AND parent_runner IN (?1,?2) AND parent_session=?3
                UNION ALL {COMPUTED_PARENT_CHILDREN}");
            let mut statement = sql(conn.prepare_cached(&query))?;
            for name in names {
                find_session_children(&mut statement, runner, name, &mut children)?;
            }
            for child in children {
                dirty_subject(conn, aliases, &child, from)?;
            }
            return Ok(());
        },
        Context::Run(run_id)=>("SELECT body FROM attribution_fact INDEXED BY attribution_parent_run WHERE parent_kind='run' AND parent_run_id=?1",run_id),
        Context::Flow(flow_id)=>("SELECT body FROM attribution_fact INDEXED BY attribution_parent_flow WHERE parent_kind='flow' AND parent_flow_id=?1",flow_id),
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
    append_alias_batch(
        tx,
        &[SessionAliases {
            runner: runner.into(),
            harness: harness.into(),
            recorded: recorded.into(),
            names: names.iter().map(|name| (*name).into()).collect(),
        }],
    )
}

pub struct SessionAliases {
    pub runner: String,
    pub harness: String,
    pub recorded: String,
    pub names: Vec<String>,
}
pub fn aliases_need_commit(conn: &Connection, entries: &[SessionAliases]) -> Result<bool, String> {
    for entry in entries {
        if entry.runner.is_empty() || entry.harness.is_empty() || entry.recorded.is_empty() {
            continue;
        }
        for name in &entry.names {
            if name.is_empty() || *name == entry.recorded {
                continue;
            }
            if sql(conn.query_row("SELECT 1 FROM session_alias WHERE runner=?1 AND harness=?2 AND host_session=?3 AND bound_session=?4",params![entry.runner,entry.harness,entry.recorded,name],|_|Ok(())).optional())?.is_none() {return Ok(true);}
        }
    }
    Ok(false)
}
pub fn append_alias_batch(
    tx: &Transaction<'_>,
    entries: &[SessionAliases],
) -> Result<bool, String> {
    let mut changed = false;
    let mut affected = BTreeSet::new();
    for entry in entries {
        if entry.runner.is_empty() || entry.harness.is_empty() || entry.recorded.is_empty() {
            continue;
        }
        for name in &entry.names {
            if name.is_empty() || *name == entry.recorded {
                continue;
            }
            let inserted=sql(tx.execute("INSERT INTO session_alias SELECT ?1,?2,?3,?4 WHERE NOT EXISTS(SELECT 1 FROM session_alias WHERE runner=?1 AND harness=?2 AND host_session=?3 AND bound_session=?4)",params![entry.runner,entry.harness,entry.recorded,name]))?!=0;
            if inserted {
                changed = true;
                affected.insert((entry.runner.clone(), entry.recorded.clone()));
            }
        }
    }
    if changed {
        let aliases = Aliases::load(tx)?;
        let mut contexts = BTreeSet::new();
        for (runner, recorded) in affected {
            // THALAMUS session-root items supply complete alias names. Their host label is not a
            // binding identity, so it must never be hashed into another name.
            let names = aliases
                .names(&runner, &recorded)
                .map(str::to_owned)
                .collect();
            contexts.insert(Context::Session {
                runner: normalized_runner(&runner).into(),
                names,
            });
        }
        for context in contexts {
            dirty_context_with_children(tx, &aliases, &context, None)?;
        }
    }
    Ok(changed)
}

mod resolve;
pub(crate) use resolve::Resolver;

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;
