use super::*;
use crate::attribution::oracle::Maps;
use crate::attribution::{append_aliases, commit_page, insert_session_ref, Fact, Page, Publisher};
use serde_json::{json, Value};

fn conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    crate::store::initialize_connection(&conn).unwrap();
    for migration in crate::store::MIGRATIONS {
        conn.execute_batch(migration.statements).unwrap();
    }
    conn
}

/// A small deterministic generator, so a failing case names a seed that
/// reproduces it.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(bound).unwrap()).unwrap()
    }
    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
    fn small(&mut self, bound: usize) -> i64 {
        i64::try_from(self.below(bound)).unwrap()
    }
}

const RUNNERS: [&str; 3] = ["thalamus", "thalamus-harness", "pi"];
const HARNESSES: [&str; 2] = ["opencode", "opencode2"];
const SESSIONS: [&str; 5] = ["s0", "s1", "s2", "s3", "s4"];
const RUNS: [&str; 3] = ["r0", "r1", "r2"];
const FLOWS: [&str; 3] = ["f0", "f1", "f2"];
const PROJECTS: [&str; 3] = ["p0", "p1", "p2"];

fn session(runner: &str, harness: &str, name: &str) -> Value {
    json!({"kind":"session","runner":runner,"harness":harness,"session":name})
}
fn subject(rng: &mut Rng, direct: bool) -> Value {
    match rng.below(6) {
        0..=2 => session(
            rng.pick(&RUNNERS),
            rng.pick(&HARNESSES),
            rng.pick(&SESSIONS),
        ),
        3 | 4 => {
            // Ingest accepts only BROCA runs; stored rows from elsewhere may
            // still name another harness, and the old maps then preferred the
            // greatest harness.
            let harness = if direct && rng.chance(15) {
                "zeta"
            } else {
                "broca"
            };
            json!({"kind":"run","harness":harness,"run_id":rng.pick(&RUNS)})
        }
        _ => json!({"kind":"flow","flow_id":rng.pick(&FLOWS)}),
    }
}
fn parent(rng: &mut Rng, direct: bool) -> Value {
    if rng.chance(40) {
        return Value::Null;
    }
    let mut parent = subject(rng, direct);
    if parent["kind"] == "session" && rng.chance(15) {
        parent.as_object_mut().unwrap().remove("runner");
    }
    parent
}

struct Universe {
    facts: Vec<(String, Value)>,
    placements: Vec<(String, Value)>,
    aliases: Vec<(String, String, String, String)>,
    names: Vec<String>,
}

fn universe(rng: &mut Rng, direct: bool) -> Universe {
    // Aliases connect raw session names, extra spellings and computed
    // references, so alias components span several hops and both kinds of
    // name a fact answers to.
    let mut names: Vec<String> = SESSIONS.iter().map(|s| (*s).to_owned()).collect();
    names.extend(["x0".to_owned(), "x1".to_owned()]);
    for _ in 0..6 {
        names.push(subject_ref(
            rng.pick(&RUNNERS),
            rng.pick(&HARNESSES),
            rng.pick(&SESSIONS),
        ));
    }
    let count = 30 + rng.below(50);
    let mut facts = Vec::new();
    for index in 0..count {
        let supersedes = match rng.below(20) {
            0..=8 => Value::Null,
            9..=17 => json!(format!("a{}", rng.below(count))),
            _ => json!(format!("missing{}", rng.below(3))),
        };
        let alfonso = if rng.chance(80) {
            json!(format!("agent{}", rng.below(4)))
        } else {
            Value::Null
        };
        let project = if rng.chance(75) {
            json!(rng.pick(&PROJECTS))
        } else {
            Value::Null
        };
        let body = json!({
            "fact_id": format!("a{index}"),
            "subject": subject(rng, direct),
            "alfonso": alfonso,
            "parent": parent(rng, direct),
            "workload": format!("w{}", rng.below(3)),
            "task": null,
            "project_id": project,
            "effective_from_ms": rng.small(4),
            "recorded_at_ms": rng.small(4),
            "supersedes": supersedes,
        });
        let publisher = if rng.chance(85) {
            "prefrontal"
        } else {
            "basal"
        };
        facts.push((publisher.to_owned(), body));
    }
    let count = rng.below(14);
    let mut placements = Vec::new();
    for index in 0..count {
        let supersedes = if rng.chance(35) {
            json!(format!("pl{}", rng.below(count)))
        } else {
            Value::Null
        };
        let workspace = if rng.chance(80) {
            json!(format!("ws{}", rng.below(4)))
        } else {
            Value::Null
        };
        let body = json!({
            "fact_id": format!("pl{index}"),
            "project_id": rng.pick(&PROJECTS),
            "workspace_id": workspace,
            "effective_from_ms": rng.small(7) * 10,
            "recorded_at_ms": rng.small(4),
            "supersedes": supersedes,
        });
        let publisher = if direct && rng.chance(10) {
            "other"
        } else {
            "entorhinal"
        };
        placements.push((publisher.to_owned(), body));
    }
    let mut aliases = Vec::new();
    for _ in 0..rng.below(14) {
        let host = rng.pick(&names).clone();
        let bound = rng.pick(&names).clone();
        if host != bound {
            aliases.push((
                (*rng.pick(&RUNNERS)).to_owned(),
                (*rng.pick(&HARNESSES)).to_owned(),
                host,
                bound,
            ));
        }
    }
    names.push("nobody".into());
    Universe {
        facts,
        placements,
        aliases,
        names,
    }
}

/// Inserts one fact row and its session reference with the same columns
/// ingest writes, skipping ingest's checks.
fn store_fact(conn: &Connection, publisher: &str, body: &Value) {
    let fact: Fact = serde_json::from_value(body.clone()).unwrap();
    let (kind, runner, harness, session, run, flow) = fact.subject.columns();
    let parent = fact.parent.as_ref().map(Subject::columns);
    conn.execute("INSERT INTO attribution_fact VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",params![fact.fact_id,publisher,body.to_string(),kind,runner,harness,session,run,flow,fact.alfonso,parent.map(|p|p.0),parent.and_then(|p|p.1),parent.and_then(|p|p.2),parent.and_then(|p|p.3),parent.and_then(|p|p.4),parent.and_then(|p|p.5),fact.workload,fact.task,fact.project_id,fact.effective_from_ms,fact.recorded_at_ms,fact.supersedes]).unwrap();
    insert_session_ref(conn, &fact.fact_id, runner, harness, session).unwrap();
}

/// Writes rows exactly as ingest would, but without ingest's checks, so the
/// store also holds states ingest only reaches over time: corrections whose
/// target arrives later under another subject or publisher, cycles, and
/// corrections of facts that never arrive.
fn store_directly(conn: &Connection, universe: &Universe) {
    for (publisher, body) in &universe.facts {
        store_fact(conn, publisher, body);
    }
    for (publisher, body) in &universe.placements {
        conn.execute(
            "INSERT INTO placement_fact VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                body["fact_id"].as_str(),
                publisher,
                body.to_string(),
                body["project_id"].as_str(),
                body["workspace_id"].as_str(),
                body["effective_from_ms"].as_i64(),
                body["recorded_at_ms"].as_i64(),
                body["supersedes"].as_str()
            ],
        )
        .unwrap();
    }
    for (runner, harness, host, bound) in &universe.aliases {
        conn.execute("INSERT INTO session_alias SELECT ?1,?2,?3,?4 WHERE NOT EXISTS(SELECT 1 FROM session_alias WHERE runner=?1 AND harness=?2 AND host_session=?3 AND bound_session=?4)",params![runner,harness,host,bound]).unwrap();
    }
}

/// Feeds the same evidence through the real ingest path in random pages.
fn ingest(conn: &mut Connection, rng: &mut Rng, universe: &Universe) {
    let mut facts: Vec<_> = universe.facts.iter().collect();
    while !facts.is_empty() {
        let take = (1 + rng.below(8)).min(facts.len());
        let batch: Vec<_> = facts.drain(..take).collect();
        for publisher in [Publisher::Prefrontal, Publisher::Basal] {
            let page = Page {
                facts: batch
                    .iter()
                    .filter(|(name, _)| name == publisher.module())
                    .map(|(_, body)| body.clone())
                    .collect(),
                next: None,
                through: json!(1),
            };
            let tx = conn.transaction().unwrap();
            commit_page(&tx, publisher, None, &page, 10).unwrap();
            tx.commit().unwrap();
        }
    }
    let page = Page {
        facts: universe
            .placements
            .iter()
            .map(|(_, body)| body.clone())
            .collect(),
        next: None,
        through: json!(1),
    };
    let tx = conn.transaction().unwrap();
    commit_page(&tx, Publisher::Entorhinal, None, &page, 10).unwrap();
    for (runner, harness, host, bound) in &universe.aliases {
        append_aliases(&tx, runner, harness, host, &[bound.as_str()]).unwrap();
    }
    tx.commit().unwrap();
}

fn compare(seed: u64, direct: bool) -> usize {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let universe = universe(&mut rng, direct);
    let mut conn = conn();
    if direct {
        store_directly(&conn, &universe);
    } else {
        ingest(&mut conn, &mut rng, &universe);
    }
    let oracle = Maps::load(&conn).unwrap();
    // One resolver answers every query of the case, as one build does, so a
    // memoized answer that differs from a fresh one also shows up.
    let mut resolver = Resolver::default();
    let mut attributed = 0;
    for query in 0..300 {
        let runner = rng
            .chance(90)
            .then(|| *rng.pick(&["thalamus", "thalamus-harness", "pi", "other"]));
        let session = rng.chance(85).then(|| rng.pick(&universe.names).clone());
        let run = rng.chance(30).then(|| *rng.pick(&["r0", "r1", "r2", "r9"]));
        let flow = rng.chance(40).then(|| *rng.pick(&["f0", "f1", "f2", "f9"]));
        let at = rng.chance(80).then(|| rng.small(70));
        let expected = oracle.resolve(runner, session.as_deref(), run, flow, at);
        let actual = resolver
            .resolve(&conn, runner, session.as_deref(), run, flow, at)
            .unwrap();
        assert_eq!(
            actual, expected,
            "seed {seed} direct {direct} query {query}: {runner:?} {session:?} {run:?} {flow:?} {at:?}"
        );
        attributed += usize::from(expected != Attribution::default());
    }
    attributed
}

#[test]
fn resolver_matches_whole_history_maps_on_directly_stored_evidence() {
    let attributed: usize = (1..=80).map(|seed| compare(seed, true)).sum();
    // Guard against a generator that only ever asks about absent subjects.
    assert!(attributed > 80 * 300 / 4, "{attributed}");
}

#[test]
fn resolver_matches_whole_history_maps_on_ingested_evidence() {
    let attributed: usize = (1..=40).map(|seed| compare(seed, false)).sum();
    assert!(attributed > 40 * 300 / 4, "{attributed}");
}

fn plan(conn: &Connection, sql: &str, parameters: usize) -> String {
    let values = vec![rusqlite::types::Value::Text("x".into()); parameters];
    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map(rusqlite::params_from_iter(values), |r| {
            r.get::<_, String>(3)
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n")
}

/// Every lookup runs on each summary and valuation build, so each must seek
/// its index; a scan of the evidence tables would bring back the cost of
/// reading all history.
#[test]
fn resolver_lookups_seek_their_indexes_and_scan_no_evidence_table() {
    let conn = conn();
    let correction = "SEARCH p USING PRIMARY KEY (fact_id=?)";
    for (sql, parameters, expected) in [
        (
            group_sql(),
            3,
            vec![
                "SEARCH f USING INDEX attribution_fact_session (subject_runner=? AND subject_session=?)",
                correction,
            ],
        ),
        (
            NAME_SQL.to_owned(),
            3,
            vec!["SEARCH attribution_fact USING INDEX attribution_fact_session (subject_runner=? AND subject_session=?)"],
        ),
        (
            REF_SQL.to_owned(),
            3,
            vec![
                "SEARCH s USING PRIMARY KEY (runner=? AND session_ref=?)",
                "SEARCH f USING PRIMARY KEY (fact_id=?)",
            ],
        ),
        (
            run_sql(),
            1,
            vec![
                "SEARCH f USING INDEX attribution_fact_run (subject_run_id=?)",
                correction,
            ],
        ),
        (
            flow_sql(),
            1,
            vec![
                "SEARCH f USING INDEX attribution_fact_flow (subject_flow_id=?)",
                correction,
            ],
        ),
        (
            placement_sql(),
            1,
            vec![
                "SEARCH f USING INDEX placement_fact_project_time (project_id=?)",
                correction,
            ],
        ),
        (
            ALIAS_HOST_SQL.to_owned(),
            3,
            vec!["SEARCH session_alias USING COVERING INDEX session_alias_host (host_session=? AND runner=?)"],
        ),
        (
            ALIAS_BOUND_SQL.to_owned(),
            3,
            vec!["SEARCH session_alias USING COVERING INDEX session_alias_bound (bound_session=? AND runner=?)"],
        ),
    ] {
        let plan = plan(&conn, &sql, parameters);
        for step in expected {
            assert!(plan.contains(step), "{sql}\n{plan}");
        }
        assert!(!plan.contains("SCAN"), "{sql}\n{plan}");
    }
}

fn event(
    conn: &Connection,
    id: &str,
    runner: &str,
    session: &str,
    run: Option<&str>,
    flow: Option<&str>,
) {
    conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,runner,harness,session,run_ref,flow_ref,occurred_at_ms,charge_basis,recorded_at_ms) VALUES(?1,'thalamus-harness',?1,?2,'opencode',?3,?4,?5,100,'metered_api',100)",params![id,runner,session,run,flow]).unwrap();
}

fn summaries(conn: &mut Connection) {
    let mut builder = crate::report::SummaryMaintenance::default();
    for _ in 0..100 {
        if builder.read_chunk(conn, 10_000).unwrap() {
            let tx = conn.transaction().unwrap();
            builder.publish(&tx, 10_000).unwrap();
            tx.commit().unwrap();
        } else if !builder.has_pending() {
            return;
        }
    }
    panic!("summary backlog did not drain");
}

fn live_fact(id: &str, subject: Value, parent: Value) -> Value {
    json!({"fact_id":id,"subject":subject,"alfonso":format!("agent-{id}"),"parent":parent,"workload":"mason","task":null,"project_id":"project-live","effective_from_ms":0,"recorded_at_ms":1,"supersedes":null})
}

/// Counts the SQLite steps of building one hour whose events name a session
/// reached through an alias, a run and a flow, with `history` unrelated
/// subjects of every kind stored elsewhere.
fn hour_steps(history: usize) -> u64 {
    let mut conn = conn();
    let tx = conn.transaction().unwrap();
    let page = Page {
        facts: vec![
            live_fact(
                "live-session",
                session("thalamus-harness", "opencode", "host"),
                json!({"kind":"flow","flow_id":"flow-live"}),
            ),
            live_fact(
                "live-run",
                json!({"kind":"run","harness":"broca","run_id":"run-live"}),
                Value::Null,
            ),
            live_fact(
                "live-flow",
                json!({"kind":"flow","flow_id":"flow-live"}),
                Value::Null,
            ),
        ],
        next: None,
        through: json!(1),
    };
    commit_page(&tx, Publisher::Prefrontal, None, &page, 10).unwrap();
    let page = Page {
        facts: vec![
            json!({"fact_id":"live-place","project_id":"project-live","workspace_id":"ws-live","effective_from_ms":0,"recorded_at_ms":1,"supersedes":null}),
        ],
        next: None,
        through: json!(1),
    };
    commit_page(&tx, Publisher::Entorhinal, None, &page, 10).unwrap();
    append_aliases(&tx, "thalamus", "opencode", "bound", &["host"]).unwrap();
    for index in 0..history {
        let subject = match index % 3 {
            0 => session("thalamus", "opencode", &format!("old-{index}")),
            1 => json!({"kind":"run","harness":"broca","run_id":format!("old-run-{index}")}),
            _ => json!({"kind":"flow","flow_id":format!("old-flow-{index}")}),
        };
        let mut body = live_fact(&format!("old-{index}"), subject, Value::Null);
        body["project_id"] = json!(format!("old-project-{index}"));
        store_fact(&tx, "prefrontal", &body);
        tx.execute(
            "INSERT INTO placement_fact VALUES(?1,'entorhinal','{}',?2,'old-ws',0,1,NULL)",
            params![format!("old-place-{index}"), format!("old-project-{index}")],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO session_alias VALUES('thalamus','opencode',?1,?2)",
            params![format!("old-{index}"), format!("older-{index}")],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    for (id, run, flow) in [
        ("call-session", None, None),
        ("call-run", Some("run-live"), None),
        ("call-flow", None, Some("flow-live")),
    ] {
        let session = if flow.is_some() { "unrelated" } else { "bound" };
        event(&conn, id, "thalamus", session, run, flow);
    }
    let counter = crate::store::VmCounter::start(&conn);
    summaries(&mut conn);
    let steps = counter.steps();
    let mut owners: Vec<(String, Option<String>, Option<String>)> = conn
        .prepare("SELECT v.value FROM attribution_cell c JOIN summary_value v ON v.value_id=c.attribution")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|value| {
            let tuple: Attribution = serde_json::from_str(&value.unwrap()).unwrap();
            (tuple.alfonso.unwrap(), tuple.parent_alfonso, tuple.workspace)
        })
        .collect();
    owners.sort();
    // The build really resolved each kind of subject, the session through its
    // alias with its parent's owner, and each project's workspace.
    let workspace = Some("ws-live".to_owned());
    assert_eq!(
        owners,
        vec![
            ("agent-live-flow".into(), None, workspace.clone()),
            ("agent-live-run".into(), None, workspace.clone()),
            (
                "agent-live-session".into(),
                Some("agent-live-flow".into()),
                workspace
            ),
        ]
    );
    steps
}

#[test]
fn summary_hour_attribution_costs_the_same_steps_whatever_the_unrelated_history() {
    assert_eq!(hour_steps(1_000), hour_steps(20_000));
}
