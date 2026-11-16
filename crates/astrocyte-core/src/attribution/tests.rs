use super::*;
use crate::report::{self, ReportRequest, SummaryMaintenance};
use serde_json::json;

fn conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    crate::store::initialize_connection(&conn).unwrap();
    for migration in crate::store::MIGRATIONS {
        conn.execute_batch(migration.statements).unwrap();
    }
    conn
}
fn fact(id: &str, subject: Value) -> Value {
    json!({"fact_id":id,"subject":subject,"alfonso":"agent_owner","parent":null,"workload":"mason","task":"core.build","project_id":"project-one","effective_from_ms":0,"recorded_at_ms":1,"supersedes":null})
}
fn session(runner: &str, harness: &str, name: &str) -> Value {
    json!({"kind":"session","runner":runner,"harness":harness,"session":name})
}
fn page(facts: Vec<Value>) -> Page {
    Page {
        facts,
        next: None,
        through: json!(1),
    }
}
fn ingest(conn: &mut Connection, publisher: Publisher, facts: Vec<Value>) {
    let page = page(facts);
    let tx = conn.transaction().unwrap();
    commit_page(&tx, publisher, None, &page, 10).unwrap();
    tx.commit().unwrap();
}
fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}
// Keep each published identity field explicit in the ledger setup.
#[allow(clippy::too_many_arguments)]
fn event(
    conn: &Connection,
    id: &str,
    runner: &str,
    harness: &str,
    name: Option<&str>,
    run: Option<&str>,
    flow: Option<&str>,
    time: i64,
) {
    let producer = if runner == "thalamus" {
        "thalamus-harness"
    } else {
        runner
    };
    conn.execute("INSERT INTO usage_event(event_id,producer,producer_ref,runner,harness,session,run_ref,flow_ref,occurred_at_ms,charge_basis,recorded_at_ms) VALUES(?1,?2,?1,?3,?4,?5,?6,?7,?8,'metered_api',?8)",params![id,producer,runner,harness,name,run,flow,time]).unwrap();
}
fn summaries(conn: &mut Connection) {
    let mut builder = SummaryMaintenance::default();
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
fn tuple(conn: &Connection) -> Attribution {
    conn.query_row("SELECT v.value FROM attribution_cell c JOIN summary_value v ON v.value_id=c.attribution LIMIT 1",[],|r|r.get::<_,Option<String>>(0)).unwrap().map(|s|serde_json::from_str(&s).unwrap()).unwrap_or_default()
}

#[test]
fn attribution_real_roots_pair_bytes_match_pinned_checksum() {
    let bytes = include_bytes!("../../test-vectors/attribution-joins-v1/claude-roots.json");
    let pinned = include_str!("../../test-vectors/attribution-joins-v1/SHA256SUMS")
        .split_whitespace()
        .next()
        .unwrap();
    assert_eq!(hex::encode(Sha256::digest(bytes)), pinned);
}

#[test]
fn attribution_validation_names_and_missing_subject_runner_are_set_aside() {
    for (field, value, reason) in [
        ("workload", json!("Uppercase"), "invalid_workload"),
        ("workload", json!("x".repeat(65)), "invalid_workload"),
        ("task", json!("core..build"), "invalid_task"),
        ("task", json!("x".repeat(129)), "invalid_task"),
    ] {
        let mut conn = conn();
        let mut bad = fact("bad", session("broca", "broca", "s"));
        bad[field] = value;
        ingest(&mut conn, Publisher::Prefrontal, vec![bad]);
        assert_eq!(count(&conn, "attribution_fact"), 0, "{field}");
        assert_eq!(
            conn.query_row("SELECT reason FROM set_aside_record", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            reason
        );
        let coverage =
            report::ledger_coverage(&conn, &report::LedgerCoverageRequest {}, 10).unwrap();
        assert_eq!(
            coverage.set_aside_by_source[&Publisher::Prefrontal.source()].records,
            1
        );
    }
    let mut conn = conn();
    let mut bad = fact("runner-missing", session("broca", "broca", "s"));
    bad["subject"].as_object_mut().unwrap().remove("runner");
    ingest(&mut conn, Publisher::Prefrontal, vec![bad]);
    assert_eq!(count(&conn, "attribution_fact"), 0);
    assert_eq!(
        conn.query_row("SELECT reason FROM set_aside_record", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "subject_missing_runner"
    );
}

#[test]
fn attribution_validation_parent_without_runner_keeps_fact_but_discloses_unknown_parent() {
    let mut conn = conn();
    let mut body = fact("child", session("broca", "broca", "child"));
    body["parent"] = json!({"kind":"session","harness":"broca","session":"parent"});
    ingest(&mut conn, Publisher::Prefrontal, vec![body]);
    assert_eq!(count(&conn, "attribution_fact"), 1);
    assert_eq!(
        conn.query_row("SELECT reason FROM set_aside_record", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "parent_missing_runner"
    );
}

#[test]
fn attribution_validation_second_publisher_and_changed_body_never_replace_evidence() {
    let mut conn = conn();
    let original = fact("one", session("broca", "broca", "s"));
    ingest(&mut conn, Publisher::Prefrontal, vec![original.clone()]);
    let mut other = original.clone();
    other["fact_id"] = json!("other");
    ingest(&mut conn, Publisher::MagicContext, vec![other]);
    let mut changed = original.clone();
    changed["workload"] = json!("other");
    ingest(&mut conn, Publisher::Prefrontal, vec![changed]);
    assert_eq!(count(&conn, "attribution_fact"), 1);
    assert_eq!(
        serde_json::from_str::<Value>(
            &conn
                .query_row("SELECT body FROM attribution_fact", [], |r| r
                    .get::<_, String>(0))
                .unwrap()
        )
        .unwrap(),
        original
    );
    let reasons: Vec<String> = conn
        .prepare("SELECT reason FROM set_aside_record ORDER BY reason")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        reasons,
        ["fact_body_changed", "subject_has_other_publisher"]
    );
}

#[test]
fn attribution_terminal_replay_preserves_opaque_cursor_and_has_no_changes() {
    let mut conn = conn();
    let first = Page {
        facts: vec![fact("one", session("broca", "broca", "s1"))],
        next: Some(json!({"opaque":"cursor"})),
        through: json!("different-watermark"),
    };
    let tx = conn.transaction().unwrap();
    commit_page(&tx, Publisher::Prefrontal, None, &first, 10).unwrap();
    tx.commit().unwrap();
    let terminal = page(vec![fact("two", session("broca", "broca", "s2"))]);
    let tx = conn.transaction().unwrap();
    commit_page(
        &tx,
        Publisher::Prefrontal,
        first.next.as_ref(),
        &terminal,
        20,
    )
    .unwrap();
    tx.commit().unwrap();
    let before = conn.total_changes();
    assert!(!page_needs_commit(&conn, Publisher::Prefrontal, &terminal).unwrap());
    assert_eq!(conn.total_changes(), before);
    let cursor: String = conn
        .query_row("SELECT cursor FROM attribution_cursor", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&cursor).unwrap(),
        first.next.unwrap()
    );
}

#[test]
fn attribution_join_broca_run_precedes_session_and_session_precedes_flow() {
    let mut conn = conn();
    let mut run = fact(
        "run",
        json!({"kind":"run","harness":"broca","run_id":"run-one"}),
    );
    run["alfonso"] = json!("run-owner");
    ingest(
        &mut conn,
        Publisher::Prefrontal,
        vec![
            run,
            fact("session", session("broca", "broca", "session-one")),
        ],
    );
    event(
        &conn,
        "call",
        "broca",
        "broca",
        Some("session-one"),
        Some("run-one"),
        Some("flow-one"),
        100,
    );
    summaries(&mut conn);
    assert_eq!(tuple(&conn).alfonso.as_deref(), Some("run-owner"));
    let mut conn = self::conn();
    ingest(
        &mut conn,
        Publisher::Prefrontal,
        vec![fact("session", session("broca", "broca", "session-one"))],
    );
    event(
        &conn,
        "call",
        "broca",
        "broca",
        Some("session-one"),
        None,
        Some("flow-one"),
        100,
    );
    summaries(&mut conn);
    assert_eq!(tuple(&conn).alfonso.as_deref(), Some("agent_owner"));
}

#[test]
fn attribution_join_basal_pinned_flow_and_flow_session() {
    for by_session in [false, true] {
        let mut conn = conn();
        let flow: Value = serde_json::from_str(include_str!(
            "../../test-vectors/attribution-facts-v1/basal/flow.json"
        ))
        .unwrap();
        let child: Value = serde_json::from_str(include_str!(
            "../../test-vectors/attribution-facts-v1/basal/flow-session.json"
        ))
        .unwrap();
        ingest(&mut conn, Publisher::Basal, vec![flow, child.clone()]);
        event(
            &conn,
            "call",
            "broca",
            "broca",
            if by_session {
                child["subject"]["session"].as_str()
            } else {
                None
            },
            None,
            Some("attribution-flow"),
            child["effective_from_ms"].as_i64().unwrap() + 10,
        );
        summaries(&mut conn);
        assert_eq!(tuple(&conn).alfonso.as_deref(), Some("agent_attribution"));
    }
}

#[test]
fn attribution_join_aliases_meet_uuid_and_bound_session_without_harness_comparison() {
    for use_uuid in [false, true] {
        let mut conn = conn();
        let case: Value = serde_json::from_str(include_str!(
            "../../test-vectors/attribution-joins-v1/claude-roots.json"
        ))
        .unwrap();
        let root = &case["entry"];
        let owner = root["owner_session"].as_str().unwrap();
        let uuid = root["session"].as_str().unwrap();
        let tx = conn.transaction().unwrap();
        append_aliases(
            &tx,
            "thalamus-harness",
            "claude-code",
            owner,
            &[uuid, root["session_ref"].as_str().unwrap()],
        )
        .unwrap();
        tx.commit().unwrap();
        ingest(
            &mut conn,
            Publisher::Prefrontal,
            vec![fact(
                "cc",
                session(
                    "thalamus-harness",
                    "claude-code",
                    if use_uuid { uuid } else { owner },
                ),
            )],
        );
        event(
            &conn,
            "call",
            "thalamus",
            "claude-code",
            Some(owner),
            None,
            None,
            100,
        );
        summaries(&mut conn);
        assert_eq!(
            tuple(&conn).alfonso.as_deref(),
            Some("agent_owner"),
            "uuid={use_uuid}"
        );
    }
    let mut conn = conn();
    let tx = conn.transaction().unwrap();
    append_aliases(
        &tx,
        "thalamus-harness",
        "opencode2",
        "ses_host",
        &["opaque-bound-ref"],
    )
    .unwrap();
    tx.commit().unwrap();
    ingest(
        &mut conn,
        Publisher::Prefrontal,
        vec![fact(
            "oc",
            session("thalamus-harness", "opencode2", "opaque-bound-ref"),
        )],
    );
    event(
        &conn,
        "oc",
        "thalamus",
        "opencode",
        Some("ses_host"),
        None,
        None,
        100,
    );
    summaries(&mut conn);
    assert_eq!(tuple(&conn).alfonso.as_deref(), Some("agent_owner"));
}

#[test]
fn attribution_join_pi_host_id_mc_parent_and_superseded_fact() {
    let mut conn = conn();
    ingest(
        &mut conn,
        Publisher::Prefrontal,
        vec![fact("pi", session("thalamus-harness", "pi", "host-uuid"))],
    );
    event(
        &conn,
        "pi",
        "thalamus",
        "pi",
        Some("host-uuid"),
        None,
        None,
        100,
    );
    summaries(&mut conn);
    assert_eq!(tuple(&conn).alfonso.as_deref(), Some("agent_owner"));
    let mut conn = self::conn();
    let wire: Value = serde_json::from_str(include_str!(
        "../../test-vectors/attribution-facts-v1/magic-context/read-page.json"
    ))
    .unwrap();
    let child = wire["response"]["result"]["facts"][0].clone();
    let mut parent = fact("head", child["parent"].clone());
    parent["alfonso"] = json!("parent-owner");
    let mut corrected = parent.clone();
    corrected["fact_id"] = json!("head-corrected");
    corrected["supersedes"] = json!("head");
    corrected["alfonso"] = json!("corrected-owner");
    ingest(&mut conn, Publisher::Prefrontal, vec![parent, corrected]);
    ingest(&mut conn, Publisher::MagicContext, vec![child.clone()]);
    event(
        &conn,
        "mc",
        "thalamus",
        "opencode",
        child["subject"]["session"].as_str(),
        None,
        None,
        child["effective_from_ms"].as_i64().unwrap() + 10,
    );
    summaries(&mut conn);
    assert_eq!(
        tuple(&conn).parent_alfonso.as_deref(),
        Some("corrected-owner")
    );
}

#[test]
fn attribution_fact_dirtying_changes_only_attribution_revisions() {
    let mut conn = conn();
    event(&conn, "call", "broca", "broca", Some("s"), None, None, 100);
    summaries(&mut conn);
    let before:(i64,i64)=conn.query_row("SELECT s.revision,a.revision FROM summary_hour s JOIN attribution_hour a USING(kind,hour_number)",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    ingest(
        &mut conn,
        Publisher::Prefrontal,
        vec![fact("one", session("broca", "broca", "s"))],
    );
    let after:(i64,i64)=conn.query_row("SELECT s.revision,a.revision FROM summary_hour s JOIN attribution_hour a USING(kind,hour_number)",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(before.0, after.0);
    assert!(after.1 > before.1);
}

#[test]
fn attribution_placement_correction_dirties_the_superseded_effective_window() {
    let mut conn = conn();
    let mut owner = fact("owner", session("broca", "broca", "s"));
    owner["project_id"] = json!("served-project");
    ingest(&mut conn, Publisher::Prefrontal, vec![owner]);
    let examples: Value = serde_json::from_str(include_str!(
        "../../test-vectors/attribution-facts-v1/entorhinal/records.json"
    ))
    .unwrap();
    let original = examples[1]["reply"]["result"]["facts"][0].clone();
    event(
        &conn,
        "before",
        "broca",
        "broca",
        Some("s"),
        None,
        None,
        700,
    );
    ingest(&mut conn, Publisher::Entorhinal, vec![original.clone()]);
    summaries(&mut conn);
    assert_eq!(tuple(&conn).workspace.as_deref(), Some("served-workspace"));
    let correction = json!({"fact_id":"correction","project_id":"served-project","workspace_id":"moved","supersedes":original["fact_id"],"effective_from_ms":3600700,"recorded_at_ms":3600700});
    ingest(&mut conn, Publisher::Entorhinal, vec![correction]);
    summaries(&mut conn);
    assert_eq!(tuple(&conn).workspace, None);
}

#[test]
fn attribution_dirty_queries_seek_existing_ledger_context_time_indexes() {
    let conn = conn();
    for (column, index, runner) in [
        ("session", "usage_event_session_time", true),
        ("run_ref", "usage_event_run_time", false),
        ("flow_ref", "usage_event_flow_time", false),
    ] {
        let query = format!(
            "EXPLAIN QUERY PLAN {}",
            dirty_sql(column, index, runner, true)
        );
        let mut parameters = vec![rusqlite::types::Value::Text("context".into())];
        if runner {
            parameters.extend(["broca", "broca"].map(|s| rusqlite::types::Value::Text(s.into())));
        }
        parameters.push(rusqlite::types::Value::Integer(100));
        let plan = conn
            .prepare(&query)
            .unwrap()
            .query_map(rusqlite::params_from_iter(parameters), |r| {
                r.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("SEARCH usage_event") && plan.contains(index),
            "{plan}"
        );
        assert!(plan.contains("occurred_at_ms>?"), "{plan}");
        assert!(!plan.contains("SCAN usage_event"), "{plan}");
    }
}

#[test]
fn attribution_report_combination_refusal_names_the_incompatible_pair() {
    let conn = conn();
    let request = ReportRequest {
        grouping: vec!["alfonso".into(), "account".into()],
        grain: "day".into(),
        timezone: "UTC".into(),
        ..Default::default()
    };
    let error = report::spend_report(&conn, &request, 100).unwrap_err();
    let report::ReportError::Refused(reason) = error else {
        panic!("unexpected error")
    };
    assert_eq!(reason.code(), "combination_unavailable");
    assert!(reason.message().contains("alfonso") && reason.message().contains("account"));
}
