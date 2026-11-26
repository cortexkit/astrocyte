use super::*;
use sha2::{Digest, Sha256};

const CHECKSUMS: &str = include_str!("../../test-vectors/broca-fact-owners-v1/SHA256SUMS");

fn checked_sample<'a>(name: &str, json: &'a [u8]) -> (&'a str, Value) {
    let actual = format!("{:x}  {name}", Sha256::digest(json));
    assert_eq!(
        format!("{:x}", Sha256::digest(CHECKSUMS)),
        "25ef298b38ac9588bde7feddf19bd256572567ef5d1eef154089f4babce3d1a8"
    );
    assert_eq!(CHECKSUMS.lines().count(), 6);
    assert!(CHECKSUMS.lines().any(|line| line == actual), "{actual}");
    (
        std::str::from_utf8(json).unwrap(),
        serde_json::from_slice(json).unwrap(),
    )
}

fn assert_sample(
    name: &str,
    json: &[u8],
    kind: CallKind,
    owner: (Option<&str>, Option<&str>, Option<&str>),
) {
    let (json, body) = checked_sample(name, json);
    let id = body["fact_id"].as_str().unwrap();
    let decoded = decode_store_row(1, id, json).unwrap_or_else(|error| panic!("{name}: {error}"));
    let (decoded_id, decoded_kind, stamp) = decoded.scope();
    assert_eq!(decoded_id, id);
    assert_eq!(decoded_kind, kind);
    assert_eq!(
        (
            stamp.agent_id.as_deref(),
            stamp.agent_run_id.as_deref(),
            stamp.flow_id.as_deref()
        ),
        owner
    );
}

macro_rules! scope_sample {
    ($test:ident, $name:literal, $kind:ident, $owner:expr) => {
        #[test]
        fn $test() {
            assert_sample(
                $name,
                include_bytes!(concat!("../../test-vectors/broca-fact-owners-v1/", $name)),
                CallKind::$kind,
                $owner,
            );
        }
    };
}

const AGENT: (Option<&str>, Option<&str>, Option<&str>) =
    (Some("agent-1"), Some("agent-run-1"), None);
const FLOW: (Option<&str>, Option<&str>, Option<&str>) = (None, None, Some("flow-1"));
scope_sample!(
    broca_scope_agent_segment_sample,
    "agent-segment.json",
    Segment,
    AGENT
);
scope_sample!(
    broca_scope_agent_attempt_sample,
    "agent-attempt.json",
    ModelAttempt,
    AGENT
);
scope_sample!(
    broca_scope_agent_system_one_sample,
    "agent-system-one.json",
    SystemOne,
    AGENT
);
scope_sample!(
    broca_scope_flow_segment_sample,
    "flow-segment.json",
    Segment,
    FLOW
);
scope_sample!(
    broca_scope_flow_attempt_sample,
    "flow-attempt.json",
    ModelAttempt,
    FLOW
);
scope_sample!(
    broca_scope_flow_system_one_sample,
    "flow-system-one.json",
    SystemOne,
    FLOW
);

const AGENT_ATTEMPT: &str =
    include_str!("../../test-vectors/broca-fact-owners-v1/agent-attempt.json");
const FLOW_SEGMENT: &str =
    include_str!("../../test-vectors/broca-fact-owners-v1/flow-segment.json");

fn stamped_segment(id: &str, agent: &str) -> Value {
    let mut body: Value = serde_json::from_str(&stored_segment(id)).unwrap();
    body["agent_id"] = agent.into();
    body["agent_run_id"] = "run-owner".into();
    body
}

#[test]
fn broca_scope_drain_stamps_only_recorded_calls_and_guards_first_evidence() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("run-index.db");
    create_store(&path, "instance-a", 1);
    let samples = [
        include_str!("../../test-vectors/broca-fact-owners-v1/agent-segment.json"),
        FLOW_SEGMENT,
        AGENT_ATTEMPT,
        include_str!("../../test-vectors/broca-fact-owners-v1/flow-attempt.json"),
        include_str!("../../test-vectors/broca-fact-owners-v1/agent-system-one.json"),
        include_str!("../../test-vectors/broca-fact-owners-v1/flow-system-one.json"),
    ];
    for (seq, json) in (1..).zip(samples) {
        let body: Value = serde_json::from_str(json).unwrap();
        add_segment(&path, seq, body["fact_id"].as_str().unwrap(), json);
    }
    add_fact(&path, 7, "unstamped");
    let state = test_state();
    drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();
    assert_eq!(count(&state, "usage_event"), 5);
    assert_eq!(count(&state, "broca_segment_evidence"), 2);
    assert_eq!(count(&state, "broca_scope_stamp"), 4);
    state.with_conn(|conn| {
        let seen = conn.prepare(
            "SELECT call_kind,agent_id,agent_run_id,flow_id FROM broca_scope_stamp ORDER BY event_key"
        ).unwrap().query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?,
                                         row.get::<_, Option<String>>(2)?, row.get::<_, Option<String>>(3)?)))
        .unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
        let stamp = |kind: &str, (agent, run, flow): (Option<&str>, Option<&str>, Option<&str>)|
            (kind.to_owned(), agent.map(str::to_owned), run.map(str::to_owned), flow.map(str::to_owned));
        assert_eq!(seen, [stamp("model_attempt", AGENT), stamp("model_attempt", FLOW),
                         stamp("system_one", AGENT), stamp("system_one", FLOW)]);
        for overwrite in [
            "UPDATE broca_scope_stamp SET agent_id='replacement'",
            "DELETE FROM broca_scope_stamp",
            "INSERT OR REPLACE INTO broca_scope_stamp SELECT * FROM broca_scope_stamp LIMIT 1",
        ] {
            assert!(conn.execute(overwrite, []).unwrap_err().to_string().contains("append-only"));
        }
        Ok(())
    }).unwrap();
}

#[test]
fn broca_scope_backfill_resumes_by_fact_identity_and_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("run-index.db");
    create_store(&path, "instance-a", 1);
    add_segment(
        &path,
        1,
        "online",
        &stamped_segment("online", "online-owner").to_string(),
    );
    let mut unstamped_attempt: Value = serde_json::from_str(AGENT_ATTEMPT).unwrap();
    let attempt_id = unstamped_attempt["fact_id"].as_str().unwrap().to_owned();
    let object = unstamped_attempt.as_object_mut().unwrap();
    object.remove("agent_id");
    object.remove("agent_run_id");
    add_segment(&path, 2, &attempt_id, &unstamped_attempt.to_string());
    let marked: Value = serde_json::from_str(FLOW_SEGMENT).unwrap();
    add_segment(&path, 4, marked["fact_id"].as_str().unwrap(), FLOW_SEGMENT);
    add_fact(&path, 8, "historical");
    let state = test_state();
    drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, 100).unwrap();
    assert_eq!(count(&state, "usage_event"), 3);
    assert_eq!(count(&state, "broca_scope_stamp"), 1);

    // Simulate the retained exports the upgraded reader sees after the old
    // reader ingested the calls without looking at these members.
    let peer = Connection::open(&path).unwrap();
    for (seq, json) in [
        (2, AGENT_ATTEMPT),
        (
            8,
            &stamped_segment("historical", "historical-owner").to_string(),
        ),
    ] {
        peer.execute(
            "UPDATE export_facts SET segment_json=?2 WHERE export_seq=?1",
            rusqlite::params![seq, json],
        )
        .unwrap();
    }
    add_segment(
        &path,
        10,
        "beyond-cursor",
        &stamped_segment("beyond-cursor", "future").to_string(),
    );
    let reads = |sql: &str| {
        state
            .with_conn(|conn| {
                Ok(conn
                    .prepare(sql)
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap())
            })
            .unwrap()
    };
    let events = "SELECT json_array(event_id,producer_ref,occurred_at_ms) FROM usage_event ORDER BY event_key";
    let measurements = "SELECT json_array(event_key,role,unit,quantity_units) FROM measurement ORDER BY event_key,role,unit";
    let before = (reads(events), reads(measurements), runtime(&state));
    let progress = || reads("SELECT json_array(lineage,ordinal,done) FROM broca_scope_backfill");
    for (ordinal, expected) in [(1, 0), (2, 1), (4, 0), (8, 1)] {
        assert_eq!(
            source(&path).backfill_scope_page(&state, 1).unwrap(),
            expected
        );
        assert_eq!(
            progress(),
            [format!(
                "[\"instance-a:1\",{ordinal},{}]",
                i64::from(ordinal == 8)
            )]
        );
    }
    assert_eq!(
        (reads(events), reads(measurements), runtime(&state)),
        before
    );
    assert_eq!(reads("SELECT json_array(e.producer_ref,s.agent_id,s.call_kind) FROM broca_scope_stamp s JOIN usage_event e USING(event_key) ORDER BY event_key"), [
        "[\"online\",\"online-owner\",\"segment\"]".to_owned(),
        format!("[\"model_attempt:{attempt_id}:0\",\"agent-1\",\"model_attempt\"]"),
        "[\"historical\",\"historical-owner\",\"segment\"]".to_owned(),
    ]);
    let revisions = "SELECT json_array(kind,hour_number,revision) FROM attribution_hour ORDER BY kind,hour_number";
    let changes = || state.with_conn(|conn| Ok(conn.total_changes())).unwrap();
    let after = (reads(revisions), progress(), changes());
    assert_eq!(
        source(&path)
            .backfill_scope_page(&state, PAGE_LIMIT)
            .unwrap(),
        0
    );
    assert_eq!((reads(revisions), progress(), changes()), after);
    assert_eq!(
        (reads(events), reads(measurements), runtime(&state)),
        before
    );
}
