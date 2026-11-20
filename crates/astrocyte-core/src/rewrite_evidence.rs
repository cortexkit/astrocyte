//! Short-lived runner facts and permanent, append-only rewrite explanations.

use cortexkit_usage_steps::{cause::rewrite_cause, Cause, StepRow, StepSource, Verdict};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const EVIDENCE_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PRUNE_BATCH: i64 = 1_000;

/// Facts available only on the non-replayed runner event. Omission is unknown.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_ttl_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_implicit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm_up: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_floor: Option<String>,
}

fn json_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn intern(conn: &Connection, digest: Option<&str>) -> rusqlite::Result<Option<i64>> {
    let Some(digest) = digest else {
        return Ok(None);
    };
    conn.execute(
        "INSERT INTO step_digest(value) SELECT ?1
         WHERE NOT EXISTS (SELECT 1 FROM step_digest WHERE value=?1)",
        [digest],
    )?;
    conn.query_row(
        "SELECT digest_key FROM step_digest WHERE value=?1",
        [digest],
        |row| row.get(0),
    )
    .map(Some)
}

/// Save an event's evidence independently of the feed's one-day lifetime.
pub(crate) fn save(
    conn: &Connection,
    now_ms: i64,
    harness: &str,
    step_ref: &str,
    runner: &str,
    evidence: &Evidence,
) -> rusqlite::Result<bool> {
    let tools = intern(conn, evidence.tools_digest.as_deref())?;
    let system = intern(conn, evidence.system_digest.as_deref())?;
    let mut facts = serde_json::to_value(evidence).map_err(json_error)?;
    // Digests live only in the intern table, not once per event or feed emission.
    facts.as_object_mut().unwrap().remove("tools_digest");
    facts.as_object_mut().unwrap().remove("system_digest");
    let changed = conn.execute(
        "INSERT INTO step_evidence(harness,step_ref,runner,recorded_at_ms,facts_json,tools_key,system_key)
         VALUES (?1,?2,?3,?4,?5,?6,?7)
         ON CONFLICT(harness,step_ref) DO UPDATE SET runner=excluded.runner,
             order_ms=COALESCE(json_extract(excluded.facts_json,'$.started_at_ms'),occurred_at_ms),
             facts_json=excluded.facts_json,tools_key=excluded.tools_key,system_key=excluded.system_key
         WHERE runner IS NOT excluded.runner OR facts_json IS NOT excluded.facts_json
             OR tools_key IS NOT excluded.tools_key OR system_key IS NOT excluded.system_key",
        params![harness, step_ref, runner, now_ms, facts.to_string(), tools, system],
    )?;
    Ok(changed != 0)
}

/// Expand interned evidence at the read boundary, so replacements keep the facts
/// without copying them into every immutable feed emission.
pub(crate) fn hydrate(
    conn: &Connection,
    row: &mut Map<String, Value>,
) -> rusqlite::Result<Option<String>> {
    let stored = conn
        .prepare_cached(
            "SELECT e.runner,e.facts_json,t.value,s.value FROM step_evidence e
         LEFT JOIN step_digest t ON t.digest_key=e.tools_key
         LEFT JOIN step_digest s ON s.digest_key=e.system_key
         WHERE e.harness=?1 AND e.step_ref=?2",
        )?
        .query_row(
            params![row["harness"].as_str(), row["step_ref"].as_str()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((runner, facts, tools, system)) = stored else {
        return Ok(None);
    };
    let facts: Map<String, Value> = serde_json::from_str(&facts).map_err(json_error)?;
    row.extend(facts);
    if let Some(tools) = tools {
        row.insert("tools_digest".into(), tools.into());
    }
    if let Some(system) = system {
        row.insert("system_digest".into(), system.into());
    }
    Ok(runner)
}

pub fn cause(conn: &Connection, harness: &str, step_ref: &str) -> rusqlite::Result<Option<Cause>> {
    let cause: Option<String> = conn.prepare_cached(
        "SELECT cause FROM step_cause WHERE harness=?1 AND step_ref=?2 ORDER BY cause_key DESC LIMIT 1",
    )?.query_row(params![harness, step_ref], |row| row.get(0)).optional()?.flatten();
    Ok(cause.as_deref().map(Cause::from_wire))
}

/// Join a page's step identities to permanent causes and unexpired event facts.
/// Reads enforce retention even when the writer's bounded prune has not yet
/// reached an expired row. A JSON array keeps the page within SQLite's bind limit.
pub fn hydrate_cache_steps(
    conn: &Connection,
    harness: &str,
    steps: &mut [crate::cache_diagnostics::CacheStep],
    now_ms: i64,
) -> rusqlite::Result<()> {
    if steps.is_empty() {
        return Ok(());
    }
    let identities =
        serde_json::to_string(&steps.iter().map(|step| &step.step_ref).collect::<Vec<_>>())
            .map_err(json_error)?;
    let mut statement = conn.prepare_cached(
        "SELECT p.key,c.cause,e.facts_json,t.value,s.value FROM json_each(?1) p
         LEFT JOIN step_cause c ON c.cause_key=(SELECT cause_key FROM step_cause
             WHERE harness=?2 AND step_ref=p.value ORDER BY cause_key DESC LIMIT 1)
         LEFT JOIN step_evidence e ON e.harness=?2 AND e.step_ref=p.value AND e.recorded_at_ms>=?3
         LEFT JOIN step_digest t ON t.digest_key=e.tools_key
         LEFT JOIN step_digest s ON s.digest_key=e.system_key",
    )?;
    let mut rows = statement.query(params![
        identities,
        harness,
        now_ms.saturating_sub(EVIDENCE_WINDOW_MS)
    ])?;
    while let Some(row) = rows.next()? {
        let index: usize = row.get(0)?;
        let cause: Option<String> = row.get(1)?;
        let facts: Option<String> = row.get(2)?;
        let mut evidence = match facts {
            Some(facts) => serde_json::from_str::<Evidence>(&facts).map_err(json_error)?,
            None => Evidence::default(),
        };
        evidence.tools_digest = row.get(3)?;
        evidence.system_digest = row.get(4)?;
        steps[index].cause = cause.as_deref().map(Cause::from_wire);
        steps[index].evidence = evidence;
    }
    Ok(())
}

fn record_cause(conn: &Connection, row: &StepRow, computed: Option<Cause>) -> rusqlite::Result<()> {
    if cause(conn, &row.harness, &row.step_ref)? != computed {
        conn.execute(
            "INSERT INTO step_cause(harness,step_ref,cause) VALUES (?1,?2,?3)",
            params![
                row.harness,
                row.step_ref,
                computed.as_ref().map(Cause::as_str)
            ],
        )?;
    }
    Ok(())
}

/// A retained distinct step, ordered by request start and first emission.
struct ComparedRow {
    row: StepRow,
    runner: Option<String>,
    order_ms: u64,
    first_cursor: i64,
}

fn compared(
    conn: &Connection,
    mut row: Map<String, Value>,
    first_cursor: i64,
) -> rusqlite::Result<ComparedRow> {
    let runner = hydrate(conn, &mut row)?;
    row.insert("cursor".into(), Value::from("0"));
    let row: StepRow = serde_json::from_value(Value::Object(row)).map_err(json_error)?;
    let order_ms = row.started_at_ms.unwrap_or(row.occurred_at_ms);
    Ok(ComparedRow {
        row,
        runner,
        order_ms,
        first_cursor,
    })
}

fn order(row: &ComparedRow) -> (u64, i64) {
    (row.order_ms, row.first_cursor)
}

/// A row can leave a comparison chain when late evidence identifies its runner
/// or request start. Capture the old position before replacing that evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Position {
    runner: Option<String>,
    order: (u64, i64),
}

pub(crate) fn position(
    conn: &Connection,
    harness: &str,
    step_ref: &str,
) -> rusqlite::Result<Option<Position>> {
    conn.prepare_cached(
        "SELECT runner,order_ms,first_feed_cursor FROM step_evidence
        WHERE harness=?1 AND step_ref=?2 AND latest_feed_cursor IS NOT NULL",
    )?
    .query_row(params![harness, step_ref], |row| {
        Ok(Position {
            runner: row.get(0)?,
            order: (row.get(1)?, row.get(2)?),
        })
    })
    .optional()
}

const PREVIOUS_SQL: &str = "SELECT f.row_json,e.first_feed_cursor FROM step_evidence e
    JOIN step_feed f ON f.feed_cursor=e.latest_feed_cursor
    WHERE e.harness=?1 AND e.session=?2 AND e.runner IS ?3 AND e.step_ref<>?4
      AND e.latest_feed_cursor IS NOT NULL AND (e.order_ms,e.first_feed_cursor)<(?5,?6)
    ORDER BY e.order_ms DESC,e.first_feed_cursor DESC LIMIT 1";
const PREVIOUS_EVIDENCE_SQL: &str = "SELECT e.step_ref,e.session,e.facts_json,e.provider,e.model,
    t.value,s.value,e.first_feed_cursor,e.order_ms FROM step_evidence e
    LEFT JOIN step_digest t ON t.digest_key=e.tools_key
    LEFT JOIN step_digest s ON s.digest_key=e.system_key
    WHERE e.harness=?1 AND e.session=?2 AND e.runner IS ?3 AND e.step_ref<>?4
      AND e.latest_feed_cursor IS NULL AND e.recorded_at_ms>=?7
      AND (e.order_ms,e.first_feed_cursor)<(?5,?6)
    ORDER BY e.order_ms DESC,e.first_feed_cursor DESC LIMIT 1";
const NEXT_SQL: &str = "SELECT f.row_json,e.first_feed_cursor FROM step_evidence e
    JOIN step_feed f ON f.feed_cursor=e.latest_feed_cursor
    WHERE e.harness=?1 AND e.session=?2 AND e.runner IS ?3 AND e.step_ref<>?4
      AND e.latest_feed_cursor IS NOT NULL AND (e.order_ms,e.first_feed_cursor)>(?5,?6)
    ORDER BY e.order_ms,e.first_feed_cursor LIMIT 1";

fn neighbour(
    conn: &Connection,
    sql: &str,
    harness: &str,
    session: &str,
    excluded: &str,
    position: &Position,
) -> rusqlite::Result<Option<ComparedRow>> {
    let stored = conn
        .prepare_cached(sql)?
        .query_row(
            params![
                harness,
                session,
                position.runner,
                excluded,
                position.order.0,
                position.order.1
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get(1)?)),
        )
        .optional()?;
    stored
        .map(|(json, cursor)| {
            compared(
                conn,
                serde_json::from_str(&json).map_err(json_error)?,
                cursor,
            )
        })
        .transpose()
}

struct StoredEvidence {
    step_ref: String,
    session: String,
    facts: String,
    provider: Option<String>,
    model: Option<String>,
    tools: Option<String>,
    system: Option<String>,
    first_cursor: i64,
    order_ms: u64,
}

fn evidence_compared(
    harness: &str,
    runner: Option<String>,
    stored: StoredEvidence,
    prompt: u64,
    prompt_dropped: bool,
    verdict: Verdict,
) -> rusqlite::Result<ComparedRow> {
    let mut evidence: Evidence = serde_json::from_str(&stored.facts).map_err(json_error)?;
    evidence.tools_digest = stored.tools;
    evidence.system_digest = stored.system;
    // This row stands for the session's earlier step whose feed copy has expired.
    // `rewrite_cause` 0.2.8 reads only its provider, model, account, finish and TTL,
    // request start, both digests, view and signed floor.
    // The remaining required row members are inert private placeholders, never
    // stored or served. The placeholder-independence test guards this assumption.
    let mut row = StepRow::new(
        "0",
        harness,
        StepSource::Other("evidence-only".into()),
        stored.step_ref,
        stored.session,
        0,
        prompt,
        verdict,
        prompt_dropped,
    );
    row.started_at_ms = evidence.started_at_ms;
    row.finished_at_ms = evidence.finished_at_ms;
    row.account_ref = evidence.account_ref;
    row.cache_ttl_ms = evidence.cache_ttl_ms;
    row.cache_implicit = evidence.cache_implicit;
    row.warm_up = evidence.warm_up;
    row.view_version = evidence.view_version;
    row.tools_digest = evidence.tools_digest;
    row.system_digest = evidence.system_digest;
    row.signed_floor = evidence.signed_floor;
    row.provider = stored.provider;
    row.model = stored.model;
    Ok(ComparedRow {
        row,
        runner,
        order_ms: stored.order_ms,
        first_cursor: stored.first_cursor,
    })
}

fn previous_evidence(
    conn: &Connection,
    now_ms: i64,
    harness: &str,
    session: &str,
    excluded: &str,
    position: &Position,
) -> rusqlite::Result<Option<ComparedRow>> {
    let stored = conn
        .prepare_cached(PREVIOUS_EVIDENCE_SQL)?
        .query_row(
            params![
                harness,
                session,
                position.runner.as_deref(),
                excluded,
                position.order.0,
                position.order.1,
                now_ms.saturating_sub(EVIDENCE_WINDOW_MS)
            ],
            |row| {
                Ok(StoredEvidence {
                    step_ref: row.get(0)?,
                    session: row.get(1)?,
                    facts: row.get(2)?,
                    provider: row.get(3)?,
                    model: row.get(4)?,
                    tools: row.get(5)?,
                    system: row.get(6)?,
                    first_cursor: row.get(7)?,
                    order_ms: row.get(8)?,
                })
            },
        )
        .optional()?;
    stored
        .map(|stored| {
            evidence_compared(
                harness,
                position.runner.clone(),
                stored,
                0,
                false,
                Verdict::Other("evidence-only".into()),
            )
        })
        .transpose()
}

fn previous(
    conn: &Connection,
    now_ms: i64,
    harness: &str,
    session: &str,
    excluded: &str,
    position: &Position,
) -> rusqlite::Result<Option<ComparedRow>> {
    let feed = neighbour(conn, PREVIOUS_SQL, harness, session, excluded, position)?;
    let evidence = previous_evidence(conn, now_ms, harness, session, excluded, position)?;
    Ok(feed.into_iter().chain(evidence).max_by_key(order))
}

/// Recompute the changed step and the successors of both its old and new
/// positions. A late insertion or newly known runner can change predecessors
/// even when the following steps' own facts have not changed.
pub(crate) fn compute(
    conn: &Connection,
    now_ms: i64,
    body: &mut Map<String, Value>,
    old_position: Option<Position>,
) -> rusqlite::Result<Vec<(String, String, String)>> {
    let harness = body["harness"].as_str().unwrap();
    let step_ref = body["step_ref"].as_str().unwrap();
    let session = body["session"].as_str().unwrap();
    let first_cursor: i64 = conn.query_row(
        "SELECT COALESCE((SELECT first_feed_cursor FROM step_evidence WHERE harness=?1 AND step_ref=?2),
            (SELECT MIN(feed_cursor) FROM step_feed WHERE harness=?1 AND step_ref=?2),
            (SELECT COALESCE(MAX(feed_cursor),0)+1 FROM step_feed))", params![harness, step_ref], |row| row.get(0))?;
    let current = compared(conn, body.clone(), first_cursor)?;
    // Preserve the tie-break through replacements. File-only placeholders also
    // need ordering keys, without inventing any runner-event facts.
    conn.execute(
        "INSERT INTO step_evidence(harness,step_ref,recorded_at_ms,facts_json,first_feed_cursor,session,order_ms)
        VALUES (?1,?2,?3,'{}',?4,?5,?6) ON CONFLICT(harness,step_ref) DO UPDATE
        SET first_feed_cursor=excluded.first_feed_cursor,session=excluded.session,order_ms=excluded.order_ms",
        params![harness, step_ref, now_ms, first_cursor,session,order(&current).0],
    )?;
    let new_position = Position {
        runner: current.runner.clone(),
        order: order(&current),
    };
    let prev = previous(conn, now_ms, harness, session, step_ref, &new_position)?;
    let computed = rewrite_cause(prev.as_ref().map(|other| &other.row), &current.row);
    record_cause(conn, &current.row, computed.clone())?;
    let mut positions = vec![new_position.clone()];
    if let Some(old) = old_position.filter(|old| *old != new_position) {
        positions.push(old);
    }
    let mut corrections = Vec::new();
    let mut visited = Vec::new();
    for position in positions {
        let Some(next) = neighbour(conn, NEXT_SQL, harness, session, step_ref, &position)? else {
            continue;
        };
        let identity = (next.row.harness.clone(), next.row.step_ref.clone());
        if visited.contains(&identity) {
            continue;
        }
        visited.push(identity);
        let next_position = Position {
            runner: next.runner.clone(),
            order: order(&next),
        };
        let previous = previous(conn, now_ms, harness, session, step_ref, &next_position)?;
        // The current emission has not been inserted yet. Its new facts must
        // win over an older feed body when it is the successor's predecessor.
        let prev = previous
            .as_ref()
            .into_iter()
            .chain(std::iter::once(&current))
            .filter(|other| other.runner == next.runner && order(other) < order(&next))
            .max_by_key(|other| order(other));
        let computed = rewrite_cause(prev.map(|other| &other.row), &next.row);
        if cause(conn, &next.row.harness, &next.row.step_ref)? == computed {
            continue;
        }
        record_cause(conn, &next.row, computed.clone())?;
        let mut next_body: Map<String, Value> = conn.query_row(
            "SELECT row_json FROM step_feed WHERE harness=?1 AND step_ref=?2 ORDER BY feed_cursor DESC LIMIT 1",
            params![next.row.harness, next.row.step_ref], |row| row.get::<_, String>(0))
            .and_then(|json| serde_json::from_str(&json).map_err(json_error))?;
        next_body.remove("cause");
        if let Some(cause) = computed {
            next_body.insert("cause".into(), cause.as_str().into());
        }
        corrections.push((
            next.row.harness.clone(),
            next.row.step_ref.clone(),
            Value::Object(next_body).to_string(),
        ));
    }
    body.remove("cause");
    if let Some(cause) = computed {
        body.insert("cause".into(), Value::from(cause.as_str()));
    }
    Ok(corrections)
}

const PRUNE_SQL: &str = "SELECT MAX(evidence_key) FROM (SELECT evidence_key,recorded_at_ms
    FROM step_evidence ORDER BY evidence_key LIMIT ?2) WHERE recorded_at_ms<?1";

pub(crate) fn prune(conn: &Connection, now_ms: i64) -> rusqlite::Result<()> {
    let cutoff = now_ms.saturating_sub(EVIDENCE_WINDOW_MS);
    let last: Option<i64> = conn
        .prepare_cached(PRUNE_SQL)?
        .query_row(params![cutoff, PRUNE_BATCH], |row| row.get(0))?;
    if let Some(last) = last {
        conn.execute(
            "DELETE FROM step_evidence WHERE evidence_key<=?1 AND recorded_at_ms<?2",
            params![last, cutoff],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::conn;

    #[test]
    fn rewrite_neighbour_queries_seek_the_order_index() {
        let conn = conn();
        for (sql, index) in [
            (PREVIOUS_SQL, "step_evidence_neighbours"),
            (PREVIOUS_EVIDENCE_SQL, "step_evidence_retained_neighbours"),
            (NEXT_SQL, "step_evidence_neighbours"),
        ] {
            for runner in [None, Some("broca")] {
                let values: Vec<&dyn rusqlite::ToSql> = if sql == PREVIOUS_EVIDENCE_SQL {
                    vec![&"pi", &"session", &runner, &"excluded", &100, &1, &0]
                } else {
                    vec![&"pi", &"session", &runner, &"excluded", &100, &1]
                };
                let plan = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                    .unwrap()
                    .query_map(values.as_slice(), |row| row.get::<_, String>(3))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                assert!(
                    plan.iter().any(|line| {
                        line.contains(&format!("SEARCH e USING INDEX {index}"))
                            && line.contains("order_ms")
                    }),
                    "{plan:?}"
                );
                if sql == PREVIOUS_EVIDENCE_SQL {
                    for digest in ["t", "s"] {
                        assert!(
                            plan.iter().any(|line| line
                                .contains(&format!("SEARCH {digest} USING INTEGER PRIMARY KEY"))),
                            "{plan:?}"
                        );
                    }
                } else {
                    assert!(
                        plan.iter()
                            .any(|line| line.contains("SEARCH f USING INTEGER PRIMARY KEY")),
                        "{plan:?}"
                    );
                }
                assert!(
                    !plan
                        .iter()
                        .any(|line| line.contains("SCAN") || line.contains("TEMP B-TREE")),
                    "{plan:?}"
                );
            }
        }
    }

    #[test]
    fn rewrite_neighbour_steps_are_flat_at_two_history_sizes() {
        for evidence_only in [false, true] {
            let mut costs = Vec::new();
            for count in [1_000_i64, 10_000] {
                let conn = conn();
                conn.execute_batch("DROP TRIGGER step_feed_index_evidence;")
                    .unwrap();
                conn.execute(
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1)
                     INSERT INTO step_feed(feed_cursor,harness,step_ref,source,session,committed_at_ms,row_json)
                     SELECT i,'pi','step-'||i,'runner','session',1,
                       json_object('cursor','0','harness','pi','source','runner',
                         'step_ref','step-'||i,'session','session','occurred_at_ms',i,
                         'prompt',100,'verdict','stable','prompt_dropped',json('false')) FROM n",
                    [count],
                )
                .unwrap();
                conn.execute(
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1)
                     INSERT INTO step_evidence(harness,step_ref,runner,recorded_at_ms,facts_json,
                       first_feed_cursor,session,order_ms,feed_source,occurred_at_ms,reports_cache,
                       positive_read,latest_feed_cursor,provider,model)
                     SELECT 'pi','step-'||i,'broca',?2,'{}',i,'session',i,'runner',i,0,0,
                       CASE ?3 WHEN 1 THEN i END,'anthropic','model' FROM n",
                    params![
                        count,
                        EVIDENCE_WINDOW_MS,
                        if evidence_only { 0_i64 } else { 1_i64 }
                    ],
                )
                .unwrap();
                let position = Position {
                    runner: Some("broca".into()),
                    order: (u64::try_from(count + 1).unwrap(), count + 1),
                };
                let counter = crate::store::VmCounter::start(&conn);
                let found = previous(
                    &conn,
                    EVIDENCE_WINDOW_MS + 1,
                    "pi",
                    "session",
                    "current",
                    &position,
                )
                .unwrap();
                costs.push(counter.steps());
                let found = found.expect("the measured lookup must find a row");
                assert_eq!(
                    order(&found),
                    (u64::try_from(count).unwrap(), count),
                    "the lookup must return the newest predecessor"
                );
            }
            eprintln!(
                "evidence-only={evidence_only} predecessor VM instructions at 1,000 / 10,000 evidence rows: {costs:?}"
            );
            assert!(
                costs[1] < costs[0] + 100,
                "predecessor lookup scanned evidence history: {costs:?}"
            );
        }
    }

    #[test]
    fn evidence_predecessor_ignores_inert_required_row_fields() {
        let stored = || {
            StoredEvidence {
                step_ref: "previous".into(),
                session: "session".into(),
                facts: r#"{"started_at_ms":100,"finished_at_ms":150,"account_ref":"account","cache_ttl_ms":10}"#.into(),
                provider: Some("anthropic".into()),
                model: Some("model".into()),
                tools: None,
            system: None,
            first_cursor: 1,
            order_ms: 100,
            }
        };
        let first = evidence_compared(
            "pi",
            Some("broca".into()),
            stored(),
            0,
            false,
            Verdict::Other("placeholder-a".into()),
        )
        .unwrap();
        let second = evidence_compared(
            "pi",
            Some("broca".into()),
            stored(),
            1_000_000,
            true,
            Verdict::Bust,
        )
        .unwrap();
        let mut current = StepRow::new(
            "2",
            "pi",
            StepSource::Runner,
            "current",
            "session",
            200,
            100,
            Verdict::FullBust,
            false,
        );
        current.started_at_ms = Some(200);
        current.account_ref = Some("account".into());
        current.provider = Some("anthropic".into());
        current.model = Some("model".into());
        let first_cause = rewrite_cause(Some(&first.row), &current);
        assert_eq!(first_cause, Some(Cause::CacheExpired));
        assert_eq!(first_cause, rewrite_cause(Some(&second.row), &current));
    }

    #[test]
    fn rewrite_evidence_pruning_is_bounded_at_two_history_sizes() {
        let mut costs = Vec::new();
        for live_rows in [1_000, 10_000] {
            let conn = conn();
            conn.execute("INSERT INTO step_digest VALUES (1,'shared')", [])
                .unwrap();
            for index in 0..live_rows + 3 {
                conn.execute("INSERT INTO step_evidence(harness,step_ref,runner,recorded_at_ms,facts_json,tools_key)
                    VALUES ('pi',?1,'runner',?2,'{}',1)",
                    params![index.to_string(), if index < 3 { 1 } else { EVIDENCE_WINDOW_MS }]).unwrap();
            }
            let counter = crate::store::VmCounter::start(&conn);
            crate::step_feed::append_changes(&conn, EVIDENCE_WINDOW_MS + 2, &[]).unwrap();
            let expired_cost = counter.steps();
            let kept: i64 = conn
                .query_row("SELECT COUNT(*) FROM step_evidence", [], |row| row.get(0))
                .unwrap();
            assert_eq!(kept, live_rows, "append must prune expired evidence");
            let counter = crate::store::VmCounter::start(&conn);
            crate::step_feed::append_changes(&conn, EVIDENCE_WINDOW_MS + 2, &[]).unwrap();
            costs.push((expired_cost, counter.steps()));
        }
        assert!(
            costs[1].0 < costs[0].0 + 200,
            "expired-row pruning scaled with history: {costs:?}"
        );
        assert!(
            costs[1].1 < costs[0].1 + 100,
            "unexpired evidence scanned the table: {costs:?}"
        );
    }

    #[test]
    fn rewrite_evidence_prunes_at_most_one_thousand_and_reclaims_shared_digests() {
        let conn = conn();
        conn.execute("INSERT INTO step_digest VALUES (1,'shared')", [])
            .unwrap();
        for index in 0..1_001 {
            conn.execute("INSERT INTO step_evidence(harness,step_ref,runner,recorded_at_ms,facts_json,tools_key,system_key)
                VALUES ('pi',?1,'runner',0,'{}',1,1)", [index.to_string()]).unwrap();
        }
        crate::step_feed::append_changes(&conn, EVIDENCE_WINDOW_MS + 1, &[]).unwrap();
        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_evidence", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kept, 1);
        let digests: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_digest", [], |row| row.get(0))
            .unwrap();
        assert_eq!(digests, 1, "the last reference keeps the interned digest");
        crate::step_feed::append_changes(&conn, EVIDENCE_WINDOW_MS + 1, &[]).unwrap();
        let digests: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_digest", [], |row| row.get(0))
            .unwrap();
        assert_eq!(digests, 0);
    }

    #[test]
    fn rewrite_cause_guards_refuse_updates_deletes_and_replace_by_rowid() {
        let conn = conn();
        conn.execute(
            "INSERT INTO step_cause(harness,step_ref,cause) VALUES ('pi','s','unexplained')",
            [],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA recursive_triggers=OFF;")
            .unwrap();
        for (sql, guard) in [
            ("UPDATE step_cause SET cause='view_changed'", "step_cause_no_update"),
            ("DELETE FROM step_cause", "step_cause_no_delete"),
            ("INSERT OR REPLACE INTO step_cause VALUES (1,'pi','s','view_changed')", "step_cause_insert_once"),
            ("REPLACE INTO step_cause(rowid,harness,step_ref,cause) VALUES (1,'pi','s','view_changed')", "step_cause_insert_once"),
        ] {
            assert!(conn.execute(sql, []).unwrap_err().to_string().contains(guard), "{sql}");
            assert_eq!(cause(&conn,"pi","s").unwrap(), Some(Cause::Unexplained));
        }
    }
}
