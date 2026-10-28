use super::*;
use crate::{ingest, report::SummaryMaintenance, test_support::conn};
use serde_json::{json, Value};

fn record(c: &mut Connection, i: u32, state: &str, time: Option<i64>) -> String {
    let producer = match i % 5 {
        0 => "broca",
        1 => "goose",
        2 => "codex",
        _ => "opencode",
    };
    let mut request:ingest::EventRecordRequest=serde_json::from_value(json!({
        "producer":producer,"producer_ref":format!("synthetic-{i}"),
        "session":if producer=="broca"&&i.is_multiple_of(10) {None}else{Some(if producer=="broca"{format!("broca-label-{}",i%3)}else if producer=="goose"{format!("ses_synthetic_goose_{i}")}else{format!("ses_synthetic_{}",i%7)})},
        "run_ref":if producer=="broca" {Some(format!("run-sid-Synthetic{}",i%3))}else{None},
        "provider":if i.is_multiple_of(2){"provider-a"}else{"provider-b"},"model":format!("model-{}",i%3),
        "occurred_at_ms":time,"charge_basis":if state=="covered"{"subscription_included"}else{"metered_api"},
        "measurements":(["input","output","reasoning","cache_read","cache_write"].into_iter().filter(|r|*r!="output"||!i.is_multiple_of(4)).map(|role|json!({"role":role,"unit":"token","kind":"billed","source":"fixture","quantity_units":i64::from(i%13),"quantity_exponent":0})).collect::<Vec<_>>())
    })).unwrap();
    if producer == "broca" && i.is_multiple_of(15) {
        request.run_ref = None;
        request.flow_ref = Some("flow-synthetic".into());
        request.work_class = Some("synthetic-batch".into());
    }
    let id = ingest::record_event(c, &request, 1).unwrap().event_id;
    if state != "pending" {
        charge(
            c,
            &id,
            state,
            1,
            None,
            if i.is_multiple_of(2) { "EUR" } else { "USD" },
        );
    }
    id
}
fn charge(
    c: &mut Connection,
    id: &str,
    state: &str,
    generation: i64,
    amount: Option<i64>,
    currency: &str,
) {
    let tx = c.transaction().unwrap();
    let currency = if state == "unpriced" {
        None
    } else {
        Some(currency)
    };
    tx.execute(
        "UPDATE usage_event SET pricing_generation=?2,currency=?3 WHERE event_id=?1",
        params![id, generation, currency],
    )
    .unwrap();
    let mut stmt=tx.prepare("SELECT m.role,m.unit,(SELECT h.charge_key FROM priced_charge h WHERE h.event_key=m.event_key AND h.role=m.role AND h.unit=m.unit AND NOT EXISTS(SELECT 1 FROM priced_charge child WHERE child.supersedes=h.charge_key)) FROM measurement m JOIN usage_event e USING(event_key) WHERE e.event_id=?1").unwrap();
    let roles = stmt
        .query_map([id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<i64>>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(stmt);
    for (role, unit, predecessor) in roles {
        let priced = state == "priced";
        tx.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms,supersedes) VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?17,?13,?14,?15,'pricing_pass',1,?16)",params![id,role,unit,generation,state,if state=="covered"{"subscription_included"}else{"metered_api"},i64::from(state=="covered"),currency,if priced{Some(amount.unwrap_or(17))}else if state=="covered"{Some(0)}else{None},if state=="unpriced"{Some("no_rate")}else{None},if priced{Some(1)}else{None},if priced{Some(9)}else{None},if priced{Some("fixture")}else{None},if priced{Some(1)}else{None},if priced{Some("{}")}else{None},predecessor,if priced{Some("none")}else{None}]).unwrap();
    }
    tx.commit().unwrap();
}
fn fill(c: &mut Connection) {
    let mut maintenance = SummaryMaintenance::default();
    c.execute(
        "UPDATE summary_audit_state SET next_due_ms=9223372036854775807",
        [],
    )
    .unwrap();
    for _ in 0..10000 {
        let dirty: i64 = c
            .query_row("SELECT count(*) FROM summary_dirty_hour", [], |r| r.get(0))
            .unwrap();
        if dirty == 0 {
            return;
        }
        let tx = c.transaction().unwrap();
        let ready = maintenance.read_chunk(&tx, 1).unwrap();
        tx.commit().unwrap();
        if ready {
            let tx = c.transaction().unwrap();
            assert!(!maintenance.publish(&tx, 1).unwrap().deferred);
            tx.commit().unwrap();
        }
    }
    panic!("bounded summary rebuild did not drain");
}

// Independent oracle: query each event's raw role sums and current charge rows,
// then add ordinary integers. It never reads stored summaries or calls the Runs list or detail code.
#[derive(Default)]
struct Figures {
    steps: u64,
    tokens: BTreeMap<String, u64>,
    priced: BTreeMap<String, i64>,
    covered: u64,
    unpriced: u64,
    pending: u64,
    reasons: BTreeMap<String, u64>,
}
impl Figures {
    fn add(&mut self, other: &Self) {
        self.steps += other.steps;
        for (k, v) in &other.tokens {
            *self.tokens.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &other.priced {
            *self.priced.entry(k.clone()).or_default() += v;
        }
        self.covered += other.covered;
        self.unpriced += other.unpriced;
        self.pending += other.pending;
        for (k, v) in &other.reasons {
            *self.reasons.entry(k.clone()).or_default() += v;
        }
    }
    fn cost(&self) -> Value {
        json!({"priced":self.priced,"covered_measurements":self.covered,"unpriced_measurements":self.unpriced,"pending_measurements":self.pending,"unpriced_by_reason":self.reasons})
    }
}
fn oracle(c: &Connection, q: &ListRequest) -> Value {
    type Group = (RunKey, Option<String>, Option<String>);
    let mut groups: BTreeMap<Group, (i64, i64, Figures)> = BTreeMap::new();
    let mut stmt=c.prepare("SELECT event_key,producer,session,run_ref,flow_ref,provider,model,occurred_at_ms FROM usage_event WHERE occurred_at_ms>=?1 AND occurred_at_ms<?2 AND (?3 IS NULL OR producer=?3)").unwrap();
    let mut rows = stmt.query(params![q.from_ms, q.to_ms, q.harness]).unwrap();
    while let Some(r) = rows.next().unwrap() {
        let id: i64 = r.get(0).unwrap();
        let producer: String = r.get(1).unwrap();
        let key = if producer == "broca" {
            if let Some(run_id) = r.get::<_, Option<String>>(3).unwrap() {
                RunKey::Run {
                    harness: producer,
                    run_id,
                }
            } else {
                RunKey::Flow {
                    harness: producer,
                    flow_id: r.get::<_, Option<String>>(4).unwrap().unwrap(),
                }
            }
        } else {
            RunKey::Session {
                harness: producer,
                session: r.get::<_, Option<String>>(2).unwrap().unwrap(),
            }
        };
        let time: i64 = r.get(7).unwrap();
        let mut f = Figures {
            steps: 1,
            ..Figures::default()
        };
        let mut s=c.prepare("SELECT role,SUM(quantity_units) FROM measurement WHERE event_key=?1 AND quantity_exponent=0 GROUP BY role").unwrap();
        for pair in s
            .query_map([id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))
            .unwrap()
        {
            let (k, v) = pair.unwrap();
            f.tokens.insert(k, v);
        }
        let reasoning = f.tokens.get("reasoning").copied().unwrap_or(0);
        if let Some(output) = f.tokens.get_mut("output") {
            *output += reasoning;
        }
        let mut s=c.prepare("SELECT h.outcome,h.currency,h.charge_nanos,h.unpriced_reason FROM measurement m LEFT JOIN priced_charge h ON h.event_key=m.event_key AND h.role=m.role AND h.unit=m.unit AND NOT EXISTS(SELECT 1 FROM priced_charge x WHERE x.supersedes=h.charge_key) WHERE m.event_key=?1").unwrap();
        let mut charges = s.query([id]).unwrap();
        while let Some(r) = charges.next().unwrap() {
            match r.get::<_, Option<String>>(0).unwrap().as_deref() {
                Some("priced") => {
                    *f.priced.entry(r.get(1).unwrap()).or_default() += r.get::<_, i64>(2).unwrap()
                }
                Some("covered") => f.covered += 1,
                Some("unpriced") => {
                    f.unpriced += 1;
                    *f.reasons.entry(r.get(3).unwrap()).or_default() += 1;
                }
                None => f.pending += 1,
                _ => panic!("unknown fixture state"),
            }
        }
        let group = groups
            .entry((key, r.get(5).unwrap(), r.get(6).unwrap()))
            .or_insert_with(|| (time, time, Figures::default()));
        group.0 = group.0.min(time);
        group.1 = group.1.max(time);
        group.2.add(&f);
    }
    let mut rows: BTreeMap<RunKey, (i64, i64, Figures, Vec<Value>)> = BTreeMap::new();
    for ((key, p, m), (first, last, f)) in groups {
        let row = rows
            .entry(key)
            .or_insert_with(|| (first, last, Figures::default(), vec![]));
        row.0 = row.0.min(first);
        row.1 = row.1.max(last);
        row.2.add(&f);
        row.3.push(
            json!({"provider":p,"model":m,"steps":f.steps,"tokens":f.tokens,"cost":f.cost()}),
        );
    }
    let mut all = Figures::default();
    let population = rows.len();
    let mut values = Vec::new();
    for (key, (first, last, f, models)) in rows {
        all.add(&f);
        let mut row = json!({"key":key,"first_at_ms":first,"last_at_ms":last,"steps":f.steps,"tokens":f.tokens,"cost":f.cost(),"models":models,"pricing_unit":match &key {RunKey::Session{harness,..}if harness=="goose"=>"session_total",RunKey::Session{..}=>"step",_=>"run_segment"}});
        if let RunKey::Run { run_id, .. } = &key {
            let label: Option<String> = c
                .query_row(
                    "SELECT max(session) FROM usage_event WHERE producer='broca' AND run_ref=?1",
                    [run_id],
                    |r| r.get(0),
                )
                .unwrap();
            if let Some(label) = label {
                row["session"] = json!(label);
            }
        }
        if matches!(key, RunKey::Flow { .. }) {
            row["work_class"] = json!("synthetic-batch");
        }
        values.push((serde_json::to_string(&key).unwrap(), row));
    }
    values.sort_by(|(ak, a), (bk, b)| {
        b["last_at_ms"]
            .as_i64()
            .cmp(&a["last_at_ms"].as_i64())
            .then(ak.cmp(bk))
    });
    let values: Vec<_> = values.into_iter().map(|(_, value)| value).collect();
    json!({"rows":values,"totals":{"rows":population,"steps":all.steps,"tokens":all.tokens,"cost":all.cost()}})
}

#[test]
fn runs_random_base_totals_are_byte_identical_filled_dirty_and_partial() {
    let mut c = conn();
    let mut random = 1234567_u64;
    for i in 0..240 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let state =
            ["priced", "covered", "unpriced", "pending"][usize::try_from(random % 4).unwrap()];
        record(
            &mut c,
            i,
            state,
            Some(i64::try_from(random % 20_000_000).unwrap() - 2 * HOUR_MS),
        );
    }
    let query = |from, to, h| ListRequest {
        from_ms: from,
        to_ms: to,
        harness: h,
        limit: Some(200),
        after: None,
    };
    for built in [false, true] {
        if built {
            fill(&mut c);
        }
        for q in [
            query(-HOUR_MS, 4 * HOUR_MS, None),
            query(-123, 12_345_678, None),
            query(-HOUR_MS, 4 * HOUR_MS, Some("opencode".into())),
        ] {
            assert_eq!(
                serde_json::to_vec(&serde_json::to_value(list(&c, &q).unwrap()).unwrap()).unwrap(),
                serde_json::to_vec(&oracle(&c, &q)).unwrap()
            );
        }
    }
    c.execute("INSERT OR IGNORE INTO summary_dirty_hour SELECT kind,hour_number FROM summary_hour WHERE hour_number%2=0",[]).unwrap();
    let q = query(-HOUR_MS, 4 * HOUR_MS, None);
    assert_eq!(
        serde_json::to_value(list(&c, &q).unwrap()).unwrap(),
        oracle(&c, &q)
    );
}

#[test]
fn runs_reprice_states_detail_and_audit_restore_current_money() {
    let mut c = conn();
    let id = record(&mut c, 7, "priced", Some(1));
    record(&mut c, 2, "covered", Some(2));
    record(&mut c, 9, "unpriced", Some(3));
    record(&mut c, 3, "pending", None);
    fill(&mut c);
    let q = ListRequest {
        from_ms: 0,
        to_ms: HOUR_MS,
        harness: None,
        limit: None,
        after: None,
    };
    let before = list(&c, &q).unwrap();
    assert_eq!(before.totals.cost.priced["USD"], 85);
    assert_eq!(before.totals.cost.covered_measurements, 5);
    assert_eq!(before.totals.cost.unpriced_measurements, 5);
    charge(&mut c, &id, "priced", 2, Some(31), "USD");
    assert_eq!(list(&c, &q).unwrap().totals.cost.priced["USD"], 155);
    fill(&mut c);
    assert_eq!(list(&c, &q).unwrap().totals.cost.priced["USD"], 155);
    let d = detail(
        &c,
        &DetailRequest {
            key: RunKey::Session {
                harness: "opencode".into(),
                session: "ses_synthetic_3".into(),
            },
            limit: None,
            after: None,
        },
    )
    .unwrap();
    assert_eq!(d.totals.cost.pending_measurements, 5);
    assert_eq!(d.items[0].occurred_at_ms, None);
    c.execute("UPDATE summary_run_cell SET steps=steps+1", [])
        .unwrap();
    c.execute(
        "UPDATE summary_audit_state SET next_due_ms=0,after_kind=NULL,after_hour=NULL",
        [],
    )
    .unwrap();
    let mut maintenance = SummaryMaintenance::default();
    let mut mismatch = false;
    for _ in 0..100 {
        let tx = c.transaction().unwrap();
        let ready = maintenance.read_chunk(&tx, 1).unwrap();
        tx.commit().unwrap();
        if ready {
            let tx = c.transaction().unwrap();
            let out = maintenance.publish(&tx, 1).unwrap();
            mismatch |= out.mismatch;
            tx.commit().unwrap();
            if mismatch {
                break;
            }
        }
    }
    assert!(mismatch);
    fill(&mut c);
    assert_eq!(
        serde_json::to_value(list(&c, &q).unwrap()).unwrap(),
        oracle(&c, &q)
    );
}

#[test]
fn runs_cursors_cover_every_row_and_item_once_and_bind_the_request() {
    let mut c = conn();
    for i in 0..70 {
        record(&mut c, i, "pending", Some(i64::from(i % 4)));
    }
    fill(&mut c);
    let mut q = ListRequest {
        from_ms: 0,
        to_ms: HOUR_MS,
        harness: None,
        limit: Some(3),
        after: None,
    };
    let mut keys = std::collections::BTreeSet::new();
    let mut totals = None;
    loop {
        let page = list(&c, &q).unwrap();
        if let Some(total) = &totals {
            assert_eq!(total, &page.totals);
        } else {
            totals = Some(page.totals.clone());
        }
        for row in page.rows {
            assert!(keys.insert(key_text(&row.key).unwrap()));
        }
        q.after = page.next;
        if q.after.is_none() {
            break;
        }
    }
    assert_eq!(u64::try_from(keys.len()).unwrap(), totals.unwrap().rows);
    q.after = list(
        &c,
        &ListRequest {
            after: None,
            ..q.clone()
        },
    )
    .unwrap()
    .next;
    q.to_ms -= 1;
    assert!(matches!(
        list(&c, &q),
        Err(Error::Refusal("invalid_request"))
    ));
    let key = RunKey::Session {
        harness: "opencode".into(),
        session: "ses_synthetic_3".into(),
    };
    let mut req = DetailRequest {
        key,
        limit: Some(1),
        after: None,
    };
    let mut refs = std::collections::BTreeSet::new();
    let whole_measurements:u64=c.query_row("SELECT count(*) FROM measurement m JOIN usage_event e USING(event_key) WHERE e.producer='opencode' AND e.session='ses_synthetic_3'",[],|r|r.get(0)).unwrap();
    assert!(whole_measurements > 5);
    loop {
        let page = detail(&c, &req).unwrap();
        assert_eq!(page.totals.cost.pending_measurements, whole_measurements);
        for item in page.items {
            assert!(refs.insert(item.step_ref.unwrap()));
        }
        req.after = page.next;
        if req.after.is_none() {
            break;
        }
    }
    let expected:i64=c.query_row("SELECT count(*) FROM usage_event WHERE producer='opencode' AND session='ses_synthetic_3'",[],|r|r.get(0)).unwrap();
    assert_eq!(i64::try_from(refs.len()).unwrap(), expected);
}

fn plan(c: &Connection, sql: &str) -> String {
    let mut statement = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    for parameter in 1..=statement.parameter_count() {
        statement
            .raw_bind_parameter(parameter, rusqlite::types::Null)
            .unwrap();
    }
    let mut rows = statement.raw_query();
    let mut text = String::new();
    while let Some(row) = rows.next().unwrap() {
        text.push_str(&row.get::<_, String>(3).unwrap());
        text.push('\n');
    }
    text
}
#[test]
fn runs_query_plans_seek_base_ranges_and_batched_keys_without_statistics() {
    let c = conn();
    let clean = "c.kind=0 AND c.hour_number BETWEEN 0 AND 720 AND c.hour_number NOT IN (12)";
    let p = plan(
        &c,
        &fallback_maxima_sql(clean, &["synthetic-a".into(), "synthetic-b".into()]),
    );
    assert!(
        p.contains("SEARCH c USING INDEX summary_run_key_time (run_key=?"),
        "{p}"
    );
    assert!(!p.contains("summary_run_activity"));
    for (filter, index) in [
        (
            "occurred_at_ms>=0 AND occurred_at_ms<3600000",
            "usage_event_summary_time",
        ),
        (
            "session=?1 AND producer=?2 ORDER BY occurred_at_ms,event_key LIMIT 201",
            "usage_event_session_time",
        ),
        (
            "run_ref=?1 AND producer=?2 ORDER BY occurred_at_ms,event_key LIMIT 201",
            "usage_event_run_time",
        ),
        (
            "flow_ref=?1 AND producer=?2 ORDER BY occurred_at_ms,event_key LIMIT 201",
            "usage_event_flow_time",
        ),
    ] {
        let p = plan(&c, &base_sql(filter, index));
        assert!(p.contains(index), "{p}");
        assert!(p.contains("SEARCH usage_event"), "{p}");
        assert!(!p.contains("SCAN usage_event"), "{p}");
        assert!(!p.contains("SCAN m"), "{p}");
    }
    let p=plan(&c,&format!("SELECT run_key FROM summary_run_cell c INDEXED BY summary_run_activity WHERE {clean} ORDER BY last_ms DESC,run_key LIMIT 51"));
    assert!(p.contains("summary_run_activity"));
    assert!(!p.contains("TEMP B-TREE"), "{p}");
    let p=plan(&c,&format!("SELECT run_key FROM summary_run_cell c INDEXED BY summary_run_key_time WHERE {clean} GROUP BY run_key"));
    assert!(p.contains("COVERING INDEX summary_run_key_time"));
    assert!(!p.contains("usage_event"));
}

fn edge(
    c: &mut Connection,
    reference: &str,
    session: &str,
    time: Option<i64>,
    quantities: Vec<Value>,
) {
    let request=serde_json::from_value(json!({"producer":"opencode","producer_ref":reference,"session":session,"occurred_at_ms":time,"charge_basis":"metered_api","measurements":quantities})).unwrap();
    ingest::record_event(c, &request, 1).unwrap();
}
fn quantity(role: &str, units: i64, exponent: i32) -> Value {
    json!({"role":role,"unit":"token","kind":"billed","source":"fixture","quantity_units":units,"quantity_exponent":exponent})
}
#[test]
fn runs_decimal_quantities_preserve_whole_tokens_and_do_not_round_fractions() {
    let mut c = conn();
    edge(
        &mut c,
        "decimal",
        "ses_decimal",
        Some(1),
        vec![
            quantity("input", 100, -1),
            quantity("output", 1, -1),
            quantity("reasoning", 2, 0),
        ],
    );
    let q = ListRequest {
        from_ms: 0,
        to_ms: HOUR_MS,
        harness: None,
        limit: None,
        after: None,
    };
    for clean in [false, true] {
        if clean {
            fill(&mut c);
        }
        let reply = list(&c, &q).unwrap();
        assert_eq!(reply.totals.tokens.input, Some(10));
        assert_eq!(reply.totals.tokens.output, None);
        assert_eq!(reply.totals.tokens.reasoning, Some(2));
        assert_eq!(reply.totals.cost.pending_measurements, 3);
    }
    let reply = detail(
        &c,
        &DetailRequest {
            key: RunKey::Session {
                harness: "opencode".into(),
                session: "ses_decimal".into(),
            },
            limit: None,
            after: None,
        },
    )
    .unwrap();
    assert!(reply.items[0]
        .measurements
        .iter()
        .any(|m| m.role == "output"
            && m.quantity
                == Some(Quantity {
                    units: 1,
                    exponent: -1
                })));
}
#[test]
fn runs_wide_counts_do_not_abort_shared_summary_publication() {
    let mut c = conn();
    for i in 0..2 {
        edge(
            &mut c,
            &format!("wide-{i}"),
            "ses_wide",
            Some(1),
            vec![quantity("input", i64::MAX, 0)],
        );
    }
    fill(&mut c);
    let reply = list(
        &c,
        &ListRequest {
            from_ms: 0,
            to_ms: HOUR_MS,
            harness: None,
            limit: None,
            after: None,
        },
    )
    .unwrap();
    assert_eq!(reply.totals.tokens.input, Some(u64::MAX - 1));
    let representation: String = c
        .query_row("SELECT typeof(input) FROM summary_run_cell", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(representation, "text");
}
#[test]
fn runs_emitted_long_cursors_empty_keys_and_undated_boundaries_round_trip() {
    let mut c = conn();
    let long = "x".repeat(5000);
    for (i, session, time) in [
        (0, "", None),
        (1, "", None),
        (2, "", Some(2)),
        (3, long.as_str(), Some(3)),
        (4, long.as_str(), Some(4)),
        (5, "z", Some(1)),
    ] {
        edge(&mut c, &format!("edge-{i}"), session, time, vec![]);
    }
    fill(&mut c);
    for session in ["", long.as_str()] {
        let mut q = DetailRequest {
            key: RunKey::Session {
                harness: "opencode".into(),
                session: session.into(),
            },
            limit: Some(1),
            after: None,
        };
        let mut times = vec![];
        loop {
            let reply = detail(&c, &q).unwrap();
            times.extend(reply.items.into_iter().map(|i| i.occurred_at_ms));
            q.after = reply.next;
            if q.after.is_none() {
                break;
            }
        }
        if session.is_empty() {
            assert_eq!(times, vec![None, None, Some(2)]);
        } else {
            assert_eq!(times, vec![Some(3), Some(4)]);
        }
    }
    let mut q = ListRequest {
        from_ms: 0,
        to_ms: HOUR_MS,
        harness: None,
        limit: Some(1),
        after: None,
    };
    let mut seen = 0;
    loop {
        let reply = list(&c, &q).unwrap();
        seen += reply.rows.len();
        q.after = reply.next;
        if q.after.is_none() {
            break;
        }
    }
    assert_eq!(seen, 3);
}
#[test]
fn runs_migration_34_requeues_existing_clean_hours_before_reuse() {
    let mut c = Connection::open_in_memory().unwrap();
    crate::store::initialize_connection(&c).unwrap();
    for migration in &crate::store::MIGRATIONS[..33] {
        c.execute_batch(migration.statements).unwrap();
    }
    edge(
        &mut c,
        "old",
        "ses_old",
        Some(1),
        vec![quantity("input", 12, 0)],
    );
    c.execute_batch("UPDATE summary_hour SET built_revision=revision,format_version=1; DELETE FROM summary_dirty_hour;").unwrap();
    c.execute_batch(crate::store::MIGRATIONS[33].statements)
        .unwrap();
    let dirty: i64 = c
        .query_row("SELECT count(*) FROM summary_dirty_hour", [], |r| r.get(0))
        .unwrap();
    assert!(dirty > 0);
    let q = ListRequest {
        from_ms: 0,
        to_ms: HOUR_MS,
        harness: None,
        limit: None,
        after: None,
    };
    let before = serde_json::to_value(list(&c, &q).unwrap()).unwrap();
    fill(&mut c);
    assert_eq!(before, serde_json::to_value(list(&c, &q).unwrap()).unwrap());
}

unsafe extern "C" fn trace_sql(
    _: u32,
    context: *mut std::ffi::c_void,
    statement: *mut std::ffi::c_void,
    _: *mut std::ffi::c_void,
) -> std::ffi::c_int {
    unsafe {
        let sql = rusqlite::ffi::sqlite3_sql(statement.cast());
        if !sql.is_null() {
            let sql = std::ffi::CStr::from_ptr(sql).to_string_lossy();
            if sql.starts_with("SELECT") || sql.starts_with("WITH") {
                (*context.cast::<Vec<String>>()).push(sql.into_owned());
            }
        }
    }
    0
}
#[test]
fn runs_actual_list_and_detail_queries_never_scan_base_events_or_measurements() {
    let mut c = conn();
    for i in 0..30 {
        record(&mut c, i, "pending", Some(i64::from(i) * 100));
    }
    fill(&mut c);
    let mut statements = Box::new(Vec::<String>::new());
    // Capture statements prepared by the real reads, not a parallel test query.
    unsafe {
        rusqlite::ffi::sqlite3_trace_v2(
            c.handle(),
            u32::try_from(rusqlite::ffi::SQLITE_TRACE_STMT).unwrap(),
            Some(trace_sql),
            std::ptr::from_mut(statements.as_mut()).cast(),
        );
    }
    for (from, to) in [(0, HOUR_MS), (1, HOUR_MS - 1)] {
        list(
            &c,
            &ListRequest {
                from_ms: from,
                to_ms: to,
                harness: None,
                limit: Some(2),
                after: None,
            },
        )
        .unwrap();
    }
    detail(
        &c,
        &DetailRequest {
            key: RunKey::Session {
                harness: "opencode".into(),
                session: "ses_synthetic_3".into(),
            },
            limit: Some(1),
            after: None,
        },
    )
    .unwrap();
    unsafe {
        rusqlite::ffi::sqlite3_trace_v2(c.handle(), 0, None, std::ptr::null_mut());
    }
    assert!(statements.len() > 12);
    for sql in statements.iter() {
        let p = plan(&c, sql);
        assert!(!p.contains("SCAN usage_event"), "{sql}\n{p}");
        assert!(!p.contains("SCAN m"), "{sql}\n{p}");
    }
}
