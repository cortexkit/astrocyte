// Included in broca_source::tests so the fixtures drive the production drain.
const MODEL_STEP_GOLDEN: &str =
    include_str!("../tests/vectors/broca-model-step/model_step_fact.json");
const MODEL_STEP_NOW: i64 = 1_790_000_100_000;

fn model_step_segment(id: &str, step: i64) -> Value {
    let mut value: Value = serde_json::from_str(MODEL_STEP_GOLDEN).unwrap();
    value["fact_id"] = Value::from(id);
    value["step_id"] = Value::from(step);
    value
}

fn model_step_store() -> (TempDir, PathBuf) {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("run-index.db");
    create_store(&path, "model-steps", 1);
    (temp, path)
}

#[test]
fn broca_model_step_golden_is_pinned() {
    use sha2::{Digest, Sha256};
    assert_eq!(
        format!("{:x}", Sha256::digest(MODEL_STEP_GOLDEN.as_bytes())),
        "fb903481bb6b8ec851524807535ed8bc00e0e6cc58bb4f412d589759b1aa0668"
    );
}

#[test]
fn broca_model_step_golden_records_every_field_without_spend() {
    let (_temp, path) = model_step_store();
    let golden: Value = serde_json::from_str(MODEL_STEP_GOLDEN).unwrap();
    let id = golden["fact_id"].as_str().unwrap();
    add_segment(&path, 1, id, MODEL_STEP_GOLDEN);
    // The SQL run_id is deliberately empty: identity comes only from the JSON.
    Connection::open(&path)
        .unwrap()
        .execute("UPDATE export_facts SET run_id=''", [])
        .unwrap();
    let state = test_state();
    let before = [
        count(&state, "usage_event"),
        count(&state, "measurement"),
        count(&state, "priced_charge"),
    ];
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW,
    )
    .unwrap();
    assert_eq!(count(&state, "broca_step"), 1);
    let columns = "step_key,run_id,step_id,fact_id,lineage_id,session_ref,project_root,harness,session,provider,model,variant,wire_family,input_tokens,cached_input_tokens,cache_write_tokens,output_tokens,reasoning_tokens,finish_reason,attempts,retried,request_body_len,request_body_sha256,started_at_ms,first_content_at_ms,finished_at_ms,context_limit,cache_policy,agent_id,agent_run_id,recorded_at_ms";
    let stored: Value = state
        .with_conn(|conn| {
            let pairs = columns
                .split(',')
                .map(|column| format!("'{column}',{column}"))
                .collect::<Vec<_>>()
                .join(",");
            let json: String = conn
                .query_row(
                    &format!("SELECT json_object({pairs}) FROM broca_step"),
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            serde_json::from_str(&json).map_err(|e| e.to_string())
        })
        .unwrap();
    assert_eq!(
        stored,
        serde_json::json!({
            "step_key":1, "run_id":"run-golden", "step_id":1,
            "fact_id":"ddde94b3c7b7d2194a8e9b52ba779f86705906e1978a2dfa394dc9240fa66f37",
            "lineage_id":"5e1f0051-7a9c-4d1e-8b0a-6f332c9041d7", "session_ref":"1284c41ebcc6c47d4ba65a74dadfbf93",
            "project_root":"/synthetic/golden-project", "harness":"golden-harness", "session":"golden-session",
            "provider":"anthropic", "model":"claude-synthetic-1", "variant":"high", "wire_family":"anthropic_messages",
            "input_tokens":3, "cached_input_tokens":41250, "cache_write_tokens":1812, "output_tokens":640, "reasoning_tokens":128,
            "finish_reason":"stop", "attempts":2, "retried":1, "request_body_len":48214,
            "request_body_sha256":"000000000000000000000000000000000000000000000000000000005eed0001",
            "started_at_ms":1790000000200_i64, "first_content_at_ms":1790000005150_i64, "finished_at_ms":1790000018900_i64,
            "context_limit":200000, "cache_policy":"{\"class\":\"markers\",\"max_breakpoints\":4,\"ttl\":\"1h\"}",
            "agent_id":"agent-synthetic", "agent_run_id":"agent-run-synthetic", "recorded_at_ms":MODEL_STEP_NOW
        })
    );
    assert_eq!(
        [
            count(&state, "usage_event"),
            count(&state, "measurement"),
            count(&state, "priced_charge")
        ],
        before
    );
    assert_eq!(runtime(&state).unwrap().1, Some(1));
    assert!(set_aside_rows(&state).is_empty());
}

#[test]
fn broca_model_step_mixed_and_set_aside_rows_keep_progress() {
    let (_temp, path) = model_step_store();
    add_fact(&path, 1, "session-spend");
    add_segment(
        &path,
        2,
        "batch-spend",
        &system_one_segment("batch-spend", |_| {}),
    );
    let mut unknown = model_step_segment("unknown-usage", 1);
    unknown["usage"]["future_tokens"] = Value::from(9);
    add_segment(&path, 3, "unknown-usage", &unknown.to_string());
    let mut missing = model_step_segment("missing-step", 2);
    missing.as_object_mut().unwrap().remove("step_id");
    add_segment(&path, 4, "missing-step", &missing.to_string());
    let mut good = model_step_segment("good-step", 3);
    good["future_top_level"] = Value::from("ignored");
    add_segment(&path, 5, "good-step", &good.to_string());
    let state = test_state();
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW,
    )
    .unwrap();
    assert_eq!(count(&state, "usage_event"), 2);
    assert_eq!(count(&state, "measurement"), 6);
    assert_eq!(count(&state, "broca_step"), 1);
    assert_eq!(runtime(&state).unwrap().1, Some(5));
    let reasons = set_aside_rows(&state);
    assert_eq!(reasons.len(), 2, "{reasons:?}");
    assert!(
        reasons.iter().any(|(_, r)| r.contains("future_tokens")),
        "{reasons:?}"
    );
    assert!(
        reasons.iter().any(|(_, r)| r.contains("step_id")),
        "{reasons:?}"
    );

    // A rebuilt export store re-reads the same identities, first exactly and
    // then with a changed count. Only the contradiction becomes evidence.
    Connection::open(&path)
        .unwrap()
        .execute("DELETE FROM export_facts WHERE export_seq IN (3,4)", [])
        .unwrap();
    Connection::open(&path)
        .unwrap()
        .execute("UPDATE export_meta SET source_epoch=2", [])
        .unwrap();
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW + 1,
    )
    .unwrap();
    assert_eq!(count(&state, "broca_step"), 1);
    assert_eq!(set_aside_rows(&state).len(), 2);
    good["usage"]["input_tokens"] = Value::from(999);
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE export_facts SET segment_json=?1 WHERE fact_id='good-step'",
        [good.to_string()],
    )
    .unwrap();
    conn.execute("UPDATE export_meta SET source_epoch=3", [])
        .unwrap();
    drop(conn);
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW + 2,
    )
    .unwrap();
    assert_eq!(count(&state, "broca_step WHERE input_tokens=3"), 1);
    assert_eq!(runtime(&state).unwrap().1, Some(5));
    assert!(set_aside_rows(&state)
        .iter()
        .any(|(_, r)| r.contains("contradicting model_step")));
    assert_eq!(count(&state, "usage_event"), 2);
}

#[test]
fn broca_model_step_requires_each_identity_member_and_keeps_absence() {
    let (_temp, path) = model_step_store();
    let required = [
        "fact_id",
        "run_id",
        "step_id",
        "session",
        "provider",
        "model",
        "finished_at_ms",
    ];
    for (index, member) in required.iter().enumerate() {
        let id = format!("missing-{member}");
        let mut segment = model_step_segment(&id, i64::try_from(index).unwrap());
        segment.as_object_mut().unwrap().remove(*member);
        add_segment(
            &path,
            i64::try_from(index + 1).unwrap(),
            &id,
            &segment.to_string(),
        );
    }
    let minimal = serde_json::json!({"kind":"model_step", "fact_id":"minimal", "run_id":"minimal", "step_id":1,
        "session":{"project_root":"/p", "harness":"test", "session":"s"}, "provider":"p", "model":"m", "finished_at_ms":MODEL_STEP_NOW});
    add_segment(&path, 8, "minimal", &minimal.to_string());
    let state = test_state();
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW,
    )
    .unwrap();
    assert_eq!(count(&state, "broca_step WHERE input_tokens IS NULL AND cached_input_tokens IS NULL AND cache_write_tokens IS NULL AND output_tokens IS NULL AND reasoning_tokens IS NULL AND started_at_ms IS NULL AND first_content_at_ms IS NULL AND agent_id IS NULL AND agent_run_id IS NULL"), 1);
    let reasons = set_aside_rows(&state);
    assert_eq!(reasons.len(), required.len(), "{reasons:?}");
    for member in required {
        assert!(
            reasons
                .iter()
                .any(|(_, reason)| reason.contains(&format!("missing field `{member}`"))),
            "{member}: {reasons:?}"
        );
    }
    assert_eq!(runtime(&state).unwrap().1, Some(8));
}

#[test]
fn broca_model_step_insert_once_and_update_guards_preserve_rows() {
    let (_temp, path) = model_step_store();
    add_segment(
        &path,
        1,
        "guard",
        &model_step_segment("guard", 1).to_string(),
    );
    let state = test_state();
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW,
    )
    .unwrap();
    state.with_conn(|conn| {
        assert!(conn.execute("UPDATE broca_step SET model='changed'", []).is_err());
        for identity in ["1,'run-golden',1,'guard'", "2,'other',2,'guard'", "3,'run-golden',1,'other'"] {
            let result = conn.execute(&format!("INSERT OR REPLACE INTO broca_step(step_key,run_id,step_id,fact_id,project_root,harness,session,provider,model,finished_at_ms,recorded_at_ms) VALUES({identity},'/p','h','s','p','changed',1,1)"), []);
            assert!(result.is_err(), "replacement succeeded for {identity}");
        }
        Ok(())
    }).unwrap();
    assert_eq!(
        count(&state, "broca_step WHERE model='claude-synthetic-1'"),
        1
    );
}

fn add_step_page(path: &Path, rows: i64, finished_at_ms: i64) {
    let mut conn = Connection::open(path).unwrap();
    let tx = conn.transaction().unwrap();
    {
        let mut insert = tx.prepare("INSERT INTO export_facts(export_seq,fact_id,run_id,segment_json) VALUES(?1,?2,'',?3)").unwrap();
        for step in 1..=rows {
            let id = format!("page-{step}");
            let mut value = model_step_segment(&id, step);
            value["finished_at_ms"] = Value::from(finished_at_ms);
            insert
                .execute(rusqlite::params![step, id, value.to_string()])
                .unwrap();
        }
    }
    tx.execute("UPDATE export_meta SET high_water=?1", [rows])
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn broca_model_step_retention_is_bounded_and_preserves_new_rows() {
    use astrocyte_core::broca_step::BROCA_STEP_RETENTION_MS;
    let (_temp, path) = model_step_store();
    let cutoff = MODEL_STEP_NOW - BROCA_STEP_RETENTION_MS;
    add_step_page(&path, 1001, cutoff - 1);
    let mut fresh = model_step_segment("fresh", 1002);
    fresh["finished_at_ms"] = Value::from(cutoff);
    add_segment(&path, 1002, "fresh", &fresh.to_string());
    let state = test_state();
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW,
    )
    .unwrap();
    for remaining in [502, 2, 1] {
        let before = count(&state, "broca_step");
        let removed = crate::refresher::retain_broca_steps(&state, MODEL_STEP_NOW).unwrap();
        assert!(removed <= 500, "maintenance removed {removed}");
        assert_eq!(count(&state, "broca_step"), remaining);
        assert_eq!(
            before - count(&state, "broca_step"),
            i64::try_from(removed).unwrap()
        );
        assert_eq!(count(&state, "broca_step WHERE fact_id='fresh'"), 1);
    }
    assert_eq!(
        crate::refresher::retain_broca_steps(&state, MODEL_STEP_NOW).unwrap(),
        0
    );
}

#[test]
fn broca_model_step_cache_ops_match_memory_store_and_shared_cursors() {
    use astrocyte_core::cache_diagnostics::{self as cache, CacheStepsRequest, Verdict};
    let (_temp, path) = model_step_store();
    let state = test_state();
    state.set_recent_steps_now_for_test(MODEL_STEP_NOW);
    let mut last = 0;
    for (step, read, len, verdict, size) in [
        (1, 41250, 48214, Verdict::ColdStart, None),
        (2, 20000, 30000, Verdict::Bust, Some("smaller")),
        (3, 0, 35000, Verdict::FullBust, Some("not_smaller")),
    ] {
        let id = format!("cache-{step}");
        let mut value = model_step_segment(&id, step);
        value["usage"]["cached_input_tokens"] = Value::from(read);
        value["request_body_len"] = Value::from(len);
        value["finished_at_ms"] = Value::from(MODEL_STEP_NOW - 100 + step);
        add_segment(&path, step, &id, &value.to_string());
        drain_once(
            &state,
            &source(&path),
            BROCA_SOURCE_KEY,
            PAGE_LIMIT,
            MODEL_STEP_NOW,
        )
        .unwrap();
        let query = CacheStepsRequest {
            harness: "broca".into(),
            session: "golden-session".into(),
            limit: Some(1),
            after_cursor: None,
        }
        .validate()
        .unwrap();
        let stored = state
            .with_conn(|conn| cache::steps_from_store(conn, &query).map_err(|e| e.to_string()))
            .unwrap();
        if step == 1 {
            assert!(state.recent_steps_answer(&query).is_none());
            assert_eq!(
                crate::ops::dispatch(
                    &state,
                    crate::ops::OP_CACHE_STEPS,
                    serde_json::json!({"harness":"broca", "session":"golden-session", "limit":1})
                )
                .unwrap(),
                serde_json::json!(stored)
            );
            state.rebuild_recent_steps();
        }
        let memory = state
            .recent_steps_answer(&query)
            .expect("BROCA session is in memory");
        assert_eq!(memory, stored);
        assert_eq!(
            crate::ops::dispatch(
                &state,
                crate::ops::OP_CACHE_STEPS,
                serde_json::json!({"harness":"broca", "session":"golden-session", "limit":1})
            )
            .unwrap(),
            serde_json::json!(stored)
        );
        assert_eq!(stored.steps[0].step_ref, format!("run-golden#{step}"));
        assert_eq!(stored.steps[0].verdict, verdict);
        assert_eq!(
            stored.steps[0].request_body_len,
            Some(u64::try_from(len).unwrap())
        );
        assert_eq!(stored.steps[0].request_size, size);
        assert_eq!(stored.steps[0].output, Some(512));
        assert_eq!(stored.steps[0].reasoning, Some(128));
        assert!(stored.steps[0].cursor > last);
        last = stored.steps[0].cursor;
        state.rebuild_recent_steps();
        assert_eq!(state.recent_steps_answer(&query).unwrap(), stored);
    }
    let sessions = state
        .with_conn(|conn| {
            cache::cache_sessions(
                conn,
                cache::CacheSessionsRequest {
                    since_ms: None,
                    harness: Some("broca".into()),
                    limit: None,
                },
                MODEL_STEP_NOW,
            )
            .map_err(|e| format!("{e:?}"))
        })
        .unwrap();
    assert_eq!(sessions.sessions.len(), 1);
    assert_eq!(sessions.sessions[0].session, "golden-session");
    assert_eq!(sessions.sessions[0].harness, "broca");
    assert_eq!(sessions.sessions[0].steps, 3);
    assert_eq!(sessions.sessions[0].busts, 2);
    assert_eq!(sessions.sessions[0].context_limit, Some(200000));
    // A ledger event after BROCA must receive a greater cursor even though
    // its independent event_key starts at one.
    let mut event =
        crate::wire_map::map_fact(serde_json::from_value(wire_fixture()).unwrap()).unwrap();
    event.producer = "opencode".into();
    event.session = Some("cursor-test".into());
    event.occurred_at_ms = Some(MODEL_STEP_NOW);
    state
        .ordered_write(|tx| {
            astrocyte_core::ingest::record_event_in_tx(
                tx,
                &event,
                MODEL_STEP_NOW,
                astrocyte_core::ingest::DeliveryPath::Read,
            )
            .map_err(|e| format!("{e:?}"))?;
            Ok(((), true))
        })
        .unwrap();
    let query = CacheStepsRequest {
        harness: "opencode".into(),
        session: "cursor-test".into(),
        limit: None,
        after_cursor: Some(last),
    }
    .validate()
    .unwrap();
    let after = state
        .with_conn(|conn| cache::steps_from_store(conn, &query).map_err(|e| e.to_string()))
        .unwrap();
    assert_eq!(after.steps.len(), 1);
    assert!(after.steps[0].cursor > last);
    assert_eq!(state.recent_steps_answer(&query).unwrap(), after);
    // Late harness detail is still addressed by event_key, not its now
    // different shared cursor.
    event.step_detail.finish = Some("late-finish".into());
    state
        .ordered_write(|tx| {
            astrocyte_core::ingest::record_event_in_tx(
                tx,
                &event,
                MODEL_STEP_NOW,
                astrocyte_core::ingest::DeliveryPath::Read,
            )
            .map_err(|e| format!("{e:?}"))?;
            Ok(((), true))
        })
        .unwrap();
    let detailed = state
        .with_conn(|conn| cache::steps_from_store(conn, &query).map_err(|e| e.to_string()))
        .unwrap();
    assert_eq!(detailed.steps[0].finish.as_deref(), Some("late-finish"));
    assert_eq!(state.recent_steps_answer(&query).unwrap(), detailed);
    // Expiry invalidates the cached lead too, not only the stored rows.
    crate::refresher::retain_broca_steps(
        &state,
        MODEL_STEP_NOW + astrocyte_core::broca_step::BROCA_STEP_RETENTION_MS,
    )
    .unwrap();
    let query = CacheStepsRequest {
        harness: "broca".into(),
        session: "golden-session".into(),
        limit: None,
        after_cursor: None,
    }
    .validate()
    .unwrap();
    assert!(state.recent_steps_answer(&query).is_none());
    assert!(state
        .with_conn(|conn| cache::steps_from_store(conn, &query).map_err(|e| e.to_string()))
        .unwrap()
        .steps
        .is_empty());
    let high = state
        .with_conn(|conn| cache::last_event_key(conn).map_err(|e| e.to_string()))
        .unwrap();
    assert!(high > last);
}

#[test]
fn broca_model_step_backlog_page_reports_time_and_vm_steps() {
    let (_temp, path) = model_step_store();
    add_step_page(&path, 10000, MODEL_STEP_NOW);
    let state = test_state();
    state.set_recent_steps_now_for_test(MODEL_STEP_NOW);
    state.rebuild_recent_steps();
    let started = std::time::Instant::now();
    drain_once(
        &state,
        &source(&path),
        BROCA_SOURCE_KEY,
        PAGE_LIMIT,
        MODEL_STEP_NOW,
    )
    .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(count(&state, "broca_step"), 10000);
    assert_eq!(count(&state, "usage_event"), 0);
    assert_eq!(runtime(&state).unwrap().1, Some(10000));
    let vm_steps = state.with_conn(|conn| {
        let columns = "run_id,step_id,fact_id,lineage_id,session_ref,project_root,harness,session,provider,model,variant,wire_family,input_tokens,cached_input_tokens,cache_write_tokens,output_tokens,reasoning_tokens,finish_reason,attempts,retried,request_body_len,request_body_sha256,started_at_ms,first_content_at_ms,finished_at_ms,context_limit,cache_policy,agent_id,agent_run_id";
        let placeholders = vec!["?"; 30].join(",");
        let insert = format!("INSERT INTO broca_step({columns},recorded_at_ms) VALUES ({placeholders})");
        let exists = "SELECT EXISTS(SELECT 1 FROM broca_step WHERE fact_id=?1 OR (run_id=?2 AND step_id=?3))";
        let mut total = 0;
        for sql in [&insert, exists] {
            total += i64::from(conn.prepare_cached(sql).map_err(|e| e.to_string())?.get_status(rusqlite::StatementStatus::VmStep));
        }
        Ok(total)
    }).unwrap();
    assert!(
        vm_steps > 10000,
        "VM instrumentation did not see the page: {vm_steps}"
    );
    println!("BROCA backlog: rows=10000 elapsed_ms={} writer_insert_and_identity_vm_steps={vm_steps} (includes triggers; excludes read/projection)", elapsed.as_millis());
}

#[test]
fn broca_model_step_cursors_survive_reopen_and_expiry_without_reuse() {
    use astrocyte_core::cache_diagnostics as cache;
    let ledger = TempDir::new().unwrap();
    let (_temp, path) = model_step_store();
    let now = crate::state::now_ms();
    let state = crate::state::leased_test_state(&ledger);
    let mut step = model_step_segment("restart-1", 1);
    step["finished_at_ms"] = Value::from(now);
    add_segment(&path, 1, "restart-1", &step.to_string());
    drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, now).unwrap();
    let high = state
        .with_conn(|conn| cache::last_event_key(conn).map_err(|e| e.to_string()))
        .unwrap();
    assert!(high > 0);
    drop(state);
    let state = crate::state::leased_test_state(&ledger);
    assert_eq!(
        state
            .with_conn(|conn| cache::last_event_key(conn).map_err(|e| e.to_string()))
            .unwrap(),
        high
    );
    drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, now).unwrap();
    assert_eq!(count(&state, "broca_step"), 1);
    assert_eq!(runtime(&state).unwrap().1, Some(1));
    crate::refresher::retain_broca_steps(
        &state,
        now + astrocyte_core::broca_step::BROCA_STEP_RETENTION_MS + 1,
    )
    .unwrap();
    assert_eq!(count(&state, "cache_step_sequence"), 0);
    drop(state);
    let state = crate::state::leased_test_state(&ledger);
    state.set_recent_steps_now_for_test(now);
    state.rebuild_recent_steps();
    assert_eq!(
        state
            .with_conn(|conn| cache::last_event_key(conn).map_err(|e| e.to_string()))
            .unwrap(),
        high
    );
    step["fact_id"] = Value::from("restart-2");
    step["step_id"] = Value::from(2);
    add_segment(&path, 2, "restart-2", &step.to_string());
    drain_once(&state, &source(&path), BROCA_SOURCE_KEY, PAGE_LIMIT, now).unwrap();
    let query = cache::CacheStepsRequest {
        harness: "broca".into(),
        session: "golden-session".into(),
        limit: None,
        after_cursor: Some(high),
    }
    .validate()
    .unwrap();
    let stored = state
        .with_conn(|conn| cache::steps_from_store(conn, &query).map_err(|e| e.to_string()))
        .unwrap();
    assert_eq!(stored.steps.len(), 1);
    assert!(stored.steps[0].cursor > high);
    assert_eq!(stored.steps[0].step_ref, "run-golden#2");
    assert_eq!(state.recent_steps_answer(&query).unwrap(), stored);
    assert_eq!(
        crate::ops::dispatch(
            &state,
            crate::ops::OP_CACHE_STEPS,
            serde_json::json!({"harness":"broca","session":"golden-session","after_cursor":high})
        )
        .unwrap(),
        serde_json::json!(stored)
    );
    let sessions = crate::ops::dispatch(
        &state,
        crate::ops::OP_CACHE_SESSIONS,
        serde_json::json!({"harness":"broca"}),
    )
    .unwrap();
    assert_eq!(sessions["sessions"][0]["harness"], "broca");
    assert_eq!(sessions["sessions"][0]["session"], "golden-session");
    assert_eq!(sessions["sessions"][0]["steps"], 1);
    assert_eq!(count(&state, "usage_event"), 0);
}
