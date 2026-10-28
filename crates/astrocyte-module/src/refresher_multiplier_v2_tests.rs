fn bounded_page(cursor: i64, events: Vec<EventRecordRequest>, pending: bool) -> ExportPage {
    ExportPage {
        cursor: SourceCursor {
            lineage: "fake-opencode:1".into(),
            ordinal: cursor,
        },
        high_water: SourceCursor {
            lineage: "fake-opencode:1".into(),
            ordinal: 2,
        },
        events,
        set_aside: Vec::new(),
        pending_known: 0,
        pending_unknown: pending,
    }
}

struct TwoPageSource {
    late: EventRecordRequest,
}
impl SourceClient for TwoPageSource {
    fn pull(&self, since: Option<SourceCursor>, limit: u32) -> Result<ExportPage, PullError> {
        assert_eq!(limit, 1);
        match since {
            None => Ok(bounded_page(1, Vec::new(), true)),
            Some(cursor) if cursor.ordinal == 1 => {
                Ok(bounded_page(2, vec![self.late.clone()], false))
            }
            _ => Err(PullError::InvalidResponse("unexpected cursor".into())),
        }
    }
}

#[test]
fn v2_bounded_drain_waits_for_older_pending_usage() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    let mut wire = crate::wire_map::wire_fixture();
    wire["fact_id"] = json!("late-opencode");
    wire["provider"] = json!("openai");
    wire["model"] = json!("m");
    wire["occurred_at_ms"] = json!(DAY_START - DAY_MS);
    wire["usage"] = json!({"input_tokens": 1000});
    let mut late = crate::wire_map::map_fact(serde_json::from_value(wire).unwrap()).unwrap();
    late.producer = "opencode".into();
    late.producer_lineage = Some("fake-opencode:1".into());
    let source = TwoPageSource { late };
    let mut stage = MultiplierStage::new(Some(fusiform));
    assert_eq!(
        drain_once(&state, &source, "opencode", 1, NOW)
            .unwrap()
            .events_recorded,
        0
    );
    let runtime = || {
        state.with_conn(|conn| conn.query_row("SELECT last_success_ms,pending_unknown FROM source_runtime WHERE source_key='opencode'", [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))).map_err(|error| error.to_string())).unwrap()
    };
    assert_eq!(runtime(), (NOW, 1));
    stage.tick(&state, false, NOW);
    assert!(!observation_days(&state).contains(&DAY.to_string()));
    assert_eq!(
        drain_once(&state, &source, "opencode", 1, NOW + 60_000)
            .unwrap()
            .events_recorded,
        1
    );
    assert_eq!(runtime(), (NOW + 60_000, 0));
    stage.tick(&state, false, NOW + 60_000);
    assert_eq!(
        current_day_observations(&state),
        [("codex".into(), DAY.into(), 20_000_000)]
    );
}

#[test]
fn v2_served_plan_rows_keep_independent_eligible_heads_on_recompute() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    let mut document: serde_json::Value = serde_json::from_str(DESIRED).unwrap();
    document["subscription_multiplier"]["plan_price_overrides"]["openai/pro_100"] =
        json!({"minor_units": 10000, "exponent": 2, "currency": "USD", "period": "month"});
    document["subscription_multiplier"]["quota_providers"]["codex"]["accounts"]["acct2"] =
        json!({"plan": "openai/pro_100", "price_override": null});
    publish_document(&state, &directory, &document.to_string());
    let fusiform = FakeFusiform::new(false);
    install_catalog(&state, &fusiform);
    seed_usage(&state);
    state.ordered_write(|tx| {
        tx.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at) SELECT provider,'acct2',pool_key,fetched_at_instant,used_percent,window_minutes,resets_at FROM window_sample", []).map_err(|error| error.to_string())?;
        // Seed account-attributed events initially, without correcting immutable
        // usage rows. The original unattributed fixture remains disclosed.
        for (prefix, account) in [("a", "acct"), ("b", "acct2")] {
          for index in 0..40 {
            let id = format!("{prefix}-{index}");
            tx.execute("INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref) VALUES (?1,'opencode','lineage',?1,?2,?2,'openai','m','unknown',?3)", params![id, DAY_START - 2 * DAY_MS, account]).map_err(|error| error.to_string())?;
            tx.execute("INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'input','token','billed','usage.input',1000,0)", [id]).map_err(|error| error.to_string())?;
          }
        }
        Ok(((), true))
    }).unwrap();
    let first = recompute(&state, &fusiform, DAY, Some("codex"), NOW).unwrap();
    let second = recompute(&state, &fusiform, DAY, Some("codex"), NOW + 1).unwrap();
    assert_eq!(first["recomputed"].as_array().unwrap().len(), 2);
    for row in second["recomputed"].as_array().unwrap() {
        let old = first["recomputed"]
            .as_array()
            .unwrap()
            .iter()
            .find(|old| old["plan"] == row["plan"])
            .unwrap();
        assert_eq!(row["supersedes"], old["observation_id"]);
    }
    let report = crate::ops::dispatch(
        &state,
        crate::ops::OP_MULTIPLIER_REPORT,
        json!({"from_ms": DAY_START - 1, "to_ms": DAY_START + 1}),
    )
    .unwrap();
    assert_eq!(report["series"].as_array().unwrap().len(), 2);
    assert_eq!(report["latest"].as_array().unwrap().len(), 2);
    for row in report["series"].as_array().unwrap() {
        assert_eq!(row["api_nanos"], 400_000_000);
        assert_eq!(row["accounts_included_count"], 1);
        assert_eq!(row["routing_eligible"], true);
        assert!(row["eligibility_reasons"].as_array().unwrap().is_empty());
    }
}
