struct TimelineFusiform {
    store: std::path::PathBuf,
    version: std::sync::atomic::AtomicI64,
    requests: std::sync::Mutex<Vec<(String, String)>>,
    /// How long each history read takes, standing in for a slow fusiform.
    delay: Duration,
    /// The deadline each history read was given.
    timeouts: std::sync::Mutex<Vec<Duration>>,
    /// Models whose history read fails, standing in for the catalog timing out.
    failing: Vec<String>,
}

impl TimelineFusiform {
    fn outside_write_transaction(&self) {
        let conn = rusqlite::Connection::open(&self.store).unwrap();
        conn.busy_timeout(Duration::ZERO).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE; ROLLBACK;")
            .expect("catalog I/O must not hold the store writer");
    }
}

impl CatalogConnection for TimelineFusiform {
    fn catalog_status(
        &self,
        _: fusiform_protocol::StatusRequest,
    ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
        self.outside_write_transaction();
        Ok(serde_json::from_value(
            json!({"source":"synthetic","catalog_version":self.version.load(Ordering::SeqCst),
            "model_count":3,"era_count":3,"recent_polls":[],"history_begins_at_ms":1}),
        )
        .unwrap())
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        self.outside_write_transaction();
        assert!(
            request.at_ms.is_none(),
            "identity reads are current, not dated rate snapshots"
        );
        let provider = request.provider_id.unwrap();
        let model = request.model_id.unwrap();
        let reference = if provider == "openai" && model == "m" {
            let target = if self.version.load(Ordering::SeqCst) < 3 {
                "target-a"
            } else {
                "target-b"
            };
            json!([{"model":"openai/m","target_provider_id":"openai","target_model_id":target,"source_ref":"synthetic"}])
        } else {
            json!([])
        };
        Ok(serde_json::from_value(
            json!({"source":"synthetic","resolved_at_ms":NOW,"catalog_version":1,
            "models":{},"withheld":[],"uncertain":[],"fact_overrides":[],"aliased":reference}),
        )
        .unwrap())
    }

    fn catalog_history(
        &self,
        request: fusiform_protocol::HistoryRequest,
        timeout: Duration,
    ) -> Result<fusiform_protocol::HistoryResponse, CatalogCallError> {
        self.outside_write_transaction();
        assert!(timeout <= RATE_FETCH_MODEL_BUDGET && !timeout.is_zero());
        self.timeouts.lock().unwrap().push(timeout);
        std::thread::sleep(self.delay);
        assert_eq!(request.served, Some(true));
        assert_eq!(request.fact_prefix.as_deref(), Some("rate."));
        self.requests
            .lock()
            .unwrap()
            .push((request.provider_id.clone(), request.model_id.clone()));
        if self.failing.contains(&request.model_id) {
            return Err(CatalogCallError {
                code: "unavailable".into(),
                message: "fusiform exchange: response timeout".into(),
            });
        }
        let units = self.version.load(Ordering::SeqCst) * 1_000_000_000;
        let segments = if request.model_id.starts_with("target-") {
            json!({"rate.input":[{"from_ms":1,"until_ms":null,"state":"value","value":{"state":"priced","currency":"USD","units":units,"exponent":9,"unit_provenance":{"kind":"stated"}}}]})
        } else {
            json!({})
        };
        Ok(serde_json::from_value(json!({"source":"synthetic","provider_id":request.provider_id,"model_id":request.model_id,
            "fact_prefix":"rate.","served_by_key":segments,"eras":[]})).unwrap())
    }
}

#[test]
fn rate_era_refresher_rereads_alias_and_twin_timelines_on_catalog_advance() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    seed_usage(&state);
    state.ordered_write(|tx| {
        tx.execute("INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,provider,model,charge_basis) VALUES('twin','opencode','twin',0,'openai-codex','m','unknown')",[]).map_err(|error|error.to_string())?;
        Ok(((),true))
    }).unwrap();
    let peer = Arc::new(TimelineFusiform {
        store: directory.path().join("ledger.db"),
        version: std::sync::atomic::AtomicI64::new(1),
        requests: Default::default(),
        delay: Duration::ZERO,
        timeouts: Default::default(),
        failing: Vec::new(),
    });
    state
        .install_catalog(Some(CatalogClient::connect(peer.clone()).unwrap()))
        .unwrap();
    let expired = state.catalog_client().unwrap().with_budget(Duration::ZERO);
    assert!(expired.history("openai", "m").is_err());
    assert!(
        peer.requests.lock().unwrap().is_empty(),
        "an expired budget sends no request"
    );
    let resolved = || {
        state
            .with_read_op(|conn| {
                astrocyte_core::rate_era::resolve(conn, "openai", "m", NOW)
                    .map_err(|error| (error.code().into(), error.message()))
            })
            .unwrap()
    };
    assert!(resolved().is_none());
    let mut stage = MultiplierStage::new(None);
    let mut refresh = |new_target| {
        const MAX_BETWEEN_TICK_PASSES: usize = 1;
        stage.rates_pending = rate_era_tick(&state, &mut stage.rate_backoff).unwrap();
        if new_target {
            assert!(stage.rates_behind(), "a new target needs a between-tick pass");
        }
        for _ in 0..MAX_BETWEEN_TICK_PASSES {
            stage.fill_rates(&state);
        }
        assert!(
            !stage.rates_behind(),
            "alias and twin timelines must resolve within {MAX_BETWEEN_TICK_PASSES} between-tick pass"
        );
    };
    refresh(true);
    let first = resolved().unwrap();
    assert!(
        matches!(&first,multiplier::ModelResolution::Rates(rates) if rates.base["input"]==multiplier::RateState::Priced {units_per_million:1_000_000_000})
    );
    let count = peer.requests.lock().unwrap().len();
    refresh(false);
    assert_eq!(
        peer.requests.lock().unwrap().len(),
        count,
        "unchanged catalogs fetch no timelines"
    );
    peer.version.store(2, Ordering::SeqCst);
    refresh(false);
    let second = resolved().unwrap();
    assert!(
        matches!(&second,multiplier::ModelResolution::Rates(rates) if rates.base["input"]==multiplier::RateState::Priced {units_per_million:2_000_000_000})
    );
    peer.version.store(3, Ordering::SeqCst);
    refresh(true);
    let third = resolved().unwrap();
    assert!(
        matches!(&third,multiplier::ModelResolution::Rates(rates) if rates.reference_model.as_ref().unwrap().model_id=="target-b")
    );
    let requests = peer.requests.lock().unwrap();
    assert!(requests.contains(&("openai-codex".into(), "m".into())));
    assert!(requests.contains(&("openai".into(), "target-a".into())));
    assert!(requests.contains(&("openai".into(), "target-b".into())));
}

#[test]
fn each_model_read_gets_its_own_deadline_and_reads_continue_between_ticks() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    state
        .ordered_write(|tx| {
            for i in 0..8 {
                let id = format!("slow-{i}");
                tx.execute(
                    "INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,provider,model,charge_basis) VALUES(?1,'opencode',?1,0,'openai',?1,'unknown')",
                    [&id],
                )
                .map_err(|error| error.to_string())?;
            }
            Ok(((), true))
        })
        .unwrap();
    // Each read takes 400 ms, so one pass reads only some of the models.
    let peer = Arc::new(TimelineFusiform {
        store: directory.path().join("ledger.db"),
        version: std::sync::atomic::AtomicI64::new(1),
        requests: Default::default(),
        delay: Duration::from_millis(400),
        timeouts: Default::default(),
        failing: Vec::new(),
    });
    state
        .install_catalog(Some(CatalogClient::connect(peer.clone()).unwrap()))
        .unwrap();
    let mut backoff = RateBackoff::default();
    let version = rate_era_tick(&state, &mut backoff)
        .unwrap()
        .expect("a full tick's pass cannot read every slow model");
    let mut stage = MultiplierStage::new(None);
    stage.rates_pending = Some(version);
    let mut passes = 0;
    while stage.rates_behind() {
        stage.fill_rates(&state);
        passes += 1;
        assert!(passes < 10, "between-tick passes must finish the reads");
    }
    let unread = state
        .with_read_op(|conn| {
            astrocyte_core::rate_era::pending(conn, 256)
                .map_err(|error| (error.code().into(), error.message()))
        })
        .unwrap();
    assert!(unread.is_empty(), "every model is read: {unread:?}");
    // A model's read must not inherit what an earlier model left of the pass
    // deadline; that is what made reads time out on this side.
    let timeouts = peer.timeouts.lock().unwrap();
    assert!(timeouts.len() >= 8);
    assert!(
        timeouts
            .iter()
            .all(|timeout| *timeout > RATE_FETCH_MODEL_BUDGET - Duration::from_secs(1)),
        "every read gets close to its own full deadline: {timeouts:?}"
    );
}

#[test]
fn a_model_fusiform_cannot_serve_waits_before_it_is_tried_again() {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(state::leased_test_state(&directory));
    publish_desired(&state, &directory);
    state
        .ordered_write(|tx| {
            for id in ["slow", "fine"] {
                tx.execute(
                    "INSERT INTO usage_event(event_id,producer,producer_ref,recorded_at_ms,provider,model,charge_basis) VALUES(?1,'opencode',?1,0,'openai',?1,'unknown')",
                    [id],
                )
                .map_err(|error| error.to_string())?;
            }
            Ok(((), true))
        })
        .unwrap();
    let peer = Arc::new(TimelineFusiform {
        store: directory.path().join("ledger.db"),
        version: std::sync::atomic::AtomicI64::new(1),
        requests: Default::default(),
        delay: Duration::ZERO,
        timeouts: Default::default(),
        failing: vec!["slow".into()],
    });
    state
        .install_catalog(Some(CatalogClient::connect(peer.clone()).unwrap()))
        .unwrap();
    let mut stage = MultiplierStage::new(None);
    stage.tick(&state, false, NOW);
    let tries = |model: &str| {
        peer.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, requested)| requested == model)
            .count()
    };
    assert_eq!(tries("slow"), 1, "the failing model is tried once");
    assert_eq!(tries("fine"), 1);
    // Only the failing model is left, and it is waiting: no between-tick
    // passes, and the next full tick within the wait does not ask again.
    assert!(!stage.rates_behind());
    stage.tick(&state, false, NOW + 30_000);
    assert_eq!(tries("slow"), 1, "a failed model waits before its next try");
    // Once the wait has passed, the next full tick tries it again.
    let model = ("openai".to_string(), "slow".to_string());
    stage.rate_backoff.waits.get_mut(&model).unwrap().0 = std::time::Instant::now();
    stage.tick(&state, false, NOW + 60_000);
    assert_eq!(tries("slow"), 2);
    assert_eq!(
        stage.rate_backoff.waits[&model].1,
        RateBackoff::FIRST * 2,
        "a second failure doubles the wait"
    );
    // A new catalog version clears the wait: the change may be the cure.
    peer.version.store(2, Ordering::SeqCst);
    stage.tick(&state, false, NOW + 90_000);
    assert_eq!(tries("slow"), 3, "a catalog change retries a waiting model");
}
