#[test]
fn stop_during_multiplier_peer_call_leaves_whole_days_for_retry() {
    use std::sync::mpsc;

    struct SlowPlans {
        inner: Arc<FakeFusiform>,
        pause_at: usize,
        calls: AtomicUsize,
        gate: std::sync::Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    }
    impl PlanPriceSource for SlowPlans {
        fn plan_prices(
            &self,
            request: fusiform_protocol::PlanPricesRequest,
        ) -> Result<fusiform_protocol::PlanPricesResponse, CatalogCallError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.pause_at {
                let (entered, resume) = self.gate.lock().unwrap().take().unwrap();
                entered.send(()).unwrap();
                resume.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            self.inner.plan_prices(request)
        }
    }

    for pause_at in [1, 2, 8] {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        publish_desired(&state, &directory);
        add_vault_plan(&state, "fixture:acct2", Some("acct2"), "pro_100");
        let fusiform = FakeFusiform::new(false);
        install_catalog(&state, &fusiform);
        seed_usage(&state);
        state.ordered_write(|tx| {
        tx.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider) SELECT provider,'acct2',pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider FROM window_sample", []).map_err(|error| error.to_string())?;
        tx.execute("INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider) SELECT 'second',account_ref,pool_key,fetched_at_instant,used_percent,window_minutes,resets_at,model_scope,api_provider FROM window_sample", []).map_err(|error| error.to_string())?;
        Ok(((), true))
    }).unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        let peer = Arc::new(SlowPlans {
            inner: fusiform.clone(),
            pause_at,
            calls: AtomicUsize::new(0),
            gate: std::sync::Mutex::new(Some((entered, resume))),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let (done, finished) = mpsc::sync_channel(1);
        let (worker_state, worker_stop, plans) = (state.clone(), stop.clone(), peer.clone());
        let worker = crate::test_support::Background::new(
            "stoppable multiplier",
            std::thread::spawn(move || {
                let mut stage = MultiplierStage::new(Some(plans));
                stage.tick_until_stopped(&worker_state, false, NOW, &worker_stop);
                done.send(stage).unwrap();
            }),
            {
                let stop = stop.clone();
                move || stop.store(true, Ordering::Relaxed)
            },
        );
        waiting
            .recv_timeout(Duration::from_secs(5))
            .expect("multiplier reached plan.prices");
        stop.store(true, Ordering::Relaxed);
        release.send(()).unwrap();
        let mut stage = finished
            .recv_timeout(Duration::from_secs(5))
            .expect("multiplier returned after the in-flight call");
        worker.stop_and_join();
        assert_eq!(
            fusiform.plan_calls.load(Ordering::SeqCst),
            pause_at,
            "no next day or provider call"
        );
        assert_eq!(
            all_rows(&state).len(),
            (pause_at - 1) * 2,
            "only earlier whole two-plan days were written"
        );
        assert_eq!(
            stage.recorded.len(),
            pause_at - 1,
            "the interrupted day is not marked recorded"
        );
        stop.store(false, Ordering::Relaxed);
        stage.tick_until_stopped(&state, false, NOW, &stop);
        assert_eq!(
            observation_days(&state).len(),
            28,
            "every provider/day has both plan rows"
        );
        assert_eq!(
            fusiform.plan_calls.load(Ordering::SeqCst),
            15,
            "only the interrupted day was recomputed"
        );
        state.with_conn(|conn| {
        let incomplete: i64 = conn.query_row("SELECT count(*) FROM (SELECT quota_provider,observation_day FROM multiplier_observation GROUP BY quota_provider,observation_day HAVING count(*)<>2 OR count(DISTINCT plan)<>2)", [], |row| row.get(0)).unwrap();
        assert_eq!(incomplete, 0, "no partial or duplicate plan days");
        Ok(())
    }).unwrap();
    }
}
