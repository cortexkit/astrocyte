#[test]
fn stop_during_catalog_discovery_leaves_it_pending_for_retry() {
    use std::sync::mpsc;

    struct SlowCatalog {
        pause: &'static str,
        calls: std::sync::Mutex<Vec<&'static str>>,
        gate: std::sync::Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    }
    impl SlowCatalog {
        fn call(&self, method: &'static str) {
            self.calls.lock().unwrap().push(method);
            if method == self.pause || (method == "status" && self.pause == "typed_status") {
                if let Some((entered, resume)) = self.gate.lock().unwrap().take() {
                    entered.send(()).unwrap();
                    resume.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            }
        }
    }
    impl CatalogConnection for SlowCatalog {
        fn catalog_status(
            &self,
            _: fusiform_protocol::StatusRequest,
        ) -> Result<fusiform_protocol::StatusResponse, CatalogCallError> {
            self.call("status");
            let mut response = serde_json::json!({"source":"fake", "catalog_version":1, "model_count":0, "era_count":0, "recent_polls":[]});
            if self.pause == "typed_status" {
                response["history_begins_at_ms"] = serde_json::json!(123);
            }
            Ok(serde_json::from_value(response).unwrap())
        }
        fn catalog_get(
            &self,
            _: CatalogGetRequest,
        ) -> Result<CatalogGetResponse, CatalogCallError> {
            self.call("get");
            Err(CatalogCallError {
                code: "no_coverage".into(),
                message: "fusiform's history begins at 123 ms".into(),
            })
        }
    }

    for pause in ["status", "typed_status", "get"] {
        let state = Arc::new(test_state());
        seed_unpriced_event(&state);
        let stop = Arc::new(AtomicBool::new(false));
        let (entered, waiting) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        let peer = Arc::new(SlowCatalog {
            pause,
            calls: Default::default(),
            gate: std::sync::Mutex::new(Some((entered, resume))),
        });
        let (done, finished) = mpsc::sync_channel(1);
        let (worker_state, worker_stop, connection) = (state.clone(), stop.clone(), peer.clone());
        let worker = crate::test_support::Background::new(
            "stoppable catalog discovery",
            std::thread::spawn(move || {
                let mut pending = Some(PendingCatalog { connection });
                run_tick(
                    &worker_state,
                    &[],
                    &FileSources::default(),
                    None,
                    &mut pending,
                    &mut PricingWalk::new(PricingBudget::WHOLE_QUEUE),
                    &mut QueueReconciler::new(QUEUE_RECONCILE_CHUNK, QUEUE_RECONCILE_TICK_BUDGET),
                    &worker_stop,
                );
                done.send(pending).unwrap();
            }),
            {
                let stop = stop.clone();
                move || stop.store(true, Ordering::Relaxed)
            },
        );
        waiting
            .recv_timeout(Duration::from_secs(5))
            .expect("discovery reached the slow peer call");
        stop.store(true, Ordering::Relaxed);
        release.send(()).unwrap();
        let pending = finished
            .recv_timeout(Duration::from_secs(5))
            .expect("discovery returned after the in-flight call");
        worker.stop_and_join();
        assert!(
            pending.is_some(),
            "{pause}: stopped discovery must remain pending"
        );
        assert!(
            state.catalog_client().is_none(),
            "{pause}: no partial catalog was installed"
        );
        assert_eq!(priced_charge_rows(&state), 0);
        let expected: &[&str] = if pause == "get" {
            &["status", "get"]
        } else {
            &["status"]
        };
        assert_eq!(*peer.calls.lock().unwrap(), expected, "no next peer call");
        assert!(
            pending
                .unwrap()
                .try_connect(&state, &AtomicBool::new(false))
                .unwrap(),
            "the next run rediscovers the boundary"
        );
        assert_eq!(state.catalog_client().unwrap().history_start_ms(), 123);
    }
}
