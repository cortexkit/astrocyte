use super::*;
use crate::refresher::harness_drain::HarnessSource;
use crate::state::test_state;
use crate::test_support::Background;
use std::sync::{mpsc, Arc, Mutex};
use subc_protocol::{Flags, Frame, FrameType, Priority};

#[test]
fn stop_during_colliculus_peer_call_leaves_the_feed_for_retry() {
    for pause in ["connect", "route.open", "usage.ack", "usage.read"] {
        let state = Arc::new(test_state());
        state.with_conn(|conn| {
            conn.execute("INSERT INTO media_feed_cursor(source_key,through_seq,skipped_local) VALUES(?1,'7',0)", [SOURCE_KEY])
                .map_err(|error| error.to_string())?;
            Ok(())
        }).unwrap();
        let (listener, path, mut connection) =
            crate::test_support::fake_daemon("media-stop", 41, 42);
        connection.daemon_ver = "0.39.0".into();
        subc_transport::connection_file::write_atomic(&path, &connection).unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let capture = calls.clone();
        let (close, mut closed) = tokio::sync::oneshot::channel();
        let peer = Background::new(
            "stoppable media peer",
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        use subc_transport::{read_frame, write_frame};
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                        let (mut stream, _) = tokio::select! {
                            _ = &mut closed => return,
                            accepted = listener.accept() => accepted.unwrap(),
                        };
                        let hold = || {
                            entered.send(()).unwrap();
                            resume.recv_timeout(Duration::from_secs(5)).unwrap();
                        };
                        if pause == "connect" {
                            hold();
                        }
                        subc_transport::auth::authenticate_server(
                            &mut stream,
                            &connection.key,
                            &connection.daemon_id,
                            &connection.daemon_ver,
                            BUDGET,
                        )
                        .await
                        .unwrap();
                        loop {
                            let request = tokio::select! {
                                _ = &mut closed => return,
                                request = read_frame(&mut stream) => match request {
                                    Ok(Some(request)) => request,
                                    _ => return,
                                },
                            };
                            if request.header.ty != FrameType::Request {
                                continue;
                            }
                            let value: Value = serde_json::from_slice(&request.body).unwrap();
                            let method = value["method"]
                                .as_str()
                                .or_else(|| value["op"].as_str())
                                .unwrap();
                            let (kind, body) = match method {
                                "server.describe" => (
                                    FrameType::Error,
                                    json!({"code":"unknown_op","message":"no description"}),
                                ),
                                "route.open" => (
                                    FrameType::Response,
                                    json!({"op":"route.open","route_channel":7,"route_epoch":1}),
                                ),
                                "usage.ack" => (
                                    FrameType::Response,
                                    json!({"result":{"acked_through_seq":7}}),
                                ),
                                // A replay observes the durable sequence before the
                                // ACK pause without adding any new usage.
                                "usage.read" if pause == "usage.ack" => (
                                    FrameType::Response,
                                    json!({"result":{"entries":[{"seq":7,"record":{
                                        "record_id":"local","principal":"person-a","job_id":"job-1",
                                        "provider":"example","model":"speech","charge_basis":"local",
                                        "started_at_ms":1,"finished_at_ms":2,"status":"done",
                                        "selectors":{},"measurements":[]
                                    }}],"last_seq":7}}),
                                ),
                                "usage.read" => (FrameType::Response, json!({"result":{"entries":[]}})),
                                other => panic!("unexpected method {other}"),
                            };
                            if method != "server.describe" {
                                capture.lock().unwrap().push(method.to_string());
                            }
                            if method == pause {
                                hold();
                            }
                            let response = Frame::build(
                                kind,
                                Flags::new(false, Priority::Passive, false),
                                request.header.channel,
                                request.header.epoch,
                                request.header.corr,
                                serde_json::to_vec(&body).unwrap(),
                            )
                            .unwrap();
                            if write_frame(&mut stream, &response).await.is_err() {
                                return;
                            }
                        }
                    });
            }),
            move || {
                let _ = close.send(());
            },
        );
        let stop = Arc::new(AtomicBool::new(false));
        let (done, finished) = mpsc::sync_channel(1);
        let (worker_state, worker_stop, worker_path) = (state.clone(), stop.clone(), path.clone());
        let worker = Background::new(
            "stoppable media drain",
            std::thread::spawn(move || {
                let mut source = ColliculusSource::new();
                source.fixture_connection = Some(worker_path);
                done.send(source.drain_for_health(&worker_state, 100, &worker_stop))
                    .unwrap();
            }),
            {
                let stop = stop.clone();
                move || stop.store(true, Ordering::Relaxed)
            },
        );
        waiting
            .recv_timeout(Duration::from_secs(5))
            .expect("drain reached the slow peer call");
        stop.store(true, Ordering::Relaxed);
        release.send(()).unwrap();
        let outcome = finished
            .recv_timeout(Duration::from_secs(5))
            .expect("drain returned after the in-flight call");
        worker.stop_and_join();
        peer.stop_and_join();
        std::fs::remove_file(path).unwrap();
        assert!(
            matches!(outcome, Err(SourceDrainError::Stopped)),
            "{pause}: {outcome:?}"
        );
        let expected: &[&str] = match pause {
            "connect" => &[],
            "route.open" => &["route.open"],
            "usage.ack" => &["route.open", "usage.read", "usage.ack"],
            _ => &["route.open", "usage.read"],
        };
        assert_eq!(
            *calls.lock().unwrap(),
            expected,
            "{pause}: no next peer call"
        );
        state
            .with_conn(|conn| {
                let cursor: String = conn
                    .query_row(
                        "SELECT through_seq FROM media_feed_cursor WHERE source_key=?1",
                        [SOURCE_KEY],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(cursor, "7");
                for table in [
                    "usage_event",
                    "measurement",
                    "set_aside_record",
                    "source_runtime WHERE source_key='colliculus'",
                ] {
                    let rows: i64 = conn
                        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                            row.get(0)
                        })
                        .unwrap();
                    let expected =
                        i64::from(pause == "usage.ack" && table.starts_with("source_runtime"));
                    assert_eq!(rows, expected, "{pause}: unexpected rows in {table}");
                }
                Ok(())
            })
            .unwrap();
    }
}
