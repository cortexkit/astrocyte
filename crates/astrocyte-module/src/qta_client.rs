//! Wire-backed QTA `usage.get` consumer — the CAPACITY axis source.
//!
//! Same lazy-connect + route-death-recovery machinery as `fusiform_client`
//! (one connection, reused until an error, reopened on a dead route), but calls
//! `usage.get` on QTA's ManagementSurface and maps the shared
//! `cortexkit-provider-usage` array into a `CapacitySnapshot`. Read-only and
//! advisory: every failure is `PullError::Unavailable` so the refresher marks
//! the axis stale and keeps the last-known windows — nothing blocks.

#[cfg(test)]
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use subc_client_rs::SubcConsumer;
use subc_protocol::RouteTarget;
#[cfg(test)]
use subc_protocol::{Flags, Frame, FrameType, Priority};
#[cfg(test)]
use subc_transport::{read_frame, write_frame};
#[cfg(test)]
use tokio::io::AsyncWriteExt;

use astrocyte_core::capacity::CapacitySnapshot;
use cortexkit_provider_usage::ProviderUsage;

use crate::capacity_map::map_usage_array;
use crate::peer_exchange::{read_peer, PeerAccess, ReadError};
use crate::refresher::{CapacitySource, PullError};

// Copied sibling facts. This pair was WRONG for three days in August: the
// module renamed to `insula` and this constant still dialled the old id, so
// every poll failed at route.open while health read green. Nothing links
// these to the sibling's source; re-derive against
// `insula/crates/quota-module/src/ids.rs` DEFAULT_MODULE_ID (not
// CREDENTIALS_MODULE_ID beside it, which is their client to the vault) and
// the op the surface matches on. Last re-derived 2026-09-06 at insula 9b7ba5c.
const QTA_MODULE_ID: &str = "insula";
#[cfg(test)]
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const OP_USAGE_GET: &str = "usage.get";

pub struct QtaClient {
    access: PeerAccess,
    request_timeout: Duration,
    runtime: tokio::runtime::Runtime,
    live: tokio::sync::Mutex<Option<SubcConsumer>>,
}

impl QtaClient {
    pub(crate) fn new(access: PeerAccess) -> Result<Self, String> {
        Self::build(access, REQUEST_TIMEOUT)
    }

    fn build(access: PeerAccess, request_timeout: Duration) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|e| format!("qta client runtime: {e}"))?;
        Ok(Self {
            access,
            request_timeout,
            runtime,
            live: tokio::sync::Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(path: impl Into<PathBuf>) -> Result<Self, String> {
        Self::new(PeerAccess::Fixture(path.into()))
    }

    #[cfg(test)]
    fn new_with_request_timeout(
        path: impl Into<PathBuf>,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        Self::build(PeerAccess::Fixture(path.into()), request_timeout)
    }

    fn fetch_inner(&self) -> Result<CapacitySnapshot, PullError> {
        let body = self
            .runtime
            .block_on(self.request(OP_USAGE_GET, json!({})))?;
        decode_usage_response(&body).map_err(PullError::Unavailable)
    }

    pub(crate) async fn event_body(&self, event_key: &str) -> Result<Vec<u8>, PullError> {
        self.request(
            "events_get",
            json!({"event_key": event_key, "event_name": "quota_sampled"}),
        )
        .await
    }

    async fn request(&self, method: &str, params: Value) -> Result<Vec<u8>, PullError> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let mut cached = tokio::time::timeout_at(deadline, self.live.lock())
            .await
            .map_err(|_| PullError::Unavailable("qta exchange: response timeout".into()))?;
        // A cancelled bus session drops the route rather than caching an unfinished request.
        let mut guard = cached.take();
        let body = serde_json::to_vec(&json!({ "method": method, "params": params }))
            .map_err(|e| PullError::Unavailable(format!("params encode: {e}")))?;
        let result = read_peer(
            &self.access,
            &mut guard,
            RouteTarget::ManagementSurface {
                module_id: QTA_MODULE_ID.into(),
            },
            "capacity-poll",
            body,
            deadline,
        )
        .await
        .map_err(|error| match error {
            ReadError::Unavailable(reason) => {
                PullError::Unavailable(format!("qta exchange: {reason}"))
            }
            ReadError::Refused(error) => {
                PullError::Unavailable(format!("{method} error: {}: {}", error.code, error.message))
            }
        });
        *cached = guard;
        result
    }
}

fn decode_usage_response(body: &[u8]) -> Result<CapacitySnapshot, String> {
    let envelope: Value =
        serde_json::from_slice(body).map_err(|error| format!("undecodable response: {error}"))?;
    let object = envelope
        .as_object()
        .ok_or_else(|| "usage.get response is not an object".to_string())?;
    let result = object
        .get("result")
        .ok_or_else(|| "response missing result".to_string())?;
    let entries: Vec<ProviderUsage> = serde_json::from_value(result.clone())
        .map_err(|error| format!("unexpected usage shape: {error}"))?;
    let complete_providers = match object.get("completeProviders") {
        None => None,
        Some(value) => Some(
            serde_json::from_value::<std::collections::BTreeSet<String>>(value.clone())
                .map_err(|error| format!("malformed completeProviders: {error}"))?,
        ),
    };
    map_usage_array(&entries, complete_providers)
        .map_err(|error| format!("invalid usage response: {error}"))
}

impl CapacitySource for QtaClient {
    fn poll(&self) -> Result<CapacitySnapshot, PullError> {
        self.fetch_inner()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use subc_transport::auth::authenticate_server;

    use super::*;
    use crate::{
        config::AstrocyteConfig,
        refresher::{configured_capacity, run_iteration_with_capacity},
        state::test_state,
        test_support::{wait_until, Background},
    };

    const TEST_REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

    /// Accepts connections, answers `route.open`, and then never answers the
    /// request, so every exchange the client attempts ends in its timeout.
    struct SilentPeer {
        connection_file_path: PathBuf,
        /// One entry per accepted connection, in accept order: whether the
        /// client got past `route.open` and sent its request on it.
        connections: Arc<Mutex<Vec<bool>>>,
        frame_types: Arc<Mutex<Vec<FrameType>>>,
        task: Option<Background<()>>,
    }

    impl SilentPeer {
        /// `route_open_delay` holds back the `route.open` reply. Zero answers at
        /// once; a delay longer than the client's timeout forces the path where
        /// opening the route itself times out.
        fn start(route_open_delay: Duration) -> Self {
            let (listener, connection_file_path, connection) =
                crate::test_support::fake_daemon("qta-timeout-eviction", 7, 9);

            let connections = Arc::new(Mutex::new(Vec::new()));
            let server_connections = Arc::clone(&connections);
            let frame_types = Arc::new(Mutex::new(Vec::new()));
            let received_types = Arc::clone(&frame_types);
            let key = connection.key.clone();
            let daemon_id = connection.daemon_id;
            let daemon_ver = connection.daemon_ver.clone();
            let (release, mut released) = tokio::sync::oneshot::channel::<()>();
            let task = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                        let mut silent_streams = Vec::new();
                        // Serve until released rather than for a fixed number of
                        // connections: how many a fetch makes depends on which path it
                        // takes, and a peer still waiting for an accept that never
                        // comes would hang `finish`.
                        loop {
                            let serve = async {
                                let (mut stream, _) = listener.accept().await.ok()?;
                                let index = {
                                    let mut connections = server_connections.lock().unwrap();
                                    connections.push(false);
                                    connections.len() - 1
                                };
                                // The client's timeout covers route.open as well as the
                                // request, so it can give up and close at any step of this
                                // setup. Each step ends the connection quietly: a panic
                                // here would kill the peer and starve the reconnect the
                                // test counts.
                                if authenticate_server(
                                    &mut stream,
                                    &key,
                                    &daemon_id,
                                    &daemon_ver,
                                    CONNECT_TIMEOUT,
                                )
                                .await
                                .is_err()
                                {
                                    return Some(());
                                }
                                let Some(route_open) =
                                    crate::test_support::read_peer_request(&mut stream).await
                                else {
                                    return Some(());
                                };
                                received_types.lock().unwrap().push(route_open.header.ty);
                                if !route_open_delay.is_zero() {
                                    tokio::select! {
                                        _ = tokio::time::sleep(route_open_delay) => {},
                                        _ = read_frame(&mut stream) => return Some(()),
                                    }
                                }
                                let route_response = Frame::build(
                                    FrameType::Response,
                                    Flags::new(false, Priority::Passive, false),
                                    0,
                                    0,
                                    route_open.header.corr,
                                    serde_json::to_vec(&json!({
                                        "op": "route.open",
                                        "route_channel": 7,
                                        "route_epoch": 1,
                                    }))
                                    .unwrap(),
                                )
                                .unwrap();
                                if write_frame(&mut stream, &route_response).await.is_err()
                                    || stream.flush().await.is_err()
                                {
                                    return Some(());
                                }
                                if let Some(request) =
                                    crate::test_support::read_peer_request(&mut stream).await
                                {
                                    received_types.lock().unwrap().push(request.header.ty);
                                    server_connections.lock().unwrap()[index] = true;
                                    // Keep the stream open after receiving the request. The
                                    // client must observe a timeout rather than a FIN/RST.
                                    silent_streams.push(stream);
                                }
                                Some(())
                            };
                            tokio::select! { _ = serve => {}, _ = &mut released => break }
                        }
                        drop(silent_streams);
                    });
            });

            Self {
                connection_file_path,
                connections,
                frame_types,
                task: Some(Background::new("the silent QTA peer", task, move || {
                    let _ = release.send(());
                })),
            }
        }

        fn connection_count(&self) -> usize {
            self.connections.lock().unwrap().len()
        }

        /// Checks that each timed-out fetch opened exactly one fresh connection.
        /// The deadline leaves no time for another attempt, but the next fetch
        /// must not reuse the timed-out socket. Returns whether setup completed.
        fn assert_fetch_reconnected(&self, before: usize) -> bool {
            // The kernel completes the TCP handshake before the peer's accept loop
            // runs, so a client can time out and return before the peer has
            // recorded the connection. Wait a bounded time for the record.
            wait_until("the QTA peer accepting the new connection", || {
                self.connections.lock().unwrap().len() > before
            });
            let connections = self.connections.lock().unwrap().clone();
            assert!(
                connections.len() > before,
                "a fetch after a timeout must open a new connection, not reuse the cached one",
            );
            let reached_request = connections[before];
            assert_eq!(
                connections.len() - before,
                1,
                "a timeout leaves no budget for a same-call retry"
            );
            reached_request
        }

        fn finish(mut self) {
            self.task.take().unwrap().stop_and_join();
            let frame_types = self.frame_types.lock().unwrap();
            assert!(
                frame_types.iter().all(|ty| *ty == FrameType::Request),
                "the peer must receive only requests: {frame_types:?}"
            );
        }
    }

    impl Drop for SilentPeer {
        fn drop(&mut self) {
            drop(self.task.take());
            let _ = std::fs::remove_file(&self.connection_file_path);
        }
    }

    /// Answers every `usage.get` request with one fixed body, on as many
    /// connections as the client opens, until the test ends.
    struct ResponsePeer {
        connection_file_path: PathBuf,
        /// The `method` of every request answered, in order.
        answered: Arc<Mutex<Vec<Value>>>,
        task: Option<Background<()>>,
    }

    impl ResponsePeer {
        fn start(response_body: Value) -> Self {
            let (listener, connection_file_path, connection) =
                crate::test_support::fake_daemon("qta-response", 17, 19);

            let answered = Arc::new(Mutex::new(Vec::new()));
            let server_answered = Arc::clone(&answered);
            let key = connection.key.clone();
            let daemon_id = connection.daemon_id;
            let daemon_ver = connection.daemon_ver.clone();
            let (release, mut released) = tokio::sync::oneshot::channel::<()>();
            let task = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                        // Serve until released rather than for one connection
                        // and one request: on a loaded runner the client can
                        // time out and reconnect, and a peer that had already
                        // exited would turn that into a refused connection, while
                        // a peer still waiting for a request that never comes
                        // would hang the test's join.
                        loop {
                            let accepted = tokio::select! {
                                accepted = listener.accept() => accepted,
                                _ = &mut released => break,
                            };
                            let Ok((stream, _)) = accepted else {
                                continue;
                            };
                            let serve = serve_responses(
                                stream,
                                &key,
                                &daemon_id,
                                &daemon_ver,
                                &response_body,
                                &server_answered,
                            );
                            tokio::select! {
                                () = serve => {}
                                _ = &mut released => break,
                            }
                        }
                    });
            });
            Self {
                connection_file_path,
                answered,
                task: Some(Background::new("the fake QTA peer", task, move || {
                    let _ = release.send(());
                })),
            }
        }

        /// Stops the peer and checks the requests it answered: every one was
        /// `usage.get`, and at least one was answered. How many depends on
        /// whether the client had to reconnect, so the count is not fixed.
        fn finish(mut self) {
            self.task.take().unwrap().stop_and_join();
            let answered = self.answered.lock().unwrap().clone();
            assert!(
                !answered.is_empty(),
                "the client never sent a usage.get request the peer could answer"
            );
            assert!(
                answered.iter().all(|method| method == OP_USAGE_GET),
                "every request must be usage.get: {answered:?}"
            );
        }
    }

    /// One connection of `ResponsePeer`. The client can give up and close at
    /// any step, so every step ends the connection quietly instead of
    /// panicking: a panic would kill the peer and fail the test with a
    /// message about the peer rather than about the client.
    async fn serve_responses(
        mut stream: tokio::net::TcpStream,
        key: &[u8],
        daemon_id: &[u8; 16],
        daemon_ver: &str,
        response_body: &Value,
        answered: &Mutex<Vec<Value>>,
    ) {
        if authenticate_server(&mut stream, key, daemon_id, daemon_ver, CONNECT_TIMEOUT)
            .await
            .is_err()
        {
            return;
        }
        let Some(route_open) = crate::test_support::read_peer_request(&mut stream).await else {
            return;
        };
        let route_response = Frame::build(
            FrameType::Response,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            route_open.header.corr,
            serde_json::to_vec(&json!({
                "op": "route.open",
                "route_channel": 7,
                "route_epoch": 1,
            }))
            .unwrap(),
        )
        .unwrap();
        if write_frame(&mut stream, &route_response).await.is_err() || stream.flush().await.is_err()
        {
            return;
        }
        while let Some(request) = crate::test_support::read_peer_request(&mut stream).await {
            let method = serde_json::from_slice::<Value>(&request.body)
                .map(|body| body["method"].clone())
                .unwrap_or(Value::Null);
            answered.lock().unwrap().push(method);
            let response = Frame::build(
                FrameType::Response,
                Flags::new(false, Priority::Passive, false),
                request.header.channel,
                request.header.epoch,
                request.header.corr,
                serde_json::to_vec(response_body).unwrap(),
            )
            .unwrap();
            if write_frame(&mut stream, &response).await.is_err() || stream.flush().await.is_err() {
                return;
            }
        }
    }

    impl Drop for ResponsePeer {
        fn drop(&mut self) {
            // Stop the peer thread before removing its connection file; a
            // test that failed before `finish` reaches here with it running.
            drop(self.task.take());
            let _ = std::fs::remove_file(&self.connection_file_path);
        }
    }

    #[test]
    fn runtime_iteration_polls_qta_and_persists_the_peer_fetched_at() {
        let fetched_at = "2026-09-05T09:15:00Z";
        let peer = ResponsePeer::start(json!({
            "result": [{
                "provider": "codex",
                "apiProvider": "openai",
                "account": "account-a",
                "source": "oauth",
                "fetchedAt": fetched_at,
                "usage": { "primary": { "usedPercent": 42.0 } }
            }]
        }));
        let state = test_state();
        let capacity = configured_capacity(
            &state,
            &AstrocyteConfig {
                poll_interval_secs: 3,
                reasoning_billed_as_output: Vec::new(),
                subscription_multiplier: crate::config::intentional_empty()
                    .config
                    .subscription_multiplier,
                harness_ingest: crate::config::HarnessIngest::all_on(),
            },
            Some(PeerAccess::Fixture(peer.connection_file_path.clone())),
        )
        .unwrap();

        run_iteration_with_capacity(&state, &[], capacity.as_deref(), &AtomicBool::new(false));

        let rows: Vec<String> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT fetched_at FROM capacity_window ORDER BY provider,account_ref,pool_key")
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(|error| error.to_string())?;
                Ok(rows)
            })
            .unwrap();
        assert_eq!(rows, vec![fetched_at.to_string()]);
        peer.finish();
    }

    #[test]
    fn response_without_complete_providers_preserves_seeded_accounts() {
        let peer = ResponsePeer::start(json!({
            "result": [{
                "provider": "codex",
                "account": "new-account",
                "source": "oauth",
                "fetchedAt": "2026-09-05T09:20:00Z",
                "usage": { "primary": { "usedPercent": 20.0 } }
            }]
        }));
        let state = test_state();
        state.configure_capacity_runtime(true, 3_000, 100).unwrap();
        let seeded = decode_usage_response(
            br#"{"result":[{"provider":"codex","account":"seeded-account","source":"oauth","fetchedAt":"2026-09-05T09:00:00Z","usage":{"primary":{"usedPercent":10.0}}}]}"#,
        )
        .unwrap();
        state
            .ordered_write(|tx| {
                astrocyte_core::capacity::apply_snapshot_in_tx(tx, &seeded, 101)
                    .map_err(|error| error.to_string())?;
                Ok(((), true))
            })
            .unwrap();
        let client = QtaClient::for_test(&peer.connection_file_path).unwrap();

        run_iteration_with_capacity(&state, &[], Some(&client), &AtomicBool::new(false));

        let accounts: Vec<String> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT DISTINCT account_ref FROM capacity_window ORDER BY account_ref",
                    )
                    .map_err(|error| error.to_string())?;
                let accounts = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(|error| error.to_string())?;
                Ok(accounts)
            })
            .unwrap();
        assert_eq!(accounts, vec!["new-account", "seeded-account"]);
        peer.finish();
    }

    #[test]
    fn qta_route_targets_insula_by_name() {
        assert_eq!(
            QTA_MODULE_ID, "insula",
            "capacity polling must target the live insula daemon by name"
        );
    }

    #[test]
    fn completion_authority_is_same_response_optional_state() {
        let without = decode_usage_response(
            br#"{"result":[{"provider":"codex","account":"account-a","usage":{"primary":{"usedPercent":10.0}}}]}"#,
        )
        .unwrap();
        assert_eq!(without.complete_providers, None);

        let with = decode_usage_response(
            br#"{"result":[{"provider":"codex","account":"account-a","usage":{"primary":{"usedPercent":10.0}}}],"completeProviders":["codex"]}"#,
        )
        .unwrap();
        assert_eq!(
            with.complete_providers,
            Some(std::collections::BTreeSet::from(["codex".to_string()]))
        );
    }

    #[test]
    fn usage_get_decodes_model_scope_from_pinned_insula_sample() {
        use astrocyte_core::capacity::WindowModelScope;
        use sha2::{Digest, Sha256};

        const PAYLOAD: &[u8] = include_bytes!("../test-vectors/insula-31e13281/usage-get.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(PAYLOAD)),
            "82067311fb0a487b46279b187b73220cd2cffe1c3477936747c6226562eb14ef"
        );
        let snapshot = decode_usage_response(PAYLOAD).unwrap();
        let scope = |provider: &str, account: &str, pool: &str| {
            snapshot
                .windows
                .iter()
                .find(|w| w.provider == provider && w.account_ref == account && w.pool_key == pool)
                .unwrap()
                .model_scope
        };
        assert_eq!(
            scope("claude", "stale@example.com", "secondary"),
            Some(WindowModelScope::All)
        );
        assert_eq!(
            scope(
                "antigravity",
                "named-windows@example.com",
                "extra:10:gemini-pro"
            ),
            Some(WindowModelScope::Subset)
        );
        assert_eq!(scope("codex", "fresh@example.com", "primary"), None);
    }

    #[test]
    fn usage_get_preserves_readings_with_unrecognised_model_scope() {
        use astrocyte_core::capacity::WindowModelScope;

        let snapshot = decode_usage_response(
            br#"{"result":[
                {"provider":"codex","account":"named","usage":{
                    "primary":{"usedPercent":20.0,"modelScope":"per_region"}}},
                {"provider":"codex","credentialRef":"cred","usage":{
                    "primary":{"usedPercent":10.0,"modelScope":"subset"},
                    "secondary":{"usedPercent":30.0,"modelScope":"per_region"}}}
            ]}"#,
        )
        .unwrap();
        assert_eq!(snapshot.windows.len(), 1);
        assert_eq!(snapshot.windows[0].model_scope, None);
        assert_eq!(snapshot.provider_readings.len(), 2);
        let scope = |pool: &str| {
            snapshot
                .provider_readings
                .iter()
                .find(|w| w.credential_ref.as_deref() == Some("cred") && w.pool_key == pool)
                .unwrap()
                .model_scope
        };
        assert_eq!(scope("secondary"), None);
        assert_eq!(scope("primary"), Some(WindowModelScope::Subset));
    }

    #[test]
    fn malformed_completion_or_usage_produces_no_snapshot() {
        let malformed_completion =
            decode_usage_response(br#"{"result":[],"completeProviders":"codex"}"#);
        assert!(malformed_completion.is_err());

        let missing_usage = decode_usage_response(
            br#"{"result":[{"provider":"codex","account":"account-a"}],"completeProviders":["codex"]}"#,
        );
        assert!(missing_usage.is_err());
    }

    /// A timed-out request must evict its connection, so the following fetch
    /// reconnects. Open the route outside the short response-wait budget so
    /// runner load cannot turn this into a route-opening timeout test.
    fn open_fixture_route(client: &QtaClient) -> SubcConsumer {
        use crate::test_support::HANG_GUARD;
        client.runtime.block_on(async {
            let consumer = client.access.connect(HANG_GUARD).await.unwrap();
            crate::peer_exchange::open_route(
                &consumer,
                RouteTarget::ManagementSurface {
                    module_id: QTA_MODULE_ID.into(),
                },
                "capacity-poll",
                crate::peer_exchange::options(HANG_GUARD),
            )
            .await
            .unwrap();
            consumer
        })
    }

    #[test]
    fn qta_timeouts_evict_cached_connections_for_fetches() {
        let peer = SilentPeer::start(Duration::ZERO);
        let client =
            QtaClient::new_with_request_timeout(&peer.connection_file_path, TEST_REQUEST_TIMEOUT)
                .unwrap();

        for _ in 0..2 {
            let before = peer.connection_count();
            {
                let mut live = client.live.blocking_lock();
                assert!(live.is_none(), "a timeout must evict the cached socket");
                *live = Some(open_fixture_route(&client));
            }
            assert!(client.fetch_inner().is_err());
            // The request's bytes can be buffered even if the peer thread is
            // not scheduled until after the client's timer fires.
            wait_until("the silent QTA peer receiving usage.get", || {
                peer.connections.lock().unwrap().get(before) == Some(&true)
            });
            assert!(
                peer.assert_fetch_reconnected(before),
                "the timed-out exchange must reach usage.get, not just route.open"
            );
            assert!(client.live.blocking_lock().is_none());
        }

        peer.finish();
    }

    #[test]
    fn qta_cancelled_event_fetch_does_not_cache_a_partly_consumed_socket() {
        let peer = SilentPeer::start(Duration::ZERO);
        let client = QtaClient::for_test(&peer.connection_file_path).unwrap();
        *client.live.blocking_lock() = Some(open_fixture_route(&client));
        client.runtime.block_on(async {
            let key = "a".repeat(64);
            let mut request = Box::pin(client.event_body(&key));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    tokio::select! {
                        result = &mut request => panic!("silent peer completed the event fetch: {result:?}"),
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {
                            if peer.connections.lock().unwrap().iter().any(|received| *received) { break; }
                        }
                    }
                }
            }).await.expect("event request did not reach the peer before cancellation");
        });
        assert!(client.live.blocking_lock().is_none());
        peer.finish();
    }

    /// When route.open itself times out, nothing is cached, and the next fetch
    /// opens a fresh connection instead of reusing a half-opened one. The peer
    /// holds the route.open reply well past the client's timeout, so every fetch
    /// takes this path without depending on the speed of route setup.
    #[test]
    fn route_open_timeout_caches_nothing_and_the_next_fetch_reconnects() {
        let peer = SilentPeer::start(TEST_REQUEST_TIMEOUT * 6);
        let client =
            QtaClient::new_with_request_timeout(&peer.connection_file_path, TEST_REQUEST_TIMEOUT)
                .unwrap();

        for _ in 0..2 {
            let before = peer.connection_count();
            assert!(client.fetch_inner().is_err());
            assert!(
                !peer.assert_fetch_reconnected(before),
                "route.open was held past the timeout, so no connection may reach the request",
            );
        }

        peer.finish();
    }

    /// Serves `death_code` for the first request on a cached route, then two
    /// fetches: the first must retry on a newly opened route and the second
    /// must keep using it. The assertions are on the route epochs the peer saw.
    fn assert_route_death_evicts_the_cached_route(death_code: &'static str) {
        use crate::route_death_peer::RouteDeathPeer;
        let peer = RouteDeathPeer::start(death_code, json!({"result": []}));
        let client = QtaClient::for_test(&peer.connection_file_path).unwrap();

        let first = client.fetch_inner();
        let second = client.fetch_inner();

        peer.assert_route_reopened(QTA_MODULE_ID, death_code);
        assert!(
            first.is_ok(),
            "retry on the reopened route: {:?}",
            first.err()
        );
        assert!(second.is_ok(), "next fetch: {:?}", second.err());
        peer.finish();
    }

    #[test]
    fn qta_stale_route_epoch_evicts_the_cached_route() {
        assert_route_death_evicts_the_cached_route("stale_route_epoch");
    }

    #[test]
    fn qta_unknown_channel_evicts_the_cached_route() {
        assert_route_death_evicts_the_cached_route("unknown_channel");
    }

    #[test]
    fn qta_module_reloading_reopens_the_route_and_retries() {
        use crate::fusiform_client::tests::{DeadlinePeer, PeerReply};
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::error("module_reloading", "provider restarting"),
            PeerReply::opened(Duration::ZERO),
            PeerReply::Frame(Duration::ZERO, FrameType::Response, json!({"result":[]})),
        ]);
        let client = QtaClient::for_test(&peer.path).unwrap();
        assert!(client.poll().unwrap().windows.is_empty());
        let requests = peer.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(
            requests[1], requests[3],
            "retry the same operation after reopening"
        );
    }

    #[test]
    fn qta_module_reloading_stops_after_two_attempts() {
        use crate::fusiform_client::tests::{DeadlinePeer, PeerReply};
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::error("module_reloading", "first refusal"),
            PeerReply::opened(Duration::ZERO),
            PeerReply::error("module_reloading", "last refusal"),
        ]);
        let client = QtaClient::for_test(&peer.path).unwrap();
        let error = client.poll().unwrap_err();
        assert!(format!("{error:?}").contains("last refusal"), "{error:?}");
        assert_eq!(peer.requests.lock().unwrap().len(), 4);
    }

    #[test]
    fn qta_peer_call_deadline_is_not_extended_by_pings() {
        use crate::fusiform_client::tests::{DeadlinePeer, PeerReply};
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::Pings,
            PeerReply::opened(Duration::ZERO),
            PeerReply::Pings,
        ]);
        let client =
            QtaClient::new_with_request_timeout(&peer.path, Duration::from_millis(500)).unwrap();
        let started = std::time::Instant::now();
        let error = client.poll().unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(750),
            "call lasted {elapsed:?}: {error:?}"
        );
        assert!(
            format!("{error:?}").contains("response timeout"),
            "{error:?}"
        );
        assert!(
            peer.requests.lock().unwrap().len() >= 2,
            "the operation must reach the peer"
        );
        assert!(client.live.blocking_lock().is_none());
    }

    #[test]
    fn qta_peer_call_deadline_includes_route_open_and_retry() {
        use crate::fusiform_client::tests::{DeadlinePeer, PeerReply};
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::from_millis(300)),
            PeerReply::error("unknown_channel", "channel removed"),
            PeerReply::opened(Duration::from_millis(300)),
            PeerReply::Pings,
        ]);
        let client =
            QtaClient::new_with_request_timeout(&peer.path, Duration::from_millis(500)).unwrap();
        let started = std::time::Instant::now();
        let error = client.poll().unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(750),
            "setup/retry lasted {elapsed:?}: {error:?}"
        );
        assert!(
            format!("{error:?}").contains("response timeout"),
            "{error:?}"
        );
        assert_eq!(peer.requests.lock().unwrap().len(), 3);
    }
}
